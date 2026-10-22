#!/usr/bin/env python3
"""Compute the consistency statistics: run-to-run jumpiness and convergence.

Definitions (see README "Consistency statistics" and statistics_plugins/
consistency.py for the parameter sets):

- **Jumpiness J** (observation-free). For a valid date V and lead pair
  (L+1, L), binarize the two successive forecasts of V at threshold q mm/day,
  convert each binary field to neighborhood fractions over a square window of
  width n *kilometers*, and compute J = 1 - FSS between the two forecasts:

      FSS = 1 - sum((f1 - f2)^2) / (sum(f1^2) + sum(f2^2))

  J = 0 means the two runs agree exactly at that threshold/scale; J -> 1 means
  a complete flip. If neither field has any exceedance the denominator is zero
  and J is undefined for that pair (missing, not zero).

- **Convergence e_L** (observation-aware). RMSE between the lead-L forecast
  and PRISM for V. Same valid-date set as J so the two curves are jointly
  interpretable (lock-in gap).

Scorable valid dates: V is scorable for a model only if *every* lead
1..lead_days_max exists on disk AND the PRISM day exists. (PRISM is not
mathematically required for J, but gating both statistics on the same date set
is what makes the J and e_L curves comparable — the whole point of the pair.)

Aggregation model (deliberate deviation from per-date means): like the other
statistics, monthly per-cell *accumulators* are the storage primitive —
jumpiness stores the FSS numerator and denominator summed over scorable dates,
convergence stores sum of squared error and sample count. Seasons/years derive
by summing months, and a region value is the ratio of region-summed
accumulators (pooled J / pooled RMSE), not the mean of per-date values. Pooled
FSS is the standard multi-case aggregation (numerator and denominator summed
separately) and is what makes arbitrary-region queries tractable; the exact
per-date curves are preserved CONUS-wide in consistency_daily.npz.

Neighborhood windows are true kilometers: the box half-width in cells is
n/2 / (cell size in km), latitude-dependent along longitude rows, with exact
fractional-cell weighting (integral of the piecewise-constant field over the
window via cumulative sums). Fractions are normalized by the window's overlap
with *finite land* cells, so coasts and grid edges renormalize rather than
dilute.

Revision planes (grid-pooled revision efficiency, contract §5): alongside the
FSS accumulators, each pair file also stores per-cell rev_sse (sum over dates
of (f_L - f_(L+1))^2 over land where both leads are finite) and rev_cnt (the
matching sample count). Pooled over a region mask and divided by the pooled
skill gain MSE(lead_max) - MSE(lead_min) from the convergence planes, they
yield the grid-pooled R for arbitrary regions/periods without per-date data.

Version windows (W1): pooled products (the monthly/seasonal/yearly planes)
fold only valid dates whose *entire* init span lies inside the model's
primary version window (ModelConfig.valid_date_in_primary_window), so no
plane ever mixes runs across a version switch. The per-date sidecar
consistency_daily.npz stays full-record — every scorable date is appended
regardless of window — and carries the window metadata so consumers can
partition offline.

Per-date pooling sidecar (W2): alongside the finalized daily J / e_L curves,
consistency_daily.npz stores the per-date CONUS-pooled *accumulator scalars*
(jump_num/jump_den per pair x threshold x scale, conv_sse/conv_cnt per lead,
rev_sse/rev_cnt per pair). Pooled J / e_L / grid-pooled R over any date
subset (a version window, an AR episode set, a cross-model intersection) is
then Σnum/Σden over that subset — the identical estimator to the planes,
without another recompute.

Regional sidecar accumulators (v4): the same per-date jump_num/jump_den
scalars additionally pooled over each western HUC2 mask (18, 17, 16, 15, 14;
keys jump_num_huc{XX}/jump_den_huc{XX}), so regional pooled J supports date
resampling exactly like the CONUS scalars. Fractions are computed grid-wide
and only the summed terms are masked: a region's J correctly sees the
neighborhood window spilling across its boundary.

Storage:
  stats_output/{model}/jumpiness/monthly/MM/pair_L.npz   (num/den per q,n + rev_sse/rev_cnt)
  stats_output/{model}/jumpiness/pair_L.npz              (yearly)
  stats_output/{model}/jumpiness/seasonal/{ssn}/pair_L.npz
  stats_output/{model}/convergence/.../lead_L.npz        (sse/count)
  stats_output/{model}/consistency_daily.npz             (per-date curves + accumulators)
  stats_output/{model}/manifest.json                     (unified stage state)

"pair_L" is the *lower* lead of the run pair: pair_3 compares the forecasts
for the same valid date issued at leads 4 and 3.

Incremental runs fold only newly scorable valid dates into the accumulators
(a date becomes scorable late when a missing lead or PRISM day arrives). A
changed lead range, threshold set, or scale set forces a full recompute, as
does a stale .consistency_in_progress marker (a previous run crashed between
writing accumulators and the manifest, so the monthly files may contain
unrecorded dates and only a recompute is safe).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Tuple

import numpy as np

from model_registry import MODEL_REGISTRY
from stats_pipeline.verification import (
    SEASONS,
    grid_transform,
    read_prism,
    reproject_prism,
)
from statistics_plugins.consistency import (
    JUMPINESS_SCALES_KM,
    JUMPINESS_THRESHOLDS_MM,
    combo_tag,
)
from stats_pipeline.manifest import fingerprint_payload, stage_state, update_stage
from stats_pipeline.io import atomic_savez

PRISM_DIR = Path("prism_data")
STATS_ROOT = Path("stats_output")

# v2: pair files gained rev_sse/rev_cnt revision planes.
# v3: planes are filtered to the primary version window (W1) and
#     consistency_daily.npz gained the per-date accumulator scalars (W2);
#     the bump forces the one full recompute that backfills both.
# v4: consistency_daily.npz gained per-date jump_num/jump_den pooled over
#     each western HUC2 mask (regional J needs per-date terms for cluster
#     resampling; the planes only give point estimates). The bump forces
#     the full recompute that backfills the regional accumulators.
MANIFEST_SCHEMA_VERSION = 4
IN_PROGRESS_MARKER = ".consistency_in_progress"
DAILY_CURVES_NAME = "consistency_daily.npz"

# Western HUC2 regions whose per-date pooled jump accumulators ride in the
# sidecar alongside the CONUS scalars. Fractions stay grid-wide — only the
# summed num/den terms are masked — so a region's pooled J legitimately sees
# neighborhoods spilling across its boundary (no edge-truncation artifact).
SIDECAR_REGION_HUC2S = ("18", "17", "16", "15", "14")

# Kilometers per degree of latitude (spherical mean Earth radius 6371.0088 km).
KM_PER_DEG = 111.195


# ---------------------------------------------------------------------------
# Neighborhood box sums (exact km windows, separable, fractional cell weights)
# ---------------------------------------------------------------------------

def _interp_cumulative(cum: np.ndarray, pos: np.ndarray) -> np.ndarray:
    """Evaluate row-wise cumulative integrals at fractional positions.

    ``cum`` is (rows, W+1): cum[r, k] = integral of row r over cells [0, k).
    ``pos`` is (rows, W) in cell-boundary coordinates, already clipped to
    [0, W]. The underlying field is piecewise constant, so its integral is
    piecewise linear and linear interpolation between knots is exact.
    """
    n_bounds = cum.shape[1] - 1
    idx = np.minimum(pos.astype(np.int64), n_bounds - 1)
    frac = pos - idx
    rows = np.arange(cum.shape[0])[:, None]
    return cum[rows, idx] * (1.0 - frac) + cum[rows, idx + 1] * frac


def _box_sum_rows(field: np.ndarray, half_width_cells: np.ndarray) -> np.ndarray:
    """Sum of ``field`` over a +-half_width window along axis 1.

    ``half_width_cells`` is per-row (in cell units, may be fractional).
    Cells partially inside the window contribute proportionally; the window
    truncates at the grid edge.
    """
    n_rows, n_cols = field.shape
    cum = np.empty((n_rows, n_cols + 1), dtype=np.float64)
    cum[:, 0] = 0.0
    np.cumsum(field, axis=1, out=cum[:, 1:])
    centers = np.arange(n_cols, dtype=np.float64) + 0.5
    half = half_width_cells[:, None]
    lo = np.clip(centers[None, :] - half, 0.0, float(n_cols))
    hi = np.clip(centers[None, :] + half, 0.0, float(n_cols))
    return _interp_cumulative(cum, hi) - _interp_cumulative(cum, lo)


class BoxOperator:
    """Area-weighted box sum over a square n x n km window on a lat/lon grid.

    Separable: a longitude pass with a latitude-dependent half-width (grid
    cells shrink east-west with cos(lat)) followed by a latitude pass with a
    constant half-width. ``fractions(values, finite)`` returns
    box_sum(values * finite) / box_sum(finite): the neighborhood mean over
    the finite cells of the window (NaN where the window holds none).
    """

    def __init__(self, lats: np.ndarray, lons: np.ndarray, n_km: float):
        dlat = abs(float(lats[1] - lats[0]))
        dlon = abs(float(lons[1] - lons[0]))
        half_km = n_km / 2.0
        dy_km = dlat * KM_PER_DEG
        dx_km = dlon * KM_PER_DEG * np.cos(np.radians(lats.astype(np.float64)))
        self._half_y = np.full(lons.size, half_km / dy_km, dtype=np.float64)
        self._half_x = (half_km / dx_km).astype(np.float64)

    def box_sum(self, field: np.ndarray) -> np.ndarray:
        along_lon = _box_sum_rows(field.astype(np.float64), self._half_x)
        return _box_sum_rows(along_lon.T, self._half_y).T


# ---------------------------------------------------------------------------
# Pure per-pair jumpiness (shared by the stage and the calibration harness)
# ---------------------------------------------------------------------------

def pair_jumpiness_terms(
    frac_a: np.ndarray, frac_b: np.ndarray, both: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """FSS accumulator planes for one fraction pair: num=(a−b)², den=a²+b².

    ``both`` masks cells where the day is land and both fractions are finite;
    outside it both terms are zero so pooled sums are unaffected. Pooled
    J = Σnum / Σden over any region/date set.
    """
    a = np.where(both, frac_a, 0.0)
    b = np.where(both, frac_b, 0.0)
    return (a - b) ** 2, a * a + b * b


def neighborhood_fractions(
    field: np.ndarray, land: np.ndarray, box_op: "BoxOperator", threshold_mm: float,
) -> np.ndarray:
    """Exceedance-neighborhood fractions of one field (NaN where no land)."""
    finite = land & np.isfinite(field)
    exceed = np.where(finite, field >= threshold_mm, False).astype(np.float64)
    den = box_op.box_sum(finite.astype(np.float64))
    with np.errstate(invalid="ignore", divide="ignore"):
        frac = box_op.box_sum(exceed) / den
    frac[den <= 0.0] = np.nan
    return frac


def pair_jumpiness(
    field_a: np.ndarray, field_b: np.ndarray, land: np.ndarray,
    box_op: "BoxOperator", threshold_mm: float,
) -> float:
    """Pooled J for one forecast pair at one (q, n) — the W4 harness entry.

    Exactly the per-date path of ``_process_valid_date`` for a single
    threshold/scale combo, callable on in-memory synthetic fields. NaN when
    neither field has any exceedance (undefined, not zero).
    """
    frac_a = neighborhood_fractions(field_a, land, box_op, threshold_mm)
    frac_b = neighborhood_fractions(field_b, land, box_op, threshold_mm)
    both = land & np.isfinite(frac_a) & np.isfinite(frac_b)
    num, den = pair_jumpiness_terms(frac_a, frac_b, both)
    den_total = den.sum()
    return float(num.sum() / den_total) if den_total > 0.0 else float("nan")


# ---------------------------------------------------------------------------
# Data discovery
# ---------------------------------------------------------------------------

def scan_inits(model_dir: Path, cycle_hour: int,
                lead_min: int, lead_max: int) -> Dict[datetime, Dict[int, Path]]:
    """Map init date -> {lead_days: precip .npy path} for one model archive."""
    inits: Dict[datetime, Dict[int, Path]] = {}
    suffix = f"_{cycle_hour:02d}z"
    for year_dir in sorted(p for p in model_dir.iterdir()
                           if p.is_dir() and p.name.isdigit()):
        for init_dir in sorted(year_dir.iterdir()):
            if not init_dir.is_dir() or not init_dir.name.endswith(suffix):
                continue
            init_date = datetime.strptime(init_dir.name.split("_")[0], "%Y%m%d")
            leads: Dict[int, Path] = {}
            for path in init_dir.iterdir():
                name = path.name
                if not name.startswith("f") or not name.endswith("_surface.npy"):
                    continue
                try:
                    fhour = int(name[1:4])
                except ValueError:
                    continue
                if fhour % 24 != 0:
                    continue
                lead = fhour // 24
                if lead_min <= lead <= lead_max:
                    leads[lead] = path
            if leads:
                inits[init_date] = leads
    return inits


def prism_path(valid_date: datetime) -> Path:
    return PRISM_DIR / str(valid_date.year) / valid_date.strftime("%Y%m%d") / "data.tif"


def scorable_dates(
    inits: Dict[datetime, Dict[int, Path]], lead_min: int, lead_max: int,
) -> Dict[datetime, Dict[int, Path]]:
    """Valid dates with every lead lead_min..lead_max AND a PRISM day on disk.

    Returns {valid_date: {lead: npy_path}}.
    """
    scorable: Dict[datetime, Dict[int, Path]] = {}
    if not inits:
        return scorable
    first = min(inits) + timedelta(days=lead_min)
    last = max(inits) + timedelta(days=lead_max)
    day = first
    while day <= last:
        lead_paths: Dict[int, Path] = {}
        for lead in range(lead_min, lead_max + 1):
            init_leads = inits.get(day - timedelta(days=lead))
            if init_leads is None or lead not in init_leads:
                break
            lead_paths[lead] = init_leads[lead]
        else:
            if prism_path(day).exists():
                scorable[day] = lead_paths
        day += timedelta(days=1)
    return scorable


# ---------------------------------------------------------------------------
# Accumulators
# ---------------------------------------------------------------------------

class MonthlyAccumulators:
    """Running per-month accumulators for both consistency statistics.

    Jumpiness num/den are float32 on disk and in RAM (values are sums of
    neighborhood fractions squared, bounded by 2x the date count — nowhere
    near float32's precision limits, and the 12-combo fan-out doubles in size
    at float64 for no benefit). Convergence follows the repo convention for
    error sums (float64).
    """

    def __init__(self, shape: Tuple[int, int], n_pairs: int, n_leads: int):
        self.shape = shape
        self.n_pairs = n_pairs
        self.n_leads = n_leads
        self.n_q = len(JUMPINESS_THRESHOLDS_MM)
        self.n_s = len(JUMPINESS_SCALES_KM)
        # month -> arrays
        self.jump_num: Dict[int, np.ndarray] = {}   # (P, NQ, NS, H, W) float32
        self.jump_den: Dict[int, np.ndarray] = {}
        self.rev_sse: Dict[int, np.ndarray] = {}    # (P, H, W) float64
        self.rev_cnt: Dict[int, np.ndarray] = {}    # (P, H, W) int32
        self.conv_sse: Dict[int, np.ndarray] = {}   # (L, H, W) float64
        self.conv_cnt: Dict[int, np.ndarray] = {}   # (L, H, W) int32
        self.date_count: Dict[int, int] = {}

    def _ensure_month(self, month: int) -> None:
        if month in self.jump_num:
            return
        h, w = self.shape
        self.jump_num[month] = np.zeros(
            (self.n_pairs, self.n_q, self.n_s, h, w), dtype=np.float32)
        self.jump_den[month] = np.zeros_like(self.jump_num[month])
        self.rev_sse[month] = np.zeros((self.n_pairs, h, w), dtype=np.float64)
        self.rev_cnt[month] = np.zeros((self.n_pairs, h, w), dtype=np.int32)
        self.conv_sse[month] = np.zeros((self.n_leads, h, w), dtype=np.float64)
        self.conv_cnt[month] = np.zeros((self.n_leads, h, w), dtype=np.int32)
        self.date_count[month] = 0

    def fold(self, month: int, jump_num: np.ndarray, jump_den: np.ndarray,
             rev_sse: np.ndarray, rev_cnt: np.ndarray,
             conv_sse: np.ndarray, conv_cnt: np.ndarray) -> None:
        self._ensure_month(month)
        self.jump_num[month] += jump_num
        self.jump_den[month] += jump_den
        self.rev_sse[month] += rev_sse
        self.rev_cnt[month] += rev_cnt
        self.conv_sse[month] += conv_sse
        self.conv_cnt[month] += conv_cnt
        self.date_count[month] += 1

    def months(self) -> list[int]:
        return sorted(self.jump_num)


# ---------------------------------------------------------------------------
# Per-date processing
# ---------------------------------------------------------------------------

def _process_valid_date(
    lead_paths: Dict[int, Path],
    prism_path: Path,
    grid_lats: np.ndarray,
    grid_lons: np.ndarray,
    box_ops: list[BoxOperator],
    lead_min: int,
    lead_max: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray,
           np.ndarray, np.ndarray, np.ndarray]:
    """Compute one scorable date's accumulator deltas and CONUS curve points.

    Returns (jump_num, jump_den, rev_sse, rev_cnt, conv_sse, conv_cnt,
    daily_J, daily_e):
      jump_num/jump_den: (P, NQ, NS, H, W) float32
      rev_sse: (P, H, W) float64; rev_cnt: (P, H, W) int32
      conv_sse: (L, H, W) float64; conv_cnt: (L, H, W) int32
      daily_J: (P, NQ, NS) float32 (NaN where undefined)
      daily_e: (L,) float32
    """
    shape = (grid_lats.size, grid_lons.size)
    n_leads = lead_max - lead_min + 1
    n_pairs = n_leads - 1
    n_q = len(JUMPINESS_THRESHOLDS_MM)
    n_s = len(JUMPINESS_SCALES_KM)

    fields = np.empty((n_leads,) + shape, dtype=np.float32)
    for i, lead in enumerate(range(lead_min, lead_max + 1)):
        data = np.load(lead_paths[lead])
        if data.shape != shape:
            raise ValueError(
                f"{lead_paths[lead]}: shape {data.shape} != grid {shape}")
        fields[i] = data

    prism_data, prism_transform, prism_crs = read_prism(prism_path)
    transform = grid_transform(grid_lats, grid_lons)
    prism_on_grid = reproject_prism(
        prism_data, prism_transform, prism_crs, shape, transform)
    land = np.isfinite(prism_on_grid)

    # --- Convergence: squared error vs PRISM per lead over land -------------
    conv_sse = np.zeros((n_leads,) + shape, dtype=np.float64)
    conv_cnt = np.zeros((n_leads,) + shape, dtype=np.int32)
    daily_e = np.full(n_leads, np.nan, dtype=np.float32)
    for i in range(n_leads):
        valid = land & np.isfinite(fields[i])
        diff = np.where(valid, fields[i] - prism_on_grid, 0.0)
        conv_sse[i] = diff.astype(np.float64) ** 2
        conv_cnt[i] = valid
        n_cells = int(valid.sum())
        if n_cells:
            daily_e[i] = np.sqrt(conv_sse[i].sum() / n_cells)

    # --- Revision planes: squared successive-lead revision per pair ---------
    # Pair p compares leads (lead_min+p+1, lead_min+p) = fields[p+1], fields[p];
    # cells count only where the day is land and BOTH leads are finite, so the
    # pooled mean revision is never diluted by missing runs.
    rev_sse = np.zeros((n_pairs,) + shape, dtype=np.float64)
    rev_cnt = np.zeros((n_pairs,) + shape, dtype=np.int32)
    for pair in range(n_pairs):
        both = land & np.isfinite(fields[pair]) & np.isfinite(fields[pair + 1])
        diff = np.where(both, fields[pair].astype(np.float64) - fields[pair + 1], 0.0)
        rev_sse[pair] = diff ** 2
        rev_cnt[pair] = both

    # --- Jumpiness: neighborhood fractions and pairwise FSS -----------------
    finite = [land & np.isfinite(fields[i]) for i in range(n_leads)]
    jump_num = np.zeros((n_pairs, n_q, n_s) + shape, dtype=np.float32)
    jump_den = np.zeros_like(jump_num)
    daily_j = np.full((n_pairs, n_q, n_s), np.nan, dtype=np.float32)

    # Fraction stacks are computed one lead at a time so at most two leads'
    # stacks (the current pair) are alive at once. NaN structure is threshold-
    # independent (it only reflects windows with no finite land area), so the
    # pairwise mask is per-scale, shared across thresholds.
    def _fractions_for(i: int) -> np.ndarray:
        out = np.empty((n_q, n_s) + shape, dtype=np.float64)
        fin = finite[i]
        fin_f = fin.astype(np.float64)
        exceed = [
            np.where(fin, fields[i] >= q, False).astype(np.float64)
            for q in JUMPINESS_THRESHOLDS_MM
        ]
        for si, op in enumerate(box_ops):
            den = op.box_sum(fin_f)
            with np.errstate(invalid="ignore", divide="ignore"):
                for qi in range(n_q):
                    frac = op.box_sum(exceed[qi]) / den
                    frac[den <= 0.0] = np.nan
                    out[qi, si] = frac
        return out

    prev = _fractions_for(0)
    for pair in range(n_pairs):
        nxt = _fractions_for(pair + 1)
        # pair index `pair` (0-based) = lead pair (lead_min+pair+1, lead_min+pair):
        # f2 is the newer run (shorter lead), f1 the older (longer lead).
        f2, f1 = prev, nxt
        for si in range(n_s):
            both = land & np.isfinite(f1[0, si]) & np.isfinite(f2[0, si])
            for qi in range(n_q):
                num, den = pair_jumpiness_terms(f1[qi, si], f2[qi, si], both)
                jump_num[pair, qi, si] = num
                jump_den[pair, qi, si] = den
                den_total = den.sum()
                if den_total > 0.0:
                    daily_j[pair, qi, si] = num.sum() / den_total
        prev = nxt

    return jump_num, jump_den, rev_sse, rev_cnt, conv_sse, conv_cnt, daily_j, daily_e


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _jump_keys() -> list[tuple[int, int, str]]:
    keys = []
    for qi, q in enumerate(JUMPINESS_THRESHOLDS_MM):
        for si, s in enumerate(JUMPINESS_SCALES_KM):
            keys.append((qi, si, combo_tag(q, s)))
    return keys


def _write_jump_lead_files(out_dir: Path, num: np.ndarray, den: np.ndarray,
                           rev_sse: np.ndarray, rev_cnt: np.ndarray,
                           date_count: int) -> None:
    """Write pair_L.npz files from (P, NQ, NS, H, W) num/den blocks.

    Each pair file also carries that pair's revision plane (rev_sse/rev_cnt,
    (H, W)) — the accumulator for grid-pooled revision efficiency.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    for pair in range(num.shape[0]):
        arrays: Dict[str, np.ndarray] = {}
        for qi, si, tag in _jump_keys():
            arrays[f"num_{tag}"] = num[pair, qi, si]
            arrays[f"den_{tag}"] = den[pair, qi, si]
        arrays["rev_sse"] = rev_sse[pair]
        arrays["rev_cnt"] = rev_cnt[pair]
        arrays["date_count"] = np.int64(date_count)
        atomic_savez(out_dir / f"pair_{pair + 1}.npz", **arrays)


def _write_conv_lead_files(out_dir: Path, sse: np.ndarray, cnt: np.ndarray,
                           date_count: int, lead_min: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for i in range(sse.shape[0]):
        count = cnt[i]
        with np.errstate(invalid="ignore", divide="ignore"):
            value = np.sqrt(sse[i] / count).astype(np.float32)
        value[count <= 0] = np.nan
        atomic_savez(
            out_dir / f"lead_{lead_min + i}.npz",
            value=value,
            sum_squared_error=sse[i],
            sample_count=count,
            date_count=np.int64(date_count),
        )


def _write_outputs(model_key: str, acc: MonthlyAccumulators, lead_min: int) -> None:
    root = STATS_ROOT / model_key
    jump_dir = root / "jumpiness"
    conv_dir = root / "convergence"

    months = acc.months()

    for month in months:
        mm = f"{month:02d}"
        _write_jump_lead_files(jump_dir / "monthly" / mm,
                               acc.jump_num[month], acc.jump_den[month],
                               acc.rev_sse[month], acc.rev_cnt[month],
                               acc.date_count[month])
        _write_conv_lead_files(conv_dir / "monthly" / mm,
                               acc.conv_sse[month], acc.conv_cnt[month],
                               acc.date_count[month], lead_min)

    def _summed(month_list: Iterable[int]):
        sel = [m for m in month_list if m in acc.jump_num]
        num = sum(acc.jump_num[m] for m in sel)
        den = sum(acc.jump_den[m] for m in sel)
        rev_sse = sum(acc.rev_sse[m] for m in sel)
        rev_cnt = sum(acc.rev_cnt[m] for m in sel)
        sse = sum(acc.conv_sse[m] for m in sel)
        cnt = sum(acc.conv_cnt[m] for m in sel)
        dates = sum(acc.date_count[m] for m in sel)
        return num, den, rev_sse, rev_cnt, sse, cnt, dates

    # Yearly (all months pooled).
    num, den, rev_sse, rev_cnt, sse, cnt, dates = _summed(months)
    _write_jump_lead_files(jump_dir, num, den, rev_sse, rev_cnt, dates)
    _write_conv_lead_files(conv_dir, sse, cnt, dates, lead_min)

    # Seasonal.
    for season, season_months in SEASONS.items():
        if not any(m in acc.jump_num for m in season_months):
            continue
        num, den, rev_sse, rev_cnt, sse, cnt, dates = _summed(season_months)
        _write_jump_lead_files(jump_dir / "seasonal" / season, num, den,
                               rev_sse, rev_cnt, dates)
        _write_conv_lead_files(conv_dir / "seasonal" / season, sse, cnt,
                               dates, lead_min)


def _load_existing(model_key: str, shape: Tuple[int, int], n_pairs: int,
                   n_leads: int, lead_min: int) -> MonthlyAccumulators | None:
    """Rebuild running accumulators from monthly npz files, or None."""
    root = STATS_ROOT / model_key
    acc = MonthlyAccumulators(shape, n_pairs, n_leads)
    jump_monthly = root / "jumpiness" / "monthly"
    conv_monthly = root / "convergence" / "monthly"
    if not jump_monthly.is_dir() or not conv_monthly.is_dir():
        return None
    keys = _jump_keys()
    try:
        for month_dir in sorted(jump_monthly.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue
            month = int(month_dir.name)
            acc._ensure_month(month)
            date_count = None
            for pair in range(n_pairs):
                with np.load(month_dir / f"pair_{pair + 1}.npz") as data:
                    for qi, si, tag in keys:
                        acc.jump_num[month][pair, qi, si] = data[f"num_{tag}"]
                        acc.jump_den[month][pair, qi, si] = data[f"den_{tag}"]
                    # Pre-v2 files lack the revision planes; the KeyError
                    # falls through to the full-recompute path below.
                    acc.rev_sse[month][pair] = data["rev_sse"]
                    acc.rev_cnt[month][pair] = data["rev_cnt"]
                    date_count = int(data["date_count"])
            for i in range(n_leads):
                with np.load(conv_monthly / month_dir.name /
                             f"lead_{lead_min + i}.npz") as data:
                    acc.conv_sse[month][i] = data["sum_squared_error"]
                    acc.conv_cnt[month][i] = data["sample_count"]
                    conv_dates = int(data["date_count"])
            if date_count is None or date_count != conv_dates:
                print(f"  Inconsistent date counts in month {month:02d}; "
                      "full recompute required.")
                return None
            acc.date_count[month] = date_count
    except (FileNotFoundError, KeyError, ValueError) as exc:
        print(f"  Could not load existing consistency accumulators ({exc}); "
              "full recompute required.")
        return None
    return acc


class DailyCurves:
    """Per-date CONUS-wide curves plus pooled-accumulator scalars (W2).

    The finalized daily J / e_L curves are what the dashboard reads; the
    accumulator arrays are their unreduced numerators/denominators, so pooled
    values over any date subset reconstruct exactly (Σnum/Σden) offline.
    Full-record by design: never window-filtered (see module docstring).
    """

    ACCUMULATOR_KEYS = ("jump_num", "jump_den", "conv_sse", "conv_cnt",
                        "rev_sse", "rev_cnt") + tuple(
        f"jump_{part}_huc{huc}"
        for huc in SIDECAR_REGION_HUC2S for part in ("num", "den"))

    def __init__(self, dates: np.ndarray, jump: np.ndarray, conv: np.ndarray,
                 accumulators: Dict[str, np.ndarray]):
        self.dates = dates  # (D,) int64 YYYYMMDD, sorted
        self.jump = jump    # (D, P, NQ, NS) float32
        self.conv = conv    # (D, L) float32
        self.acc = accumulators
        # jump_num/jump_den: (D, P, NQ, NS) float64 — per-date CONUS FSS sums
        # conv_sse: (D, L) float64; conv_cnt: (D, L) int64
        # rev_sse: (D, P) float64; rev_cnt: (D, P) int64

    @classmethod
    def empty(cls, n_pairs: int, n_leads: int) -> "DailyCurves":
        n_q = len(JUMPINESS_THRESHOLDS_MM)
        n_s = len(JUMPINESS_SCALES_KM)
        accumulators = {
            "jump_num": np.empty((0, n_pairs, n_q, n_s), dtype=np.float64),
            "jump_den": np.empty((0, n_pairs, n_q, n_s), dtype=np.float64),
            "conv_sse": np.empty((0, n_leads), dtype=np.float64),
            "conv_cnt": np.empty((0, n_leads), dtype=np.int64),
            "rev_sse": np.empty((0, n_pairs), dtype=np.float64),
            "rev_cnt": np.empty((0, n_pairs), dtype=np.int64),
        }
        for huc in SIDECAR_REGION_HUC2S:
            for part in ("num", "den"):
                accumulators[f"jump_{part}_huc{huc}"] = np.empty(
                    (0, n_pairs, n_q, n_s), dtype=np.float64)
        return cls(np.empty(0, dtype=np.int64),
                   np.empty((0, n_pairs, n_q, n_s), dtype=np.float32),
                   np.empty((0, n_leads), dtype=np.float32),
                   accumulators)

    @classmethod
    def load(cls, path: Path, n_pairs: int, n_leads: int) -> "DailyCurves | None":
        if not path.exists():
            return cls.empty(n_pairs, n_leads)
        try:
            with np.load(path) as data:
                # Pre-W2 files lack the accumulators; the KeyError forces the
                # full recompute that backfills them.
                accumulators = {key: np.array(data[key])
                                for key in cls.ACCUMULATOR_KEYS}
                curves = cls(data["dates"].astype(np.int64),
                             data["jumpiness"], data["convergence"],
                             accumulators)
        except (OSError, KeyError, ValueError) as exc:
            print(f"  Could not load {path.name} ({exc}); full recompute required.")
            return None
        if (curves.jump.shape[1:] != (n_pairs, len(JUMPINESS_THRESHOLDS_MM),
                                      len(JUMPINESS_SCALES_KM))
                or curves.conv.shape[1:] != (n_leads,)
                or any(curves.acc[key].shape != curves.jump.shape
                       for key in cls.ACCUMULATOR_KEYS
                       if key.startswith("jump_"))
                or curves.acc["conv_sse"].shape != curves.conv.shape
                or curves.acc["rev_sse"].shape[1:] != (n_pairs,)):
            print(f"  {path.name} has an incompatible shape; full recompute required.")
            return None
        return curves

    def append(self, date_key: int, daily_j: np.ndarray, daily_e: np.ndarray,
               accumulators: Dict[str, np.ndarray]) -> None:
        self.dates = np.append(self.dates, np.int64(date_key))
        self.jump = np.concatenate([self.jump, daily_j[None]], axis=0)
        self.conv = np.concatenate([self.conv, daily_e[None]], axis=0)
        for key in self.ACCUMULATOR_KEYS:
            self.acc[key] = np.concatenate(
                [self.acc[key], accumulators[key][None]], axis=0)

    def save(self, path: Path, lead_min: int, lead_max: int,
             window_block_json: str) -> None:
        order = np.argsort(self.dates)
        atomic_savez(
            path,
            dates=self.dates[order],
            jumpiness=self.jump[order],
            convergence=self.conv[order],
            **{key: self.acc[key][order] for key in self.ACCUMULATOR_KEYS},
            thresholds_mm=np.array(JUMPINESS_THRESHOLDS_MM, dtype=np.float64),
            scales_km=np.array(JUMPINESS_SCALES_KM, dtype=np.int64),
            sidecar_region_hucs=np.array(SIDECAR_REGION_HUC2S),
            lead_min=np.int64(lead_min),
            lead_max=np.int64(lead_max),
            # Window metadata rides along so offline consumers can partition
            # this full-record file without re-reading the registry.
            version_windows=np.array(window_block_json),
        )


def _load_manifest(root: Path) -> dict | None:
    return stage_state(root, "consistency")


def _save_manifest(root: Path, processed: Iterable[str],
                   lead_min: int, lead_max: int, window_block: dict) -> None:
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "lead_days_min": lead_min,
        "lead_days_max": lead_max,
        "thresholds_mm": list(JUMPINESS_THRESHOLDS_MM),
        "scales_km": list(JUMPINESS_SCALES_KM),
        "version_window": window_block,
        "processed_valid_dates": sorted(processed),
        "algorithm_fingerprint": fingerprint_payload({
            "schema": MANIFEST_SCHEMA_VERSION,
            "lead_min": lead_min,
            "lead_max": lead_max,
            "thresholds_mm": JUMPINESS_THRESHOLDS_MM,
            "scales_km": JUMPINESS_SCALES_KM,
            "version_window": window_block,
        }),
    }
    update_stage(root, "consistency", manifest)


def _manifest_compatible(manifest: dict | None, lead_min: int, lead_max: int,
                         window_block: dict) -> bool:
    if not manifest or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        return False
    return (
        manifest.get("lead_days_min") == lead_min
        and manifest.get("lead_days_max") == lead_max
        and manifest.get("thresholds_mm") == list(JUMPINESS_THRESHOLDS_MM)
        and manifest.get("scales_km") == list(JUMPINESS_SCALES_KM)
        # A changed window redefines which dates the planes may pool, so it
        # forces the full recompute like a changed threshold set.
        and manifest.get("version_window") == window_block
    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def _sidecar_region_masks(model_key: str,
                          shape: Tuple[int, int]) -> Dict[str, np.ndarray]:
    """Boolean HUC2 masks on *this model's* grid for the sidecar regions.

    Reuses the figure stack's mask builder (the backend rasterizer over the
    WBD layer cache) so the sidecar's regional pooling matches the map
    figures cell for cell — including NBM's half-cell grid registration.
    """
    from figures.lib.geo import region_masks, wbd_units

    units = wbd_units(2)
    masks = region_masks(model_key, units, SIDECAR_REGION_HUC2S)
    for huc, mask in masks.items():
        if mask.shape != shape:
            raise ValueError(
                f"HUC2 {huc} mask shape {mask.shape} != model grid {shape}; "
                "stats_output metadata disagrees with model_data — re-run "
                "the verification stage first")
    return masks


def run_consistency_stage(model_key: str, force: bool = False) -> None:
    config = MODEL_REGISTRY[model_key]
    lead_min, lead_max = config.lead_days_min, config.lead_days_max
    n_leads = lead_max - lead_min + 1
    n_pairs = n_leads - 1
    model_dir = Path(config.data_dir)
    root = STATS_ROOT / model_key
    root.mkdir(parents=True, exist_ok=True)
    marker = root / IN_PROGRESS_MARKER

    print(f"Computing consistency statistics for '{model_key}' "
          f"(leads {lead_min}-{lead_max}, pairs 1-{n_pairs}, "
          f"q={JUMPINESS_THRESHOLDS_MM} mm/day, n={JUMPINESS_SCALES_KM} km)")

    grid_lats = np.load(model_dir / "grid_lats.npy")
    grid_lons = np.load(model_dir / "grid_lons.npy")
    shape = (grid_lats.size, grid_lons.size)
    box_ops = [BoxOperator(grid_lats, grid_lons, n) for n in JUMPINESS_SCALES_KM]
    region_masks_by_huc = _sidecar_region_masks(model_key, shape)

    inits = scan_inits(model_dir, config.cycle_hour, lead_min, lead_max)
    scorable = scorable_dates(inits, lead_min, lead_max)
    print(f"  {len(inits)} init(s) on disk -> {len(scorable)} scorable valid date(s)")
    if not scorable:
        print("  Nothing to do.")
        return

    window_block = config.window_block()
    window = config.primary
    if window is not None:
        valid_start, valid_end = config.primary_valid_range()
        print(f"  Pooled planes restricted to version window '{window.label}' "
              f"(valid dates {valid_start or 'open'} .. {valid_end or 'open'}); "
              "daily sidecar stays full-record.")

    manifest = _load_manifest(root)
    incremental = (not force
                   and not marker.exists()
                   and _manifest_compatible(manifest, lead_min, lead_max,
                                            window_block))
    if marker.exists():
        print("  Stale in-progress marker found (previous run crashed mid-write); "
              "forcing full recompute.")

    acc: MonthlyAccumulators | None = None
    curves: DailyCurves | None = None
    processed: set[str] = set()
    if incremental:
        processed = set(manifest.get("processed_valid_dates", []))
        # Cheap up-to-date check before touching the (large) accumulator files.
        if all(v.strftime("%Y%m%d") in processed for v in scorable):
            print("  Consistency statistics are up to date.")
            return
        acc = _load_existing(model_key, shape, n_pairs, n_leads, lead_min)
        if acc is not None:
            curves = DailyCurves.load(root / DAILY_CURVES_NAME, n_pairs, n_leads)
        if acc is None or curves is None:
            incremental = False

    if not incremental:
        processed = set()
        acc = MonthlyAccumulators(shape, n_pairs, n_leads)
        curves = DailyCurves.empty(n_pairs, n_leads)

    pending = {v: paths for v, paths in scorable.items()
               if v.strftime("%Y%m%d") not in processed}
    if not pending:
        print("  Consistency statistics are up to date.")
        return
    print(f"  Processing {len(pending)} new valid date(s)"
          f"{' (full recompute)' if not incremental else ''}...")

    done = 0
    pooled = 0
    for valid_date in sorted(pending):
        lead_paths = pending[valid_date]
        try:
            (jump_num, jump_den, rev_sse, rev_cnt,
             conv_sse, conv_cnt, daily_j, daily_e) = (
                _process_valid_date(lead_paths, prism_path(valid_date),
                                    grid_lats, grid_lons, box_ops,
                                    lead_min, lead_max)
            )
        except Exception as exc:
            print(f"  WARNING: skipping valid date {valid_date.date()}: {exc}")
            continue
        # Planes pool only version-clean dates (W1); the sidecar records all.
        if config.valid_date_in_primary_window(valid_date.date()):
            acc.fold(valid_date.month, jump_num, jump_den, rev_sse, rev_cnt,
                     conv_sse, conv_cnt)
            pooled += 1
        date_key = valid_date.strftime("%Y%m%d")
        accumulators = {
            "jump_num": jump_num.astype(np.float64).sum(axis=(-2, -1)),
            "jump_den": jump_den.astype(np.float64).sum(axis=(-2, -1)),
            "conv_sse": conv_sse.sum(axis=(-2, -1)),
            "conv_cnt": conv_cnt.astype(np.int64).sum(axis=(-2, -1)),
            "rev_sse": rev_sse.sum(axis=(-2, -1)),
            "rev_cnt": rev_cnt.astype(np.int64).sum(axis=(-2, -1)),
        }
        # Regional pooling reuses the grid-wide fractions — only the summed
        # terms are masked, so neighborhoods spill across region borders by
        # design (do not clip fractions to the region).
        for huc, mask in region_masks_by_huc.items():
            accumulators[f"jump_num_huc{huc}"] = (
                jump_num[:, :, :, mask].astype(np.float64).sum(axis=-1))
            accumulators[f"jump_den_huc{huc}"] = (
                jump_den[:, :, :, mask].astype(np.float64).sum(axis=-1))
        curves.append(int(date_key), daily_j, daily_e, accumulators)
        processed.add(date_key)
        done += 1
        if done % 50 == 0:
            print(f"    ...{done}/{len(pending)} dates")

    if done == 0:
        print("  No dates processed successfully; outputs unchanged.")
        return
    if done != pooled:
        print(f"  {done - pooled}/{done} new date(s) fall outside the primary "
              "window: sidecar only, not pooled.")

    # Crash guard: if we die between here and the manifest write, the monthly
    # files may contain dates the manifest doesn't record; the marker forces
    # the next run to recompute from scratch instead of double-counting.
    marker.touch()
    if acc.months():
        _write_outputs(model_key, acc, lead_min)
    else:
        print("  No in-window dates yet; pooled planes not written.")
    curves.save(root / DAILY_CURVES_NAME, lead_min, lead_max,
                json.dumps(window_block, sort_keys=True))
    _save_manifest(root, processed, lead_min, lead_max, window_block)
    marker.unlink()
    print(f"  Folded {done} date(s); wrote jumpiness + convergence under "
          f"{root.resolve()}")
