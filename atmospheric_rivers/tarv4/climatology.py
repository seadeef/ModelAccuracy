"""IVT percentile climatology for local tARget v4 detection.

Reproduces the threshold construction of Guan & Waliser (2024): for each
pixel and calendar month, percentiles [85, 87.5, 90, 92.5, 95] of 6-hourly
ERA5 IVT magnitude over the five months centered on that month during
1980-2019, plus the hemispheric floor (area-weighted spatial 5th percentile
of the pixel thresholds, per hemisphere and month; target.m lines 463-473).

Two disclosed deviations from the canonical build, both driven by transfer
cost against full-globe-per-hour chunks and both validated downstream by
frame-level comparison against the canonical catalog:
  - samples are 00Z and 12Z (2/day) rather than all four synoptic hours;
  - pixel thresholds are kept at 0.25 deg only inside the detection domain
    (10S-70N, 140E-50W); the hemispheric floor uses an every-6th-pixel
    global subsample (the floor is a spatial percentile of a smooth field,
    insensitive to subsampling).

Histograms use 1 kg/m/s bins; quantiles interpolate within bins, so the
worst-case threshold quantization is well under 1 kg/m/s.
"""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from atmospheric_rivers.paths import TARGET_AR_DATA_DIR
from atmospheric_rivers.tarv4.arco import GRID_LATS, GRID_LONS, iter_ivt_steps

LOCAL_DIR = TARGET_AR_DATA_DIR / "tarv4_local"
CHECKPOINT_DIR = LOCAL_DIR / "climatology_ckpt"
THRESHOLDS_PATH = LOCAL_DIR / "pixel_ivt_limit_local.npz"

CLIMATOLOGY_YEARS = (1980, 2019)
SAMPLE_HOURS = (0, 12)
PERCENTILES = (85.0, 87.5, 90.0, 92.5, 95.0)
GLOBAL_FLOOR_PERCENTILE = 5.0

# Detection domain: generous NE-Pacific/North-America window. Rows/cols on
# the 721 x 1440 grid (lat descending from 90, lon ascending from 0).
DOMAIN_ROWS = slice(80, 401)     # lat 70.0 .. -10.0
DOMAIN_COLS = slice(560, 1241)   # lon 140.0E .. 310.0E (50W)
SUBSAMPLE = 6                    # global floor grid: every 6th pixel (1.5 deg)

BIN_WIDTH = 1.0                  # kg/m/s
NBINS = 1201                     # 0..1200, top bin clamps the tail

DOMAIN_LATS = GRID_LATS[DOMAIN_ROWS]
DOMAIN_LONS = GRID_LONS[DOMAIN_COLS]
SUB_LATS = GRID_LATS[::SUBSAMPLE]
SUB_LONS = GRID_LONS[::SUBSAMPLE]

EARTH_RADIUS = 6371e3


def pixel_areas(lats: np.ndarray, dlat: float, dlon: float) -> np.ndarray:
    """Spherical quad areas per latitude row (target.m areaquad)."""
    upper = np.radians(np.clip(lats + dlat / 2, -90.0, 90.0))
    lower = np.radians(np.clip(lats - dlat / 2, -90.0, 90.0))
    return (EARTH_RADIUS ** 2 * np.radians(dlon)
            * np.abs(np.sin(upper) - np.sin(lower)))


def sample_steps() -> list[np.datetime64]:
    first, last = CLIMATOLOGY_YEARS
    steps = []
    day = date(first, 1, 1)
    stop = date(last, 12, 31)
    while day <= stop:
        for hour in SAMPLE_HOURS:
            steps.append(np.datetime64(f"{day.isoformat()}T{hour:02d}", "h"))
        day += timedelta(days=1)
    return steps


def _config() -> dict:
    return {
        "years": list(CLIMATOLOGY_YEARS), "hours": list(SAMPLE_HOURS),
        "nbins": NBINS, "bin_width": BIN_WIDTH,
        "domain_rows": [DOMAIN_ROWS.start, DOMAIN_ROWS.stop],
        "domain_cols": [DOMAIN_COLS.start, DOMAIN_COLS.stop],
        "subsample": SUBSAMPLE,
    }


def _load_checkpoint():
    cursor_path = CHECKPOINT_DIR / "cursor.json"
    if not cursor_path.is_file():
        return None
    cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
    if cursor.get("config") != _config():
        raise RuntimeError(
            f"climatology checkpoint at {CHECKPOINT_DIR} was built with a "
            "different configuration; delete it to rebuild")
    hist_dom = np.load(CHECKPOINT_DIR / "hist_dom.npy")
    hist_sub = np.load(CHECKPOINT_DIR / "hist_sub.npy")
    return cursor["steps_done"], hist_dom, hist_sub


def _save_checkpoint(steps_done: int, hist_dom, hist_sub) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    for name, array in (("hist_dom", hist_dom), ("hist_sub", hist_sub)):
        temporary = CHECKPOINT_DIR / f"{name}.npy.part"
        with temporary.open("wb") as output:
            np.save(output, array)
        temporary.replace(CHECKPOINT_DIR / f"{name}.npy")
    cursor = {"steps_done": steps_done, "config": _config()}
    temporary = CHECKPOINT_DIR / "cursor.json.part"
    temporary.write_text(json.dumps(cursor), encoding="utf-8")
    temporary.replace(CHECKPOINT_DIR / "cursor.json")


def build(*, workers: int = 24, checkpoint_every: int = 2500) -> None:
    """Stream ERA5 IVT and accumulate per-calendar-month histograms."""
    steps = sample_steps()
    ndom = DOMAIN_LATS.size * DOMAIN_LONS.size
    nsub = SUB_LATS.size * SUB_LONS.size
    state = _load_checkpoint()
    if state is None:
        done = 0
        hist_dom = np.zeros((12, ndom, NBINS), dtype=np.uint16)
        hist_sub = np.zeros((12, nsub, NBINS), dtype=np.uint16)
    else:
        done, hist_dom, hist_sub = state
    if done >= len(steps):
        print(f"climatology histograms already complete ({done} steps)")
        return

    dom_cells = np.arange(ndom)
    sub_cells = np.arange(nsub)
    started = time.time()
    processed_at_start = done
    print(f"accumulating {len(steps) - done} of {len(steps)} steps "
          f"(resume at {steps[done]})")
    for ts, east, north in iter_ivt_steps(steps[done:], workers=workers):
        month = int(str(np.datetime64(ts, "M"))[5:7])
        mag_dom = np.hypot(east[DOMAIN_ROWS, DOMAIN_COLS],
                           north[DOMAIN_ROWS, DOMAIN_COLS])
        mag_sub = np.hypot(east[::SUBSAMPLE, ::SUBSAMPLE],
                           north[::SUBSAMPLE, ::SUBSAMPLE])
        if not (np.isfinite(mag_dom).all() and np.isfinite(mag_sub).all()):
            raise RuntimeError(f"non-finite IVT at {ts}")
        idx = np.clip((mag_dom.ravel() / BIN_WIDTH).astype(np.int32),
                      0, NBINS - 1)
        hist_dom[month - 1][dom_cells, idx] += 1
        idx = np.clip((mag_sub.ravel() / BIN_WIDTH).astype(np.int32),
                      0, NBINS - 1)
        hist_sub[month - 1][sub_cells, idx] += 1
        done += 1
        if done % checkpoint_every == 0 or done == len(steps):
            _save_checkpoint(done, hist_dom, hist_sub)
            rate = (done - processed_at_start) / (time.time() - started)
            remaining = (len(steps) - done) / rate if rate else float("nan")
            print(f"  checkpoint {done}/{len(steps)} steps "
                  f"({rate:.1f} steps/s, ~{remaining / 60:.0f} min left)",
                  flush=True)
    if done % checkpoint_every:
        _save_checkpoint(done, hist_dom, hist_sub)
    print(f"histogram accumulation complete: {done} steps")


def _window_months(month: int) -> list[int]:
    """Five calendar months centered on *month* (target.m lines 396-402)."""
    return [(month - 3 + offset) % 12 + 1 for offset in range(5)]


def _histogram_percentiles(hist: np.ndarray) -> np.ndarray:
    """(12, ncells, NBINS) counts -> (P, 12, ncells) interpolated quantiles."""
    months, ncells, _ = hist.shape
    out = np.empty((len(PERCENTILES), 12, ncells), dtype=np.float32)
    for month in range(1, 13):
        window = np.zeros((ncells, NBINS), dtype=np.uint32)
        for other in _window_months(month):
            window += hist[other - 1]
        cumulative = np.cumsum(window, axis=1)
        totals = cumulative[:, -1].astype(np.float64)
        for pi, percentile in enumerate(PERCENTILES):
            target = totals * (percentile / 100.0)
            first = np.minimum((cumulative < target[:, None]).sum(axis=1),
                               NBINS - 1)
            rows = np.arange(ncells)
            before = np.where(first > 0, cumulative[rows, first - 1], 0)
            counts = window[rows, first]
            with np.errstate(invalid="ignore", divide="ignore"):
                fraction = np.where(counts > 0, (target - before) / counts, 0.0)
            out[pi, month - 1] = ((first + fraction) * BIN_WIDTH
                                  ).astype(np.float32)
    return out


def _weighted_percentile(values, weights, percentile: float) -> float:
    """target.m prctilew: weighted midpoint positions, linear interp."""
    order = np.argsort(values, kind="stable")
    ordered = values[order]
    weight = weights[order]
    positions = (np.cumsum(weight) - 0.5 * weight) / weight.sum() * 100.0
    positions = np.concatenate(([0.0], positions, [100.0]))
    ordered = np.concatenate(([ordered[0]], ordered, [ordered[-1]]))
    positions, unique_idx = np.unique(positions, return_index=True)
    return float(np.interp(percentile, positions, ordered[unique_idx]))


def finalize() -> Path:
    """Compute thresholds + hemispheric floors and write the local file."""
    state = _load_checkpoint()
    if state is None:
        raise FileNotFoundError(f"no climatology checkpoint in {CHECKPOINT_DIR}")
    done, hist_dom, hist_sub = state
    total = len(sample_steps())
    if done < total:
        raise RuntimeError(f"climatology incomplete: {done}/{total} steps")

    dom_pct = _histogram_percentiles(hist_dom).reshape(
        len(PERCENTILES), 12, DOMAIN_LATS.size, DOMAIN_LONS.size)
    sub_pct = _histogram_percentiles(hist_sub).reshape(
        len(PERCENTILES), 12, SUB_LATS.size, SUB_LONS.size)

    sub_area = np.broadcast_to(
        pixel_areas(SUB_LATS, 0.25 * SUBSAMPLE, 0.25 * SUBSAMPLE)[:, None],
        (SUB_LATS.size, SUB_LONS.size))
    floors = np.empty((len(PERCENTILES), 12, 2), dtype=np.float32)  # (P, m, hemi)
    north = SUB_LATS >= 0
    south = SUB_LATS <= 0
    for pi in range(len(PERCENTILES)):
        for month in range(12):
            field = sub_pct[pi, month]
            floors[pi, month, 0] = _weighted_percentile(
                field[north].ravel(), sub_area[north].ravel(),
                GLOBAL_FLOOR_PERCENTILE)
            floors[pi, month, 1] = _weighted_percentile(
                field[south].ravel(), sub_area[south].ravel(),
                GLOBAL_FLOOR_PERCENTILE)

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    meta = dict(_config(), percentiles=list(PERCENTILES),
                global_floor_percentile=GLOBAL_FLOOR_PERCENTILE,
                source="ARCO-ERA5 (gcp-public-data-arco-era5, ar/full_37-1h)",
                method=("Guan & Waliser 2024 tARget v4 thresholds, local "
                        "build; 00Z+12Z sampling; floor from 1.5-deg global "
                        "subsample"))
    temporary = THRESHOLDS_PATH.with_suffix(".npz.part")
    with temporary.open("wb") as output:
        np.savez_compressed(
            output, dom_pct=dom_pct, floors_nh_sh=floors,
            lat=DOMAIN_LATS, lon=DOMAIN_LONS,
            percentiles=np.array(PERCENTILES), meta=json.dumps(meta))
    temporary.replace(THRESHOLDS_PATH)
    print(f"wrote {THRESHOLDS_PATH}")
    return THRESHOLDS_PATH
