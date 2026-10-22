#!/usr/bin/env python3
"""Precompute HUC6 atmospheric-river episode diagnostics for a HUC2 region.

Run this after caching tARget netCDF date ranges with ``download.py`` and computing
basin histories. A basin is active on a UTC day when the raw tARget mask covers
more than ``--min-area-fraction`` of its area. Consecutive active days form one
episode. The output contains only episode metadata plus precomputed statistics;
the API never derives or invents episodes on demand.

Schema v7 (v6 plus provenance-labeled AR coverage; see the SCHEMA_VERSION
note for the non-canonical local tARget extension):
- Per-episode values are ungated point estimates (no bootstrap, no CI keys) —
  null only when the mean skill gain is non-positive or no complete date exists.
- Inference lives at three pooled scopes per basin: ``ar_all`` (every
  AR-episode day), ``non_ar`` (complete scorable days in no episode), and
  ``all_days`` (their union — the 2026-07-18 amendment), each with a
  cluster-bootstrap percentile interval. The resampling unit is a *cluster*:
  episodes of the same basin whose gap is < 72h share one cluster; non-AR days
  form pseudo-clusters split at >= 3-day gaps (the mirror rule) AND at
  epoch-aligned 10-day calendar-block boundaries (the 2026-07-20b amendment —
  long contiguous records must not become single resampling units). Under
  ``all_days`` both kinds coexist unchanged; every date is in exactly one
  cluster.
- A region-pooled block (2026-07-20 amendment) pools (date x basin) pairs
  across all of the region's HUC6 basins per scope; the resampling unit is the
  cross-basin temporal cluster (per-basin clusters merged transitively when
  their date ranges overlap), seeds ``scope:R:CA:{scope}:{digest}`` for
  California and ``scope:R:{huc2}:{scope}:{digest}`` for every other region
  (2026-08-03 amendment item 1 — the PNW replication scope, region 17). Seeds
  stay model-independent everywhere.
- Complete dates are restricted to the model's primary version window (W1) —
  pooled statistics never mix runs across a model-version switch.
- Seeds are model-independent (basin + scope + episode-set digest), so paired
  cross-model comparisons use common random numbers (run_compare_stage).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from itertools import combinations
import hashlib
import json
from pathlib import Path

import numpy as np
from atmospheric_rivers import TARGET_AR_DATA_DIR, discover_target_ar_sources, iter_normalized_frames
from stats_pipeline.episode_metrics import (
    BASIN_HISTORY_FILENAME,
    CALIFORNIA_HUC2,
    DEFAULT_BOOTSTRAP_SAMPLES,
    DEFAULT_CONFIDENCE,
    basin_history_filename,
    episode_lock_in_gap,
    episode_revision_efficiency,
    paired_delta_j,
    paired_delta_r,
    per_date_components,
    scope_lock_in_gap,
    scope_revision_efficiency,
)
from stats_pipeline.basin import basin_weights, load_basins
from statistics_plugins.registry import EPISODE_CATALOG_FILENAME
from stats_pipeline.manifest import digest_files_content
from model_registry import MODEL_REGISTRY


# v6: catalog meta gained the ``ar_coverage`` block — the tARget catalog's
# cached coverage ranges plus the residual span of complete model days past
# the catalog end. Upstream tARget releases lag the model/PRISM record;
# dates past the catalog end carry no AR labeling, so their dependence is
# modeled by the >=3-day-gap pseudo-clusters split at epoch-aligned 10-day
# calendar blocks (disclose this wherever those intervals are shown).
# v7: AR sources may include the locally generated tARget v4 extension
# (atmospheric_rivers.tarv4) past the canonical release's end. Coverage
# ranges carry a ``source`` label, ``ar_coverage`` gains ``canonical_end``
# and ``non_canonical_ranges``, and episode catalogs may now contain
# episodes labeled by the non-canonical extension — captions must disclose
# that segment (it is NOT the canonical UCLA Dataverse catalog).
SCHEMA_VERSION = 7
DEFAULT_MIN_AREA_FRACTION = 0.0
# Episodes of the same basin whose gap (next.start - prev.end) is < 72h share
# one bootstrap cluster (<= 2 calendar days between active days). Displayed
# episodes are unchanged; clustering only affects resampling.
CLUSTER_MERGE_HOURS = 72
# Mirror of the 72h rule for non-AR pseudo-clusters: a break of >= 3 days
# between scope dates starts a new pseudo-cluster.
NON_AR_BREAK_DAYS = CLUSTER_MERGE_HOURS // 24
# Pseudo-clusters additionally split at fixed calendar-block boundaries
# (2026-07-20b amendment): contiguity alone is not a dependence scale — an
# unbroken dry-season record would otherwise form one months-long cluster
# whose date range chains nearly the whole record into a single resampling
# unit under the cross-basin merge. Blocks are epoch-aligned so they coincide
# across basins and the merge cannot re-chain adjacent blocks.
PSEUDO_CLUSTER_BLOCK_DAYS = 10
PSEUDO_CLUSTER_EPOCH = date(2025, 1, 1)
SCOPE_NAMES = ("ar_all", "non_ar", "all_days")
# Container key for the cross-basin pooled scopes (2026-07-20 amendment).
CA_POOLED_KEY = "CA"
# ΔJ reference lead pairs (2026-07-24 amendment §3: 3 primary, 9 secondary).
DELTA_J_REFERENCE_PAIRS = (3, 9)


def region_pooled_key(huc2: str = CALIFORNIA_HUC2) -> str:
    """Seed/row key of a region's cross-basin pooled scope.

    California keeps the frozen ``CA`` key (its seeds and digests must not
    move); every other region keys on its HUC2 code, exactly as the
    2026-08-03 amendment declares for the PNW (``scope:R:17:{scope}:…``).
    """
    return CA_POOLED_KEY if huc2 == CALIFORNIA_HUC2 else huc2


def episode_catalog_filename(huc2: str = CALIFORNIA_HUC2) -> str:
    """Per-region episodes JSON name (CA keeps the historical filename)."""
    if huc2 == CALIFORNIA_HUC2:
        return EPISODE_CATALOG_FILENAME
    return f"episodes_huc{huc2}.json"


def comparison_filename(huc2: str = CALIFORNIA_HUC2) -> str:
    """Per-region cross-model comparison JSON name."""
    if huc2 == CALIFORNIA_HUC2:
        return COMPARISON_FILENAME
    stem, _, ext = COMPARISON_FILENAME.rpartition(".")
    return f"{stem}_huc{huc2}.{ext}"

# Window metadata recorded for histories whose model key has no registry
# entry (synthetic test fixtures): full record, no filtering.
NO_WINDOW_BLOCK = {"windows": [], "primary": None,
                   "primary_valid_range": [None, None]}

COMPARISON_FILENAME = "episode_model_comparison.json"
# v2: all_days scope, CA-pooled rows, matched-lead-span variants, version
# windows applied to each model's complete dates.
# v3: pseudo-clusters split at epoch-aligned calendar blocks (2026-07-20b).
COMPARISON_SCHEMA_VERSION = 3


def _utc_day(text: str) -> date:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date()


def ar_catalog_coverage() -> list[dict] | None:
    """Coverage ranges of every discovered AR source, labeled by provenance.

    The canonical virtual catalog contributes its cached
    ``available_date_ranges`` (``source: "canonical_catalog"``).  When the
    locally generated tARget v4 extension is present its sidecar range is
    appended with ``source: "local_tarv4_extension"`` and
    ``canonical: false`` — downstream captions must disclose that segment
    as non-canonical (see the SCHEMA_VERSION v7 note).
    """
    from atmospheric_rivers import target_ar_extension_meta

    ranges: list[dict] = []
    for source in discover_target_ar_sources():
        if not source.name.endswith(".index.json"):
            continue
        try:
            payload = json.loads(Path(source).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for entry in payload.get("available_date_ranges") or []:
            if entry.get("start") and entry.get("end"):
                ranges.append({"start": str(entry["start"]),
                               "end": str(entry["end"]),
                               "source": "canonical_catalog"})
    extension = target_ar_extension_meta()
    if extension is not None:
        ranges.append({"start": str(extension["start"]),
                       "end": str(extension["end"]),
                       "source": "local_tarv4_extension",
                       "canonical": False})
    return sorted(ranges, key=lambda r: r["start"]) or None


def find_basin_episodes(
    sources: list[Path],
    *,
    min_area_fraction: float = DEFAULT_MIN_AREA_FRACTION,
    variable: str | None = None,
    huc2: str = CALIFORNIA_HUC2,
) -> dict[str, list[dict]]:
    """Find basin episode ranges directly from range-cached tARget mask arrays.

    ``huc2`` selects the region's HUC6 basin set; the AR-state rule, the
    daily area-weighted coverage fraction, the >0 activity test and the
    consecutive-day episode rule are region-independent.
    """
    if not sources:
        raise FileNotFoundError(
            f"no cached tARget date ranges under {TARGET_AR_DATA_DIR}; "
            "run download.py --ar with --start-date/--end-date first"
        )
    basins = load_basins(huc2)
    daily: dict[str, dict[date, float]] = {
        huc: defaultdict(float) for huc, _, _ in basins
    }
    scanned = 0
    weights = None
    grid_signature = None
    for frame in iter_normalized_frames(sources, variable=variable):
        signature = (frame.lats.tobytes(), frame.lons.tobytes())
        if signature != grid_signature:
            weights, _ = basin_weights(frame.lats, frame.lons, basins)
            grid_signature = signature
        fractions = weights @ frame.mask.reshape(-1).astype(np.float64)
        day = _utc_day(frame.timestamp)
        for bi, (huc, _, _) in enumerate(basins):
            fraction = float(fractions[bi])
            if fraction > min_area_fraction:
                daily[huc][day] = max(daily[huc][day], fraction)
        scanned += 1
        if scanned % 500 == 0:
            print(f"  scanned {scanned} raw tARget frames")

    episodes: dict[str, list[dict]] = {}
    for huc, name, _ in basins:
        days = sorted(daily[huc])
        groups: list[list[date]] = []
        for day in days:
            if not groups or day > groups[-1][-1] + timedelta(days=1):
                groups.append([day])
            else:
                groups[-1].append(day)
        episodes[huc] = [
            {
                "id": f"{huc}-{group[0]:%Y%m%d}-{group[-1]:%Y%m%d}",
                "name": name,
                "start_date": group[0].isoformat(),
                "end_date": group[-1].isoformat(),
                "active_days": len(group),
                "peak_basin_area_fraction": max(daily[huc][day] for day in group),
            }
            for group in groups
        ]
    return episodes


# ---------------------------------------------------------------------------
# Clusters, scopes and seeds (model-independent by construction)
# ---------------------------------------------------------------------------

def _parse_day(text: str) -> date:
    return date.fromisoformat(text)


def _episode_days(episode: dict) -> list[date]:
    start = _parse_day(episode["start_date"])
    end = _parse_day(episode["end_date"])
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def assign_clusters(episodes: list[dict]) -> list[list[dict]]:
    """Group a basin's catalog episodes into bootstrap clusters (72h rule).

    Episodes are sorted by start date; an episode joins the previous cluster
    when its gap to the cluster's latest end (next.start - prev.end) is
    < CLUSTER_MERGE_HOURS, i.e. <= 2 calendar days between active days.
    """
    ordered = sorted(episodes, key=lambda e: (e["start_date"], e["end_date"]))
    clusters: list[list[dict]] = []
    cluster_end: date | None = None
    for episode in ordered:
        start = _parse_day(episode["start_date"])
        end = _parse_day(episode["end_date"])
        if cluster_end is not None and (start - cluster_end).days * 24 < CLUSTER_MERGE_HOURS:
            clusters[-1].append(episode)
            cluster_end = max(cluster_end, end)
        else:
            clusters.append([episode])
            cluster_end = end
    return clusters


def cluster_ids(huc: str, episodes: list[dict]) -> dict[str, str]:
    """Map episode id -> "{huc}-c{NN}" (01-based, clusters ordered by start)."""
    mapping: dict[str, str] = {}
    for number, cluster in enumerate(assign_clusters(episodes), start=1):
        for episode in cluster:
            mapping[episode["id"]] = f"{huc}-c{number:02d}"
    return mapping


def group_pseudo_clusters(days: list[date]) -> list[list[date]]:
    """Group non-AR scope days into pseudo-clusters.

    A >= 3-day break starts a new pseudo-cluster (mirror of the 72 h episode
    rule), and so does crossing an epoch-aligned calendar-block boundary
    (2026-07-20b amendment) — long contiguous records are chopped into
    PSEUDO_CLUSTER_BLOCK_DAYS-day resampling units instead of one
    season-long cluster.
    """
    def block(day: date) -> int:
        return (day - PSEUDO_CLUSTER_EPOCH).days // PSEUDO_CLUSTER_BLOCK_DAYS

    clusters: list[list[date]] = []
    for day in sorted(days):
        if clusters and (day - clusters[-1][-1]).days < NON_AR_BREAK_DAYS \
                and block(day) == block(clusters[-1][-1]):
            clusters[-1].append(day)
        else:
            clusters.append([day])
    return clusters


def build_scope_clusters(
    catalog_episodes: list[dict], complete_days: list[date],
) -> dict[str, list[list[date]]]:
    """Cluster date sets for the three scopes of one basin.

    ``ar_all``: per episode cluster, the union of member episodes' active days
    intersected with the model's complete dates; empty clusters are dropped
    (episodes with zero overlapping dates are skipped, as before).
    ``non_ar``: complete dates in no episode, grouped into pseudo-clusters.
    ``all_days``: their union — episode clusters and non-AR pseudo-clusters
    coexist unchanged (2026-07-18 amendment); every date in exactly one
    cluster.
    """
    complete = set(complete_days)
    ar_clusters: list[list[date]] = []
    for cluster in assign_clusters(catalog_episodes):
        days = sorted({day for episode in cluster for day in _episode_days(episode)} & complete)
        if days:
            ar_clusters.append(days)
    episode_days = {day for episode in catalog_episodes for day in _episode_days(episode)}
    non_ar_days = [day for day in sorted(complete) if day not in episode_days]
    non_ar_clusters = group_pseudo_clusters(non_ar_days)
    return {
        "ar_all": ar_clusters,
        "non_ar": non_ar_clusters,
        "all_days": ar_clusters + non_ar_clusters,
    }


def merge_cross_basin_clusters(
    clusters_by_huc: dict[str, list[list[date]]],
) -> list[list[tuple[str, date]]]:
    """Cross-basin temporal clusters for the CA-pooled scope (2026-07-20).

    Per-basin clusters are merged transitively whenever their [first, last]
    date ranges overlap (share at least one calendar date), so simultaneous
    storms in multiple basins resample together. Each merged cluster holds
    (huc, date) pair units, ordered by basin then date.
    """
    spans: list[tuple[date, date, list[tuple[str, date]]]] = []
    for huc in sorted(clusters_by_huc):
        for days in clusters_by_huc[huc]:
            spans.append((days[0], days[-1], [(huc, day) for day in days]))
    spans.sort(key=lambda item: (item[0], item[1]))
    merged: list[tuple[date, date, list[tuple[str, date]]]] = []
    for start, end, pairs in spans:
        if merged and start <= merged[-1][1]:
            prev_start, prev_end, prev_pairs = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end), prev_pairs + pairs)
        else:
            merged.append((start, end, pairs))
    return [sorted(pairs) for _, _, pairs in merged]


def episode_set_digest(clusters: list[list[date]]) -> str:
    """12-hex digest of the sorted cluster date-ranges of one basin+scope.

    This is the catalog-derived seed component: the same basin/scope/cluster
    structure digests identically regardless of model, keeping bootstrap
    draws common across models.
    """
    ranges = sorted(f"{days[0].isoformat()}:{days[-1].isoformat()}" for days in clusters)
    return hashlib.sha256("|".join(ranges).encode("utf-8")).hexdigest()[:12]


def _flatten_clusters(
    clusters_days: list[list[date]], index_by_day: dict[date, int],
) -> tuple[np.ndarray, list[np.ndarray]]:
    """Rows of the full per-date arrays for a scope, plus cluster index lists.

    Returns (positions, clusters): ``positions`` selects the scope's dates
    from the basin's full history arrays; ``clusters`` holds index lists into
    that flattened selection (the shape the bootstrap machinery expects).
    """
    positions: list[int] = []
    clusters: list[np.ndarray] = []
    offset = 0
    for days in clusters_days:
        positions.extend(index_by_day[day] for day in days)
        clusters.append(np.arange(offset, offset + len(days), dtype=np.intp))
        offset += len(days)
    return np.asarray(positions, dtype=np.intp), clusters


def _comparison(values: list[float], value: float) -> dict:
    arr = np.asarray(values, dtype=np.float64)
    return {
        "episode_count": int(arr.size),
        "percentile": float(100.0 * np.mean(arr <= value)),
        "median": float(np.median(arr)),
        "iqr": [float(x) for x in np.quantile(arr, [0.25, 0.75])],
    }


def _decorate_comparisons(episodes: list[dict], key: str) -> None:
    values = [episode[key]["value"] for episode in episodes if episode[key]["value"] is not None]
    for episode in episodes:
        value = episode[key]["value"]
        episode[key]["comparison"] = _comparison(values, value) if value is not None else None


def _ca_flatten(
    merged: list[list[tuple[str, date]]], by_huc: dict[str, dict], value_key: str,
) -> tuple[np.ndarray, list[np.ndarray]]:
    """Flatten (huc, date) pair values in cluster order + cluster index lists."""
    values: list[float] = []
    clusters: list[np.ndarray] = []
    offset = 0
    for pairs in merged:
        for huc, day in pairs:
            row = by_huc[huc]["index_by_day"][day]
            values.append(float(by_huc[huc][value_key][row]))
        clusters.append(np.arange(offset, offset + len(pairs), dtype=np.intp))
        offset += len(pairs)
    return np.asarray(values, dtype=np.float64), clusters


def _region_pooled(by_huc: dict[str, dict],
                   pooled_key: str = CA_POOLED_KEY) -> dict:
    """Cross-basin pooled scopes over (date x basin) pairs (2026-07-20).

    Per-basin scope clusters are built under the unchanged per-basin rules,
    then merged transitively across basins wherever their date ranges overlap
    (merge_cross_basin_clusters). Estimand shape and all inference rules match
    the per-basin scopes; only the unit (pair) and the resampling cluster
    (cross-basin) differ. Seeds are model-independent by the same argument as
    the per-basin scopes; ``pooled_key`` is the region component of the seed
    (``CA`` for California, the HUC2 code elsewhere).
    """
    scope_clusters_by_huc: dict[str, dict[str, list[list[date]]]] = {
        huc: build_scope_clusters(entry["catalog"], entry["complete_days"])
        for huc, entry in by_huc.items()
    }
    scopes = {}
    for scope_name in SCOPE_NAMES:
        merged = merge_cross_basin_clusters({
            huc: clusters[scope_name]
            for huc, clusters in scope_clusters_by_huc.items()
        })
        if not merged:
            continue
        digest = episode_set_digest(
            [sorted({day for _, day in pairs}) for pairs in merged])
        revision, clusters = _ca_flatten(merged, by_huc, "revision")
        skill, _ = _ca_flatten(merged, by_huc, "skill")
        gaps, _ = _ca_flatten(merged, by_huc, "gaps")
        scope_days = sorted({day for pairs in merged for _, day in pairs})
        scopes[scope_name] = {
            "pairs": int(revision.size),
            "dates": len(scope_days),
            "clusters": len(merged),
            "date_range": [scope_days[0].isoformat(), scope_days[-1].isoformat()],
            "revision_efficiency": scope_revision_efficiency(
                revision, skill, clusters,
                seed_key=f"scope:R:{pooled_key}:{scope_name}:{digest}"),
            "lock_in_gap": scope_lock_in_gap(
                gaps, clusters,
                seed_key=f"scope:gap:{pooled_key}:{scope_name}:{digest}"),
        }
    block = {
        "unit": "(date x basin) pair",
        "resampling_unit": "cross_basin_temporal_cluster",
        "hucs": sorted(by_huc),
        "scopes": scopes,
    }
    if pooled_key != CA_POOLED_KEY:
        # CA's block shape is frozen (exports bind to it verbatim);
        # other regions carry their seed key explicitly.
        block["pooled_key"] = pooled_key
    return block


def _california_pooled(by_huc: dict[str, dict]) -> dict:
    """Backwards-compatible alias for the California-pooled block."""
    return _region_pooled(by_huc, CA_POOLED_KEY)


def _write_json_atomic(out: Path, payload: dict) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    temporary = out.with_name(out.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(out)


def run_episode_stage(
    model: str,
    episode_catalog: dict[str, list[dict]],
    *,
    min_area_fraction: float = DEFAULT_MIN_AREA_FRACTION,
    force: bool = False,
    huc2: str = CALIFORNIA_HUC2,
) -> Path:
    pooled_key = region_pooled_key(huc2)
    src = Path("stats_output") / model / basin_history_filename(huc2)
    if not src.is_file():
        raise FileNotFoundError(f"missing {src}; run compute_stats.py --only basin first")
    out = Path("stats_output") / model / episode_catalog_filename(huc2)
    config = MODEL_REGISTRY.get(model)
    window_block = config.window_block() if config else dict(NO_WINDOW_BLOCK)
    basin_history_digest = digest_files_content([src])
    catalog_digest = hashlib.sha256(json.dumps(
        episode_catalog, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    if out.is_file() and not force:
        try:
            old = json.loads(out.read_text(encoding="utf-8"))
            if (old.get("schema_version") == SCHEMA_VERSION
                    and old.get("episode_catalog_digest") == catalog_digest
                    and old.get("basin_history_digest") == basin_history_digest
                    and old.get("version_window") == window_block
                    and old.get("minimum_basin_area_fraction") == float(min_area_fraction)):
                print(f"  {model}: episode statistics are up to date")
                return out
        except (OSError, ValueError):
            pass

    with np.load(src) as raw:
        data = {name: np.array(raw[name]) for name in raw.files}
    hucs = data["hucs"].astype(str)
    dates = data["dates"].astype(np.int64)
    lead_min = int(data["lead_min"])
    lead_max = int(data["lead_max"])
    if data["forecasts"].shape[1] != lead_max - lead_min + 1:
        raise ValueError(f"{src}: forecast columns do not match the recorded lead band")
    day_objects = [datetime.strptime(str(key), "%Y%m%d").date() for key in dates]
    # Pooled statistics only see version-clean dates (W1); the basin history
    # itself stays full-record.
    if config is not None:
        in_window = np.array(
            [config.valid_date_in_primary_window(day) for day in day_objects],
            dtype=bool)
    else:
        in_window = np.ones(len(day_objects), dtype=bool)
    basins = {}
    # Per-basin per-date components retained for the CA-pooled block.
    by_huc: dict[str, dict] = {}
    for bi, huc in enumerate(hucs):
        catalog_episodes = episode_catalog.get(huc, [])
        complete, revision_all, skill_all = per_date_components(
            data["forecasts"][:, :, bi], data["observed"][:, bi])
        complete &= in_window
        gaps_all = data["lock_gap"][:, bi].astype(np.float64)
        complete_rows = np.flatnonzero(complete)
        complete_days = [day_objects[i] for i in complete_rows]
        index_by_day = {day_objects[i]: int(i) for i in complete_rows}
        episode_cluster_ids = cluster_ids(huc, catalog_episodes)

        # Per-episode ungated point estimates over the episode's complete dates.
        model_episodes = []
        for episode in catalog_episodes:
            start = int(episode["start_date"].replace("-", ""))
            end = int(episode["end_date"].replace("-", ""))
            selected = (dates >= start) & (dates <= end)
            if int(selected.sum()) == 0:
                continue
            in_episode = selected & complete
            model_episodes.append({
                **episode,
                "cluster_id": episode_cluster_ids[episode["id"]],
                "forecast_dates": int(selected.sum()),
                "revision_efficiency": episode_revision_efficiency(
                    revision_all[in_episode], skill_all[in_episode]),
                "lock_in_gap": episode_lock_in_gap(gaps_all[in_episode]),
            })
        _decorate_comparisons(model_episodes, "revision_efficiency")
        _decorate_comparisons(model_episodes, "lock_in_gap")

        # Pooled scopes with cluster-bootstrap inference.
        scopes = {}
        for scope_name, clusters_days in build_scope_clusters(
                catalog_episodes, complete_days).items():
            positions, clusters = _flatten_clusters(clusters_days, index_by_day)
            digest = episode_set_digest(clusters_days)
            scope_days = [day for days in clusters_days for day in days]
            scopes[scope_name] = {
                "dates": int(positions.size),
                "clusters": len(clusters_days),
                "date_range": (
                    [min(scope_days).isoformat(), max(scope_days).isoformat()]
                    if scope_days else [None, None]),
                "revision_efficiency": scope_revision_efficiency(
                    revision_all[positions], skill_all[positions], clusters,
                    seed_key=f"scope:R:{huc}:{scope_name}:{digest}"),
                "lock_in_gap": scope_lock_in_gap(
                    gaps_all[positions], clusters,
                    seed_key=f"scope:gap:{huc}:{scope_name}:{digest}"),
            }

        basins[huc] = {
            "name": str(data["names"][bi]),
            "episode_count": len(model_episodes),
            "episodes": model_episodes,
            "scopes": scopes,
        }
        by_huc[huc] = {
            "catalog": catalog_episodes,
            "complete_days": complete_days,
            "index_by_day": index_by_day,
            "revision": revision_all,
            "skill": skill_all,
            "gaps": gaps_all,
        }

    # Coverage disclosure (v6): complete model days past the tARget catalog
    # end have no AR labeling — they resample as generic pseudo-clusters, so
    # any interval over them understates storm clustering. Record the
    # residual span so downstream captions can disclose it.
    coverage = ar_catalog_coverage()
    coverage_end = max(_parse_day(r["end"]) for r in coverage) if coverage else None
    all_complete_days = sorted(
        {day for entry in by_huc.values() for day in entry["complete_days"]})
    uncovered = ([day for day in all_complete_days if day > coverage_end]
                 if coverage_end is not None else [])
    non_canonical = [r for r in (coverage or [])
                     if r.get("source") == "local_tarv4_extension"]
    note = ("dates after catalog_end carry no AR labeling; their "
            "dependence is modeled by >=3-day-gap pseudo-clusters "
            "split at epoch-aligned 10-day calendar blocks")
    if non_canonical:
        note += ("; AR labeling within the local_tarv4_extension range(s) "
                 "is locally generated with the published tARget v4 "
                 "algorithm on ERA5/ERA5T IVT — NOT the canonical UCLA "
                 "Dataverse release — and must be disclosed as such")
    ar_coverage_block = {
        "catalog_ranges": coverage,
        "catalog_end": coverage_end.isoformat() if coverage_end else None,
        "canonical_end": (max((_parse_day(r["end"]) for r in (coverage or [])
                               if r.get("source") == "canonical_catalog"),
                              default=None)),
        "non_canonical_ranges": non_canonical or None,
        "uncovered_span": ([uncovered[0].isoformat(), uncovered[-1].isoformat()]
                           if uncovered else None),
        "uncovered_days": len(uncovered),
        "note": note,
    }
    if ar_coverage_block["canonical_end"] is not None:
        ar_coverage_block["canonical_end"] = (
            ar_coverage_block["canonical_end"].isoformat())

    payload = {
        "schema_version": SCHEMA_VERSION,
        "episode_catalog_digest": catalog_digest,
        "basin_history_digest": basin_history_digest,
        "minimum_basin_area_fraction": float(min_area_fraction),
        "ar_coverage": ar_coverage_block,
        "model": model,
        "huc2": str(data["huc2"]),
        "huc_level": 6,
        "lead_min": lead_min,
        "lead_max": lead_max,
        "lock_in_fraction": float(data["lock_in_fraction"]),
        "version_window": window_block,
        "analysis": {
            "bootstrap_samples": DEFAULT_BOOTSTRAP_SAMPLES,
            "confidence": DEFAULT_CONFIDENCE,
            "cluster_merge_hours": CLUSTER_MERGE_HOURS,
            "pseudo_cluster_block_days": PSEUDO_CLUSTER_BLOCK_DAYS,
            "pseudo_cluster_epoch": PSEUDO_CLUSTER_EPOCH.isoformat(),
            "resampling_unit": "episode_cluster",
            "weighting_primary": "date",
            "weighting_robustness": "episode",
            "seed_policy": "basin+episode-set+statistic (model-independent)",
            "scopes": list(SCOPE_NAMES),
        },
        "basins": basins,
    }
    # Cross-basin pooled scopes (2026-07-20 amendment): (date x basin) pair
    # units, cross-basin temporal clusters as the resampling unit. California
    # keeps the frozen container name; other regions use "region_pooled".
    pooled_block = _region_pooled(by_huc, pooled_key)
    if huc2 == CALIFORNIA_HUC2:
        payload["california_pooled"] = pooled_block
    else:
        payload["region_pooled"] = pooled_block
    _write_json_atomic(out, payload)
    print(f"  {model}: wrote {out}")
    return out


# ---------------------------------------------------------------------------
# Cross-model paired ΔR comparison
# ---------------------------------------------------------------------------

def _load_episode_payload(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if payload.get("schema_version") == SCHEMA_VERSION else None


class _CompareHistory:
    """One model's basin history plus per-lead-band component memoization."""

    def __init__(self, model: str, stats_root: Path,
                 huc2: str = CALIFORNIA_HUC2):
        self.model = model
        config = MODEL_REGISTRY.get(model)
        with np.load(stats_root / model / basin_history_filename(huc2)) as raw:
            self.hucs = [str(h) for h in raw["hucs"].astype(str)]
            dates = raw["dates"].astype(np.int64)
            self.forecasts = np.array(raw["forecasts"])
            self.observed = np.array(raw["observed"])
            self.lead_min = int(raw["lead_min"])
            self.lead_max = int(raw["lead_max"])
        self.days = [datetime.strptime(str(key), "%Y%m%d").date() for key in dates]
        if config is not None:
            self.in_window = np.array(
                [config.valid_date_in_primary_window(day) for day in self.days],
                dtype=bool)
        else:
            self.in_window = np.ones(len(self.days), dtype=bool)
        self.window_block = config.window_block() if config else dict(NO_WINDOW_BLOCK)
        self._memo: dict[tuple[int, int], dict] = {}

    def components(self, band: tuple[int, int]) -> dict[str, tuple[dict, np.ndarray, np.ndarray]]:
        """Per-huc (index_by_day, revision, skill) for one lead band.

        Truncating to a matched band happens *before* per_date_components, so
        completeness (and hence the intersected date set) is band-specific by
        design.
        """
        if band not in self._memo:
            lo, hi = band
            cols = slice(lo - self.lead_min, hi - self.lead_min + 1)
            per_huc = {}
            for bi, huc in enumerate(self.hucs):
                complete, revision, skill = per_date_components(
                    self.forecasts[:, cols, bi], self.observed[:, bi])
                complete &= self.in_window
                index_by_day = {self.days[i]: int(i)
                                for i in np.flatnonzero(complete)}
                per_huc[huc] = (index_by_day, revision, skill)
            self._memo[band] = per_huc
        return self._memo[band]


def _pair_rows(
    model_a: str, model_b: str,
    comp_a: dict, comp_b: dict,
    episode_metadata: dict[str, dict],
    lead_span: dict,
    pooled_key: str = CA_POOLED_KEY,
) -> list[dict]:
    """All ΔR rows (per-basin + region-pooled) for one model pair and band."""
    rows: list[dict] = []
    shared_hucs = sorted(set(comp_a) & set(comp_b))
    # Per-huc intersected days and union-catalog scope clusters, kept for the
    # cross-basin merge below.
    scope_clusters_by_huc: dict[str, dict[str, list[list[date]]]] = {}
    for huc in shared_hucs:
        index_a, revision_a, skill_a = comp_a[huc]
        index_b, revision_b, skill_b = comp_b[huc]
        intersected = sorted(set(index_a) & set(index_b))
        if not intersected:
            continue
        # The episode catalog is model-independent; the union of both models'
        # recorded episodes recovers every window that can hold an intersected
        # date (episodes absent from both lists overlap neither model's
        # history, so they cannot contribute dates).
        union_episodes = {
            episode["id"]: episode
            for model in (model_a, model_b)
            for episode in episode_metadata[model]["basins"].get(huc, {}).get("episodes", [])
        }
        scope_clusters = build_scope_clusters(list(union_episodes.values()), intersected)
        scope_clusters_by_huc[huc] = scope_clusters
        for scope_name, clusters_days in scope_clusters.items():
            if not clusters_days:
                continue
            digest = episode_set_digest(clusters_days)
            positions_a, clusters = _flatten_clusters(clusters_days, index_a)
            positions_b, _ = _flatten_clusters(clusters_days, index_b)
            result = paired_delta_r(
                revision_a[positions_a], skill_a[positions_a],
                revision_b[positions_b], skill_b[positions_b],
                clusters,
                seed_key=f"delta:R:{huc}:{scope_name}:{digest}",
            )
            rows.append({
                "model_a": model_a,
                "model_b": model_b,
                "huc": huc,
                "scope": scope_name,
                "lead_span": lead_span,
                **result,
            })

    # Region-pooled ΔR (2026-07-20 amendment): (date x basin) pairs over the
    # intersected sets, cross-basin temporal clusters, shared seed.
    for scope_name in SCOPE_NAMES:
        merged = merge_cross_basin_clusters({
            huc: clusters[scope_name]
            for huc, clusters in scope_clusters_by_huc.items()
        })
        if not merged:
            continue
        digest = episode_set_digest(
            [sorted({day for _, day in pairs}) for pairs in merged])

        def _values(comp: dict) -> tuple[np.ndarray, np.ndarray, list[np.ndarray]]:
            revision: list[float] = []
            skill: list[float] = []
            clusters: list[np.ndarray] = []
            offset = 0
            for cluster_pairs in merged:
                for huc, day in cluster_pairs:
                    index_by_day, rev_arr, skl_arr = comp[huc]
                    row = index_by_day[day]
                    revision.append(float(rev_arr[row]))
                    skill.append(float(skl_arr[row]))
                clusters.append(np.arange(offset, offset + len(cluster_pairs),
                                          dtype=np.intp))
                offset += len(cluster_pairs)
            return (np.asarray(revision, dtype=np.float64),
                    np.asarray(skill, dtype=np.float64), clusters)

        revision_a, skill_a, clusters = _values(comp_a)
        revision_b, skill_b, _ = _values(comp_b)
        result = paired_delta_r(
            revision_a, skill_a, revision_b, skill_b, clusters,
            seed_key=f"delta:R:{pooled_key}:{scope_name}:{digest}",
        )
        rows.append({
            "model_a": model_a,
            "model_b": model_b,
            "huc": pooled_key,
            "scope": scope_name,
            "lead_span": lead_span,
            **result,
        })
    return rows


def _region_date_clusters(
    catalog: dict[str, list[dict]], days: list[date],
) -> list[list[date]]:
    """Region-level date clusters for the ΔJ bootstrap (2026-07-24 §3 shape).

    AR side: per-basin episodes grouped by the 72 h rule, clusters merged
    transitively across basins when their date ranges overlap (the pooled
    rule), projected onto unique dates ∩ ``days``. Non-AR side: the remaining
    dates through ``group_pseudo_clusters`` (≥3-day breaks + 10-day calendar
    blocks). Kept here
    so the region-scoped compare stage can reuse it.
    """
    spans: list[tuple[date, date, set]] = []
    for huc in sorted(catalog):
        for cluster in assign_clusters(catalog[huc]):
            cluster_days = sorted({d for ep in cluster for d in _episode_days(ep)})
            spans.append((cluster_days[0], cluster_days[-1], set(cluster_days)))
    spans.sort(key=lambda item: (item[0], item[1]))
    merged: list[tuple[date, date, set]] = []
    for start, end, dayset in spans:
        if merged and start <= merged[-1][1]:
            prev_start, prev_end, prev_days = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end), prev_days | dayset)
        else:
            merged.append((start, end, dayset))

    wanted = set(days)
    ar_clusters: list[list[date]] = []
    ar_days: set = set()
    for _, _, dayset in merged:
        hit = sorted(dayset & wanted)
        if hit:
            ar_clusters.append(hit)
            ar_days.update(hit)
    non_ar = group_pseudo_clusters([d for d in sorted(wanted) if d not in ar_days])
    return ar_clusters + non_ar


def _delta_j_rows(
    models: list[str],
    episode_metadata: dict[str, dict],
    catalog: dict[str, list[dict]],
    pooled_key: str,
    *,
    stats_root: Path,
    daily_name: str = "consistency_daily.npz",
    reference_pairs: tuple[int, ...] = DELTA_J_REFERENCE_PAIRS,
) -> list[dict]:
    """Paired ΔJ at the reference lead pairs, region cluster structure.

    IMPORTANT SCOPE NOTE (data-retention limit):
    the per-date jumpiness accumulators retained by the pipeline are pooled
    over **CONUS land**, not per region. The J statistic in these rows is
    therefore the CONUS-pooled J; what the region parameter changes is the
    *resampling* structure (the region's AR clusters and pseudo-clusters) and
    the intersected date population. Rows carry ``j_domain: "CONUS land"`` so
    no consumer can read them as region-resolved jumpiness.
    """
    from statistics_plugins.consistency import DEFAULT_SCALE_KM, DEFAULT_THRESHOLD_MM

    daily: dict[str, dict] = {}
    for model in models:
        path = stats_root / model / daily_name
        if not path.is_file():
            continue
        with np.load(path) as raw:
            data = {name: np.array(raw[name]) for name in raw.files}
        if not {"jump_num", "jump_den"} <= set(data):
            continue
        config = MODEL_REGISTRY.get(model)
        days = [datetime.strptime(str(int(k)), "%Y%m%d").date()
                for k in data["dates"]]
        keep = [config is None or config.valid_date_in_primary_window(d)
                for d in days]
        daily[model] = {
            "index": {d: i for i, (d, ok) in enumerate(zip(days, keep)) if ok},
            "num": data["jump_num"],
            "den": data["jump_den"],
            "lead_min": int(data["lead_min"]) if "lead_min" in data else 1,
            "qi": [float(q) for q in data["thresholds_mm"]].index(
                float(DEFAULT_THRESHOLD_MM)),
            "si": [int(s) for s in data["scales_km"]].index(int(DEFAULT_SCALE_KM)),
        }

    rows: list[dict] = []
    for model_a, model_b in combinations([m for m in models if m in daily], 2):
        da, db = daily[model_a], daily[model_b]
        for ref in reference_pairs:
            pa, pb = ref - da["lead_min"], ref - db["lead_min"]
            if not (0 <= pa < da["num"].shape[1] and 0 <= pb < db["num"].shape[1]):
                continue
            num_a = da["num"][:, pa, da["qi"], da["si"]]
            den_a = da["den"][:, pa, da["qi"], da["si"]]
            num_b = db["num"][:, pb, db["qi"], db["si"]]
            den_b = db["den"][:, pb, db["qi"], db["si"]]
            days = sorted(
                d for d in set(da["index"]) & set(db["index"])
                if np.isfinite(num_a[da["index"][d]])
                and np.isfinite(den_a[da["index"][d]])
                and np.isfinite(num_b[db["index"][d]])
                and np.isfinite(den_b[db["index"][d]]))
            if not days:
                continue
            clusters_days = _region_date_clusters(catalog, days)
            digest = episode_set_digest(clusters_days)
            flat = [d for cluster in clusters_days for d in cluster]
            clusters, offset = [], 0
            for cluster in clusters_days:
                clusters.append(np.arange(offset, offset + len(cluster),
                                          dtype=np.intp))
                offset += len(cluster)
            pos_a = np.asarray([da["index"][d] for d in flat], dtype=np.intp)
            pos_b = np.asarray([db["index"][d] for d in flat], dtype=np.intp)
            seed_key = f"delta:J:{pooled_key}:all_days:{digest}"
            rows.append({
                "model_a": model_a,
                "model_b": model_b,
                "huc": pooled_key,
                "scope": "all_days",
                "reference_pair_lead": ref,
                "threshold_mm": float(DEFAULT_THRESHOLD_MM),
                "scale_km": int(DEFAULT_SCALE_KM),
                "j_domain": "CONUS land (per-date J accumulators are CONUS-pooled)",
                "resampling_domain": f"HUC2 {pooled_key} clusters",
                "seed_key": seed_key,
                "date_range": [flat and min(flat).isoformat(),
                               flat and max(flat).isoformat()],
                **paired_delta_j(num_a[pos_a], den_a[pos_a],
                                 num_b[pos_b], den_b[pos_b],
                                 clusters, seed_key=seed_key),
            })
    return rows


def run_compare_stage(
    models: list[str],
    *,
    stats_root: Path = Path("stats_output"),
    huc2: str = CALIFORNIA_HUC2,
    include_delta_j: bool = False,
) -> Path | None:
    """Paired ΔR for every unordered model pair with current episode statistics.

    For each shared basin the two models' complete-date sets are intersected
    first; the cluster structure (and hence the seed digest) is built on the
    intersected set, so both models see identical cluster draws — ΔR
    replicates share the weather sample and difference it out. Every pair is
    emitted in two variants: the models' native lead bands
    (``lead_span.matched == false``) and the common band
    ``[max(lead_min), min(lead_max)]`` with both forecast matrices truncated
    before computing components (``matched == true`` — the variant cross-model
    orderings are stated on). Returns None (skip-gracefully) when fewer than
    two models have the required inputs. Cheap enough to recompute every run;
    no manifest gating.

    ``huc2`` selects the region (default California, whose artifact name and
    ``CA`` pooled key are frozen). ``include_delta_j`` additionally emits the
    paired ΔJ rows at the declared reference lead pairs — off by default
    so the California artifact keeps its exact historical shape.
    """
    pooled_key = region_pooled_key(huc2)
    catalog_name = episode_catalog_filename(huc2)
    history_name = basin_history_filename(huc2)
    ready: list[str] = []
    episode_metadata: dict[str, dict] = {}
    for model in models:
        payload = _load_episode_payload(stats_root / model / catalog_name)
        if payload is None or not (stats_root / model / history_name).is_file():
            continue
        ready.append(model)
        episode_metadata[model] = payload
    if len(ready) < 2:
        return None

    histories = {model: _CompareHistory(model, stats_root, huc2)
                 for model in ready}

    pairs: list[dict] = []
    for model_a, model_b in combinations(ready, 2):
        hist_a, hist_b = histories[model_a], histories[model_b]
        native_a = (hist_a.lead_min, hist_a.lead_max)
        native_b = (hist_b.lead_min, hist_b.lead_max)
        pairs.extend(_pair_rows(
            model_a, model_b,
            hist_a.components(native_a), hist_b.components(native_b),
            episode_metadata,
            {"matched": False, "model_a": list(native_a), "model_b": list(native_b)},
            pooled_key,
        ))
        matched = (max(hist_a.lead_min, hist_b.lead_min),
                   min(hist_a.lead_max, hist_b.lead_max))
        if matched[0] <= matched[1]:
            pairs.extend(_pair_rows(
                model_a, model_b,
                hist_a.components(matched), hist_b.components(matched),
                episode_metadata,
                {"matched": True, "lead_min": matched[0], "lead_max": matched[1]},
                pooled_key,
            ))

    payload = {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "scopes": list(SCOPE_NAMES),
        "windows": {model: histories[model].window_block for model in ready},
        "pairs": pairs,
    }
    if huc2 != CALIFORNIA_HUC2:
        payload["huc2"] = huc2
        payload["pooled_key"] = pooled_key
    if include_delta_j:
        # The AR catalog is model-independent (truth-derived): take it from
        # the first ready model's episodes payload.
        catalog = {huc: block["episodes"]
                   for huc, block in episode_metadata[ready[0]]["basins"].items()}
        payload["delta_j_reference_pairs"] = list(DELTA_J_REFERENCE_PAIRS)
        payload["delta_j_pairs"] = _delta_j_rows(
            ready, episode_metadata, catalog, pooled_key, stats_root=stats_root)
    out = stats_root / comparison_filename(huc2)
    _write_json_atomic(out, payload)
    print(f"  wrote {out} ({len(pairs)} pair rows over {len(ready)} models)")
    return out
