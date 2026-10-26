#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import numpy as np

from backend import admin_boundaries, military_boundaries, wbd_boundaries
from backend.static_store import LocalStaticStore, StaticStore, default_static_site_root
from model_registry import MODEL_REGISTRY
from forecast_variables import FORECAST_VARIABLES
from statistics_plugins.consistency import (
    DEFAULT_SCALE_KM,
    DEFAULT_THRESHOLD_MM,
    combo_tag,
)
from statistics_plugins.registry import STATISTICS_BY_NAME
from statistics_plugins.registry import EPISODE_CATALOG_FILENAME

# ---------------------------------------------------------------------------
# In-process caches — keyed so that S3 round-trips are skipped entirely on
# cache hits.  Dict reads/writes for simple keys are atomic under CPython's
# GIL, which is sufficient for the ThreadPoolExecutor concurrency used below.
# ---------------------------------------------------------------------------


class _BoundedLRU:
    """Thread-safe bounded LRU (OrderedDict + Lock). Values must be non-None."""

    def __init__(self, maxsize: int):
        self._max = maxsize
        self._lock = Lock()
        self._d: "OrderedDict" = OrderedDict()
        self._pending: dict = {}  # key -> Lock gating an in-flight compute

    def get(self, key):
        with self._lock:
            value = self._d.get(key)
            if value is not None:
                self._d.move_to_end(key)
            return value

    def put(self, key, value) -> None:
        with self._lock:
            self._d[key] = value
            self._d.move_to_end(key)
            while len(self._d) > self._max:
                self._d.popitem(last=False)  # evict least-recently used

    def get_or_compute(self, key, compute):
        """Cached value, running ``compute`` at most once across threads.

        Concurrent misses on the same key wait on a per-key lock instead of all
        running ``compute``. Without this, a cold panel burst (all-leads stats +
        lead-winners + forecast, ~100 tasks) rasterizes the same region masks
        dozens of times concurrently, serialized by the GIL — seconds of wall
        time for one 50 ms answer.
        """
        value = self.get(key)
        if value is not None:
            return value
        with self._lock:
            gate = self._pending.get(key)
            if gate is None:
                gate = Lock()
                self._pending[key] = gate
        try:
            with gate:
                value = self.get(key)
                if value is not None:
                    return value
                value = compute()
                self.put(key, value)
                return value
        finally:
            with self._lock:
                self._pending.pop(key, None)


# Shared thread pools for I/O fan-out. Module-level pools bound total worker
# threads across concurrent requests instead of each request building its own —
# an unauthenticated caller can no longer multiply the process's thread count by
# fanning out. Sized to overlap S3 latency and to match the S3 client's
# connection pool (see static_store).
#
# INVARIANT — two tiers, blocking flows one way only. ``_EXECUTOR`` runs *leaf*
# tasks: pure reads that never submit further work. ``_LEADS_EXECUTOR`` runs the
# per-lead tasks of ``stats_for_region_all_leads``, which block on leaf tasks.
# Never submit a task to ``_EXECUTOR`` that itself waits on ``_EXECUTOR``: with
# a bounded pool, two concurrent all-leads requests fill every worker with
# outer tasks and the inner reads they wait on can never be scheduled — a
# permanent deadlock that wedges the whole process (observed live).
_EXECUTOR_WORKERS = 16
_EXECUTOR = ThreadPoolExecutor(max_workers=_EXECUTOR_WORKERS, thread_name_prefix="stats")
_LEADS_EXECUTOR = ThreadPoolExecutor(max_workers=_EXECUTOR_WORKERS, thread_name_prefix="stats-leads")

_grid_cache: dict[tuple[str, str], tuple[np.ndarray, np.ndarray, int, int]] = {}
# Verification stat bins (~156 KB each).  Bounded LRU so a long-lived
# worker that serves many (model, stat, lead, accumulator) combinations can't
# accumulate the entire dataset (~650 MB+) in RAM.  Same OrderedDict+Lock LRU as
# wbd_boundaries._chunk_cache: a hit is moved to the end, the least-recently-used
# entry is evicted first.  512 bins ≈ 80 MB resident.
_BIN_CACHE_MAX = 512
_bin_cache_lock = Lock()
_bin_cache: "OrderedDict[tuple, np.ndarray]" = OrderedDict()
# Region masks (bool arrays, ~40 KB each). Admin/HUC masks are keyed by the
# resident grid arrays' id() + boundary id; ad-hoc polygon/rectangle masks by
# grid id + geometry signature, so lead_winners reuses one mask across every
# lead. All bounded so a long-lived worker can't accumulate masks without limit.
_MASK_CACHE_MAX = 2048
_admin_mask_cache = _BoundedLRU(_MASK_CACHE_MAX)
_huc_mask_cache = _BoundedLRU(_MASK_CACHE_MAX)
_military_mask_cache = _BoundedLRU(_MASK_CACHE_MAX)
_geom_mask_cache = _BoundedLRU(_MASK_CACHE_MAX)

# Forecast data lives in a separate store (S3 prefix) updated daily without
# redeploying the container.  TTL caches ensure new data is picked up.
_FORECAST_TTL = 300
_FORECAST_CACHE_MAX = 512
_forecast_bin_cache_lock = Lock()
_forecast_bin_cache: "OrderedDict[tuple, tuple[np.ndarray, float]]" = OrderedDict()
# forecast_calendar.json is no longer read by the backend — the SPA fetches it
# directly from the static asset (/stats/forecast/forecast_calendar.json).


def _get_forecast_bin(
    store: StaticStore, model: str, variable: str, lead_key: str,
) -> np.ndarray:
    cache_key = (store.cache_key, model, variable, lead_key)
    with _forecast_bin_cache_lock:
        cached = _forecast_bin_cache.get(cache_key)
        if cached is not None:
            arr, ts = cached
            if time.monotonic() - ts < _FORECAST_TTL:
                _forecast_bin_cache.move_to_end(cache_key)
                return arr
            del _forecast_bin_cache[cache_key]  # expired — drop the stale array
    raw_bytes = store.read_bytes(f"{model}/{variable}/lead_{lead_key}.bin")
    arr = np.frombuffer(raw_bytes, dtype=np.float32)
    with _forecast_bin_cache_lock:
        _forecast_bin_cache[cache_key] = (arr, time.monotonic())
        _forecast_bin_cache.move_to_end(cache_key)
        while len(_forecast_bin_cache) > _FORECAST_CACHE_MAX:
            _forecast_bin_cache.popitem(last=False)  # evict least-recently used
    return arr


def _read_forecast_bin_or_none(
    store: StaticStore, model: str, variable: str, lead_key: str,
) -> np.ndarray | None:
    try:
        return _get_forecast_bin(store, model, variable, lead_key)
    except (FileNotFoundError, OSError):
        return None


def _get_forecast_cell(
    store: StaticStore, model: str, variable: str, lead_key: str, flat_idx: int,
) -> float | None:
    """Single grid-cell forecast value for point regions.

    Fetches the full bin through the TTL cache and indexes it, rather than an
    uncached per-cell byte-range GET. A point click reads one cell per
    (model, variable, lead); caching the whole bin means repeated clicks (within
    the 5-min TTL) hit RAM instead of re-issuing ~200 S3 GETs each time, and a
    416 InvalidRange can't happen. Returns the raw float (may be NaN — callers
    test ``isfinite``).
    """
    if flat_idx < 0:
        return None
    arr = _read_forecast_bin_or_none(store, model, variable, lead_key)
    if arr is None or flat_idx >= arr.size:
        return None
    return float(arr[flat_idx])


def _data_bin_relative_path(
    model: str,
    stat_name: str,
    lead_key: str,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
) -> Path:
    path = Path("model_data") / model / stat_name
    if period == "monthly" and month is not None:
        path = path / "monthly" / month
    elif period == "seasonal" and season is not None:
        path = path / "seasonal" / season
    return path / f"lead_{lead_key}.bin"


def _get_grid(store: StaticStore, model: str) -> tuple[np.ndarray, np.ndarray, int, int]:
    key = (store.cache_key, model)
    cached = _grid_cache.get(key)
    if cached is not None:
        return cached
    relative_path = str(Path("model_data") / model / "grid.json")
    grid_text = store.read_text(relative_path)
    payload = json.loads(grid_text)
    lats = np.asarray(payload["lats"], dtype=np.float64)
    lons = np.asarray(payload["lons"], dtype=np.float64)
    n_lat = int(payload.get("nLat", lats.size))
    n_lon = int(payload.get("nLon", lons.size))
    if lats.size != n_lat or lons.size != n_lon:
        raise ValueError(f"Invalid grid.json for model {model}")
    result = (lats, lons, n_lat, n_lon)
    _grid_cache[key] = result
    return result


def _get_bin(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    *,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
) -> np.ndarray:
    cache_key = (store.cache_key, model, stat_name, lead_key, period, month, season)
    with _bin_cache_lock:
        cached = _bin_cache.get(cache_key)
        if cached is not None:
            _bin_cache.move_to_end(cache_key)
            return cached
    relative_path = str(_data_bin_relative_path(
        model, stat_name, lead_key, period=period, month=month, season=season
    ))
    raw_bytes = store.read_bytes(relative_path)
    arr = np.frombuffer(raw_bytes, dtype=np.float32)
    with _bin_cache_lock:
        _bin_cache[cache_key] = arr
        _bin_cache.move_to_end(cache_key)
        while len(_bin_cache) > _BIN_CACHE_MAX:
            _bin_cache.popitem(last=False)  # evict least-recently used
    return arr


def _read_bin_or_none(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    *,
    period: str,
    month: str | None,
    season: str | None,
) -> np.ndarray | None:
    try:
        return _get_bin(
            store,
            model,
            stat_name,
            lead_key,
            period=period,
            month=month,
            season=season,
        )
    except (FileNotFoundError, OSError):
        return None


def _get_bin_cell(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    flat_idx: int,
    *,
    period: str,
    month: str | None,
    season: str | None,
) -> float | None:
    """Single grid-cell verification value for point regions.

    Fetches the full bin through the bounded LRU cache and indexes it, rather than
    an uncached per-cell byte-range GET — repeated point queries then hit RAM
    instead of re-reading from S3, and a 416 InvalidRange can't happen (mirrors
    _get_forecast_cell). Returns the raw float (may be NaN).
    """
    if flat_idx < 0:
        return None
    arr = _read_bin_or_none(
        store, model, stat_name, lead_key, period=period, month=month, season=season
    )
    if arr is None or flat_idx >= arr.size:
        return None
    return float(arr[flat_idx])


# ---------------------------------------------------------------------------
# Consistency statistics (jumpiness / convergence).
#
# Their bins are PACKED: one (2, nCells) array per layer — plane 0 the
# region-summable numerator, plane 1 the denominator (jumpiness: FSS numerator
# Σ(f1-f2)² and denominator Σ(f1²+f2²) pooled over scorable valid dates;
# convergence: Σ squared error and sample count). A region value is the ratio
# of REGION-SUMMED planes (pooled FSS / pooled RMSE), not a mean of per-cell
# values — the FSS sums run over the region by definition. Jumpiness bins are
# float16 on disk (see export_static.export_consistency_layers), widened to
# float32 here. Monthly bins are not exported (period UI is yearly/seasonal),
# so monthly queries fall through to no_data.
# ---------------------------------------------------------------------------

def _packed_stat_names() -> set[str]:
    """Packed consistency stats with per-lead regional entries.

    Filtered to scope=="region": revision_efficiency is also packed_grid but
    scope=="region_pooled" — a single pooled scalar served only by
    ``revision_efficiency_for_region``, never as per-lead consistency entries.
    """
    return {name for name, entry in STATISTICS_BY_NAME.items()
            if entry.spec.storage.kind == "packed_grid"
            and entry.spec.scope == "region"}


def _consistency_bin_relative_path(
    model: str,
    stat_name: str,
    lead_key: str,
    *,
    period: str,
    month: str | None,
    season: str | None,
    threshold: float,
    scale: int,
) -> Path:
    spec = STATISTICS_BY_NAME[stat_name].spec
    # The export directory is the first segment of the storage path template,
    # not the statistic name: revision_efficiency's planes live under
    # ``revision/`` (jumpiness/convergence match their own names).
    path = Path("model_data") / model / spec.storage.path_template.split("/", 1)[0]
    if spec.axis == "lead_pair" and "thresholds_mm" in spec.parameters:
        # Only the threshold/scale-parameterized stat (jumpiness) has combo
        # subdirectories; revision planes share the lead_pair axis but have a
        # single un-tagged tree. threshold/scale are whitelisted in
        # request_models (VALID_THRESHOLDS/VALID_SCALES), so the tag can only
        # name a computed combination.
        path = path / combo_tag(threshold, scale)
    if period == "monthly" and month is not None:
        path = path / "monthly" / month
    elif period == "seasonal" and season is not None:
        path = path / "seasonal" / season
    prefix = "pair" if spec.axis == "lead_pair" else "lead"
    return path / f"{prefix}_{lead_key}.bin"


def _get_consistency_bin(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    *,
    period: str,
    month: str | None,
    season: str | None,
    threshold: float,
    scale: int,
) -> np.ndarray:
    cache_key = (store.cache_key, model, stat_name, lead_key,
                 period, month, season, threshold, scale)
    with _bin_cache_lock:
        cached = _bin_cache.get(cache_key)
        if cached is not None:
            _bin_cache.move_to_end(cache_key)
            return cached
    relative_path = str(_consistency_bin_relative_path(
        model, stat_name, lead_key, period=period, month=month, season=season,
        threshold=threshold, scale=scale,
    ))
    raw_bytes = store.read_bytes(relative_path)
    dtype = np.dtype(STATISTICS_BY_NAME[stat_name].spec.storage.export_dtype)
    flat = np.frombuffer(raw_bytes, dtype=dtype).astype(np.float32)
    if flat.size % 2:
        raise OSError(f"Corrupt consistency bin: {relative_path}")
    planes = flat.reshape(2, -1)
    with _bin_cache_lock:
        _bin_cache[cache_key] = planes
        _bin_cache.move_to_end(cache_key)
        while len(_bin_cache) > _BIN_CACHE_MAX:
            _bin_cache.popitem(last=False)  # evict least-recently used
    return planes


def _read_consistency_bin_or_none(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    *,
    period: str,
    month: str | None,
    season: str | None,
    threshold: float,
    scale: int,
) -> np.ndarray | None:
    try:
        return _get_consistency_bin(
            store, model, stat_name, lead_key, period=period, month=month,
            season=season, threshold=threshold, scale=scale,
        )
    except (FileNotFoundError, OSError):
        return None


def _pooled_num_den(
    planes: np.ndarray,
    *,
    mask: np.ndarray | None = None,
    flat_idx: int | None = None,
) -> tuple[float, float] | None:
    """Region-summed (numerator, denominator) from packed planes.

    None when the region is empty/off-grid, either pooled term is non-finite,
    or the pooled denominator is <= 0 (the ratio is undefined — missing, not
    zero). Shared by the finalized consistency values and the raw MSE pooling
    of ``revision_efficiency_for_region``.
    """
    if flat_idx is not None:
        if flat_idx >= planes.shape[1]:
            return None
        num = float(planes[0, flat_idx])
        den = float(planes[1, flat_idx])
    else:
        if mask is None or mask.size != planes.shape[1] or not np.any(mask):
            return None
        num = float(planes[0][mask].sum(dtype=np.float64))
        den = float(planes[1][mask].sum(dtype=np.float64))
    if not np.isfinite(num) or not np.isfinite(den) or den <= 0.0:
        return None
    return num, den


def _consistency_value(
    stat_name: str,
    planes: np.ndarray,
    *,
    mask: np.ndarray | None = None,
    flat_idx: int | None = None,
) -> float | None:
    """Pooled region value from packed planes; None where undefined.

    Jumpiness J = Σnum/Σden (= 1 - FSS between the paired runs); a zero
    denominator means neither run exceeded the threshold anywhere in the
    region over the pooled dates — undefined, per the statistic's definition
    (missing, not zero). Convergence is pooled RMSE sqrt(Σsse/Σcount).
    """
    pooled = _pooled_num_den(planes, mask=mask, flat_idx=flat_idx)
    if pooled is None:
        return None
    num, den = pooled
    finalize = STATISTICS_BY_NAME[stat_name].spec.storage.finalize
    ratio = num / den
    if finalize == "sqrt_ratio":
        return float(np.sqrt(ratio))
    if finalize == "one_minus_ratio":
        return float(1.0 - ratio)
    return float(ratio)


_consistency_meta_cache: dict[tuple[str, str], dict | None] = {}


_basin_episode_cache = _BoundedLRU(16)


class ScopesNotComputedError(LookupError):
    """episodes.json predates schema v3: no per-basin ``scopes`` block."""


# HUC6 is the (only) basin level the episode catalog is computed for.
EPISODE_HUC_LEVEL = 6

# Pooled scope keys, in display order (CONTRACT §0/§6).
EPISODE_SCOPES = ("ar_all", "non_ar", "all_days")

_EPISODE_METADATA_KEYS = (
    "id", "start_date", "end_date", "active_days",
    "peak_basin_area_fraction", "forecast_dates",
)


# Episode catalogs are computed per HUC2 region. California (18) keeps the
# original ``episodes.json`` name; every other region is ``episodes_huc{NN}.json``
# (mirrors ``stats_pipeline.episodes.episode_catalog_filename``, not imported so
# the Lambda package stays free of the offline pipeline).
CALIFORNIA_HUC2 = "18"


def episode_catalog_filename(huc2: str) -> str:
    huc2 = str(huc2)
    if huc2 == CALIFORNIA_HUC2:
        return EPISODE_CATALOG_FILENAME
    stem, ext = EPISODE_CATALOG_FILENAME.rsplit(".", 1)
    return f"{stem}_huc{huc2}.{ext}"


def huc2_of(huc: str | None) -> str:
    """HUC2 region prefix of a HUC6 code (California when absent)."""
    return str(huc)[:2] if huc else CALIFORNIA_HUC2


def _basin_episodes(store: StaticStore, model: str, huc2: str = CALIFORNIA_HUC2) -> dict:
    key = (store.cache_key, model, str(huc2))

    def load() -> dict:
        rel = str(Path("model_data") / model / episode_catalog_filename(huc2))
        return json.loads(store.read_text(rel))

    return _basin_episode_cache.get_or_compute(key, load)


def _episode_metadata(episode: dict) -> dict:
    """Display metadata for one episode — never its statistics values.

    ``cluster_id`` (v3: the bootstrap resampling unit, CONTRACT §0) rides
    along; it is None for a pre-v3 catalog rather than fabricated.
    """
    meta = {key: episode[key] for key in _EPISODE_METADATA_KEYS}
    meta["cluster_id"] = episode.get("cluster_id")
    return meta


def _scopes_available(basin: dict) -> list[str]:
    """Pooled scopes computed for this basin ([] on a pre-v3 catalog)."""
    scopes = basin.get("scopes") or {}
    return [scope for scope in EPISODE_SCOPES if scope in scopes]


def episode_catalog_for_huc(
    store: StaticStore,
    model: str,
    huc: str,
) -> dict:
    """Return metadata only for a basin's canonical episode catalog."""
    data = _basin_episodes(store, model, huc2_of(huc))
    basin = data.get("basins", {}).get(huc)
    if basin is None:
        raise KeyError(f"HUC6 {huc} is not present in the basin episode catalog")
    base = {
        "schema_version": data["schema_version"],
        "model": model,
        "huc": huc,
        "name": basin["name"],
        "lead_min": data["lead_min"],
        "lead_max": data["lead_max"],
        "lock_in_fraction": data["lock_in_fraction"],
        "minimum_basin_area_fraction": data.get("minimum_basin_area_fraction"),
    }
    base["episodes"] = [_episode_metadata(episode) for episode in basin["episodes"]]
    base["episode_count"] = len(base["episodes"])
    base["scopes_available"] = _scopes_available(basin)
    return base


def episode_catalog_all_basins(
    store: StaticStore, model: str, huc2: str = CALIFORNIA_HUC2,
) -> dict:
    """All-basin episode metadata listing (GET /api/ar/episodes without huc).

    Shape per CONTRACT §6: ``{model, huc_level: 6, basins: [{huc, name,
    episode_count, episodes: [metadata-only entries]}]}`` — supports
    episode-first entry into AR mode before a basin is chosen. Statistics
    values are withheld here exactly as in the per-basin catalog.
    """
    data = _basin_episodes(store, model, huc2)
    basins_out = []
    for huc, basin in sorted(data.get("basins", {}).items()):
        episodes = [_episode_metadata(episode) for episode in basin.get("episodes", [])]
        basins_out.append({
            "huc": huc,
            "name": basin["name"],
            "episode_count": len(episodes),
            "episodes": episodes,
        })
    return {
        "model": model,
        "huc2": str(huc2),
        "huc_level": EPISODE_HUC_LEVEL,
        "basins": basins_out,
    }


def episode_statistics(
    store: StaticStore, model: str, huc: str, episode_id: str,
    stat_names: list[str],
) -> dict:
    data = _basin_episodes(store, model, huc2_of(huc))
    basin = data.get("basins", {}).get(huc)
    if basin is None:
        raise KeyError(f"HUC6 {huc} is not present in the basin episode catalog")
    episode = next((item for item in basin["episodes"] if item["id"] == episode_id), None)
    if episode is None:
        raise KeyError(f"AR episode {episode_id} is not available for HUC6 {huc}")
    return {
        name: {
            **episode[name],
            "units": STATISTICS_BY_NAME[name].spec.units,
            "no_data": episode[name].get("value") is None,
        }
        for name in stat_names
    }


def scope_statistics(
    store: StaticStore, model: str, huc: str, scope: str,
    stat_names: list[str],
) -> dict:
    """Pooled scope statistics (ar_all / non_ar) from episodes.json v3.

    The per-statistic objects pass through verbatim (value/value_ci/clusters/…
    per CONTRACT §3) with ``units`` and ``no_data`` added — same mechanics as
    ``episode_statistics``. A catalog without a ``scopes`` block (schema v2)
    raises :class:`ScopesNotComputedError`, which the service maps to a 404
    telling the operator to rerun the episodes stage.
    """
    data = _basin_episodes(store, model, huc2_of(huc))
    basin = data.get("basins", {}).get(huc)
    if basin is None:
        raise KeyError(f"HUC6 {huc} is not present in the basin episode catalog")
    scopes = basin.get("scopes")
    if not scopes:
        raise ScopesNotComputedError(
            "pooled AR scopes not computed; rerun compute_stats.py --only episodes"
        )
    block = scopes.get(scope)
    if block is None:
        raise KeyError(f"Scope {scope!r} is not available for HUC6 {huc}")
    result = {}
    for name in stat_names:
        entry = block.get(name)
        if entry is None:
            raise KeyError(f"Statistic {name!r} is not present in the {scope!r} scope for HUC6 {huc}")
        result[name] = {
            **entry,
            "units": STATISTICS_BY_NAME[name].spec.units,
            "no_data": entry.get("value") is None,
        }
    return result


def _consistency_meta(store: StaticStore, model: str) -> dict | None:
    """Cached consistency_meta.json (None when absent — older exports)."""
    key = (store.cache_key, model)
    if key not in _consistency_meta_cache:
        try:
            text = store.read_text(str(Path("model_data") / model / "consistency_meta.json"))
            _consistency_meta_cache[key] = json.loads(text)
        except (FileNotFoundError, OSError, ValueError):
            _consistency_meta_cache[key] = None
    return _consistency_meta_cache[key]


def _consistency_dates(
    store: StaticStore, model: str, period: str,
    month: str | None, season: str | None,
) -> int | None:
    """Pooled valid-date count for the period, from consistency_meta.json.

    Surfaced with each consistency value because cross-model comparisons are
    only valid over identical valid-date sets — the count makes a mismatch
    visible. None when the meta file is absent (older exports).
    """
    meta = _consistency_meta(store, model)
    if not meta:
        return None
    counts = meta.get("date_counts", {})
    if period == "seasonal" and season is not None:
        value = counts.get("seasonal", {}).get(season)
    elif period == "monthly" and month is not None:
        value = counts.get("monthly", {}).get(month)
    else:
        value = counts.get("yearly")
    return int(value) if value is not None else None


def _consistency_entries(
    store: StaticStore,
    model: str,
    lead_key: str,
    stat_names: list[str],
    *,
    period: str,
    month: str | None,
    season: str | None,
    threshold: float,
    scale: int,
    mask: np.ndarray | None = None,
    flat_idx: int | None = None,
) -> dict[str, dict[str, float | None | str | bool | int]]:
    """Response entries for the requested consistency stats (point or area)."""
    result: dict[str, dict[str, float | None | str | bool | int]] = {}
    for stat_name in stat_names:
        planes = _read_consistency_bin_or_none(
            store, model, stat_name, lead_key, period=period, month=month,
            season=season, threshold=threshold, scale=scale,
        )
        value = None
        if planes is not None:
            value = _consistency_value(stat_name, planes, mask=mask, flat_idx=flat_idx)
        if value is None:
            result[stat_name] = _no_data_entry(stat_name)
            continue
        entry: dict[str, float | None | str | bool | int] = {
            "value": value,
            "units": STATISTICS_BY_NAME[stat_name].spec.units,
            "no_data": False,
        }
        dates = _consistency_dates(store, model, period, month, season)
        if dates is not None:
            entry["dates"] = dates
        result[stat_name] = entry
    return result


def _cells_for_stats_parallel(
    store: StaticStore,
    model: str,
    lead_key: str,
    stat_names: list[str],
    flat_idx: int,
    *,
    period: str,
    month: str | None,
    season: str | None,
) -> dict[str, float | None]:
    """Point-region counterpart of ``_bins_for_stats_parallel``: one byte-range cell
    read per statistic instead of one full-grid read."""
    if not stat_names:
        return {}
    if len(stat_names) == 1:
        sn = stat_names[0]
        return {sn: _get_bin_cell(
            store, model, sn, lead_key, flat_idx, period=period, month=month, season=season
        )}
    out: dict[str, float | None] = {}
    future_to_stat = {
        _EXECUTOR.submit(
            _get_bin_cell,
            store, model, sn, lead_key, flat_idx,
            period=period, month=month, season=season,
        ): sn
        for sn in stat_names
    }
    for fut in as_completed(future_to_stat):
        out[future_to_stat[fut]] = fut.result()
    return out


def _bins_for_stats_parallel(
    store: StaticStore,
    model: str,
    lead_key: str,
    stat_names: list[str],
    *,
    period: str,
    month: str | None,
    season: str | None,
) -> dict[str, np.ndarray | None]:
    """Fetch all statistic layers for one lead concurrently (S3 / disk I/O bound)."""
    if not stat_names:
        return {}
    if len(stat_names) == 1:
        sn = stat_names[0]
        return {sn: _read_bin_or_none(store, model, sn, lead_key, period=period, month=month, season=season)}
    out: dict[str, np.ndarray | None] = {}
    future_to_stat = {
        _EXECUTOR.submit(
            _read_bin_or_none,
            store,
            model,
            sn,
            lead_key,
            period=period,
            month=month,
            season=season,
        ): sn
        for sn in stat_names
    }
    for fut in as_completed(future_to_stat):
        out[future_to_stat[fut]] = fut.result()
    return out


def _points_in_ring(xs: np.ndarray, ys: np.ndarray, ring: list[list[float]]) -> np.ndarray:
    """Vectorized ray-cast of many points against one ring.

    Classic even-odd rule: an edge whose endpoints straddle the point's
    latitude toggles containment when the point sits left of the edge's
    crossing longitude. Edges are processed in blocks so the (edges × points)
    temporaries stay bounded however large the ring is — a HUC2 boundary is
    thousands of vertices, and the scalar per-cell loop this replaces took
    ~0.8 s per grid.
    """
    if len(ring) < 3 or xs.size == 0:
        return np.zeros(xs.shape, dtype=bool)
    verts = np.asarray([(p[0], p[1]) for p in ring], dtype=np.float64)
    vx, vy = verts[:, 0], verts[:, 1]
    px, py = np.roll(vx, 1), np.roll(vy, 1)  # previous vertex of each edge
    crossings = np.zeros(xs.shape, dtype=np.int64)
    block = max(16, min(int(vx.size), 2_000_000 // int(xs.size)))
    for start in range(0, vx.size, block):
        yi = vy[start:start + block, None]
        yj = py[start:start + block, None]
        straddles = (yi > ys[None, :]) != (yj > ys[None, :])
        # Horizontal edges (yi == yj) never straddle; the div-by-zero rows they
        # produce are masked out by ``straddles`` (NaN/inf compare False anyway).
        with np.errstate(divide="ignore", invalid="ignore"):
            x_int = vx[start:start + block, None] + (
                (px[start:start + block, None] - vx[start:start + block, None])
                * (ys[None, :] - yi)
                / (yj - yi)
            )
        crossings += (straddles & (xs[None, :] < x_int)).sum(axis=0)
    return (crossings % 2).astype(bool)


def _cell_axis_bounds(centers: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Low/high edge of each grid cell along a 1D axis of cell-center coordinates."""
    c = np.asarray(centers, dtype=np.float64)
    n = int(c.size)
    if n == 0:
        return c, c
    if n == 1:
        span = 1.0
        return np.array([c[0] - span / 2]), np.array([c[0] + span / 2])
    low = np.empty(n, dtype=np.float64)
    high = np.empty(n, dtype=np.float64)
    low[0] = c[0] - (c[1] - c[0]) / 2
    high[-1] = c[-1] + (c[-1] - c[-2]) / 2
    mid = (c[:-1] + c[1:]) / 2
    low[1:] = mid
    high[:-1] = mid
    return low, high


def _mask_rectangle_overlap(
    lats: np.ndarray,
    lons: np.ndarray,
    west: float,
    south: float,
    east: float,
    north: float,
) -> np.ndarray:
    """Cells whose footprint overlaps the query rectangle (not only centers inside)."""
    lat_low, lat_high = _cell_axis_bounds(lats)
    lon_low, lon_high = _cell_axis_bounds(lons)
    lat_ok = (lat_low <= north) & (lat_high >= south)
    lon_ok = (lon_low <= east) & (lon_high >= west)
    return np.outer(lat_ok, lon_ok).reshape(-1)


def _subgrid_mask(
    n_lon: int,
    n_cells: int,
    ridx: np.ndarray,
    cidx: np.ndarray,
    inside: np.ndarray,
) -> np.ndarray:
    """Scatter a bbox-subgrid ``inside`` vector back onto the full flat grid."""
    mask = np.zeros(n_cells, dtype=bool)
    flat = (ridx[:, None] * n_lon + cidx[None, :]).ravel()
    mask[flat[inside]] = True
    return mask


def _polygon_mask_centers(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    ring: list[list[float]],
) -> np.ndarray:
    if len(ring) < 3:
        return np.zeros(n_lat * n_lon, dtype=bool)
    lons_ring = [p[0] for p in ring]
    lats_ring = [p[1] for p in ring]
    ridx = np.nonzero((lats >= min(lats_ring)) & (lats <= max(lats_ring)))[0]
    cidx = np.nonzero((lons >= min(lons_ring)) & (lons <= max(lons_ring)))[0]
    if ridx.size == 0 or cidx.size == 0:
        return np.zeros(n_lat * n_lon, dtype=bool)
    gy, gx = np.meshgrid(
        np.asarray(lats[ridx], dtype=np.float64),
        np.asarray(lons[cidx], dtype=np.float64),
        indexing="ij",
    )
    inside = _points_in_ring(gx.ravel(), gy.ravel(), ring)
    return _subgrid_mask(n_lon, n_lat * n_lon, ridx, cidx, inside)


def _polygon_mask_centers_or_corners(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    ring: list[list[float]],
) -> np.ndarray:
    """Include cells whose center or any corner lies inside the polygon."""
    if len(ring) < 3:
        return np.zeros(n_lat * n_lon, dtype=bool)
    lat_low, lat_high = _cell_axis_bounds(lats)
    lon_low, lon_high = _cell_axis_bounds(lons)
    lons_ring = [p[0] for p in ring]
    lats_ring = [p[1] for p in ring]
    ridx = np.nonzero((lat_high >= min(lats_ring)) & (lat_low <= max(lats_ring)))[0]
    cidx = np.nonzero((lon_high >= min(lons_ring)) & (lon_low <= max(lons_ring)))[0]
    if ridx.size == 0 or cidx.size == 0:
        return np.zeros(n_lat * n_lon, dtype=bool)
    inside = np.zeros(ridx.size * cidx.size, dtype=bool)
    for ys_axis, xs_axis in (
        (lats[ridx], lons[cidx]),        # cell centers
        (lat_low[ridx], lon_low[cidx]),  # then the four cell corners
        (lat_low[ridx], lon_high[cidx]),
        (lat_high[ridx], lon_low[cidx]),
        (lat_high[ridx], lon_high[cidx]),
    ):
        gy, gx = np.meshgrid(
            np.asarray(ys_axis, dtype=np.float64),
            np.asarray(xs_axis, dtype=np.float64),
            indexing="ij",
        )
        inside |= _points_in_ring(gx.ravel(), gy.ravel(), ring)
    return _subgrid_mask(n_lon, n_lat * n_lon, ridx, cidx, inside)


# Sub-cell regions (a HUC12, a small county, a tight drawn shape) take the
# cell(s) whose footprint overlaps their bounding box — the answer the point
# tool gives for a click on them — instead of no_data. The fallback is gated
# on the bbox being at most this many grid steps on BOTH axes, so a region
# that merely clips the grid's edge (Alaska's bbox reaches the top rows) is
# still honestly empty.
SUBCELL_FALLBACK_MAX_STEPS = 2.0


def _subcell_bbox_mask(
    lats: np.ndarray, lons: np.ndarray, bbox: list | tuple | None,
) -> np.ndarray | None:
    """Footprint-overlap mask for a sub-cell region's bbox, or None when the
    bbox is missing or too large to be a sub-cell case."""
    if not bbox or len(bbox) != 4:
        return None
    west, south, east, north = (float(v) for v in bbox)
    lat_step = float(abs(lats[1] - lats[0])) if lats.size > 1 else float("inf")
    lon_step = float(abs(lons[1] - lons[0])) if lons.size > 1 else float("inf")
    if (east - west) > SUBCELL_FALLBACK_MAX_STEPS * lon_step or (north - south) > SUBCELL_FALLBACK_MAX_STEPS * lat_step:
        return None
    return _mask_rectangle_overlap(lats, lons, west, south, east, north)


def _multi_polygon_mask(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    polygons: list,
    *,
    use_corners: bool,
) -> np.ndarray:
    """Union of (outer ∖ holes) across MultiPolygon parts.

    Holes always use a strict centers-only test so cells that merely clip a
    hole's edge aren't dropped.  ``use_corners`` controls only the outer test.
    """
    mask = np.zeros(n_lat * n_lon, dtype=bool)
    outer_fn = _polygon_mask_centers_or_corners if use_corners else _polygon_mask_centers
    for poly in polygons:
        if not poly:
            continue
        outer = poly[0]
        if len(outer) < 3:
            continue
        outer_mask = outer_fn(lats, lons, n_lat, n_lon, outer)
        if len(poly) > 1:
            hole_mask = np.zeros(n_lat * n_lon, dtype=bool)
            for hole in poly[1:]:
                if len(hole) >= 3:
                    hole_mask |= _polygon_mask_centers(lats, lons, n_lat, n_lon, hole)
            outer_mask &= ~hole_mask
        mask |= outer_mask
    return mask


def _admin_region_mask(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    level: str,
    fips: str,
) -> np.ndarray:
    def compute() -> np.ndarray:
        try:
            entry = admin_boundaries.get_entry(level, fips)
        except (KeyError, admin_boundaries.AdminBoundariesUnavailable):
            return np.zeros(n_lat * n_lon, dtype=bool)
        polygons = entry.get("polygons") or []
        mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=False)
        if not np.any(mask):
            mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=True)
        if not np.any(mask):
            # Sub-cell county → the cell(s) containing it (see _subcell_bbox_mask).
            # An off-grid region (bbox larger than the gate, or overlapping no
            # cell) stays honestly empty — never the nearest edge cell.
            fallback = _subcell_bbox_mask(lats, lons, entry.get("bbox"))
            if fallback is not None:
                return fallback
        return mask

    return _admin_mask_cache.get_or_compute((id(lats), id(lons), level, fips), compute)


def _huc_region_mask(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    huc: str,
) -> np.ndarray:
    def compute() -> np.ndarray:
        try:
            entry = wbd_boundaries.get_entry(huc)
        except (KeyError, ValueError, wbd_boundaries.WbdBoundariesUnavailable):
            return np.zeros(n_lat * n_lon, dtype=bool)
        polygons = entry.get("polygons") or []
        mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=False)
        if not np.any(mask):
            mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=True)
        if not np.any(mask):
            # Sub-cell HUC10/12 → the cell(s) containing it (see _subcell_bbox_mask).
            fallback = _subcell_bbox_mask(lats, lons, entry.get("bbox"))
            if fallback is not None:
                return fallback
        return mask

    return _huc_mask_cache.get_or_compute((id(lats), id(lons), huc), compute)


def _military_region_mask(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    site: str,
) -> np.ndarray:
    def compute() -> np.ndarray:
        try:
            entry = military_boundaries.get_entry(site)
        except (KeyError, military_boundaries.MilitaryBoundariesUnavailable):
            return np.zeros(n_lat * n_lon, dtype=bool)
        polygons = entry.get("polygons") or []
        mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=False)
        if not np.any(mask):
            mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=True)
        if not np.any(mask):
            # Unlike admin/HUC regions, most installations are smaller than a grid
            # cell, so a polygon that contains no cell center or corner is the norm,
            # not an off-grid query. Fall back to the cells whose footprint overlaps
            # the site's bbox — the same "cell containing the site" answer the point
            # tool would give for a click on it. A genuinely off-grid site still
            # yields an empty mask here (bbox overlaps no cell footprint) → no_data.
            bbox = entry.get("bbox") or []
            if len(bbox) == 4:
                west, south, east, north = (float(v) for v in bbox)
                return _mask_rectangle_overlap(lats, lons, west, south, east, north)
        return mask

    return _military_mask_cache.get_or_compute((id(lats), id(lons), site), compute)


def _build_region_mask(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    region: dict,
) -> np.ndarray:
    mask = np.zeros(n_lat * n_lon, dtype=bool)
    region_type = region.get("type")

    if region_type == "rectangle":
        west, south, east, north = region["bounds"]
        geom_key = (id(lats), id(lons), "rect", west, south, east, north)
        # No nearest-cell fabrication: a rectangle covering no grid cell is no_data.
        return _geom_mask_cache.get_or_compute(
            geom_key,
            lambda: _mask_rectangle_overlap(lats, lons, west, south, east, north),
        )

    if region_type == "polygon":
        ring = [[float(p[0]), float(p[1])] for p in region.get("coordinates", [])]
        if len(ring) < 3:
            return mask
        geom_key = (id(lats), id(lons), "poly", tuple((p[0], p[1]) for p in ring))

        def compute_poly() -> np.ndarray:
            poly_mask = _polygon_mask_centers(lats, lons, n_lat, n_lon, ring)
            if not np.any(poly_mask):
                poly_mask = _polygon_mask_centers_or_corners(lats, lons, n_lat, n_lon, ring)
            if not np.any(poly_mask):
                # Sub-cell drawn shape → the cell(s) containing it (see _subcell_bbox_mask).
                xs = [p[0] for p in ring]
                ys = [p[1] for p in ring]
                fallback = _subcell_bbox_mask(lats, lons, (min(xs), min(ys), max(xs), max(ys)))
                if fallback is not None:
                    return fallback
            return poly_mask

        return _geom_mask_cache.get_or_compute(geom_key, compute_poly)

    if region_type == "admin":
        level = str(region.get("level") or "")
        fips = str(region.get("fips") or "")
        if not level or not fips:
            return mask
        return _admin_region_mask(lats, lons, n_lat, n_lon, level, fips)

    if region_type == "huc":
        huc = str(region.get("huc") or "")
        if not huc:
            return mask
        return _huc_region_mask(lats, lons, n_lat, n_lon, huc)

    if region_type == "military":
        site = str(region.get("site") or "")
        if not site:
            return mask
        return _military_region_mask(lats, lons, n_lat, n_lon, site)

    return mask


def _no_data_entry(stat_name: str) -> dict[str, float | None | str | bool]:
    plugin = STATISTICS_BY_NAME[stat_name]
    return {"value": None, "units": plugin.spec.units, "no_data": True}


def _masked_mean(values: np.ndarray, mask: np.ndarray) -> float | None:
    masked = values[mask]
    finite = masked[np.isfinite(masked)]
    if finite.size == 0:
        return None
    return float(finite.mean())


def _point_flat_idx(
    lats: np.ndarray, lons: np.ndarray, n_lon: int, region: dict,
) -> int | None:
    """Nearest-cell flat index for a point region; None when the click is
    off-grid (more than ~one grid step from the nearest cell center on either
    axis) — honest no-data instead of reporting the nearest edge cell."""
    lon = float(region["coordinates"][0])
    lat = float(region["coordinates"][1])
    lat_idx = int(np.argmin(np.abs(lats - lat)))
    lon_idx = int(np.argmin(np.abs(lons - lon)))
    lat_step = float(abs(lats[1] - lats[0])) if lats.size > 1 else float("inf")
    lon_step = float(abs(lons[1] - lons[0])) if lons.size > 1 else float("inf")
    if (abs(float(lats[lat_idx]) - lat) > lat_step
            or abs(float(lons[lon_idx]) - lon) > lon_step):
        return None
    return lat_idx * n_lon + lon_idx


def revision_efficiency_for_region(
    *,
    static_root: Path | None = None,
    store: StaticStore | None = None,
    model: str,
    region: dict,
    period: str = "yearly",
    season: str | None = None,
) -> dict[str, object]:
    """Grid-pooled revision efficiency R over a region (CONTRACT §5).

    numer = Σ_pairs (Σ_mask rev_sse / Σ_mask rev_cnt) — pooled mean squared
    revision per run pair, summed over pairs lead_min..lead_max-1 from the
    ``revision/`` packed planes.
    denom = MSE(lead_max) − MSE(lead_min), each MSE the pooled Σ_mask sse /
    Σ_mask cnt from the convergence planes (MSE, not the finalized RMSE).
    R = numer/denom. denom <= 0 → value None + ``skill_gain_nonpositive``.
    Any missing bin, empty/off-grid region, or invalid pooled term → explicit
    no-data — a partial pooling is never reported as the region's value.
    No CI: there is no per-date data online for the gridded planes.

    ``dates`` is the pooled valid-date count from consistency_meta.json (the
    revision planes are accumulated over the same scorable-date set as the
    consistency stats); ``definition`` marks the value as grid-pooled so it is
    never conflated with the basin-history R of the episode catalog.
    """
    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    entry: dict[str, object] = {
        "value": None,
        "units": STATISTICS_BY_NAME["revision_efficiency"].spec.units,
        "no_data": True,
        "dates": _consistency_dates(resolved_store, model, period, None, season),
        "definition": "grid-pooled",
    }

    # Lead range: prefer consistency_meta.json (describes the data actually on
    # disk); fall back to the model registry for older exports without meta.
    meta = _consistency_meta(resolved_store, model) or {}
    config = MODEL_REGISTRY.get(model)
    lead_min = meta.get("lead_days_min")
    lead_max = meta.get("lead_days_max")
    if lead_min is None and config is not None:
        lead_min = config.lead_days_min
    if lead_max is None and config is not None:
        lead_max = config.lead_days_max
    if lead_min is None or lead_max is None or int(lead_max) - int(lead_min) < 1:
        return entry
    lead_min, lead_max = int(lead_min), int(lead_max)

    lats, lons, n_lat, n_lon = _get_grid(resolved_store, model)
    mask: np.ndarray | None = None
    flat_idx: int | None = None
    if region.get("type") == "point":
        flat_idx = _point_flat_idx(lats, lons, n_lon, region)
        if flat_idx is None:
            return entry
    else:
        mask = _build_region_mask(lats, lons, n_lat, n_lon, region)
        if not np.any(mask):
            return entry

    # Fan out the bin reads on the leaf executor (pure reads — pool invariant
    # holds). threshold/scale are irrelevant here (neither stat is combo-
    # tagged) but the shared read path requires them; pass the defaults.
    tasks = {("revision_efficiency", str(pair)) for pair in range(lead_min, lead_max)}
    tasks |= {("convergence", str(lead_min)), ("convergence", str(lead_max))}
    planes_by_key: dict[tuple[str, str], np.ndarray | None] = {}
    future_to_key = {
        _EXECUTOR.submit(
            _read_consistency_bin_or_none,
            resolved_store, model, stat_name, lead_key,
            period=period, month=None, season=season,
            threshold=DEFAULT_THRESHOLD_MM, scale=DEFAULT_SCALE_KM,
        ): (stat_name, lead_key)
        for stat_name, lead_key in tasks
    }
    for fut in as_completed(future_to_key):
        planes_by_key[future_to_key[fut]] = fut.result()

    numer = 0.0
    for pair in range(lead_min, lead_max):
        planes = planes_by_key.get(("revision_efficiency", str(pair)))
        if planes is None:
            return entry
        pooled = _pooled_num_den(planes, mask=mask, flat_idx=flat_idx)
        if pooled is None:
            return entry
        rev_sse, rev_cnt = pooled
        numer += rev_sse / rev_cnt

    mse: dict[int, float] = {}
    for lead in (lead_min, lead_max):
        planes = planes_by_key.get(("convergence", str(lead)))
        if planes is None:
            return entry
        pooled = _pooled_num_den(planes, mask=mask, flat_idx=flat_idx)
        if pooled is None:
            return entry
        sse, cnt = pooled
        mse[lead] = sse / cnt

    denom = mse[lead_max] - mse[lead_min]
    if denom <= 0.0:
        entry["skill_gain_nonpositive"] = True
        return entry
    entry["value"] = float(numer / denom)
    entry["no_data"] = False
    return entry


def stats_for_region(
    *,
    static_root: Path | None = None,
    store: StaticStore | None = None,
    model: str,
    lead: str | int,
    region: dict,
    stat_names: list[str] | None = None,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
    threshold: float | None = None,
    scale: int | None = None,
) -> dict[str, dict[str, float | None | str | bool]]:
    lead_key = str(lead)
    stat_names = stat_names or [name for name, entry in STATISTICS_BY_NAME.items()
                                if entry.spec.scope == "region"]
    # Consistency stats aggregate as ratios of region-summed accumulator
    # planes, so they take a separate read path from the per-cell-mean stats.
    packed_names = _packed_stat_names()
    consistency_names = [s for s in stat_names if s in packed_names]
    stat_names = [s for s in stat_names if s not in packed_names]
    threshold = DEFAULT_THRESHOLD_MM if threshold is None else float(threshold)
    scale = DEFAULT_SCALE_KM if scale is None else int(scale)
    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    lats, lons, n_lat, n_lon = _get_grid(resolved_store, model)
    region_type = region.get("type")
    result: dict[str, dict[str, float | None | str | bool]] = {}

    if region_type == "point":
        # Honest no-data for an off-grid click (see _point_flat_idx): don't
        # report the nearest edge cell as though it were the point's value.
        flat_idx = _point_flat_idx(lats, lons, n_lon, region)
        if flat_idx is None:
            return {stat_name: _no_data_entry(stat_name)
                    for stat_name in stat_names + consistency_names}
        # One cell needed — the cached full bin is indexed, not the whole grid re-read.
        cells = _cells_for_stats_parallel(
            resolved_store,
            model,
            lead_key,
            stat_names,
            flat_idx,
            period=period,
            month=month,
            season=season,
        )
        for stat_name in stat_names:
            value = cells.get(stat_name)
            if value is None or not np.isfinite(value):
                result[stat_name] = _no_data_entry(stat_name)
                continue
            result[stat_name] = {
                "value": value,
                "units": STATISTICS_BY_NAME[stat_name].spec.units,
                "no_data": False,
            }
        if consistency_names:
            result.update(_consistency_entries(
                resolved_store, model, lead_key, consistency_names,
                period=period, month=month, season=season,
                threshold=threshold, scale=scale, flat_idx=flat_idx,
            ))
        return result

    mask = _build_region_mask(lats, lons, n_lat, n_lon, region)
    if not np.any(mask):
        return {stat_name: _no_data_entry(stat_name)
                for stat_name in stat_names + consistency_names}

    bins = _bins_for_stats_parallel(
        resolved_store,
        model,
        lead_key,
        stat_names,
        period=period,
        month=month,
        season=season,
    )
    for stat_name in stat_names:
        values = bins.get(stat_name)
        if values is None:
            result[stat_name] = _no_data_entry(stat_name)
            continue
        if values.size != n_lat * n_lon:
            result[stat_name] = _no_data_entry(stat_name)
            continue
        value = _masked_mean(values, mask)
        if value is None:
            result[stat_name] = _no_data_entry(stat_name)
            continue
        result[stat_name] = {
            "value": value,
            "units": STATISTICS_BY_NAME[stat_name].spec.units,
            "no_data": False,
        }

    if consistency_names:
        result.update(_consistency_entries(
            resolved_store, model, lead_key, consistency_names,
            period=period, month=month, season=season,
            threshold=threshold, scale=scale, mask=mask,
        ))

    return result


def _winner_metric_label(stat_name: str) -> str:
    """Human-readable description of how models are ranked for this statistic."""
    mode = STATISTICS_BY_NAME[stat_name].spec.winner_mode
    if mode == "absolute_lower":
        return "region_abs_mean_value_lower_better"
    if mode == "higher":
        return "region_mean_value_higher_better"
    return "region_mean_value_lower_better"


def _loss_from_value(v: float, stat_name: str) -> float | None:
    """Per-cell loss (lower is better): abs for signed RB, negated for CC, else identity."""
    if not np.isfinite(v):
        return None
    mode = STATISTICS_BY_NAME[stat_name].spec.winner_mode
    if mode == "absolute_lower":
        return float(abs(v))
    if mode == "higher":
        return float(-v)
    return float(v)


def _region_loss_scalar(
    values: np.ndarray,
    stat_name: str,
    *,
    mask: np.ndarray | None = None,
    flat_idx: int | None = None,
) -> float | None:
    """
    Scalar loss for winner selection; **lower is better**. RB (signed) ranks by
    the magnitude of the region's signed mean (|mean|, O5), so the winner
    matches the signed number shown in the panel; CC negates the mean (higher
    correlation → lower loss); every other stat uses the plain region mean.
    """
    if flat_idx is not None:
        return _loss_from_value(values[flat_idx], stat_name)

    if mask is None or not np.any(mask):
        return None
    masked = values[mask]
    finite = masked[np.isfinite(masked)]
    if finite.size == 0:
        return None
    mode = STATISTICS_BY_NAME[stat_name].spec.winner_mode
    if mode == "absolute_lower":
        return float(abs(np.mean(finite)))
    if mode == "higher":
        return float(-np.mean(finite))
    return float(np.mean(finite))


def _region_loss_for_model_lead(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    region: dict,
    *,
    period: str,
    month: str | None,
    season: str | None,
) -> float | None:
    lats, lons, n_lat, n_lon = _get_grid(store, model)
    region_type = region.get("type")

    if region_type == "point":
        # Single cell — byte-range read instead of the whole grid.
        lon = float(region["coordinates"][0])
        lat = float(region["coordinates"][1])
        lat_idx = int(np.argmin(np.abs(lats - lat)))
        lon_idx = int(np.argmin(np.abs(lons - lon)))
        flat_idx = lat_idx * n_lon + lon_idx
        cell = _get_bin_cell(
            store, model, stat_name, lead_key, flat_idx,
            period=period, month=month, season=season,
        )
        if cell is None:
            return None
        return _loss_from_value(cell, stat_name)

    try:
        values = _get_bin(
            store,
            model,
            stat_name,
            lead_key,
            period=period,
            month=month,
            season=season,
        )
    except FileNotFoundError:
        return None
    if values.size != n_lat * n_lon:
        return None

    mask = _build_region_mask(lats, lons, n_lat, n_lon, region)
    return _region_loss_scalar(values, stat_name, mask=mask)


def lead_winners_for_region(
    *,
    store: StaticStore,
    region: dict,
    stat_name: str,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
    min_lead: int,
    max_lead: int,
) -> dict[str, object]:
    """
    For each lead day, pick the model with the lowest loss over ``region`` (see
    ``_region_loss_scalar``: |signed mean| for RB, negated mean for CC, plain
    mean otherwise). Computed live — there is no static lead_winners.json.
    """
    model_order = tuple(sorted(MODEL_REGISTRY.keys()))

    # Prefetch grids so the parallel workers don't all race to download them.
    for mk in model_order:
        _get_grid(store, mk)

    # Fan out every (model, lead) combination in one flat pool.
    lead_range = list(range(min_lead, max_lead + 1))
    tasks: list[tuple[str, str]] = [
        (mk, str(lead)) for lead in lead_range for mk in model_order
    ]
    loss_results: dict[tuple[str, str], float | None] = {}
    future_to_key = {
        _EXECUTOR.submit(
            _region_loss_for_model_lead,
            store, mk, stat_name, lead_key, region,
            period=period, month=month, season=season,
        ): (mk, lead_key)
        for mk, lead_key in tasks
    }
    for fut in as_completed(future_to_key):
        loss_results[future_to_key[fut]] = fut.result()

    leads_out: dict[str, str] = {}
    for lead in lead_range:
        lead_key = str(lead)
        eligible_loss: dict[str, float] = {}
        for mk in model_order:
            loss = loss_results.get((mk, lead_key))
            if loss is not None and np.isfinite(loss):
                eligible_loss[mk] = loss
        if not eligible_loss:
            continue
        if len(eligible_loss) == 1:
            winner = next(iter(eligible_loss))
        else:
            winner = min(
                eligible_loss,
                key=lambda m: (eligible_loss[m], model_order.index(m)),
            )
        leads_out[lead_key] = winner

    accumulator_key: str | None
    if period == "monthly":
        accumulator_key = month
    elif period == "seasonal":
        accumulator_key = season
    else:
        accumulator_key = None

    return {
        "schema": "best_model_by_lead/v1",
        "statistic": stat_name,
        "accumulator": period,
        "accumulator_key": accumulator_key,
        "models_considered": list(model_order),
        "metric": _winner_metric_label(stat_name),
        "aggregation": "region",
        "leads": leads_out,
    }


def forecast_all_models(
    *,
    store: StaticStore,
    forecast_store: StaticStore,
    region: dict,
    variable: str = "precip",
) -> dict[str, object]:
    """Forecast values for all models × all leads for a region (yearly only).

    *store* provides grids (``model_data/{model}/grid.json``).  *forecast_store* is
    the separate forecast tree (``{model}/{variable}/lead_{n}.bin``) that can be
    updated on S3 daily without redeploying the container.  *variable* selects the
    forecast quantity (precip/temp/wind/humidity); unknown values fall back to
    precip.  ``forecast_calendar.json`` is fetched by the SPA directly from the
    static asset, not echoed here.
    """
    model_order = tuple(sorted(MODEL_REGISTRY.keys()))
    if variable not in FORECAST_VARIABLES:
        variable = "precip"
    units = FORECAST_VARIABLES[variable].units

    # Prefetch grids (from the stats store).
    for mk in model_order:
        _get_grid(store, mk)

    # Build (model, lead_key) tasks covering each model's full lead range.
    tasks: list[tuple[str, str]] = []
    for mk in model_order:
        cfg = MODEL_REGISTRY[mk]
        for lead in range(cfg.lead_days_min, cfg.lead_days_max + 1):
            tasks.append((mk, str(lead)))

    region_type = region.get("type")
    is_point = region_type == "point"

    # value_by_task[(model, lead_key)] = finite region value, or None for no_data.
    value_by_task: dict[tuple[str, str], float | None] = {}

    if is_point:
        # Each model has its own grid, so resolve the cell index per model, then
        # read only that one float32 per (model, lead) — not the whole ~156 KB bin.
        lon = float(region["coordinates"][0])
        lat = float(region["coordinates"][1])
        flat_by_model: dict[str, int] = {}
        for mk in model_order:
            lats, lons, n_lat, n_lon = _get_grid(store, mk)
            lat_idx = int(np.argmin(np.abs(lats - lat)))
            lon_idx = int(np.argmin(np.abs(lons - lon)))
            flat_by_model[mk] = lat_idx * n_lon + lon_idx
        future_to_key = {
            _EXECUTOR.submit(
                _get_forecast_cell, forecast_store, mk, variable, lead_key,
                flat_by_model[mk],
            ): (mk, lead_key)
            for mk, lead_key in tasks
        }
        for fut in as_completed(future_to_key):
            v = fut.result()
            value_by_task[future_to_key[fut]] = (
                v if v is not None and np.isfinite(v) else None
            )
    else:
        # Region masks need the full grid, so read whole bins and take the mean.
        bin_results: dict[tuple[str, str], np.ndarray | None] = {}
        future_to_key = {
            _EXECUTOR.submit(
                _read_forecast_bin_or_none, forecast_store, mk, variable, lead_key,
            ): (mk, lead_key)
            for mk, lead_key in tasks
        }
        for fut in as_completed(future_to_key):
            bin_results[future_to_key[fut]] = fut.result()
        for mk in model_order:
            lats, lons, n_lat, n_lon = _get_grid(store, mk)
            mask = _build_region_mask(lats, lons, n_lat, n_lon, region)
            cfg = MODEL_REGISTRY[mk]
            for lead in range(cfg.lead_days_min, cfg.lead_days_max + 1):
                lead_key = str(lead)
                values = bin_results.get((mk, lead_key))
                if values is None or values.size != n_lat * n_lon:
                    value_by_task[(mk, lead_key)] = None
                else:
                    value_by_task[(mk, lead_key)] = _masked_mean(values, mask)

    # Assemble per-model results from the resolved values.
    models_out: dict[str, dict] = {}
    for mk in model_order:
        cfg = MODEL_REGISTRY[mk]
        leads_out: dict[str, dict] = {}
        for lead in range(cfg.lead_days_min, cfg.lead_days_max + 1):
            lead_key = str(lead)
            v = value_by_task.get((mk, lead_key))
            if v is None:
                leads_out[lead_key] = {"value": None, "units": units, "no_data": True}
            else:
                leads_out[lead_key] = {"value": float(v), "units": units, "no_data": False}
        models_out[mk] = {"leads": leads_out}

    # forecast_calendar.json is intentionally NOT included here: the SPA fetches it
    # directly from the static asset (/stats/forecast/forecast_calendar.json) so init
    # dates stay current with new forecast exports without the backend echoing them.
    return {"variable": variable, "models": models_out}


def stats_for_region_all_leads(
    *,
    static_root: Path | None = None,
    store: StaticStore | None = None,
    model: str,
    region: dict,
    min_lead: int,
    max_lead: int,
    stat_names: list[str] | None = None,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
    threshold: float | None = None,
    scale: int | None = None,
) -> list[dict[str, object]]:
    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    lead_range = list(range(min_lead, max_lead + 1))

    # Prefetch grid once so parallel workers don't all race to S3 for it.
    _get_grid(resolved_store, model)

    if len(lead_range) <= 1:
        return [
            {
                "lead": lead,
                "stats": stats_for_region(
                    store=resolved_store,
                    model=model,
                    lead=lead,
                    region=region,
                    stat_names=stat_names,
                    period=period,
                    month=month,
                    season=season,
                    threshold=threshold,
                    scale=scale,
                ),
            }
            for lead in lead_range
        ]
    by_lead: dict[int, dict[str, dict[str, float | None | str | bool]]] = {}
    # NOTE: stats_for_region fans out its per-stat reads on _EXECUTOR, so these
    # per-lead tasks must run on the separate _LEADS_EXECUTOR (see pool invariant
    # at the top of this module) or concurrent requests deadlock the shared pool.
    future_to_lead = {
        _LEADS_EXECUTOR.submit(
            stats_for_region,
            store=resolved_store,
            model=model,
            lead=lead,
            region=region,
            stat_names=stat_names,
            period=period,
            month=month,
            season=season,
            threshold=threshold,
            scale=scale,
        ): lead
        for lead in lead_range
    }
    for fut in as_completed(future_to_lead):
        lead = future_to_lead[fut]
        by_lead[lead] = fut.result()
    return [{"lead": lead, "stats": by_lead[lead]} for lead in lead_range]
