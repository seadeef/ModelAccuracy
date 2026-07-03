#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import numpy as np

from backend import admin_boundaries, wbd_boundaries
from backend.static_store import LocalStaticStore, StaticStore, default_static_site_root
from model_registry import MODEL_REGISTRY
from forecast_variables import FORECAST_VARIABLES
from statistics_plugins.registry import STATISTICS_BY_NAME

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


# Shared thread pool for every I/O fan-out (bin reads, per-(model, lead)
# losses). One module-level pool bounds total worker threads across concurrent
# requests instead of each request building its own — an unauthenticated caller
# can no longer multiply Fargate's thread count by fanning out. Sized to overlap
# S3 latency and to match the S3 client's connection pool (see static_store).
_EXECUTOR_WORKERS = 16
_EXECUTOR = ThreadPoolExecutor(max_workers=_EXECUTOR_WORKERS, thread_name_prefix="stats")

_grid_cache: dict[tuple[str, str], tuple[np.ndarray, np.ndarray, int, int]] = {}
# Verification stat bins (~156 KB each).  Bounded LRU so a long-lived Fargate
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


def _point_in_polygon(lon: float, lat: float, ring: list[list[float]]) -> bool:
    if len(ring) < 3:
        return False
    inside = False
    j = len(ring) - 1
    for i, (xi, yi) in enumerate(ring):
        xj, yj = ring[j]
        if (yi > lat) != (yj > lat):
            x_int = xi + ((xj - xi) * (lat - yi)) / (yj - yi)
            if lon < x_int:
                inside = not inside
        j = i
    return inside


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


def _polygon_mask_centers(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    ring: list[list[float]],
) -> np.ndarray:
    mask = np.zeros(n_lat * n_lon, dtype=bool)
    lons_ring = [p[0] for p in ring]
    lats_ring = [p[1] for p in ring]
    min_lon = min(lons_ring)
    max_lon = max(lons_ring)
    min_lat = min(lats_ring)
    max_lat = max(lats_ring)
    for r, lat in enumerate(lats):
        if lat < min_lat or lat > max_lat:
            continue
        base = r * n_lon
        for c, lon in enumerate(lons):
            if lon < min_lon or lon > max_lon:
                continue
            if _point_in_polygon(float(lon), float(lat), ring):
                mask[base + c] = True
    return mask


def _polygon_mask_centers_or_corners(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    ring: list[list[float]],
) -> np.ndarray:
    """Include cells whose center or any corner lies inside the polygon."""
    lat_low, lat_high = _cell_axis_bounds(lats)
    lon_low, lon_high = _cell_axis_bounds(lons)
    mask = np.zeros(n_lat * n_lon, dtype=bool)
    lons_ring = [p[0] for p in ring]
    lats_ring = [p[1] for p in ring]
    min_lon = min(lons_ring)
    max_lon = max(lons_ring)
    min_lat = min(lats_ring)
    max_lat = max(lats_ring)
    corners_template = ((0, 0), (1, 0), (1, 1), (0, 1))

    for r in range(n_lat):
        if lat_high[r] < min_lat or lat_low[r] > max_lat:
            continue
        cy = float(lats[r])
        y_lo, y_hi = float(lat_low[r]), float(lat_high[r])
        base = r * n_lon
        for c in range(n_lon):
            if lon_high[c] < min_lon or lon_low[c] > max_lon:
                continue
            cx = float(lons[c])
            x_lo, x_hi = float(lon_low[c]), float(lon_high[c])
            if _point_in_polygon(cx, cy, ring):
                mask[base + c] = True
                continue
            for ix, iy in corners_template:
                x = x_lo if ix == 0 else x_hi
                y = y_lo if iy == 0 else y_hi
                if _point_in_polygon(x, y, ring):
                    mask[base + c] = True
                    break
    return mask


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
    cache_key = (id(lats), id(lons), level, fips)
    cached = _admin_mask_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        entry = admin_boundaries.get_entry(level, fips)
    except (KeyError, admin_boundaries.AdminBoundariesUnavailable):
        empty = np.zeros(n_lat * n_lon, dtype=bool)
        _admin_mask_cache.put(cache_key, empty)
        return empty
    polygons = entry.get("polygons") or []
    mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=False)
    if not np.any(mask):
        mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=True)
    # No centroid fallback: a region that covers no grid cell (too small, or off
    # the grid) is honest no_data — not the nearest edge cell dressed up as the
    # region's value.
    _admin_mask_cache.put(cache_key, mask)
    return mask


def _huc_region_mask(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    huc: str,
) -> np.ndarray:
    cache_key = (id(lats), id(lons), huc)
    cached = _huc_mask_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        entry = wbd_boundaries.get_entry(huc)
    except (KeyError, ValueError, wbd_boundaries.WbdBoundariesUnavailable):
        empty = np.zeros(n_lat * n_lon, dtype=bool)
        _huc_mask_cache.put(cache_key, empty)
        return empty
    polygons = entry.get("polygons") or []
    mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=False)
    if not np.any(mask):
        mask = _multi_polygon_mask(lats, lons, n_lat, n_lon, polygons, use_corners=True)
    # No centroid fallback (see _admin_region_mask): no covered cell → no_data.
    _huc_mask_cache.put(cache_key, mask)
    return mask


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
        cached = _geom_mask_cache.get(geom_key)
        if cached is not None:
            return cached
        # No nearest-cell fabrication: a rectangle covering no grid cell is no_data.
        mask = _mask_rectangle_overlap(lats, lons, west, south, east, north)
        _geom_mask_cache.put(geom_key, mask)
        return mask

    if region_type == "polygon":
        ring = [[float(p[0]), float(p[1])] for p in region.get("coordinates", [])]
        if len(ring) < 3:
            return mask
        geom_key = (id(lats), id(lons), "poly", tuple((p[0], p[1]) for p in ring))
        cached = _geom_mask_cache.get(geom_key)
        if cached is not None:
            return cached
        mask = _polygon_mask_centers(lats, lons, n_lat, n_lon, ring)
        if not np.any(mask):
            mask = _polygon_mask_centers_or_corners(lats, lons, n_lat, n_lon, ring)
        # No nearest-cell fabrication: a polygon covering no grid cell is no_data.
        _geom_mask_cache.put(geom_key, mask)
        return mask

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
) -> dict[str, dict[str, float | None | str | bool]]:
    lead_key = str(lead)
    stat_names = stat_names or list(STATISTICS_BY_NAME)
    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    lats, lons, n_lat, n_lon = _get_grid(resolved_store, model)
    region_type = region.get("type")
    result: dict[str, dict[str, float | None | str | bool]] = {}

    if region_type == "point":
        lon = float(region["coordinates"][0])
        lat = float(region["coordinates"][1])
        lat_idx = int(np.argmin(np.abs(lats - lat)))
        lon_idx = int(np.argmin(np.abs(lons - lon)))
        # Honest no-data for an off-grid click: if the nearest cell center is more
        # than ~one grid step away on either axis, the point isn't inside any cell,
        # so don't report that edge cell as though it were the point's value.
        lat_step = float(abs(lats[1] - lats[0])) if lats.size > 1 else float("inf")
        lon_step = float(abs(lons[1] - lons[0])) if lons.size > 1 else float("inf")
        if (abs(float(lats[lat_idx]) - lat) > lat_step
                or abs(float(lons[lon_idx]) - lon) > lon_step):
            return {stat_name: _no_data_entry(stat_name) for stat_name in stat_names}
        flat_idx = lat_idx * n_lon + lon_idx
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
        return result

    mask = _build_region_mask(lats, lons, n_lat, n_lon, region)
    if not np.any(mask):
        return {stat_name: _no_data_entry(stat_name) for stat_name in stat_names}

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

    return result


def _winner_metric_label(stat_name: str) -> str:
    """Human-readable description of how models are ranked for this statistic."""
    if stat_name == "bias":
        # O5: rank by |regional signed-mean bias| so the ranking matches the
        # signed mean shown in the panel (not the mean of per-cell |bias|).
        return "region_abs_mean_value_lower_better"
    if stat_name == "cc":
        return "region_mean_value_higher_better"
    return "region_mean_value_lower_better"


def _loss_from_value(v: float, stat_name: str) -> float | None:
    """Per-cell loss (lower is better): abs for bias, negated for CC, else identity."""
    if not np.isfinite(v):
        return None
    if stat_name == "bias":
        return float(abs(v))
    if stat_name == "cc":
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
    Scalar loss for winner selection; **lower is better**. Bias ranks by the
    magnitude of the region's signed-mean bias (|mean|, O5), so the winner
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
    if stat_name == "bias":
        return float(abs(np.mean(finite)))
    if stat_name == "cc":
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
    ``_region_loss_scalar``: |signed mean| for bias, negated mean for CC, plain
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
                ),
            }
            for lead in lead_range
        ]
    by_lead: dict[int, dict[str, dict[str, float | None | str | bool]]] = {}
    future_to_lead = {
        _EXECUTOR.submit(
            stats_for_region,
            store=resolved_store,
            model=model,
            lead=lead,
            region=region,
            stat_names=stat_names,
            period=period,
            month=month,
            season=season,
        ): lead
        for lead in lead_range
    }
    for fut in as_completed(future_to_lead):
        lead = future_to_lead[fut]
        by_lead[lead] = fut.result()
    return [{"lead": lead, "stats": by_lead[lead]} for lead in lead_range]
