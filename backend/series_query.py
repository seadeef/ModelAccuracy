"""On-demand daily forecast/observation series for any region.

Reads the per-cell cube written by ``stats_pipeline.cube`` (exported to
``cube/{model}/`` under the static root) and pools it over the cells whose
centres fall inside the requested region — the accuracy pane's masking idiom,
applied to the date axis. The result has the same shape as the exported
per-basin JSON (``export_static.export_basin_revisions``), so every client-side
basin analysis (revision paths, R, heavy-rain triggers, day winners) runs
unchanged on a state, a HUC2, a county or a drawn shape.

Differences from the canonical basin export, stated in the payload:

* ``source: "cube"`` and equal cell weights (the basin stage weights by
  fractional intersection area);
* sub-cell regions fall back to the nearest land cell (``nearest_cell: true``)
  instead of answering no_data — the series is then that one cell's history;
* no lock-in leads (the dashboard does not read them).

Local stores memory-map the arrays, so a request touches only the region's
cells. Other stores load the arrays once into RAM.
"""

from __future__ import annotations

import io
import json
import threading
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from backend import admin_boundaries, military_boundaries, wbd_boundaries
from backend.static_store import LocalStaticStore, StaticStore
from backend.stats_query import _build_region_mask, _get_grid, _huc_region_mask
from model_registry import MODEL_REGISTRY

CUBE_DIR = "cube"
# How far (in grid steps, Chebyshev) the nearest-land-cell fallback may reach
# from a sub-cell region's centre before the answer is honestly empty.
NEAREST_CELL_MAX_STEPS = 1.5


@dataclass
class RegionCube:
    model: str
    meta: dict
    cells: np.ndarray          # int32 flat indices of land cells
    cell_pos: np.ndarray       # flat index → row in the arrays (-1 = not land)
    forecasts: np.ndarray      # float16 (cells, dates, leads), memmap or array
    observed: np.ndarray       # float16 (cells, dates)
    lats: np.ndarray
    lons: np.ndarray
    n_lat: int
    n_lon: int


_cube_cache: dict[tuple[str, str], RegionCube | None] = {}
_cube_lock = threading.Lock()


def _load_array(store: StaticStore, relative_path: str) -> np.ndarray:
    if isinstance(store, LocalStaticStore):
        return np.load(store.root / relative_path, mmap_mode="r")
    return np.load(io.BytesIO(store.read_bytes(relative_path)), allow_pickle=False)


def cube_available(store: StaticStore, model: str) -> bool:
    return store.exists(f"{CUBE_DIR}/{model}/meta.json")


def get_cube(store: StaticStore, model: str) -> RegionCube | None:
    """The model's cube, or None when it has not been built/exported."""
    key = (store.cache_key, model)
    with _cube_lock:
        if key in _cube_cache:
            return _cube_cache[key]
    cube: RegionCube | None = None
    base = f"{CUBE_DIR}/{model}"
    if store.exists(f"{base}/meta.json"):
        meta = json.loads(store.read_text(f"{base}/meta.json"))
        lats, lons, n_lat, n_lon = _get_grid(store, model)
        if (int(meta["n_lat"]), int(meta["n_lon"])) != (n_lat, n_lon):
            raise RuntimeError(f"{model}: cube grid {meta['n_lat']}×{meta['n_lon']} "
                               f"does not match grid.json {n_lat}×{n_lon}")
        cells = np.asarray(_load_array(store, f"{base}/cells.npy"), dtype=np.int32)
        cell_pos = np.full(n_lat * n_lon, -1, dtype=np.int32)
        cell_pos[cells] = np.arange(cells.size, dtype=np.int32)
        cube = RegionCube(
            model=model, meta=meta, cells=cells, cell_pos=cell_pos,
            forecasts=_load_array(store, f"{base}/forecasts.npy"),
            observed=_load_array(store, f"{base}/observed.npy"),
            lats=lats, lons=lons, n_lat=n_lat, n_lon=n_lon,
        )
    with _cube_lock:
        # Cache misses too, but only while the file is absent: re-probe cheaply
        # on the next call so a cube exported after startup is picked up.
        if cube is not None:
            _cube_cache[key] = cube
    return cube


# ---------------------------------------------------------------------------
# Region → cell rows
# ---------------------------------------------------------------------------

def _region_centre(region: dict) -> tuple[float, float] | None:
    """(lon, lat) at the middle of the region's bounding box, or None."""
    kind = region.get("type")
    if kind == "point":
        c = region.get("coordinates") or []
        return (float(c[0]), float(c[1])) if len(c) >= 2 else None
    if kind == "rectangle":
        b = region.get("bounds") or []
        return ((float(b[0]) + float(b[2])) / 2, (float(b[1]) + float(b[3])) / 2) if len(b) == 4 else None
    if kind == "polygon":
        pts = region.get("coordinates") or []
        if not pts:
            return None
        xs = [float(p[0]) for p in pts]
        ys = [float(p[1]) for p in pts]
        return ((min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2)
    entry = None
    try:
        if kind == "admin":
            entry = admin_boundaries.get_entry(str(region.get("level") or ""), str(region.get("fips") or ""))
        elif kind == "huc":
            entry = wbd_boundaries.get_entry(str(region.get("huc") or ""))
        elif kind == "military":
            entry = military_boundaries.get_entry(str(region.get("site") or ""))
    except Exception:  # unavailable boundaries → no fallback centre
        entry = None
    if not entry:
        return None
    bbox = entry.get("bbox") or []
    if len(bbox) == 4:
        return ((float(bbox[0]) + float(bbox[2])) / 2, (float(bbox[1]) + float(bbox[3])) / 2)
    xs: list[float] = []
    ys: list[float] = []
    for poly in entry.get("polygons") or []:
        for p in (poly[0] if poly else []):
            xs.append(float(p[0]))
            ys.append(float(p[1]))
    if not xs:
        return None
    return ((min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2)


def _nearest_land_row(cube: RegionCube, lon: float, lat: float) -> int | None:
    """Row of the land cell nearest (lon, lat), within NEAREST_CELL_MAX_STEPS."""
    lat_step = float(abs(cube.lats[1] - cube.lats[0])) if cube.lats.size > 1 else 1.0
    lon_step = float(abs(cube.lons[1] - cube.lons[0])) if cube.lons.size > 1 else 1.0
    rows = cube.cells // cube.n_lon
    cols = cube.cells % cube.n_lon
    dy = np.abs(cube.lats[rows] - lat) / lat_step
    dx = np.abs(cube.lons[cols] - lon) / lon_step
    dist = np.maximum(dy, dx)
    i = int(np.argmin(dist))
    return i if dist[i] <= NEAREST_CELL_MAX_STEPS else None


def region_rows(cube: RegionCube, region: dict) -> tuple[np.ndarray, bool]:
    """Rows into the cube arrays for a region, plus whether the nearest-cell
    fallback was used. Empty when the region is off-grid or all water.

    The mask is the stats endpoints' own (``_build_region_mask``, including
    its sub-cell bbox fallback) intersected with the land cells. A region
    whose cells are all water (a coastal HUC12 whose containing cell is
    ocean) or a point takes the nearest land cell within
    ``NEAREST_CELL_MAX_STEPS`` of its centre, flagged ``nearest_cell``.
    """
    if region.get("type") != "point":
        mask = _build_region_mask(cube.lats, cube.lons, cube.n_lat, cube.n_lon, region)
        rows = cube.cell_pos[np.flatnonzero(mask)]
        rows = rows[rows >= 0]
        if rows.size:
            return np.sort(rows), False
    centre = _region_centre(region)
    if centre is None:
        return np.zeros(0, dtype=np.int64), False
    row = _nearest_land_row(cube, *centre)
    return (np.array([row]) if row is not None else np.zeros(0, dtype=np.int64)), True


# ---------------------------------------------------------------------------
# Catalogued AR basins touching a region
# ---------------------------------------------------------------------------

BASINS_INDEX_PATH = "ar/basins.json"
_basins_index_cache: dict[str, list[str]] = {}


def catalogued_basins(store: StaticStore) -> list[str]:
    """HUC6 codes of the exported AR basins (``ar/basins.json``), or [] when
    the AR export is absent."""
    key = store.cache_key
    hit = _basins_index_cache.get(key)
    if hit is None:
        try:
            doc = json.loads(store.read_text(BASINS_INDEX_PATH))
            hit = sorted(str(h) for h in (doc.get("basins") or {}))
        except Exception:
            hit = []
        _basins_index_cache[key] = hit
    return hit


def _grid_for_any_model(store: StaticStore) -> tuple[np.ndarray, np.ndarray, int, int] | None:
    for model in MODEL_REGISTRY:
        try:
            return _get_grid(store, model)
        except Exception:
            continue
    return None


def _containing_cell_mask(lats: np.ndarray, lons: np.ndarray, n_lat: int, n_lon: int,
                          lon: float, lat: float) -> np.ndarray:
    """Mask of the one cell whose centre is nearest (lon, lat), within
    NEAREST_CELL_MAX_STEPS; empty beyond that (an off-grid region)."""
    mask = np.zeros(n_lat * n_lon, dtype=bool)
    if not lats.size or not lons.size:
        return mask
    lat_step = float(abs(lats[1] - lats[0])) if lats.size > 1 else 1.0
    lon_step = float(abs(lons[1] - lons[0])) if lons.size > 1 else 1.0
    i = int(np.argmin(np.abs(lats - lat)))
    j = int(np.argmin(np.abs(lons - lon)))
    if abs(lats[i] - lat) / lat_step <= NEAREST_CELL_MAX_STEPS and abs(lons[j] - lon) / lon_step <= NEAREST_CELL_MAX_STEPS:
        mask[i * n_lon + j] = True
    return mask


def basins_for_region(store: StaticStore, region: dict) -> list[str]:
    """The catalogued AR basins (HUC6) a region touches, by grid cell.

    A HUC region answers by code: its own HUC6 ancestor, or every catalogued
    basin inside a HUC2/HUC4. Any other region takes the stats mask
    (``_build_region_mask``; a sub-cell region falls back to its containing
    cell, as ``region_rows`` does) and keeps the basins whose own mask shares
    a cell with it. The storm pickers filter the curated storm catalogs
    (``ar/storms/{huc2}.json``, ``basin_hucs``) by this list, so a state, a
    county or a drawn shape lists only the storms that reached it.
    """
    hucs = catalogued_basins(store)
    if not hucs:
        return []
    if region.get("type") == "huc":
        code = str(region.get("huc") or "")
        if not code:
            return []
        return [h for h in hucs if h.startswith(code) or code.startswith(h)]
    grid = _grid_for_any_model(store)
    if grid is None:
        return []
    lats, lons, n_lat, n_lon = grid
    mask = _build_region_mask(lats, lons, n_lat, n_lon, region)
    if not np.any(mask):
        centre = _region_centre(region)
        if centre is None:
            return []
        mask = _containing_cell_mask(lats, lons, n_lat, n_lon, *centre)
        if not np.any(mask):
            return []
    # Bounding box of the region's cells (padded a step) prunes the basins
    # whose polygon cannot overlap before any of them is rasterised.
    idx = np.flatnonzero(mask)
    cell_lats = lats[idx // n_lon]
    cell_lons = lons[idx % n_lon]
    lat_step = float(abs(lats[1] - lats[0])) if lats.size > 1 else 1.0
    lon_step = float(abs(lons[1] - lons[0])) if lons.size > 1 else 1.0
    south, north = float(cell_lats.min()) - lat_step, float(cell_lats.max()) + lat_step
    west, east = float(cell_lons.min()) - lon_step, float(cell_lons.max()) + lon_step
    out: list[str] = []
    for huc in hucs:
        try:
            bbox = wbd_boundaries.get_entry(huc).get("bbox") or []
        except Exception:
            continue
        if len(bbox) == 4 and (bbox[2] < west or bbox[0] > east or bbox[3] < south or bbox[1] > north):
            continue
        if np.any(mask & _huc_region_mask(lats, lons, n_lat, n_lon, huc)):
            out.append(huc)
    return out


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------

def region_series_id(region: dict) -> str:
    """Stable string id for a region (seeds the client's bootstrap draws)."""
    kind = region.get("type")
    if kind == "huc":
        return f"huc:{region.get('huc')}"
    if kind == "admin":
        return f"admin:{region.get('level')}:{region.get('fips')}"
    if kind == "military":
        return f"mil:{region.get('site')}"
    if kind == "point":
        c = region.get("coordinates") or []
        return "pt:" + ",".join(f"{float(v):.4f}" for v in c[:2])
    if kind == "rectangle":
        return "rect:" + ",".join(f"{float(v):.4f}" for v in (region.get("bounds") or []))
    if kind == "polygon":
        pts = region.get("coordinates") or []
        return "poly:" + ";".join(f"{float(p[0]):.4f},{float(p[1]):.4f}" for p in pts)
    return json.dumps(region, sort_keys=True)


def _round_list(values: np.ndarray, ndigits: int) -> list[float | None]:
    out: list[float | None] = []
    for v in values.tolist():
        out.append(None if v is None or not np.isfinite(v) else round(float(v), ndigits))
    return out


def series_for_region(store: StaticStore, model: str, region: dict) -> dict | None:
    """Pooled daily series for ``region`` from ``model``'s cube, or None when the
    cube is absent or the region covers no land cell (even by fallback)."""
    cube = get_cube(store, model)
    if cube is None:
        return None
    rows, nearest = region_rows(cube, region)
    if rows.size == 0:
        return None
    # Fancy-index the memmap once per array: only the region's cell rows are read.
    fc = np.asarray(cube.forecasts[rows], dtype=np.float32)   # (k, D, L)
    ob = np.asarray(cube.observed[rows], dtype=np.float32)    # (k, D)
    with np.errstate(invalid="ignore"):
        fc_mean = np.nanmean(fc, axis=0) if rows.size > 1 else fc[0]
        ob_mean = np.nanmean(ob, axis=0) if rows.size > 1 else ob[0]
    meta = cube.meta
    n_dates = len(meta["dates"])
    return {
        "huc": region_series_id(region),
        "huc2": None,
        "region": None,
        "name": None,
        "model": model,
        "source": "cube",
        "cells": int(rows.size),
        "nearest_cell": bool(nearest),
        "weighting": meta.get("weighting"),
        "lead_min": int(meta["lead_min"]),
        "lead_max": int(meta["lead_max"]),
        "primary_window": meta.get("primary_window"),
        "primary_valid_range": meta.get("primary_valid_range") or [None, None],
        "dates": list(meta["dates"]),
        "forecasts": [_round_list(fc_mean[di], 2) for di in range(n_dates)],
        "observed": _round_list(ob_mean, 2),
        "lock": None,
        "lock_in_fraction": None,
    }


# ---------------------------------------------------------------------------
# Heavy-rain trigger sweep pooled over CELL-DAYS
#
# The browser's ``triggerSkill`` judges the region-MEAN series: did the mean
# over the region cross q. That is the right event for a river basin (a daily
# basin total) but degenerates for a state, where the mean over hundreds of
# cells almost never reaches 25 mm and the precision–recall curve collapses
# to a few steps. Here every (cell, day) is its own trial: event = PRISM ≥ q
# in that cell on that day, call = the model's forecast for that cell at the
# lead ≥ trigger t. Pooled over the region's cells this is the per-cell
# verification idiom the accuracy maps use, with hundreds of thousands of
# trials for a state. Output mirrors ``triggerSkill`` field for field.
# ---------------------------------------------------------------------------

TRIGGER_GRID_MM: tuple[float, ...] = tuple(
    [t / 2 for t in range(0, 61)] + [float(t) for t in range(31, 81)])


def _average_precision(precision: np.ndarray, recall: np.ndarray) -> float:
    """Step-function AP over descending trigger (ascending recall), as the client."""
    ap = 0.0
    prev = 0.0
    for i in range(precision.size - 1, -1, -1):
        ap += (float(recall[i]) - prev) * float(precision[i])
        prev = float(recall[i])
    return ap


def heavy_rain_for_region(
    store: StaticStore, model: str, region: dict, *, q: float, lead: int,
    dates: list[str] | None = None,
) -> dict | None:
    """Precision/recall/F1/frequency bias per trigger for ``model`` at ``lead``,
    pooled over the region's cell-days (``dates`` = YYYYMMDD subset, default
    every date in the cube). None when the cube is absent or the region covers
    no land cell; ``{"unavailable": True}`` when the model has no such lead."""
    cube = get_cube(store, model)
    if cube is None:
        return None
    rows, nearest = region_rows(cube, region)
    if rows.size == 0:
        return None
    meta = cube.meta
    lead_min, lead_max = int(meta["lead_min"]), int(meta["lead_max"])
    if not lead_min <= lead <= lead_max:
        return {"unavailable": True, "lead": lead, "model": model}
    li = lead - lead_min
    all_dates = meta["dates"]
    if dates is None:
        di = np.arange(len(all_dates))
    else:
        wanted = set(dates)
        di = np.fromiter((i for i, d in enumerate(all_dates) if d in wanted), dtype=np.int64)
    if di.size == 0:
        return {"unavailable": False, "model": model, "q": q, "lead": lead, "dates": 0, "cellDays": 0,
                "nObs": 0, "baseRate": None, "triggers": list(TRIGGER_GRID_MM),
                "precision": [1.0] * len(TRIGGER_GRID_MM), "recall": [0.0] * len(TRIGGER_GRID_MM),
                "f1": [0.0] * len(TRIGGER_GRID_MM), "bias": [None] * len(TRIGGER_GRID_MM),
                "ap": None, "point": None, "bestF1": None, "cells": int(rows.size), "pooling": "cell_days"}
    # (k cells, n dates) at one lead; only finite pairs are trials.
    fc = np.asarray(cube.forecasts[rows][:, di, li], dtype=np.float32).ravel()
    ob = np.asarray(cube.observed[rows][:, di], dtype=np.float32).ravel()
    ok = np.isfinite(fc) & np.isfinite(ob)
    fc, ob = fc[ok], ob[ok]
    event = ob >= q
    n_obs = int(event.sum())
    n = int(fc.size)
    # Sort forecasts once; calls at trigger t = forecasts ≥ t, counted by
    # searchsorted, hits via the cumulative event count in sorted order.
    order = np.argsort(fc, kind="stable")
    fc_sorted = fc[order]
    ev_sorted = event[order].astype(np.int64)
    ev_cum_from_right = np.concatenate([np.cumsum(ev_sorted[::-1])[::-1], [0]])
    triggers = np.asarray(TRIGGER_GRID_MM, dtype=np.float32)
    first_ge = np.searchsorted(fc_sorted, triggers, side="left")
    calls = (n - first_ge).astype(np.float64)
    hits = ev_cum_from_right[first_ge].astype(np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        precision = np.where(calls > 0, hits / calls, 1.0)
        recall = np.where(n_obs > 0, hits / max(n_obs, 1), 0.0)
        f1 = np.where((calls + n_obs) > 0, 2 * hits / np.maximum(calls + n_obs, 1), 0.0)
        bias = calls / n_obs if n_obs > 0 else np.full_like(calls, np.nan)
    at = int(np.argmin(np.abs(triggers - q)))
    point = None
    if abs(float(triggers[at]) - q) < 1e-6:
        point = {
            "trigger": q, "hits": int(hits[at]), "misses": int(n_obs - hits[at]),
            "falseAlarms": int(calls[at] - hits[at]),
            "precision": float(precision[at]), "recall": float(recall[at]), "f1": float(f1[at]),
            "frequencyBias": (None if n_obs == 0 else float(bias[at])),
        }
    best = int(np.argmax(f1))
    return {
        "unavailable": False, "model": model, "q": q, "lead": lead,
        "dates": int(di.size), "cellDays": n, "nObs": n_obs,
        "baseRate": (n_obs / n) if n else None,
        "triggers": list(TRIGGER_GRID_MM),
        "precision": [round(float(v), 5) for v in precision],
        "recall": [round(float(v), 5) for v in recall],
        "f1": [round(float(v), 5) for v in f1],
        "bias": [None if not np.isfinite(v) else round(float(v), 4) for v in bias],
        "ap": _average_precision(precision, recall) if n_obs > 0 else None,
        "point": point,
        "bestF1": {"trigger": float(triggers[best]), "f1": float(f1[best]),
                   "precision": float(precision[best]), "recall": float(recall[best])},
        "cells": int(rows.size), "nearest_cell": bool(nearest), "pooling": "cell_days",
    }


def heavy_rain_for_region_all_models(
    store: StaticStore, region: dict, *, q: float, lead: int,
    dates: list[str] | None = None, models: list[str] | None = None,
) -> dict:
    keys = models or list(MODEL_REGISTRY)
    return {
        "pooling": "cell_days",
        "models": {k: heavy_rain_for_region(store, k, region, q=q, lead=lead, dates=dates) for k in keys},
        "available": [k for k in keys if cube_available(store, k)],
    }


def series_for_region_all_models(
    store: StaticStore, region: dict, models: list[str] | None = None,
) -> dict:
    keys = models or list(MODEL_REGISTRY)
    out: dict[str, dict | None] = {}
    for key in keys:
        out[key] = series_for_region(store, key, region)
    return {
        "source": "cube",
        "models": out,
        "available": [k for k in keys if cube_available(store, k)],
    }
