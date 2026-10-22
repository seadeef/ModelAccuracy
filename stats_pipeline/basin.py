#!/usr/bin/env python3
"""Record daily raw precipitation histories for a HUC2 region's HUC6 basins.

The ordinary consistency accumulators intentionally discard the per-date,
per-basin forecast sequence. Revision efficiency cannot be reconstructed from
them, so this producer reads each scorable valid date once and stores basin-
mean F_1..F_max plus the matching PRISM basin mean.

The region is a parameter: California
(HUC2 ``18``) is the default and keeps the historical artifact name; the
Pacific Northwest (HUC2 ``17``) is the declared inferential replication
region, and dry-interior regions are available descriptively. Nothing about the
estimands, weighting, or completeness rules changes with the region.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
from shapely.geometry import Polygon, box
from shapely.ops import unary_union

from stats_pipeline.episode_metrics import (
    BASIN_HISTORY_FILENAME,  # noqa: F401  (re-exported for existing importers)
    CALIFORNIA_HUC2,
    HUC_LEVEL,
    basin_history_filename,
    daily_lock_in,
)
from boundary_paths import WBD_LAYERS_DIR
from stats_pipeline.consistency import prism_path, scan_inits, scorable_dates
from stats_pipeline.verification import grid_coords_from_transform, read_prism
from model_registry import MODEL_REGISTRY
from statistics_plugins.consistency import LOCK_IN_FRACTION
from stats_pipeline.io import atomic_savez


# v3: gained the version_windows metadata block (W1). The history itself is
# deliberately full-record — window filtering happens in pooled consumers.
SCHEMA_VERSION = 3
_EARTH_RADIUS_M = 6_371_008.8


def load_basins(huc2: str = CALIFORNIA_HUC2) -> list[tuple[str, str, object]]:
    """The HUC6 basins beneath one HUC2 region, ordered by HUC code."""
    path = WBD_LAYERS_DIR / f"hu{HUC_LEVEL}.json"
    with open(path) as f:
        units = json.load(f)["units"]
    result = []
    for huc, entry in sorted(units.items()):
        if not huc.startswith(huc2):
            continue
        geometries = []
        for polygon in entry.get("polygons", []):
            if not polygon:
                continue
            geometries.append(Polygon(polygon[0], polygon[1:]))
        if geometries:
            result.append((huc, entry.get("name", ""), unary_union(geometries)))
    if not result:
        raise RuntimeError(
            f"no HUC{HUC_LEVEL} basins found beneath HUC2 {huc2}")
    return result


def _cell_edges(centers: np.ndarray) -> np.ndarray:
    centers = np.asarray(centers, dtype=np.float64)
    if centers.size < 2:
        raise ValueError("grid needs at least two centers per axis")
    edges = np.empty(centers.size + 1, dtype=np.float64)
    edges[1:-1] = (centers[:-1] + centers[1:]) / 2.0
    edges[0] = centers[0] - (centers[1] - centers[0]) / 2.0
    edges[-1] = centers[-1] + (centers[-1] - centers[-2]) / 2.0
    return edges


def _area_m2(geometry) -> float:
    """Spherical area of a lon/lat Shapely geometry (holes subtracted)."""
    if geometry.is_empty:
        return 0.0
    if geometry.geom_type in {"MultiPolygon", "GeometryCollection"}:
        return sum(_area_m2(part) for part in geometry.geoms)
    if geometry.geom_type != "Polygon":
        return 0.0

    def ring_area(ring) -> float:
        coords = list(ring.coords)
        total = 0.0
        for (lon1, lat1), (lon2, lat2) in zip(coords, coords[1:]):
            dlon = math.radians(lon2 - lon1)
            total += dlon * (math.sin(math.radians(lat1)) + math.sin(math.radians(lat2)))
        return abs(total) * (_EARTH_RADIUS_M ** 2) / 2.0

    return max(0.0, ring_area(geometry.exterior)
               - sum(ring_area(ring) for ring in geometry.interiors))


def basin_weights(
    lats: np.ndarray, lons: np.ndarray, basins: list[tuple[str, str, object]],
) -> tuple[np.ndarray, np.ndarray]:
    """Fractional-cell geodesic-area weights, returned as dense (B,cells)."""
    lat_edges = _cell_edges(lats)
    lon_edges = _cell_edges(lons)
    weights = np.zeros((len(basins), lats.size * lons.size), dtype=np.float64)
    for bi, (huc, _, geom) in enumerate(basins):
        west, south, east, north = geom.bounds
        rows = np.flatnonzero(
            (np.maximum(lat_edges[:-1], lat_edges[1:]) >= south)
            & (np.minimum(lat_edges[:-1], lat_edges[1:]) <= north)
        )
        cols = np.flatnonzero(
            (np.maximum(lon_edges[:-1], lon_edges[1:]) >= west)
            & (np.minimum(lon_edges[:-1], lon_edges[1:]) <= east)
        )
        for ri in rows:
            y0, y1 = sorted((lat_edges[ri], lat_edges[ri + 1]))
            for ci in cols:
                x0, x1 = sorted((lon_edges[ci], lon_edges[ci + 1]))
                intersection = geom.intersection(box(x0, y0, x1, y1))
                if not intersection.is_empty:
                    weights[bi, ri * lons.size + ci] = _area_m2(intersection)
        total = float(weights[bi].sum())
        if total <= 0.0:
            raise RuntimeError(f"HUC6 {huc} has no overlap with the model grid")
        weights[bi] /= total
    return weights, np.count_nonzero(weights, axis=1).astype(np.int32)


def weighted_means(field: np.ndarray, weights: np.ndarray) -> np.ndarray:
    flat = np.asarray(field, dtype=np.float64).reshape(-1)
    finite = np.isfinite(flat)
    numer = weights[:, finite] @ flat[finite]
    denom = weights[:, finite].sum(axis=1)
    return np.divide(numer, denom, out=np.full(weights.shape[0], np.nan), where=denom > 0)


def run_basin_stage(model_key: str, force: bool = False,
                    huc2: str = CALIFORNIA_HUC2) -> Path:
    """Build (or refresh) one model's per-date basin history for a HUC2 region."""
    config = MODEL_REGISTRY[model_key]
    root = Path("stats_output") / model_key
    root.mkdir(parents=True, exist_ok=True)
    out = root / basin_history_filename(huc2)
    basins = load_basins(huc2)
    lats = np.load(Path(config.data_dir) / "grid_lats.npy")
    lons = np.load(Path(config.data_dir) / "grid_lons.npy")
    inits = scan_inits(Path(config.data_dir), config.cycle_hour,
                        config.lead_days_min, config.lead_days_max)
    scorable = scorable_dates(inits, config.lead_days_min, config.lead_days_max)
    dates = np.array([int(d.strftime("%Y%m%d")) for d in sorted(scorable)], dtype=np.int64)

    if out.exists() and not force:
        try:
            with np.load(out) as old:
                if (int(old["schema_version"]) == SCHEMA_VERSION
                        and np.array_equal(old["dates"], dates)
                        and int(old["lead_min"]) == config.lead_days_min
                        and int(old["lead_max"]) == config.lead_days_max):
                    print(f"  {model_key}: basin history is up to date")
                    return out
        except (OSError, KeyError, ValueError):
            pass

    print(f"  {model_key}: building weights for {len(basins)} HUC6 basins "
          f"(HUC2 {huc2})")
    weights, cell_counts = basin_weights(lats, lons, basins)
    n_dates = len(dates)
    n_leads = config.lead_days_max - config.lead_days_min + 1
    forecasts = np.full((n_dates, n_leads, len(basins)), np.nan, dtype=np.float32)
    observed = np.full((n_dates, len(basins)), np.nan, dtype=np.float32)
    first_date = min(scorable)
    prism_sample, prism_transform, prism_crs = read_prism(prism_path(first_date))
    if "4269" not in prism_crs and "4326" not in prism_crs:
        raise RuntimeError(f"expected a geographic PRISM grid, got {prism_crs}")
    prism_lats, prism_lons = grid_coords_from_transform(
        prism_transform, *prism_sample.shape)
    print(f"  {model_key}: building native-PRISM basin weights")
    prism_weights, _ = basin_weights(prism_lats, prism_lons, basins)

    for di, valid_date in enumerate(sorted(scorable)):
        for li, lead in enumerate(range(config.lead_days_min, config.lead_days_max + 1)):
            forecasts[di, li] = weighted_means(np.load(scorable[valid_date][lead]), weights)
        prism_data, _, _ = read_prism(prism_path(valid_date))
        observed[di] = weighted_means(prism_data, prism_weights)
        if (di + 1) % 50 == 0:
            print(f"    ...{di + 1}/{n_dates} dates")

    stab, acc, gap = daily_lock_in(
        forecasts, observed, config.lead_days_min, LOCK_IN_FRACTION)
    atomic_savez(
        out,
        schema_version=np.int64(SCHEMA_VERSION),
        huc2=np.array(huc2),
        hucs=np.array([x[0] for x in basins]),
        names=np.array([x[1] for x in basins]),
        dates=dates,
        lead_min=np.int64(config.lead_days_min),
        lead_max=np.int64(config.lead_days_max),
        forecasts=forecasts,
        observed=observed,
        lock_stabilization=stab,
        lock_accuracy=acc,
        lock_gap=gap,
        lock_in_fraction=np.float64(LOCK_IN_FRACTION),
        version_windows=np.array(json.dumps(config.window_block(), sort_keys=True)),
        basin_grid_cell_counts=cell_counts,
        weighting=np.array(
            "fractional grid-cell intersection area (spherical); forecasts on model grid, "
            "observations on native PRISM grid"),
    )
    print(f"  {model_key}: wrote {out} ({n_dates} dates)")
    return out
