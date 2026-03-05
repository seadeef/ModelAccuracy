#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
from lead_windows import LEAD_WINDOWS, window_to_key
from lead_config import LEAD_DAYS_MAX, LEAD_DAYS_MIN
from statistics_plugins.registry import ENABLED_STATISTICS

try:
    import xarray as xr
except Exception as exc:  # pragma: no cover - environment dependent
    raise RuntimeError(
        "xarray is required for reading GFS GRIB2 files. "
        "Install with: pip install xarray cfgrib"
    ) from exc

try:
    import rasterio
    from rasterio.warp import Resampling, reproject
except Exception as exc:  # pragma: no cover - environment dependent
    raise RuntimeError(
        "rasterio is required for reading PRISM GeoTIFFs. "
        "Install with: pip install rasterio"
    ) from exc


GFS_MODEL = "gfs"
GFS_DIR = Path("model_data") / GFS_MODEL
PRISM_DIR = Path("prism_data")
OUTPUT_ROOT = Path("stats")
TASKS_PER_CHUNK = 64

GFS_FILE_RE = re.compile(r"f(?P<fhour>\d{3})_(?P<level>[^.]+)\.grib2$")
PRISM_CACHE_MAX_ITEMS = 32
_PRISM_CACHE: dict[str, tuple[np.ndarray, rasterio.Affine, str]] = {}
_PRISM_CACHE_ORDER: list[str] = []

REPROJECTED_CACHE_MAX_ITEMS = 32
_REPROJECTED_CACHE: dict[str, np.ndarray] = {}
_REPROJECTED_CACHE_ORDER: list[str] = []


@dataclass(frozen=True)
class GridMeta:
    transform: rasterio.Affine
    crs: str
    lats: np.ndarray
    lons: np.ndarray


@dataclass(frozen=True)
class StatsTask:
    grib_path: Path
    prism_path: Path
    lead_days: int
    valid_date: datetime


def _parse_init_date(dir_name: str) -> datetime:
    date_str = dir_name.split("_")[0]
    return datetime.strptime(date_str, "%Y%m%d")


def _list_gfs_inits() -> list[Path]:
    if not GFS_DIR.exists():
        raise FileNotFoundError(f"Missing GFS directory: {GFS_DIR.resolve()}")
    init_dirs: list[Path] = []
    for year_dir in sorted(p for p in GFS_DIR.iterdir() if p.is_dir()):
        for init_dir in sorted(year_dir.iterdir()):
            if init_dir.is_dir() and init_dir.name.endswith("_12z"):
                init_dirs.append(init_dir)
    return init_dirs


def _get_prism_tif_path(date: datetime) -> Path:
    date_str = date.strftime("%Y%m%d")
    day_dir = PRISM_DIR / str(date.year) / date_str
    return day_dir / "data.tif"


def _read_gfs_apcp(grib_path: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    ds = xr.open_dataset(grib_path, engine="cfgrib")
    if not ds.data_vars:
        raise ValueError(f"No variables found in {grib_path}")
    var_name = list(ds.data_vars)[0]
    data = ds[var_name].values.astype(np.float32)
    lats = ds["latitude"].values
    lons = ds["longitude"].values

    if lons.max() > 180:
        lons = ((lons + 180) % 360) - 180
    sort_idx = np.argsort(lons)
    lons = lons[sort_idx]
    data = data[:, sort_idx]

    if lats[0] > lats[-1]:
        lats = lats[::-1]
        data = data[::-1, :]

    return data, lats, lons


def _gfs_transform(lats: np.ndarray, lons: np.ndarray) -> rasterio.Affine:
    lat_res = abs(float(lats[1] - lats[0]))
    lon_res = abs(float(lons[1] - lons[0]))
    west = float(lons.min()) - lon_res / 2.0
    north = float(lats.max()) + lat_res / 2.0
    return rasterio.transform.from_origin(west, north, lon_res, lat_res)


def _read_prism_ppt(prism_path: Path) -> Tuple[np.ndarray, rasterio.Affine, str]:
    with rasterio.open(prism_path) as src:
        data = src.read(1).astype(np.float32)
        if src.nodata is not None:
            data = np.where(data == src.nodata, np.nan, data)
        return data, src.transform, str(src.crs)


def _read_prism_ppt_cached(prism_path: Path) -> Tuple[np.ndarray, rasterio.Affine, str]:
    key = str(prism_path)
    cached = _PRISM_CACHE.get(key)
    if cached is not None:
        return cached
    value = _read_prism_ppt(prism_path)
    _PRISM_CACHE[key] = value
    _PRISM_CACHE_ORDER.append(key)
    if len(_PRISM_CACHE_ORDER) > PRISM_CACHE_MAX_ITEMS:
        old_key = _PRISM_CACHE_ORDER.pop(0)
        _PRISM_CACHE.pop(old_key, None)
    return value


def _reproject_prism_cached(
    prism_path: Path,
    gfs_shape: Tuple[int, int],
    gfs_transform: rasterio.Affine,
) -> np.ndarray:
    """Read and reproject PRISM to the GFS grid, caching by PRISM file path.

    The GFS grid is identical across all tasks, so the PRISM file path is a
    sufficient cache key.
    """
    key = str(prism_path)
    cached = _REPROJECTED_CACHE.get(key)
    if cached is not None:
        return cached
    prism_data, prism_transform, prism_crs = _read_prism_ppt_cached(prism_path)
    reprojected = _reproject_prism_to_gfs(
        prism_data, prism_transform, prism_crs, gfs_shape, gfs_transform,
    )
    _REPROJECTED_CACHE[key] = reprojected
    _REPROJECTED_CACHE_ORDER.append(key)
    if len(_REPROJECTED_CACHE_ORDER) > REPROJECTED_CACHE_MAX_ITEMS:
        old_key = _REPROJECTED_CACHE_ORDER.pop(0)
        _REPROJECTED_CACHE.pop(old_key, None)
    return reprojected


def _grid_coords_from_transform(
    transform: rasterio.Affine, height: int, width: int
) -> Tuple[np.ndarray, np.ndarray]:
    if transform.b == 0.0 and transform.d == 0.0:
        lons = transform.c + (np.arange(width) + 0.5) * transform.a
        lats = transform.f + (np.arange(height) + 0.5) * transform.e
        return lats.astype(np.float32), lons.astype(np.float32)
    cols = np.arange(width)
    rows = np.arange(height)
    xs, _ = rasterio.transform.xy(transform, rows[0], cols, offset="center")
    _, ys = rasterio.transform.xy(transform, rows, cols[0], offset="center")
    return np.array(ys, dtype=np.float32), np.array(xs, dtype=np.float32)


def _reproject_prism_to_gfs(
    prism_data: np.ndarray,
    prism_transform: rasterio.Affine,
    prism_crs: str,
    gfs_shape: Tuple[int, int],
    gfs_transform: rasterio.Affine,
) -> np.ndarray:
    dst = np.full(gfs_shape, np.nan, dtype=np.float32)
    reproject(
        source=prism_data,
        destination=dst,
        src_transform=prism_transform,
        src_crs=prism_crs,
        dst_transform=gfs_transform,
        dst_crs="EPSG:4326",
        resampling=Resampling.bilinear,
        dst_nodata=np.nan,
    )
    return dst


def _affine_to_tuple(transform: rasterio.Affine) -> Tuple[float, float, float, float, float, float]:
    return (
        float(transform.a),
        float(transform.b),
        float(transform.c),
        float(transform.d),
        float(transform.e),
        float(transform.f),
    )


def _build_stats_tasks() -> Tuple[list[StatsTask], int]:
    tasks: list[StatsTask] = []
    skipped_partial_files = 0

    for init_dir in _list_gfs_inits():
        init_date = _parse_init_date(init_dir.name)

        for grib_path in sorted(init_dir.iterdir()):
            if not grib_path.is_file():
                continue
            if grib_path.name.endswith(".part"):
                skipped_partial_files += 1
                continue
            match = GFS_FILE_RE.match(grib_path.name)
            if not match:
                continue
            fhour = int(match.group("fhour"))
            lead_days = fhour // 24
            if lead_days < LEAD_DAYS_MIN or lead_days > LEAD_DAYS_MAX:
                continue

            valid_date = init_date + timedelta(days=lead_days)
            prism_tif = _get_prism_tif_path(valid_date)
            if not prism_tif.exists():
                continue
            tasks.append(
                StatsTask(
                    grib_path=grib_path,
                    prism_path=prism_tif,
                    lead_days=lead_days,
                    valid_date=valid_date,
                )
            )

    return tasks, skipped_partial_files


def _chunk_tasks(tasks: list[StatsTask], chunk_size: int) -> list[list[StatsTask]]:
    return [tasks[i : i + chunk_size] for i in range(0, len(tasks), chunk_size)]


def _merge_accumulator_maps(
    target: Dict[int, Dict[str, Dict[str, np.ndarray]]],
    source: Dict[int, Dict[str, Dict[str, np.ndarray]]],
) -> None:
    for lead, source_stats in source.items():
        target_stats = target.setdefault(lead, {})
        for stat_name, source_acc in source_stats.items():
            if stat_name not in target_stats:
                target_stats[stat_name] = {
                    key: np.zeros_like(value) for key, value in source_acc.items()
                }
            for key, value in source_acc.items():
                target_stats[stat_name][key] += value


def _validate_or_build_grid_meta(
    grid_meta: GridMeta | None,
    transform_tuple: Tuple[float, float, float, float, float, float],
    grid_crs: str,
    grid_shape: Tuple[int, int],
) -> GridMeta:
    grid_transform = rasterio.Affine(*transform_tuple)
    if grid_meta is None:
        grid_lats, grid_lons = _grid_coords_from_transform(
            grid_transform, grid_shape[0], grid_shape[1]
        )
        return GridMeta(
            transform=grid_transform,
            crs=grid_crs,
            lats=grid_lats,
            lons=grid_lons,
        )

    if (
        _affine_to_tuple(grid_meta.transform) != transform_tuple
        or grid_crs != grid_meta.crs
        or grid_shape != (grid_meta.lats.size, grid_meta.lons.size)
    ):
        raise RuntimeError(
            "GFS grid mismatch across dates. "
            "Expected consistent GFS grid for all tasks."
        )
    return grid_meta


def _compute_chunk_locals(
    chunk: list[StatsTask],
) -> Tuple[
    Dict[int, Dict[str, Dict[str, np.ndarray]]],
    Tuple[float, float, float, float, float, float],
    str,
    Tuple[int, int],
]:
    chunk_accumulators: Dict[int, Dict[str, Dict[str, np.ndarray]]] = {}
    chunk_grid_tuple: Tuple[float, float, float, float, float, float] | None = None
    chunk_grid_crs: str | None = None
    chunk_grid_shape: Tuple[int, int] | None = None

    for task in chunk:
        gfs_data, gfs_lats, gfs_lons = _read_gfs_apcp(task.grib_path)
        gfs_transform = _gfs_transform(gfs_lats, gfs_lons)
        prism_on_gfs = _reproject_prism_cached(
            task.prism_path, gfs_data.shape, gfs_transform,
        )
        valid_mask = np.isfinite(gfs_data) & np.isfinite(prism_on_gfs)
        diff = gfs_data - prism_on_gfs
        derived = {
            "diff": diff,
            "abs_diff": np.abs(diff),
            "sq_diff": diff * diff,
        }

        lead_stats = chunk_accumulators.setdefault(task.lead_days, {})
        for plugin in ENABLED_STATISTICS:
            stat_name = plugin.spec.name
            if stat_name not in lead_stats:
                lead_stats[stat_name] = plugin.init_accumulator(gfs_data.shape)
            plugin.update(
                lead_stats[stat_name],
                gfs_data,
                prism_on_gfs,
                valid_mask,
                derived=derived,
            )

        transform_tuple = _affine_to_tuple(gfs_transform)
        if chunk_grid_tuple is None:
            chunk_grid_tuple = transform_tuple
            chunk_grid_crs = "EPSG:4326"
            chunk_grid_shape = gfs_data.shape
        else:
            if (
                transform_tuple != chunk_grid_tuple
                or chunk_grid_crs != "EPSG:4326"
                or chunk_grid_shape != gfs_data.shape
            ):
                raise RuntimeError(
                    "GFS grid mismatch within chunk. Expected consistent grid."
                )

    if chunk_grid_tuple is None or chunk_grid_crs is None or chunk_grid_shape is None:
        raise RuntimeError("Chunk produced no tasks.")
    return chunk_accumulators, chunk_grid_tuple, chunk_grid_crs, chunk_grid_shape


def _compute_lead_stats() -> Tuple[
    Dict[int, Dict[str, Dict[str, np.ndarray]]], GridMeta, int
]:
    accumulators: Dict[int, Dict[str, Dict[str, np.ndarray]]] = {}
    grid_meta: GridMeta | None = None
    tasks, skipped_partial_files = _build_stats_tasks()
    if not tasks:
        raise RuntimeError("No GFS/PRISM overlaps found. Check data paths.")

    max_workers = max(1, (os.cpu_count() or 1) - 1)
    use_parallel = len(tasks) > 1 and max_workers > 1

    task_chunks = _chunk_tasks(tasks, TASKS_PER_CHUNK)

    if use_parallel:
        print(
            f"Processing {len(tasks)} tasks in {len(task_chunks)} chunks "
            f"with {max_workers} worker processes..."
        )
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {
                executor.submit(_compute_chunk_locals, chunk): chunk for chunk in task_chunks
            }
            total = len(future_to_chunk)
            for completed, future in enumerate(as_completed(future_to_chunk), start=1):
                chunk = future_to_chunk[future]
                try:
                    chunk_accumulators, transform_tuple, grid_crs, grid_shape = future.result()
                except Exception as exc:
                    sample_task = chunk[0]
                    raise RuntimeError(
                        f"Failed chunk starting at {sample_task.grib_path} "
                        f"(valid date {sample_task.valid_date:%Y-%m-%d})"
                    ) from exc
                grid_meta = _validate_or_build_grid_meta(
                    grid_meta, transform_tuple, grid_crs, grid_shape
                )
                _merge_accumulator_maps(accumulators, chunk_accumulators)
                if completed % 5 == 0 or completed == total:
                    print(f"Completed {completed}/{total} chunks...")
    else:
        for chunk in task_chunks:
            chunk_accumulators, transform_tuple, grid_crs, grid_shape = _compute_chunk_locals(chunk)
            grid_meta = _validate_or_build_grid_meta(
                grid_meta, transform_tuple, grid_crs, grid_shape
            )
            _merge_accumulator_maps(accumulators, chunk_accumulators)

    if grid_meta is None:
        raise RuntimeError("No GFS/PRISM overlaps found. Check data paths.")

    return accumulators, grid_meta, skipped_partial_files


def _write_stats(
    accumulators: Dict[int, Dict[str, Dict[str, np.ndarray]]],
    grid_meta: GridMeta,
) -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    for plugin in ENABLED_STATISTICS:
        stat_dir = OUTPUT_ROOT / plugin.spec.name
        stat_dir.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            stat_dir / "metadata.npz",
            lats=grid_meta.lats,
            lons=grid_meta.lons,
            transform=np.array(grid_meta.transform),
            crs=grid_meta.crs,
        )

        for lead in sorted(accumulators):
            lead_acc = accumulators[lead].get(plugin.spec.name)
            if lead_acc is None:
                continue
            outputs = plugin.finalize(lead_acc)
            np.savez_compressed(stat_dir / f"lead_{lead}.npz", **outputs)

        # Build requested combined lead windows from already-computed per-lead accumulators.
        for start, end in LEAD_WINDOWS:
            lead_ids = [lead for lead in sorted(accumulators) if start <= lead <= end]
            expected_count = end - start + 1
            if len(lead_ids) != expected_count:
                continue

            window_acc: dict[str, np.ndarray] | None = None
            for lead in lead_ids:
                lead_acc = accumulators[lead].get(plugin.spec.name)
                if lead_acc is None:
                    continue
                if window_acc is None:
                    window_acc = {
                        key: np.zeros_like(value) for key, value in lead_acc.items()
                    }
                for key, value in lead_acc.items():
                    window_acc[key] += value
            if window_acc is None:
                continue

            outputs = plugin.finalize(window_acc)
            window_key = window_to_key(start, end)
            np.savez_compressed(stat_dir / f"lead_{window_key}.npz", **outputs)


def main() -> None:
    stat_names = ", ".join(plugin.spec.name for plugin in ENABLED_STATISTICS)
    print(
        "Computing statistics on native GFS grid. "
        f"Enabled statistics: {stat_names}"
    )
    accumulators, grid_meta, skipped_partial_files = _compute_lead_stats()
    _write_stats(accumulators, grid_meta)
    if skipped_partial_files:
        print(f"Skipped {skipped_partial_files} partial GFS files (*.part).")
    print(f"Wrote statistics to {OUTPUT_ROOT.resolve()}")


if __name__ == "__main__":
    main()
