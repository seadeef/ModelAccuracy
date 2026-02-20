#!/usr/bin/env python3
from __future__ import annotations

import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
from lead_config import LEAD_DAYS_MAX, LEAD_DAYS_MIN

try:
    import xarray as xr
except Exception as exc:  # pragma: no cover - environment dependent
    raise RuntimeError(
        "xarray is required for reading GFS GRIB2 files. "
        "Install with: pip install xarray cfgrib"
    ) from exc

try:
    import rasterio
    from rasterio.warp import reproject, Resampling
except Exception as exc:  # pragma: no cover - environment dependent
    raise RuntimeError(
        "rasterio is required for reading PRISM GeoTIFFs. "
        "Install with: pip install rasterio"
    ) from exc


GFS_MODEL = "gfs"

GFS_DIR = Path("model_data") / GFS_MODEL
PRISM_DIR = Path("prism_data")
OUTPUT_DIR = Path("stats") / "bias"

GFS_FILE_RE = re.compile(r"f(?P<fhour>\d{3})_(?P<level>[^.]+)\.grib2$")


@dataclass(frozen=True)
class GridMeta:
    transform: rasterio.Affine
    crs: str
    lats: np.ndarray
    lons: np.ndarray


@dataclass(frozen=True)
class BiasTask:
    grib_path: Path
    prism_path: Path
    lead_days: int
    valid_date: datetime


def _parse_init_date(dir_name: str) -> datetime:
    # Example: 20220103_12z
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

    # Normalize longitudes to [-180, 180] and sort
    if lons.max() > 180:
        lons = ((lons + 180) % 360) - 180
    sort_idx = np.argsort(lons)
    lons = lons[sort_idx]
    data = data[:, sort_idx]

    # Ensure latitude is ascending
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


def _reproject_gfs_to_prism(
    gfs_data: np.ndarray,
    gfs_transform: rasterio.Affine,
    prism_shape: Tuple[int, int],
    prism_transform: rasterio.Affine,
    prism_crs: str,
) -> np.ndarray:
    dst = np.full(prism_shape, np.nan, dtype=np.float32)
    reproject(
        source=gfs_data,
        destination=dst,
        src_transform=gfs_transform,
        src_crs="EPSG:4326",
        dst_transform=prism_transform,
        dst_crs=prism_crs,
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


def _build_bias_tasks() -> Tuple[list[BiasTask], int]:
    tasks: list[BiasTask] = []
    skipped_partial_files = 0

    for init_dir in _list_gfs_inits():
        init_date = _parse_init_date(init_dir.name)

        for grib_path in sorted(init_dir.iterdir()):
            if not grib_path.is_file():
                continue
            # Ignore downloader artifacts such as *.grib2.part.
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
                BiasTask(
                    grib_path=grib_path,
                    prism_path=prism_tif,
                    lead_days=lead_days,
                    valid_date=valid_date,
                )
            )

    return tasks, skipped_partial_files


def _compute_task_locals(
    task: BiasTask,
) -> Tuple[
    int,
    np.ndarray,
    np.ndarray,
    Tuple[float, float, float, float, float, float],
    str,
    Tuple[int, int],
]:
    gfs_data, gfs_lats, gfs_lons = _read_gfs_apcp(task.grib_path)
    gfs_transform = _gfs_transform(gfs_lats, gfs_lons)
    prism_data, prism_transform, prism_crs = _read_prism_ppt(task.prism_path)

    gfs_on_prism = _reproject_gfs_to_prism(
        gfs_data,
        gfs_transform,
        prism_data.shape,
        prism_transform,
        prism_crs,
    )
    diff = gfs_on_prism - prism_data
    valid_mask = np.isfinite(diff)

    local_sum_diff = np.zeros_like(diff, dtype=np.float32)
    local_sum_diff[valid_mask] = diff[valid_mask]
    local_count = valid_mask.astype(np.int32)

    return (
        task.lead_days,
        local_sum_diff,
        local_count,
        _affine_to_tuple(prism_transform),
        prism_crs,
        prism_data.shape,
    )


def _merge_task_result(
    result: Tuple[
        int,
        np.ndarray,
        np.ndarray,
        Tuple[float, float, float, float, float, float],
        str,
        Tuple[int, int],
    ],
    sum_diff: Dict[int, np.ndarray],
    count: Dict[int, np.ndarray],
    grid_meta: GridMeta | None,
) -> GridMeta:
    key, local_sum_diff, local_count, transform_tuple, prism_crs, prism_shape = result
    prism_transform = rasterio.Affine(*transform_tuple)

    if grid_meta is None:
        prism_lats, prism_lons = _grid_coords_from_transform(
            prism_transform, prism_shape[0], prism_shape[1]
        )
        grid_meta = GridMeta(
            transform=prism_transform,
            crs=prism_crs,
            lats=prism_lats,
            lons=prism_lons,
        )
    else:
        if (
            _affine_to_tuple(grid_meta.transform) != transform_tuple
            or prism_crs != grid_meta.crs
            or prism_shape != (grid_meta.lats.size, grid_meta.lons.size)
        ):
            raise RuntimeError(
                "PRISM grid mismatch across dates. "
                "Expected consistent PRISM grid for all tasks."
            )

    if key not in sum_diff:
        sum_diff[key] = np.zeros_like(local_sum_diff, dtype=np.float32)
        count[key] = np.zeros_like(local_count, dtype=np.int32)

    sum_diff[key] += local_sum_diff
    count[key] += local_count
    return grid_meta


def _compute_lead_stats() -> Tuple[
    Dict[int, np.ndarray], Dict[int, np.ndarray], GridMeta, int
]:
    sum_diff: Dict[int, np.ndarray] = {}
    count: Dict[int, np.ndarray] = {}
    grid_meta: GridMeta | None = None
    tasks, skipped_partial_files = _build_bias_tasks()
    if not tasks:
        raise RuntimeError("No GFS/PRISM overlaps found. Check data paths.")

    max_workers = max(1, (os.cpu_count() or 1) - 1)
    use_parallel = len(tasks) > 1 and max_workers > 1

    if use_parallel:
        print(f"Processing {len(tasks)} tasks with {max_workers} worker processes...")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(_compute_task_locals, task): task for task in tasks}
            total = len(future_to_task)
            for completed, future in enumerate(as_completed(future_to_task), start=1):
                task = future_to_task[future]
                try:
                    result = future.result()
                except Exception as exc:
                    raise RuntimeError(
                        f"Failed task for {task.grib_path} (valid date {task.valid_date:%Y-%m-%d})"
                    ) from exc
                grid_meta = _merge_task_result(result, sum_diff, count, grid_meta)
                if completed % 50 == 0 or completed == total:
                    print(f"Completed {completed}/{total} tasks...")
    else:
        for task in tasks:
            result = _compute_task_locals(task)
            grid_meta = _merge_task_result(result, sum_diff, count, grid_meta)

    if grid_meta is None:
        raise RuntimeError("No GFS/PRISM overlaps found. Check data paths.")

    return sum_diff, count, grid_meta, skipped_partial_files


def _write_tiles(
    sum_diff: Dict[int, np.ndarray],
    count: Dict[int, np.ndarray],
    grid_meta: GridMeta,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Write metadata once
    np.savez_compressed(
        OUTPUT_DIR / "metadata.npz",
        lats=grid_meta.lats,
        lons=grid_meta.lons,
        transform=np.array(grid_meta.transform),
        crs=grid_meta.crs,
    )

    for lead, s in sum_diff.items():
        c = count[lead]
        bias_mean = np.full_like(s, np.nan, dtype=np.float32)
        valid_mask = c > 0
        bias_mean[valid_mask] = s[valid_mask] / c[valid_mask]

        out_path = OUTPUT_DIR / f"lead_{lead}.npz"
        np.savez_compressed(
            out_path,
            bias_mean=bias_mean,
            sum_diff=s,
            sample_count=c,
        )


def main() -> None:
    print(
        "Computing bias on PRISM grid (higher resolution). "
        "This increases memory usage and runtime."
    )
    sum_diff, count, grid_meta, skipped_partial_files = _compute_lead_stats()
    _write_tiles(sum_diff, count, grid_meta)
    if skipped_partial_files:
        print(f"Skipped {skipped_partial_files} partial GFS files (*.part).")
    print(f"Wrote tiles to {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
