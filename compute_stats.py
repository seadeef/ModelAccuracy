#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
from lead_windows import LEAD_WINDOWS, window_to_key
from lead_config import LEAD_DAYS_MAX, LEAD_DAYS_MIN
from statistics_plugins.registry import VERIFICATION_STATISTICS

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
GFS_GRID_LATS_PATH = GFS_DIR / "grid_lats.npy"
GFS_GRID_LONS_PATH = GFS_DIR / "grid_lons.npy"
PRISM_DIR = Path("prism_data")
OUTPUT_ROOT = Path("stats")

GFS_FILE_RE = re.compile(r"f(?P<fhour>\d{3})_(?P<level>[^.]+)\.grib2$")

THREADS_PER_PROCESS = 4
CACHE_MAX_ITEMS = 32


class _ReprojectionCache:
    """Per-process LRU cache for PRISM reads and reprojections."""

    def __init__(self, max_items: int = CACHE_MAX_ITEMS):
        self._max_items = max_items
        self._prism_cache: dict[str, tuple[np.ndarray, rasterio.Affine, str]] = {}
        self._prism_order: list[str] = []
        self._reproj_cache: dict[str, np.ndarray] = {}
        self._reproj_order: list[str] = []

    def read_prism(self, prism_path: Path) -> tuple[np.ndarray, rasterio.Affine, str]:
        key = str(prism_path)
        cached = self._prism_cache.get(key)
        if cached is not None:
            return cached
        value = _read_prism_ppt(prism_path)
        self._prism_cache[key] = value
        self._prism_order.append(key)
        if len(self._prism_order) > self._max_items:
            old_key = self._prism_order.pop(0)
            self._prism_cache.pop(old_key, None)
        return value

    def reproject_prism(
        self,
        prism_path: Path,
        gfs_shape: Tuple[int, int],
        gfs_transform: rasterio.Affine,
    ) -> np.ndarray:
        key = str(prism_path)
        cached = self._reproj_cache.get(key)
        if cached is not None:
            return cached
        prism_data, prism_transform, prism_crs = self.read_prism(prism_path)
        reprojected = _reproject_prism_to_gfs(
            prism_data, prism_transform, prism_crs, gfs_shape, gfs_transform,
        )
        self._reproj_cache[key] = reprojected
        self._reproj_order.append(key)
        if len(self._reproj_order) > self._max_items:
            old_key = self._reproj_order.pop(0)
            self._reproj_cache.pop(old_key, None)
        return reprojected


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


def _group_inits_by_year(init_dirs: list[Path]) -> dict[int, list[Path]]:
    by_year: dict[int, list[Path]] = defaultdict(list)
    for d in init_dirs:
        year = int(d.parent.name)
        by_year[year].append(d)
    return dict(sorted(by_year.items()))


def _distribute_years(
    year_items: list[tuple[int, list[Path]]], num_chunks: int
) -> list[list[tuple[int, list[Path]]]]:
    chunks: list[list[tuple[int, list[Path]]]] = [[] for _ in range(num_chunks)]
    for i, item in enumerate(year_items):
        chunks[i % num_chunks].append(item)
    return [c for c in chunks if c]


def _get_prism_tif_path(date: datetime) -> Path:
    date_str = date.strftime("%Y%m%d")
    day_dir = PRISM_DIR / str(date.year) / date_str
    return day_dir / "data.tif"


def _read_gfs_apcp(grib_path: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    npy_path = grib_path.with_suffix(".npy")
    if npy_path.exists() and GFS_GRID_LATS_PATH.exists() and GFS_GRID_LONS_PATH.exists():
        data = np.load(npy_path)
        lats = np.load(GFS_GRID_LATS_PATH)
        lons = np.load(GFS_GRID_LONS_PATH)
        return data, lats, lons

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
        resampling=Resampling.nearest,
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


def _build_tasks_for_inits(init_dirs: list[Path]) -> Tuple[list[StatsTask], int]:
    tasks: list[StatsTask] = []
    skipped_partial_files = 0

    for init_dir in init_dirs:
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


def _process_single_task(
    task: StatsTask,
    cache: _ReprojectionCache,
) -> Tuple[
    int,
    Dict[str, Dict[str, np.ndarray]],
    Tuple[float, float, float, float, float, float],
    Tuple[int, int],
]:
    """Process one task and return per-lead accumulator deltas.

    Returns (lead_days, stat_accumulators, transform_tuple, grid_shape).
    The accumulators contain one sample's contribution so the caller can merge.
    """
    gfs_data, gfs_lats, gfs_lons = _read_gfs_apcp(task.grib_path)
    gfs_tf = _gfs_transform(gfs_lats, gfs_lons)
    transform_tuple = _affine_to_tuple(gfs_tf)

    prism_on_gfs = cache.reproject_prism(
        task.prism_path, gfs_data.shape, gfs_tf,
    )
    valid_mask = np.isfinite(gfs_data) & np.isfinite(prism_on_gfs)
    diff = gfs_data - prism_on_gfs
    derived = {
        "diff": diff,
        "abs_diff": np.abs(diff),
        "sq_diff": diff * diff,
    }

    stat_accs: Dict[str, Dict[str, np.ndarray]] = {}
    for plugin in VERIFICATION_STATISTICS:
        acc = plugin.init_accumulator(gfs_data.shape)
        plugin.update(acc, gfs_data, prism_on_gfs, valid_mask, derived=derived)
        stat_accs[plugin.spec.name] = acc

    return task.lead_days, stat_accs, transform_tuple, gfs_data.shape


def _merge_task_result(
    accumulators: Dict[int, Dict[str, Dict[str, np.ndarray]]],
    lead_days: int,
    stat_accs: Dict[str, Dict[str, np.ndarray]],
) -> None:
    """Merge a single task's accumulator deltas into the running totals."""
    lead_stats = accumulators.get(lead_days)
    if lead_stats is None:
        # First time seeing this lead — take ownership of the arrays.
        accumulators[lead_days] = {
            stat: {k: v.copy() for k, v in acc.items()}
            for stat, acc in stat_accs.items()
        }
        return
    for stat, acc in stat_accs.items():
        existing = lead_stats.get(stat)
        if existing is None:
            lead_stats[stat] = {k: v.copy() for k, v in acc.items()}
        else:
            for key, arr in acc.items():
                existing[key] += arr


def _compute_years_chunk(
    year_init_dirs: list[tuple[int, list[Path]]],
) -> Tuple[
    Dict[int, Dict[str, Dict[str, np.ndarray]]],
    Tuple[float, float, float, float, float, float] | None,
    Tuple[int, int] | None,
    int,
    int,
]:
    """Process a chunk of years. Runs in a child process.

    Uses a ThreadPoolExecutor internally to overlap I/O (rasterio reproject
    releases the GIL). The reprojection cache is local to this process and
    shared safely across threads under CPython's GIL.

    Returns (accumulators, transform_tuple, grid_shape, task_count, skipped).
    """
    cache = _ReprojectionCache()
    accumulators: Dict[int, Dict[str, Dict[str, np.ndarray]]] = {}
    grid_transform_tuple: Tuple[float, float, float, float, float, float] | None = None
    grid_shape: Tuple[int, int] | None = None
    total_tasks = 0
    total_skipped = 0

    # Collect all tasks across years in this chunk, preserving date order.
    all_tasks: list[StatsTask] = []
    for _year, init_dirs in year_init_dirs:
        tasks, skipped = _build_tasks_for_inits(init_dirs)
        all_tasks.extend(tasks)
        total_skipped += skipped

    if not all_tasks:
        return accumulators, grid_transform_tuple, grid_shape, 0, total_skipped

    def process_task(task: StatsTask):
        return _process_single_task(task, cache)

    with ThreadPoolExecutor(max_workers=THREADS_PER_PROCESS) as executor:
        futures = {executor.submit(process_task, t): t for t in all_tasks}
        for future in as_completed(futures):
            task = futures[future]
            try:
                lead_days, stat_accs, t_tuple, shape = future.result()
            except Exception as exc:
                raise RuntimeError(
                    f"Failed task for {task.grib_path} "
                    f"(valid date {task.valid_date:%Y-%m-%d})"
                ) from exc

            # Validate grid consistency.
            if grid_transform_tuple is None:
                grid_transform_tuple = t_tuple
                grid_shape = shape
            elif grid_transform_tuple != t_tuple or grid_shape != shape:
                raise RuntimeError(
                    "GFS grid mismatch across dates within process."
                )

            _merge_task_result(accumulators, lead_days, stat_accs)
            total_tasks += 1

    return accumulators, grid_transform_tuple, grid_shape, total_tasks, total_skipped


def _merge_chunk_accumulators(
    results: list[Dict[int, Dict[str, Dict[str, np.ndarray]]]],
) -> Dict[int, Dict[str, Dict[str, np.ndarray]]]:
    """Merge accumulators from multiple process chunks by summing arrays."""
    merged: Dict[int, Dict[str, Dict[str, np.ndarray]]] = {}
    for chunk_acc in results:
        for lead, stats in chunk_acc.items():
            if lead not in merged:
                merged[lead] = {
                    stat: {k: v.copy() for k, v in acc.items()}
                    for stat, acc in stats.items()
                }
            else:
                for stat, acc in stats.items():
                    if stat not in merged[lead]:
                        merged[lead][stat] = {k: v.copy() for k, v in acc.items()}
                    else:
                        for key, arr in acc.items():
                            merged[lead][stat][key] += arr
    return merged


def _compute_lead_stats() -> Tuple[
    Dict[int, Dict[str, Dict[str, np.ndarray]]], GridMeta, int
]:
    init_dirs = _list_gfs_inits()
    if not init_dirs:
        raise RuntimeError("No GFS init dirs found. Check data paths.")

    by_year = _group_inits_by_year(init_dirs)
    year_items = list(by_year.items())
    num_workers = min(len(year_items), max(1, os.cpu_count() or 1))
    chunks = _distribute_years(year_items, num_workers)

    print(
        f"Found {len(year_items)} year(s) of data, "
        f"dispatching to {len(chunks)} process(es) "
        f"with {THREADS_PER_PROCESS} threads each..."
    )
    for chunk in chunks:
        years_str = ", ".join(str(y) for y, _ in chunk)
        print(f"  Process chunk: years [{years_str}]")

    total_tasks = 0
    total_skipped = 0
    chunk_accumulators: list[Dict[int, Dict[str, Dict[str, np.ndarray]]]] = []
    grid_transform_tuple: Tuple[float, float, float, float, float, float] | None = None
    grid_shape: Tuple[int, int] | None = None

    with ProcessPoolExecutor(max_workers=len(chunks)) as pool:
        futures = {
            pool.submit(_compute_years_chunk, chunk): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            chunk_idx = futures[future]
            acc, t_tuple, shape, task_count, skipped = future.result()
            total_tasks += task_count
            total_skipped += skipped

            if task_count > 0:
                chunk_accumulators.append(acc)
                if grid_transform_tuple is None:
                    grid_transform_tuple = t_tuple
                    grid_shape = shape
                elif grid_transform_tuple != t_tuple or grid_shape != shape:
                    raise RuntimeError(
                        "GFS grid mismatch across processes."
                    )

            chunk_years = ", ".join(
                str(y) for y, _ in chunks[chunk_idx]
            )
            print(f"  Chunk [{chunk_years}] done: {task_count} tasks")

    if grid_transform_tuple is None or grid_shape is None:
        raise RuntimeError("No GFS/PRISM overlaps found. Check data paths.")

    print(f"Total tasks processed: {total_tasks}")

    merged = _merge_chunk_accumulators(chunk_accumulators)
    grid_meta = _validate_or_build_grid_meta(
        None, grid_transform_tuple, "EPSG:4326", grid_shape
    )

    return merged, grid_meta, total_skipped


def _write_stats(
    accumulators: Dict[int, Dict[str, Dict[str, np.ndarray]]],
    grid_meta: GridMeta,
) -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    for plugin in VERIFICATION_STATISTICS:
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


def _convert_single_grib(grib_path: Path) -> bool:
    """Convert a single GRIB2 file to .npy. Returns True if converted, False if skipped."""
    npy_path = grib_path.with_suffix(".npy")
    if npy_path.exists():
        return False
    data, _lats, _lons = _read_gfs_apcp(grib_path)
    np.save(npy_path, data)
    return True


def preconvert_grib2_to_npy() -> None:
    """Convert all GFS GRIB2 files to .npy for fast reading."""
    print("Scanning for GRIB2 files to convert...")
    grib_paths: list[Path] = []
    for init_dir in _list_gfs_inits():
        for f in sorted(init_dir.iterdir()):
            if f.is_file() and GFS_FILE_RE.match(f.name):
                grib_paths.append(f)

    if not grib_paths:
        print("No GRIB2 files found.")
        return

    print(f"Found {len(grib_paths)} GRIB2 files.")

    # Save grid lats/lons from the first file (grid is constant across all files).
    if not GFS_GRID_LATS_PATH.exists() or not GFS_GRID_LONS_PATH.exists():
        print("Saving grid coordinates from first file...")
        _data, lats, lons = _read_gfs_apcp(grib_paths[0])
        np.save(GFS_GRID_LATS_PATH, lats)
        np.save(GFS_GRID_LONS_PATH, lons)
        print(f"  Saved {GFS_GRID_LATS_PATH} and {GFS_GRID_LONS_PATH}")

    max_workers = max(1, (os.cpu_count() or 1) - 1)
    converted = 0
    skipped = 0

    print(f"Converting with {max_workers} worker processes...")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_path = {
            executor.submit(_convert_single_grib, p): p for p in grib_paths
        }
        total = len(future_to_path)
        for i, future in enumerate(as_completed(future_to_path), start=1):
            path = future_to_path[future]
            try:
                was_converted = future.result()
            except Exception as exc:
                print(f"  ERROR converting {path}: {exc}")
                continue
            if was_converted:
                converted += 1
            else:
                skipped += 1
            if i % 500 == 0 or i == total:
                print(f"  Progress: {i}/{total} ({converted} converted, {skipped} skipped)")

    print(f"Done. Converted {converted} files, skipped {skipped} (already existed).")


def main() -> None:
    stat_names = ", ".join(plugin.spec.name for plugin in VERIFICATION_STATISTICS)
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
    if "--preconvert" in sys.argv:
        preconvert_grib2_to_npy()
    else:
        main()
