"""Forecast-versus-PRISM verification stage."""
from __future__ import annotations

import os
import re
import warnings
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
from model_registry import MODEL_REGISTRY, DEFAULT_MODEL
from statistics_plugins.registry import VERIFICATION_STATISTICS
from stats_grid_metadata import load_model_metadata, save_model_metadata
from stats_pipeline.manifest import fingerprint_payload, stage_state, update_stage
from stats_pipeline.io import atomic_savez

try:
    import rasterio
    from rasterio.warp import Resampling, reproject
    try:
        from rasterio.errors import NotGeoreferencedWarning as _RASTERIO_NOT_GEOREFERENCED
    except Exception:  # pragma: no cover - older rasterio
        _RASTERIO_NOT_GEOREFERENCED = None
except Exception as exc:  # pragma: no cover - environment dependent
    raise RuntimeError(
        "rasterio is required for reading PRISM GeoTIFFs. "
        "Install with: pip install rasterio"
    ) from exc


# These module-level variables are set by _configure_for_model() before computation.
GFS_DIR: Path = Path("model_data/gfs")
GFS_GRID_LATS_PATH: Path = GFS_DIR / "grid_lats.npy"
GFS_GRID_LONS_PATH: Path = GFS_DIR / "grid_lons.npy"
PRISM_DIR = Path("prism_data")
OUTPUT_ROOT: Path = Path("stats_output")
LEAD_DAYS_MIN: int = 1
LEAD_DAYS_MAX: int = 14
CYCLE_HOUR: int = MODEL_REGISTRY[DEFAULT_MODEL].cycle_hour


def _configure_for_model(model_key: str) -> None:
    global GFS_DIR, GFS_GRID_LATS_PATH, GFS_GRID_LONS_PATH, OUTPUT_ROOT
    global LEAD_DAYS_MIN, LEAD_DAYS_MAX, CYCLE_HOUR
    config = MODEL_REGISTRY[model_key]
    GFS_DIR = Path(config.data_dir)
    GFS_GRID_LATS_PATH = GFS_DIR / "grid_lats.npy"
    GFS_GRID_LONS_PATH = GFS_DIR / "grid_lons.npy"
    OUTPUT_ROOT = Path("stats_output") / model_key
    LEAD_DAYS_MIN = config.lead_days_min
    LEAD_DAYS_MAX = config.lead_days_max
    CYCLE_HOUR = config.cycle_hour

# Task discovery: downloaders produce .npy files (CONUS-cropped float32 arrays).
# The (?P<level>...) group is the variable suffix; only precipitation
# ("surface", the historical precip suffix per downloaders/base.py) is verified
# against PRISM — see _build_tasks_for_inits.
LEAD_NPY_RE = re.compile(r"f(?P<fhour>\d{3})_(?P<level>[^.]+)\.npy$")

# manifest.json schema. v2 records processed work per (init, lead) pair so a
# late-arriving PRISM day is picked up for an init that was only partially
# ingested, and a failed task simply retries (it is absent from the map). A v1
# manifest triggers a full recompute (which the statistics-correctness changes
# require anyway).
MANIFEST_SCHEMA_VERSION = 2

THREADS_PER_PROCESS = 4
CACHE_MAX_ITEMS = 32

SEASONS = {"djf": (12, 1, 2), "mam": (3, 4, 5), "jja": (6, 7, 8), "son": (9, 10, 11)}

# Type alias for the month-keyed accumulator structure:
#   Dict[month, Dict[lead, Dict[stat, Dict[key, array]]]]
MonthAccumulators = Dict[int, Dict[int, Dict[str, Dict[str, np.ndarray]]]]


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
        value = read_prism(prism_path)
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
        reprojected = reproject_prism(
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
    npy_path: Path
    prism_path: Path
    lead_days: int
    valid_date: datetime


def _parse_init_date(dir_name: str) -> datetime:
    date_str = dir_name.split("_")[0]
    return datetime.strptime(date_str, "%Y%m%d")


def _list_gfs_inits() -> list[Path]:
    if not GFS_DIR.exists():
        raise FileNotFoundError(f"Missing GFS directory: {GFS_DIR.resolve()}")
    suffix = f"_{CYCLE_HOUR:02d}z"
    init_dirs: list[Path] = []
    for year_dir in sorted(p for p in GFS_DIR.iterdir() if p.is_dir()):
        for init_dir in sorted(year_dir.iterdir()):
            if init_dir.is_dir() and init_dir.name.endswith(suffix):
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


def _read_model_npy(npy_path: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Read a CONUS-cropped .npy precipitation array and its grid coordinates."""
    if not npy_path.exists():
        raise FileNotFoundError(f"Missing .npy file: {npy_path}")
    if not GFS_GRID_LATS_PATH.exists() or not GFS_GRID_LONS_PATH.exists():
        raise FileNotFoundError(
            f"Missing grid coordinate files: {GFS_GRID_LATS_PATH}, {GFS_GRID_LONS_PATH}"
        )
    data = np.load(npy_path)
    lats = np.load(GFS_GRID_LATS_PATH)
    lons = np.load(GFS_GRID_LONS_PATH)
    if data.shape != (lats.size, lons.size):
        raise ValueError(
            f"{npy_path}: shape {data.shape} != grid ({lats.size}, {lons.size}). "
            f"Re-run the downloader to regenerate .npy files."
        )
    return data, lats, lons


def grid_transform(lats: np.ndarray, lons: np.ndarray) -> rasterio.Affine:
    lat_res = abs(float(lats[1] - lats[0]))
    lon_res = abs(float(lons[1] - lons[0]))
    west = float(lons.min()) - lon_res / 2.0
    if lats[0] < lats[-1]:
        # Data is south-to-north: origin at bottom-left, positive y step.
        south = float(lats.min()) - lat_res / 2.0
        return rasterio.Affine(lon_res, 0, west, 0, lat_res, south)
    else:
        # Data is north-to-south (standard raster convention).
        north = float(lats.max()) + lat_res / 2.0
        return rasterio.transform.from_origin(west, north, lon_res, lat_res)


def read_prism(prism_path: Path) -> Tuple[np.ndarray, rasterio.Affine, str]:
    with rasterio.open(prism_path) as src:
        data = src.read(1).astype(np.float32)
        if src.nodata is not None:
            data = np.where(data == src.nodata, np.nan, data)
        return data, src.transform, str(src.crs)


def grid_coords_from_transform(
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


def reproject_prism(
    prism_data: np.ndarray,
    prism_transform: rasterio.Affine,
    prism_crs: str,
    gfs_shape: Tuple[int, int],
    gfs_transform: rasterio.Affine,
) -> np.ndarray:
    dst = np.full(gfs_shape, np.nan, dtype=np.float32)
    with warnings.catch_warnings():
        if _RASTERIO_NOT_GEOREFERENCED is not None:
            warnings.simplefilter("ignore", _RASTERIO_NOT_GEOREFERENCED)
        # Rasterio 1.4+ message includes "gcps, or rpcs" / "identity matrix"
        warnings.filterwarnings(
            "ignore",
            message=r".*[Nn]o geotransform.*|.*identity matrix will be returned.*",
            category=UserWarning,
        )
        # Downsampling PRISM (~4 km) onto the model grid (~25 km): area-average,
        # not nearest. The forecast cell is an areal mean, so verifying it against
        # a single 4 km PRISM pixel (nearest) inflated RMSE/MAD; averaging the
        # ~40 fine pixels into each coarse cell compares like with like.
        # src_nodata=NaN so PRISM's ocean/off-CONUS NaNs are excluded from the
        # mean rather than poisoning coastal cells.
        reproject(
            source=prism_data,
            destination=dst,
            src_transform=prism_transform,
            src_crs=prism_crs,
            src_nodata=np.nan,
            dst_transform=gfs_transform,
            dst_crs="EPSG:4326",
            resampling=Resampling.average,
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
        grid_lats, grid_lons = grid_coords_from_transform(
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


def _grid_meta_from_saved_metadata(stats_root: Path) -> GridMeta:
    meta = load_model_metadata(stats_root)
    return GridMeta(
        transform=rasterio.Affine(*meta["transform"]),
        crs=str(meta["crs"]),
        lats=meta["lats"],
        lons=meta["lons"],
    )


def _build_tasks_for_inits(
    init_dirs: list[Path],
    processed_pairs: dict[str, set[int]] | None = None,
) -> Tuple[list[StatsTask], int]:
    """Build the (init, lead) tasks to process from these init dirs.

    A task is emitted only when the file is the precipitation field, the lead is
    in range, its PRISM day is on disk, and the (init, lead) pair has not already
    been folded into the accumulators (``processed_pairs``: {init_name: {leads}}).
    Passing PRISM existence here means a day whose PRISM arrives late becomes a
    task on the next run without touching the init that referenced it.
    """
    tasks: list[StatsTask] = []
    skipped_partial_files = 0
    processed_pairs = processed_pairs or {}

    for init_dir in init_dirs:
        init_date = _parse_init_date(init_dir.name)
        done_leads = processed_pairs.get(init_dir.name, ())

        for path in sorted(init_dir.iterdir()):
            if not path.is_file():
                continue
            if path.name.endswith(".part"):
                skipped_partial_files += 1
                continue
            m = LEAD_NPY_RE.match(path.name)
            if not m:
                continue
            # Precipitation only. Since 2026-05-21 every init also carries
            # f{hh}_temp/_wind/_humidity .npy files; folding °C / m/s into the
            # precip accumulators would permanently corrupt them. Temp/wind/
            # humidity verification is future work with a different reference
            # dataset (PRISM ppt is precip-only).
            if m.group("level") != "surface":
                continue
            fhour = int(m.group("fhour"))
            lead_days = fhour // 24
            if lead_days < LEAD_DAYS_MIN or lead_days > LEAD_DAYS_MAX:
                continue
            if lead_days in done_leads:
                continue  # already ingested; skip to avoid double-counting

            valid_date = init_date + timedelta(days=lead_days)
            prism_tif = _get_prism_tif_path(valid_date)
            if not prism_tif.exists():
                continue
            tasks.append(
                StatsTask(
                    npy_path=path,
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
    gfs_data, gfs_lats, gfs_lons = _read_model_npy(task.npy_path)
    gfs_tf = grid_transform(gfs_lats, gfs_lons)
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
    accumulators: MonthAccumulators,
    month: int,
    lead_days: int,
    stat_accs: Dict[str, Dict[str, np.ndarray]],
) -> None:
    """Merge a single task's accumulator deltas into the running totals."""
    if month not in accumulators:
        accumulators[month] = {}
    month_acc = accumulators[month]
    lead_stats = month_acc.get(lead_days)
    if lead_stats is None:
        month_acc[lead_days] = {
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
    model_key: str,
    year_init_dirs: list[tuple[int, list[Path]]],
    processed_pairs: dict[str, set[int]] | None = None,
) -> Tuple[
    MonthAccumulators,
    Tuple[float, float, float, float, float, float] | None,
    Tuple[int, int] | None,
    int,
    int,
    dict[str, set[int]],
]:
    """Process a chunk of years. Runs in a child process.

    Uses a ThreadPoolExecutor internally to overlap I/O (rasterio reproject
    releases the GIL). The reprojection cache is local to this process and
    shared safely across threads under CPython's GIL.

    Re-applies model configuration in the child so ``GFS_GRID_LATS_PATH`` /
    ``GFS_DIR`` match *model_key* (required when the process start method is
    ``spawn``, which re-imports this module and resets globals to defaults).

    ``processed_pairs`` ({init_name: {leads}}) is forwarded to the task builder
    to skip already-ingested pairs.

    Returns (accumulators, transform_tuple, grid_shape, task_count, skipped,
    ingested_pairs). ``ingested_pairs`` ({init_name: {leads}}) is exactly the
    (init, lead) pairs successfully folded in — failed tasks are absent so they
    retry. accumulators is month-keyed: {month: {lead: {stat: {key: array}}}}.
    """
    _configure_for_model(model_key)
    cache = _ReprojectionCache()
    accumulators: MonthAccumulators = {}
    grid_transform_tuple: Tuple[float, float, float, float, float, float] | None = None
    grid_shape: Tuple[int, int] | None = None
    total_tasks = 0
    total_skipped = 0
    ingested_pairs: dict[str, set[int]] = defaultdict(set)

    # Collect all tasks across years in this chunk, preserving date order.
    all_tasks: list[StatsTask] = []
    for _year, init_dirs in year_init_dirs:
        tasks, skipped = _build_tasks_for_inits(init_dirs, processed_pairs)
        all_tasks.extend(tasks)
        total_skipped += skipped

    if not all_tasks:
        return accumulators, grid_transform_tuple, grid_shape, 0, total_skipped, {}

    def process_task(task: StatsTask):
        return task, _process_single_task(task, cache)

    with ThreadPoolExecutor(max_workers=THREADS_PER_PROCESS) as executor:
        futures = {executor.submit(process_task, t): t for t in all_tasks}
        for future in as_completed(futures):
            try:
                task, (lead_days, stat_accs, t_tuple, shape) = future.result()
            except Exception as exc:
                failed_task = futures[future]
                print(f"  WARNING: skipping {failed_task.npy_path} "
                      f"(valid {failed_task.valid_date.date()}): {exc}")
                continue

            # Validate grid consistency.
            if grid_transform_tuple is None:
                grid_transform_tuple = t_tuple
                grid_shape = shape
            elif grid_transform_tuple != t_tuple or grid_shape != shape:
                raise RuntimeError(
                    "GFS grid mismatch across dates within process."
                )

            month = task.valid_date.month
            _merge_task_result(accumulators, month, lead_days, stat_accs)
            ingested_pairs[task.npy_path.parent.name].add(task.lead_days)
            total_tasks += 1

    return (
        accumulators, grid_transform_tuple, grid_shape,
        total_tasks, total_skipped, dict(ingested_pairs),
    )


def _merge_lead_accumulators(
    results: list[Dict[int, Dict[str, Dict[str, np.ndarray]]]],
) -> Dict[int, Dict[str, Dict[str, np.ndarray]]]:
    """Merge lead-keyed accumulators by summing arrays."""
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


def _merge_chunk_accumulators(
    results: list[MonthAccumulators],
) -> MonthAccumulators:
    """Merge month-keyed accumulators from multiple process chunks."""
    merged: MonthAccumulators = {}
    for chunk_acc in results:
        for month, lead_stats in chunk_acc.items():
            if month not in merged:
                merged[month] = {
                    lead: {
                        stat: {k: v.copy() for k, v in acc.items()}
                        for stat, acc in stats.items()
                    }
                    for lead, stats in lead_stats.items()
                }
            else:
                for lead, stats in lead_stats.items():
                    if lead not in merged[month]:
                        merged[month][lead] = {
                            stat: {k: v.copy() for k, v in acc.items()}
                            for stat, acc in stats.items()
                        }
                    else:
                        for stat, acc in stats.items():
                            if stat not in merged[month][lead]:
                                merged[month][lead][stat] = {k: v.copy() for k, v in acc.items()}
                            else:
                                for key, arr in acc.items():
                                    merged[month][lead][stat][key] += arr
    return merged


def _load_manifest(output_root: Path) -> dict | None:
    """Load manifest.json from the stats output directory."""
    return stage_state(output_root, "verification")


def _manifest_processed_pairs(manifest: dict | None) -> dict[str, set[int]]:
    """Extract the {init_name: {leads}} map from a v2 manifest.

    Returns {} for a missing manifest or one predating schema v2, so callers can
    treat 'nothing processed yet' uniformly (a full recompute then re-populates
    it from scratch).
    """
    if not manifest or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        return {}
    return {init: set(leads) for init, leads in manifest.get("processed", {}).items()}


def _merge_pairs(dst: dict[str, set[int]], src: dict[str, set[int]]) -> None:
    """Union the (init, lead) pairs of *src* into *dst* in place."""
    for init, leads in src.items():
        dst.setdefault(init, set()).update(leads)


def _save_manifest(
    output_root: Path, processed_pairs: dict[str, set[int]],
    lead_min: int, lead_max: int,
) -> None:
    """Write manifest.json (schema v2) recording the exact (init, lead) pairs
    folded into the accumulators."""
    output_root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "storage": "monthly",
        "lead_days_min": lead_min,
        "lead_days_max": lead_max,
        "processed": {
            init: sorted(leads) for init, leads in sorted(processed_pairs.items())
        },
        "algorithm_fingerprint": fingerprint_payload({
            "schema": MANIFEST_SCHEMA_VERSION,
            "statistics": [plugin.spec.name for plugin in VERIFICATION_STATISTICS],
            "lead_min": lead_min,
            "lead_max": lead_max,
        }),
    }
    update_stage(output_root, "verification", manifest, invalidate=("basin", "episodes"))


def _load_existing_accumulators(
    output_root: Path,
) -> MonthAccumulators | None:
    """Load raw accumulators from existing monthly .npz files.

    Returns the nested dict {month: {lead: {stat: {key: array}}}} or None if
    files are missing or incompatible.
    """
    accumulators: MonthAccumulators = {}
    for plugin in VERIFICATION_STATISTICS:
        acc_keys = set(plugin.init_accumulator((1, 1)).keys())
        stat_dir = output_root / plugin.spec.name
        monthly_dir = stat_dir / "monthly"
        if not monthly_dir.exists():
            print(f"  No monthly directory for '{plugin.spec.name}', "
                  "falling back to full recompute.")
            return None
        for month_dir in sorted(monthly_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue
            month = int(month_dir.name)
            for npz_path in sorted(month_dir.glob("lead_*.npz")):
                stem = npz_path.stem.removeprefix("lead_")
                if not stem.isdigit():
                    continue
                lead = int(stem)
                with np.load(npz_path) as data:
                    missing = acc_keys - set(data.files)
                    if missing:
                        print(f"  Missing accumulator keys {missing} in "
                              f"monthly/{month_dir.name}/{npz_path.name} "
                              f"for '{plugin.spec.name}'. Falling back to full recompute.")
                        return None
                    lead_acc = {k: data[k].copy() for k in acc_keys}
                if month not in accumulators:
                    accumulators[month] = {}
                if lead not in accumulators[month]:
                    accumulators[month][lead] = {}
                accumulators[month][lead][plugin.spec.name] = lead_acc
    return accumulators


def _compute_lead_stats(model_key: str) -> Tuple[
    MonthAccumulators, GridMeta, int, dict[str, set[int]]
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
    chunk_accumulators: list[MonthAccumulators] = []
    ingested_pairs: dict[str, set[int]] = {}
    grid_transform_tuple: Tuple[float, float, float, float, float, float] | None = None
    grid_shape: Tuple[int, int] | None = None

    with ProcessPoolExecutor(max_workers=len(chunks)) as pool:
        futures = {
            pool.submit(_compute_years_chunk, model_key, chunk): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            chunk_idx = futures[future]
            acc, t_tuple, shape, task_count, skipped, chunk_pairs = future.result()
            total_tasks += task_count
            total_skipped += skipped
            _merge_pairs(ingested_pairs, chunk_pairs)

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

    return merged, grid_meta, total_skipped, ingested_pairs


def _compute_lead_stats_incremental(model_key: str) -> Tuple[
    MonthAccumulators | None, GridMeta | None, int, dict[str, set[int]] | None
]:
    """Incremental update: load existing accumulators and fold in every
    (init, lead) pair whose PRISM is now on disk but that the manifest has not
    yet recorded. Unlike the old whole-init scan, this picks up late-arriving
    PRISM for *old* inits — an init that contributed zero tasks because its
    PRISM days were missing is revisited once they land, instead of being
    permanently marked done."""
    manifest = _load_manifest(OUTPUT_ROOT)
    if manifest is None:
        raise RuntimeError(
            f"No manifest found at {OUTPUT_ROOT / 'manifest.json'}. "
            "Run without --incremental first to create one."
        )

    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise RuntimeError(
            "Manifest predates per-(init, lead) tracking (schema v2). "
            "Run without --incremental to recompute from scratch."
        )

    if (manifest["lead_days_min"] != LEAD_DAYS_MIN
            or manifest["lead_days_max"] != LEAD_DAYS_MAX):
        raise RuntimeError(
            f"Lead range changed (manifest: {manifest['lead_days_min']}-{manifest['lead_days_max']}, "
            f"current: {LEAD_DAYS_MIN}-{LEAD_DAYS_MAX}). "
            "Run without --incremental to recompute from scratch."
        )

    existing_acc = _load_existing_accumulators(OUTPUT_ROOT)
    if existing_acc is None:
        raise RuntimeError(
            "Could not load existing accumulators. "
            "Run without --incremental to recompute from scratch."
        )

    processed_pairs = _manifest_processed_pairs(manifest)
    all_init_dirs = _list_gfs_inits()

    # Cheap pre-scan (globs + stat only, no array loads) to see whether any
    # unprocessed (init, lead) with PRISM now on disk remains. Also tells us
    # which years actually have work, so we don't spawn idle processes.
    pending, _ = _build_tasks_for_inits(all_init_dirs, processed_pairs)
    if not pending:
        print("No new (init, lead) tasks to process. Statistics are up to date.")
        return None, None, 0, None

    pending_years = {int(t.npy_path.parent.parent.name) for t in pending}
    relevant_init_dirs = [d for d in all_init_dirs if int(d.parent.name) in pending_years]
    print(f"Found {len(pending)} new (init, lead) task(s) across "
          f"{len(pending_years)} year(s) to process.")

    by_year = _group_inits_by_year(relevant_init_dirs)
    year_items = list(by_year.items())
    num_workers = min(len(year_items), max(1, os.cpu_count() or 1))
    chunks = _distribute_years(year_items, num_workers)

    print(
        f"Dispatching to {len(chunks)} process(es) "
        f"with {THREADS_PER_PROCESS} threads each..."
    )

    total_tasks = 0
    total_skipped = 0
    chunk_accumulators: list[MonthAccumulators] = []
    ingested_pairs: dict[str, set[int]] = {}
    grid_transform_tuple: Tuple[float, float, float, float, float, float] | None = None
    grid_shape: Tuple[int, int] | None = None

    with ProcessPoolExecutor(max_workers=len(chunks)) as pool:
        futures = {
            pool.submit(_compute_years_chunk, model_key, chunk, processed_pairs): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            chunk_idx = futures[future]
            acc, t_tuple, shape, task_count, skipped, chunk_pairs = future.result()
            total_tasks += task_count
            total_skipped += skipped
            _merge_pairs(ingested_pairs, chunk_pairs)

            if task_count > 0:
                chunk_accumulators.append(acc)
                if grid_transform_tuple is None:
                    grid_transform_tuple = t_tuple
                    grid_shape = shape
                elif grid_transform_tuple != t_tuple or grid_shape != shape:
                    raise RuntimeError("GFS grid mismatch across processes.")

            chunk_years = ", ".join(str(y) for y, _ in chunks[chunk_idx])
            print(f"  Chunk [{chunk_years}] done: {task_count} tasks")

    print(f"New tasks processed: {total_tasks}")

    if chunk_accumulators:
        new_acc = _merge_chunk_accumulators(chunk_accumulators)
        merged = _merge_chunk_accumulators([existing_acc, new_acc])
    else:
        merged = existing_acc

    if grid_transform_tuple is not None and grid_shape is not None:
        grid_meta = _validate_or_build_grid_meta(
            None, grid_transform_tuple, "EPSG:4326", grid_shape
        )
    else:
        # No new tasks produced grid info; load model-level metadata from disk.
        grid_meta = _grid_meta_from_saved_metadata(OUTPUT_ROOT)

    # New manifest state = everything previously recorded ∪ what we just folded in.
    combined_pairs = {init: set(leads) for init, leads in processed_pairs.items()}
    _merge_pairs(combined_pairs, ingested_pairs)

    return merged, grid_meta, total_skipped, combined_pairs


def _sum_lead_accumulators(
    month_acc: MonthAccumulators,
    months: tuple[int, ...],
    stat_name: str,
) -> Dict[int, Dict[str, np.ndarray]]:
    """Sum per-lead accumulators across the given months for one stat."""
    combined: Dict[int, Dict[str, np.ndarray]] = {}
    for m in months:
        if m not in month_acc:
            continue
        for lead, stats in month_acc[m].items():
            acc = stats.get(stat_name)
            if acc is None:
                continue
            if lead not in combined:
                combined[lead] = {k: v.copy() for k, v in acc.items()}
            else:
                for k, v in acc.items():
                    combined[lead][k] += v
    return combined


def _write_lead_files(
    stat_dir: Path,
    plugin,
    lead_accs: Dict[int, Dict[str, np.ndarray]],
) -> None:
    """Write one per-lead .npz file for each integer daily lead."""
    for lead in sorted(lead_accs):
        outputs = plugin.finalize(lead_accs[lead])
        atomic_savez(stat_dir / f"lead_{lead}.npz", **outputs)


def _write_stats(
    accumulators: MonthAccumulators,
    grid_meta: GridMeta,
) -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    save_model_metadata(
        OUTPUT_ROOT,
        lats=grid_meta.lats,
        lons=grid_meta.lons,
        transform=np.array(grid_meta.transform),
        crs=grid_meta.crs,
    )

    for plugin in VERIFICATION_STATISTICS:
        stat_name = plugin.spec.name
        stat_dir = OUTPUT_ROOT / stat_name
        stat_dir.mkdir(parents=True, exist_ok=True)

        # 1. Monthly .npz files (the primitive).
        for month in sorted(accumulators):
            month_dir = stat_dir / "monthly" / f"{month:02d}"
            month_dir.mkdir(parents=True, exist_ok=True)
            month_leads = _sum_lead_accumulators(accumulators, (month,), stat_name)
            _write_lead_files(month_dir, plugin, month_leads)

        # 2. Yearly .npz files (sum all months).
        all_months = tuple(sorted(accumulators.keys()))
        yearly_leads = _sum_lead_accumulators(accumulators, all_months, stat_name)
        _write_lead_files(stat_dir, plugin, yearly_leads)

        # 3. Seasonal .npz files.
        seasonal_dir = stat_dir / "seasonal"
        for season_name, season_months in SEASONS.items():
            if not any(m in accumulators for m in season_months):
                continue
            season_dir = seasonal_dir / season_name
            season_dir.mkdir(parents=True, exist_ok=True)
            season_leads = _sum_lead_accumulators(accumulators, season_months, stat_name)
            _write_lead_files(season_dir, plugin, season_leads)




def _check_stats_complete() -> bool:
    """Check whether all expected stat outputs exist on disk.

    Verifies that each plugin has monthly subdirectories with lead .npz
    files containing the required accumulator keys. At least one month
    must have all expected leads. Yearly/seasonal are derived, so only
    monthly is the source of truth.
    """
    expected_leads = set(range(LEAD_DAYS_MIN, LEAD_DAYS_MAX + 1))
    for plugin in VERIFICATION_STATISTICS:
        acc_keys = set(plugin.init_accumulator((1, 1)).keys())
        stat_dir = OUTPUT_ROOT / plugin.spec.name
        monthly_dir = stat_dir / "monthly"
        if not monthly_dir.exists():
            print(f"  Missing monthly directory: {stat_dir.name}/monthly/")
            return False
        month_dirs = [d for d in monthly_dir.iterdir()
                      if d.is_dir() and d.name.isdigit()]
        if not month_dirs:
            print(f"  No month subdirectories in {stat_dir.name}/monthly/")
            return False
        for month_dir in month_dirs:
            found_leads: set[int] = set()
            for npz_path in month_dir.glob("lead_*.npz"):
                stem = npz_path.stem.removeprefix("lead_")
                if not stem.isdigit():
                    continue
                lead = int(stem)
                if lead not in expected_leads:
                    continue
                with np.load(npz_path) as data:
                    if acc_keys - set(data.files):
                        print(f"  Incompatible .npz (missing accumulator keys): "
                              f"{stat_dir.name}/monthly/{month_dir.name}/{npz_path.name}")
                        return False
                found_leads.add(lead)
            missing = expected_leads - found_leads
            if missing:
                print(f"  Missing lead file(s) in {stat_dir.name}/monthly/{month_dir.name}/: "
                      f"{', '.join(f'lead_{l}.npz' for l in sorted(missing))}")
                return False
    return True


def _can_do_incremental() -> bool:
    """Check whether incremental update is possible.

    Requires: manifest exists with monthly storage, lead range matches,
    and all stat outputs are complete on disk.
    """
    manifest = _load_manifest(OUTPUT_ROOT)
    if manifest is None:
        return False
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        print("  Manifest predates per-(init, lead) tracking (schema v2). "
              "Full recompute required.")
        return False
    if manifest.get("storage") != "monthly":
        print("  Old manifest without monthly storage. Full recompute required.")
        return False
    if (manifest["lead_days_min"] != LEAD_DAYS_MIN
            or manifest["lead_days_max"] != LEAD_DAYS_MAX):
        print("  Lead range changed since last run.")
        return False
    return _check_stats_complete()


def run_verification_stage(model_key: str = DEFAULT_MODEL, force: bool = False) -> None:
    _configure_for_model(model_key)
    stat_names = ", ".join(plugin.spec.name for plugin in VERIFICATION_STATISTICS)
    print(
        f"Computing statistics for model '{model_key}'. "
        f"Enabled statistics: {stat_names}"
    )

    if not force and _can_do_incremental():
        print("Existing stats complete, checking for new data...")
        accumulators, grid_meta, skipped_partial_files, processed_pairs = (
            _compute_lead_stats_incremental(model_key)
        )
        if accumulators is None:
            return
    else:
        print("Full recompute required.")
        accumulators, grid_meta, skipped_partial_files, processed_pairs = (
            _compute_lead_stats(model_key)
        )
    _write_stats(accumulators, grid_meta)
    _save_manifest(OUTPUT_ROOT, processed_pairs, LEAD_DAYS_MIN, LEAD_DAYS_MAX)
    if skipped_partial_files:
        print(f"Skipped {skipped_partial_files} partial files (*.part).")
    print(f"Wrote statistics to {OUTPUT_ROOT.resolve()}")
