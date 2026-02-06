#!/usr/bin/env python3
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

import numpy as np

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
GFS_VAR = "APCP"
PRISM_VAR = "ppt"
STATS_VAR = PRISM_VAR

GFS_DIR = Path("model_data") / GFS_MODEL / GFS_VAR
PRISM_DIR = Path("prism_data") / PRISM_VAR
OUTPUT_DIR = Path("stats") / "bias" / STATS_VAR

GFS_FILE_RE = re.compile(r"f(?P<fhour>\d{3})_(?P<level>[^.]+)\.grib2")


@dataclass(frozen=True)
class GridMeta:
    transform: rasterio.Affine
    crs: str
    lats: np.ndarray
    lons: np.ndarray


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


def _reproject_prism_to_gfs(
    prism_path: Path, gfs_shape: Tuple[int, int], gfs_transform: rasterio.Affine
) -> np.ndarray:
    with rasterio.open(prism_path) as src:
        dst = np.full(gfs_shape, np.nan, dtype=np.float32)
        reproject(
            source=rasterio.band(src, 1),
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=gfs_transform,
            dst_crs="EPSG:4326",
            resampling=Resampling.average,
            dst_nodata=np.nan,
        )
        return dst


def _compute_week_lead_stats() -> Tuple[Dict[Tuple[int, int], np.ndarray], Dict[Tuple[int, int], np.ndarray], GridMeta]:
    sum_diff: Dict[Tuple[int, int], np.ndarray] = {}
    count: Dict[Tuple[int, int], np.ndarray] = {}
    grid_meta: GridMeta | None = None

    for init_dir in _list_gfs_inits():
        init_date = _parse_init_date(init_dir.name)
        week = int(init_date.isocalendar().week)

        for grib_path in sorted(init_dir.iterdir()):
            if not grib_path.is_file():
                continue
            match = GFS_FILE_RE.match(grib_path.name)
            if not match:
                continue
            fhour = int(match.group("fhour"))
            lead_days = fhour // 24
            if lead_days < 1 or lead_days > 7:
                continue

            valid_date = init_date + timedelta(days=lead_days)
            prism_tif = _get_prism_tif_path(valid_date)
            if not prism_tif.exists():
                continue

            gfs_data, lats, lons = _read_gfs_apcp(grib_path)
            if grid_meta is None:
                grid_meta = GridMeta(
                    transform=_gfs_transform(lats, lons),
                    crs="EPSG:4326",
                    lats=lats,
                    lons=lons,
                )

            prism_on_gfs = _reproject_prism_to_gfs(
                prism_tif, gfs_data.shape, grid_meta.transform
            )

            diff = gfs_data - prism_on_gfs
            key = (week, lead_days)

            if key not in sum_diff:
                sum_diff[key] = np.zeros_like(diff, dtype=np.float32)
                count[key] = np.zeros_like(diff, dtype=np.int32)

            valid_mask = np.isfinite(diff)
            sum_diff[key][valid_mask] += diff[valid_mask]
            count[key][valid_mask] += 1

    if grid_meta is None:
        raise RuntimeError("No GFS/PRISM overlaps found. Check data paths.")

    return sum_diff, count, grid_meta


def _write_tiles(
    sum_diff: Dict[Tuple[int, int], np.ndarray],
    count: Dict[Tuple[int, int], np.ndarray],
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

    for (week, lead), s in sum_diff.items():
        c = count[(week, lead)]
        bias_mean = np.full_like(s, np.nan, dtype=np.float32)
        valid_mask = c > 0
        bias_mean[valid_mask] = s[valid_mask] / c[valid_mask]

        out_dir = OUTPUT_DIR / f"week_{week:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"lead_{lead}.npz"
        np.savez_compressed(
            out_path,
            bias_mean=bias_mean,
            sum_diff=s,
            sample_count=c,
        )


def bias_at_point(lat: float, lon: float, week: int, lead_days: int) -> float:
    meta = np.load(OUTPUT_DIR / "metadata.npz", allow_pickle=False)
    lats = meta["lats"]
    lons = meta["lons"]

    lat_idx = int(np.argmin(np.abs(lats - lat)))
    lon_idx = int(np.argmin(np.abs(lons - lon)))

    tile_path = OUTPUT_DIR / f"week_{week:02d}" / f"lead_{lead_days}.npz"
    tile = np.load(tile_path, allow_pickle=False)
    return float(tile["bias_mean"][lat_idx, lon_idx])


def main() -> None:
    sum_diff, count, grid_meta = _compute_week_lead_stats()
    _write_tiles(sum_diff, count, grid_meta)
    print(f"Wrote tiles to {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
