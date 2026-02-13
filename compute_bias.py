#!/usr/bin/env python3
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
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


def _season_from_date(date: datetime) -> str:
    month = date.month
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "fall"


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


def _compute_season_lead_stats() -> Tuple[Dict[Tuple[str, int], np.ndarray], Dict[Tuple[str, int], np.ndarray], GridMeta]:
    sum_diff: Dict[Tuple[str, int], np.ndarray] = {}
    count: Dict[Tuple[str, int], np.ndarray] = {}
    grid_meta: GridMeta | None = None

    for init_dir in _list_gfs_inits():
        init_date = _parse_init_date(init_dir.name)

        for grib_path in sorted(init_dir.iterdir()):
            if not grib_path.is_file():
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
            season = _season_from_date(valid_date)

            gfs_data, gfs_lats, gfs_lons = _read_gfs_apcp(grib_path)
            gfs_transform = _gfs_transform(gfs_lats, gfs_lons)

            prism_data, prism_transform, prism_crs = _read_prism_ppt(prism_tif)
            if grid_meta is None:
                prism_lats, prism_lons = _grid_coords_from_transform(
                    prism_transform, prism_data.shape[0], prism_data.shape[1]
                )
                grid_meta = GridMeta(
                    transform=prism_transform,
                    crs=prism_crs,
                    lats=prism_lats,
                    lons=prism_lons,
                )
            else:
                if (
                    prism_transform != grid_meta.transform
                    or prism_crs != grid_meta.crs
                    or prism_data.shape != (grid_meta.lats.size, grid_meta.lons.size)
                ):
                    raise RuntimeError(
                        f"PRISM grid mismatch for {prism_tif}. "
                        "Expected consistent PRISM grid across dates."
                    )

            gfs_on_prism = _reproject_gfs_to_prism(
                gfs_data,
                gfs_transform,
                prism_data.shape,
                prism_transform,
                prism_crs,
            )

            diff = gfs_on_prism - prism_data
            key = (season, lead_days)

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
    sum_diff: Dict[Tuple[str, int], np.ndarray],
    count: Dict[Tuple[str, int], np.ndarray],
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

    for (season, lead), s in sum_diff.items():
        c = count[(season, lead)]
        bias_mean = np.full_like(s, np.nan, dtype=np.float32)
        valid_mask = c > 0
        bias_mean[valid_mask] = s[valid_mask] / c[valid_mask]

        out_dir = OUTPUT_DIR / f"season_{season}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"lead_{lead}.npz"
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
    sum_diff, count, grid_meta = _compute_season_lead_stats()
    _write_tiles(sum_diff, count, grid_meta)
    print(f"Wrote tiles to {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
