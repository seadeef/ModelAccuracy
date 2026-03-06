#!/usr/bin/env python3
"""Extract raw GFS precipitation forecast into the stats/ format for tile generation."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import rasterio.transform
import xarray as xr

from lead_config import FORECAST_HOURS
from lead_windows import LEAD_WINDOWS, window_to_key

GFS_DIR = Path("model_data/gfs")
OUTPUT_ROOT = Path("stats")
GFS_CYCLE = 12


def _read_gfs_apcp(grib_path: Path):
    npy_path = grib_path.with_suffix(".npy")
    grid_lats = GFS_DIR / "grid_lats.npy"
    grid_lons = GFS_DIR / "grid_lons.npy"

    if npy_path.exists() and grid_lats.exists() and grid_lons.exists():
        return np.load(npy_path), np.load(grid_lats), np.load(grid_lons)

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


def _gfs_transform(lats, lons):
    lat_res = abs(float(lats[1] - lats[0]))
    lon_res = abs(float(lons[1] - lons[0]))
    west = float(lons.min()) - lon_res / 2.0
    north = float(lats.max()) + lat_res / 2.0
    return rasterio.transform.from_origin(west, north, lon_res, lat_res)


def _grid_coords_from_transform(transform, height, width):
    """Generate lat/lon arrays from transform (north-to-south), matching compute_stats.py."""
    lons = transform.c + (np.arange(width) + 0.5) * transform.a
    lats = transform.f + (np.arange(height) + 0.5) * transform.e
    return lats.astype(np.float32), lons.astype(np.float32)


def main():
    parser = argparse.ArgumentParser(description="Extract GFS forecast into stats format")
    parser.add_argument(
        "--date",
        help="Init date (YYYY-MM-DD). Defaults to most recent available.",
    )
    parser.add_argument(
        "--gfs-dir", default=str(GFS_DIR),
        help="GFS data directory",
    )
    parser.add_argument(
        "--output", default=str(OUTPUT_ROOT),
        help="Stats output directory",
    )
    args = parser.parse_args()

    gfs_dir = Path(args.gfs_dir)
    output_root = Path(args.output)
    forecast_dir = output_root / "forecast"
    forecast_dir.mkdir(parents=True, exist_ok=True)

    # Find init directory.
    if args.date:
        init_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        all_inits = sorted(gfs_dir.glob("*/*_12z"))
        if not all_inits:
            raise SystemExit("No GFS init directories found.")
        init_date = datetime.strptime(all_inits[-1].name[:8], "%Y%m%d")
        print(f"Using most recent init date: {init_date.date()}")

    date_str = init_date.strftime("%Y%m%d")
    init_dir = gfs_dir / str(init_date.year) / f"{date_str}_{GFS_CYCLE:02d}z"
    if not init_dir.exists():
        raise SystemExit(f"Init directory not found: {init_dir}")

    # Build land mask from an existing statistic's coverage (e.g. bias).
    land_mask = None
    for stat_dir in sorted(output_root.iterdir()):
        if stat_dir.name == "forecast" or not stat_dir.is_dir():
            continue
        for npz_file in stat_dir.glob("lead_*.npz"):
            with np.load(npz_file) as f:
                field = list(f.keys())[0]
                land_mask = np.isfinite(f[field])
            break
        if land_mask is not None:
            print(f"Land mask from {npz_file} ({np.count_nonzero(land_mask)} valid pixels)")
            break

    if land_mask is None:
        print("Warning: no existing statistics found for land mask. Forecast will include ocean.")

    # Read all available lead days.
    lead_data: dict[int, np.ndarray] = {}
    transform = None

    for fhour in FORECAST_HOURS:
        lead_days = fhour // 24
        grib_path = init_dir / f"f{fhour:03d}_surface.grib2"
        if not grib_path.exists():
            print(f"  Skipping lead {lead_days} (missing {grib_path.name})")
            continue

        data, lats, lons = _read_gfs_apcp(grib_path)
        if transform is None:
            transform = _gfs_transform(lats, lons)

        if land_mask is not None and data.shape == land_mask.shape:
            data[~land_mask] = np.nan

        lead_data[lead_days] = data
        print(f"  Lead {lead_days}: {grib_path.name}")

    if not lead_data or transform is None:
        raise SystemExit("No forecast hours found.")

    # Write metadata with lats derived from transform (north-to-south),
    # consistent with compute_stats.py.
    height, width = next(iter(lead_data.values())).shape
    meta_lats, meta_lons = _grid_coords_from_transform(transform, height, width)
    np.savez_compressed(
        forecast_dir / "metadata.npz",
        lats=meta_lats,
        lons=meta_lons,
        transform=np.array(transform),
        crs="EPSG:4326",
        init_date=init_date.strftime("%Y-%m-%d"),
    )

    # Write individual lead days.
    for lead_days, data in sorted(lead_data.items()):
        np.savez_compressed(forecast_dir / f"lead_{lead_days}.npz", precip=data)
        print(f"  Wrote lead_{lead_days}.npz")

    # Write lead windows (average precipitation across days in window).
    for start, end in LEAD_WINDOWS:
        leads_in_window = [ld for ld in sorted(lead_data) if start <= ld <= end]
        expected = end - start + 1
        if len(leads_in_window) != expected:
            print(f"  Skipping window {start}-{end} (have {len(leads_in_window)}/{expected} leads)")
            continue

        avg = np.mean([lead_data[ld] for ld in leads_in_window], axis=0)
        window_key = window_to_key(start, end)
        np.savez_compressed(forecast_dir / f"lead_{window_key}.npz", precip=avg)
        print(f"  Wrote lead_{window_key}.npz (avg of leads {start}-{end})")

    print(f"\nWrote {len(lead_data)} lead files + windows to {forecast_dir}")
    print(f"Init date: {init_date.date()} {GFS_CYCLE:02d}z")


if __name__ == "__main__":
    main()
