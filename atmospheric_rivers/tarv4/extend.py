"""Detect a date range and write the local AR-mask extension netCDF.

The output is a plain HDF5/netCDF4 file that atmospheric_rivers.dataset
normalizes exactly like the canonical virtual catalog (time/lat/lon
coordinates plus a ``shapemap`` variable whose nonzero cells are AR).
Masks are saved on a crop generous enough to contain the pipeline's
consumption bounds; detection itself runs on the full domain so crop-edge
objects keep their true extent.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import h5py
import numpy as np

from atmospheric_rivers.paths import (
    TARGET_AR_DATA_DIR, TARGET_AR_EXTENSION_FILENAME,
    TARGET_AR_EXTENSION_META_FILENAME,
)
from atmospheric_rivers.tarv4 import arco
from atmospheric_rivers.tarv4.climatology import DOMAIN_ROWS, DOMAIN_COLS
from atmospheric_rivers.tarv4.detect import (
    SGOLAY_WINDOW, Thresholds, build_grid, detect_step,
)

EXTENSION_FILENAME = TARGET_AR_EXTENSION_FILENAME
EXTENSION_META_FILENAME = TARGET_AR_EXTENSION_META_FILENAME
# Saved crop: superset of the pipeline's (-130, 20, -60, 55) bounds.
SAVE_LATS = slice_bounds = (60.0, 15.0)   # north, south
SAVE_LONS = (225.0, 305.0)                # 135W .. 55W
SYNOPTIC_HOURS = (0, 6, 12, 18)
TIME_EPOCH = "hours since 1900-01-01 00:00:00"


def _timestamps(start: str, end: str) -> list[np.datetime64]:
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    steps = []
    day = first
    while day <= last:
        for hour in SYNOPTIC_HOURS:
            steps.append(np.datetime64(f"{day.isoformat()}T{hour:02d}", "h"))
        day += timedelta(days=1)
    return steps


IVT_CACHE_DIR = TARGET_AR_DATA_DIR / "tarv4_local" / "ivt_cache"


def _domain_ivt(ts: np.datetime64) -> tuple[np.ndarray, np.ndarray]:
    """Domain-cropped (ivtx, ivty) for one step, disk-cached.

    The cache holds only the detection domain (~1.7 MB/step), so repeated
    validation/calibration runs do not re-stream full-globe chunks.
    """
    key = str(np.datetime64(ts, "h")).replace(":", "")
    path = IVT_CACHE_DIR / f"{key}.npz"
    if path.is_file():
        try:
            with np.load(path) as payload:
                return payload["ivtx"], payload["ivty"]
        except (OSError, ValueError, KeyError):
            path.unlink(missing_ok=True)
    index = arco.hour_index(ts)
    east = arco.fetch_field(arco.IVT_EAST, index)[DOMAIN_ROWS, DOMAIN_COLS]
    north = arco.fetch_field(arco.IVT_NORTH, index)[DOMAIN_ROWS, DOMAIN_COLS]
    IVT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".part")
    with temporary.open("wb") as output:
        np.savez_compressed(output, ivtx=east, ivty=north)
    temporary.replace(path)
    return east, north


def detect_range(start: str, end: str, *, workers: int = 24,
                 progress_every: int = 40):
    """Run detection; return (timestamps, masks on the save crop, lats, lons)."""
    from concurrent.futures import ThreadPoolExecutor

    grid = build_grid()
    thresholds = Thresholds()
    rows = np.flatnonzero((grid.lats <= SAVE_LATS[0])
                          & (grid.lats >= SAVE_LATS[1]))
    cols = np.flatnonzero((grid.lons >= SAVE_LONS[0])
                          & (grid.lons <= SAVE_LONS[1]))
    row_slice = slice(rows[0], rows[-1] + 1)
    col_slice = slice(cols[0], cols[-1] + 1)
    steps = _timestamps(start, end)
    masks = np.zeros((len(steps), rows.size, cols.size), dtype=np.uint8)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = []
        cursor = 0
        position = 0
        while cursor < len(steps) or pending:
            while cursor < len(steps) and len(pending) < 32:
                pending.append((steps[cursor],
                                executor.submit(_domain_ivt, steps[cursor])))
                cursor += 1
            ts, future = pending.pop(0)
            east, north = future.result()
            month = int(str(np.datetime64(ts, "M"))[5:7])
            union, _ = detect_step(east, north, month, thresholds, grid)
            masks[position] = union[row_slice, col_slice]
            position += 1
            if position % progress_every == 0:
                print(f"  detected {position}/{len(steps)} steps ({ts})",
                      flush=True)
    return steps, masks, grid.lats[row_slice], grid.lons[col_slice]


def write_extension(steps, masks, lats, lons, *, provenance: dict,
                    out_dir: Path = TARGET_AR_DATA_DIR) -> Path:
    hours = np.asarray(
        [(np.datetime64(ts, "h") - np.datetime64("1900-01-01T00", "h"))
         // np.timedelta64(1, "h") for ts in steps], dtype=np.float64)
    out_path = out_dir / EXTENSION_FILENAME
    temporary = out_path.with_name(out_path.name + ".part")
    with h5py.File(temporary, "w") as root:
        time = root.create_dataset("time", data=hours)
        time.attrs["units"] = np.bytes_(TIME_EPOCH)
        time.make_scale("time")
        lat = root.create_dataset("lat", data=np.asarray(lats, dtype=np.float64))
        lat.attrs["units"] = np.bytes_("degrees_north")
        lat.make_scale("lat")
        lon = root.create_dataset("lon", data=np.asarray(lons, dtype=np.float64))
        lon.attrs["units"] = np.bytes_("degrees_east")
        lon.make_scale("lon")
        shapemap = root.create_dataset(
            "shapemap", data=masks, dtype=np.uint8,
            chunks=(1, masks.shape[1], masks.shape[2]),
            compression="gzip", compression_opts=4)
        shapemap.dims[0].attach_scale(time)
        shapemap.dims[1].attach_scale(lat)
        shapemap.dims[2].attach_scale(lon)
        shapemap.attrs["long_name"] = np.bytes_(
            "AR shape mask (local tARget v4 extension, non-canonical)")
        root.attrs["title"] = np.bytes_(
            "Local tARget v4 AR extension from ERA5/ERA5T IVT")
        root.attrs["provenance"] = np.bytes_(json.dumps(provenance))
    temporary.replace(out_path)
    meta_path = out_dir / EXTENSION_META_FILENAME
    meta_temporary = meta_path.with_name(meta_path.name + ".part")
    meta_temporary.write_text(json.dumps(provenance, indent=2, sort_keys=True)
                              + "\n", encoding="utf-8")
    meta_temporary.replace(meta_path)
    return out_path


def run(start: str, end: str, *, workers: int = 24) -> Path:
    coverage = arco.store_time_coverage()
    if end > coverage["valid_time_stop_era5t"]:
        raise ValueError(
            f"requested end {end} exceeds ARCO-ERA5 coverage "
            f"({coverage['valid_time_stop_era5t']})")
    steps, masks, lats, lons = detect_range(start, end, workers=workers)
    thresholds = Thresholds()
    provenance = {
        "kind": "local_tarv4_extension",
        "canonical": False,
        "note": ("Locally generated with a Python port of the published "
                 "tARget v4 algorithm (doi:10.25346/S6/SF4AFW) on "
                 "ERA5/ERA5T IVT from ARCO-ERA5; NOT the canonical UCLA "
                 "Dataverse catalog release."),
        "start": start,
        "end": end,
        "frames": len(steps),
        "era5_final_through": coverage["valid_time_stop"],
        "era5t_preliminary_through": coverage["valid_time_stop_era5t"],
        "thresholds": thresholds.meta,
        "sgolay_window": SGOLAY_WINDOW,
        "validation": ("vs canonical catalog 2025-10-01..2025-12-31: "
                       "cellwise agreement 0.9923, pooled Jaccard 0.9307, "
                       "AR cell-hours +1.0%, CA basin active-day mismatches "
                       "16/1380 (isolated event-margin days); "
                       "target_ar_data/tarv4_local/validation/"),
        "generated_at": datetime.now(timezone.utc).isoformat(
            timespec="seconds"),
        "citations": [
            "Guan & Waliser 2015 doi:10.1002/2015JD024257",
            "Guan & Waliser 2024 doi:10.1038/s41597-024-03258-4",
        ],
    }
    out_path = write_extension(steps, masks, lats, lons,
                               provenance=provenance)
    ar_fraction = float(masks.any(axis=0).mean())
    print(f"wrote {out_path} ({len(steps)} frames; "
          f"{ar_fraction:.1%} of crop cells ever AR)")
    return out_path
