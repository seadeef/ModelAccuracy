#!/usr/bin/env python3
"""Per-cell daily forecast/observation cube for on-demand regional series.

The basin stage (``stats_pipeline.basin``) records daily forecast histories
for a fixed list of HUC6 basins; anything not in that list has no storm-level
analysis. This stage records the same history for EVERY land grid cell so the
backend can pool any region (state, HUC2, county, drawn shape) at request time
with the accuracy-pane idiom: cells whose centres fall inside the polygon,
equal weights.

Layout per model, under ``stats_output/{model}/cube/``:

* ``meta.json``       dates (YYYYMMDD), lead range, grid shape, dtype, windows
* ``cells.npy``       int32 flat indices (row-major lat×lon, the grid.json
                      order) of the land cells carried in the arrays
* ``forecasts.npy``   float16 (cells, dates, leads), mm/day, NaN = missing
* ``observed.npy``    float16 (cells, dates), PRISM area-averaged onto the
                      model grid, NaN = missing

Cell-major order keeps each cell's whole history contiguous (dates × leads ×
2 bytes ≈ 11 KB), so a memory-mapped fancy index reads only the region's
cells — and the same layout maps onto S3 byte-range reads later. "Land" is
every cell where PRISM is finite on at least one scorable date; ocean and
off-CONUS cells are dropped (about 35 % of the grid is kept).

Precision note: float16 holds 0.01 mm resolution below 16 mm and 0.25 mm at
500 mm — far finer than the per-cell forecast error — at half the size of
float32.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np

from model_registry import MODEL_REGISTRY
from stats_pipeline.consistency import prism_path, scan_inits, scorable_dates
from stats_pipeline.verification import grid_transform, read_prism, reproject_prism

SCHEMA_VERSION = 1
CUBE_DIRNAME = "cube"
META_FILENAME = "meta.json"
CELLS_FILENAME = "cells.npy"
FORECASTS_FILENAME = "forecasts.npy"
OBSERVED_FILENAME = "observed.npy"

# PRISM regridded onto a model grid, keyed by (grid signature, valid date).
# Several models share the 0.25° grid, so one pipeline run regrids each PRISM
# day once per distinct grid rather than once per model.
_PRISM_ON_GRID: dict[tuple[bytes, datetime], np.ndarray] = {}


def cube_dir(model_key: str, stats_root: Path = Path("stats_output")) -> Path:
    return stats_root / model_key / CUBE_DIRNAME


def _grid_signature(lats: np.ndarray, lons: np.ndarray) -> bytes:
    return np.asarray(lats, dtype=np.float64).tobytes() + np.asarray(lons, dtype=np.float64).tobytes()


def _prism_on_grid(valid_date: datetime, lats: np.ndarray, lons: np.ndarray,
                   signature: bytes) -> np.ndarray:
    key = (signature, valid_date)
    cached = _PRISM_ON_GRID.get(key)
    if cached is not None:
        return cached
    data, transform, crs = read_prism(prism_path(valid_date))
    if "4269" not in crs and "4326" not in crs:
        raise RuntimeError(f"expected a geographic PRISM grid, got {crs}")
    field = reproject_prism(data, transform, crs, (lats.size, lons.size),
                            grid_transform(lats, lons))
    _PRISM_ON_GRID[key] = field
    return field


def _window_meta(config) -> tuple[str | None, list[str | None]]:
    window = config.window_block()
    primary_label = (window.get("primary") or {}).get("label")
    primary_valid_range = [
        (d.replace("-", "") if isinstance(d, str) else None)
        for d in (window.get("primary_valid_range") or [None, None])
    ]
    return primary_label, primary_valid_range


def _up_to_date(out: Path, dates: list[str], lead_min: int, lead_max: int) -> bool:
    meta_path = out / META_FILENAME
    if not meta_path.is_file():
        return False
    for name in (CELLS_FILENAME, FORECASTS_FILENAME, OBSERVED_FILENAME):
        if not (out / name).is_file():
            return False
    try:
        meta = json.loads(meta_path.read_text())
    except (OSError, ValueError):
        return False
    return (meta.get("schema_version") == SCHEMA_VERSION
            and meta.get("dates") == dates
            and meta.get("lead_min") == lead_min
            and meta.get("lead_max") == lead_max)


def run_cube_stage(model_key: str, force: bool = False,
                   stats_root: Path = Path("stats_output")) -> Path:
    """Build (or refresh) one model's per-cell daily cube."""
    config = MODEL_REGISTRY[model_key]
    out = cube_dir(model_key, stats_root)
    data_dir = Path(config.data_dir)
    lats = np.load(data_dir / "grid_lats.npy")
    lons = np.load(data_dir / "grid_lons.npy")
    n_lat, n_lon = int(lats.size), int(lons.size)
    inits = scan_inits(data_dir, config.cycle_hour, config.lead_days_min, config.lead_days_max)
    scorable = scorable_dates(inits, config.lead_days_min, config.lead_days_max)
    ordered = sorted(scorable)
    dates = [d.strftime("%Y%m%d") for d in ordered]
    lead_min, lead_max = config.lead_days_min, config.lead_days_max
    n_leads = lead_max - lead_min + 1

    if not force and _up_to_date(out, dates, lead_min, lead_max):
        print(f"  {model_key}: cube is up to date ({len(dates)} dates)")
        return out
    if not dates:
        raise RuntimeError(f"{model_key}: no scorable dates (forecast archive or PRISM missing)")

    # Pass 1: PRISM on the model grid for every date → the land mask.
    print(f"  {model_key}: regridding PRISM for {len(dates)} dates")
    signature = _grid_signature(lats, lons)
    observed_full = np.full((len(dates), n_lat * n_lon), np.nan, dtype=np.float32)
    for di, valid_date in enumerate(ordered):
        observed_full[di] = _prism_on_grid(valid_date, lats, lons, signature).reshape(-1)
        if (di + 1) % 100 == 0:
            print(f"    ...{di + 1}/{len(dates)} PRISM days")
    land = np.any(np.isfinite(observed_full), axis=0)
    cells = np.flatnonzero(land).astype(np.int32)
    print(f"  {model_key}: {cells.size} land cells of {n_lat * n_lon}")

    # Pass 2: every forecast lead for every date, land cells only.
    forecasts = np.full((len(dates), n_leads, cells.size), np.nan, dtype=np.float32)
    for di, valid_date in enumerate(ordered):
        lead_paths = scorable[valid_date]
        for li, lead in enumerate(range(lead_min, lead_max + 1)):
            field = np.load(lead_paths[lead])
            if field.shape != (n_lat, n_lon):
                raise RuntimeError(
                    f"{lead_paths[lead]}: shape {field.shape} != grid ({n_lat}, {n_lon})")
            forecasts[di, li] = field.reshape(-1)[cells]
        if (di + 1) % 50 == 0:
            print(f"    ...{di + 1}/{len(dates)} forecast days")

    primary_label, primary_valid_range = _window_meta(config)
    out.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    if tmp.exists():
        for p in tmp.iterdir():
            p.unlink()
    tmp.mkdir(parents=True, exist_ok=True)
    np.save(tmp / CELLS_FILENAME, cells)
    # Cell-major on disk: (cells, dates, leads) and (cells, dates).
    np.save(tmp / FORECASTS_FILENAME,
            np.ascontiguousarray(forecasts.transpose(2, 0, 1)).astype(np.float16))
    np.save(tmp / OBSERVED_FILENAME,
            np.ascontiguousarray(observed_full[:, cells].T).astype(np.float16))
    meta = {
        "schema_version": SCHEMA_VERSION,
        "model": model_key,
        "dates": dates,
        "lead_min": lead_min,
        "lead_max": lead_max,
        "n_lat": n_lat,
        "n_lon": n_lon,
        "n_cells": int(cells.size),
        "dtype": "float16",
        "layout": {"forecasts": "(cells, dates, leads)", "observed": "(cells, dates)"},
        "units": "mm/day",
        "primary_window": primary_label,
        "primary_valid_range": primary_valid_range,
        "weighting": "equal weight over cells whose centre lies in the region (pooled at request time)",
        "observations": "PRISM area-averaged onto the model grid",
    }
    (tmp / META_FILENAME).write_text(json.dumps(meta, separators=(",", ":")))
    for name in (CELLS_FILENAME, FORECASTS_FILENAME, OBSERVED_FILENAME, META_FILENAME):
        (tmp / name).replace(out / name)
    tmp.rmdir()
    size_mb = sum((out / n).stat().st_size for n in (FORECASTS_FILENAME, OBSERVED_FILENAME)) / 1e6
    print(f"  {model_key}: wrote {out} ({len(dates)} dates × {n_leads} leads × "
          f"{cells.size} cells, {size_mb:.0f} MB)")
    return out


if __name__ == "__main__":  # pragma: no cover - manual rebuild helper
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=list(MODEL_REGISTRY), help="one model (default: all)")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    for key in ([args.model] if args.model else list(MODEL_REGISTRY)):
        run_cube_stage(key, force=args.force)
