#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import re
from functools import lru_cache
from pathlib import Path

import numpy as np

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from lead_windows import build_lead_options, normalize_lead_key
from lead_config import LEAD_DAYS_MAX, LEAD_DAYS_MIN
from statistics_plugins.registry import STATISTICS_BY_NAME
from stats_query import DEFAULT_STATS_ROOT, stats_at_point

MAPTILER_API_KEY = os.getenv("MAPTILER_API_KEY", "")
MAPTILER_STYLE_ID = "streets-v2"
ZIP_LOOKUP_CSV = Path(os.getenv("ZIP_LOOKUP_CSV", "zip_lookup.csv"))
TILES_OUTPUT = Path(os.getenv("TILES_OUTPUT", "tiles_output"))
US_CROP_BOUNDS = [-125.125, 23.875, -65.875, 50.125]  # pixel-snapped to 0.25° GFS grid

app = FastAPI(title="Model Statistics Query API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://127.0.0.1:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve pre-rendered PNG images for image-mode statistics.
_images_dir = TILES_OUTPUT / "images"
if _images_dir.exists():
    app.mount("/images", StaticFiles(directory=str(_images_dir)), name="images")


def _normalize_zip(zip_code: str) -> str | None:
    digits = re.sub(r"\D", "", zip_code)
    if len(digits) < 5:
        return None
    return digits[:5]


@lru_cache(maxsize=1)
def _load_zip_lookup(path_str: str) -> dict[str, dict[str, float]]:
    path = Path(path_str)
    if not path.exists():
        return {}

    lookup: dict[str, dict[str, float]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            zip_code = _normalize_zip(row.get("zip", ""))
            if not zip_code:
                continue
            try:
                lookup[zip_code] = {
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "min_lon": float(row["min_lon"]),
                    "min_lat": float(row["min_lat"]),
                    "max_lon": float(row["max_lon"]),
                    "max_lat": float(row["max_lat"]),
                }
            except (KeyError, TypeError, ValueError):
                continue
    return lookup


def _get_forecast_init_date() -> str | None:
    meta_path = DEFAULT_STATS_ROOT / "forecast" / "metadata.npz"
    if not meta_path.exists():
        return None
    try:
        meta = np.load(meta_path, allow_pickle=True)
        return str(meta["init_date"])
    except (KeyError, Exception):
        return None


@app.get("/api/stats")
def get_stats(
    lead: str = Query(..., pattern=r"^\d+([_-]\d+)?$"),
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    stats_root: str | None = None,
):
    lead_key = normalize_lead_key(lead)
    stats_path = Path(stats_root) if stats_root else DEFAULT_STATS_ROOT
    values = stats_at_point(lat, lon, lead_key, stats_root=stats_path)
    return {
        "lead": lead_key,
        "lat": lat,
        "lon": lon,
        "stats": values,
    }


@app.get("/api/config")
def get_config():
    stat_info = {}
    for name, plugin in sorted(STATISTICS_BY_NAME.items()):
        info: dict = {"tile_mode": plugin.spec.tile_mode}
        stat_info[name] = info

    forecast_init_date = _get_forecast_init_date()

    return {
        "lead_days_min": LEAD_DAYS_MIN,
        "lead_days_max": LEAD_DAYS_MAX,
        "maptiler_api_key": MAPTILER_API_KEY,
        "maptiler_style_id": MAPTILER_STYLE_ID,
        "statistics": stat_info,
        "default_statistic": "bias",
        "lead_options": build_lead_options(LEAD_DAYS_MIN, LEAD_DAYS_MAX),
        "image_bounds": US_CROP_BOUNDS,
        "forecast_init_date": forecast_init_date,
    }


@app.get("/api/zip")
def get_zip(zip_code: str = Query(..., alias="zip")):
    normalized = _normalize_zip(zip_code)
    if normalized is None:
        return {"zip": zip_code, "found": False, "error": "Invalid ZIP format."}

    lookup = _load_zip_lookup(str(ZIP_LOOKUP_CSV))
    record = lookup.get(normalized)
    if record is None:
        return {"zip": normalized, "found": False}

    return {
        "zip": normalized,
        "found": True,
        "lat": record["lat"],
        "lon": record["lon"],
        "bounds": [
            record["min_lon"],
            record["min_lat"],
            record["max_lon"],
            record["max_lat"],
        ],
    }
