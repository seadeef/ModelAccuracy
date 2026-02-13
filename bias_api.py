#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import os
import re
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from bias_query import bias_at_point, DEFAULT_STATS_DIR
from lead_config import LEAD_DAYS_MAX, LEAD_DAYS_MIN

MAPTILER_API_KEY = os.getenv("MAPTILER_API_KEY", "")
MAPTILER_STYLE_ID = "streets-v2"
ZIP_LOOKUP_CSV = Path(os.getenv("ZIP_LOOKUP_CSV", "zip_lookup.csv"))

app = FastAPI(title="Bias Query API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://127.0.0.1:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


@app.get("/api/bias")
def get_bias(
    season: str = Query(..., pattern="^(winter|spring|summer|fall)$"),
    lead: int = Query(..., ge=LEAD_DAYS_MIN, le=LEAD_DAYS_MAX),
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    stats_dir: str | None = None,
):
    stats_path = Path(stats_dir) if stats_dir else DEFAULT_STATS_DIR
    value = bias_at_point(lat, lon, season, lead, stats_dir=stats_path)
    if math.isnan(value):
        return {"value": None, "units": "mm", "no_data": True}
    return {"value": value, "units": "mm", "no_data": False}


@app.get("/api/config")
def get_config():
    return {
        "lead_days_min": LEAD_DAYS_MIN,
        "lead_days_max": LEAD_DAYS_MAX,
        "maptiler_api_key": MAPTILER_API_KEY,
        "maptiler_style_id": MAPTILER_STYLE_ID,
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
