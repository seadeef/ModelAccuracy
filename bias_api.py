#!/usr/bin/env python3
from __future__ import annotations

import math
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from bias_query import bias_at_point, DEFAULT_STATS_DIR

app = FastAPI(title="Bias Query API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://127.0.0.1:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/bias")
def get_bias(
    week: int = Query(..., ge=1, le=53),
    lead: int = Query(..., ge=1, le=7),
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    stats_dir: str | None = None,
):
    stats_path = Path(stats_dir) if stats_dir else DEFAULT_STATS_DIR
    value = bias_at_point(lat, lon, week, lead, stats_dir=stats_path)
    if math.isnan(value):
        return {"value": None, "units": "mm", "no_data": True}
    return {"value": value, "units": "mm", "no_data": False}
