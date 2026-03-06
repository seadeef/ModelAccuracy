#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

from stats_query import DEFAULT_STATS_ROOT, stats_at_point

DEFAULT_STATS_ROOT_DIR = DEFAULT_STATS_ROOT


# Backward-compatible helper for callers that still import bias_at_point.
def bias_at_point(
    lat: float, lon: float, lead_days: int, stats_root: Path = DEFAULT_STATS_ROOT_DIR
) -> float:
    values = stats_at_point(
        lat,
        lon,
        lead_days,
        stats_root=stats_root,
        stat_names=["bias"],
    )
    value = values["bias"]["value"]
    if value is None:
        raise ValueError("No bias value at this location/lead.")
    return float(value)
