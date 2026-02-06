#!/usr/bin/env python3
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import numpy as np

DEFAULT_STATS_DIR = Path("stats") / "bias" / "ppt"


@lru_cache(maxsize=1)
def _load_metadata(stats_dir_str: str) -> tuple[np.ndarray, np.ndarray]:
    stats_dir = Path(stats_dir_str)
    meta = np.load(stats_dir / "metadata.npz", allow_pickle=False)
    return meta["lats"], meta["lons"]


@lru_cache(maxsize=64)
def _load_bias(stats_dir_str: str, week: int, lead_days: int) -> np.ndarray:
    stats_dir = Path(stats_dir_str)
    tile_path = stats_dir / f"week_{week:02d}" / f"lead_{lead_days}.npz"
    tile = np.load(tile_path, allow_pickle=False)
    return tile["bias_mean"]


def bias_at_point(
    lat: float, lon: float, week: int, lead_days: int, stats_dir: Path = DEFAULT_STATS_DIR
) -> float:
    lats, lons = _load_metadata(str(stats_dir))
    lat_idx = int(np.argmin(np.abs(lats - lat)))
    lon_idx = int(np.argmin(np.abs(lons - lon)))
    bias = _load_bias(str(stats_dir), week, lead_days)
    return float(bias[lat_idx, lon_idx])
