#!/usr/bin/env python3
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import numpy as np

from statistics_plugins.registry import STATISTICS_BY_NAME


@lru_cache(maxsize=64)
def _load_metadata(stats_root_str: str, stat_name: str) -> tuple[np.ndarray, np.ndarray]:
    stats_dir = Path(stats_root_str) / stat_name
    meta = np.load(stats_dir / "metadata.npz", allow_pickle=False)
    return meta["lats"], meta["lons"]


@lru_cache(maxsize=256)
def _load_stat_values(stats_root_str: str, stat_name: str, lead_key: str, field: str) -> np.ndarray:
    stats_dir = Path(stats_root_str) / stat_name
    tile = np.load(stats_dir / f"lead_{lead_key}.npz", allow_pickle=False)
    return tile[field]


def stats_at_point(
    lat: float,
    lon: float,
    lead: str | int,
    stats_root: Path = Path("stats"),
    stat_names: list[str] | None = None,
) -> dict[str, dict[str, float | None | str | bool]]:
    lead_key = str(lead)
    names = stat_names or sorted(STATISTICS_BY_NAME)
    result: dict[str, dict[str, float | None | str | bool]] = {}

    for stat_name in names:
        plugin = STATISTICS_BY_NAME[stat_name]
        lats, lons = _load_metadata(str(stats_root), stat_name)
        lat_idx = int(np.argmin(np.abs(lats - lat)))
        lon_idx = int(np.argmin(np.abs(lons - lon)))
        field = plugin.spec.render_field
        try:
            values = _load_stat_values(str(stats_root), stat_name, lead_key, field)
        except (FileNotFoundError, KeyError):
            result[stat_name] = {
                "value": None,
                "units": plugin.spec.units,
                "no_data": True,
            }
            continue
        value = float(values[lat_idx, lon_idx])

        if np.isnan(value):
            result[stat_name] = {
                "value": None,
                "units": plugin.spec.units,
                "no_data": True,
            }
        else:
            result[stat_name] = {
                "value": value,
                "units": plugin.spec.units,
                "no_data": False,
            }

    return result
