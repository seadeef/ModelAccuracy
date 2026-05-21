"""Lookup for US admin (state/county) boundaries by FIPS code.

Reads ``static_export/static/admin/boundaries.json`` once at first use and
caches it in process.  The file is produced by ``scripts/fetch_admin_boundaries.py``
from the us-atlas TopoJSON (US Census Cartographic Boundary Files, 2017).
"""

from __future__ import annotations

import json
from pathlib import Path
from threading import Lock

from backend.static_store import default_static_site_root

_DEFAULT_PATH = default_static_site_root() / "static" / "admin" / "boundaries.json"

_VALID_LEVELS = ("state", "county")
_BUCKET_BY_LEVEL = {"state": "states", "county": "counties"}

_lock = Lock()
_cached: dict | None = None


class AdminBoundariesUnavailable(RuntimeError):
    """Boundaries file is missing or unparseable."""


def _load(path: Path = _DEFAULT_PATH) -> dict:
    global _cached
    if _cached is not None:
        return _cached
    with _lock:
        if _cached is not None:
            return _cached
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError as exc:
            raise AdminBoundariesUnavailable(
                f"admin boundaries not found at {path}; "
                "run scripts/fetch_admin_boundaries.py"
            ) from exc
        except json.JSONDecodeError as exc:
            raise AdminBoundariesUnavailable(
                f"admin boundaries at {path} is not valid JSON"
            ) from exc
        _cached = data
    return _cached


def get_entry(level: str, fips: str) -> dict:
    """Return ``{"name": str, "bbox": [w,s,e,n], "polygons": [[outer, hole?, ...], ...]}``."""
    if level not in _VALID_LEVELS:
        raise ValueError(f"level must be one of {_VALID_LEVELS}")
    bucket_key = _BUCKET_BY_LEVEL[level]
    bucket = _load().get(bucket_key) or {}
    entry = bucket.get(fips)
    if entry is None:
        raise KeyError(f"unknown {level} FIPS {fips}")
    return entry


def reset_cache() -> None:
    """For tests."""
    global _cached
    with _lock:
        _cached = None
