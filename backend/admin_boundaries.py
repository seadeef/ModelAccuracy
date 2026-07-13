"""Lookup for US admin (state/county) boundaries by FIPS code.

Reads ``admin/boundaries.json`` once at first use through the static-site
store: ``static_export/admin/boundaries.json`` on disk in dev, the same S3 key
in production Lambda.  The file is produced by ``export_static.py --admin``
from ``boundary_data/admin/`` (populated by ``load_boundaries.py --admin``).
"""

from __future__ import annotations

import json
from threading import Lock

from backend.static_store import static_site_store_from_env

_REL_PATH = "admin/boundaries.json"

_VALID_LEVELS = ("state", "county")
_BUCKET_BY_LEVEL = {"state": "states", "county": "counties"}

_lock = Lock()
_cached: dict | None = None


class AdminBoundariesUnavailable(RuntimeError):
    """Boundaries file is missing or unparseable."""


def _load() -> dict:
    global _cached
    if _cached is not None:
        return _cached
    with _lock:
        if _cached is not None:
            return _cached
        store = static_site_store_from_env()
        try:
            data = json.loads(store.read_text(_REL_PATH))
        except FileNotFoundError as exc:
            raise AdminBoundariesUnavailable(
                f"admin boundaries not found at {store.cache_key}/{_REL_PATH}; "
                "run export_static.py --admin, then deploy_static.sh for production"
            ) from exc
        except json.JSONDecodeError as exc:
            raise AdminBoundariesUnavailable(
                f"admin boundaries at {_REL_PATH} is not valid JSON"
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
