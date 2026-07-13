"""Lookup for military installation site boundaries by site key (slug).

Reads ``military/boundaries.json`` once at first use through the static-site
store: ``static_export/military/boundaries.json`` on disk in dev, the same S3
key in production Lambda.  The file is produced by ``export_static.py
--military`` from ``boundary_data/military/`` (populated by
``load_boundaries.py --military``).
"""

from __future__ import annotations

import json
from threading import Lock

from backend.static_store import static_site_store_from_env

_REL_PATH = "military/boundaries.json"

_lock = Lock()
_cached: dict | None = None


class MilitaryBoundariesUnavailable(RuntimeError):
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
            raise MilitaryBoundariesUnavailable(
                f"military boundaries not found at {store.cache_key}/{_REL_PATH}; "
                "run export_static.py --military, then deploy_static.sh for production"
            ) from exc
        except json.JSONDecodeError as exc:
            raise MilitaryBoundariesUnavailable(
                f"military boundaries at {_REL_PATH} is not valid JSON"
            ) from exc
        _cached = data
    return _cached


def get_entry(site: str) -> dict:
    """Return ``{"name": str, "bbox": [w,s,e,n], "polygons": [[outer, hole?, ...], ...]}``."""
    entry = (_load().get("sites") or {}).get(site)
    if entry is None:
        raise KeyError(f"unknown military site {site}")
    return entry


def reset_cache() -> None:
    """For tests."""
    global _cached
    with _lock:
        _cached = None
