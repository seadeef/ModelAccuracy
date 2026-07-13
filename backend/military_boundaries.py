"""Lookup for military installation site boundaries by site key (slug).

Reads ``static_export/military/boundaries.json`` once at first use. The file is
produced by ``export_static.py --image`` or ``--military`` from
``boundary_data/military/`` (populated by ``load_boundaries.py --military``).
"""

from __future__ import annotations

import json
from pathlib import Path
from threading import Lock

from backend.static_store import default_static_site_root

_DEFAULT_PATH = default_static_site_root() / "military" / "boundaries.json"

_lock = Lock()
_cached: dict | None = None


class MilitaryBoundariesUnavailable(RuntimeError):
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
            raise MilitaryBoundariesUnavailable(
                f"military boundaries not found at {path}; "
                "run export_static.py --image (after load_boundaries.py --military)"
            ) from exc
        except json.JSONDecodeError as exc:
            raise MilitaryBoundariesUnavailable(
                f"military boundaries at {path} is not valid JSON"
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
