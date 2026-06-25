"""Lookup for US WBD watershed boundaries by HUC code.

Lazy-loads per-parent chunk JSON from ``static_export/wbd/chunks/``
(baked into the Docker image).  Map display TopoJSON and ``index.json`` are
frontend-only assets on S3 / ``/wbd/``.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from threading import Lock

from backend.static_store import default_static_site_root

_CHUNKS_ROOT = default_static_site_root() / "wbd" / "chunks"

# Bound the in-memory chunk cache so a process that drills across many parents
# doesn't accumulate every visited chunk's sibling polygons forever. LRU: a
# cache hit is moved to the end, the least-recently-used entry is evicted first.
_CHUNK_CACHE_MAX = 256

_lock = Lock()
_chunk_cache: "OrderedDict[str, dict]" = OrderedDict()


class WbdBoundariesUnavailable(RuntimeError):
    """Chunk directory is missing or unparseable."""


def _chunk_path(parent_level: int, parent_huc: str) -> Path:
    parent_file = "_root" if not parent_huc else parent_huc
    return _CHUNKS_ROOT / str(parent_level) / f"{parent_file}.json"


def _load_chunk(parent_level: int, parent_huc: str) -> dict:
    path = _chunk_path(parent_level, parent_huc)
    key = str(path.resolve())
    with _lock:
        cached = _chunk_cache.get(key)
        if cached is not None:
            _chunk_cache.move_to_end(key)  # mark most-recently used
            return cached
        if not _CHUNKS_ROOT.is_dir():
            raise WbdBoundariesUnavailable(
                f"WBD chunks not found at {_CHUNKS_ROOT}; "
                "run export_static.py --image (after load_boundaries.py --wbd)"
            )
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError as exc:
            raise KeyError(f"WBD chunk not found: {path}") from exc
        except json.JSONDecodeError as exc:
            raise WbdBoundariesUnavailable(
                f"WBD chunk at {path} is not valid JSON"
            ) from exc
        _chunk_cache[key] = data
        while len(_chunk_cache) > _CHUNK_CACHE_MAX:
            _chunk_cache.popitem(last=False)  # evict least-recently used
        return data


def get_entry(huc: str) -> dict:
    """Return ``{"name": str, "bbox": [w,s,e,n], "polygons": [[outer, hole?, ...], ...]}``."""
    huc = str(huc or "").strip()
    if not huc.isdigit() or len(huc) < 2 or len(huc) > 16 or len(huc) % 2:
        raise ValueError(f"invalid HUC code: {huc!r}")

    if len(huc) == 2:
        parent_level = 2
        parent_huc = ""
    else:
        parent_huc = huc[:-2]
        parent_level = len(parent_huc)

    chunk = _load_chunk(parent_level, parent_huc)
    entry = (chunk.get("units") or {}).get(huc)
    if entry is None:
        raise KeyError(f"unknown HUC {huc}")
    return {
        "name": entry.get("name") or "",
        "bbox": entry["bbox"],
        "polygons": entry["polygons"],
    }


def reset_cache() -> None:
    """For tests."""
    with _lock:
        _chunk_cache.clear()
