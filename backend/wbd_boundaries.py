"""Lookup for US WBD watershed boundaries by HUC code.

Lazy-loads per-parent chunk JSON through the static-site store: in local dev
the chunks live at ``static_export/wbd/chunks/`` on disk; on Lambda the same
paths are fetched from S3 on first use of each parent (``wbd/chunks/...``
keys uploaded by deploy_static.sh) and LRU-cached in the process.  Map display
TopoJSON and ``index.json`` are frontend-only assets on S3 / ``/wbd/``.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from threading import Lock

from backend.static_store import StaticStore, static_site_store_from_env

# Bound the in-memory chunk cache so a process that drills across many parents
# doesn't accumulate every visited chunk's sibling polygons forever. LRU: a
# cache hit is moved to the end, the least-recently-used entry is evicted first.
_CHUNK_CACHE_MAX = 256

_lock = Lock()
_chunk_cache: "OrderedDict[str, dict]" = OrderedDict()
_store: StaticStore | None = None
# Positive-only cache of the "does wbd/chunks exist at all" probe, so a miss on
# an unknown HUC doesn't re-LIST S3 once the tree is known to be present.
_chunks_root_seen = False


class WbdBoundariesUnavailable(RuntimeError):
    """Chunk tree is missing or unparseable."""


def _get_store() -> StaticStore:
    global _store
    if _store is None:
        _store = static_site_store_from_env()
    return _store


def _chunk_rel_path(parent_level: int, parent_huc: str) -> str:
    parent_file = "_root" if not parent_huc else parent_huc
    return f"wbd/chunks/{parent_level}/{parent_file}.json"


def _chunks_root_exists(store: StaticStore) -> bool:
    global _chunks_root_seen
    if _chunks_root_seen:
        return True
    if store.exists("wbd/chunks"):
        _chunks_root_seen = True
        return True
    return False


def _load_chunk(parent_level: int, parent_huc: str) -> dict:
    store = _get_store()
    rel = _chunk_rel_path(parent_level, parent_huc)
    key = f"{store.cache_key}::{rel}"
    with _lock:
        cached = _chunk_cache.get(key)
        if cached is not None:
            _chunk_cache.move_to_end(key)  # mark most-recently used
            return cached
    # Fetch outside the lock: an S3 read takes tens of ms and distinct chunks
    # must not serialize each other. A rare duplicate fetch of the same chunk
    # by two threads is harmless (last write wins with identical content).
    try:
        data = json.loads(store.read_text(rel))
    except FileNotFoundError as exc:
        if not _chunks_root_exists(store):
            raise WbdBoundariesUnavailable(
                f"WBD chunks not found under {store.cache_key}/wbd/chunks; "
                "run export_static.py --huc, then deploy_static.sh for production"
            ) from exc
        raise KeyError(f"WBD chunk not found: {rel}") from exc
    except json.JSONDecodeError as exc:
        raise WbdBoundariesUnavailable(f"WBD chunk at {rel} is not valid JSON") from exc
    with _lock:
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
    global _store, _chunks_root_seen
    with _lock:
        _chunk_cache.clear()
        _store = None
        _chunks_root_seen = False
