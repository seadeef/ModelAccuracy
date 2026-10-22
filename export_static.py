#!/usr/bin/env python3
"""Export stats, tiles, config, and the Vite SPA for static hosting.

Reads from stats_output/ and tiles_output/, writes to static_export/:

    static_export/                 ← upload this folder as the site root (S3, etc.)
        config.json                ← the only regular file at the root
        frontend/                  ← bundled Vite SPA; its CONTENTS serve at the site root
            index.html             ← served at /
            assets/                ← served at /assets/
            favicon.svg / favicon.ico
        model_data/                ← grid + verification .bin (baked into the API image)
            {model}/
                grid.json
                {stat}/…           (bias, rmse, … — NOT forecast)
        forecast/                  ← separate tree for forecast data (daily S3 uploads)
            forecast_calendar.json
            {model}/
                lead_1.bin
                lead_2.bin
                …
        zip/                       ← config + these dirs serve from the site root (e.g. /zip/…)
            95124.json
        tiles/
            {model}/{stat}/lead_1.png
            ...
        ranges/
            {model}/{stat}/yearly.json
            {model}/{stat}/monthly/{MM}.json
            {model}/{stat}/seasonal/{djf|…}.json
        admin/
            boundaries.json
            states.topojson
            counties.topojson
        wbd/
            chunks/ …
            children/ …
            index.json          (HUC2 units only)
            index/{huc2}.json   (per-HUC2 descendant metadata)
            huc2.topojson
        military/
            boundaries.json     (backend region masks — S3-read by the Lambda)
            sites.geojson       (frontend map layer)
        ar/
            manifest.json       (tARget v4 6-hour frame index)
            frames/*.geojson    (visualization-only AR footprints for one valid time)

URL scheme: every browser-fetched data file is served from the site root mirroring
this layout (e.g. ``/config.json``, ``/wbd/index.json``, ``/model_data/…``); the SPA
(``frontend/`` contents) is served from the root too (``/`` → ``index.html``).

Boundary source data lives in ``boundary_data/`` (data only), populated by
``load_boundaries.py``. ``export_static.py`` reads it and writes
``static_export/`` (the deploy input tree).

Build modes (at most one; omit all for a full export):

* ``--static`` — the S3 site tree: ``zip/``, admin/wbd/military (map TopoJSON/GeoJSON
  **and** the backend polygon chunks / ``boundaries.json`` the Lambda reads from S3),
  ``config.json``, ``tiles/``, ``ranges/``, ``forecast/`` (bins + ``forecast_calendar.json``),
  and ``ar/`` when tARget input is configured.
* ``--data`` — only ``static_export/model_data/``.
* ``--forecast`` — only ``static_export/forecast/``.
* ``--frontend`` — only the bundled Vite SPA under ``static_export/frontend/``.
* ``--ar`` — only ``static_export/ar/`` from tARget v4 netCDF input.

Boundary export flags (combinable with each other; not with build modes above):

* ``--zip`` — ``zip/{code}.json`` from ``boundary_data/zip/zip_lookup.csv``.
* ``--admin`` — ``boundaries.json`` + ``states.topojson`` + ``counties.topojson``.
* ``--huc`` — WBD map assets (``huc2.topojson``, ``children/``, ``index.json`` + ``index/{huc2}.json``) + ``chunks/``.
* ``--military`` — DISDI MIRTA sites: ``military/boundaries.json`` (backend) + ``military/sites.geojson`` (map).

WBD export depth is capped by ``--max-huc-level`` (default 12) to bound
S3 object count; ``boundary_data/`` keeps the full HU16 source for re-export.

Load boundaries first when ``boundary_data/`` is empty::

    python load_boundaries.py            # zip + admin + wbd
    python load_boundaries.py --wbd-sample   # synthetic WBD layers for local dev

Every mode is **incremental by default**: a step is skipped when its outputs
already exist on disk and are at least as new as the source files they derive
from (WBD layer cache, stats ``.npz``, boundary source TopoJSON, the ZIP CSV).
This is mtime-based, so rerun with ``--force`` after changing ``--max-huc-level``
or otherwise altering output shape without touching sources.

``--force`` re-exports every artifact regardless of staleness (outputs are kept).
``--clean`` deletes the entire output directory first (which also forces a full
rebuild); only allowed for a **full** export (no ``--static`` /
``--data`` / ``--forecast`` / ``--frontend`` / ``--ar``).

The .bin files are raw float32 arrays (row-major, height × width).

Best-model-by-lead for a map region is computed on demand via
``POST /api/stats/lead-winners`` (not exported to static files).
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path

import numpy as np
from dotenv import load_dotenv

from boundary_paths import (
    ADMIN_COUNTIES_TOPOJSON,
    ADMIN_SOURCE_LABEL,
    ADMIN_STATES_TOPOJSON,
    MILITARY_SITES_JSON,
    WBD_LAYERS_DIR,
    WBD_SOURCE_LABEL,
    ZIP_LOOKUP_CSV,
)
from wbd_core import DEFAULT_MAX_HUC_LEVEL
from wbd_core import HU_LEVELS as WBD_HU_LEVELS
from wbd_core import in_branch as _wbd_in_branch
from wbd_core import layer_cache_path as _wbd_layer_cache_path
from wbd_core import parent_huc as _wbd_parent_huc
from wbd_core import polygons_bbox as _polygons_bbox
from model_registry import MODEL_REGISTRY, DEFAULT_MODEL
from stats_pipeline.verification import SEASONS
from stats_pipeline.episode_metrics import BASIN_HISTORY_FILENAME, basin_history_filename
from stats_pipeline.episodes import episode_catalog_filename
from forecast_variables import FORECAST_VARIABLES
from stats_grid_metadata import load_model_metadata, model_metadata_path
from statistics_plugins.registry import (
    ALL_STATISTICS,
    DEFAULT_STATISTIC,
    EPISODE_CATALOG_FILENAME,
    EPISODE_CONTEXT_STATS,
    STATISTICS_BY_NAME,
)
from statistics_plugins.consistency import (
    combo_tag,
)
from atmospheric_rivers import TARGET_AR_DATA_DIR, discover_target_ar_sources

_PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(_PROJECT_ROOT / ".env", override=True)
STATS_ROOT = _PROJECT_ROOT / "stats_output"
TILES_ROOT = _PROJECT_ROOT / "tiles_output"
EXPORT_ROOT = _PROJECT_ROOT / "static_export"
FRONTEND_ROOT = _PROJECT_ROOT / "frontend"
# Flat layout: config.json, zip/, admin/, wbd/, tiles/, ranges/ live at the
# site root and are served from the URL root. Model verification data is
# ``model_data/``; the built SPA is bundled under ``frontend/`` and its contents
# are served from the site root too (``/`` → index.html).
DATA_DIR_NAME = "model_data"
FORECAST_DIR_NAME = "forecast"
FRONTEND_DIR_NAME = "frontend"
AR_DIR_NAME = "ar"
# HUC2 regions with a basin stage (per-HUC6 forecast histories + AR episode
# catalogs): California, Pacific Northwest, Upper Colorado. Order = display order.
BASIN_HUC2_REGIONS: dict[str, str] = {
    "18": "California",
    "17": "Pacific Northwest",
    "14": "Upper Colorado",
}
# Curated storm catalogs, per region.
FIGURE_DATA_ROOT = _PROJECT_ROOT / "figures" / "out" / "data"
# Served WBD depth (backend chunks + map assets, all on S3). Default comes
# from wbd_core.DEFAULT_MAX_HUC_LEVEL — the one knob shared with the loader.
# Override per-run with --max-huc-level.


def _write_text_if_changed(path: Path, content: str) -> bool:
    """Write *content* to *path* only if it differs from the existing file.

    Preserves mtime when content is identical, so downstream ``aws s3 sync``
    (which compares by mtime + size) doesn't re-upload unchanged files. The
    100k ZIP JSONs are the motivating case — they're CSV-deterministic and
    re-running the export rewrites them with new mtimes if we don't skip here.
    Returns True if a write occurred.
    """
    try:
        if path.read_text(encoding="utf-8") == content:
            return False
    except (FileNotFoundError, UnicodeDecodeError):
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def _write_bytes_if_changed(path: Path, content: bytes) -> bool:
    """Binary counterpart to :func:`_write_text_if_changed`.

    Note: float32 ``.bin`` files have fixed sizes (set by the model grid), so
    size-only comparison wouldn't detect content changes — must read+compare.
    """
    try:
        if path.read_bytes() == content:
            return False
    except FileNotFoundError:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return True


# ── incremental export: skip work when outputs are newer than sources ──
#
# The export is already idempotent (``_write_*_if_changed`` preserves mtimes so
# ``aws s3 sync`` skips unchanged files), but the *work* of regenerating an
# artifact — building WBD TopoJSON, reading every stats ``.npz`` — still runs on
# each invocation. The deploy scripts rerun ``--static`` on every forecast
# refresh, so each step below first checks whether its outputs already exist and
# are at least as new as the source files they derive from; if so the step is
# skipped entirely. ``--force`` (and ``--clean``, which wipes the tree) bypasses
# the check and regenerates everything. Staleness is mtime-based, so after
# changing ``--max-huc-level`` rerun with ``--force`` to pick up the new depth.
_FORCE_EXPORT = False


def _newest_mtime(sources: list[Path]) -> float:
    """Newest mtime among existing *sources* (directories scanned recursively).

    Returns 0.0 when none of the paths exist.
    """
    newest = 0.0
    for src in sources:
        if src.is_dir():
            for f in src.rglob("*"):
                if f.is_file():
                    newest = max(newest, f.stat().st_mtime)
        elif src.is_file():
            newest = max(newest, src.stat().st_mtime)
    return newest


def _outputs_fresh(outputs: list[Path], sources: list[Path]) -> bool:
    """True if regeneration can be skipped for this step.

    Skips only when ``--force`` is off, every *output* file exists, at least one
    *source* exists, and the oldest output is no older than the newest source. A
    missing output, or a source newer than the outputs, returns False so the
    caller rebuilds. *outputs* must be files the step always writes on success
    (not directories), so their presence proves a prior run completed.
    """
    if _FORCE_EXPORT or not outputs:
        return False
    out_mtimes: list[float] = []
    for o in outputs:
        if not o.is_file():
            return False
        out_mtimes.append(o.stat().st_mtime)
    src_newest = _newest_mtime(sources)
    if src_newest == 0.0:
        return False
    return min(out_mtimes) >= src_newest


def _get_forecast_init_date(model_key: str) -> str | None:
    # Read the init date from the same source the bins come from (multi-variable
    # forecast/{precip}/metadata.npz when present, else legacy flat metadata.npz),
    # so the calendar can't drift from the exported forecast values.
    src_dir, _ = _forecast_source(model_key)
    meta_path = src_dir / "metadata.npz"
    if not meta_path.exists():
        meta_path = STATS_ROOT / model_key / "forecast" / "metadata.npz"
    if not meta_path.exists():
        return None
    try:
        meta = np.load(meta_path, allow_pickle=True)
        return str(meta["init_date"])
    except Exception:
        return None


def _resolve_zip_lookup_csv() -> Path:
    if not ZIP_LOOKUP_CSV.is_file():
        raise FileNotFoundError(
            "zip lookup missing — run python load_boundaries.py --zip"
        )
    return ZIP_LOOKUP_CSV


def export_zip_directory(export_dir: Path) -> None:
    """Write ``static/zip/{code}.json`` lookup files (frontend reads these, not the CSV)."""
    csv_path = _resolve_zip_lookup_csv()
    zip_dir = export_dir / "zip"
    # Skip the 100k-file rewrite when nothing changed: the dir's mtime tracks the
    # last add/remove (a no-op rerun writes nothing, so it stays older than a
    # freshly regenerated CSV).
    if (
        not _FORCE_EXPORT
        and zip_dir.is_dir()
        and next(zip_dir.glob("*.json"), None) is not None
        and zip_dir.stat().st_mtime >= csv_path.stat().st_mtime
    ):
        print(f"  {zip_dir}/ up to date — skipping")
        return
    zip_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            z = row.get("zip", "").strip()
            if len(z) < 5:
                continue
            code = z[:5]
            payload = {
                "lat": round(float(row["lat"]), 6),
                "lon": round(float(row["lon"]), 6),
                "bounds": [
                    round(float(row["min_lon"]), 6),
                    round(float(row["min_lat"]), 6),
                    round(float(row["max_lon"]), 6),
                    round(float(row["max_lat"]), 6),
                ],
            }
            _write_text_if_changed(zip_dir / f"{code}.json", json.dumps(payload))
            count += 1
    total_bytes = sum(p.stat().st_size for p in zip_dir.glob("*.json"))
    print(f"  {zip_dir}/ ({count} files, {total_bytes / (1024 * 1024):.1f} MB)")


def _admin_decode_arcs(topology: dict) -> list[list[list[float]]]:
    transform = topology.get("transform")
    raw_arcs = topology["arcs"]
    if transform is None:
        return [[list(map(float, p)) for p in arc] for arc in raw_arcs]
    sx, sy = transform["scale"]
    tx, ty = transform["translate"]
    decoded: list[list[list[float]]] = []
    for arc in raw_arcs:
        out: list[list[float]] = []
        x = 0
        y = 0
        for dx, dy in arc:
            x += dx
            y += dy
            out.append([x * sx + tx, y * sy + ty])
        decoded.append(out)
    return decoded


def _admin_stitch_ring(arc_indices: list[int], arcs: list[list[list[float]]]) -> list[list[float]]:
    ring: list[list[float]] = []
    for n, idx in enumerate(arc_indices):
        arc = list(reversed(arcs[~idx])) if idx < 0 else arcs[idx]
        ring.extend(arc if n == 0 else arc[1:])
    return ring


def _admin_geometry_to_polygons(geom: dict, arcs: list[list[list[float]]]) -> list[list[list[list[float]]]]:
    gtype = geom.get("type")
    if gtype == "Polygon":
        return [[_admin_stitch_ring(ring_arcs, arcs) for ring_arcs in geom["arcs"]]]
    if gtype == "MultiPolygon":
        return [
            [_admin_stitch_ring(ring_arcs, arcs) for ring_arcs in poly_arcs]
            for poly_arcs in geom["arcs"]
        ]
    return []


def _admin_build_boundaries(topology: dict, object_name: str) -> dict[str, dict]:
    arcs = _admin_decode_arcs(topology)
    collection = topology["objects"][object_name]
    boundaries: dict[str, dict] = {}
    for geom in collection.get("geometries", []):
        fips = str(geom.get("id") or "").strip()
        if not fips:
            continue
        name = (geom.get("properties") or {}).get("name") or ""
        polygons = _admin_geometry_to_polygons(geom, arcs)
        if not polygons:
            continue
        boundaries[fips] = {"name": name, "bbox": _polygons_bbox(polygons), "polygons": polygons}
    return boundaries


def _admin_slim_topology(topology: dict, object_name: str) -> dict:
    keep = topology["objects"][object_name]
    return {
        "type": "Topology",
        "transform": topology.get("transform"),
        "bbox": topology.get("bbox"),
        "arcs": topology["arcs"],
        "objects": {object_name: keep},
    }


def export_admin_boundaries(
    out_dir: Path,
    *,
    write_boundaries_json: bool = True,
    write_topojson: bool = True,
) -> None:
    if not ADMIN_STATES_TOPOJSON.is_file() or not ADMIN_COUNTIES_TOPOJSON.is_file():
        missing = [p for p in (ADMIN_STATES_TOPOJSON, ADMIN_COUNTIES_TOPOJSON) if not p.is_file()]
        raise FileNotFoundError(
            "admin boundaries missing — run python load_boundaries.py --admin "
            f"({', '.join(str(p) for p in missing)})"
        )
    expected: list[Path] = []
    if write_boundaries_json:
        expected.append(out_dir / "boundaries.json")
    if write_topojson:
        expected += [out_dir / "states.topojson", out_dir / "counties.topojson"]
    if _outputs_fresh(expected, [ADMIN_STATES_TOPOJSON, ADMIN_COUNTIES_TOPOJSON]):
        print(f"  {out_dir}/ up to date — skipping")
        return
    states_topo = json.loads(ADMIN_STATES_TOPOJSON.read_text(encoding="utf-8"))
    counties_topo = json.loads(ADMIN_COUNTIES_TOPOJSON.read_text(encoding="utf-8"))
    written: list[Path] = []
    if write_boundaries_json:
        boundaries = {
            "version": 1,
            "source": ADMIN_SOURCE_LABEL,
            "states": _admin_build_boundaries(states_topo, "states"),
            "counties": _admin_build_boundaries(counties_topo, "counties"),
        }
        boundaries_path = out_dir / "boundaries.json"
        _write_text_if_changed(boundaries_path, json.dumps(boundaries, separators=(",", ":")))
        written.append(boundaries_path)
    if write_topojson:
        states_topo_path = out_dir / "states.topojson"
        counties_topo_path = out_dir / "counties.topojson"
        _write_text_if_changed(
            states_topo_path,
            json.dumps(_admin_slim_topology(states_topo, "states"), separators=(",", ":")),
        )
        _write_text_if_changed(
            counties_topo_path,
            json.dumps(_admin_slim_topology(counties_topo, "counties"), separators=(",", ":")),
        )
        for old in ("states.geojson", "counties.geojson"):
            p = out_dir / old
            if p.exists():
                p.unlink()
        written.extend((states_topo_path, counties_topo_path))
    for p in written:
        print(f"  {p} ({p.stat().st_size:,} bytes)")


def export_military_boundaries(
    out_dir: Path,
    *,
    write_boundaries_json: bool = True,
    write_geojson: bool = True,
) -> None:
    """DISDI MIRTA installation sites, from ``boundary_data/military/sites.json``.

    Two halves (mirrors admin): ``boundaries.json`` — key → {name, bbox, polygons}
    for backend region masks (baked into the API image); ``sites.geojson`` — a
    FeatureCollection served to the frontend map. Sites don't share borders, so
    plain GeoJSON is used (TopoJSON's shared-arc encoding wouldn't shrink it).
    Feature ids are numeric (MapLibre feature-state needs castable ids; the slug
    key rides in ``properties.key``); the site's fly-to point rides in
    ``properties.point``.
    """
    if not MILITARY_SITES_JSON.is_file():
        raise FileNotFoundError(
            "military sites missing — run python load_boundaries.py --military "
            f"({MILITARY_SITES_JSON})"
        )
    expected: list[Path] = []
    if write_boundaries_json:
        expected.append(out_dir / "boundaries.json")
    if write_geojson:
        expected.append(out_dir / "sites.geojson")
    if _outputs_fresh(expected, [MILITARY_SITES_JSON]):
        print(f"  {out_dir}/ up to date — skipping")
        return
    data = json.loads(MILITARY_SITES_JSON.read_text(encoding="utf-8"))
    source = data.get("source") or ""
    sites = data.get("sites") or {}
    written: list[Path] = []
    if write_boundaries_json:
        boundaries = {
            "version": 1,
            "source": source,
            "sites": {
                key: {"name": s["name"], "bbox": s["bbox"], "polygons": s["polygons"]}
                for key, s in sites.items()
            },
        }
        boundaries_path = out_dir / "boundaries.json"
        _write_text_if_changed(boundaries_path, json.dumps(boundaries, separators=(",", ":")))
        written.append(boundaries_path)
    if write_geojson:
        features = []
        for i, (key, s) in enumerate(sorted(sites.items())):
            features.append({
                "type": "Feature",
                "id": i,
                "properties": {
                    "key": key,
                    "name": s["name"],
                    "component": s.get("component") or "",
                    "status": s.get("status") or "",
                    "state": s.get("state") or "",
                    "point": s.get("point") or [],
                },
                "geometry": {"type": "MultiPolygon", "coordinates": s["polygons"]},
            })
        geojson_path = out_dir / "sites.geojson"
        _write_text_if_changed(
            geojson_path,
            json.dumps({"type": "FeatureCollection", "features": features},
                       separators=(",", ":")),
        )
        written.append(geojson_path)
    for p in written:
        print(f"  {p} ({p.stat().st_size:,} bytes)")


def _wbd_features_to_topojson(features: list[dict], object_name: str) -> dict:
    from topojson import Topology

    geojson = {"type": "FeatureCollection", "features": features}
    topo = Topology(geojson, prequantize=False, presimplify=False)
    obj = topo.to_dict()
    objects = obj.get("objects", {})
    # The topojson library names its single object "data" by default; rename it to
    # `object_name` so the frontend can look it up as `huc{level}`.
    keep = objects.get(object_name) or (next(iter(objects.values())) if objects else None)
    if keep is not None:
        return {
            "type": "Topology",
            "transform": obj.get("transform"),
            "bbox": obj.get("bbox"),
            "arcs": obj["arcs"],
            "objects": {object_name: keep},
        }
    return obj


def _wbd_load_layer_units(layers_dir: Path, level: int) -> dict[str, dict]:
    path = _wbd_layer_cache_path(layers_dir, level)
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("units") or {}


def _prune_stale(root: Path, keep: set[Path], suffix: str) -> int:
    """Delete ``*suffix`` files under *root* not in *keep*, then drop empty dirs.

    Keeps the served WBD tree matching the current source + ``--max-huc-level``:
    units removed from the source (or levels excluded by a lowered cap) leave no
    orphaned chunk/children files to be synced to S3.
    Files in *keep* are untouched, so ``_write_*_if_changed`` mtimes survive.
    """
    if not root.is_dir():
        return 0
    removed = 0
    for p in root.rglob(f"*{suffix}"):
        if p.resolve() not in keep:
            p.unlink()
            removed += 1
    for d in sorted((d for d in root.rglob("*") if d.is_dir()),
                    key=lambda x: len(x.parts), reverse=True):
        try:
            d.rmdir()  # succeeds only if empty
        except OSError:
            pass
    return removed


# Chunk polygons feed ONLY the backend's region-mask rasterization onto the
# stats grid (0.25° cells ≈ 25 km); map display uses the separate TopoJSON
# tree. Source rings arrive pre-simplified at 0.005° (wbd_core), but with
# full-precision floats — most of the ~240 MB chunk tree was noise digits.
# Rounding to CHUNK_COORD_DECIMALS (~11 m) halves the bytes with ZERO mask
# changes (verified on a 400-HUC10 sample). Do not raise the tolerance for a
# deeper shrink: 0.01°/0.02° cut 4×/7.6× but flipped mask cells in 6%/14% of
# sampled HUC10s (a HUC10 spans only ~1 grid cell, so sub-cell distortion
# moves its reported mean). At 0.005° the simplify is a near-no-op guard.
CHUNK_SIMPLIFY_TOLERANCE = 0.005
CHUNK_COORD_DECIMALS = 4


def _chunk_slim_polygons(
    polygons: list[list[list[list[float]]]],
) -> list[list[list[list[float]]]]:
    """Simplify + round mask polygons; fall back to the input on degeneracy."""
    from shapely.geometry import Polygon

    from wbd_core import geom_to_polygons

    def _round_ring(ring: list[list[float]]) -> list[list[float]] | None:
        pts = [
            [round(float(x), CHUNK_COORD_DECIMALS), round(float(y), CHUNK_COORD_DECIMALS)]
            for x, y in ring
        ]
        dedup = [pts[0]]
        for p in pts[1:]:
            if p != dedup[-1]:
                dedup.append(p)
        if dedup[0] != dedup[-1]:
            dedup.append(list(dedup[0]))
        return dedup if len(dedup) >= 4 else None

    out: list[list[list[list[float]]]] = []
    for poly in polygons:
        if not poly or len(poly[0]) < 4:
            continue
        try:
            geom = Polygon(poly[0], [h for h in poly[1:] if len(h) >= 4])
            simplified = geom.simplify(CHUNK_SIMPLIFY_TOLERANCE, preserve_topology=True)
            if simplified.is_empty:
                simplified = geom
            parts = geom_to_polygons(simplified)
        except Exception:
            parts = [poly]
        for part in parts:
            if not part:
                continue
            outer = _round_ring(part[0])
            if outer is None:
                continue
            rings = [outer]
            for hole in part[1:]:
                r = _round_ring(hole)
                if r is not None:
                    rings.append(r)
            out.append(rings)
    # A unit must never vanish from the chunk tree because simplification
    # collapsed it — the mask code handles tiny polygons; a missing HUC 404s.
    return out if out else polygons


def _chunk_unit(entry: dict) -> dict:
    return {
        "name": entry["name"],
        "bbox": [round(float(v), CHUNK_COORD_DECIMALS) for v in entry["bbox"]],
        "polygons": _chunk_slim_polygons(entry["polygons"]),
    }


def export_wbd_boundaries(
    out_dir: Path,
    branch: str = "",
    *,
    write_chunks: bool = True,
    write_map_assets: bool = True,
    max_level: int = DEFAULT_MAX_HUC_LEVEL,
) -> None:
    if not WBD_LAYERS_DIR.is_dir():
        raise FileNotFoundError(
            "WBD layers missing — run python load_boundaries.py --wbd"
        )
    # Skip the (expensive) TopoJSON rebuild when the served tree is already newer
    # than the WBD layer cache. Representative outputs prove a prior run finished;
    # a branch filter changes the output shape without touching sources, so only
    # cache the unfiltered export path.
    if not branch:
        expected: list[Path] = []
        if write_map_assets:
            expected += [out_dir / "huc2.topojson", out_dir / "index.json"]
        if write_chunks:
            expected.append(out_dir / "chunks" / "2" / "_root.json")
        sources = [
            _wbd_layer_cache_path(WBD_LAYERS_DIR, lv)
            for lv in WBD_HU_LEVELS
            if lv <= max_level
        ]
        # The per-HUC2 index subtrees (index/{huc2}.json) are written alongside
        # index.json but their names depend on the source, so they aren't in
        # `expected`. A pre-split tree can hold index.json + huc2.topojson yet no
        # index/ subtrees — skipping then would 404 the frontend drill — so never
        # skip unless that directory is actually populated. (Fail toward
        # re-export, never toward a sync --delete of a gutted tree.)
        subtrees_ok = (not write_map_assets) or (
            (out_dir / "index").is_dir()
            and next((out_dir / "index").glob("*.json"), None) is not None
        )
        if subtrees_ok and _outputs_fresh(expected, sources):
            print(f"  {out_dir}/ up to date (≤ HU{max_level}) — skipping")
            return
    units_by_level: dict[int, dict[str, dict]] = {}
    available_levels: list[int] = []
    for level in WBD_HU_LEVELS:
        if level > max_level:
            break  # HU_LEVELS is ascending — skip the heavier deep layers entirely
        units = _wbd_load_layer_units(WBD_LAYERS_DIR, level)
        if not units:
            continue
        if branch:
            units = {
                huc: entry
                for huc, entry in units.items()
                if level == 2 or _wbd_in_branch(huc, branch)
            }
        if not units:
            continue
        units_by_level[level] = units
        available_levels.append(level)
    if not units_by_level.get(2):
        raise RuntimeError("no HUC-2 units in layer cache")
    index: dict[str, dict] = {}
    child_levels = sorted(lv for lv in available_levels if lv > 2)
    for level in available_levels:
        for huc, entry in units_by_level[level].items():
            next_level = level + 2
            has_children = False
            if next_level in available_levels:
                child_bucket = units_by_level[next_level]
                has_children = any(_wbd_parent_huc(ch) == huc for ch in child_bucket)
            index[huc] = {"name": entry["name"], "level": level, "hasChildren": has_children}
    huc2_features = []
    for huc, entry in sorted(units_by_level[2].items()):
        polys = entry["polygons"]
        if not polys:
            continue
        geom = (
            {"type": "Polygon", "coordinates": polys[0]}
            if len(polys) == 1
            else {"type": "MultiPolygon", "coordinates": polys}
        )
        huc2_features.append(
            {
                "type": "Feature",
                "id": huc,
                "properties": {"huc": huc, "name": entry["name"], "level": 2},
                "geometry": geom,
            }
        )
    huc2_topo = _wbd_features_to_topojson(huc2_features, "huc2")
    huc2_path = out_dir / "huc2.topojson"
    if write_map_assets:
        _write_text_if_changed(huc2_path, json.dumps(huc2_topo, separators=(",", ":")))
    chunks_dir = out_dir / "chunks"
    children_dir = out_dir / "children"
    written_chunks: set[Path] = set()
    written_children: set[Path] = set()
    for level in child_levels:
        parent_level = level - 2
        by_parent: dict[str, dict[str, dict]] = defaultdict(dict)
        for huc, entry in units_by_level[level].items():
            parent = _wbd_parent_huc(huc)
            if not parent:
                continue
            by_parent[parent][huc] = {
                "name": entry["name"],
                "bbox": entry["bbox"],
                "polygons": entry["polygons"],
            }
        for parent, units in sorted(by_parent.items()):
            if branch and not _wbd_in_branch(parent, branch) and parent != branch:
                if not any(_wbd_in_branch(u, branch) for u in units):
                    continue
            if write_chunks:
                # Slim at write time only — `units` also feeds the full-detail
                # map TopoJSON below.
                chunk_units = {h: _chunk_unit(u) for h, u in units.items()}
                chunk_path = chunks_dir / str(parent_level) / f"{parent}.json"
                written_chunks.add(chunk_path.resolve())
                _write_text_if_changed(
                    chunk_path,
                    json.dumps({"parent": parent, "level": level, "units": chunk_units}, separators=(",", ":")),
                )
            if write_map_assets:
                child_features = []
                for huc, entry in sorted(units.items()):
                    polys = entry["polygons"]
                    geom = (
                        {"type": "Polygon", "coordinates": polys[0]}
                        if len(polys) == 1
                        else {"type": "MultiPolygon", "coordinates": polys}
                    )
                    child_features.append(
                        {
                            "type": "Feature",
                            "id": huc,
                            "properties": {"huc": huc, "name": entry["name"], "level": level},
                            "geometry": geom,
                        }
                    )
                child_topo = _wbd_features_to_topojson(child_features, f"huc{level}")
                child_path = children_dir / str(level) / f"{parent}.topojson"
                written_children.add(child_path.resolve())
                _write_text_if_changed(
                    child_path,
                    json.dumps(child_topo, separators=(",", ":")),
                )
    if write_chunks:
        root_units = {huc: _chunk_unit(e) for huc, e in units_by_level[2].items()}
        root_path = chunks_dir / "2" / "_root.json"
        written_chunks.add(root_path.resolve())
        _write_text_if_changed(
            root_path,
            json.dumps({"parent": "", "level": 2, "units": root_units}, separators=(",", ":")),
        )
    # Index split per HUC2. A small top-level index.json carries only the ~22
    # HUC2 units, so entering watershed mode no longer pulls metadata for all
    # ~125k units (~9.5 MB). One index/{huc2}.json per HUC2 holds that region's
    # descendants (levels 4..cap); the drill loader fetches a region's subtree
    # only when the user drills into it. HUC codes are strict prefixes, so a
    # unit's HUC2 root is simply huc[:2].
    index_path = out_dir / "index.json"
    index_dir = out_dir / "index"
    written_index: set[Path] = set()
    if write_map_assets:
        index_huc2 = {huc: meta for huc, meta in index.items() if meta["level"] == 2}
        subtrees: dict[str, dict[str, dict]] = defaultdict(dict)
        for huc, meta in index.items():
            if meta["level"] != 2:
                subtrees[huc[:2]][huc] = meta
        _write_text_if_changed(
            index_path,
            json.dumps(
                {
                    "version": 1,
                    "source": WBD_SOURCE_LABEL,
                    "levels": available_levels,
                    "units": index_huc2,
                },
                separators=(",", ":"),
            ),
        )
        # One subtree file per HUC2 — including HUC2s with no descendants, so the
        # loader never 404s on a region the top index reports as childless.
        for huc2 in sorted(units_by_level[2]):
            sub_path = index_dir / f"{huc2}.json"
            written_index.add(sub_path.resolve())
            _write_text_if_changed(
                sub_path,
                json.dumps(
                    {"version": 1, "parent": huc2, "units": subtrees.get(huc2, {})},
                    separators=(",", ":"),
                ),
            )
    # Prune stale artifacts so the served tree matches the current source + cap.
    # Each half only prunes what it writes (chunks vs. children/index).
    if write_chunks:
        stale = _prune_stale(chunks_dir, written_chunks, ".json")
        if stale:
            print(f"  pruned {stale} stale chunk file(s)")
    if write_map_assets:
        stale = _prune_stale(children_dir, written_children, ".topojson")
        if stale:
            print(f"  pruned {stale} stale children topojson(s)")
        stale = _prune_stale(index_dir, written_index, ".json")
        if stale:
            print(f"  pruned {stale} stale index file(s)")
    if write_map_assets:
        for p in (index_path, huc2_path):
            print(f"  {p} ({p.stat().st_size:,} bytes)")
        n_sub = sum(1 for _ in index_dir.glob("*.json"))
        print(f"  {n_sub} per-HUC2 index file(s) under {index_dir}/")
    if write_chunks:
        chunk_files = list(chunks_dir.rglob("*.json"))
        chunk_mb = sum(p.stat().st_size for p in chunk_files) / (1024 * 1024)
        print(f"  {len(chunk_files)} chunk files ({chunk_mb:.1f} MB) under {chunks_dir}/")


def export_boundary_parts(
    site_root: Path,
    *,
    zip: bool = False,
    admin_topo: bool = False,
    admin_polys: bool = False,
    huc_topo: bool = False,
    huc_polys: bool = False,
    military_geo: bool = False,
    military_polys: bool = False,
    huc_max_level: int = DEFAULT_MAX_HUC_LEVEL,
) -> None:
    """Write selected boundary artifacts at the ``static_export/`` root from cache."""
    assets_dir = site_root
    assets_dir.mkdir(parents=True, exist_ok=True)
    if zip:
        print("  zip/...")
        export_zip_directory(assets_dir)
    if admin_topo or admin_polys:
        print("  admin/...")
        export_admin_boundaries(
            assets_dir / "admin",
            write_boundaries_json=admin_polys,
            write_topojson=admin_topo,
        )
    if huc_topo or huc_polys:
        print(f"  wbd/... (≤ HU{huc_max_level})")
        export_wbd_boundaries(
            assets_dir / "wbd",
            write_chunks=huc_polys,
            write_map_assets=huc_topo,
            max_level=huc_max_level,
        )
    if military_geo or military_polys:
        print("  military/...")
        export_military_boundaries(
            assets_dir / "military",
            write_boundaries_json=military_polys,
            write_geojson=military_geo,
        )


def export_boundaries(site_root: Path, huc_max_level: int = DEFAULT_MAX_HUC_LEVEL) -> None:
    """All boundary parts (zip, admin topo+polys, HUC topo+polys, military geo+polys)."""
    export_boundary_parts(
        site_root,
        zip=True,
        admin_topo=True,
        admin_polys=True,
        huc_topo=True,
        huc_polys=True,
        military_geo=True,
        military_polys=True,
        huc_max_level=huc_max_level,
    )


def export_s3_assets(site_root: Path, maptiler_key: str) -> None:
    """Export S3 core assets: config, tiles, ranges (not zip or boundary TopoJSON)."""
    assets_dir = site_root
    assets_dir.mkdir(parents=True, exist_ok=True)
    export_config(assets_dir, maptiler_key)
    export_tiles(assets_dir)
    export_value_ranges(assets_dir)
    export_episode_model_comparison(assets_dir)


def export_episode_model_comparison(site_root: Path) -> None:
    """Copy stats_output/episode_model_comparison.json → ar/model_comparison.json.

    Cross-model paired-ΔR curated artifact (CONTRACT §4); statically served,
    no UI requirement. Absent until compute_stats.py --only episodes has run
    for at least two models — skipping then is normal, not an error.
    """
    src = STATS_ROOT / "episode_model_comparison.json"
    if not src.is_file():
        print("  SKIP: stats_output/episode_model_comparison.json not found")
        return
    out = site_root / AR_DIR_NAME / "model_comparison.json"
    _write_text_if_changed(out, src.read_text(encoding="utf-8"))
    print(f"  {out} ({out.stat().st_size:,} bytes)")


# ── config.json ─────────────────────────────────────────────────────

def export_config(export_dir: Path, maptiler_key: str) -> None:
    models = []
    for _, config in sorted(MODEL_REGISTRY.items()):
        entry = {
            "key": config.key,
            "label": config.label,
            "lead_days_min": config.lead_days_min,
            "lead_days_max": config.lead_days_max,
            # W1: which model-version window the pooled statistics cover.
            # Dashboards and CSV metadata state it from here (hard constraint:
            # every stats surface names its version window).
            "version_windows": config.window_block(),
        }
        if config.ident:
            entry["ident"] = config.ident
        models.append(entry)

    statistics = [{
        "key": entry.spec.name,
        "label": entry.spec.label,
        "units": entry.spec.units,
        "scope": entry.spec.scope,
        "axis": entry.spec.axis,
        "periods": list(entry.spec.periods),
        "map_support": entry.spec.map_support,
        "winner_support": entry.spec.winner_support,
        "winner_mode": entry.spec.winner_mode,
        "parameters": entry.spec.parameters,
    } for entry in ALL_STATISTICS]

    # Per-lead consistency stats only (scope=="region"): revision_efficiency is
    # also packed_grid but region_pooled — it has its own endpoint below and
    # must not appear in the per-lead consistency stat list or parameters.
    packed_specs = [entry.spec for entry in ALL_STATISTICS
                    if entry.spec.storage.kind == "packed_grid"
                    and entry.spec.scope == "region"]
    packed_parameters = packed_specs[0].parameters if packed_specs else {}
    config_data = {
        "models": models,
        "default_model": DEFAULT_MODEL,
        "statistics": statistics,
        "default_statistic": DEFAULT_STATISTIC,
        # Parameters of the consistency statistics (jumpiness/convergence):
        # the frontend renders its q/n selectors and the lock-in definition
        # from here so they can never drift from the compute pipeline.
        "consistency": {
            "stats": sorted(spec.name for spec in packed_specs),
            "thresholds_mm": packed_parameters.get("thresholds_mm", []),
            "scales_km": packed_parameters.get("scales_km", []),
            "default_threshold_mm": packed_parameters.get("default_threshold_mm"),
            "default_scale_km": packed_parameters.get("default_scale_km"),
            "lock_in_fraction": packed_parameters.get("lock_in_fraction"),
            "curve_roles": {spec.parameters.get("curve_role"): spec.name
                            for spec in packed_specs if spec.parameters.get("curve_role")},
            # Grid-pooled revision efficiency R (CONTRACT §5/§6).
            "revision_statistic": "revision_efficiency",
            "revision_endpoint": "/api/stats/revision-efficiency",
        },
        "ar_episodes": {
            # EPISODE_CONTEXT_STATS, not a scope filter: revision_efficiency's
            # spec is scope="region_pooled" yet it is servable in the episode
            # context alongside lock_in_gap.
            "statistics": list(EPISODE_CONTEXT_STATS),
            "huc_level": 6,
            "catalog_endpoint": "/api/ar/episodes",
            "stats_endpoint": "/api/stats",
            # Pooled scope selectors (CONTRACT §0/§6) + the episode-cluster
            # merge rule they are bootstrapped with.
            "scopes": [
                {"key": "ar_all", "label": "All AR days"},
                {"key": "non_ar", "label": "Non-AR days"},
                {"key": "all_days", "label": "All days"},
            ],
            "cluster_merge_hours": 72,
        },
        "atmospheric_rivers": {
            "dataset": "Guan & Waliser tARget v4",
            "manifest_url": "/ar/manifest.json",
            "temporal_resolution_hours": 6,
            "grid_resolution_degrees": 0.25,
            # Per-basin raw forecast revision series (one JSON per model/HUC6);
            # AR mode fetches these directly (see export_basin_revisions).
            "revisions_base": "/ar/basin_revisions",
        },
        "maptiler_api_key": maptiler_key,
    }

    out = export_dir / "config.json"
    _write_text_if_changed(out, json.dumps(config_data, indent=2))
    print(f"  {out}")


# ── data layers (.bin) ──────────────────────────────────────────────

def _prune_stale_stat_dirs(parent: Path, valid_stats: set[str]) -> int:
    """Remove exported per-statistic dirs for stats no longer in the registry
    (e.g. ``sacc`` after the CC rename) so ``aws s3 sync --delete`` drops them
    from S3 too. *parent* is a single model's export dir; files (``grid.json``)
    are left alone."""
    removed = 0
    if not parent.is_dir():
        return 0
    for child in parent.iterdir():
        if child.is_dir() and child.name not in valid_stats:
            shutil.rmtree(child)
            removed += 1
    return removed


def _verification_stat_names() -> set[str]:
    """Valid exported per-stat directory names under model_data/{model}/.

    Grid stats export under their own name; packed stats export under the
    first segment of their storage path template — which differs from the
    stat name for revision_efficiency (directory ``revision/``). Using the
    template keeps the prune from deleting a freshly exported revision tree.
    """
    names: set[str] = set()
    for entry in ALL_STATISTICS:
        kind = entry.spec.storage.kind
        if kind == "grid":
            names.add(entry.spec.name)
        elif kind == "packed_grid":
            names.add(entry.spec.storage.path_template.split("/", 1)[0])
    return names


def export_data_layers(data_root: Path) -> None:
    data_dir = data_root
    total = 0
    valid_stats = _verification_stat_names()
    pruned_dirs = pruned_bins = 0

    for model_key in MODEL_REGISTRY:
        model_stats = STATS_ROOT / model_key
        if not model_stats.exists():
            continue

        # Drop artifacts from removed features so they don't linger in S3:
        # renamed stats (sacc → cc) and lead-window bins.
        pruned_dirs += _prune_stale_stat_dirs(data_dir / model_key, valid_stats)
        for stale_bin in (data_dir / model_key).rglob("lead_*_*.bin"):
            stale_bin.unlink()
            pruned_bins += 1

        # Export each layer as flat float32 .bin (verification stats + any non-forecast dirs).
        for plugin in ALL_STATISTICS:
            stat_name = plugin.spec.name
            if plugin.spec.storage.kind != "grid":
                # Packed two-plane bins — see ``export_consistency_layers``.
                continue
            field = plugin.spec.render_field
            stat_dir = model_stats / stat_name
            if not stat_dir.exists():
                continue

            # Yearly layers (in stat root).
            for npz_path in sorted(stat_dir.glob("lead_*.npz")):
                total += _export_layer(npz_path, field, data_dir / model_key / stat_name)

            # Monthly layers.
            for month_dir in sorted((stat_dir / "monthly").glob("*")):
                if not month_dir.is_dir():
                    continue
                for npz_path in sorted(month_dir.glob("lead_*.npz")):
                    total += _export_layer(
                        npz_path, field,
                        data_dir / model_key / stat_name / "monthly" / month_dir.name,
                    )

            # Seasonal layers.
            for season_dir in sorted((stat_dir / "seasonal").glob("*")):
                if not season_dir.is_dir():
                    continue
                for npz_path in sorted(season_dir.glob("lead_*.npz")):
                    total += _export_layer(
                        npz_path, field,
                        data_dir / model_key / stat_name / "seasonal" / season_dir.name,
                    )

    if pruned_dirs or pruned_bins:
        print(f"  pruned {pruned_dirs} stale stat dir(s), {pruned_bins} stale window bin(s)")
    print(f"  {total} .bin layer files exported")
    export_consistency_layers(data_dir)


# ── consistency statistics (jumpiness / convergence) ────────────────
#
# Region values for the consistency statistics are ratios of region-summed
# accumulators (pooled FSS: 1 - Σnum/Σden; pooled RMSE: sqrt(Σsse/Σcount)),
# not means of per-cell values, so the bins carry BOTH accumulator planes,
# packed as one array of shape (2, nLat, nLon): plane 0 the numerator, plane
# 1 the denominator.
#
# Jumpiness planes are float16: values are sums of squared neighborhood
# fractions, bounded by 2x the pooled date count (thousands at most, well
# inside float16 range) and read back as float32 for the region sums —
# quantization is ~1e-3 relative per cell and averages out over region sums,
# invisible at the 2-3 digits J is displayed with. This halves an otherwise
# 12-combo-per-pair fan-out that would double the image size. Convergence
# keeps float32 (squared mm/day errors overflow float16).
#
# Monthly bins are deliberately NOT exported: the period UI exposes yearly +
# seasonal only (open decision O7), and the 12-month fan-out would triple the
# consistency payload. The monthly .npz primitives exist in stats_output, so
# wiring monthly in later is an exporter change only.

def _export_packed_layer(npz_path: Path, keys: tuple[str, str], dtype,
                         out_path: Path) -> int:
    """Write two npz arrays as one packed (2, H, W) .bin. Returns 1 on success."""
    if (
        not _FORCE_EXPORT
        and out_path.is_file()
        and out_path.stat().st_mtime >= npz_path.stat().st_mtime
    ):
        return 1
    try:
        with np.load(npz_path) as data:
            if keys[0] not in data or keys[1] not in data:
                return 0
            packed = np.stack([data[keys[0]], data[keys[1]]]).astype(dtype)
    except Exception:
        return 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _write_bytes_if_changed(out_path, packed.tobytes())
    return 1


def _consistency_periods(stat_dir: Path) -> list[tuple[Path, Path]]:
    """(source dir, relative out dir) pairs: yearly root + each season."""
    periods = [(stat_dir, Path("."))]
    seasonal_root = stat_dir / "seasonal"
    if seasonal_root.is_dir():
        for season_dir in sorted(seasonal_root.glob("*")):
            if season_dir.is_dir():
                periods.append((season_dir, Path("seasonal") / season_dir.name))
    return periods


def _export_revision_layers(model_key: str, model_stats: Path, data_root: Path) -> int:
    """Export the gridded revision planes (CONTRACT §5).

    ``rev_sse``/``rev_cnt`` are accumulated by the consistency stage *inside*
    the jumpiness ``pair_<P>.npz`` files (yearly + seasonal); they export as
    two packed float32 planes to ``model_data/{model}/revision/…/pair_<P>.bin``
    (identical packing convention to convergence). A jumpiness npz written
    before the revision recompute lacks the rev keys — those files are skipped
    with a warning rather than failing the export.
    """
    spec = STATISTICS_BY_NAME["revision_efficiency"].spec
    src_root = model_stats / "jumpiness"
    if not src_root.is_dir():
        return 0
    out_root = data_root / model_key / spec.storage.path_template.split("/", 1)[0]
    total = 0
    missing = 0
    for src_dir, rel_out in _consistency_periods(src_root):
        for npz_path in sorted(src_dir.glob("pair_*.npz")):
            if not npz_path.stem.removeprefix("pair_").isdigit():
                continue
            out_path = out_root / rel_out / (npz_path.stem + ".bin")
            written = _export_packed_layer(
                npz_path, spec.storage.source_fields,
                np.dtype(spec.storage.export_dtype), out_path,
            )
            if written == 0:
                missing += 1
            total += written
    if missing:
        print(f"  WARNING: {model_key}: {missing} jumpiness pair npz file(s) lack "
              "rev_sse/rev_cnt (revision planes not yet computed) — skipped; "
              "rerun compute_stats.py --only consistency to produce them")
    return total


def export_consistency_layers(data_root: Path) -> None:
    total = 0
    for model_key in MODEL_REGISTRY:
        model_stats = STATS_ROOT / model_key
        for entry in ALL_STATISTICS:
            spec = entry.spec
            if spec.storage.kind != "packed_grid" or spec.scope != "region":
                # region_pooled packed planes (revision_efficiency) have no
                # stats_output dir of their own — they ride inside the
                # jumpiness npz files; see _export_revision_layers below.
                continue
            stat_dir = model_stats / spec.name
            if not stat_dir.is_dir():
                continue
            prefix = "pair" if spec.axis == "lead_pair" else "lead"
            thresholds = spec.parameters.get("thresholds_mm", [None])
            scales = spec.parameters.get("scales_km", [None])
            combinations = (
                [(combo_tag(q, n),) for q in thresholds for n in scales]
                if spec.axis == "lead_pair" else [(None,)]
            )
            for src_dir, rel_out in _consistency_periods(stat_dir):
                for npz_path in sorted(src_dir.glob(f"{prefix}_*.npz")):
                    if not npz_path.stem.removeprefix(f"{prefix}_").isdigit():
                        continue
                    for (combo,) in combinations:
                        keys = tuple(key.format(combo=combo) for key in spec.storage.source_fields)
                        base = data_root / model_key / spec.name
                        if combo:
                            base /= combo
                        out_path = base / rel_out / (npz_path.stem + ".bin")
                        total += _export_packed_layer(
                            npz_path, keys, np.dtype(spec.storage.export_dtype), out_path,
                        )

        total += _export_revision_layers(model_key, model_stats, data_root)

        _export_consistency_meta(model_stats, data_root / model_key)

        # One catalog per HUC2 region: California keeps the bare name, the
        # others carry the region suffix (see stats_pipeline.episodes).
        for huc2 in BASIN_HUC2_REGIONS:
            episode_src = model_stats / episode_catalog_filename(huc2)
            if episode_src.is_file():
                episode_out = data_root / model_key / episode_catalog_filename(huc2)
                episode_out.parent.mkdir(parents=True, exist_ok=True)
                _write_text_if_changed(episode_out, episode_src.read_text(encoding="utf-8"))

    print(f"  {total} consistency .bin layer files exported")


def _export_consistency_meta(model_stats: Path, out_dir: Path) -> None:
    """Write consistency_meta.json: the valid-date counts per period.

    The interpretation rules require identical (q, n, lead-pair, region,
    valid-date set) slices for cross-model comparison; the date counts make
    the valid-date-set part of that visible to the client.
    """
    manifest_path = model_stats / "manifest.json"
    if not manifest_path.exists():
        return
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except (json.JSONDecodeError, OSError):
        return
    dates = manifest.get("stages", {}).get("consistency", {}).get("processed_valid_dates", [])
    month_counts: dict[str, int] = {}
    season_counts: dict[str, int] = {}
    month_to_season = {
        month: season for season, months in SEASONS.items() for month in months
    }
    for date_str in dates:
        month = int(date_str[4:6])
        month_counts[f"{month:02d}"] = month_counts.get(f"{month:02d}", 0) + 1
        season = month_to_season[month]
        season_counts[season] = season_counts.get(season, 0) + 1
    payload = {
        "date_counts": {
            "yearly": len(dates),
            "seasonal": season_counts,
            "monthly": month_counts,
        },
        "first_valid_date": dates[0] if dates else None,
        "last_valid_date": dates[-1] if dates else None,
        "lead_days_min": manifest.get("lead_days_min"),
        "lead_days_max": manifest.get("lead_days_max"),
        "thresholds_mm": manifest.get("thresholds_mm"),
        "scales_km": manifest.get("scales_km"),
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_text_if_changed(out_dir / "consistency_meta.json",
                           json.dumps(payload, indent=2))


def export_model_grids(data_root: Path) -> None:
    """Write ``data/{model}/grid.json`` from each model's ``metadata.npz`` (for static stats queries)."""
    count = 0
    for model_key in MODEL_REGISTRY:
        model_stats = STATS_ROOT / model_key
        out_path = data_root / model_key / "grid.json"
        if _outputs_fresh([out_path], [model_metadata_path(model_stats)]):
            count += 1
            continue
        try:
            meta = load_model_metadata(model_stats)
        except FileNotFoundError:
            continue
        lats = meta["lats"]
        lons = meta["lons"]
        payload = {
            "nLat": int(lats.size),
            "nLon": int(lons.size),
            "lats": lats.astype(float).tolist(),
            "lons": lons.astype(float).tolist(),
        }
        out_path.parent.mkdir(parents=True, exist_ok=True)
        _write_text_if_changed(out_path, json.dumps(payload))
        count += 1
    print(f"  {count} grid.json files under data/{{model}}/")


def _export_layer(npz_path: Path, field: str, out_dir: Path) -> int:
    """Extract the render field from an .npz and write as flat float32 .bin. Returns 1 on success."""
    # Only integer daily leads (lead_{N}.npz); skip any legacy lead-window
    # aggregate (lead_{a}_{b}.npz) that may still linger in stats_output.
    if not npz_path.stem.removeprefix("lead_").isdigit():
        return 0
    bin_name = npz_path.stem + ".bin"  # e.g. lead_1.bin
    out_path = out_dir / bin_name
    # Skip the load + byte-compare when the .bin is already newer than its source.
    if (
        not _FORCE_EXPORT
        and out_path.is_file()
        and out_path.stat().st_mtime >= npz_path.stat().st_mtime
    ):
        return 1
    try:
        with np.load(npz_path) as data:
            if field not in data:
                return 0
            arr = data[field].astype(np.float32)
    except Exception:
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_bytes_if_changed(out_path, arr.tobytes())
    return 1


# The forecast point/region series is the precip variable. The multi-variable
# downloaders write ``forecast/{var}/lead_*.npz`` with the array under ``"value"``;
# older single-variable runs wrote ``forecast/lead_*.npz`` under the variable name
# (e.g. ``"precip"``). Prefer the multi-variable layout so re-downloaded forecasts
# are actually picked up — the legacy flat files are left stale on disk otherwise.
def _forecast_source(model_key: str) -> tuple[Path, str]:
    """Return ``(dir, npz_key)`` for a model's forecast precip series.

    Multi-variable layout (``forecast/precip/lead_*.npz`` keyed ``"value"``) wins;
    falls back to the legacy flat layout (``forecast/lead_*.npz`` keyed by the
    render field) for older exports.
    """
    variable = STATISTICS_BY_NAME["forecast"].spec.render_field
    base = STATS_ROOT / model_key / "forecast"
    var_dir = base / variable
    if var_dir.is_dir() and any(var_dir.glob("lead_*.npz")):
        return var_dir, "value"
    return base, variable


def _forecast_variables_on_disk(model_key: str) -> list[str]:
    """Forecast variable keys this model has on disk, in FORECAST_VARIABLES order.

    A variable is present when ``forecast/{var}/`` holds at least one ``lead_*.npz``.
    Returns ``[]`` for the legacy flat layout (no per-variable subdirs); callers
    treat that as precip-only.
    """
    base = STATS_ROOT / model_key / "forecast"
    return [
        var_key
        for var_key in FORECAST_VARIABLES
        if (base / var_key).is_dir() and any((base / var_key).glob("lead_*.npz"))
    ]


def export_forecast_data_layers(forecast_root: Path) -> None:
    """Export each model's forecast variables → ``forecast/{model}/{var}/*.bin``.

    The multi-variable downloaders write ``stats_output/{model}/forecast/{var}/
    lead_*.npz`` (single key ``"value"``) for every variable a model exposes
    (precip/temp/wind/humidity). We mirror that
    into a per-variable ``.bin`` tree (``forecast/{model}/{var}/``) so forecast
    data uploads to S3 independently of any code deploy.
    For older single-variable exports (the legacy flat ``forecast/lead_*.npz``
    keyed ``"precip"``) we still emit ``forecast/{model}/precip/`` so the backend
    has one uniform path layout to read.
    """
    total = 0
    pruned = 0
    for model_key in MODEL_REGISTRY:
        base = STATS_ROOT / model_key / "forecast"
        if not base.is_dir():
            continue
        # Lead windows are gone (Phase 2) — drop stale window bins so the S3
        # sync --delete removes them too.
        for stale_bin in (forecast_root / model_key).rglob("lead_*_*.bin"):
            stale_bin.unlink()
            pruned += 1
        variables = _forecast_variables_on_disk(model_key)
        if variables:
            for var_key in variables:
                var_dir = base / var_key
                out_dir = forecast_root / model_key / var_key
                for npz_path in sorted(var_dir.glob("lead_*.npz")):
                    total += _export_layer(npz_path, "value", out_dir)
        else:
            # Legacy flat layout (precip only) → forecast/{model}/precip/.
            _, npz_key = _forecast_source(model_key)
            out_dir = forecast_root / model_key / "precip"
            for npz_path in sorted(base.glob("lead_*.npz")):
                total += _export_layer(npz_path, npz_key, out_dir)
    if pruned:
        print(f"  pruned {pruned} stale window bin(s)")
    print(f"  {total} forecast .bin files (forecast/{{model}}/{{var}}/)")


def export_forecast_calendar(forecast_root: Path) -> None:
    """Write ``forecast/forecast_calendar.json`` with per-model init dates + variables.

    Each model lists the forecast ``variables`` it has on disk (in
    FORECAST_VARIABLES order; ``["precip"]`` for the legacy flat layout). A
    top-level ``variables`` catalog carries display metadata (label/units/colormap)
    so the SPA can build a variable selector and legend without hardcoding it.
    """
    out = forecast_root / "forecast_calendar.json"
    sources: list[Path] = []
    for key in MODEL_REGISTRY:
        src_dir, _ = _forecast_source(key)
        sources += [src_dir / "metadata.npz", STATS_ROOT / key / "forecast" / "metadata.npz"]
    if _outputs_fresh([out], sources):
        print(f"  {out} (up to date)")
        return

    per_model: dict[str, dict] = {}
    for key, config in sorted(MODEL_REGISTRY.items()):
        init_date = _get_forecast_init_date(key)
        if init_date is None:
            continue
        variables = _forecast_variables_on_disk(key) or ["precip"]
        per_model[key] = {
            "initDate": init_date,
            "leadDaysMin": config.lead_days_min,
            "leadDaysMax": config.lead_days_max,
            "variables": variables,
        }

    catalog = [
        {"key": v.key, "label": v.label, "units": v.units, "colormap": v.colormap}
        for v in FORECAST_VARIABLES.values()
    ]
    payload = {"per_model": per_model, "variables": catalog}
    _write_text_if_changed(out, json.dumps(payload, indent=2))
    print(f"  {out}")


# ── tiles (copy PNGs) ──────────────────────────────────────────────

def _copy_file_if_changed(src: Path, dst: Path) -> bool:
    """Copy *src* → *dst* only if content differs, preserving mtime when unchanged.

    Like the ``_write_*_if_changed`` helpers, this keeps ``aws s3 sync`` from
    re-uploading byte-identical tiles on a rerun. Returns True if a copy occurred.
    """
    try:
        # copy2 preserves the source mtime, so dst.mtime >= src.mtime means the
        # tile is current (fast skip). The byte-compare still runs when --force
        # is set or the source is newer, so identical content never bumps mtime.
        if not _FORCE_EXPORT and dst.stat().st_mtime >= src.stat().st_mtime:
            return False
        if dst.stat().st_size == src.stat().st_size and dst.read_bytes() == src.read_bytes():
            return False
    except FileNotFoundError:
        pass
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def export_tiles(export_dir: Path) -> None:
    """Mirror ``tiles_output/{model}/`` PNGs into ``static/tiles/`` idempotently."""
    tiles_dest = export_dir / "tiles"
    if not TILES_ROOT.exists():
        print("  SKIP: tiles_output/ not found")
        return

    total = copied = pruned = 0
    for model_key in MODEL_REGISTRY:
        src = TILES_ROOT / model_key
        dst = tiles_dest / model_key
        if not src.exists():
            # Model no longer produces tiles — drop any stale exported copy.
            if dst.exists():
                shutil.rmtree(dst)
            continue
        src_rel = {p.relative_to(src) for p in src.rglob("*.png")}
        for rel in sorted(src_rel):
            total += 1
            if _copy_file_if_changed(src / rel, dst / rel):
                copied += 1
        # Prune tiles that no longer exist in the source, then any directory
        # emptied by the prune (e.g. a whole stat dir after a rename).
        if dst.exists():
            for p in dst.rglob("*.png"):
                if p.relative_to(dst) not in src_rel:
                    p.unlink()
                    pruned += 1
            for d in sorted((q for q in dst.rglob("*") if q.is_dir()), reverse=True):
                if not any(d.iterdir()):
                    d.rmdir()

    print(f"  {total} tile PNGs ({copied} updated, {pruned} pruned) under {tiles_dest}")


def _compute_export_value_range(
    layer_paths: list[Path],
    field: str,
    colormap: str,
    fixed_range: tuple[float, float] | None,
) -> tuple[float, float]:
    """Legend numeric range for map tiles (2–98% NPZ percentiles; matches tile coloring)."""
    if fixed_range is not None:
        return fixed_range
    is_diverging = colormap in ("diverging", "diverging_reversed")
    if not layer_paths:
        return (-1.0, 1.0) if is_diverging else (0.0, 1.0)

    finite_parts: list[np.ndarray] = []
    for path in layer_paths:
        try:
            with np.load(path) as data:
                if field not in data:
                    continue
                vals = data[field]
                finite = vals[np.isfinite(vals)]
                if finite.size > 0:
                    finite_parts.append(finite.astype(np.float32))
        except Exception:
            continue

    if not finite_parts:
        return (-1.0, 1.0) if is_diverging else (0.0, 1.0)

    merged = np.concatenate(finite_parts)
    if is_diverging:
        low = float(np.nanpercentile(merged, 2.0))
        high = float(np.nanpercentile(merged, 98.0))
        if high <= low:
            high = low + 1.0
        return low, high

    vmax = float(np.nanpercentile(merged, 98.0))
    return 0.0, max(vmax, 1.0)


def _write_range_json(path: Path, vmin: float, vmax: float, colormap: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_text_if_changed(
        path,
        json.dumps({"vmin": vmin, "vmax": vmax, "colormap": colormap}, indent=2),
    )


def export_value_ranges(assets_dir: Path) -> None:
    """Write ``static/ranges/…/*.json`` for client-side map PNG export (legend scale)."""
    ranges_root = assets_dir / "ranges"
    written = 0
    valid_stats = _verification_stat_names()
    pruned_dirs = 0
    for model_key in MODEL_REGISTRY:
        model_stats = STATS_ROOT / model_key
        if not model_stats.exists():
            continue
        # Drop ranges for stats no longer in the registry (sacc → cc, forecast).
        pruned_dirs += _prune_stale_stat_dirs(ranges_root / model_key, valid_stats)

        for plugin in ALL_STATISTICS:
            stat_name = plugin.spec.name
            if not plugin.spec.map_support:
                continue  # panel-only stats — no map tiles or legend ranges
            stat_dir = model_stats / stat_name
            if not stat_dir.is_dir():
                continue

            spec = plugin.spec
            field = spec.render_field
            colormap = spec.colormap
            fixed = spec.fixed_range
            base = ranges_root / model_key / stat_name

            yearly_npz = sorted(stat_dir.glob("lead_*.npz"))
            if yearly_npz:
                out_path = base / "yearly.json"
                if not _outputs_fresh([out_path], yearly_npz):
                    vmin, vmax = _compute_export_value_range(yearly_npz, field, colormap, fixed)
                    _write_range_json(out_path, vmin, vmax, colormap)
                written += 1

            monthly_root = stat_dir / "monthly"
            if monthly_root.is_dir():
                for month_dir in sorted(monthly_root.glob("*")):
                    if not month_dir.is_dir():
                        continue
                    layer_paths = sorted(month_dir.glob("lead_*.npz"))
                    if not layer_paths:
                        continue
                    out_path = base / "monthly" / f"{month_dir.name}.json"
                    if not _outputs_fresh([out_path], layer_paths):
                        vmin, vmax = _compute_export_value_range(layer_paths, field, colormap, fixed)
                        _write_range_json(out_path, vmin, vmax, colormap)
                    written += 1

            seasonal_root = stat_dir / "seasonal"
            if seasonal_root.is_dir():
                for season_dir in sorted(seasonal_root.glob("*")):
                    if not season_dir.is_dir():
                        continue
                    layer_paths = sorted(season_dir.glob("lead_*.npz"))
                    if not layer_paths:
                        continue
                    out_path = base / "seasonal" / f"{season_dir.name}.json"
                    if not _outputs_fresh([out_path], layer_paths):
                        vmin, vmax = _compute_export_value_range(layer_paths, field, colormap, fixed)
                        _write_range_json(out_path, vmin, vmax, colormap)
                    written += 1

    if pruned_dirs:
        print(f"  pruned {pruned_dirs} stale range dir(s)")
    if written == 0:
        print("  SKIP: no range JSON files (stats_output/ missing or empty)")
    else:
        print(f"  {written} legend range JSON files under {ranges_root}/")


def clear_site_frontend_artifacts(site_root: Path) -> None:
    """Remove the prior bundled SPA (``static_export/frontend/``)."""
    frontend_dir = site_root / FRONTEND_DIR_NAME
    if frontend_dir.is_dir():
        shutil.rmtree(frontend_dir)
        print(f"  removed {frontend_dir}/")


def build_static_frontend(frontend_out_dir: Path) -> None:
    """Build the Svelte SPA into ``frontend_out_dir`` using Vite."""
    frontend_out_dir.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "npm",
            "run",
            "build",
            "--",
            "--outDir",
            str(frontend_out_dir),
        ],
        cwd=FRONTEND_ROOT,
        check=True,
    )
    print(f"  Frontend built to {frontend_out_dir}")


# ── main ────────────────────────────────────────────────────────────

def _maptiler_key() -> str:
    return os.getenv("MAPTILER_API_KEY", "")


def _has_boundary_flags(args: argparse.Namespace) -> bool:
    return any((args.zip, args.admin, args.huc, args.military))


def _resolve_boundary_parts(args: argparse.Namespace) -> dict[str, bool]:
    # --admin / --huc / --military each export both halves
    # (TopoJSON/GeoJSON for S3 + polygons for the image).
    return {
        "zip": args.zip,
        "admin_topo": args.admin,
        "admin_polys": args.admin,
        "huc_topo": args.huc,
        "huc_polys": args.huc,
        "military_geo": args.military,
        "military_polys": args.military,
    }


def _discover_ar_sources() -> list[Path]:
    """Resolve tARget inputs through the canonical AR path authority."""
    return discover_target_ar_sources()


def export_atmospheric_rivers(
    site_root: Path,
    sources: list[Path],
    *,
    variable: str | None,
    min_cells: int,
) -> None:
    """Export tARget v4 frame GeoJSON + manifest through the shared pipeline."""
    from atmospheric_rivers.export import export_files

    export_files(
        sources,
        site_root / AR_DIR_NAME,
        variable,
        max(1, min_cells),
        force=_FORCE_EXPORT,
    )


def _round_or_none(value, ndigits: int) -> float | None:
    """Round to *ndigits*, mapping non-finite (NaN/inf) to None so the emitted
    JSON is valid (``json.dumps`` would otherwise write a bare ``NaN``)."""
    v = float(value)
    return round(v, ndigits) if math.isfinite(v) else None


def export_basin_revisions(site_root: Path) -> None:
    """Export the raw per-basin forecast revision series for the dashboard.

    The dashboard's revision pane and every basin-level analysis (R, lock-in,
    commitment, heavy-rain triggers, revision rationality) ride on the raw
    basin-mean forecast series in ``stats_output/{model}/{basin history}``
    (produced by ``stats_pipeline.basin``), which is otherwise never served.
    One JSON per (model, HUC6) is written under ``ar/basin_revisions/`` for
    every HUC2 region with a basin stage (``BASIN_HUC2_REGIONS``), keeping each
    fetch small: the dashboard reads one basin at a time and fans out one file
    per model. Models/regions whose npz is absent are skipped with a warning.

    ``ar/basins.json`` indexes what was written so the SPA can tell which HUC6
    selections support basin analysis without probing.
    """
    out_root = site_root / AR_DIR_NAME / "basin_revisions"
    written = 0
    index: dict[str, dict] = {}
    for huc2, region_name in BASIN_HUC2_REGIONS.items():
        for model_key in MODEL_REGISTRY:
            npz_path = STATS_ROOT / model_key / basin_history_filename(huc2)
            if not npz_path.is_file():
                print(f"  WARNING: {model_key}: no HUC2 {huc2} basin history "
                      f"({npz_path.name}) — run the basin stage of compute_stats.py; skipped")
                continue
            with np.load(npz_path, allow_pickle=False) as data:
                hucs = [str(h) for h in data["hucs"]]
                names = [str(n) for n in data["names"]]
                dates = [f"{int(d):08d}" for d in data["dates"]]
                lead_min = int(data["lead_min"])
                lead_max = int(data["lead_max"])
                forecasts = data["forecasts"]       # (D, L, B)
                observed = data["observed"]         # (D, B)
                stab = data["lock_stabilization"]   # (D, B)
                acc = data["lock_accuracy"]         # (D, B)
                gap = data["lock_gap"]              # (D, B)
                lock_in_fraction = float(data["lock_in_fraction"])
            n_dates, n_leads, _ = forecasts.shape
            window = MODEL_REGISTRY[model_key].window_block()
            primary_label = (window.get("primary") or {}).get("label")
            primary_valid_range = [
                (d.replace("-", "") if isinstance(d, str) else None)
                for d in (window.get("primary_valid_range") or [None, None])
            ]
            for bi, huc in enumerate(hucs):
                payload = {
                    "huc": huc,
                    "huc2": huc2,
                    "region": region_name,
                    "name": names[bi],
                    "model": model_key,
                    "lead_min": lead_min,
                    "lead_max": lead_max,
                    # Primary model-version window as [first, last] valid date
                    # (YYYYMMDD or null = open): pooled products restrict to it.
                    "primary_window": primary_label,
                    "primary_valid_range": primary_valid_range,
                    "dates": dates,
                    # Forecast/observation precip rounded to 0.01 mm; lock leads are
                    # already 0.5-day-quantized so one decimal is lossless.
                    "forecasts": [
                        [_round_or_none(forecasts[di, li, bi], 2) for li in range(n_leads)]
                        for di in range(n_dates)
                    ],
                    "observed": [_round_or_none(observed[di, bi], 2) for di in range(n_dates)],
                    "lock": {
                        "stabilization": [_round_or_none(stab[di, bi], 1) for di in range(n_dates)],
                        "accuracy": [_round_or_none(acc[di, bi], 1) for di in range(n_dates)],
                        "gap": [_round_or_none(gap[di, bi], 1) for di in range(n_dates)],
                    },
                    "lock_in_fraction": lock_in_fraction,
                }
                out_path = out_root / model_key / f"{huc}.json"
                if _write_text_if_changed(out_path, json.dumps(payload, separators=(",", ":"))):
                    written += 1
                entry = index.setdefault(huc, {
                    "name": names[bi], "huc2": huc2, "region": region_name, "models": [],
                })
                if model_key not in entry["models"]:
                    entry["models"].append(model_key)
    index_payload = {
        "huc_level": 6,
        "regions": [
            {"huc2": huc2, "name": name,
             "basins": sorted(h for h, e in index.items() if e["huc2"] == huc2)}
            for huc2, name in BASIN_HUC2_REGIONS.items()
        ],
        "basins": dict(sorted(index.items())),
    }
    _write_text_if_changed(site_root / AR_DIR_NAME / "basins.json",
                           json.dumps(index_payload, separators=(",", ":")))
    print(f"  {written} basin revision file(s) exported; {len(index)} basins indexed")


# Storm-table fields carried per event (per-model dicts), in catalog order.
_STORM_MODEL_FIELDS = ("j3", "j3_ca", "r_eff", "mae3", "fss3", "fss3_ca", "r3")


def export_storm_catalog(site_root: Path) -> None:
    """Export the curated storm catalogs as ``ar/storms/{huc2}.json``.

    Merges ``data/ar_events[_hucNN].json`` (the region-wide AR
    event catalog: 72-h clusters merged across basins, PRISM accumulation,
    volume rank, peak-day climatology percentile) with
    ``storm_table[_hucNN].json`` (the per-storm ledger: storm-pooled J at pair
    3, per-event R, lead-3 MAE/FSS/correlation, and the per-event crowns).
    Both are offline curated artifacts; a region whose extractors have not
    run is skipped — the dashboard then falls back to the episode catalog.
    """
    out_root = site_root / AR_DIR_NAME / "storms"
    for huc2, region_name in BASIN_HUC2_REGIONS.items():
        suffix = "" if huc2 == "18" else f"_huc{huc2}"
        events_path = FIGURE_DATA_ROOT / f"ar_events{suffix}.json"
        if not events_path.is_file():
            print(f"  SKIP: {events_path.name} not found (run figures.data.ar_events)")
            continue
        events_doc = json.loads(events_path.read_text(encoding="utf-8"))
        table_path = FIGURE_DATA_ROOT / f"storm_table{suffix}.json"
        table_by_id: dict[str, dict] = {}
        table_meta = None
        if table_path.is_file():
            table_doc = json.loads(table_path.read_text(encoding="utf-8"))
            table_by_id = {e["event_id"]: e for e in table_doc.get("events", [])}
            table_meta = table_doc.get("meta")
        events_out = []
        for ev in events_doc.get("events", []):
            obs = ev.get("observed") or {}
            row = {
                "event_id": ev["event_id"],
                "start_date": ev["start_date"],
                "end_date": ev["end_date"],
                "span_days": ev.get("span_days"),
                "basins": ev.get("basins"),
                "basin_hucs": ev.get("basin_hucs", []),
                "in_analysis": bool(ev.get("in_analysis")),
                "observed_coverage": ev.get("observed_coverage"),
                "volume_km3": obs.get("volume_km3"),
                "record_share_pct": obs.get("record_share_pct"),
                "volume_rank": ev.get("volume_rank"),
                "volume_rank_of": ev.get("volume_rank_of"),
                "peak": {
                    "date": obs.get("ca_peak_date"),
                    "mm": obs.get("ca_peak_day_mm"),
                    "prism_pct": obs.get("ca_peak_prism_pct"),
                    "basin_huc": obs.get("peak_basin_huc"),
                    "basin_name": obs.get("peak_basin_name"),
                    "basin_date": obs.get("peak_basin_date"),
                    "basin_mm": obs.get("peak_basin_day_mm"),
                },
                "climatology": (ev.get("climatology") or {}).get("ca_peak_day"),
                "flagship": bool(ev.get("flagship")),
            }
            table = table_by_id.get(ev["event_id"])
            if table:
                row["scored"] = bool(table.get("scored"))
                row["models"] = {field: table.get(field) for field in _STORM_MODEL_FIELDS
                                 if table.get(field) is not None}
                row["r_eff_detail"] = table.get("r_eff_detail")
                row["winners"] = table.get("winners")
                row["j_by_pair"] = table.get("j_ca_by_pair")
            events_out.append(row)
        payload = {
            "huc2": huc2,
            "region": region_name,
            "record": events_doc.get("record"),
            "baseline": events_doc.get("baseline"),
            "basin_names": events_doc.get("basin_names"),
            "flagship": events_doc.get("flagship"),
            "events": events_out,
            "ledger_meta": table_meta,
            "events_meta": events_doc.get("meta"),
        }
        out_path = out_root / f"{huc2}.json"
        _write_text_if_changed(out_path, json.dumps(payload, separators=(",", ":")))
        print(f"  {out_path} ({len(events_out)} events, "
              f"{sum(1 for e in events_out if e['in_analysis'])} in analysis)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export static files for serverless deployment.",
    )
    parser.add_argument(
        "--output", default=str(EXPORT_ROOT),
        help=f"Output directory (default: {EXPORT_ROOT})",
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Delete output directory before exporting (full export only)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-export every artifact even if it's up to date on disk "
             "(default skips outputs newer than their sources)",
    )
    parser.add_argument(
        "--static", action="store_true",
        help="S3 site export: zip/, admin/wbd/military (TopoJSON + backend polygon "
             "chunks), config, tiles, ranges, forecast/ — everything but model_data/ "
             "(--data) and the SPA (--frontend)",
    )
    parser.add_argument(
        "--data", action="store_true",
        help="Only rebuild static_export/model_data/ (.bin layers and grid.json)",
    )
    parser.add_argument(
        "--forecast", action="store_true",
        help="Only rebuild forecast bins + forecast_calendar.json under static_export/forecast/",
    )
    parser.add_argument(
        "--frontend", action="store_true",
        help="Only rebuild the bundled Vite SPA under static_export/frontend/",
    )
    parser.add_argument(
        "--ar", action="store_true",
        help="Only export Guan & Waliser tARget v4 netCDF masks under static_export/ar/",
    )
    parser.add_argument(
        "--basins", action="store_true",
        help="Only export the per-basin revision series, basins index and storm "
             "catalogs under static_export/ar/ (plus the regional episode catalogs "
             "under model_data/)",
    )
    parser.add_argument(
        "--ar-variable", metavar="NAME",
        help="tARget mask variable name (auto-detected by default)",
    )
    parser.add_argument(
        "--ar-min-cells", type=int, default=1, metavar="N",
        help="Drop connected AR components smaller than N grid cells (default: 1)",
    )
    parser.add_argument(
        "--zip", action="store_true",
        help="Only zip/{code}.json from boundary_data/zip/zip_lookup.csv",
    )
    parser.add_argument(
        "--admin", action="store_true",
        help="Admin boundaries.json + states/counties TopoJSON",
    )
    parser.add_argument(
        "--huc", action="store_true",
        help="WBD map TopoJSON/index + chunks/ polygon tree",
    )
    parser.add_argument(
        "--military", action="store_true",
        help="Military sites boundaries.json + sites.geojson (DISDI MIRTA)",
    )
    parser.add_argument(
        "--max-huc-level", type=int, choices=WBD_HU_LEVELS, default=DEFAULT_MAX_HUC_LEVEL,
        help=f"Deepest WBD HU level to export (default: {DEFAULT_MAX_HUC_LEVEL}; "
             "boundary_data/ keeps full HU16 source)",
    )
    args = parser.parse_args()

    mode_count = (
        int(args.static) + int(args.data)
        + int(args.forecast) + int(args.frontend) + int(args.ar) + int(args.basins)
    )
    if mode_count > 1:
        parser.error("Use at most one of --static, --data, --forecast, --frontend, --ar.")
    if args.clean and mode_count:
        parser.error(
            "--clean applies only to a full export "
            "(omit --static, --data, --forecast, --frontend, and --ar)."
        )
    if _has_boundary_flags(args) and mode_count:
        parser.error(
            "Boundary flags (--zip, --admin, --huc, --military) cannot combine "
            "with --static, --data, --forecast, --frontend, or --ar."
        )
    ar_options_set = bool(args.ar_variable or args.ar_min_cells != 1)
    if ar_options_set and (args.data or args.forecast or args.frontend or _has_boundary_flags(args)):
        parser.error(
            "AR options can only be used with --ar, --static, or a full export."
        )

    # --clean wipes the tree, so a full rebuild follows naturally; --force keeps
    # the existing outputs but regenerates them regardless of staleness.
    global _FORCE_EXPORT
    _FORCE_EXPORT = args.force or args.clean

    site_root = Path(args.output)
    ar_sources = _discover_ar_sources()
    if args.ar and not ar_sources:
        parser.error(
            f"--ar requires a cached tARget date range under "
            f"{TARGET_AR_DATA_DIR}; run download.py --ar with dates first."
        )

    if _has_boundary_flags(args) and mode_count == 0:
        parts = _resolve_boundary_parts(args)
        if not any(parts.values()):
            parser.error("No boundary export selected.")
        site_root.mkdir(parents=True, exist_ok=True)
        print("Exporting boundaries...")
        export_boundary_parts(site_root, **parts, huc_max_level=args.max_huc_level)
        print(f"\nDone. Boundaries under {site_root}")
        return

    if args.frontend:
        site_root.mkdir(parents=True, exist_ok=True)
        frontend_root = site_root / FRONTEND_DIR_NAME
        print("--frontend: clearing old Vite output...")
        clear_site_frontend_artifacts(site_root)
        print("Building static frontend...")
        build_static_frontend(frontend_root)
        spa_bytes = sum(
            f.stat().st_size for f in frontend_root.rglob("*") if f.is_file()
        )
        print(f"\nDone (--frontend). SPA ~ {spa_bytes / (1024 * 1024):.2f} MB under {frontend_root}")
        return

    if args.ar:
        site_root.mkdir(parents=True, exist_ok=True)
        print("--ar: exporting Guan & Waliser tARget v4 frames...")
        export_atmospheric_rivers(
            site_root,
            ar_sources,
            variable=args.ar_variable,
            min_cells=args.ar_min_cells,
        )
        print(f"\nDone (--ar). Atmospheric-river assets under {site_root / AR_DIR_NAME}")
        return

    if args.basins:
        site_root.mkdir(parents=True, exist_ok=True)
        data_root = site_root / DATA_DIR_NAME
        print("--basins: copying regional episode catalogs...")
        for model_key in MODEL_REGISTRY:
            model_stats = STATS_ROOT / model_key
            for huc2 in BASIN_HUC2_REGIONS:
                episode_src = model_stats / episode_catalog_filename(huc2)
                if episode_src.is_file():
                    episode_out = data_root / model_key / episode_catalog_filename(huc2)
                    episode_out.parent.mkdir(parents=True, exist_ok=True)
                    _write_text_if_changed(episode_out, episode_src.read_text(encoding="utf-8"))
        print("--basins: exporting basin revision series...")
        export_basin_revisions(site_root)
        print("--basins: exporting storm catalogs...")
        export_storm_catalog(site_root)
        print(f"\nDone (--basins). Basin assets under {site_root / AR_DIR_NAME}")
        return

    if args.static:
        site_root.mkdir(parents=True, exist_ok=True)
        maptiler_key = _maptiler_key()
        # Both halves of every boundary set: map TopoJSON/GeoJSON for the
        # browser AND the polygon chunks/boundaries.json the backend reads
        # (all S3-served now — the Lambda fetches its polygons on demand).
        print("--static: exporting zip/, admin/, wbd/, military/ (map assets + backend polygons)...")
        export_boundaries(site_root, huc_max_level=args.max_huc_level)
        print("--static: exporting config, tiles, ranges...")
        export_s3_assets(site_root, maptiler_key)
        forecast_root = site_root / FORECAST_DIR_NAME
        forecast_root.mkdir(parents=True, exist_ok=True)
        print("--static: exporting forecast data layers...")
        export_forecast_data_layers(forecast_root)
        print("--static: exporting forecast calendar...")
        export_forecast_calendar(forecast_root)
        if ar_sources:
            print("--static: exporting tARget v4 atmospheric-river frames...")
            export_atmospheric_rivers(
                site_root,
                ar_sources,
                variable=args.ar_variable,
                min_cells=args.ar_min_cells,
            )
        else:
            print("--static: SKIP atmospheric-river frames (no tARget source configured)")
        print("--static: exporting basin revision series...")
        export_basin_revisions(site_root)
        export_storm_catalog(site_root)
        print(
            f"\nDone (--static). S3 assets under {site_root} "
            f"and {forecast_root}"
        )
        return

    if args.data:
        site_root.mkdir(parents=True, exist_ok=True)
        data_root = site_root / DATA_DIR_NAME
        data_root.mkdir(parents=True, exist_ok=True)
        print("--data: exporting data layers...")
        export_data_layers(data_root)
        print("--data: exporting model grids...")
        export_model_grids(data_root)
        print(f"\nDone (--data). Data under {data_root}")
        return

    if args.forecast:
        site_root.mkdir(parents=True, exist_ok=True)
        forecast_root = site_root / FORECAST_DIR_NAME
        forecast_root.mkdir(parents=True, exist_ok=True)
        print("--forecast: exporting forecast data layers...")
        export_forecast_data_layers(forecast_root)
        print("--forecast: exporting forecast calendar...")
        export_forecast_calendar(forecast_root)
        print(f"\nDone (--forecast). Forecast data under {forecast_root}")
        return

    if args.clean and site_root.exists():
        shutil.rmtree(site_root)
    site_root.mkdir(parents=True, exist_ok=True)
    data_root = site_root / DATA_DIR_NAME
    forecast_root = site_root / FORECAST_DIR_NAME
    data_root.mkdir(parents=True, exist_ok=True)
    forecast_root.mkdir(parents=True, exist_ok=True)

    maptiler_key = _maptiler_key()
    print("Exporting boundaries...")
    export_boundaries(site_root, huc_max_level=args.max_huc_level)

    print("Exporting S3 static assets (config, tiles, ranges)...")
    export_s3_assets(site_root, maptiler_key)

    print("Exporting data layers...")
    export_data_layers(data_root)

    print("Exporting model grids...")
    export_model_grids(data_root)

    print("Exporting forecast data layers...")
    export_forecast_data_layers(forecast_root)

    print("Exporting forecast calendar...")
    export_forecast_calendar(forecast_root)

    if ar_sources:
        print("Exporting tARget v4 atmospheric-river frames...")
        export_atmospheric_rivers(
            site_root,
            ar_sources,
            variable=args.ar_variable,
            min_cells=args.ar_min_cells,
        )
    else:
        print("SKIP atmospheric-river frames (no tARget source configured)")

    print("Exporting basin revision series...")
    export_basin_revisions(site_root)
    export_storm_catalog(site_root)

    print("Building static frontend...")
    build_static_frontend(site_root / FRONTEND_DIR_NAME)

    total_size = sum(f.stat().st_size for f in site_root.rglob("*") if f.is_file())
    print(f"\nDone. Total export: {total_size / (1024 * 1024):.1f} MB in {site_root}")


if __name__ == "__main__":
    main()
