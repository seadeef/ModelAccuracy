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
            index.json
            huc2.topojson

URL scheme: every browser-fetched data file is served from the site root mirroring
this layout (e.g. ``/config.json``, ``/wbd/index.json``, ``/model_data/…``); the SPA
(``frontend/`` contents) is served from the root too (``/`` → ``index.html``).

Boundary source data lives in ``boundary_data/`` (data only), populated by
``load_boundaries.py``. ``export_static.py`` reads it and writes
``static_export/`` (the deploy input tree).

Build modes (at most one; omit all for a full export):

* ``--image`` — Docker image inputs: admin/HUC polygon exports + ``model_data/`` (``.bin`` + ``grid.json``).
* ``--static`` — S3/CloudFront assets: ``zip/``, admin/HUC TopoJSON, ``config.json``,
  ``tiles/``, ``ranges/``, and ``forecast/`` (bins + ``forecast_calendar.json``).
* ``--data`` — only ``static_export/model_data/``.
* ``--forecast`` — only ``static_export/forecast/``.
* ``--frontend`` — only the bundled Vite SPA under ``static_export/frontend/``.

Boundary export flags (combinable with each other; not with build modes above):

* ``--zip`` — ``zip/{code}.json`` from ``boundary_data/zip/zip_lookup.csv``.
* ``--admin`` — ``boundaries.json`` + ``states.topojson`` + ``counties.topojson``.
* ``--huc`` — WBD map assets (``huc2.topojson``, ``children/``, ``index.json``) + ``chunks/``.

WBD export depth is capped by ``--max-huc-level`` (default 12) to bound image size /
S3 object count; ``boundary_data/`` keeps the full HU16 source for re-export.

Load boundaries first when ``boundary_data/`` is empty::

    python load_boundaries.py            # zip + admin + wbd
    python load_boundaries.py --wbd-sample   # synthetic WBD layers for local dev

``--clean`` deletes the entire output directory first; only allowed for a **full**
export (no ``--static`` / ``--image`` / ``--data`` / ``--forecast`` / ``--frontend``).

The .bin files are raw float32 arrays (row-major, height × width).

Best-model-by-lead for a map region is computed on demand via
``POST /api/stats/lead-winners`` (not exported to static files).
"""

from __future__ import annotations

import argparse
import csv
import json
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
from forecast_variables import FORECAST_VARIABLES
from stats_grid_metadata import load_model_metadata
from statistics_plugins.registry import (
    DEFAULT_STATISTIC,
    ENABLED_STATISTICS,
    STATISTICS_BY_NAME,
)

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
# Served WBD depth (chunks baked into the image + map assets on S3). Default comes
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
    orphaned chunk/children files to be baked into the image or synced to S3.
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
                chunk_path = chunks_dir / str(parent_level) / f"{parent}.json"
                written_chunks.add(chunk_path.resolve())
                _write_text_if_changed(
                    chunk_path,
                    json.dumps({"parent": parent, "level": level, "units": units}, separators=(",", ":")),
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
    root_units = {
        huc: {"name": e["name"], "bbox": e["bbox"], "polygons": e["polygons"]}
        for huc, e in units_by_level[2].items()
    }
    if write_chunks:
        root_path = chunks_dir / "2" / "_root.json"
        written_chunks.add(root_path.resolve())
        _write_text_if_changed(
            root_path,
            json.dumps({"parent": "", "level": 2, "units": root_units}, separators=(",", ":")),
        )
    index_path = out_dir / "index.json"
    if write_map_assets:
        _write_text_if_changed(
            index_path,
            json.dumps(
                {
                    "version": 1,
                    "source": WBD_SOURCE_LABEL,
                    "levels": available_levels,
                    "units": index,
                },
                separators=(",", ":"),
            ),
        )
    # Prune stale artifacts so the served tree matches the current source + cap.
    # Each mode only prunes what it writes (--image → chunks, --static → children).
    if write_chunks:
        stale = _prune_stale(chunks_dir, written_chunks, ".json")
        if stale:
            print(f"  pruned {stale} stale chunk file(s)")
    if write_map_assets:
        stale = _prune_stale(children_dir, written_children, ".topojson")
        if stale:
            print(f"  pruned {stale} stale children topojson(s)")
    if write_map_assets:
        for p in (index_path, huc2_path):
            print(f"  {p} ({p.stat().st_size:,} bytes)")
    if write_chunks:
        print(f"  {sum(1 for _ in chunks_dir.rglob('*.json'))} chunk files under {chunks_dir}/")


def export_boundary_parts(
    site_root: Path,
    *,
    zip: bool = False,
    admin_topo: bool = False,
    admin_polys: bool = False,
    huc_topo: bool = False,
    huc_polys: bool = False,
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


def export_boundaries(site_root: Path, huc_max_level: int = DEFAULT_MAX_HUC_LEVEL) -> None:
    """All boundary parts (zip, admin topo+polys, HUC topo+polys)."""
    export_boundary_parts(
        site_root,
        zip=True,
        admin_topo=True,
        admin_polys=True,
        huc_topo=True,
        huc_polys=True,
        huc_max_level=huc_max_level,
    )


def export_s3_assets(site_root: Path, maptiler_key: str) -> None:
    """Export S3 core assets: config, tiles, ranges (not zip or boundary TopoJSON)."""
    assets_dir = site_root
    assets_dir.mkdir(parents=True, exist_ok=True)
    export_config(assets_dir, maptiler_key)
    export_tiles(assets_dir)
    export_value_ranges(assets_dir)


# ── config.json ─────────────────────────────────────────────────────

def export_config(export_dir: Path, maptiler_key: str) -> None:
    models = []
    for _, config in sorted(MODEL_REGISTRY.items()):
        entry = {
            "key": config.key,
            "label": config.label,
            "lead_days_min": config.lead_days_min,
            "lead_days_max": config.lead_days_max,
            "lead_windows": [list(w) for w in config.lead_windows],
        }
        if config.ident:
            entry["ident"] = config.ident
        models.append(entry)

    statistics = [
        {"key": p.spec.name, "label": p.spec.label, "units": p.spec.units}
        for p in ENABLED_STATISTICS
    ]

    config_data = {
        "models": models,
        "default_model": DEFAULT_MODEL,
        "statistics": statistics,
        "default_statistic": DEFAULT_STATISTIC,
        "maptiler_api_key": maptiler_key,
    }

    out = export_dir / "config.json"
    _write_text_if_changed(out, json.dumps(config_data, indent=2))
    print(f"  {out}")


# ── data layers (.bin) ──────────────────────────────────────────────

def export_data_layers(data_root: Path) -> None:
    data_dir = data_root
    total = 0

    for model_key in MODEL_REGISTRY:
        model_stats = STATS_ROOT / model_key
        if not model_stats.exists():
            continue

        # Export each layer as flat float32 .bin (verification stats + any non-forecast dirs).
        for plugin in ENABLED_STATISTICS:
            stat_name = plugin.spec.name
            if stat_name == "forecast":
                # Separate tree (``static_export/forecast/``) — see ``export_forecast_data_layers``.
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

    print(f"  {total} .bin layer files exported")


def export_model_grids(data_root: Path) -> None:
    """Write ``data/{model}/grid.json`` from each model's ``metadata.npz`` (for static stats queries)."""
    count = 0
    for model_key in MODEL_REGISTRY:
        model_stats = STATS_ROOT / model_key
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
        out_path = data_root / model_key / "grid.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        _write_text_if_changed(out_path, json.dumps(payload))
        count += 1
    print(f"  {count} grid.json files under data/{{model}}/")


def _export_layer(npz_path: Path, field: str, out_dir: Path) -> int:
    """Extract the render field from an .npz and write as flat float32 .bin. Returns 1 on success."""
    try:
        with np.load(npz_path) as data:
            if field not in data:
                return 0
            arr = data[field].astype(np.float32)
    except Exception:
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    bin_name = npz_path.stem + ".bin"  # e.g. lead_1.bin
    out_path = out_dir / bin_name
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
    (precip/temp/wind/humidity, plus the lead-window aggregates). We mirror that
    into a per-variable ``.bin`` tree (``forecast/{model}/{var}/``) so forecast
    data can be uploaded to S3 independently and is never baked into the Fargate
    image. For older single-variable exports (the legacy flat ``forecast/lead_*.npz``
    keyed ``"precip"``) we still emit ``forecast/{model}/precip/`` so the backend
    has one uniform path layout to read.
    """
    total = 0
    for model_key in MODEL_REGISTRY:
        base = STATS_ROOT / model_key / "forecast"
        if not base.is_dir():
            continue
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
    print(f"  {total} forecast .bin files (forecast/{{model}}/{{var}}/)")


def export_forecast_calendar(forecast_root: Path) -> None:
    """Write ``forecast/forecast_calendar.json`` with per-model init dates + variables.

    Each model lists the forecast ``variables`` it has on disk (in
    FORECAST_VARIABLES order; ``["precip"]`` for the legacy flat layout). A
    top-level ``variables`` catalog carries display metadata (label/units/colormap)
    so the SPA can build a variable selector and legend without hardcoding it.
    """
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
    out = forecast_root / "forecast_calendar.json"
    _write_text_if_changed(out, json.dumps(payload, indent=2))
    print(f"  {out}")


# ── tiles (copy PNGs) ──────────────────────────────────────────────

def _copy_file_if_changed(src: Path, dst: Path) -> bool:
    """Copy *src* → *dst* only if content differs, preserving mtime when unchanged.

    Like the ``_write_*_if_changed`` helpers, this keeps ``aws s3 sync`` from
    re-uploading byte-identical tiles on a rerun. Returns True if a copy occurred.
    """
    try:
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
        # Prune tiles that no longer exist in the source.
        if dst.exists():
            for p in dst.rglob("*.png"):
                if p.relative_to(dst) not in src_rel:
                    p.unlink()
                    pruned += 1

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
    for model_key in MODEL_REGISTRY:
        model_stats = STATS_ROOT / model_key
        if not model_stats.exists():
            continue

        for plugin in ENABLED_STATISTICS:
            stat_name = plugin.spec.name
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
                vmin, vmax = _compute_export_value_range(yearly_npz, field, colormap, fixed)
                _write_range_json(base / "yearly.json", vmin, vmax, colormap)
                written += 1

            monthly_root = stat_dir / "monthly"
            if monthly_root.is_dir():
                for month_dir in sorted(monthly_root.glob("*")):
                    if not month_dir.is_dir():
                        continue
                    layer_paths = sorted(month_dir.glob("lead_*.npz"))
                    if not layer_paths:
                        continue
                    vmin, vmax = _compute_export_value_range(layer_paths, field, colormap, fixed)
                    _write_range_json(base / "monthly" / f"{month_dir.name}.json", vmin, vmax, colormap)
                    written += 1

            seasonal_root = stat_dir / "seasonal"
            if seasonal_root.is_dir():
                for season_dir in sorted(seasonal_root.glob("*")):
                    if not season_dir.is_dir():
                        continue
                    layer_paths = sorted(season_dir.glob("lead_*.npz"))
                    if not layer_paths:
                        continue
                    vmin, vmax = _compute_export_value_range(layer_paths, field, colormap, fixed)
                    _write_range_json(base / "seasonal" / f"{season_dir.name}.json", vmin, vmax, colormap)
                    written += 1

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
    return any((args.zip, args.admin, args.huc))


def _resolve_boundary_parts(args: argparse.Namespace) -> dict[str, bool]:
    # --admin / --huc each export both halves (TopoJSON for S3 + polygons for the image).
    return {
        "zip": args.zip,
        "admin_topo": args.admin,
        "admin_polys": args.admin,
        "huc_topo": args.huc,
        "huc_polys": args.huc,
    }


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
        "--static", action="store_true",
        help="S3/CloudFront export: zip/, admin/HUC TopoJSON, config, tiles, ranges, forecast/",
    )
    parser.add_argument(
        "--image", action="store_true",
        help="Docker image export: admin/HUC polygons + static_export/model_data/",
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
        "--max-huc-level", type=int, choices=WBD_HU_LEVELS, default=DEFAULT_MAX_HUC_LEVEL,
        help=f"Deepest WBD HU level to export (default: {DEFAULT_MAX_HUC_LEVEL}; "
             "boundary_data/ keeps full HU16 source)",
    )
    args = parser.parse_args()

    mode_count = (
        int(args.static) + int(args.image) + int(args.data)
        + int(args.forecast) + int(args.frontend)
    )
    if mode_count > 1:
        parser.error("Use at most one of --static, --image, --data, --forecast, --frontend.")
    if args.clean and mode_count:
        parser.error(
            "--clean applies only to a full export "
            "(omit --static, --image, --data, --forecast, and --frontend)."
        )
    if _has_boundary_flags(args) and mode_count:
        parser.error(
            "Boundary flags (--zip, --admin, --huc) cannot combine "
            "with --static, --image, --data, --forecast, or --frontend."
        )

    site_root = Path(args.output)

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

    if args.static:
        site_root.mkdir(parents=True, exist_ok=True)
        maptiler_key = _maptiler_key()
        print("--static: exporting zip/ and admin/HUC TopoJSON...")
        export_boundary_parts(
            site_root, zip=True, admin_topo=True, huc_topo=True,
            huc_max_level=args.max_huc_level,
        )
        print("--static: exporting config, tiles, ranges...")
        export_s3_assets(site_root, maptiler_key)
        forecast_root = site_root / FORECAST_DIR_NAME
        forecast_root.mkdir(parents=True, exist_ok=True)
        print("--static: exporting forecast data layers...")
        export_forecast_data_layers(forecast_root)
        print("--static: exporting forecast calendar...")
        export_forecast_calendar(forecast_root)
        print(
            f"\nDone (--static). S3 assets under {site_root} "
            f"and {forecast_root}"
        )
        return

    if args.image:
        site_root.mkdir(parents=True, exist_ok=True)
        print("--image: exporting admin/HUC polygons...")
        export_boundary_parts(
            site_root, admin_polys=True, huc_polys=True,
            huc_max_level=args.max_huc_level,
        )
        data_root = site_root / DATA_DIR_NAME
        data_root.mkdir(parents=True, exist_ok=True)
        print("--image: exporting data layers...")
        export_data_layers(data_root)
        print("--image: exporting model grids...")
        export_model_grids(data_root)
        print(f"\nDone (--image). Docker inputs under {site_root}")
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

    print("Building static frontend...")
    build_static_frontend(site_root / FRONTEND_DIR_NAME)

    total_size = sum(f.stat().st_size for f in site_root.rglob("*") if f.is_file())
    print(f"\nDone. Total export: {total_size / (1024 * 1024):.1f} MB in {site_root}")


if __name__ == "__main__":
    main()
