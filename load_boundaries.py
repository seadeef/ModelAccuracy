#!/usr/bin/env python3
"""Load boundary source data into ``boundary_data/`` (the data-only store).

Writes the normalized outputs that ``export_static.py`` reads:

    boundary_data/zip/zip_lookup.csv          (--zip)
    boundary_data/admin/{states,counties}-10m.json   (--admin)
    boundary_data/wbd/layers/hu{2,4,…}.json   (--wbd, or --wbd-sample for dev)
    boundary_data/military/sites.json         (--military, or --military-sample for dev)

The ZIP lookup CSV is derived from the US Census ZCTA cartographic-boundary
shapefile (each ZIP's fly-to centroid and zoom bbox come from its polygon); the
raw ~64 MB zip lands in ``boundary_data/cache/`` and is reused on reruns. The
large WBD National GDB download (~2.7 GB zip + extracted ``.gdb``) lands there
too and is likewise reused.

Usage::

    python load_boundaries.py                  # everything (zip + admin + wbd + military)
    python load_boundaries.py --zip            # only the ZIP lookup CSV
    python load_boundaries.py --admin          # only us-atlas TopoJSON
    python load_boundaries.py --wbd            # only WBD HU layers (downloads GDB)
    python load_boundaries.py --wbd-sample     # synthetic WBD layers for local dev
    python load_boundaries.py --wbd --force    # re-decode even if layers exist
    python load_boundaries.py --wbd --max-huc-level 16   # full source (default caps at HU12)
    python load_boundaries.py --military       # only DISDI MIRTA military sites (~4 MB zip)
    python load_boundaries.py --military-sample  # synthetic military sites for local dev

Selection flags (``--zip``/``--admin``/``--wbd``/``--wbd-sample``/``--military``/
``--military-sample``) are additive; with none given, all of zip + admin + wbd +
military run.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import ssl
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

from boundary_paths import (
    ADMIN_COUNTIES_TOPOJSON,
    ADMIN_STATES_TOPOJSON,
    MILITARY_CA_PEM,
    MILITARY_DIR,
    MILITARY_GDB_DIR,
    MILITARY_SITES_JSON,
    MILITARY_SOURCE_LABEL,
    MILITARY_URL,
    MILITARY_ZIP,
    PROJECT_ROOT,
    US_ATLAS_CDN,
    WBD_CACHE,
    WBD_GDB_DIR,
    WBD_GDB_ZIP,
    WBD_LAYERS_DIR,
    WBD_SCIENCEBASE_URL,
    ZIP_DIR,
    ZIP_LOOKUP_CSV,
    ZIP_ZCTA_CODE_FIELD,
    ZIP_ZCTA_SHP_NAME,
    ZIP_ZCTA_URL,
    ZIP_ZCTA_ZIP,
)
from wbd_core import (
    DEFAULT_MAX_HUC_LEVEL,
    HU_LEVELS,
    SIMPLIFY_TOLERANCE,
    geom_to_polygons,
    huc_field,
    in_branch,
    layer_cache_path,
    layer_name,
    normalize_huc,
    polygons_bbox,
)

# Completion marker listing the HU levels a finished decode produced. Used so the
# --wbd short-circuit only treats layers as cached when *every* produced detail
# layer is on disk (an interrupted decode leaves no manifest → it re-runs).
WBD_LAYERS_MANIFEST = WBD_LAYERS_DIR / "manifest.json"


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _download(
    url: str, dest: Path, *, user_agent: str, timeout: int = 600, extra_cafile: Path | None = None
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"==> downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    context = None
    if extra_cafile is not None:
        # Default verification plus a pinned chain, for hosts that serve an
        # incomplete certificate chain (e.g. www.acq.osd.mil).
        context = ssl.create_default_context()
        context.load_verify_locations(cafile=str(extra_cafile))
    with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
        data = resp.read()
    dest.write_bytes(data)
    print(f"  wrote {_rel(dest)} ({dest.stat().st_size:,} bytes)")


# ── ZIP lookup ──────────────────────────────────────────────────────
#
# The frontend flies to a ZIP's centroid and zooms to its bounding box (it
# ``fitBounds`` the [W,S,E,N] ``bounds`` from ``static/zip/{code}.json``). Both
# are derived here from the US Census ZCTA cartographic-boundary shapefile: the
# fly target is each ZCTA polygon's centroid, the zoom box is its bounds. The
# normalized output keeps the historical CSV schema so ``export_static.py`` and
# the frontend need no changes.
ZIP_CSV_HEADER = ["zip", "lat", "lon", "min_lon", "min_lat", "max_lon", "max_lat"]


def _resolve_zcta_zip(zcta_zip: Path | None) -> Path:
    """Return the ZCTA boundary zip, preferring the cached copy in ``cache/``.

    Resolution order (the ~64 MB zip is downloaded only when nothing is cached):

      1. an explicit ``--zcta-zip`` (staged into ``cache/`` once, then reused)
      2. the cached zip in ``cache/``
      3. download the ZCTA cartographic-boundary zip from the US Census source
    """
    if (
        zcta_zip is not None
        and zcta_zip.is_file()
        and zcta_zip.resolve() != ZIP_ZCTA_ZIP.resolve()
        and not ZIP_ZCTA_ZIP.is_file()
    ):
        WBD_CACHE.mkdir(parents=True, exist_ok=True)
        print(f"==> staging {_rel(zcta_zip)} → {_rel(ZIP_ZCTA_ZIP)}")
        shutil.copy2(zcta_zip, ZIP_ZCTA_ZIP)

    if ZIP_ZCTA_ZIP.is_file() and ZIP_ZCTA_ZIP.stat().st_size > 1_000_000:
        print(f"==> using cached {_rel(ZIP_ZCTA_ZIP)}")
        return ZIP_ZCTA_ZIP

    _download(ZIP_ZCTA_URL, ZIP_ZCTA_ZIP, user_agent="ModelAccuracy load_boundaries (zip)")
    return ZIP_ZCTA_ZIP


def _read_zcta_rows(zcta_zip: Path) -> list[tuple[str, float, float, float, float, float, float]]:
    """Decode each ZCTA polygon into ``(zip, lat, lon, min_lon, min_lat, max_lon, max_lat)``."""
    from pyogrio.raw import read as ogr_read
    from shapely import from_wkb

    # Read the shapefile straight out of the zip via GDAL's /vsizip/ layer.
    src = f"/vsizip/{zcta_zip}/{ZIP_ZCTA_SHP_NAME}"
    meta, _, geoms, col_arrays = ogr_read(src, read_geometry=True)
    field_names = [str(f) for f in meta["fields"]]
    if ZIP_ZCTA_CODE_FIELD not in field_names:
        raise KeyError(f"ZCTA layer missing column {ZIP_ZCTA_CODE_FIELD!r}")
    code_vals = col_arrays[field_names.index(ZIP_ZCTA_CODE_FIELD)]

    rows: list[tuple[str, float, float, float, float, float, float]] = []
    seen: set[str] = set()
    for i, wkb in enumerate(geoms):
        if wkb is None:
            continue
        code = str(code_vals[i]).strip()[:5]
        if len(code) != 5 or not code.isdigit() or code in seen:
            continue
        geom = from_wkb(wkb)
        if geom.is_empty:
            continue
        w, s, e, n = geom.bounds
        c = geom.centroid  # fly-to target (matches the historical lat/lon)
        seen.add(code)
        rows.append((
            code,
            round(c.y, 6), round(c.x, 6),
            round(w, 6), round(s, 6), round(e, 6), round(n, 6),
        ))
    rows.sort()
    return rows


def _write_zip_csv(rows: list[tuple]) -> None:
    ZIP_DIR.mkdir(parents=True, exist_ok=True)
    with open(ZIP_LOOKUP_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(ZIP_CSV_HEADER)
        writer.writerows(rows)


def load_zip(*, zcta_zip: Path | None = None, force: bool = False) -> int:
    """Build the ZIP centroid/bounds CSV from the Census ZCTA boundary shapefile."""
    if ZIP_LOOKUP_CSV.is_file() and not force:
        print(f"==> using cached {_rel(ZIP_LOOKUP_CSV)}")
        return 0
    try:
        import pyogrio  # noqa: F401
        from shapely import from_wkb  # noqa: F401
    except ImportError as exc:
        print("ERROR: pyogrio and shapely required for --zip", file=sys.stderr)
        raise SystemExit(1) from exc

    src_zip = _resolve_zcta_zip(zcta_zip)
    rows = _read_zcta_rows(src_zip)
    if not rows:
        print("ERROR: no ZCTA rows decoded from shapefile", file=sys.stderr)
        return 1
    _write_zip_csv(rows)
    print(f"==> wrote {len(rows):,} ZIP rows → {_rel(ZIP_LOOKUP_CSV)}")
    return 0


# ── Admin (us-atlas) TopoJSON ───────────────────────────────────────

def _fetch_admin(url: str, dest: Path, *, force: bool) -> None:
    if not force and dest.is_file() and dest.stat().st_size > 1000:
        print(f"==> using cached {_rel(dest)}")
        return
    _download(url, dest, user_agent="ModelAccuracy load_boundaries (admin)", timeout=120)


def load_admin(*, states_url: str = "", counties_url: str = "", force: bool = False) -> int:
    """Download us-atlas state/county TopoJSON into ``boundary_data/admin/``."""
    states_url = states_url or f"{US_ATLAS_CDN}/states-10m.json"
    counties_url = counties_url or f"{US_ATLAS_CDN}/counties-10m.json"
    _fetch_admin(states_url, ADMIN_STATES_TOPOJSON, force=force)
    _fetch_admin(counties_url, ADMIN_COUNTIES_TOPOJSON, force=force)
    return 0


# ── WBD HU layers ───────────────────────────────────────────────────

def _fetch_gdb_zip(dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size > 1_000_000:
        print(f"==> using cached {_rel(dest)}")
        return
    _download(WBD_SCIENCEBASE_URL, dest, user_agent="ModelAccuracy load_boundaries (wbd)")


def _extract_gdb_zip(zip_path: Path, gdb_dir: Path) -> Path:
    if gdb_dir.is_dir() and any(gdb_dir.iterdir()):
        print(f"==> using extracted GDB at {_rel(gdb_dir)}")
        return gdb_dir
    print("==> extracting GDB zip")
    with tempfile.TemporaryDirectory(prefix="wbd_gdb_") as tmp:
        tmp_path = Path(tmp)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_path)
        gdb_dirs = list(tmp_path.rglob("*.gdb"))
        if not gdb_dirs:
            raise RuntimeError("no .gdb directory found in WBD zip")
        if gdb_dir.exists():
            shutil.rmtree(gdb_dir)
        shutil.copytree(gdb_dirs[0], gdb_dir)
    print(f"  extracted to {_rel(gdb_dir)}")
    return gdb_dir


def _read_layer_units(gdb_path: Path, level: int) -> list[tuple[str, str, object]]:
    from pyogrio.raw import read as ogr_read
    from shapely import from_wkb

    layer = layer_name(level)
    huc_col = huc_field(level)
    meta, _, geoms, col_arrays = ogr_read(str(gdb_path), layer=layer, read_geometry=True)
    field_names = [str(f) for f in meta["fields"]]
    if huc_col not in field_names:
        raise KeyError(f"{layer}: missing column {huc_col!r}")
    huc_idx = field_names.index(huc_col)
    name_idx = field_names.index("name") if "name" in field_names else None
    huc_vals = col_arrays[huc_idx]
    name_vals = col_arrays[name_idx] if name_idx is not None else None
    rows: list[tuple[str, str, object]] = []
    for i, wkb in enumerate(geoms):
        if wkb is None:
            continue
        huc = normalize_huc(huc_vals[i], level)
        if not huc:
            continue
        name = str(name_vals[i] if name_vals is not None else "").strip()
        rows.append((huc, name, from_wkb(wkb)))
    return rows


def _write_layer(path: Path, level: int, units: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"level": level, "units": units}, separators=(",", ":")))


def _write_layers_manifest(levels: list[int]) -> None:
    """Record the levels a completed decode produced (written last, as a completion marker)."""
    WBD_LAYERS_MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    WBD_LAYERS_MANIFEST.write_text(json.dumps({"levels": sorted({int(lvl) for lvl in levels})}))


def _layers_complete() -> bool:
    """True only if the manifest exists and every level it lists still has its layer file."""
    if not WBD_LAYERS_MANIFEST.is_file():
        return False
    try:
        levels = json.loads(WBD_LAYERS_MANIFEST.read_text()).get("levels") or []
    except (json.JSONDecodeError, OSError):
        return False
    if not levels or 2 not in levels:
        return False
    return all(layer_cache_path(WBD_LAYERS_DIR, int(lvl)).is_file() for lvl in levels)


def _present_levels() -> list[int]:
    """HU levels with a layer file currently on disk."""
    return [lvl for lvl in HU_LEVELS if layer_cache_path(WBD_LAYERS_DIR, lvl).is_file()]


def _drop_layers_above(cap: int) -> list[int]:
    """Delete layer files deeper than *cap* so the store matches the requested depth."""
    removed: list[int] = []
    for lvl in HU_LEVELS:
        if lvl > cap:
            path = layer_cache_path(WBD_LAYERS_DIR, lvl)
            if path.is_file():
                path.unlink()
                removed.append(lvl)
    return removed


def _normalize_gdb(gdb_path: Path, layers_dir: Path, branch: str, max_level: int) -> list[int]:
    from shapely import make_valid
    from shapely.validation import explain_validity

    layers_dir.mkdir(parents=True, exist_ok=True)
    available_levels: list[int] = []
    for level in HU_LEVELS:
        if level > max_level:
            break  # HU_LEVELS is ascending — don't read deeper WBDHU layers
        layer = layer_name(level)
        try:
            rows = _read_layer_units(gdb_path, level)
        except Exception as exc:
            print(f"  skip {layer}: {exc}")
            continue
        if not rows:
            print(f"  skip {layer}: empty")
            continue
        units: dict[str, dict] = {}
        count = 0
        for huc, name, geom in rows:
            if len(huc) != level:
                continue
            if branch and level > 2 and not in_branch(huc, branch):
                continue
            if geom is None or geom.is_empty:
                continue
            if not geom.is_valid:
                fixed = make_valid(geom)
                if fixed.is_empty:
                    print(f"  WARN: invalid geom for {huc}: {explain_validity(geom)}")
                    continue
                geom = fixed
            simplified = geom.simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
            if simplified.is_empty:
                simplified = geom
            polygons = geom_to_polygons(simplified)
            if not polygons:
                continue
            units[huc] = {
                "name": name,
                "bbox": polygons_bbox(polygons),
                "polygons": polygons,
            }
            count += 1
        if count:
            out_path = layer_cache_path(layers_dir, level)
            _write_layer(out_path, level, units)
            available_levels.append(level)
            print(f"==> {layer}: {count} units → {_rel(out_path)}")
    return available_levels


def _resolve_gdb(gdb_zip: Path | None, gdb_path: Path | None) -> Path | None:
    """Return a decodable ``.gdb`` dir, preferring cached artifacts in ``boundary_data/cache/``.

    Cache-first resolution order — the ~2.7 GB zip is only downloaded when nothing
    usable is already on disk:

      1. explicit ``--gdb-path`` (an already-extracted GDB elsewhere)
      2. the extracted GDB in ``cache/`` (no zip needed)
      3. an explicit ``--gdb-zip`` (staged into ``cache/`` once, then extracted)
      4. the cached zip in ``cache/`` (extracted)
      5. download the National GDB zip from the USGS source, then extract
    """
    if gdb_path is not None:
        if not gdb_path.is_dir():
            print(f"ERROR: --gdb-path {gdb_path} is not a directory", file=sys.stderr)
            return None
        return gdb_path

    # Already-extracted GDB in cache/ → reuse it, no zip required.
    if WBD_GDB_DIR.is_dir() and any(WBD_GDB_DIR.iterdir()):
        print(f"==> using extracted GDB at {_rel(WBD_GDB_DIR)}")
        return WBD_GDB_DIR

    # An externally-supplied zip → stage it into cache/ once before extracting.
    if (
        gdb_zip is not None
        and gdb_zip.is_file()
        and gdb_zip.resolve() != WBD_GDB_ZIP.resolve()
        and not WBD_GDB_ZIP.is_file()
    ):
        WBD_CACHE.mkdir(parents=True, exist_ok=True)
        print(f"==> staging {_rel(gdb_zip)} → {_rel(WBD_GDB_ZIP)}")
        shutil.copy2(gdb_zip, WBD_GDB_ZIP)

    # Cached zip in cache/ → extract; otherwise download from the USGS source first.
    _fetch_gdb_zip(WBD_GDB_ZIP)
    return _extract_gdb_zip(WBD_GDB_ZIP, WBD_GDB_DIR)


def load_wbd(
    *,
    gdb_zip: Path | None = None,
    gdb_path: Path | None = None,
    branch: str = "",
    force: bool = False,
    max_level: int = DEFAULT_MAX_HUC_LEVEL,
) -> int:
    """Normalize WBD HU layers into ``boundary_data/wbd/layers/`` (fetches the GDB only if needed).

    ``max_level`` (one of ``HU_LEVELS``; default = ``wbd_core.DEFAULT_MAX_HUC_LEVEL``,
    the knob shared with the exporter) decodes only ``WBDHU2…WBDHU{max_level}`` and
    drops any previously-decoded deeper layers, so the store holds exactly the
    requested depth. Pass ``16`` for the full source. The download is still the
    full national GDB regardless.
    """
    try:
        import pyogrio  # noqa: F401
        from shapely import make_valid  # noqa: F401
    except ImportError as exc:
        print("ERROR: pyogrio and shapely required for --wbd", file=sys.stderr)
        raise SystemExit(1) from exc

    branch = (branch or "").strip()
    cap = int(max_level)

    # Drop layers deeper than the requested cap so the store matches it, then
    # refresh the manifest to reflect what remains on disk.
    removed = _drop_layers_above(cap)
    if removed:
        print(f"==> dropped layers above HU{cap}: {', '.join(f'HU{l}' for l in removed)}")
        _write_layers_manifest(_present_levels())

    # Idempotent: short-circuit only when the manifest shows every produced layer is present.
    if not force and _layers_complete():
        print(f"==> using cached layers under {_rel(WBD_LAYERS_DIR)}")
        return 0

    resolved_gdb = _resolve_gdb(gdb_zip, gdb_path)
    if resolved_gdb is None:
        return 1

    levels = _normalize_gdb(resolved_gdb, WBD_LAYERS_DIR, branch, cap)
    if 2 not in levels:
        print("ERROR: no HUC-2 units decoded", file=sys.stderr)
        return 1
    _write_layers_manifest(levels)
    return 0


# ── WBD synthetic sample (local dev) ────────────────────────────────

_SAMPLE_HUC2_BOXES: dict[str, tuple[str, list[float]]] = {
    "01": ("New England", [-73.5, 41.0, -66.5, 47.5]),
    "08": ("Lower Mississippi", [-95.5, 29.0, -88.0, 37.0]),
    "18": ("California", [-124.5, 32.5, -114.0, 42.5]),
}


def _box_polygon(bbox: list[float]) -> list[list[list[float]]]:
    w, s, e, n = bbox
    return [[[[w, s], [e, s], [e, n], [w, n], [w, s]]]]


def _subdivide(parent: str, parent_bbox: list[float]) -> dict[str, dict]:
    w, s, e, n = parent_bbox
    mid_x, mid_y = (w + e) / 2, (s + n) / 2
    quads = [
        (f"{parent}01", "NW", [w, mid_y, mid_x, n]),
        (f"{parent}02", "NE", [mid_x, mid_y, e, n]),
        (f"{parent}03", "SW", [w, s, mid_x, mid_y]),
        (f"{parent}04", "SE", [mid_x, s, e, mid_y]),
    ]
    return {
        huc: {"name": name, "bbox": bbox, "polygons": _box_polygon(bbox)}
        for huc, name, bbox in quads
    }


def load_wbd_sample() -> int:
    """Write synthetic WBD layers (no 2.7 GB download) for local dev."""
    huc2 = {
        h: {"name": n, "bbox": b, "polygons": _box_polygon(b)}
        for h, (n, b) in _SAMPLE_HUC2_BOXES.items()
    }
    _write_layer(layer_cache_path(WBD_LAYERS_DIR, 2), 2, huc2)
    branch = "08"
    hu4 = _subdivide(branch, _SAMPLE_HUC2_BOXES[branch][1])
    _write_layer(layer_cache_path(WBD_LAYERS_DIR, 4), 4, hu4)
    parent4 = f"{branch}01"
    hu6 = _subdivide(parent4, hu4[parent4]["bbox"])
    _write_layer(layer_cache_path(WBD_LAYERS_DIR, 6), 6, hu6)
    parent6 = f"{parent4}01"
    hu8 = _subdivide(parent6, hu6[parent6]["bbox"])
    _write_layer(layer_cache_path(WBD_LAYERS_DIR, 8), 8, hu8)
    _write_layers_manifest([2, 4, 6, 8])
    print(f"Wrote sample layers under {_rel(WBD_LAYERS_DIR)}")
    return 0


# ── Military installations (DISDI MIRTA) ───────────────────────────
#
# The DoD DISDI program publishes MIRTA — one public zip holding an ESRI file
# geodatabase with two layers: per-site points (FY{yy}MirtaLocations) and site
# boundary multipolygons (FY{yy}MirtaLocations_A), both EPSG:4326. Sites are
# keyed here by a slug of siteName + state (the source has no stable ID field);
# boundary polygons are aggregated per key and the matching point (fly-to
# target) is attached, falling back to the bbox center for polygon-only sites.
MILITARY_SIMPLIFY_TOLERANCE = 0.001  # ~100 m; sites are far smaller than HUCs
_MILITARY_COORD_DECIMALS = 5


def _slugify(*parts: str) -> str:
    joined = "-".join(p for p in parts if p)
    slug = re.sub(r"[^a-z0-9]+", "-", joined.lower()).strip("-")
    return slug or "site"


def _resolve_military_zip(mirta_zip: Path | None) -> Path:
    """Return the MIRTA zip, preferring the cached copy in ``cache/`` (same
    resolution order as :func:`_resolve_zcta_zip`)."""
    if (
        mirta_zip is not None
        and mirta_zip.is_file()
        and mirta_zip.resolve() != MILITARY_ZIP.resolve()
        and not MILITARY_ZIP.is_file()
    ):
        WBD_CACHE.mkdir(parents=True, exist_ok=True)
        print(f"==> staging {_rel(mirta_zip)} → {_rel(MILITARY_ZIP)}")
        shutil.copy2(mirta_zip, MILITARY_ZIP)

    if MILITARY_ZIP.is_file() and MILITARY_ZIP.stat().st_size > 100_000:
        print(f"==> using cached {_rel(MILITARY_ZIP)}")
        return MILITARY_ZIP

    _download(
        MILITARY_URL,
        MILITARY_ZIP,
        user_agent="ModelAccuracy load_boundaries (military)",
        extra_cafile=MILITARY_CA_PEM if MILITARY_CA_PEM.is_file() else None,
    )
    return MILITARY_ZIP


def _extract_military_gdb(zip_path: Path, gdb_dir: Path) -> Path:
    if gdb_dir.is_dir() and any(gdb_dir.iterdir()):
        print(f"==> using extracted GDB at {_rel(gdb_dir)}")
        return gdb_dir
    print("==> extracting MIRTA zip")
    with tempfile.TemporaryDirectory(prefix="mirta_gdb_") as tmp:
        tmp_path = Path(tmp)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_path)
        gdb_dirs = list(tmp_path.rglob("*.gdb"))
        if not gdb_dirs:
            raise RuntimeError("no .gdb directory found in MIRTA zip")
        if gdb_dir.exists():
            shutil.rmtree(gdb_dir)
        shutil.copytree(gdb_dirs[0], gdb_dir)
    print(f"  extracted to {_rel(gdb_dir)}")
    return gdb_dir


def _mirta_layer_names(gdb_path: Path) -> tuple[str, str]:
    """(point_layer, polygon_layer) — layer names embed the fiscal year, so
    discover them by geometry type instead of hard-coding FY24."""
    import pyogrio

    point_layer = polygon_layer = ""
    for name, gtype in pyogrio.list_layers(str(gdb_path)):
        gtype = str(gtype or "")
        if "Polygon" in gtype and not polygon_layer:
            polygon_layer = str(name)
        elif "Point" in gtype and not point_layer:
            point_layer = str(name)
    if not polygon_layer:
        raise RuntimeError(f"no polygon layer found in {gdb_path}")
    return point_layer, polygon_layer


def _read_mirta_rows(gdb_path: Path, layer: str) -> list[tuple[dict, object]]:
    """Rows of (attributes, shapely geometry) for a MIRTA layer."""
    from pyogrio.raw import read as ogr_read
    from shapely import from_wkb

    meta, _, geoms, col_arrays = ogr_read(str(gdb_path), layer=layer, read_geometry=True)
    field_names = [str(f) for f in meta["fields"]]
    cols = {name: col_arrays[i] for i, name in enumerate(field_names)}

    def val(name: str, i: int) -> str:
        arr = cols.get(name)
        return str(arr[i]).strip() if arr is not None and arr[i] is not None else ""

    rows: list[tuple[dict, object]] = []
    for i, wkb in enumerate(geoms):
        if wkb is None:
            continue
        attrs = {
            "name": val("siteName", i) or val("featureName", i),
            "component": val("siteReportingComponent", i),
            "status": val("siteOperationalStatus", i).lower(),
            "state": val("stateNameCode", i).lower(),
            "joint_base": val("isJointBase", i).lower() == "yes",
        }
        if not attrs["name"]:
            continue
        rows.append((attrs, from_wkb(wkb)))
    return rows


def _round_polygons(polygons: list[list[list[list[float]]]]) -> list[list[list[list[float]]]]:
    return [
        [[[round(x, _MILITARY_COORD_DECIMALS), round(y, _MILITARY_COORD_DECIMALS)] for x, y in ring]
         for ring in poly]
        for poly in polygons
    ]


def _normalize_mirta(gdb_path: Path) -> dict[str, dict]:
    from shapely import make_valid

    point_layer, polygon_layer = _mirta_layer_names(gdb_path)
    sites: dict[str, dict] = {}
    for attrs, geom in _read_mirta_rows(gdb_path, polygon_layer):
        if geom is None or geom.is_empty:
            continue
        if not geom.is_valid:
            geom = make_valid(geom)
            if geom.is_empty:
                continue
        simplified = geom.simplify(MILITARY_SIMPLIFY_TOLERANCE, preserve_topology=True)
        if simplified.is_empty:
            simplified = geom
        polygons = _round_polygons(geom_to_polygons(simplified))
        if not polygons:
            continue
        key = _slugify(attrs["name"], attrs["state"])
        entry = sites.get(key)
        if entry is None:
            sites[key] = {
                "name": attrs["name"],
                "bbox": polygons_bbox(polygons),
                "polygons": polygons,
                "component": attrs["component"],
                "status": attrs["status"],
                "state": attrs["state"],
                "joint_base": attrs["joint_base"],
            }
        else:
            # Same site split across several boundary features → union them.
            entry["polygons"].extend(polygons)
            entry["bbox"] = polygons_bbox(entry["polygons"])

    # Attach each site's published point (fly-to target); bbox center fallback.
    points: dict[str, list[float]] = {}
    if point_layer:
        for attrs, geom in _read_mirta_rows(gdb_path, point_layer):
            if geom is None or geom.is_empty or geom.geom_type != "Point":
                continue
            key = _slugify(attrs["name"], attrs["state"])
            points.setdefault(
                key,
                [round(float(geom.x), _MILITARY_COORD_DECIMALS),
                 round(float(geom.y), _MILITARY_COORD_DECIMALS)],
            )
    for key, entry in sites.items():
        w, s, e, n = entry["bbox"]
        entry["point"] = points.get(
            key,
            [round((w + e) / 2, _MILITARY_COORD_DECIMALS),
             round((s + n) / 2, _MILITARY_COORD_DECIMALS)],
        )
    return sites


def _write_military_sites(sites: dict[str, dict]) -> None:
    MILITARY_DIR.mkdir(parents=True, exist_ok=True)
    MILITARY_SITES_JSON.write_text(
        json.dumps(
            {"version": 1, "source": MILITARY_SOURCE_LABEL, "sites": sites},
            separators=(",", ":"),
        )
    )


def load_military(*, mirta_zip: Path | None = None, force: bool = False) -> int:
    """Normalize DISDI MIRTA installation sites into ``boundary_data/military/sites.json``."""
    if MILITARY_SITES_JSON.is_file() and not force:
        print(f"==> using cached {_rel(MILITARY_SITES_JSON)}")
        return 0
    try:
        import pyogrio  # noqa: F401
        from shapely import make_valid  # noqa: F401
    except ImportError as exc:
        print("ERROR: pyogrio and shapely required for --military", file=sys.stderr)
        raise SystemExit(1) from exc

    src_zip = _resolve_military_zip(mirta_zip)
    gdb_dir = _extract_military_gdb(src_zip, MILITARY_GDB_DIR)
    sites = _normalize_mirta(gdb_dir)
    if not sites:
        print("ERROR: no MIRTA sites decoded", file=sys.stderr)
        return 1
    _write_military_sites(sites)
    print(f"==> wrote {len(sites):,} military sites → {_rel(MILITARY_SITES_JSON)}")
    return 0


_SAMPLE_MILITARY_BOXES: dict[str, tuple[str, str, str, list[float]]] = {
    # key → (name, component, state, bbox)
    "sample-army-base-ks": ("Sample Army Base", "usa", "ks", [-97.0, 38.7, -96.6, 39.1]),
    "sample-air-station-ca": ("Sample Air Station", "usaf", "ca", [-120.7, 34.6, -120.5, 34.8]),
    "sample-naval-range-fl": ("Sample Naval Range", "usn", "fl", [-86.9, 30.4, -86.4, 30.7]),
}


def load_military_sample() -> int:
    """Write synthetic military sites (no download) for local dev."""
    sites = {}
    for key, (name, component, state, bbox) in _SAMPLE_MILITARY_BOXES.items():
        w, s, e, n = bbox
        sites[key] = {
            "name": name,
            "bbox": bbox,
            "polygons": _box_polygon(bbox),
            "component": component,
            "status": "act",
            "state": state,
            "joint_base": False,
            "point": [round((w + e) / 2, 5), round((s + n) / 2, 5)],
        }
    _write_military_sites(sites)
    print(f"Wrote {len(sites)} sample military sites → {_rel(MILITARY_SITES_JSON)}")
    return 0


# ── CLI ─────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--zip", action="store_true", help="Load the ZIP lookup CSV")
    parser.add_argument("--admin", action="store_true", help="Load us-atlas state/county TopoJSON")
    parser.add_argument("--wbd", action="store_true", help="Load WBD HU layers (downloads ~2.7 GB GDB)")
    parser.add_argument(
        "--wbd-sample", action="store_true",
        help="Write synthetic WBD layers for local dev (no download)",
    )
    parser.add_argument(
        "--military", action="store_true",
        help="Load DISDI MIRTA military installation sites (~4 MB zip)",
    )
    parser.add_argument(
        "--military-sample", action="store_true",
        help="Write synthetic military sites for local dev (no download)",
    )
    parser.add_argument(
        "--mirta-zip", type=Path, default=None,
        help="Use an existing MIRTA installations_ranges.zip",
    )
    parser.add_argument("--force", action="store_true", help="Re-fetch/re-decode even if outputs exist")
    parser.add_argument("--zcta-zip", type=Path, default=None, help="Use an existing ZCTA boundary shapefile zip")
    parser.add_argument("--branch", default="", help="Restrict WBD layers to a HUC-2 branch prefix")
    parser.add_argument(
        "--max-huc-level", type=int, choices=HU_LEVELS, default=DEFAULT_MAX_HUC_LEVEL,
        help=f"Deepest WBD HU level to decode (default: {DEFAULT_MAX_HUC_LEVEL}; pass 16 for "
             "full source). Drops deeper layers if present.",
    )
    parser.add_argument("--gdb-zip", type=Path, default=None, help="Use an existing WBD GDB zip")
    parser.add_argument("--gdb-path", type=Path, default=None, help="Use an already-extracted .gdb dir")
    args = parser.parse_args()

    if args.wbd and args.wbd_sample:
        parser.error("use at most one of --wbd / --wbd-sample")
    if args.military and args.military_sample:
        parser.error("use at most one of --military / --military-sample")

    # No selection flag → load everything (real WBD/military, not the samples).
    select_all = not (
        args.zip or args.admin or args.wbd or args.wbd_sample
        or args.military or args.military_sample
    )

    rc = 0
    if args.zip or select_all:
        print("=== ZIP lookup ===")
        rc |= load_zip(zcta_zip=args.zcta_zip, force=args.force)
    if args.admin or select_all:
        print("=== Admin (us-atlas) TopoJSON ===")
        rc |= load_admin(force=args.force)
    if args.wbd_sample:
        print("=== WBD layers (synthetic sample) ===")
        rc |= load_wbd_sample()
    elif args.wbd or select_all:
        print("=== WBD HU layers ===")
        rc |= load_wbd(
            gdb_zip=args.gdb_zip,
            gdb_path=args.gdb_path,
            branch=args.branch,
            force=args.force,
            max_level=args.max_huc_level,
        )
    if args.military_sample:
        print("=== Military sites (synthetic sample) ===")
        rc |= load_military_sample()
    elif args.military or select_all:
        print("=== Military sites (DISDI MIRTA) ===")
        rc |= load_military(mirta_zip=args.mirta_zip, force=args.force)
    return rc


if __name__ == "__main__":
    sys.exit(main())
