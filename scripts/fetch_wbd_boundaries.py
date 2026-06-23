#!/usr/bin/env python3
"""Download WBD National GDB and emit watershed boundary artifacts.

Run when WBD boundaries change (yearly at most).  Outputs land in
``static_export/static/wbd/``:

* ``chunks/{level}/{parent}.json`` — backend polygon lookup (baked into Docker)
* ``huc2.topojson`` / ``children/{level}/{parent}.topojson`` — frontend map (S3 only)
* ``index.json`` — frontend drill metadata with dynamic ``hasChildren`` (S3 only)

Source: USGS Watershed Boundary Dataset (WBD) National GDB via ScienceBase.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = _PROJECT_ROOT / "static_export" / "static" / "wbd"
DEFAULT_GDB_CACHE = _PROJECT_ROOT / ".cache" / "WBD_National_GDB.zip"

SCIENCEBASE_URL = (
    "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/"
    "WBD_National_GDB.zip"
)
SOURCE_LABEL = "USGS WBD National GDB (TNM StagedProducts/Hydrography/WBD/National/GDB)"

HU_LEVELS = (2, 4, 6, 8, 10, 12, 14, 16)
SIMPLIFY_TOLERANCE = 0.005  # degrees — sufficient for grid-cell averaging


def fetch(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size > 1_000_000:
        print(f"==> using cached {dest.relative_to(_PROJECT_ROOT)}")
        return
    print(f"==> downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "ModelAccuracy fetch_wbd_boundaries"})
    with urllib.request.urlopen(req, timeout=600) as resp:
        data = resp.read()
    dest.write_bytes(data)
    print(f"  wrote {dest.relative_to(_PROJECT_ROOT)} ({dest.stat().st_size:,} bytes)")


def _layer_name(level: int) -> str:
    return f"WBDHU{level}"


def _huc_field(level: int) -> str:
    return f"huc{level}"


def _read_layer_units(gdb_path: Path, level: int) -> list[tuple[str, str, object]]:
    """Return [(huc, name, shapely_geom), ...] for one WBD HU layer."""
    from pyogrio.raw import read as ogr_read
    from shapely import from_wkb

    layer = _layer_name(level)
    huc_col = _huc_field(level)
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
        huc = _normalize_huc(huc_vals[i], level)
        if not huc:
            continue
        name = str(name_vals[i] if name_vals is not None else "").strip()
        rows.append((huc, name, from_wkb(wkb)))
    return rows


def _normalize_huc(raw, level: int) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    # WBD stores HUC codes as strings or ints; pad to level width.
    if s.isdigit():
        return s.zfill(level)
    # Some layers use dotted notation — strip non-digits.
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:level].zfill(level) if digits else ""


def _ring_coords(coords) -> list[list[float]]:
    return [[float(x), float(y)] for x, y in coords]


def _geom_to_polygons(geom) -> list[list[list[list[float]]]]:
    """MultiPolygon-style nesting: ``[[outer, hole?, ...], ...]``."""
    if geom is None or geom.is_empty:
        return []
    gtype = geom.geom_type
    if gtype == "Polygon":
        outer = _ring_coords(list(geom.exterior.coords))
        holes = [_ring_coords(list(r.coords)) for r in (geom.interiors or [])]
        return [[outer, *holes]] if outer else []
    if gtype == "MultiPolygon":
        out: list[list[list[list[float]]]] = []
        for poly in geom.geoms:
            part = _geom_to_polygons(poly)
            out.extend(part)
        return out
    return []


def polygons_bbox(polygons: list[list[list[list[float]]]]) -> list[float]:
    west = float("inf")
    east = float("-inf")
    south = float("inf")
    north = float("-inf")
    for poly in polygons:
        if not poly:
            continue
        for x, y in poly[0]:
            west = min(west, x)
            east = max(east, x)
            if y < south:
                south = y
            if y > north:
                north = y
    return [west, south, east, north]


def _features_to_topojson(features: list[dict], object_name: str) -> dict:
    from topojson import Topology

    geojson = {"type": "FeatureCollection", "features": features}
    topo = Topology(geojson, prequantize=False, presimplify=False)
    obj = topo.to_dict()
    # Keep only the requested object collection.
    if object_name in obj.get("objects", {}):
        keep = obj["objects"][object_name]
        return {
            "type": "Topology",
            "transform": obj.get("transform"),
            "bbox": obj.get("bbox"),
            "arcs": obj["arcs"],
            "objects": {object_name: keep},
        }
    return obj


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, separators=(",", ":")))


def _parent_huc(huc: str) -> str:
    return huc[:-2] if len(huc) > 2 else ""


def _in_branch(huc: str, branch: str) -> bool:
    return huc == branch or huc.startswith(branch)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--gdb-zip", type=Path, default=DEFAULT_GDB_CACHE)
    parser.add_argument(
        "--gdb-path",
        type=Path,
        default=None,
        help="Use an already-extracted .gdb directory (skips zip download/extract).",
    )
    parser.add_argument(
        "--branch",
        default="",
        help="Only emit artifacts for one HUC branch (e.g. 08). Empty = full national build.",
    )
    args = parser.parse_args()
    branch = args.branch.strip()

    try:
        import pyogrio  # noqa: F401
        from shapely import make_valid  # noqa: F401
    except ImportError as exc:
        print(
            "ERROR: pyogrio and shapely are required — pip install pyogrio shapely topojson",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    if args.gdb_path is None:
        fetch(SCIENCEBASE_URL, args.gdb_zip)

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.gdb_path is not None:
        gdb_path = args.gdb_path
        if not gdb_path.is_dir():
            print(f"ERROR: --gdb-path {gdb_path} is not a directory", file=sys.stderr)
            return 1
        print(f"==> using existing GDB at {gdb_path}")
        return _process_gdb(gdb_path, out_dir, branch)

    with tempfile.TemporaryDirectory(prefix="wbd_gdb_") as tmp:
        tmp_path = Path(tmp)
        print("==> extracting GDB zip")
        with zipfile.ZipFile(args.gdb_zip) as zf:
            zf.extractall(tmp_path)
        gdb_dirs = list(tmp_path.rglob("*.gdb"))
        if not gdb_dirs:
            print("ERROR: no .gdb directory found in WBD zip", file=sys.stderr)
            return 1
        gdb_path = gdb_dirs[0]
        print(f"  using {gdb_path.name}")
        return _process_gdb(gdb_path, out_dir, branch)


def _process_gdb(gdb_path: Path, out_dir: Path, branch: str) -> int:
    from shapely import make_valid
    from shapely.validation import explain_validity

    units_by_level: dict[int, dict[str, dict]] = {lv: {} for lv in HU_LEVELS}
    available_levels: list[int] = []

    for level in HU_LEVELS:
        layer = _layer_name(level)
        try:
            rows = _read_layer_units(gdb_path, level)
        except Exception as exc:
            print(f"  skip {layer}: {exc}")
            continue
        if not rows:
            print(f"  skip {layer}: empty")
            continue

        count = 0
        for huc, name, geom in rows:
            if len(huc) != level:
                continue
            if branch and level > 2 and not _in_branch(huc, branch):
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
            polygons = _geom_to_polygons(simplified)
            if not polygons:
                continue
            units_by_level[level][huc] = {
                "name": name,
                "bbox": polygons_bbox(polygons),
                "polygons": polygons,
            }
            count += 1
        if count:
            available_levels.append(level)
            print(f"==> {layer}: {count} units")

    if not units_by_level[2]:
        print("ERROR: no HUC-2 units decoded", file=sys.stderr)
        return 1

    index: dict[str, dict] = {}
    child_levels = sorted(lv for lv in available_levels if lv > 2)
    for level in available_levels:
        for huc, entry in units_by_level[level].items():
            next_level = level + 2
            has_children = False
            if next_level in available_levels:
                child_bucket = units_by_level[next_level]
                has_children = any(_parent_huc(ch) == huc for ch in child_bucket)
            index[huc] = {
                "name": entry["name"],
                "level": level,
                "hasChildren": has_children,
            }

    huc2_features = []
    for huc, entry in sorted(units_by_level[2].items()):
        polys = entry["polygons"]
        if not polys:
            continue
        if len(polys) == 1:
            geom = {"type": "Polygon", "coordinates": polys[0]}
        else:
            geom = {"type": "MultiPolygon", "coordinates": polys}
        huc2_features.append(
            {
                "type": "Feature",
                "id": huc,
                "properties": {"huc": huc, "name": entry["name"], "level": 2},
                "geometry": geom,
            }
        )
    huc2_topo = _features_to_topojson(huc2_features, "huc2")
    huc2_path = out_dir / "huc2.topojson"
    huc2_path.write_text(json.dumps(huc2_topo, separators=(",", ":")))

    chunks_dir = out_dir / "chunks"
    children_dir = out_dir / "children"
    for level in child_levels:
        parent_level = level - 2
        by_parent: dict[str, dict[str, dict]] = defaultdict(dict)
        for huc, entry in units_by_level[level].items():
            parent = _parent_huc(huc)
            if not parent:
                continue
            by_parent[parent][huc] = {
                "name": entry["name"],
                "bbox": entry["bbox"],
                "polygons": entry["polygons"],
            }

        for parent, units in sorted(by_parent.items()):
            if branch and not _in_branch(parent, branch) and parent != branch:
                if not any(_in_branch(u, branch) for u in units):
                    continue

            _write_json(
                chunks_dir / str(parent_level) / f"{parent}.json",
                {"parent": parent, "level": level, "units": units},
            )

            child_features = []
            for huc, entry in sorted(units.items()):
                polys = entry["polygons"]
                if len(polys) == 1:
                    geom = {"type": "Polygon", "coordinates": polys[0]}
                else:
                    geom = {"type": "MultiPolygon", "coordinates": polys}
                child_features.append(
                    {
                        "type": "Feature",
                        "id": huc,
                        "properties": {
                            "huc": huc,
                            "name": entry["name"],
                            "level": level,
                        },
                        "geometry": geom,
                    }
                )
            obj_name = f"huc{level}"
            child_topo = _features_to_topojson(child_features, obj_name)
            child_path = children_dir / str(level) / f"{parent}.topojson"
            child_path.parent.mkdir(parents=True, exist_ok=True)
            child_path.write_text(json.dumps(child_topo, separators=(",", ":")))

    _write_json(
        chunks_dir / "2" / "_root.json",
        {
            "parent": "",
            "level": 2,
            "units": {
                huc: {
                    "name": e["name"],
                    "bbox": e["bbox"],
                    "polygons": e["polygons"],
                }
                for huc, e in units_by_level[2].items()
            },
        },
    )

    index_path = out_dir / "index.json"
    index_path.write_text(
        json.dumps(
            {
                "version": 1,
                "source": SOURCE_LABEL,
                "levels": available_levels,
                "units": index,
            },
            separators=(",", ":"),
        )
    )

    for p in (index_path, huc2_path):
        print(f"  wrote {p.relative_to(_PROJECT_ROOT)} ({p.stat().st_size:,} bytes)")
    n_chunks = sum(1 for _ in chunks_dir.rglob("*.json"))
    print(f"  wrote {n_chunks} chunk files under {chunks_dir.relative_to(_PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
