"""Shared WBD constants and geometry helpers for loader scripts."""

from __future__ import annotations

from pathlib import Path

HU_LEVELS = (2, 4, 6, 8, 10, 12, 14, 16)
SIMPLIFY_TOLERANCE = 0.005

# Single source of truth for the default WBD depth, shared by both the loader
# (`load_boundaries.py --wbd`, what lands in boundary_data/) and the exporter
# (`export_static.py`, the served chunks/map tree). Change this one value to move
# the default depth for both; either side can still override per-run with
# --max-huc-level. Must be one of HU_LEVELS.
DEFAULT_MAX_HUC_LEVEL = 12


def layer_name(level: int) -> str:
    return f"WBDHU{level}"


def huc_field(level: int) -> str:
    return f"huc{level}"


def layer_cache_path(layers_dir: Path, level: int) -> Path:
    return layers_dir / f"hu{level}.json"


def normalize_huc(raw, level: int) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if s.isdigit():
        return s.zfill(level)
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:level].zfill(level) if digits else ""


def geom_to_polygons(geom) -> list[list[list[list[float]]]]:
    if geom is None or geom.is_empty:
        return []
    gtype = geom.geom_type
    if gtype == "Polygon":
        outer = [[float(x), float(y)] for x, y in geom.exterior.coords]
        holes = [[[float(x), float(y)] for x, y in r.coords] for r in (geom.interiors or [])]
        return [[outer, *holes]] if outer else []
    if gtype == "MultiPolygon":
        out: list[list[list[list[float]]]] = []
        for poly in geom.geoms:
            out.extend(geom_to_polygons(poly))
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


def in_branch(huc: str, branch: str) -> bool:
    return huc == branch or huc.startswith(branch)


def parent_huc(huc: str) -> str:
    return huc[:-2] if len(huc) > 2 else ""
