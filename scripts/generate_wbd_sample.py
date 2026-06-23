#!/usr/bin/env python3
"""Emit minimal WBD sample artifacts for local dev / CI when the full GDB is unavailable.

Produces ``static_export/static/wbd/`` with national HUC-2 outlines and a drillable
branch under HUC ``08``.  Backend reads ``chunks/``; the frontend reads
``index.json`` + TopoJSON from ``/static/wbd/``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = _PROJECT_ROOT / "static_export" / "static" / "wbd"

HUC2_BOXES: dict[str, tuple[str, list[float]]] = {
    "01": ("New England", [-73.5, 41.0, -66.5, 47.5]),
    "02": ("Mid-Atlantic", [-80.5, 37.5, -73.5, 42.5]),
    "03": ("South Atlantic-Gulf", [-84.5, 24.5, -75.0, 37.5]),
    "04": ("Great Lakes", [-92.5, 41.0, -76.0, 49.5]),
    "05": ("Ohio", [-89.0, 36.5, -79.0, 42.5]),
    "06": ("Tennessee", [-90.5, 34.5, -81.0, 37.5]),
    "07": ("Upper Mississippi", [-97.5, 36.0, -86.0, 49.5]),
    "08": ("Lower Mississippi", [-95.5, 29.0, -88.0, 37.0]),
    "09": ("Souris-Red-Rainy", [-104.5, 45.5, -89.5, 49.5]),
    "10": ("Missouri", [-104.5, 37.0, -89.5, 49.5]),
    "11": ("Arkansas-White-Red", [-104.5, 33.0, -89.5, 38.5]),
    "12": ("Texas-Gulf", [-106.5, 25.5, -93.5, 37.0]),
    "13": ("Rio Grande", [-109.0, 25.5, -101.0, 37.0]),
    "14": ("Upper Colorado", [-114.0, 36.5, -102.0, 43.5]),
    "15": ("Lower Colorado", [-118.0, 31.0, -109.0, 42.0]),
    "16": ("Great Basin", [-120.0, 35.0, -109.0, 45.5]),
    "17": ("Pacific Northwest", [-125.0, 42.0, -110.0, 49.5]),
    "18": ("California", [-124.5, 32.5, -114.0, 42.5]),
}


def box_polygon(bbox: list[float]) -> list[list[list[float]]]:
    w, s, e, n = bbox
    ring = [[w, s], [e, s], [e, n], [w, n], [w, s]]
    return [[ring]]


def rect_feature(huc: str, name: str, level: int, bbox: list[float]) -> dict:
    polys = box_polygon(bbox)
    return {
        "type": "Feature",
        "id": huc,
        "properties": {"huc": huc, "name": name, "level": level},
        "geometry": {"type": "Polygon", "coordinates": polys[0]},
    }


def subdivide(parent_huc: str, parent_bbox: list[float]) -> dict[str, dict]:
    w, s, e, n = parent_bbox
    mid_x = (w + e) / 2
    mid_y = (s + n) / 2
    quads = [
        (f"{parent_huc}01", "Northwest", [w, mid_y, mid_x, n]),
        (f"{parent_huc}02", "Northeast", [mid_x, mid_y, e, n]),
        (f"{parent_huc}03", "Southwest", [w, s, mid_x, mid_y]),
        (f"{parent_huc}04", "Southeast", [mid_x, s, e, mid_y]),
    ]
    units: dict[str, dict] = {}
    for huc, name, bbox in quads:
        polys = box_polygon(bbox)
        units[huc] = {"name": f"{name} ({name.split()[0]})", "bbox": bbox, "polygons": polys}
    return units


def write_chunk(path: Path, parent: str, level: int, units: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"parent": parent, "level": level, "units": units}, separators=(",", ":"))
    )


def main() -> int:
    out = DEFAULT_OUT
    out.mkdir(parents=True, exist_ok=True)

    index_units: dict[str, dict] = {}
    huc2_units: dict[str, dict] = {}

    for huc, (name, bbox) in HUC2_BOXES.items():
        polys = box_polygon(bbox)
        huc2_units[huc] = {"name": name, "bbox": bbox, "polygons": polys}
        index_units[huc] = {"name": name, "level": 2, "hasChildren": huc == "08"}

    write_chunk(out / "chunks" / "2" / "_root.json", "", 2, huc2_units)

    huc2_features = [
        rect_feature(h, name, 2, bbox) for h, (name, bbox) in HUC2_BOXES.items()
    ]
    (out / "huc2.topojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": huc2_features}, separators=(",", ":"))
    )

    branch = "08"
    _, bbox08 = HUC2_BOXES[branch]
    hu4 = subdivide(branch, bbox08)
    for huc, entry in hu4.items():
        index_units[huc] = {"name": entry["name"], "level": 4, "hasChildren": huc == f"{branch}01"}
    write_chunk(out / "chunks" / "2" / f"{branch}.json", branch, 4, hu4)
    (out / "children" / "4").mkdir(parents=True, exist_ok=True)
    (out / "children" / "4" / f"{branch}.topojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [rect_feature(h, u["name"], 4, u["bbox"]) for h, u in hu4.items()],
            },
            separators=(",", ":"),
        )
    )

    parent4 = f"{branch}01"
    hu6 = subdivide(parent4, hu4[parent4]["bbox"])
    for huc, entry in hu6.items():
        index_units[huc] = {"name": entry["name"], "level": 6, "hasChildren": huc == f"{parent4}01"}
    write_chunk(out / "chunks" / "4" / f"{parent4}.json", parent4, 6, hu6)
    (out / "children" / "6").mkdir(parents=True, exist_ok=True)
    (out / "children" / "6" / f"{parent4}.topojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [rect_feature(h, u["name"], 6, u["bbox"]) for h, u in hu6.items()],
            },
            separators=(",", ":"),
        )
    )

    parent6 = f"{parent4}01"
    hu8 = subdivide(parent6, hu6[parent6]["bbox"])
    for huc, entry in hu8.items():
        index_units[huc] = {"name": entry["name"], "level": 8, "hasChildren": False}
    write_chunk(out / "chunks" / "6" / f"{parent6}.json", parent6, 8, hu8)
    (out / "children" / "8").mkdir(parents=True, exist_ok=True)
    (out / "children" / "8" / f"{parent6}.topojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [rect_feature(h, u["name"], 8, u["bbox"]) for h, u in hu8.items()],
            },
            separators=(",", ":"),
        )
    )

    (out / "index.json").write_text(
        json.dumps(
            {
                "version": 1,
                "source": "generate_wbd_sample.py (synthetic boxes — not authoritative WBD)",
                "levels": [2, 4, 6, 8],
                "units": index_units,
            },
            separators=(",", ":"),
        )
    )

    print(f"Wrote sample WBD artifacts under {out.relative_to(_PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
