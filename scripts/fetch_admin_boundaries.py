#!/usr/bin/env python3
"""Download us-atlas TopoJSON and emit admin-boundary artifacts.

Run once when boundaries change (yearly at most).  Outputs land in
``static_export/static/admin/`` so that:

* The backend reads ``boundaries.json`` from local disk (baked into the Docker
  image via ``COPY static_export``) to resolve FIPS → polygon rings for masks.
* The frontend fetches ``states.topojson`` / ``counties.topojson`` from
  ``/static/admin/*``.  TopoJSON is ~4–6× smaller than the equivalent
  GeoJSON because shared borders are stored once.  The frontend decodes
  with ``topojson-client`` (~3 KB) before handing to MapLibre.

Source: https://github.com/topojson/us-atlas (ISC), which is itself derived
from the US Census Bureau's cartographic boundary shapefiles, 2017 edition.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

CDN = "https://cdn.jsdelivr.net/npm/us-atlas@3"
SOURCE_LABEL = "us-atlas@3 (US Census Cartographic Boundaries, 2017)"

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = _PROJECT_ROOT / "static_export" / "static" / "admin"


def fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "ModelAccuracy fetch_admin_boundaries"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def decode_arcs(topology: dict) -> list[list[list[float]]]:
    """Apply the quantization transform to every arc; return [[x,y], ...] per arc."""
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


def stitch_ring(arc_indices: list[int], arcs: list[list[list[float]]]) -> list[list[float]]:
    """Concatenate arcs into one ring; negative ``i`` means arc ``~i`` reversed."""
    ring: list[list[float]] = []
    for n, idx in enumerate(arc_indices):
        if idx < 0:
            arc = list(reversed(arcs[~idx]))
        else:
            arc = arcs[idx]
        if n == 0:
            ring.extend(arc)
        else:
            # First point of subsequent arc duplicates the previous arc's last point.
            ring.extend(arc[1:])
    return ring


def geometry_to_polygons(geom: dict, arcs: list[list[list[float]]]) -> list[list[list[list[float]]]]:
    """Return MultiPolygon-style nesting: ``[[outer_ring, hole_ring?, ...], ...]``."""
    gtype = geom.get("type")
    if gtype == "Polygon":
        return [[stitch_ring(ring_arcs, arcs) for ring_arcs in geom["arcs"]]]
    if gtype == "MultiPolygon":
        return [
            [stitch_ring(ring_arcs, arcs) for ring_arcs in poly_arcs]
            for poly_arcs in geom["arcs"]
        ]
    return []


def polygons_bbox(polygons: list[list[list[list[float]]]]) -> list[float]:
    """[west, south, east, north] from outer rings."""
    west = float("inf")
    east = float("-inf")
    south = float("inf")
    north = float("-inf")
    for poly in polygons:
        if not poly:
            continue
        for x, y in poly[0]:
            if x < west:
                west = x
            if x > east:
                east = x
            if y < south:
                south = y
            if y > north:
                north = y
    return [west, south, east, north]


def build_boundaries(topology: dict, object_name: str) -> dict[str, dict]:
    """Decode topology arcs into the backend's flat FIPS → {name, bbox, polygons} map."""
    arcs = decode_arcs(topology)
    collection = topology["objects"][object_name]
    boundaries: dict[str, dict] = {}
    for geom in collection.get("geometries", []):
        fips = str(geom.get("id") or "").strip()
        if not fips:
            continue
        name = (geom.get("properties") or {}).get("name") or ""
        polygons = geometry_to_polygons(geom, arcs)
        if not polygons:
            continue
        bbox = polygons_bbox(polygons)
        boundaries[fips] = {"name": name, "bbox": bbox, "polygons": polygons}
    return boundaries


def slim_topology(topology: dict, object_name: str) -> dict:
    """Strip everything except the requested geometry collection.

    us-atlas's ``counties-10m.json`` ships ``counties``, ``states``, and ``nation``
    in one file.  Frontend only needs the one we ask for, so drop the rest to
    keep the payload focused (and avoid duplicate state polygons in the counties
    file).
    """
    keep = topology["objects"][object_name]
    return {
        "type": "Topology",
        "transform": topology.get("transform"),
        "bbox": topology.get("bbox"),
        "arcs": topology["arcs"],
        "objects": {object_name: keep},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--counties-url",
        default=f"{CDN}/counties-10m.json",
    )
    parser.add_argument(
        "--states-url",
        default=f"{CDN}/states-10m.json",
    )
    args = parser.parse_args()

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"==> downloading {args.states_url}")
    states_topo = json.loads(fetch(args.states_url))
    print(f"==> downloading {args.counties_url}")
    counties_topo = json.loads(fetch(args.counties_url))

    states_b = build_boundaries(states_topo, "states")
    counties_b = build_boundaries(counties_topo, "counties")
    print(f"==> decoded {len(states_b)} states, {len(counties_b)} counties")

    boundaries = {
        "version": 1,
        "source": SOURCE_LABEL,
        "states": states_b,
        "counties": counties_b,
    }
    boundaries_path = out_dir / "boundaries.json"
    states_topo_path = out_dir / "states.topojson"
    counties_topo_path = out_dir / "counties.topojson"

    boundaries_path.write_text(json.dumps(boundaries, separators=(",", ":")))
    states_topo_path.write_text(
        json.dumps(slim_topology(states_topo, "states"), separators=(",", ":"))
    )
    counties_topo_path.write_text(
        json.dumps(slim_topology(counties_topo, "counties"), separators=(",", ":"))
    )

    # Older artifacts (pre-TopoJSON migration); removed if present so a stale
    # frontend doesn't pick up GeoJSON that no longer matches the live data.
    for old in ("states.geojson", "counties.geojson"):
        p = out_dir / old
        if p.exists():
            p.unlink()
            print(f"  removed legacy {p.relative_to(_PROJECT_ROOT)}")

    for p in (boundaries_path, states_topo_path, counties_topo_path):
        print(f"  wrote {p.relative_to(_PROJECT_ROOT)} ({p.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
