"""Convert Guan & Waliser tARget v4 0.25-degree masks to browser GeoJSON.

The source database is not redistributed by this repository. This consumes the
locally indexed, range-cached virtual netCDF and writes ``manifest.json`` plus one compact
GeoJSON frame per 6-hour timestamp for the frontend's atmospheric-river mode.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from affine import Affine
from rasterio.features import shapes
from atmospheric_rivers.dataset import iter_normalized_frames


def _frame(mask: np.ndarray, lats: np.ndarray, lons: np.ndarray, timestamp: str, min_cells: int):
    binary = np.asarray(mask) > 0
    if lats.size < 2 or lons.size < 2:
        raise ValueError("Latitude and longitude dimensions must each contain at least two cells")
    dx = float(np.median(np.diff(lons)))
    dy = float(abs(np.median(np.diff(lats))))
    transform = Affine(dx, 0, lons[0] - dx / 2, 0, -dy, lats[0] + dy / 2)
    features = []
    for geometry, value in shapes(binary.astype(np.uint8), mask=binary, transform=transform):
        if not value:
            continue
        # Pixel count is inexpensive and prevents tiny single-cell artifacts when requested.
        coords = geometry.get("coordinates") or []
        west = min(p[0] for poly in (coords if geometry["type"] == "MultiPolygon" else [coords]) for ring in poly for p in ring)
        east = max(p[0] for poly in (coords if geometry["type"] == "MultiPolygon" else [coords]) for ring in poly for p in ring)
        south = min(p[1] for poly in (coords if geometry["type"] == "MultiPolygon" else [coords]) for ring in poly for p in ring)
        north = max(p[1] for poly in (coords if geometry["type"] == "MultiPolygon" else [coords]) for ring in poly for p in ring)
        approx_cells = max(1, round((east - west) / dx) * round((north - south) / dy))
        if approx_cells < min_cells:
            continue
        ar_id = f"{timestamp}:{len(features) + 1}"
        features.append({
            "type": "Feature",
            "id": ar_id,
            "properties": {"ar_id": ar_id, "time": timestamp},
            "geometry": geometry,
        })
    return {"type": "FeatureCollection", "features": features}


def _write_json_if_changed(
    path: Path, payload: dict, *, pretty: bool = False, force: bool = False,
) -> bool:
    content = json.dumps(payload, indent=2 if pretty else None, separators=None if pretty else (",", ":"))
    if not force:
        try:
            if path.read_text(encoding="utf-8") == content:
                return False
        except (FileNotFoundError, UnicodeDecodeError):
            pass
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def export_files(
    inputs: list[Path],
    output: Path,
    variable: str | None,
    min_cells: int,
    *,
    force: bool = False,
) -> None:
    from atmospheric_rivers.paths import (
        TARGET_AR_EXTENSION_FILENAME, target_ar_extension_meta,
    )

    output.mkdir(parents=True, exist_ok=True)
    frames = []
    frame_number = 0
    seen_timestamps = set()
    for frame in iter_normalized_frames(inputs, variable=variable):
        # Sources are ordered canonical-first; on any timestamp overlap the
        # canonical catalog frame wins over the local extension.
        if frame.timestamp in seen_timestamps:
            continue
        seen_timestamps.add(frame.timestamp)
        geojson = _frame(frame.mask, frame.lats, frame.lons, frame.timestamp, min_cells)
        filename = f"frames/{frame_number:07d}.geojson"
        target = output / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        _write_json_if_changed(target, geojson, force=force)
        entry = {"time": frame.timestamp, "path": filename, "count": len(geojson["features"])}
        if Path(frame.source).name == TARGET_AR_EXTENSION_FILENAME:
            entry["non_canonical"] = True
        frames.append(entry)
        frame_number += 1
    frames.sort(key=lambda item: item["time"])
    manifest = {
        "dataset": "Guan & Waliser tARget v4",
        "temporal_resolution_hours": 6,
        "grid_resolution_degrees": 0.25,
        "frames": frames,
    }
    extension_meta = target_ar_extension_meta()
    if extension_meta is not None and any(f.get("non_canonical") for f in frames):
        manifest["local_extension"] = {
            "start": extension_meta.get("start"),
            "end": extension_meta.get("end"),
            "note": ("frames flagged non_canonical are locally generated "
                     "with the published tARget v4 algorithm on ERA5/ERA5T "
                     "IVT — not the canonical UCLA Dataverse release"),
        }
    _write_json_if_changed(output / "manifest.json", manifest, pretty=True, force=force)
    keep = {output / item["path"] for item in frames}
    frames_root = output / "frames"
    if frames_root.is_dir():
        for stale in frames_root.glob("*.geojson"):
            if stale not in keep:
                stale.unlink()
    print(f"Wrote {len(frames)} AR frames to {output}")
