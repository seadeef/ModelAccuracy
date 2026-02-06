#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path

import numpy as np

try:
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.transform import Affine
    from rasterio.warp import calculate_default_transform, reproject
except Exception as exc:  # pragma: no cover - environment dependent
    raise RuntimeError(
        "rasterio is required. Install with: pip install rasterio"
    ) from exc

DEFAULT_STATS_DIR = Path("stats") / "bias" / "ppt"
# Web Mercator (EPSG:3857) is only valid for |lat| <= this (degrees)
WEB_MERCATOR_MAX_LAT = 85.05112878


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate bias PMTiles from GFS/PRISM bias stats (via rio-pmtiles)."
    )
    parser.add_argument(
        "--stats-dir",
        type=Path,
        default=DEFAULT_STATS_DIR,
        help="Directory containing week_XX/lead_Y.npz and metadata.npz",
    )
    parser.add_argument("--week", type=int, help="Optional week filter (1-53).")
    parser.add_argument("--lead", type=int, help="Optional lead filter (1-7).")
    parser.add_argument(
        "--min-zoom",
        type=int,
        default=0,
        help="Minimum zoom to render.",
    )
    parser.add_argument(
        "--max-zoom",
        type=int,
        required=True,
        metavar="Z",
        help="Maximum zoom to render (required).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("tiles_output"),
        help="Output directory for pmtiles and metadata.",
    )
    parser.add_argument(
        "--percentile",
        type=float,
        default=98.0,
        help="Upper percentile for symmetric range (lower=100-percentile).",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=None,
        metavar="N",
        help="Number of parallel workers for rio pmtiles (default: CPU count).",
    )
    return parser.parse_args()


def load_metadata(stats_dir: Path) -> dict:
    meta_path = stats_dir / "metadata.npz"
    if not meta_path.exists():
        raise FileNotFoundError(f"Missing metadata: {meta_path.resolve()}")
    meta = np.load(meta_path)
    transform = Affine(*meta["transform"][:6])
    return {
        "lats": meta["lats"],
        "lons": meta["lons"],
        "transform": transform,
        "crs": str(meta["crs"]),
    }


def iter_layers(stats_dir: Path, week: int | None, lead: int | None):
    for week_dir in sorted(p for p in stats_dir.iterdir() if p.is_dir()):
        if not week_dir.name.startswith("week_"):
            continue
        week_num = int(week_dir.name.split("_")[1])
        if week is not None and week_num != week:
            continue
        for lead_path in sorted(week_dir.glob("lead_*.npz")):
            lead_num = int(lead_path.stem.split("_")[1])
            if lead is not None and lead_num != lead:
                continue
            yield week_num, lead_num, lead_path


def symmetric_range(values: np.ndarray, percentile: float) -> tuple[float, float]:
    lower = 100.0 - percentile
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return -1.0, 1.0
    low, high = np.nanpercentile(finite, [lower, percentile])
    vmax = float(max(abs(low), abs(high)))
    if vmax == 0.0:
        vmax = 1.0
    return -vmax, vmax


def quantize_to_int16(values: np.ndarray, vmin: float, vmax: float) -> tuple[np.ndarray, float, float]:
    scale = vmax / 32767.0
    offset = 0.0
    quant = np.zeros(values.shape, dtype=np.int16)
    valid = np.isfinite(values)
    quant_vals = np.round(values[valid] / scale)
    quant_vals = np.clip(quant_vals, -32768, 32767).astype(np.int16)
    quant[valid] = quant_vals
    return quant, scale, offset


def diverging_colormap(data: np.ndarray, vmin: float, vmax: float, mask: np.ndarray) -> np.ndarray:
    # Simple blue-white-red ramp.
    blue = np.array([44, 123, 182], dtype=np.float32)
    white = np.array([255, 255, 255], dtype=np.float32)
    red = np.array([215, 25, 28], dtype=np.float32)

    valid = mask & np.isfinite(data)
    denom = vmax - vmin
    if denom == 0:
        denom = 1.0
    t = (data - vmin) / denom
    t = np.clip(t, 0.0, 1.0)
    lower = t <= 0.5
    upper = ~lower

    rgb = np.zeros((data.shape[0], data.shape[1], 3), dtype=np.float32)
    lower = lower & valid
    upper = upper & valid
    if np.any(lower):
        t_low = (t[lower] / 0.5).reshape(-1, 1)
        rgb[lower] = (blue + (white - blue) * t_low).reshape((-1, 3))
    if np.any(upper):
        t_high = ((t[upper] - 0.5) / 0.5).reshape(-1, 1)
        rgb[upper] = (white + (red - white) * t_high).reshape((-1, 3))

    alpha = np.where(valid, 255, 0).astype(np.uint8)
    rgba = np.dstack([rgb.astype(np.uint8), alpha])
    return rgba


def write_raster(path: Path, data: np.ndarray, transform: Affine, crs: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=np.nan,
    ) as dst:
        dst.write(data.astype(np.float32), 1)


def clip_to_web_mercator_lat(
    rgba: np.ndarray, transform: Affine, lats: np.ndarray
) -> tuple[np.ndarray, Affine]:
    """Clip raster to latitude range valid for Web Mercator to avoid PROJ warnings."""
    valid = (lats >= -WEB_MERCATOR_MAX_LAT) & (lats <= WEB_MERCATOR_MAX_LAT)
    if not np.any(valid):
        return rgba, transform
    row_indices = np.where(valid)[0]
    i_min, i_max = int(row_indices.min()), int(row_indices.max())
    rgba_clip = rgba[i_min : i_max + 1, :, :].copy()
    t = transform
    new_f = t.f + i_min * t.e  # y-origin for row i_min
    new_transform = Affine(t.a, t.b, t.c, t.d, t.e, new_f)
    return rgba_clip, new_transform


def write_rgba_raster(path: Path, rgba: np.ndarray, transform: Affine, crs: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=rgba.shape[0],
        width=rgba.shape[1],
        count=4,
        dtype="uint8",
        crs=crs,
        transform=transform,
        nodata=0,
    ) as dst:
        for idx in range(4):
            dst.write(rgba[:, :, idx], idx + 1)


def reproject_to_mercator(src_path: Path, dst_path: Path) -> None:
    """Reproject RGBA GeoTIFF from source CRS (e.g. WGS84) to EPSG:3857.
    Avoids rio-pmtiles guess_maxzoom ZeroDivisionError on geographic sources."""
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(src_path) as src:
        dst_transform, width, height = calculate_default_transform(
            src.crs, "EPSG:3857", src.width, src.height, *src.bounds
        )
        profile = src.profile.copy()
        profile.update(
            crs="EPSG:3857",
            transform=dst_transform,
            width=width,
            height=height,
            nodata=0,
        )
        with rasterio.open(dst_path, "w", **profile) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs="EPSG:3857",
                    resampling=Resampling.bilinear,
                    dst_nodata=0,
                )


def export_pmtiles(
    source_path: Path,
    pmtiles_path: Path,
    min_zoom: int,
    max_zoom: int,
    jobs: int | None = None,
) -> None:
    """Export RGBA GeoTIFF (must be EPSG:3857) to PMTiles using rio-pmtiles."""
    rio = shutil.which("rio")
    if rio is None:
        raise RuntimeError(
            "rio CLI not found. Install rasterio and rio-pmtiles: pip install rasterio rio-pmtiles"
        )
    pmtiles_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        rio,
        "pmtiles",
        str(source_path),
        str(pmtiles_path),
        "--rgba",
        "--zoom-levels",
        f"{min_zoom}..{max_zoom}",
        "--tile-size",
        "256",
        "-f",
        "PNG",
    ]
    if jobs is not None:
        cmd.extend(["-j", str(jobs)])
    subprocess.run(cmd, check=True)


def write_metadata(
    out_dir: Path,
    week: int,
    lead: int,
    vmin: float,
    vmax: float,
    scale: float,
    offset: float,
    min_zoom: int,
    max_zoom: int,
) -> None:
    metadata = {
        "week": week,
        "lead_days": lead,
        "units": "mm",
        "nodata": None,
        "vmin": vmin,
        "vmax": vmax,
        "scale": scale,
        "offset": offset,
        "colormap": {
            "type": "diverging",
            "stops": [
                {"value": vmin, "color": "#2c7bb6"},
                {"value": 0.0, "color": "#ffffff"},
                {"value": vmax, "color": "#d7191c"},
            ],
        },
        "zoom": {"min": min_zoom, "max": max_zoom},
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"week_{week:02d}_lead_{lead}.json"
    out_path.write_text(json.dumps(metadata, indent=2))


def main() -> None:
    args = parse_args()
    stats_dir = args.stats_dir
    meta = load_metadata(stats_dir)
    max_zoom = args.max_zoom

    pmtiles_root = args.output_dir / "pmtiles"
    metadata_root = args.output_dir / "metadata"
    tmp_root = args.output_dir / "tmp"

    for week, lead, npz_path in iter_layers(stats_dir, args.week, args.lead):
        layer = np.load(npz_path)
        bias_mean = layer["bias_mean"]
        vmin, vmax = symmetric_range(bias_mean, args.percentile)
        quant, scale, offset = quantize_to_int16(bias_mean, vmin, vmax)
        valid = np.isfinite(bias_mean)
        dequant = np.full_like(bias_mean, np.nan, dtype=np.float32)
        dequant[valid] = quant[valid].astype(np.float32) * scale + offset
        rgba = diverging_colormap(dequant, vmin, vmax, valid)
        rgba, write_transform = clip_to_web_mercator_lat(
            rgba, meta["transform"], meta["lats"]
        )

        rgba_path = tmp_root / f"week_{week:02d}_lead_{lead}_4326_rgba.tif"
        merc_path = tmp_root / f"week_{week:02d}_lead_{lead}_3857_rgba.tif"
        pmtiles_path = pmtiles_root / f"week_{week:02d}_lead_{lead}.pmtiles"
        write_rgba_raster(rgba_path, rgba, write_transform, meta["crs"])
        reproject_to_mercator(rgba_path, merc_path)

        write_metadata(
            metadata_root,
            week,
            lead,
            vmin,
            vmax,
            scale,
            offset,
            args.min_zoom,
            max_zoom,
        )

        export_pmtiles(
            merc_path,
            pmtiles_path,
            args.min_zoom,
            max_zoom,
            args.jobs,
        )

    if tmp_root.exists():
        shutil.rmtree(tmp_root)
    print(f"PMTiles written to {pmtiles_root.resolve()}")


if __name__ == "__main__":
    main()
