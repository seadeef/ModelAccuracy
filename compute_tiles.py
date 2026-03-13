#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import numpy as np
from model_registry import MODEL_REGISTRY, DEFAULT_MODEL
from statistics_plugins.registry import ENABLED_STATISTICS

try:
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.transform import Affine
    from rasterio.warp import calculate_default_transform, reproject
except Exception as exc:  # pragma: no cover - environment dependent
    raise RuntimeError(
        "rasterio is required. Install with: pip install rasterio"
    ) from exc

US_HEADER_BOUNDS = [-125.0, 24.0, -66.0, 50.0]

SEASONS = {"djf": (12, 1, 2), "mam": (3, 4, 5), "jja": (6, 7, 8), "son": (9, 10, 11)}



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate tile images (PNGs) for configured statistics."
    )
    parser.add_argument(
        "--model",
        default=None,
        choices=list(MODEL_REGISTRY),
        help="Model to generate tiles for (default: all models).",
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


def iter_layers(stats_dir: Path, subdir: str | None = None):
    source_dir = stats_dir / subdir if subdir else stats_dir
    if not source_dir.exists():
        return
    for lead_path in sorted(source_dir.glob("lead_*.npz")):
        key = lead_path.stem.replace("lead_", "", 1)
        yield key, lead_path


def value_range(
    values: np.ndarray,
    percentile: float,
    colormap: str,
    fixed_range: tuple[float, float] | None = None,
) -> tuple[float, float]:
    if fixed_range is not None:
        return fixed_range
    finite = values[np.isfinite(values)]
    is_diverging = colormap in ("diverging", "diverging_reversed")
    if finite.size == 0:
        return (-1.0, 1.0) if is_diverging else (0.0, 1.0)

    if is_diverging:
        lower = 100.0 - percentile
        low, high = np.nanpercentile(finite, [lower, percentile])
        low_f = float(low)
        high_f = float(high)
        if high_f <= low_f:
            high_f = low_f + 1.0
        return low_f, high_f

    vmax = float(np.nanpercentile(finite, percentile))
    if vmax <= 0.0:
        vmax = 1.0
    return 0.0, vmax


def value_range_from_layers(
    layer_paths: list[Path],
    field: str,
    percentile: float,
    colormap: str,
    fixed_range: tuple[float, float] | None = None,
) -> tuple[float, float]:
    if fixed_range is not None:
        return fixed_range
    if not layer_paths:
        return (-1.0, 1.0) if colormap in ("diverging", "diverging_reversed") else (0.0, 1.0)

    finite_parts: list[np.ndarray] = []
    for layer_path in layer_paths:
        with np.load(layer_path) as layer:
            if field not in layer:
                raise KeyError(f"Missing field '{field}' in {layer_path}.")
            values = layer[field]
            finite = values[np.isfinite(values)]
            if finite.size > 0:
                finite_parts.append(finite.astype(np.float32))

    if not finite_parts:
        return (-1.0, 1.0) if colormap == "diverging" else (0.0, 1.0)

    merged = np.concatenate(finite_parts)
    return value_range(merged, percentile, colormap, fixed_range)


def diverging_colormap(
    data: np.ndarray, vmin: float, vmax: float, mask: np.ndarray,
) -> np.ndarray:
    blue = np.array([44, 123, 182], dtype=np.float32)
    white = np.array([255, 255, 255], dtype=np.float32)
    red = np.array([215, 25, 28], dtype=np.float32)

    valid = mask & np.isfinite(data)
    denom = vmax - vmin if vmax != vmin else 1.0
    t = np.clip((data - vmin) / denom, 0.0, 1.0)

    rgb = np.zeros((data.shape[0], data.shape[1], 3), dtype=np.uint8)
    low = t <= 0.5
    high = ~low
    low_valid = valid & low
    high_valid = valid & high
    frac_low = (t[low_valid] / 0.5)[:, None]
    frac_high = ((t[high_valid] - 0.5) / 0.5)[:, None]
    rgb[low_valid] = np.clip(blue + (white - blue) * frac_low, 0, 255).astype(np.uint8)
    rgb[high_valid] = np.clip(white + (red - white) * frac_high, 0, 255).astype(np.uint8)
    alpha = np.where(valid, 255, 0).astype(np.uint8)
    return np.dstack([rgb, alpha])


def diverging_reversed_colormap(
    data: np.ndarray, vmin: float, vmax: float, mask: np.ndarray,
) -> np.ndarray:
    """Red (low) → White (mid) → Blue (high)."""
    red = np.array([215, 25, 28], dtype=np.float32)
    white = np.array([255, 255, 255], dtype=np.float32)
    blue = np.array([44, 123, 182], dtype=np.float32)

    valid = mask & np.isfinite(data)
    denom = vmax - vmin if vmax != vmin else 1.0
    t = np.clip((data - vmin) / denom, 0.0, 1.0)

    rgb = np.zeros((data.shape[0], data.shape[1], 3), dtype=np.uint8)
    low = t <= 0.5
    high = ~low
    low_valid = valid & low
    high_valid = valid & high
    frac_low = (t[low_valid] / 0.5)[:, None]
    frac_high = ((t[high_valid] - 0.5) / 0.5)[:, None]
    rgb[low_valid] = np.clip(red + (white - red) * frac_low, 0, 255).astype(np.uint8)
    rgb[high_valid] = np.clip(white + (blue - white) * frac_high, 0, 255).astype(np.uint8)
    alpha = np.where(valid, 255, 0).astype(np.uint8)
    return np.dstack([rgb, alpha])


def sequential_colormap(data: np.ndarray, vmin: float, vmax: float, mask: np.ndarray) -> np.ndarray:
    white = np.array([255, 255, 255], dtype=np.float32)
    blue = np.array([44, 123, 182], dtype=np.float32)
    valid = mask & np.isfinite(data)
    denom = vmax - vmin if vmax != vmin else 1.0
    t = np.clip((data - vmin) / denom, 0.0, 1.0)
    rgb = np.zeros((data.shape[0], data.shape[1], 3), dtype=np.float32)
    if np.any(valid):
        tv = t[valid].reshape(-1, 1)
        rgb[valid] = (white + (blue - white) * tv).reshape((-1, 3))
    alpha = np.where(valid, 255, 0).astype(np.uint8)
    return np.dstack([rgb.astype(np.uint8), alpha])


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
    ) as dst:
        for idx in range(4):
            dst.write(rgba[:, :, idx], idx + 1)


def reproject_to_mercator(
    src_path: Path,
    dst_path: Path,
) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(src_path) as src:
        dst_transform, dst_width, dst_height = calculate_default_transform(
            src.crs, "EPSG:3857", src.width, src.height, *src.bounds
        )
        profile = src.profile.copy()
        profile.update(
            crs="EPSG:3857",
            transform=dst_transform,
            width=dst_width,
            height=dst_height,
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
                )


def _process_layer_image(
    lead_key: str,
    npz_path: Path,
    field: str,
    global_vmin: float,
    global_vmax: float,
    transform_tuple: tuple,
    lats: np.ndarray,
    crs: str,
    stat_name: str,
    images_root: Path,
    tmp_root: Path,
    colormap: str = "diverging",
) -> str:
    """Render a single layer as a mercator-reprojected PNG, cropped to US."""
    from PIL import Image

    transform = Affine(*transform_tuple[:6])

    with np.load(npz_path) as layer:
        if field not in layer:
            raise KeyError(f"Missing field '{field}' in {npz_path}.")
        values = layer[field]

    # Crop to US in source grid before colormapping.
    west, south, east, north = US_HEADER_BOUNDS
    lat_res = abs(transform.e)
    lon_res = abs(transform.a)
    col_start = max(0, int((west - transform.c) / lon_res))
    col_end = min(values.shape[1], int((east - transform.c) / lon_res) + 1)

    if transform.e > 0:
        # South-to-north: row 0 is southernmost, transform.f is south edge.
        row_start = max(0, int((south - transform.f) / lat_res))
        row_end = min(values.shape[0], int((north - transform.f) / lat_res) + 1)
    else:
        # North-to-south: row 0 is northernmost, transform.f is north edge.
        row_start = max(0, int((transform.f - north) / lat_res))
        row_end = min(values.shape[0], int((transform.f - south) / lat_res) + 1)

    cropped = values[row_start:row_end, col_start:col_end]
    crop_transform = Affine(
        transform.a, transform.b, transform.c + col_start * lon_res,
        transform.d, transform.e, transform.f + row_start * transform.e,
    )

    valid = np.isfinite(cropped)
    _COLORMAP_FNS = {
        "sequential": sequential_colormap,
        "diverging": diverging_colormap,
        "diverging_reversed": diverging_reversed_colormap,
    }
    colormap_fn = _COLORMAP_FNS.get(colormap, diverging_colormap)
    rgba = colormap_fn(cropped, global_vmin, global_vmax, valid)

    # Write as 4326 GeoTIFF, reproject to 3857, then save as PNG.
    tif_4326 = tmp_root / stat_name / f"lead_{lead_key}_img_4326.tif"
    tif_3857 = tmp_root / stat_name / f"lead_{lead_key}_img_3857.tif"
    write_rgba_raster(tif_4326, rgba, crop_transform, crs)
    reproject_to_mercator(tif_4326, tif_3857)

    # Read the reprojected raster and save as PNG.
    with rasterio.open(tif_3857) as src:
        merc_rgba = np.stack([src.read(i) for i in range(1, 5)], axis=-1)

    out_dir = images_root / stat_name
    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / f"lead_{lead_key}.png"
    Image.fromarray(merc_rgba, "RGBA").save(png_path, optimize=True)

    return f"{stat_name}/lead_{lead_key}"


PERCENTILE = 98.0


def _collect_tasks_for_period(
    plugins: list,
    stats_root: Path,
    images_root: Path,
    tmp_root: Path,
    source_subdir: str | None,
    output_subdir: str | None,
    skip_forecast: bool = False,
) -> list[dict]:
    """Collect tile-generation tasks for a single period (yearly/monthly/seasonal)."""
    tasks: list[dict] = []

    for plugin in plugins:
        stat_name = plugin.spec.name
        if skip_forecast and stat_name == "forecast":
            continue
        stats_dir = stats_root / stat_name
        if not stats_dir.exists():
            continue

        meta = load_metadata(stats_dir)
        layers = list(iter_layers(stats_dir, subdir=source_subdir))
        if not layers:
            continue
        field = plugin.spec.render_field
        layer_paths = [npz_path for _, npz_path in layers]
        colormap = plugin.spec.colormap
        global_vmin, global_vmax = value_range_from_layers(
            layer_paths,
            field,
            PERCENTILE,
            colormap,
            plugin.spec.fixed_range,
        )

        transform_tuple = tuple(meta["transform"])[:6]

        for lead_key, npz_path in layers:
            if output_subdir:
                out_path = images_root / stat_name / output_subdir / f"lead_{lead_key}.png"
            else:
                out_path = images_root / stat_name / f"lead_{lead_key}.png"
            if out_path.exists():
                continue

            effective_stat_name = f"{stat_name}/{output_subdir}" if output_subdir else stat_name

            tasks.append(dict(
                lead_key=lead_key,
                npz_path=npz_path,
                field=field,
                global_vmin=global_vmin,
                global_vmax=global_vmax,
                transform_tuple=transform_tuple,
                lats=meta["lats"],
                crs=meta["crs"],
                stat_name=effective_stat_name,
                images_root=images_root,
                tmp_root=tmp_root,
                colormap=colormap,
            ))

    return tasks


OUTPUT_DIR = Path("tiles_output")


def _run_for_model(model_key: str) -> None:
    stats_root = Path("stats_output") / model_key
    plugins = ENABLED_STATISTICS
    images_root = OUTPUT_DIR / "images" / model_key
    tmp_root = OUTPUT_DIR / "tmp"

    # Collect tasks for all periods.
    tasks: list[dict] = []

    # 1. Yearly (source from stat root, output to stat root — unchanged paths).
    tasks.extend(_collect_tasks_for_period(
        plugins, stats_root, images_root, tmp_root,
        source_subdir=None, output_subdir=None,
    ))

    # 2. All 12 months (skip forecast).
    for m in range(1, 13):
        mm = f"{m:02d}"
        tasks.extend(_collect_tasks_for_period(
            plugins, stats_root, images_root, tmp_root,
            source_subdir=f"monthly/{mm}",
            output_subdir=f"monthly/{mm}",
            skip_forecast=True,
        ))

    # 3. All 4 seasons (skip forecast).
    for season_name in SEASONS:
        tasks.extend(_collect_tasks_for_period(
            plugins, stats_root, images_root, tmp_root,
            source_subdir=f"seasonal/{season_name}",
            output_subdir=f"seasonal/{season_name}",
            skip_forecast=True,
        ))

    total = len(tasks)
    if total == 0:
        print("No layers to process.")
    else:
        print(f"Processing {total} layer(s) …")
        for i, kwargs in enumerate(tasks, start=1):
            label = f"{kwargs['stat_name']}/lead_{kwargs['lead_key']}"
            print(f"  [{i}/{total}] {label} …", flush=True)
            _process_layer_image(**kwargs)

    if tmp_root.exists():
        shutil.rmtree(tmp_root)
    print(f"Output written to {OUTPUT_DIR.resolve()}")


def main() -> None:
    args = parse_args()
    models = [args.model] if args.model else list(MODEL_REGISTRY)
    for model_key in models:
        print(f"\n=== Generating tiles for model '{model_key}' ===")
        _run_for_model(model_key)


if __name__ == "__main__":
    main()
