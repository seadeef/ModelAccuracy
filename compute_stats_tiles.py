#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

import numpy as np
from lead_windows import normalize_lead_key
from statistics_plugins.registry import ENABLED_STATISTICS, STATISTICS_BY_NAME

try:
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.transform import Affine
    from rasterio.warp import calculate_default_transform, reproject
except Exception as exc:  # pragma: no cover - environment dependent
    raise RuntimeError(
        "rasterio is required. Install with: pip install rasterio"
    ) from exc

DEFAULT_STATS_ROOT = Path("stats")
WEB_MERCATOR_MAX_LAT = 85.05112878
US_HEADER_CENTER = [-98.5795, 39.8283, 3.0]
US_HEADER_BOUNDS = [-125.0, 24.0, -66.0, 50.0]
SHARED_COLORMAP = "diverging"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate PMTiles for configured statistics (via rio-pmtiles)."
    )
    parser.add_argument(
        "--stats-root",
        type=Path,
        default=DEFAULT_STATS_ROOT,
        help="Root directory containing stats/<stat_name>/ subdirectories.",
    )
    parser.add_argument(
        "--stat",
        action="append",
        dest="stats",
        help="Statistic name to process. Repeat for multiple values.",
    )
    parser.add_argument(
        "--lead",
        type=str,
        help="Optional lead filter (single lead like '7' or range key like '1_7'/'1-7').",
    )
    parser.add_argument("--min-zoom", type=int, default=0, help="Minimum zoom to render.")
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
        help="Upper percentile for value range scaling.",
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


def resolve_statistics(selected: list[str] | None) -> list:
    if not selected:
        return ENABLED_STATISTICS
    unknown = [name for name in selected if name not in STATISTICS_BY_NAME]
    if unknown:
        raise ValueError(
            f"Unknown statistic(s): {', '.join(sorted(unknown))}. "
            f"Available: {', '.join(sorted(STATISTICS_BY_NAME))}"
        )
    return [STATISTICS_BY_NAME[name] for name in selected]


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


def iter_layers(stats_dir: Path, lead_key: str | None):
    for lead_path in sorted(stats_dir.glob("lead_*.npz")):
        key = lead_path.stem.replace("lead_", "", 1)
        if lead_key is not None and key != lead_key:
            continue
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
    if finite.size == 0:
        return (-1.0, 1.0) if colormap == "diverging" else (0.0, 1.0)

    if colormap == "diverging":
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
        return (-1.0, 1.0) if colormap == "diverging" else (0.0, 1.0)

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


def quantize_to_int16(values: np.ndarray, vmin: float, vmax: float) -> tuple[np.ndarray, float, float]:
    if vmax <= vmin:
        vmax = vmin + 1.0
    scale = (vmax - vmin) / 65535.0
    offset = vmin + 32768.0 * scale
    quant = np.zeros(values.shape, dtype=np.int16)
    valid = np.isfinite(values)
    quant_vals = np.round((values[valid] - offset) / scale)
    quant_vals = np.clip(quant_vals, -32768, 32767).astype(np.int16)
    quant[valid] = quant_vals
    return quant, float(scale), float(offset)


def diverging_colormap(
    data: np.ndarray, vmin: float, vmax: float, mask: np.ndarray, gamma: float = 0.7,
) -> np.ndarray:
    blue = np.array([44, 123, 182], dtype=np.float32)
    white = np.array([255, 255, 255], dtype=np.float32)
    red = np.array([215, 25, 28], dtype=np.float32)
    valid = mask & np.isfinite(data)
    denom = vmax - vmin if vmax != vmin else 1.0
    t = np.clip((data - vmin) / denom, 0.0, 1.0)

    # Apply gamma symmetrically around the midpoint so small deviations
    # from zero get more color separation while preserving the scale.
    dist = np.abs(t - 0.5) * 2.0
    mapped = np.power(dist, gamma)
    t = np.where(t <= 0.5, 0.5 - 0.5 * mapped, 0.5 + 0.5 * mapped)

    rgb = np.zeros((data.shape[0], data.shape[1], 3), dtype=np.float32)
    lower = (t <= 0.5) & valid
    upper = (~(t <= 0.5)) & valid
    if np.any(lower):
        t_low = (t[lower] / 0.5).reshape(-1, 1)
        rgb[lower] = (blue + (white - blue) * t_low).reshape((-1, 3))
    if np.any(upper):
        t_high = ((t[upper] - 0.5) / 0.5).reshape(-1, 1)
        rgb[upper] = (white + (red - white) * t_high).reshape((-1, 3))
    alpha = np.where(valid, 255, 0).astype(np.uint8)
    return np.dstack([rgb.astype(np.uint8), alpha])


def sequential_colormap(data: np.ndarray, vmin: float, vmax: float, mask: np.ndarray) -> np.ndarray:
    white = np.array([255, 255, 255], dtype=np.float32)
    green = np.array([33, 145, 140], dtype=np.float32)
    valid = mask & np.isfinite(data)
    denom = vmax - vmin if vmax != vmin else 1.0
    t = np.clip((data - vmin) / denom, 0.0, 1.0)
    rgb = np.zeros((data.shape[0], data.shape[1], 3), dtype=np.float32)
    if np.any(valid):
        tv = t[valid].reshape(-1, 1)
        rgb[valid] = (white + (green - white) * tv).reshape((-1, 3))
    alpha = np.where(valid, 255, 0).astype(np.uint8)
    return np.dstack([rgb.astype(np.uint8), alpha])


def clip_to_web_mercator_lat(
    rgba: np.ndarray, transform: Affine, lats: np.ndarray
) -> tuple[np.ndarray, Affine]:
    valid = (lats >= -WEB_MERCATOR_MAX_LAT) & (lats <= WEB_MERCATOR_MAX_LAT)
    if not np.any(valid):
        return rgba, transform
    row_indices = np.where(valid)[0]
    i_min, i_max = int(row_indices.min()), int(row_indices.max())
    rgba_clip = rgba[i_min : i_max + 1, :, :].copy()
    t = transform
    new_f = t.f + i_min * t.e
    return rgba_clip, Affine(t.a, t.b, t.c, t.d, t.e, new_f)


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


def compute_mercator_transform(
    src_crs: str,
    src_width: int,
    src_height: int,
    src_bounds: tuple[float, float, float, float],
) -> tuple[Affine, int, int]:
    """Compute the EPSG:3857 destination transform, width, and height once."""
    dst_transform, width, height = calculate_default_transform(
        src_crs, "EPSG:3857", src_width, src_height, *src_bounds
    )
    return dst_transform, width, height


def reproject_to_mercator(
    src_path: Path,
    dst_path: Path,
    dst_transform: Affine | None = None,
    dst_width: int | None = None,
    dst_height: int | None = None,
) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(src_path) as src:
        if dst_transform is None or dst_width is None or dst_height is None:
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


def export_pmtiles(
    source_path: Path,
    pmtiles_path: Path,
    min_zoom: int,
    max_zoom: int,
    jobs: int | None = None,
) -> None:
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
        "--exclude-empty-tiles",
    ]
    if jobs is not None:
        cmd.extend(["-j", str(jobs)])
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def edit_pmtiles_header(
    pmtiles_path: Path,
    header_path: Path,
    center: list[float],
    bounds: list[float],
) -> None:
    pmtiles_bin = shutil.which("pmtiles")
    if pmtiles_bin is None:
        return
    result = subprocess.run(
        [pmtiles_bin, "show", str(pmtiles_path), "--header-json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return
    header = json.loads(result.stdout)
    header["center"] = center
    header["bounds"] = bounds
    header_path.write_text(json.dumps(header))
    try:
        subprocess.run(
            [pmtiles_bin, "edit", str(pmtiles_path), f"--header-json={header_path}"],
            check=True,
        )
    except subprocess.CalledProcessError:
        return
    finally:
        header_path.unlink(missing_ok=True)


def write_metadata(
    out_path: Path,
    stat_name: str,
    units: str,
    lead_key: str,
    vmin: float,
    vmax: float,
    scale: float,
    offset: float,
    min_zoom: int,
    max_zoom: int,
    colormap: str,
) -> None:
    metadata = {
        "statistic": stat_name,
        "lead": lead_key,
        "units": units,
        "nodata": None,
        "vmin": vmin,
        "vmax": vmax,
        "scale": scale,
        "offset": offset,
        "colormap": {"type": colormap},
        "zoom": {"min": min_zoom, "max": max_zoom},
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(metadata, indent=2))


def _process_layer(
    lead_key: str,
    npz_path: Path,
    field: str,
    global_vmin: float,
    global_vmax: float,
    transform_tuple: tuple,
    lats: np.ndarray,
    crs: str,
    stat_name: str,
    units: str,
    tmp_root: Path,
    pmtiles_root: Path,
    metadata_root: Path,
    min_zoom: int,
    max_zoom: int,
    rio_jobs: int | None,
    colormap: str,
    merc_transform_tuple: tuple,
    merc_width: int,
    merc_height: int,
) -> str:
    """Process a single (statistic, lead) layer. Designed for use in a process pool."""
    transform = Affine(*transform_tuple[:6])
    merc_transform = Affine(*merc_transform_tuple[:6])

    with np.load(npz_path) as layer:
        if field not in layer:
            raise KeyError(f"Missing field '{field}' in {npz_path}.")
        values = layer[field]

    vmin, vmax = global_vmin, global_vmax
    quant, scale, offset = quantize_to_int16(values, vmin, vmax)
    valid = np.isfinite(values)
    dequant = np.full_like(values, np.nan, dtype=np.float32)
    dequant[valid] = quant[valid].astype(np.float32) * scale + offset
    rgba = diverging_colormap(dequant, vmin, vmax, valid)
    rgba, write_transform = clip_to_web_mercator_lat(rgba, transform, lats)

    rgba_path = tmp_root / stat_name / f"lead_{lead_key}_4326_rgba.tif"
    merc_path = tmp_root / stat_name / f"lead_{lead_key}_3857_rgba.tif"
    pmtiles_path = pmtiles_root / stat_name / f"lead_{lead_key}.pmtiles"
    metadata_path = metadata_root / stat_name / f"lead_{lead_key}.json"

    write_rgba_raster(rgba_path, rgba, write_transform, crs)
    reproject_to_mercator(rgba_path, merc_path, merc_transform, merc_width, merc_height)

    write_metadata(
        metadata_path,
        stat_name,
        units,
        lead_key,
        vmin,
        vmax,
        scale,
        offset,
        min_zoom,
        max_zoom,
        colormap,
    )

    export_pmtiles(merc_path, pmtiles_path, min_zoom, max_zoom, rio_jobs)

    header_tmp = tmp_root / stat_name / f"header_lead_{lead_key}.json"
    edit_pmtiles_header(pmtiles_path, header_tmp, US_HEADER_CENTER, US_HEADER_BOUNDS)

    return f"{stat_name}/lead_{lead_key}"


def main() -> None:
    args = parse_args()
    selected_lead_key = normalize_lead_key(args.lead) if args.lead is not None else None

    selected_plugins = resolve_statistics(args.stats)
    pmtiles_root = args.output_dir / "pmtiles"
    metadata_root = args.output_dir / "metadata"
    tmp_root = args.output_dir / "tmp"

    rio_jobs = args.jobs

    # Collect all (stat, lead) tasks up-front so we can submit them in bulk.
    tasks: list[dict] = []

    for plugin in selected_plugins:
        stat_name = plugin.spec.name
        stats_dir = args.stats_root / stat_name
        if not stats_dir.exists():
            print(f"Skipping statistic '{stat_name}' (missing {stats_dir}).")
            continue

        meta = load_metadata(stats_dir)
        layers = list(iter_layers(stats_dir, selected_lead_key))
        if not layers:
            print(f"Skipping statistic '{stat_name}' (no matching leads found).")
            continue
        field = plugin.spec.render_field
        layer_paths = [npz_path for _, npz_path in layers]
        global_vmin, global_vmax = value_range_from_layers(
            layer_paths,
            field,
            args.percentile,
            SHARED_COLORMAP,
            plugin.spec.fixed_range,
        )

        # Pre-compute the clipped transform (identical for all leads sharing
        # the same grid) so we can derive the EPSG:3857 reprojection params
        # once and reuse them for every lead layer.
        _clip_dummy = np.zeros(
            (len(meta["lats"]), len(meta["lons"]), 4), dtype=np.uint8
        )
        _clipped_dummy, clipped_transform = clip_to_web_mercator_lat(
            _clip_dummy, meta["transform"], meta["lats"]
        )
        clipped_height, clipped_width = _clipped_dummy.shape[:2]

        merc_transform, merc_width, merc_height = compute_mercator_transform(
            meta["crs"],
            clipped_width,
            clipped_height,
            rasterio.transform.array_bounds(
                clipped_height, clipped_width, clipped_transform
            ),
        )

        # Serialize Affine objects as tuples so they pickle safely across processes.
        transform_tuple = tuple(meta["transform"])[:6]
        merc_transform_tuple = tuple(merc_transform)[:6]

        for lead_key, npz_path in layers:
            tasks.append(dict(
                lead_key=lead_key,
                npz_path=npz_path,
                field=field,
                global_vmin=global_vmin,
                global_vmax=global_vmax,
                transform_tuple=transform_tuple,
                lats=meta["lats"],
                crs=meta["crs"],
                stat_name=stat_name,
                units=plugin.spec.units,
                tmp_root=tmp_root,
                pmtiles_root=pmtiles_root,
                metadata_root=metadata_root,
                min_zoom=args.min_zoom,
                max_zoom=args.max_zoom,
                rio_jobs=rio_jobs,
                colormap=SHARED_COLORMAP,
                merc_transform_tuple=merc_transform_tuple,
                merc_width=merc_width,
                merc_height=merc_height,
            ))

    total = len(tasks)
    if total == 0:
        print("No layers to process.")
    else:
        print(f"Processing {total} layer(s) sequentially (rio pmtiles handles parallelism) …")
        for i, t in enumerate(tasks, start=1):
            label = f"{t['stat_name']}/lead_{t['lead_key']}"
            print(f"  [{i}/{total}] {label} …", flush=True)
            _process_layer(**t)

    if tmp_root.exists():
        shutil.rmtree(tmp_root)
    print(f"PMTiles written to {(args.output_dir / 'pmtiles').resolve()}")


if __name__ == "__main__":
    main()
