#!/usr/bin/env python3
"""Export stats, tiles, config, and the Vite SPA for static hosting.

Reads from stats_output/ and tiles_output/, writes to static_export/:

    static_export/                 ← upload this folder as the site root (S3, etc.)
        index.html
        assets/
        data/                      ← URL prefix /data (grid, .bin only)
            {model}/
                grid.json
                {stat}/…
        static/                    ← URL prefix /static (config, zip, tiles, ranges)
            config.json
            zip/
                95124.json
            tiles/
                {model}/{stat}/lead_1.png
                ...
            ranges/
                {model}/{stat}/yearly.json
                {model}/{stat}/monthly/{MM}.json
                {model}/{stat}/seasonal/{djf|…}.json

Use ``--skip-frontend-build`` / ``--no-deploy-site`` to skip ``npm run build``;
``static_export/static/`` and ``static_export/data/`` are still written.

Use ``--frontend-only`` to rebuild only the Vite SPA (removes old ``index.html`` and
``assets/`` under the site root, then runs ``npm run build``). Does not touch
``static/`` or ``data/``. Cannot be combined with ``--clean`` (full clean would
delete those trees).

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
from pathlib import Path

import numpy as np

from model_registry import MODEL_REGISTRY, DEFAULT_MODEL
from stats_grid_metadata import load_model_metadata
from statistics_plugins.registry import (
    DEFAULT_STATISTIC,
    ENABLED_STATISTICS,
    STATISTICS_BY_NAME,
)

_PROJECT_ROOT = Path(__file__).resolve().parent
STATS_ROOT = _PROJECT_ROOT / "stats_output"
TILES_ROOT = _PROJECT_ROOT / "tiles_output"
EXPORT_ROOT = _PROJECT_ROOT / "static_export"
FRONTEND_ROOT = _PROJECT_ROOT / "frontend"
# Served at /static (config, zip, tiles). ``data/`` is top-level → /data.
STATIC_ASSETS_DIR_NAME = "static"
DATA_DIR_NAME = "data"


def _get_forecast_init_date(model_key: str) -> str | None:
    meta_path = STATS_ROOT / model_key / "forecast" / "metadata.npz"
    if not meta_path.exists():
        return None
    try:
        meta = np.load(meta_path, allow_pickle=True)
        return str(meta["init_date"])
    except Exception:
        return None


# ── config.json ─────────────────────────────────────────────────────

def export_config(export_dir: Path, maptiler_key: str) -> None:
    models = []
    forecast_init_dates = {}
    for key, config in sorted(MODEL_REGISTRY.items()):
        models.append({
            "key": config.key,
            "label": config.label,
            "lead_days_min": config.lead_days_min,
            "lead_days_max": config.lead_days_max,
            "lead_windows": [list(w) for w in config.lead_windows],
        })
        init_date = _get_forecast_init_date(key)
        if init_date is not None:
            forecast_init_dates[key] = init_date

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
        "forecast_init_date": forecast_init_dates,
        "forecastInitDate": forecast_init_dates,
    }

    out = export_dir / "config.json"
    out.write_text(json.dumps(config_data, indent=2))
    print(f"  {out}")


# ── zip/NNNNN.json (from zip_lookup.csv) ────────────────────────────

def export_zip_directory(export_dir: Path, csv_path: Path) -> None:
    if not csv_path.exists():
        print(f"  SKIP: {csv_path} not found")
        return

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
            out_path = zip_dir / f"{code}.json"
            out_path.write_text(json.dumps(payload))
            count += 1

    total_bytes = sum(p.stat().st_size for p in zip_dir.glob("*.json"))
    size_mb = total_bytes / (1024 * 1024)
    print(f"  {zip_dir}/ ({count} files, {size_mb:.1f} MB)")


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
                # Yearly precip only, under ``{model}/forecast/`` — see ``export_forecast_data_layers``.
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
        out_path.write_text(json.dumps(payload))
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
    out_path.write_bytes(arr.tobytes())
    return 1


def export_forecast_data_layers(data_root: Path) -> None:
    """Export ``stats_output/{model}/forecast/lead_*.npz`` → ``data/{model}/forecast/*.bin``.

    Forecast tiles use the same tree; static stats queries need these bins in forecast mode.
    """
    field = STATISTICS_BY_NAME["forecast"].spec.render_field
    data_dir = data_root
    total = 0
    for model_key in MODEL_REGISTRY:
        forecast_dir = STATS_ROOT / model_key / "forecast"
        if not forecast_dir.is_dir():
            continue
        for npz_path in sorted(forecast_dir.glob("lead_*.npz")):
            total += _export_layer(npz_path, field, data_dir / model_key / "forecast")
    print(f"  {total} forecast .bin files (data/{{model}}/forecast/)")


# ── tiles (copy PNGs) ──────────────────────────────────────────────

def export_tiles(export_dir: Path) -> None:
    tiles_dest = export_dir / "tiles"
    if not TILES_ROOT.exists():
        print("  SKIP: tiles_output/ not found")
        return

    count = 0
    for model_key in MODEL_REGISTRY:
        src = TILES_ROOT / model_key
        if not src.exists():
            continue
        dst = tiles_dest / model_key
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
        count += sum(1 for _ in dst.rglob("*.png"))

    print(f"  {count} tile PNGs copied to {tiles_dest}")


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
    path.write_text(
        json.dumps({"vmin": vmin, "vmax": vmax, "colormap": colormap}, indent=2),
        encoding="utf-8",
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
    """Remove prior Vite output at the site root (hashed chunks under ``assets/``)."""
    index = site_root / "index.html"
    if index.is_file():
        index.unlink()
        print(f"  removed {index}")
    assets = site_root / "assets"
    if assets.is_dir():
        shutil.rmtree(assets)
        print(f"  removed {assets}/")


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


def write_export_manifest(site_root: Path) -> None:
    """Small schema marker for ops (paths intentionally relative / portable)."""
    manifest = {
        "schema": "model-accuracy-static-site/v1",
        "static_assets_dir": STATIC_ASSETS_DIR_NAME,
        "data_dir": DATA_DIR_NAME,
        "url_prefix_for_assets": "/static",
        "url_prefix_for_data": "/data",
        "legend_ranges_dir": f"{STATIC_ASSETS_DIR_NAME}/ranges",
    }
    out = site_root / "export_manifest.json"
    out.write_text(json.dumps(manifest, indent=2))
    print(f"  {out}")


# ── main ────────────────────────────────────────────────────────────

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
        help="Delete output directory before exporting",
    )
    parser.add_argument(
        "--skip-frontend-build",
        "--no-deploy-site",
        action="store_true",
        dest="skip_frontend_build",
        help="Skip npm/Vite (no index.html/assets); still writes static_export/static/ and data/.",
    )
    parser.add_argument(
        "--frontend-only",
        action="store_true",
        dest="frontend_only",
        help=(
            "Only rebuild the SPA: delete site-root index.html and assets/, run Vite, "
            "refresh export_manifest.json. Leaves static/ and data/ unchanged."
        ),
    )
    args = parser.parse_args()

    if args.clean and args.frontend_only:
        parser.error("--clean cannot be used with --frontend-only (use --frontend-only alone to replace SPA files).")
    if args.skip_frontend_build and args.frontend_only:
        parser.error("--skip-frontend-build conflicts with --frontend-only.")

    site_root = Path(args.output)

    if args.frontend_only:
        site_root.mkdir(parents=True, exist_ok=True)
        print("Frontend-only: clearing old Vite output...")
        clear_site_frontend_artifacts(site_root)
        print("Building static frontend...")
        build_static_frontend(site_root)
        write_export_manifest(site_root)
        spa_bytes = sum(
            f.stat().st_size
            for f in site_root.rglob("*")
            if f.is_file()
            and "data" not in f.relative_to(site_root).parts[:1]
            and "static" not in f.relative_to(site_root).parts[:1]
        )
        print(f"\nDone (frontend only). Site-root SPA + manifest ~ {spa_bytes / (1024 * 1024):.2f} MB under {site_root}")
        return

    if args.clean and site_root.exists():
        shutil.rmtree(site_root)
    site_root.mkdir(parents=True, exist_ok=True)
    assets_dir = site_root / STATIC_ASSETS_DIR_NAME
    data_root = site_root / DATA_DIR_NAME
    assets_dir.mkdir(parents=True, exist_ok=True)
    data_root.mkdir(parents=True, exist_ok=True)

    # Read MapTiler key if available.
    maptiler_key_file = Path(".maptiler_key")
    maptiler_key = (
        maptiler_key_file.read_text().strip()
        if maptiler_key_file.is_file()
        else os.getenv("MAPTILER_API_KEY", "")
    )
    print("Exporting config...")
    export_config(assets_dir, maptiler_key)

    print("Exporting ZIP directory...")
    export_zip_directory(assets_dir, _PROJECT_ROOT / "backend" / "zip_lookup.csv")

    print("Exporting data layers...")
    export_data_layers(data_root)

    print("Exporting forecast data layers...")
    export_forecast_data_layers(data_root)

    print("Exporting model grids (for static stats queries)...")
    export_model_grids(data_root)

    print("Exporting tiles...")
    export_tiles(assets_dir)

    print("Exporting legend ranges (for client map download)...")
    export_value_ranges(assets_dir)

    if not args.skip_frontend_build:
        print("Building static frontend...")
        build_static_frontend(site_root)

    write_export_manifest(site_root)

    total_size = sum(f.stat().st_size for f in site_root.rglob("*") if f.is_file())
    print(f"\nDone. Total export: {total_size / (1024 * 1024):.1f} MB in {site_root}")


if __name__ == "__main__":
    main()
