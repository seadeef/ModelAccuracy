#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import re
import sys
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

_this_dir = Path(__file__).resolve().parent
_project_root = _this_dir.parent
sys.path.insert(0, str(_project_root))

import numpy as np

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from model_registry import MODEL_REGISTRY, DEFAULT_MODEL
from statistics_plugins.registry import STATISTICS_BY_NAME
from backend.stats_query import stats_at_point

MAPTILER_API_KEY = os.getenv("MAPTILER_API_KEY", "")
MAPTILER_STYLE_ID = "streets-v2"
ZIP_LOOKUP_CSV = _this_dir / os.getenv("ZIP_LOOKUP_CSV", "zip_lookup.csv")
TILES_OUTPUT = _project_root / os.getenv("TILES_OUTPUT", "tiles_output")
US_CROP_BOUNDS = [-125.125, 23.875, -65.875, 50.125]  # pixel-snapped to 0.25° GFS grid

app = FastAPI(title="Model Statistics Query API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://127.0.0.1:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve pre-rendered PNG images for image-mode statistics.
_images_dir = TILES_OUTPUT / "images"
if _images_dir.exists():
    app.mount("/images", StaticFiles(directory=str(_images_dir)), name="images")


def _normalize_zip(zip_code: str) -> str | None:
    digits = re.sub(r"\D", "", zip_code)
    if len(digits) < 5:
        return None
    return digits[:5]


@lru_cache(maxsize=1)
def _load_zip_lookup(path_str: str) -> dict[str, dict[str, float]]:
    path = Path(path_str)
    if not path.exists():
        return {}

    lookup: dict[str, dict[str, float]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            zip_code = _normalize_zip(row.get("zip", ""))
            if not zip_code:
                continue
            try:
                lookup[zip_code] = {
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "min_lon": float(row["min_lon"]),
                    "min_lat": float(row["min_lat"]),
                    "max_lon": float(row["max_lon"]),
                    "max_lat": float(row["max_lat"]),
                }
            except (KeyError, TypeError, ValueError):
                continue
    return lookup


STATS_ROOT = _project_root / "stats_output"
SEASONS = {"djf": (12, 1, 2), "mam": (3, 4, 5), "jja": (6, 7, 8), "son": (9, 10, 11)}


def _current_month_str() -> str:
    return f"{datetime.now(timezone.utc).month:02d}"


def _current_season_str() -> str:
    month = datetime.now(timezone.utc).month
    for name, months in SEASONS.items():
        if month in months:
            return name
    return "djf"


def _get_forecast_init_date(model_key: str) -> str | None:
    meta_path = STATS_ROOT / model_key / "forecast" / "metadata.npz"
    if not meta_path.exists():
        return None
    try:
        meta = np.load(meta_path, allow_pickle=True)
        return str(meta["init_date"])
    except (KeyError, Exception):
        return None


@app.get("/api/stats")
def get_stats(
    model: str = Query(DEFAULT_MODEL),
    lead: str = Query(..., pattern=r"^\d+([_-]\d+)?$"),
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    period: str = Query("yearly", pattern=r"^(yearly|monthly|seasonal)$"),
    month: str | None = Query(None, pattern=r"^\d{2}$"),
    season: str | None = Query(None, pattern=r"^(djf|mam|jja|son)$"),
):
    if model not in MODEL_REGISTRY:
        return {"error": f"Unknown model: {model}"}
    lead_key = lead
    stats_path = STATS_ROOT / model

    # Default month/season to current if not specified.
    if period == "monthly" and month is None:
        month = _current_month_str()
    if period == "seasonal" and season is None:
        season = _current_season_str()

    values = stats_at_point(
        lat, lon, lead_key, stats_root=stats_path,
        period=period, month=month, season=season,
    )
    return {
        "model": model,
        "lead": lead_key,
        "lat": lat,
        "lon": lon,
        "period": period,
        "month": month,
        "season": season,
        "stats": values,
    }


@app.get("/api/config")
def get_config():
    stat_info = {}
    for name, plugin in STATISTICS_BY_NAME.items():
        stat_info[name] = {}

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

    return {
        "models": models,
        "default_model": DEFAULT_MODEL,
        "maptiler_api_key": MAPTILER_API_KEY,
        "maptiler_style_id": MAPTILER_STYLE_ID,
        "statistics": stat_info,
        "default_statistic": "forecast",
        "image_bounds": US_CROP_BOUNDS,
        "forecast_init_date": forecast_init_dates,
        "accumulation_modes": ["yearly", "monthly", "seasonal"],
        "current_month": _current_month_str(),
        "current_season": _current_season_str(),
    }


@app.get("/api/zip")
def get_zip(zip_code: str = Query(..., alias="zip")):
    normalized = _normalize_zip(zip_code)
    if normalized is None:
        return {"zip": zip_code, "found": False, "error": "Invalid ZIP format."}

    lookup = _load_zip_lookup(str(ZIP_LOOKUP_CSV))
    record = lookup.get(normalized)
    if record is None:
        return {"zip": normalized, "found": False}

    return {
        "zip": normalized,
        "found": True,
        "lat": record["lat"],
        "lon": record["lon"],
        "bounds": [
            record["min_lon"],
            record["min_lat"],
            record["max_lon"],
            record["max_lat"],
        ],
    }


# ---------------------------------------------------------------------------
# Export image endpoint
# ---------------------------------------------------------------------------

_STAT_LABELS = {
    "forecast": "Forecast", "bias": "Bias", "sacc": "SACC",
    "nrmse": "NRMSE", "nmad": "NMAD",
}
_MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
_SEASON_LABELS = {
    "djf": "Winter (DJF)", "mam": "Spring (MAM)",
    "jja": "Summer (JJA)", "son": "Autumn (SON)",
}


def _get_font(size: int):
    """Load a TrueType font, falling back to Pillow's built-in default."""
    from PIL import ImageFont

    for fp in [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNSText.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/TTF/DejaVuSans.ttf",
    ]:
        try:
            return ImageFont.truetype(fp, size)
        except (OSError, IOError):
            continue
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _compute_export_value_range(
    layer_paths: list[Path],
    field: str,
    colormap: str,
    fixed_range: tuple[float, float] | None,
) -> tuple[float, float]:
    """Compute color scale range from NPZ files (same logic as compute_tiles)."""
    if fixed_range is not None:
        return fixed_range
    is_diverging = colormap in ("diverging", "diverging_reversed")
    if not layer_paths:
        return (-1.0, 1.0) if is_diverging else (0.0, 1.0)

    finite_parts: list[np.ndarray] = []
    for path in layer_paths:
        try:
            with np.load(path) as data:
                if field in data:
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


def _make_gradient_bar(width: int, height: int, colormap: str):
    """Create a gradient color bar as a PIL Image."""
    from PIL import Image as PILImage

    blue = np.array([44, 123, 182], dtype=np.float64)
    white_c = np.array([255, 255, 255], dtype=np.float64)
    red = np.array([215, 25, 28], dtype=np.float64)

    bar = np.zeros((height, width, 3), dtype=np.uint8)
    for x in range(width):
        t = x / max(width - 1, 1)
        if colormap == "diverging":
            if t <= 0.5:
                frac = t / 0.5
                c = blue + (white_c - blue) * frac
            else:
                frac = (t - 0.5) / 0.5
                c = white_c + (red - white_c) * frac
        elif colormap == "diverging_reversed":
            if t <= 0.5:
                frac = t / 0.5
                c = red + (white_c - red) * frac
            else:
                frac = (t - 0.5) / 0.5
                c = white_c + (blue - white_c) * frac
        else:
            c = white_c + (blue - white_c) * t
        bar[:, x] = np.clip(c, 0, 255).astype(np.uint8)

    return PILImage.fromarray(bar, "RGB")


def _format_legend_value(v: float) -> str:
    av = abs(v)
    if av >= 100:
        return f"{v:.0f}"
    if av >= 10:
        return f"{v:.1f}"
    return f"{v:.2f}"


@app.get("/api/export-image")
def export_image(
    model: str = Query(DEFAULT_MODEL),
    statistic: str = Query(...),
    lead: str = Query(..., pattern=r"^\d+([_]\d+)?$"),
    period: str = Query("yearly", pattern=r"^(yearly|monthly|seasonal)$"),
    month: str | None = Query(None, pattern=r"^\d{2}$"),
    season: str | None = Query(None, pattern=r"^(djf|mam|jja|son)$"),
):
    """Generate a downloadable PNG with title, map, and color legend."""
    from io import BytesIO
    from PIL import Image as PILImage, ImageDraw

    if model not in MODEL_REGISTRY:
        return JSONResponse({"error": f"Unknown model: {model}"}, status_code=400)
    if statistic not in STATISTICS_BY_NAME:
        return JSONResponse({"error": f"Unknown statistic: {statistic}"}, status_code=400)

    plugin = STATISTICS_BY_NAME[statistic]

    # Default month/season to current if not specified.
    if period == "monthly" and month is None:
        month = _current_month_str()
    if period == "seasonal" and season is None:
        season = _current_season_str()

    # Resolve tile PNG path.
    if period == "monthly" and month:
        tile_subdir = f"monthly/{month}"
    elif period == "seasonal" and season:
        tile_subdir = f"seasonal/{season}"
    else:
        tile_subdir = ""

    if tile_subdir:
        tile_path = _images_dir / model / statistic / tile_subdir / f"lead_{lead}.png"
    else:
        tile_path = _images_dir / model / statistic / f"lead_{lead}.png"

    if not tile_path.exists():
        return JSONResponse({"error": "Tile image not found"}, status_code=404)

    # Load tile image.
    tile_img = PILImage.open(tile_path).convert("RGBA")
    tile_w, tile_h = tile_img.size

    # Compute value range for legend from all leads in this period.
    stats_dir = STATS_ROOT / model / statistic
    layer_dir = stats_dir / tile_subdir if tile_subdir else stats_dir
    layer_paths = sorted(layer_dir.glob("lead_*.npz")) if layer_dir.exists() else []

    field = plugin.spec.render_field
    colormap = plugin.spec.colormap
    units = plugin.spec.units
    vmin, vmax = _compute_export_value_range(
        layer_paths, field, colormap, plugin.spec.fixed_range,
    )

    # Build title.
    model_label = MODEL_REGISTRY[model].label
    stat_label = _STAT_LABELS.get(statistic, statistic)
    if "_" in lead:
        parts = lead.split("_")
        lead_label = f"{parts[0]}\u2013{parts[1]} Day Average"
    else:
        lead_label = f"Day {lead}"

    if period == "monthly" and month:
        mi = int(month)
        period_label = _MONTH_NAMES[mi] if 1 <= mi <= 12 else month
    elif period == "seasonal" and season:
        period_label = _SEASON_LABELS.get(season, season.upper())
    else:
        period_label = "Yearly"

    title = f"{model_label} {stat_label} ({units})  \u2014  {lead_label}  \u2014  {period_label}"

    # Layout constants.
    MIN_WIDTH = 800
    TITLE_H = 48
    LEGEND_H = 70
    PADDING = 16  # horizontal padding around map

    # Scale the tile up so the canvas is at least MIN_WIDTH.
    scale = max(1.0, MIN_WIDTH / (tile_w + 2 * PADDING))
    scaled_w = round(tile_w * scale)
    scaled_h = round(tile_h * scale)
    if scale > 1.0:
        tile_img = tile_img.resize((scaled_w, scaled_h), PILImage.LANCZOS)

    img_w = scaled_w + 2 * PADDING
    img_h = TITLE_H + scaled_h + LEGEND_H

    # Canvas.
    canvas = PILImage.new("RGB", (img_w, img_h), (255, 255, 255))

    # Composite tile onto a light gray background (transparent areas become gray).
    map_bg = PILImage.new("RGB", (scaled_w, scaled_h), (240, 240, 240))
    map_bg.paste(tile_img, (0, 0), tile_img)
    canvas.paste(map_bg, (PADDING, TITLE_H))

    draw = ImageDraw.Draw(canvas)

    # Title — pick font size that fits the canvas width.
    for font_size in (20, 18, 16, 14, 12):
        title_font = _get_font(font_size)
        title_bbox = draw.textbbox((0, 0), title, font=title_font)
        title_tw = title_bbox[2] - title_bbox[0]
        if title_tw <= img_w - 2 * PADDING:
            break
    title_th = title_bbox[3] - title_bbox[1]
    draw.text(
        ((img_w - title_tw) // 2, (TITLE_H - title_th) // 2),
        title, fill=(0, 0, 0), font=title_font,
    )

    # Separator lines.
    draw.line([(0, TITLE_H - 1), (img_w, TITLE_H - 1)], fill=(200, 200, 200))
    draw.line([(0, TITLE_H + scaled_h), (img_w, TITLE_H + scaled_h)], fill=(200, 200, 200))

    # Legend: gradient bar.
    bar_w = min(int(img_w * 0.5), 400)
    bar_h = 16
    bar_x = (img_w - bar_w) // 2
    bar_y = TITLE_H + scaled_h + 12

    gradient = _make_gradient_bar(bar_w, bar_h, colormap)
    canvas.paste(gradient, (bar_x, bar_y))
    draw.rectangle(
        [bar_x - 1, bar_y - 1, bar_x + bar_w, bar_y + bar_h],
        outline=(120, 120, 120),
    )

    # Legend: value labels.
    label_font = _get_font(12)
    label_y = bar_y + bar_h + 4

    vmin_str = _format_legend_value(vmin)
    vmax_str = _format_legend_value(vmax)

    # Left label (vmin).
    vmin_bbox = draw.textbbox((0, 0), vmin_str, font=label_font)
    vmin_tw = vmin_bbox[2] - vmin_bbox[0]
    draw.text(
        (bar_x - vmin_tw // 2, label_y),
        vmin_str, fill=(60, 60, 60), font=label_font,
    )

    # Right label (vmax).
    vmax_bbox = draw.textbbox((0, 0), vmax_str, font=label_font)
    vmax_tw = vmax_bbox[2] - vmax_bbox[0]
    draw.text(
        (bar_x + bar_w - vmax_tw // 2, label_y),
        vmax_str, fill=(60, 60, 60), font=label_font,
    )

    # Center label (midpoint for diverging).
    if colormap in ("diverging", "diverging_reversed"):
        mid = (vmin + vmax) / 2.0
        mid_str = _format_legend_value(mid)
        mid_bbox = draw.textbbox((0, 0), mid_str, font=label_font)
        mid_tw = mid_bbox[2] - mid_bbox[0]
        draw.text(
            (bar_x + bar_w // 2 - mid_tw // 2, label_y),
            mid_str, fill=(60, 60, 60), font=label_font,
        )

    # Units label (centered below value labels).
    units_y = label_y + 16
    units_bbox = draw.textbbox((0, 0), units, font=label_font)
    units_tw = units_bbox[2] - units_bbox[0]
    draw.text(
        ((img_w - units_tw) // 2, units_y),
        units, fill=(120, 120, 120), font=label_font,
    )

    # Serialize to PNG.
    buf = BytesIO()
    canvas.save(buf, format="PNG", optimize=True)
    buf.seek(0)

    # Filename for download.
    lead_tag = lead.replace("_", "-")
    if period == "monthly" and month:
        period_tag = f"month{month}"
    elif period == "seasonal" and season:
        period_tag = season
    else:
        period_tag = "yearly"
    filename = f"{model}_{statistic}_lead{lead_tag}_{period_tag}.png"

    return StreamingResponse(
        buf,
        media_type="image/png",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
