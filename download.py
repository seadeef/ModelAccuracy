#!/usr/bin/env python3
"""Unified model data downloader.

Usage examples:
    python download.py --model gfs --start-date 2025-01-01 --end-date 2025-01-31
    python download.py --model gfs --start-year 2024 --end-year 2025
    python download.py --forecast-only                     # all models, today's forecast
    python download.py --forecast-only --model gfs         # just GFS
    python download.py --all --start-date 2025-01-01 --end-date 2025-01-31
    python download.py --no-prism --model gfs --start-date 2025-01-01  # skip PRISM
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from model_registry import MODEL_REGISTRY, DEFAULT_MODEL, ModelConfig
from downloaders.prism_downloader import PRISMDownloaderParallel


def _resolve_models(args: argparse.Namespace) -> list[ModelConfig]:
    if args.all or (args.forecast_only and args.model is None):
        return list(MODEL_REGISTRY.values())
    key = args.model or DEFAULT_MODEL
    if key not in MODEL_REGISTRY:
        print(f"Unknown model: {key}. Available: {', '.join(sorted(MODEL_REGISTRY))}")
        sys.exit(1)
    return [MODEL_REGISTRY[key]]


def _run_prism(args: argparse.Namespace) -> None:
    """Download PRISM observations for the same date range as model data."""
    dl = PRISMDownloaderParallel(
        output_dir="prism_data",
        max_workers=args.workers or 12,
        max_retries=3,
        timeout_seconds=60,
        remove_zip_after_extract=True,
    )
    if args.start_year:
        dl.download_year_range(
            start_year=args.start_year,
            end_year=args.end_year or args.start_year,
        )
    else:
        start = args.start_date or datetime.utcnow().strftime("%Y-%m-%d")
        end = args.end_date or start
        dl.download_date_range(start_date=start, end_date=end)


def _run_download(config: ModelConfig, args: argparse.Namespace) -> None:
    cls = config.get_downloader_class()
    defaults = dict(config.downloader_defaults)
    if args.workers is not None:
        defaults["max_workers"] = args.workers
    downloader = cls(**defaults)
    forecast_hours = config.forecast_hours
    level = getattr(args, "level", "surface")

    if args.start_year:
        downloader.download_year_range(
            start_year=args.start_year,
            end_year=args.end_year or args.start_year,
            forecast_hours=forecast_hours,
            level=level,
        )
    else:
        start = args.start_date or datetime.utcnow().strftime("%Y-%m-%d")
        end = args.end_date or start
        downloader.download_date_range(
            start_date=start,
            end_date=end,
            forecast_hours=forecast_hours,
            level=level,
        )


def _run_forecast(config: ModelConfig, args: argparse.Namespace) -> None:
    cls = config.get_downloader_class()
    defaults = dict(config.downloader_defaults)
    if args.workers is not None:
        defaults["max_workers"] = args.workers
    downloader = cls(**defaults)
    forecast_hours = config.forecast_hours

    today = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"\n--- Downloading forecast for model '{config.key}' (today: {today}) ---")
    downloader.download_date_range(
        start_date=today,
        end_date=today,
        forecast_hours=forecast_hours,
    )

    output_root = Path("stats_output") / config.key
    print(f"\n--- Extracting forecast for model '{config.key}' ---")
    downloader.extract_forecast(
        forecast_hours=forecast_hours,
        lead_windows=list(config.lead_windows),
        output_root=output_root,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unified model data downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available models: {', '.join(sorted(MODEL_REGISTRY))}",
    )
    parser.add_argument("--model", default=None,
                        help=f"Model to download (default: {DEFAULT_MODEL})")
    parser.add_argument("--all", action="store_true",
                        help="Download for all registered models")
    parser.add_argument("--forecast-only", action="store_true",
                        help="Download today's forecast for each model through its max lead time, "
                             "then extract into stats format")
    parser.add_argument("--no-prism", action="store_true",
                        help="Skip PRISM observation download")
    parser.add_argument("--start-date",
                        help="Start date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--end-date",
                        help="End date (YYYY-MM-DD). Defaults to start date.")
    parser.add_argument("--start-year", type=int,
                        help="Start year (downloads full years)")
    parser.add_argument("--end-year", type=int,
                        help="End year (downloads full years)")
    parser.add_argument("--level", default="surface",
                        help="GFS level (default: surface)")
    parser.add_argument("--workers", type=int, default=None,
                        help="Override max download workers")
    args = parser.parse_args()

    models = _resolve_models(args)

    if args.forecast_only:
        for config in models:
            _run_forecast(config, args)
    else:
        if not args.no_prism:
            print("\n=== Downloading PRISM observations ===")
            _run_prism(args)
        for config in models:
            print(f"\n=== Downloading model '{config.key}' ===")
            _run_download(config, args)


if __name__ == "__main__":
    main()
