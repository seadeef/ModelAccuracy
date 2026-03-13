#!/usr/bin/env python3
"""Unified model data downloader.

Usage examples:
    python download.py --start-date 2025-01-01 --end-date 2025-01-31   # all models + PRISM
    python download.py --model gfs --start-date 2025-01-01             # just GFS + PRISM
    python download.py --model gfs --start-year 2024 --end-year 2025
    python download.py --forecast                                      # all models, today's forecast
    python download.py --forecast --model gfs                          # just GFS
    python download.py --catchup                                       # fill gap through yesterday
    python download.py --catchup --model gfs                           # just GFS + PRISM
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from model_registry import MODEL_REGISTRY, DEFAULT_MODEL, ModelConfig
from downloaders.prism_downloader import PRISMDownloaderParallel


def _find_latest_model_date(config: ModelConfig) -> datetime | None:
    """Find the most recent init date on disk for a model."""
    data_dir = Path(config.data_dir)
    if not data_dir.exists():
        return None
    for year_dir in sorted(data_dir.iterdir(), reverse=True):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for init_dir in sorted(year_dir.iterdir(), reverse=True):
            if init_dir.is_dir() and init_dir.name.endswith(f"_{config.cycle_hour:02d}z"):
                date_str = init_dir.name.split("_")[0]
                return datetime.strptime(date_str, "%Y%m%d")
    return None


def _find_latest_prism_date() -> datetime | None:
    """Find the most recent PRISM date on disk."""
    prism_dir = Path("prism_data")
    if not prism_dir.exists():
        return None
    for year_dir in sorted(prism_dir.iterdir(), reverse=True):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for day_dir in sorted(year_dir.iterdir(), reverse=True):
            if day_dir.is_dir() and len(day_dir.name) == 8 and day_dir.name.isdigit():
                return datetime.strptime(day_dir.name, "%Y%m%d")
    return None


def _resolve_models(args: argparse.Namespace) -> list[ModelConfig]:
    if args.model is None:
        return list(MODEL_REGISTRY.values())
    return [MODEL_REGISTRY[args.model]]


def _run_prism(start_date: str, end_date: str) -> None:
    """Download PRISM observations for the given date range."""
    dl = PRISMDownloaderParallel(
        output_dir="prism_data",
        max_retries=3,
        timeout_seconds=60,
        remove_zip_after_extract=True,
    )
    dl.download_date_range(start_date=start_date, end_date=end_date)


def _run_prism_years(start_year: int, end_year: int) -> None:
    """Download PRISM observations for the given year range."""
    dl = PRISMDownloaderParallel(
        output_dir="prism_data",
        max_retries=3,
        timeout_seconds=60,
        remove_zip_after_extract=True,
    )
    dl.download_year_range(start_year=start_year, end_year=end_year)


def _run_download(config: ModelConfig, args: argparse.Namespace) -> None:
    cls = config.get_downloader_class()
    defaults = dict(config.downloader_defaults)
    downloader = cls(**defaults)
    forecast_hours = config.forecast_hours

    if args.start_year:
        downloader.download_year_range(
            start_year=args.start_year,
            end_year=args.end_year or args.start_year,
            forecast_hours=forecast_hours,
        )
    else:
        start = args.start_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        end = args.end_date or start
        downloader.download_date_range(
            start_date=start,
            end_date=end,
            forecast_hours=forecast_hours,
        )


def _run_catchup(models: list[ModelConfig]) -> None:
    """Download missing historical data from last saved date through yesterday."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    latest_prism = _find_latest_prism_date()
    if latest_prism is None:
        print("ERROR: No existing PRISM data found. Cannot determine catchup start date.")
        print("Run with explicit --start-date first.")
        sys.exit(1)
    prism_start = (latest_prism + timedelta(days=1)).strftime("%Y-%m-%d")
    if prism_start <= yesterday:
        print(f"\n=== PRISM catchup: {prism_start} through {yesterday} ===")
        _run_prism(prism_start, yesterday)
    else:
        print(f"PRISM is up to date (latest: {latest_prism.date()})")

    for config in models:
        latest = _find_latest_model_date(config)
        if latest is None:
            print(f"ERROR: No existing data found for model '{config.key}'. "
                  f"Cannot determine catchup start date.")
            print("Run with explicit --start-date first.")
            sys.exit(1)
        model_start = (latest + timedelta(days=1)).strftime("%Y-%m-%d")
        if model_start <= yesterday:
            print(f"\n=== Model '{config.key}' catchup: {model_start} through {yesterday} ===")
            cls = config.get_downloader_class()
            defaults = dict(config.downloader_defaults)
            downloader = cls(**defaults)
            downloader.download_date_range(
                start_date=model_start,
                end_date=yesterday,
                forecast_hours=config.forecast_hours,
            )
        else:
            print(f"Model '{config.key}' is up to date (latest: {latest.date()})")


def _forecast_dir_has_data(config: ModelConfig, date: datetime) -> bool:
    """Check whether the init directory for the given date has any GRIB2 files."""
    date_str = date.strftime("%Y%m%d")
    init_dir = Path(config.data_dir) / str(date.year) / f"{date_str}_{config.cycle_hour:02d}z"
    return init_dir.exists() and any(init_dir.glob("f*_*.grib2"))


def _run_forecast(config: ModelConfig) -> None:
    forecast_hours = config.forecast_hours
    today = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=1)

    cls = config.get_downloader_class()
    defaults = dict(config.downloader_defaults)
    downloader = cls(**defaults)

    # Skip download if data already exists; still need to extract below.
    if _forecast_dir_has_data(config, today):
        print(f"\nForecast data for model '{config.key}' already downloaded ({today.date()}).")
    elif _forecast_dir_has_data(config, yesterday):
        print(f"\nForecast data for model '{config.key}' already downloaded ({yesterday.date()}).")
    else:
        # Try today first; if no files were downloaded, fall back to yesterday.
        for attempt_date in [today, yesterday]:
            date_str = attempt_date.strftime("%Y-%m-%d")
            print(f"\n--- Downloading forecast for model '{config.key}' ({date_str}) ---")
            downloader.download_date_range(
                start_date=date_str,
                end_date=date_str,
                forecast_hours=forecast_hours,
            )
            if _forecast_dir_has_data(config, attempt_date):
                break
            print(f"No forecast data available for {date_str}, trying previous day...")

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
    parser.add_argument("--model", default=None, choices=list(MODEL_REGISTRY),
                        help="Model to download (default: all models)")

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--forecast", action="store_true",
                            help="Download today's forecast for each model through its max lead time, "
                                 "then extract into stats format")
    mode_group.add_argument("--catchup", action="store_true",
                            help="Auto-detect last downloaded date and download through yesterday")

    parser.add_argument("--start-date",
                        help="Start date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--end-date",
                        help="End date (YYYY-MM-DD). Defaults to start date.")
    parser.add_argument("--start-year", type=int,
                        help="Start year (downloads full years)")
    parser.add_argument("--end-year", type=int,
                        help="End year (downloads full years)")
    args = parser.parse_args()

    if args.catchup and (args.start_date or args.end_date or args.start_year or args.end_year):
        parser.error("--catchup cannot be combined with explicit date/year ranges")
    if args.forecast and (args.start_date or args.end_date or args.start_year or args.end_year):
        parser.error("--forecast cannot be combined with explicit date/year ranges")

    models = _resolve_models(args)

    if args.catchup:
        _run_catchup(models)
    elif args.forecast:
        for config in models:
            _run_forecast(config)
    else:
        print("\n=== Downloading PRISM observations ===")
        if args.start_year:
            _run_prism_years(args.start_year, args.end_year or args.start_year)
        else:
            start = args.start_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
            end = args.end_date or start
            _run_prism(start, end)
        for config in models:
            print(f"\n=== Downloading model '{config.key}' ===")
            _run_download(config, args)


if __name__ == "__main__":
    main()
