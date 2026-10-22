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
    python download.py --ar                                            # index tARget v4 ERA5 data
    python download.py --ar --start-date 2025-01-01 --end-date 2025-01-31
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from atmospheric_rivers import TARGET_AR_DATASET_URL, download_default_dataset
from model_registry import MODEL_REGISTRY, DEFAULT_MODEL, ModelConfig
from downloaders.prism_downloader import PRISMDownloaderParallel


def _dir_has_no_regular_files(d: Path) -> bool:
    """True if *d* exists but contains no regular file anywhere below it.

    A failed day can leave an init dir holding only empty ``_apcp``/``_inst``/
    ``_6h`` subdirs; ``rglob`` walks past those so we can tell 'nothing was
    written' from 'a subdir exists'.
    """
    return d.exists() and not any(p.is_file() for p in d.rglob("*"))


def _find_latest_model_date(config: ModelConfig) -> datetime | None:
    """Most recent init date on disk that actually holds assembled data.

    Requires ≥1 ``f*_surface.npy`` — a bare init dir (mkdir'd before any byte
    landed, or left behind by a failed day) does not count, so a failed day
    can't poison catchup's start-date detection into skipping it permanently.
    """
    data_dir = Path(config.data_dir)
    if not data_dir.exists():
        return None
    for year_dir in sorted(data_dir.iterdir(), reverse=True):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for init_dir in sorted(year_dir.iterdir(), reverse=True):
            if (init_dir.is_dir()
                    and init_dir.name.endswith(f"_{config.cycle_hour:02d}z")
                    and next(init_dir.glob("f*_surface.npy"), None) is not None):
                date_str = init_dir.name.split("_")[0]
                return datetime.strptime(date_str, "%Y%m%d")
    return None


def _find_latest_prism_date() -> datetime | None:
    """Most recent PRISM date on disk with an extracted ``data.tif``.

    An empty day directory (mkdir'd before download, or left by a failed
    extraction) is ignored so it can't push catchup's start date past a day
    that was never actually downloaded.
    """
    prism_dir = Path("prism_data")
    if not prism_dir.exists():
        return None
    for year_dir in sorted(prism_dir.iterdir(), reverse=True):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for day_dir in sorted(year_dir.iterdir(), reverse=True):
            if (day_dir.is_dir() and len(day_dir.name) == 8 and day_dir.name.isdigit()
                    and (day_dir / "data.tif").exists()):
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


def _run_ar_download(
    *, start_date: str | None = None, end_date: str | None = None,
    force: bool = False,
) -> list[Path]:
    """Delegate acquisition to the canonical tARget adapter."""
    downloaded = download_default_dataset(
        start_date=start_date, end_date=end_date, force=force,
    )
    if start_date is None:
        print(
            f"\nUpdated {len(downloaded)} canonical tARget virtual source index. "
            "No frame data was requested.\n"
            "Cache a range with --start-date/--end-date or --start-year/--end-year."
        )
    else:
        print(
            f"\nUpdated {len(downloaded)} canonical tARget virtual source(s). Next run:\n"
            "  python3 compute_stats.py --only episodes\n"
            "  python3 export_static.py"
        )
    return downloaded


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


def _run_weathernext2_forecast(config: ModelConfig) -> None:
    """Forecast download for WeatherNext 2. Google publishes 00/06/12/18z inits
    on GCS; probe for whatever is available and pick the freshest complete cycle
    instead of hardcoding 12z.
    """
    cls = config.get_downloader_class()
    defaults = dict(config.downloader_defaults)
    downloader = cls(**defaults)

    today = datetime.now(timezone.utc) - timedelta(hours=config.publish_delay_hours)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    candidates = [today, today - timedelta(days=1), today - timedelta(days=2)]

    target = downloader.find_latest_forecast_init(candidates)
    if target is None:
        print(
            f"\nERROR: no WeatherNext 2 init published in {candidates[-1].date()}..{candidates[0].date()}. "
            f"Skipping extraction so a stale forecast is not republished."
        )
        return
    init_date, cycle = target
    print(f"\n--- WeatherNext 2 latest available init: {init_date.date()} {cycle:02d}z ---")

    if downloader.forecast_init_complete(config, init_date, cycle):
        print(f"Already downloaded ({init_date.date()} {cycle:02d}z).")
    else:
        if not downloader.download_forecast_init(init_date, cycle):
            print(
                f"\nERROR: WeatherNext 2 forecast download failed for {init_date.date()} {cycle:02d}z. "
                f"Skipping extraction so a stale forecast is not republished."
            )
            return

    output_root = Path("stats_output") / config.key
    print(f"\n--- Extracting forecast for model '{config.key}' ---")
    downloader.extract_forecast(
        config,
        init_date=init_date,
        cycle=cycle,
        output_root=output_root,
    )


def _run_forecast(config: ModelConfig) -> None:
    if config.key == "weathernext2":
        _run_weathernext2_forecast(config)
        return

    forecast_hours = config.forecast_hours
    today = datetime.now(timezone.utc) - timedelta(hours=config.publish_delay_hours)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    cls = config.get_downloader_class()
    defaults = dict(config.downloader_defaults)
    downloader = cls(**defaults)

    # "Has data" now means the init is *complete* (every variable × every lead),
    # so an interrupted download resumes instead of publishing partial leads.
    resolved_date = None
    if downloader.forecast_init_complete(config, today):
        print(f"\nForecast data for model '{config.key}' already downloaded ({today.date()}).")
        resolved_date = today
    elif downloader.forecast_init_complete(config, yesterday):
        print(f"\nForecast data for model '{config.key}' already downloaded ({yesterday.date()}).")
        resolved_date = yesterday
    else:
        for attempt_date in [today, yesterday]:
            date_str = attempt_date.strftime("%Y-%m-%d")
            print(f"\n--- Downloading forecast for model '{config.key}' ({date_str}) ---")
            downloader.download_date_range(
                start_date=date_str,
                end_date=date_str,
                forecast_hours=forecast_hours,
            )
            if downloader.forecast_init_complete(config, attempt_date):
                resolved_date = attempt_date
                break
            # Drop an init dir that never got a file (only empty _apcp/_inst/_6h
            # subdirs), which would otherwise be a permanent archive hole.
            empty_dir = downloader.init_dir_for(config, attempt_date)
            if _dir_has_no_regular_files(empty_dir):
                shutil.rmtree(empty_dir, ignore_errors=True)
            print(f"No forecast data available for {date_str}, trying previous day...")

    if resolved_date is None:
        print(
            f"\nERROR: forecast download failed for model '{config.key}' on both "
            f"{today.date()} and {yesterday.date()}. Skipping extraction so a stale "
            f"forecast is not republished."
        )
        return

    output_root = Path("stats_output") / config.key
    print(f"\n--- Extracting forecast for model '{config.key}' ---")
    downloader.extract_forecast(
        config,
        init_date=resolved_date,
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
    mode_group.add_argument(
        "--ar", action="store_true",
        help=f"Index/cache tARget v4 ERA5 data from {TARGET_AR_DATASET_URL}",
    )

    parser.add_argument("--start-date",
                        help="Start date (YYYY-MM-DD). Model/PRISM default: today; AR default: index only")
    parser.add_argument("--end-date",
                        help="End date (YYYY-MM-DD). Defaults to start date when one is supplied")
    parser.add_argument("--no-prism", action="store_true",
                        help="Skip PRISM observation download")
    parser.add_argument("--start-year", type=int,
                        help="Start year (downloads full years)")
    parser.add_argument("--end-year", type=int,
                        help="End year (downloads full years)")
    parser.add_argument(
        "--force", action="store_true",
        help="With --ar and an explicit range, redownload blocks touched by that range",
    )
    args = parser.parse_args()

    if args.catchup and (args.start_date or args.end_date or args.start_year or args.end_year):
        parser.error("--catchup cannot be combined with explicit date/year ranges")
    if args.forecast and (args.start_date or args.end_date or args.start_year or args.end_year):
        parser.error("--forecast cannot be combined with explicit date/year ranges")
    if args.ar:
        if args.model is not None or args.no_prism:
            parser.error("--ar cannot be combined with model or PRISM options")
        if args.start_year and (args.start_date or args.end_date):
            parser.error("Use either AR date options or AR year options, not both")
        if args.end_year and not args.start_year:
            parser.error("--end-year requires --start-year")
        if args.start_year:
            ar_start = f"{args.start_year:04d}-01-01"
            ar_end = f"{(args.end_year or args.start_year):04d}-12-31"
        elif args.start_date or args.end_date:
            ar_start = args.start_date or args.end_date
            ar_end = args.end_date or args.start_date
        else:
            ar_start = ar_end = None
        try:
            _run_ar_download(start_date=ar_start, end_date=ar_end, force=args.force)
        except (ValueError, OSError) as exc:
            parser.error(str(exc))
        return

    if args.force:
        parser.error("--force is currently supported only with --ar")

    models = _resolve_models(args)

    if args.catchup:
        _run_catchup(models)
    elif args.forecast:
        for config in models:
            _run_forecast(config)
    else:
        if not args.no_prism:
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
