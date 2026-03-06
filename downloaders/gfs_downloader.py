#!/usr/bin/env python3

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from base import BaseDownloader
from lead_config import FORECAST_HOURS

# Fixed 12z cycle for GFS; used in remote paths and local dir names.
GFS_CYCLE = 12
GFS_VARIABLE = "APCP"


@dataclass(frozen=True)
class DownloadTask:
    init_date: datetime
    fhour: int
    level: str


class GFSFilteredDownloaderParallel(BaseDownloader):
    def __init__(
        self,
        output_dir: str = "model_data/gfs",
        max_workers: int = 16,
        max_retries: int = 3,
        timeout_seconds: int = 120,
        polite_delay_seconds: float = 0.0,
    ):
        super().__init__(
            output_dir,
            max_workers=max_workers,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            polite_delay_seconds=polite_delay_seconds,
            base_url="https://noaa-gfs-bdp-pds.s3.amazonaws.com",
            user_agent="GFSFilteredDownloaderParallel/1.0",
        )

    def _status_key(self, status: str) -> str:
        if status.startswith("downloaded"):
            return "downloaded"
        if status.startswith("failed"):
            return "failed"
        return status

    @staticmethod
    def _to_dt(d) -> datetime:
        if isinstance(d, str):
            return datetime.strptime(d, "%Y-%m-%d")
        return d

    def _paths(self, init_date: datetime, fhour: int) -> tuple[str, str, str]:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{GFS_CYCLE:02d}"
        grib_filename = f"gfs.t{cycle_str}z.pgrb2.0p25.f{fhour:03d}"
        idx_filename = f"{grib_filename}.idx"
        grib_path = f"gfs.{date_str}/{cycle_str}/atmos/{grib_filename}"
        idx_path = f"gfs.{date_str}/{cycle_str}/atmos/{idx_filename}"
        return grib_filename, grib_path, idx_path

    def _output_file(self, init_date: datetime, fhour: int, level: str) -> Path:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{GFS_CYCLE:02d}"
        safe_level = level.replace(" ", "_")
        out_dir = self.output_dir / str(init_date.year) / f"{date_str}_{cycle_str}z"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"f{fhour:03d}_{safe_level}.grib2"

    def _find_byte_range(self, idx_text: str, level: str):
        lines = idx_text.strip().split("\n")
        start_byte = None
        end_byte = None
        for i, line in enumerate(lines):
            parts = line.split(":")
            if len(parts) < 5:
                continue
            var_code = parts[3].strip()
            lvl_desc = parts[4].strip()
            if var_code == GFS_VARIABLE and lvl_desc == level:
                start_byte = int(parts[1])
                if i + 1 < len(lines):
                    next_parts = lines[i + 1].split(":")
                    end_byte = int(next_parts[1]) - 1
                break
        return start_byte, end_byte

    def _download_task(self, task: DownloadTask) -> tuple[DownloadTask, str]:
        init_date, fhour, level = (
            task.init_date,
            task.fhour,
            task.level,
        )
        out_file = self._output_file(init_date, fhour, level)
        if out_file.exists():
            return task, "exists"

        grib_filename, grib_path, idx_path = self._paths(init_date, fhour)
        idx_url = f"{self.base_url}/{idx_path}"
        grib_url = f"{self.base_url}/{grib_path}"
        last_err = None

        if self.polite_delay_seconds:
            time.sleep(self.polite_delay_seconds)

        for attempt in range(1, self.max_retries + 1):
            part = out_file.with_suffix(out_file.suffix + ".part")
            try:
                idx_resp = self.session.get(idx_url, timeout=self.timeout_seconds)
                if idx_resp.status_code == 404:
                    return task, "not_found_idx"
                idx_resp.raise_for_status()
                start_byte, end_byte = self._find_byte_range(idx_resp.text, level)
                if start_byte is None:
                    return task, "not_found_var"
                headers = {"Range": f"bytes={start_byte}-{end_byte}"} if end_byte is not None else {"Range": f"bytes={start_byte}-"}
                resp = self.session.get(grib_url, headers=headers, stream=True, timeout=self.timeout_seconds)
                resp.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                part.replace(out_file)
                size_kb = out_file.stat().st_size / 1024
                return task, f"downloaded ({size_kb:.1f} KB)"
            except Exception as e:
                last_err = e
                if part.exists():
                    part.unlink(missing_ok=True)
                if out_file.exists():
                    out_file.unlink(missing_ok=True)
                time.sleep(1.25 * attempt)
        return task, f"failed: {last_err}"

    def download_date_range(
        self,
        start_date: str | datetime,
        end_date: str | datetime,
        *,
        forecast_hours: list[int] = FORECAST_HOURS,
        level: str = "surface",
    ):
        start = self._to_dt(start_date)
        end = self._to_dt(end_date)
        return self._download(start, end, forecast_hours=forecast_hours, level=level)

    def download_year_range(
        self,
        start_year: int,
        end_year: int,
        *,
        forecast_hours: list[int] = FORECAST_HOURS,
        level: str = "surface",
    ):
        start = datetime(int(start_year), 1, 1)
        end = datetime(int(end_year), 12, 31)
        return self._download(start, end, forecast_hours=forecast_hours, level=level)

    def _download(
        self,
        start: datetime,
        end: datetime,
        *,
        forecast_hours: list[int] = FORECAST_HOURS,
        level: str = "surface",
    ):
        init_dates: list[datetime] = []
        cur = datetime(start.year, start.month, start.day)
        end0 = datetime(end.year, end.month, end.day)
        while cur <= end0:
            init_dates.append(cur)
            cur += timedelta(days=1)
        if not init_dates:
            raise ValueError("No init dates selected. Check start/end dates.")

        tasks: list[DownloadTask] = []
        for d in init_dates:
            for fh in forecast_hours:
                tasks.append(DownloadTask(d, int(fh), str(level)))

        print("\n" + "=" * 70)
        print("Parallel GFS filtered download (idx + Range)")
        print(f"Period: {start.date()} to {end.date()} | Daily")
        print(f"Cycle: {GFS_CYCLE:02d}z")
        print(f"Forecast hours: {forecast_hours}")
        print(f"Variable: {GFS_VARIABLE} | Level: {level}")
        print(f"Workers: {self.max_workers} | Retries: {self.max_retries}")
        print(f"Output: {self.output_dir.resolve()}")
        print("=" * 70)

        results = self._run_parallel(
            tasks,
            self._download_task,
            description="GFS download",
            progress_interval=25,
        )

        examples: list[str] = []
        for task, status in results:
            if status == "not_found_idx" and len(examples) < 5:
                examples.append(f"Missing idx: {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d}")
            elif status == "not_found_var" and len(examples) < 5:
                examples.append(f"Var not found: {GFS_VARIABLE} @ {task.level} in {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d}")
            elif status.startswith("failed") and len(examples) < 5:
                examples.append(f"Failed: {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d} ({GFS_VARIABLE}@{task.level}) -> {status}")
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print("-" * 70 + "\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download GFS APCP forecasts")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD). Defaults to start date.")
    parser.add_argument("--start-year", type=int, help="Start year (downloads full years)")
    parser.add_argument("--end-year", type=int, help="End year (downloads full years)")
    parser.add_argument("--level", default="surface")
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    downloader = GFSFilteredDownloaderParallel(
        output_dir="model_data/gfs",
        max_workers=args.workers,
        max_retries=3,
        timeout_seconds=120,
        polite_delay_seconds=0.0,
    )

    if args.start_year:
        downloader.download_year_range(
            start_year=args.start_year,
            end_year=args.end_year or args.start_year,
            level=args.level,
            forecast_hours=FORECAST_HOURS,
        )
    else:
        start = args.start_date or datetime.utcnow().strftime("%Y-%m-%d")
        end = args.end_date or start
        downloader.download_date_range(
            start_date=start,
            end_date=end,
            level=args.level,
            forecast_hours=FORECAST_HOURS,
        )
