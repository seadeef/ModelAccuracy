#!/usr/bin/env python3

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta
from downloaders.base import BaseDownloader
from lead_config import FORECAST_HOURS

# Fixed 12z cycle for GFS; used in remote paths and local dir names.
GFS_CYCLE = 12
WEEKDAY_NAMES = [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
]


@dataclass(frozen=True)
class DownloadTask:
    init_date: datetime
    fhour: int
    variable: str
    level: str


class GFSFilteredDownloaderParallel(BaseDownloader):
    def __init__(
        self,
        output_dir: str = "model_data/gfs",
        max_workers: int = 8,
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
            user_agent="GFSFilteredDownloaderParallel/1.0 (+https://chatgpt.com)",
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

    @staticmethod
    def _normalize_weekdays(download_weekdays: list[int] | None) -> set[int] | None:
        if not download_weekdays:
            return None
        normalized: set[int] = set()
        for day in download_weekdays:
            if day < 0 or day > 6:
                raise ValueError(f"Weekday integers must be 0..6, got {day}")
            normalized.add(day)
        return normalized

    def _paths(self, init_date: datetime, fhour: int) -> tuple[str, str, str]:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{GFS_CYCLE:02d}"
        grib_filename = f"gfs.t{cycle_str}z.pgrb2.0p25.f{fhour:03d}"
        idx_filename = f"{grib_filename}.idx"
        grib_path = f"gfs.{date_str}/{cycle_str}/atmos/{grib_filename}"
        idx_path = f"gfs.{date_str}/{cycle_str}/atmos/{idx_filename}"
        return grib_filename, grib_path, idx_path

    def _output_file(self, init_date: datetime, fhour: int, variable: str, level: str) -> Path:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{GFS_CYCLE:02d}"
        safe_level = level.replace(" ", "_")
        out_dir = self.output_dir / str(variable) / str(init_date.year) / f"{date_str}_{cycle_str}z"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"f{fhour:03d}_{safe_level}.grib2"

    def _find_byte_range(self, idx_text: str, variable: str, level: str):
        lines = idx_text.strip().split("\n")
        start_byte = None
        end_byte = None
        for i, line in enumerate(lines):
            parts = line.split(":")
            if len(parts) < 5:
                continue
            var_code = parts[3].strip()
            lvl_desc = parts[4].strip()
            if var_code == variable and lvl_desc == level:
                start_byte = int(parts[1])
                if i + 1 < len(lines):
                    next_parts = lines[i + 1].split(":")
                    end_byte = int(next_parts[1]) - 1
                break
        return start_byte, end_byte

    def _download_task(self, task: DownloadTask) -> tuple[DownloadTask, str]:
        init_date, fhour, variable, level = (
            task.init_date,
            task.fhour,
            task.variable,
            task.level,
        )
        out_file = self._output_file(init_date, fhour, variable, level)
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
                start_byte, end_byte = self._find_byte_range(idx_resp.text, variable, level)
                if start_byte is None:
                    return task, "not_found_var"
                headers = {"Range": f"bytes={start_byte}-{end_byte}"} if end_byte is not None else {"Range": f"bytes={start_byte}-"}
                resp = self.session.get(grib_url, headers=headers, stream=True, timeout=self.timeout_seconds)
                resp.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 64):
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

    def download_year_range(
        self,
        start_year: int,
        end_year: int,
        *,
        forecast_hours: list[int] = FORECAST_HOURS,
        variables: list[str] = ["APCP"],
        level: str = "surface",
        download_weekdays: list[int] | None = None,
    ):
        start = datetime(int(start_year), 1, 1)
        end = datetime(int(end_year), 12, 31)
        weekdays = self._normalize_weekdays(download_weekdays)
        init_dates: list[datetime] = []
        cur = datetime(start.year, start.month, start.day)
        end0 = datetime(end.year, end.month, end.day)
        while cur <= end0:
            if weekdays is None or cur.weekday() in weekdays:
                init_dates.append(cur)
            cur += timedelta(days=1)
        if not init_dates:
            raise ValueError("No init dates selected. Check start/end years and download_weekdays.")

        tasks: list[DownloadTask] = []
        for d in init_dates:
            for fh in forecast_hours:
                for var in variables:
                    tasks.append(DownloadTask(d, int(fh), str(var), str(level)))

        print("\n" + "=" * 70)
        print("Parallel GFS filtered download (idx + Range)")
        if weekdays is None:
            cadence = "Daily"
        else:
            selected = ", ".join(WEEKDAY_NAMES[idx] for idx in sorted(weekdays))
            cadence = f"Selected weekdays: {selected}"
        print(f"Period: {start.date()} to {end.date()} | {cadence}")
        print(f"Cycle: {GFS_CYCLE:02d}z")
        print(f"Forecast hours: {forecast_hours}")
        print(f"Variables: {variables} | Level: {level}")
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
                examples.append(f"Var not found: {task.variable} @ {task.level} in {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d}")
            elif status.startswith("failed") and len(examples) < 5:
                examples.append(f"Failed: {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d} ({task.variable}@{task.level}) -> {status}")
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print("-" * 70 + "\n")


if __name__ == "__main__":
    downloader = GFSFilteredDownloaderParallel(
        output_dir="model_data/gfs",
        max_workers=8,
        max_retries=3,
        timeout_seconds=120,
        polite_delay_seconds=0.0,
    )
    downloader.download_year_range(
        start_year=2022,
        end_year=2024,
        variables=["APCP"],
        level="surface",
        forecast_hours=FORECAST_HOURS,
        download_weekdays=[0, 3],
    )
