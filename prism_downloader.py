#!/usr/bin/env python3
"""
Parallel PRISM daily downloader (time_series tree).
Downloads only PRISM ppt; output under prism_data/<year>/<YYYYMMDD>/.
"""

from __future__ import annotations

import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta

from downloaders.base import BaseDownloader

PRISM_VARIABLE = "ppt"


@dataclass(frozen=True)
class PRISMTask:
    date: datetime
    extract: bool


class PRISMDownloaderParallel(BaseDownloader):
    def __init__(
        self,
        output_dir: str | Path = "prism_data",
        max_workers: int = 6,
        max_retries: int = 3,
        timeout_seconds: int = 60,
        polite_delay_seconds: float = 0.0,
        remove_zip_after_extract: bool = False,
    ):
        super().__init__(
            output_dir,
            max_workers=max_workers,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            polite_delay_seconds=polite_delay_seconds,
            base_url="https://ftp.prism.oregonstate.edu/time_series/us/an/4km",
            user_agent="PRISMDownloaderParallel/1.0 (+https://chatgpt.com)",
        )
        self.remove_zip_after_extract = bool(remove_zip_after_extract)

    def _status_key(self, status: str) -> str:
        if status.startswith("failed"):
            return "failed"
        return status

    def _daily_filename(self, date: datetime) -> str:
        return f"prism_{PRISM_VARIABLE}_us_25m_{date:%Y%m%d}.zip"

    def _daily_url(self, date: datetime) -> str:
        return f"{self.base_url}/{PRISM_VARIABLE}/daily/{date.year}/{self._daily_filename(date)}"

    def _output_path(self, date: datetime) -> Path:
        date_dir = self.output_dir / str(date.year) / f"{date:%Y%m%d}"
        date_dir.mkdir(parents=True, exist_ok=True)
        return date_dir / "data.zip"

    def extract_zip(self, zip_file: Path, date: datetime) -> Path:
        extract_dir = zip_file.parent
        with zipfile.ZipFile(zip_file, "r") as z:
            z.extractall(extract_dir)
        prefix = f"prism_{PRISM_VARIABLE}_us_25m_{date:%Y%m%d}"
        for path in extract_dir.iterdir():
            if not path.is_file() or not path.name.startswith(prefix):
                continue
            suffix = path.name[len(prefix):]
            path.rename(path.with_name(f"data{suffix}"))
        return extract_dir

    def _download_one(self, task: PRISMTask) -> tuple[PRISMTask, str]:
        date, extract = task.date, task.extract
        url = self._daily_url(date)
        out = self._output_path(date)

        if out.exists():
            return task, "exists"

        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                if self.polite_delay_seconds:
                    time.sleep(self.polite_delay_seconds)

                with self.session.get(url, stream=True, timeout=self.timeout_seconds) as r:
                    if r.status_code == 404:
                        return task, "404"
                    r.raise_for_status()
                    tmp = out.with_suffix(out.suffix + ".part")
                    total_size = int(r.headers.get("content-length", 0))
                    downloaded = 0
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 64):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                    if total_size and downloaded != total_size:
                        raise IOError(f"Incomplete download: got {downloaded} bytes, expected {total_size}")
                    tmp.replace(out)

                if extract:
                    self.extract_zip(out, date)
                    if self.remove_zip_after_extract and out.exists():
                        out.unlink(missing_ok=True)

                return task, "downloaded"

            except Exception as e:
                last_err = e
                part = out.with_suffix(out.suffix + ".part")
                if part.exists():
                    part.unlink(missing_ok=True)
                if out.exists():
                    out.unlink(missing_ok=True)
                time.sleep(1.5 * attempt)

        return task, f"failed: {last_err}"

    def _date_list(self, start_date: str | datetime, end_date: str | datetime) -> list[datetime]:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        current = datetime(start_date.year, start_date.month, start_date.day)
        end = datetime(end_date.year, end_date.month, end_date.day)
        dates = []
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    def download_year_range(
        self,
        start_year: int,
        end_year: int,
        extract: bool = True,
    ) -> None:
        start_date = datetime(int(start_year), 1, 1)
        end_date = datetime(int(end_year), 12, 31)
        dates = self._date_list(start_date, end_date)
        tasks = [PRISMTask(date=d, extract=extract) for d in dates]

        print("\n" + "=" * 70)
        print("PRISM parallel daily download")
        print(f"Variable: {PRISM_VARIABLE}")
        print(f"Range: {dates[0].date()} to {dates[-1].date()}  ({len(dates)} days)")
        print(f"Workers: {self.max_workers} | Retries: {self.max_retries} | Extract: {extract} | Remove zip after extract: {self.remove_zip_after_extract}")
        print(f"Output: {self.output_dir.resolve()}")
        print("=" * 70)

        results = self._run_parallel(
            tasks,
            self._download_one,
            description="PRISM download",
            progress_interval=25,
        )

        failed_examples: list[tuple[str, str]] = []
        for task, status in results:
            if status.startswith("failed") or (status not in ("downloaded", "exists", "404") and "failed" in status):
                if len(failed_examples) < 5:
                    failed_examples.append((task.date.date().isoformat(), status))
        if failed_examples:
            print("Sample failures:")
            for day, msg in failed_examples:
                print(f"  {day}: {msg}")
        print("-" * 70 + "\n")


if __name__ == "__main__":
    dl = PRISMDownloaderParallel(
        output_dir="prism_data",
        max_workers=6,
        max_retries=3,
        timeout_seconds=60,
        polite_delay_seconds=0.0,
        remove_zip_after_extract=True,
    )
    dl.download_year_range(
        start_year=2022,
        end_year=2024,
        extract=True
    )
