#!/usr/bin/env python3
"""IFS ENS control downloader (ECMWF Open Data, enfo stream, 0.25°).

Source: s3://ecmwf-forecasts/{YYYYMMDD}/{HH}z/ifs/0p25/enfo/
        {YYYYMMDDHH0000}-{N}h-enfo-ef.grib2  (+ JSONL .index sidecar)

Same bucket and byte-range machinery as the AIFS downloader (which this
subclasses); three differences:

  1. every enfo lead file holds all ensemble members — the control member
     is selected by filtering index lines on ``type == "cf"``. From Cycle
     50r1 (2026-05-12) the open-data ENS carries only the 50 perturbed
     members, so the unperturbed run is read from the HRES deterministic
     stream instead (``oper``, ``type == "fc"``, same 0.25° grid); the
     registry's 49r1/50r1 version windows keep the two eras unpooled;
  2. ``tp`` is cumulative *meters* of water (GRIB_units "m"), not kg m**-2,
     so daily totals are ×1000 → mm via ``daily_from_cumulative_meters``
     (shared with the fig7_1 round-trip harness — one source of truth);
  3. raw GRIB records are purged after an init assembles completely
     (``purge_gribs``, default on): the full-span raw archive (~26 GB) does
     not fit next to the existing model_data, and the assembled .npy files
     are the only thing downstream stages read.

Output layout matches aifs_downloader exactly:
    model_data/ifs/{year}/{YYYYMMDD}_{HH}z/f024_surface.npy ...  (precip mm)
    model_data/ifs/{year}/{YYYYMMDD}_{HH}z/f024_temp.npy / _wind.npy / _humidity.npy
    model_data/ifs/grid_lats.npy / grid_lons.npy
"""

from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from accumulation import daily_from_cumulative_meters
from aifs_downloader import AIFSDownloaderParallel

# Cycle 49r1 went operational 2024-11-12; earlier enfo cycles exist in the
# bucket but predate every other model's record (peers start 2025-05-06).
IFS_FIRST_DATE = datetime(2024, 11, 12)
# Cycle 50r1 (operational 2026-05-12, mirrored in MODEL_REGISTRY["ifs"]
# version_windows) dropped the control member from the open-data ENS: the
# enfo index lists ``type == "pf"`` only. The unperturbed forecast continues
# as the HRES deterministic run (stream ``oper``, ``type == "fc"``).
IFS_OPER_FIRST_DATE = datetime(2026, 5, 12)


class IFSControlDownloaderParallel(AIFSDownloaderParallel):
    DAILY_FN = staticmethod(daily_from_cumulative_meters)
    FIRST_DATE = IFS_FIRST_DATE
    SOURCE_NAME = "IFS ENS control (ECMWF)"

    def __init__(
        self,
        output_dir: str = "model_data/ifs",
        max_workers: int = 8,
        max_retries: int = 3,
        timeout_seconds: int = 120,
        polite_delay_seconds: float = 0.0,
        purge_gribs: bool = True,
    ):
        super().__init__(
            output_dir=output_dir,
            max_workers=max_workers,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            polite_delay_seconds=polite_delay_seconds,
        )
        self.purge_gribs = purge_gribs

    @staticmethod
    def _source(init_date: datetime) -> tuple[str, str, str]:
        """(stream, file suffix, index ``type``) for the unperturbed run of an init."""
        if init_date >= IFS_OPER_FIRST_DATE:
            return "oper", "fc", "fc"
        return "enfo", "ef", "cf"

    def _remote_paths(self, init_date: datetime, cycle: int, fhour: int) -> tuple[str, str]:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{cycle:02d}"
        stream, suffix, _ = self._source(init_date)
        prefix = f"{date_str}/{cycle_str}z/ifs/0p25/{stream}"
        stem = f"{date_str}{cycle_str}0000-{fhour}h-{stream}-{suffix}"
        grib_url = f"{self.base_url}/{prefix}/{stem}.grib2"
        idx_url = f"{self.base_url}/{prefix}/{stem}.index"
        return grib_url, idx_url

    def _download_task(self, task):
        init_date, cycle, fhour, param = task.init_date, task.cycle, task.fhour, task.param
        out_file = self._record_grib(init_date, cycle, fhour, param)
        grib_url, idx_url = self._remote_paths(init_date, cycle, fhour)
        member_type = self._source(init_date)[2]
        status = self._byte_range_fetch(
            grib_url=grib_url,
            idx_url=idx_url,
            idx_parser=lambda txt: self._find_byte_range(txt, param, member_type),
            out_path=out_file,
        )
        return task, status

    def _find_byte_range(self, idx_text: str, param: str = "tp",
                         member_type: str = "cf") -> tuple[int | None, int | None]:
        """One member only: match the index ``type`` (``cf`` control in the
        ENS era, ``fc`` deterministic from 50r1) in addition to param."""
        for line in idx_text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("type") == member_type and d.get("param") == param:
                off = d.get("_offset")
                ln = d.get("_length")
                if off is None or ln is None:
                    return None, None
                return int(off), int(off) + int(ln) - 1
        return None, None

    def _assemble_daily(self, init_dates, cycle, daily_fhours, level) -> None:
        super()._assemble_daily(init_dates, cycle, daily_fhours, level)
        if not self.purge_gribs:
            return
        purged = 0
        for init_date in init_dates:
            if not self._init_complete(init_date, cycle, daily_fhours, level):
                continue
            for sub in ("_tp", "_inst"):
                raw_dir = self._init_dir(init_date, cycle) / sub
                if raw_dir.exists():
                    shutil.rmtree(raw_dir)
                    purged += 1
        if purged:
            print(f"Purged raw GRIB dirs for assembled inits ({purged} dirs).")


if __name__ == "__main__":
    import argparse

    sys.path.insert(0, str(_this_dir.parent))
    from model_registry import MODEL_REGISTRY

    cfg = MODEL_REGISTRY["ifs"]

    parser = argparse.ArgumentParser(description="Download IFS ENS control (ECMWF) forecasts")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD).")
    parser.add_argument("--start-year", type=int, help="Start year")
    parser.add_argument("--end-year", type=int, help="End year")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--keep-gribs", action="store_true",
                        help="Keep raw GRIB records after assembly (default: purge)")
    args = parser.parse_args()

    downloader = IFSControlDownloaderParallel(
        output_dir=cfg.data_dir,
        max_workers=args.workers,
        max_retries=3,
        timeout_seconds=120,
        purge_gribs=not args.keep_gribs,
    )

    if args.start_year:
        downloader.download_year_range(
            start_year=args.start_year,
            end_year=args.end_year or args.start_year,
        )
    else:
        start = args.start_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        end = args.end_date or start
        downloader.download_date_range(start_date=start, end_date=end)
