#!/usr/bin/env python3
"""AIFS downloader (ECMWF Open Data, aifs-single 0.25°).

Source: s3://ecmwf-forecasts/{YYYYMMDD}/{HH}z/aifs-single/0p25/oper/
        {YYYYMMDDHH0000}-{N}h-oper-fc.grib2  (every 6h, 0..360h)

`tp` is total precipitation accumulated *from forecast start*. The GRIB attr
`GRIB_units` reports `kg m**-2` (≡ mm of water), so no scaling is applied —
unlike the WeatherNext 2 pipeline whose source delivers meters.
To get a daily total ending at lead day D we subtract:
    daily_total = tp(D*24) - tp((D-1)*24)

We download tp at f0, f24, f48, ..., f336 per init via byte-range using the
sidecar .index (JSONL with _offset/_length), then assemble per-day .npy files
on a regular 0.25° lat/lon CONUS grid.

Besides precip, AIFS exposes instantaneous 2 m / 10 m fields as snapshots at
each daily fhour (single record, no diff):
    2t        -> temp     (K -> degC)
    10u/10v   -> wind     (hypot, m/s)
    2t + 2d   -> humidity (RH derived from dewpoint via Magnus)
(AIFS has no surface RH field; humidity is derived from 2 m temp + dewpoint.)

Output (matches gfs_downloader layout):
    model_data/aifs/{year}/{YYYYMMDD}_{HH}z/f024_surface.npy ...   (precip mm)
    model_data/aifs/{year}/{YYYYMMDD}_{HH}z/f024_temp.npy / _wind.npy / _humidity.npy
    model_data/aifs/{year}/{YYYYMMDD}_{HH}z/_tp/f000.grib2 ...     (raw cumulative tp)
    model_data/aifs/{year}/{YYYYMMDD}_{HH}z/_inst/f024_2t.grib2 ... (raw instant records)
    model_data/aifs/grid_lats.npy
    model_data/aifs/grid_lons.npy
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from accumulation import daily_from_cumulative
from base import BaseDownloader, grib_first_field, normalize_and_crop
from forecast_variables import kelvin_to_celsius, rh_from_dewpoint, wind_speed

AIFS_CYCLE = 12
AIFS_VARIABLE = "tp"
AIFS_MAX_LEAD_H = 336  # 14 days, clipped from native 360h
AIFS_FIRST_DATE = datetime(2025, 3, 1)  # earliest archived AIFS-single

# Instantaneous records (ECMWF .index ``param`` names) snapshotted at each daily
# fhour. humidity is derived from 2t + 2d, so both must be fetched.
AIFS_INSTANT_PARAMS: tuple[str, ...] = ("2t", "2d", "10u", "10v")


def _save_grid(output_dir: Path, kept_lats, kept_lons) -> None:
    """Write grid_lats.npy/grid_lons.npy once. All variables share one grid."""
    import numpy as np

    glat = output_dir / "grid_lats.npy"
    glon = output_dir / "grid_lons.npy"
    if not glat.exists():
        np.save(glat, kept_lats)
        np.save(glon, kept_lons)


def _convert_grib_to_npy(grib_path: Path, prev_grib_path: Path | None, npy_path: Path, output_dir: Path,
                         daily_fn=daily_from_cumulative) -> None:
    """Compute tp(fh) - tp(fh-24), crop to CONUS, save as .npy. cfgrib is not thread-safe.

    ``daily_fn`` is the accumulation core turning (cur, prev) cumulative fields
    into a daily mm total — AIFS tp is kg m^-2 == mm (daily_from_cumulative);
    the IFS control subclass passes the meters variant.
    """
    import numpy as np

    if npy_path.exists():
        return

    cur, lats, lons = grib_first_field(grib_path)
    if prev_grib_path is not None and prev_grib_path.exists():
        prev, _, _ = grib_first_field(prev_grib_path)
    else:
        # No predecessor (e.g. f24 with no f0) — treat tp as a 24h delta from 0.
        prev = None
    # The shared core differences and clips tiny negative noise (crop commutes
    # with the elementwise math).
    diff = daily_fn(cur, prev)

    cropped, kept_lats, kept_lons = normalize_and_crop(diff, lats, lons)

    npy_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_path, cropped)
    _save_grid(output_dir, kept_lats, kept_lons)


def _convert_temp_to_npy(t_grib: Path, npy_path: Path, output_dir: Path) -> None:
    """2 m temperature snapshot: K -> degC, crop to CONUS, save as .npy."""
    import numpy as np

    if npy_path.exists():
        return
    vals, lats, lons = grib_first_field(t_grib)
    cropped, kept_lats, kept_lons = normalize_and_crop(vals, lats, lons)
    np.save(npy_path, kelvin_to_celsius(cropped))
    _save_grid(output_dir, kept_lats, kept_lons)


def _convert_wind_to_npy(u_grib: Path, v_grib: Path, npy_path: Path, output_dir: Path) -> None:
    """10 m wind speed from u/v snapshots (hypot), crop to CONUS, save as .npy."""
    import numpy as np

    if npy_path.exists():
        return
    u, ulats, ulons = grib_first_field(u_grib)
    v, _, _ = grib_first_field(v_grib)
    uc, kept_lats, kept_lons = normalize_and_crop(u, ulats, ulons)
    vc, _, _ = normalize_and_crop(v, ulats, ulons)
    np.save(npy_path, wind_speed(uc, vc))
    _save_grid(output_dir, kept_lats, kept_lons)


def _convert_humidity_to_npy(t_grib: Path, td_grib: Path, npy_path: Path, output_dir: Path) -> None:
    """RH (%) from 2 m temp + dewpoint (Magnus), crop to CONUS, save as .npy."""
    import numpy as np

    if npy_path.exists():
        return
    t, tlats, tlons = grib_first_field(t_grib)
    td, _, _ = grib_first_field(td_grib)
    tc, kept_lats, kept_lons = normalize_and_crop(t, tlats, tlons)
    tdc, _, _ = normalize_and_crop(td, tlats, tlons)
    np.save(npy_path, rh_from_dewpoint(tc, tdc))
    _save_grid(output_dir, kept_lats, kept_lons)


@dataclass(frozen=True)
class DownloadTask:
    init_date: datetime
    cycle: int
    fhour: int  # raw lead hour in source (0, 24, 48, ..., 336)
    param: str = "tp"  # "tp" | "2t" | "2d" | "10u" | "10v"


class AIFSDownloaderParallel(BaseDownloader):
    # Subclass hooks (see ifs_downloader.IFSControlDownloaderParallel): the
    # accumulation core for tp → daily mm, the archive's first available init,
    # and the name used in progress banners.
    DAILY_FN = staticmethod(daily_from_cumulative)
    FIRST_DATE = AIFS_FIRST_DATE
    SOURCE_NAME = "AIFS (ECMWF)"

    def __init__(
        self,
        output_dir: str = "model_data/aifs",
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
            base_url="https://ecmwf-forecasts.s3.amazonaws.com",
            user_agent="AIFSDownloaderParallel/1.0",
        )

    def _remote_paths(self, init_date: datetime, cycle: int, fhour: int) -> tuple[str, str]:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{cycle:02d}"
        prefix = f"{date_str}/{cycle_str}z/aifs-single/0p25/oper"
        stem = f"{date_str}{cycle_str}0000-{fhour}h-oper-fc"
        grib_url = f"{self.base_url}/{prefix}/{stem}.grib2"
        idx_url = f"{self.base_url}/{prefix}/{stem}.index"
        return grib_url, idx_url

    def _init_dir(self, init_date: datetime, cycle: int) -> Path:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{cycle:02d}"
        out = self.output_dir / str(init_date.year) / f"{date_str}_{cycle_str}z"
        out.mkdir(parents=True, exist_ok=True)
        return out

    def _tp_grib(self, init_date: datetime, cycle: int, fhour: int) -> Path:
        d = self._init_dir(init_date, cycle) / "_tp"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"f{fhour:03d}.grib2"

    def _record_grib(self, init_date: datetime, cycle: int, fhour: int, param: str) -> Path:
        """Raw GRIB2 path for a record: tp in _tp/, instant params in _inst/."""
        if param == "tp":
            return self._tp_grib(init_date, cycle, fhour)
        d = self._init_dir(init_date, cycle) / "_inst"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"f{fhour:03d}_{param}.grib2"

    def _daily_npy(self, init_date: datetime, cycle: int, daily_fhour: int, level: str) -> Path:
        return self._init_dir(init_date, cycle) / f"f{daily_fhour:03d}_{level}.npy"

    def _var_npy(self, init_date: datetime, cycle: int, daily_fhour: int, var_suffix: str) -> Path:
        """Assembled per-variable daily .npy path (suffix: surface/temp/wind/humidity)."""
        return self._init_dir(init_date, cycle) / f"f{daily_fhour:03d}_{var_suffix}.npy"

    def _init_complete(self, init_date: datetime, cycle: int, daily_fhours: list[int], level: str) -> bool:
        """True iff precip + temp + wind + humidity .npy exist for every daily fhour."""
        suffixes = (level, "temp", "wind", "humidity")
        return all(
            self._var_npy(init_date, cycle, fh, sfx).exists()
            for fh in daily_fhours
            for sfx in suffixes
        )

    def _find_byte_range(self, idx_text: str, param: str = AIFS_VARIABLE) -> tuple[int | None, int | None]:
        """Parse JSONL .index file; return (offset, end_byte_inclusive) for ``param``."""
        for line in idx_text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("param") == param:
                off = d.get("_offset")
                ln = d.get("_length")
                if off is None or ln is None:
                    return None, None
                return int(off), int(off) + int(ln) - 1
        return None, None

    def _download_task(self, task: DownloadTask) -> tuple[DownloadTask, str]:
        init_date, cycle, fhour, param = task.init_date, task.cycle, task.fhour, task.param
        out_file = self._record_grib(init_date, cycle, fhour, param)
        grib_url, idx_url = self._remote_paths(init_date, cycle, fhour)
        status = self._byte_range_fetch(
            grib_url=grib_url,
            idx_url=idx_url,
            idx_parser=lambda txt: self._find_byte_range(txt, param),
            out_path=out_file,
        )
        return task, status

    def _assemble_daily(
        self,
        init_dates: list[datetime],
        cycle: int,
        daily_fhours: list[int],
        level: str,
    ) -> None:
        """Assemble all four variables per daily fhour. cfgrib is not thread-safe.

        precip = tp(fh) - tp(fh-24); temp/wind/humidity are instantaneous
        snapshots at fh (no diff). Humidity is derived from 2t + 2d (Magnus).
        Each assembled .npy uses the variable suffix expected by extract_forecast.
        """
        # Each work item: (kind, npy_out, *grib_inputs).
        work: list[tuple] = []
        skipped_existing = 0
        skipped_missing = 0

        for init_date in init_dates:
            for daily_fh in daily_fhours:
                # ── precip (cumulative tp diff) ──
                npy_out = self._var_npy(init_date, cycle, daily_fh, level)
                if npy_out.exists():
                    skipped_existing += 1
                else:
                    cur_grib = self._tp_grib(init_date, cycle, daily_fh)
                    prev_fh = daily_fh - 24
                    prev_grib = self._tp_grib(init_date, cycle, prev_fh) if prev_fh >= 0 else None
                    if not cur_grib.exists() or (prev_grib is not None and not prev_grib.exists()):
                        skipped_missing += 1
                    else:
                        work.append(("precip", npy_out, cur_grib, prev_grib))

                # ── temp (2t, K -> degC) ──
                temp_out = self._var_npy(init_date, cycle, daily_fh, "temp")
                if temp_out.exists():
                    skipped_existing += 1
                else:
                    t = self._record_grib(init_date, cycle, daily_fh, "2t")
                    if t.exists():
                        work.append(("temp", temp_out, t))
                    else:
                        skipped_missing += 1

                # ── wind (10u/10v -> hypot) ──
                wind_out = self._var_npy(init_date, cycle, daily_fh, "wind")
                if wind_out.exists():
                    skipped_existing += 1
                else:
                    u = self._record_grib(init_date, cycle, daily_fh, "10u")
                    v = self._record_grib(init_date, cycle, daily_fh, "10v")
                    if u.exists() and v.exists():
                        work.append(("wind", wind_out, u, v))
                    else:
                        skipped_missing += 1

                # ── humidity (2t + 2d -> RH) ──
                hum_out = self._var_npy(init_date, cycle, daily_fh, "humidity")
                if hum_out.exists():
                    skipped_existing += 1
                else:
                    t = self._record_grib(init_date, cycle, daily_fh, "2t")
                    td = self._record_grib(init_date, cycle, daily_fh, "2d")
                    if t.exists() and td.exists():
                        work.append(("humidity", hum_out, t, td))
                    else:
                        skipped_missing += 1

        if not work:
            print(f"\nAssembly: 0 created | {skipped_existing} existed | {skipped_missing} skipped (missing grib)")
            return

        print(f"  {len(work)} variable files to assemble (sequential cfgrib)...")
        done = 0
        failed = 0
        for item in work:
            kind, npy_out = item[0], item[1]
            try:
                if kind == "precip":
                    _convert_grib_to_npy(item[2], item[3], npy_out, self.output_dir,
                                         daily_fn=self.DAILY_FN)
                elif kind == "temp":
                    _convert_temp_to_npy(item[2], npy_out, self.output_dir)
                elif kind == "wind":
                    _convert_wind_to_npy(item[2], item[3], npy_out, self.output_dir)
                elif kind == "humidity":
                    _convert_humidity_to_npy(item[2], item[3], npy_out, self.output_dir)
                done += 1
            except Exception as e:
                failed += 1
                if failed <= 5:
                    print(f"  WARNING: assembly failed for {npy_out}: {e}")
            n = done + failed
            if n % 100 == 0 or n == len(work):
                print(f"  Assembly progress: {n}/{len(work)}", flush=True)
        # Clean up cfgrib sidecar idx files.
        for init_date in init_dates:
            for sub in ("_tp", "_inst"):
                raw_dir = self._init_dir(init_date, cycle) / sub
                if raw_dir.exists():
                    for sidecar in raw_dir.glob("*.idx"):
                        sidecar.unlink(missing_ok=True)
        print(f"\nAssembly: {done} created | {skipped_existing} existed | {skipped_missing} skipped"
              + (f" | {failed} failed" if failed else ""))

    # ── Public download interface ───────────────────────────────────

    def _download(self, start: datetime, end: datetime, *, forecast_hours=None, level: str = "surface"):
        # Clip to archive availability.
        if start < self.FIRST_DATE:
            print(f"{self.SOURCE_NAME} archive starts {self.FIRST_DATE.date()}; clipping start.")
            start = self.FIRST_DATE
        cycle = AIFS_CYCLE

        init_dates: list[datetime] = []
        cur = datetime(start.year, start.month, start.day)
        end0 = datetime(end.year, end.month, end.day)
        while cur <= end0:
            init_dates.append(cur)
            cur += timedelta(days=1)
        if not init_dates:
            raise ValueError("No init dates selected (post-clip).")

        daily_fhours = [d * 24 for d in range(1, AIFS_MAX_LEAD_H // 24 + 1)]
        # tp needs f0 (for the first diff) plus every daily_fhour; instant params
        # are snapshots only at the daily fhours.
        tp_fhours = sorted({0, *daily_fhours})

        tasks: list[DownloadTask] = []
        skipped = 0
        for d in init_dates:
            # If all variables already assembled for this init, skip the whole init.
            if self._init_complete(d, cycle, daily_fhours, level):
                skipped += 1
                continue
            for fh in tp_fhours:
                if not self._record_grib(d, cycle, fh, "tp").exists():
                    tasks.append(DownloadTask(d, cycle, fh, "tp"))
            for fh in daily_fhours:
                for param in AIFS_INSTANT_PARAMS:
                    if not self._record_grib(d, cycle, fh, param).exists():
                        tasks.append(DownloadTask(d, cycle, fh, param))

        print("\n" + "=" * 70)
        print(f"Parallel {self.SOURCE_NAME} download")
        print(f"Period: {start.date()} to {end.date()} | Daily")
        print(f"Cycle: {cycle:02d}z | Lead 1-{AIFS_MAX_LEAD_H // 24} days")
        print(f"tp fhours per init: {tp_fhours}")
        print(f"Variables: tp (cumulative) + 2t/10u/10v/2d (instant)")
        print(f"Workers: {self.max_workers} | Retries: {self.max_retries}")
        print(f"Output: {self.output_dir.resolve()}")
        print(f"Tasks: {len(tasks)} grib2 records to download | {skipped} inits already complete")
        print("=" * 70)

        results = self._run_parallel(
            tasks,
            self._download_task,
            description="AIFS download",
            progress_interval=50,
        )

        examples: list[str] = []
        for task, status in results:
            if status == "not_found_idx" and len(examples) < 5:
                examples.append(f"Missing idx: {task.init_date:%Y-%m-%d} {task.cycle:02d}z f{task.fhour:03d} ({task.param})")
            elif status == "not_found_var" and len(examples) < 5:
                examples.append(f"{task.param} not found: {task.init_date:%Y-%m-%d} {task.cycle:02d}z f{task.fhour:03d}")
            elif status.startswith("failed") and len(examples) < 5:
                examples.append(f"Failed: {task.init_date:%Y-%m-%d} {task.cycle:02d}z f{task.fhour:03d} ({task.param}) -> {status}")
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print("-" * 70)

        print("\nAssembling daily totals from cumulative tp...")
        self._assemble_daily(init_dates, cycle, daily_fhours, level)


if __name__ == "__main__":
    import argparse

    sys.path.insert(0, str(_this_dir.parent))
    from model_registry import MODEL_REGISTRY

    cfg = MODEL_REGISTRY["aifs"]

    parser = argparse.ArgumentParser(description="Download AIFS (ECMWF) precipitation forecasts")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD).")
    parser.add_argument("--start-year", type=int, help="Start year")
    parser.add_argument("--end-year", type=int, help="End year")
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    downloader = AIFSDownloaderParallel(
        output_dir=cfg.data_dir,
        max_workers=args.workers,
        max_retries=3,
        timeout_seconds=120,
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
