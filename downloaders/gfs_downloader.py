#!/usr/bin/env python3
"""GFS downloader.

Downloads APCP (precipitation) GRIB2 files from the NOAA GFS S3 bucket
using byte-range requests via .idx index files.

GFS publishes APCP as a `0-N day acc fcst` cumulative bucket at every daily
forecast hour (f024, f048, ..., f336). To get a daily total ending at lead
day D we subtract:
    daily_total = cumulative(D*24) - cumulative((D-1)*24)
This mirrors the AIFS pipeline (`aifs_downloader.py`).

Besides precip, GFS exposes the instantaneous 2 m / 10 m fields directly in the
same f{fh} file (snapshot at the lead's valid time, descriptor "{fh} hour fcst"):
    TMP:2 m above ground        -> temp     (K -> degC)
    RH:2 m above ground         -> humidity (%, direct)
    UGRD/VGRD:10 m above ground -> wind     (hypot, m/s)

Output:
    model_data/gfs/{year}/{YYYYMMDD}_{HH}z/f024_surface.npy ...   (precip mm)
    model_data/gfs/{year}/{YYYYMMDD}_{HH}z/f024_temp.npy / _wind.npy / _humidity.npy
    model_data/gfs/{year}/{YYYYMMDD}_{HH}z/_apcp/f024.grib2 ...   (raw cumulative APCP)
    model_data/gfs/{year}/{YYYYMMDD}_{HH}z/_inst/f024_temp.grib2 ... (raw instant records)
    model_data/gfs/grid_lats.npy
    model_data/gfs/grid_lons.npy
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, timezone

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from base import BaseDownloader, grib_first_field, normalize_and_crop
from forecast_variables import kelvin_to_celsius, wind_speed

# Fixed 12z cycle for GFS; used in remote paths and local dir names.
GFS_CYCLE = 12
GFS_VARIABLE = "APCP"

# Instantaneous 2 m / 10 m records: record key -> (grib var code, level descriptor).
# Each is a snapshot at the daily fhour (idx descriptor "{fh} hour fcst").
GFS_INSTANT_RECORDS: dict[str, tuple[str, str]] = {
    "temp": ("TMP", "2 m above ground"),
    "humidity": ("RH", "2 m above ground"),
    "ugrd": ("UGRD", "10 m above ground"),
    "vgrd": ("VGRD", "10 m above ground"),
}


def _save_grid(output_dir: Path, kept_lats, kept_lons) -> None:
    """Write grid_lats.npy/grid_lons.npy once. All variables share one grid."""
    import numpy as np

    glat = output_dir / "grid_lats.npy"
    glon = output_dir / "grid_lons.npy"
    if not glat.exists():
        np.save(glat, kept_lats)
        np.save(glon, kept_lons)


def _convert_grib_to_npy(grib_path: Path, prev_grib_path: Path | None, npy_path: Path, output_dir: Path) -> None:
    """Compute APCP(fh) - APCP(fh-24), crop to CONUS, save as .npy. cfgrib is not thread-safe."""
    import numpy as np

    if npy_path.exists():
        return

    cur, lats, lons = grib_first_field(grib_path)
    if prev_grib_path is not None and prev_grib_path.exists():
        prev, _, _ = grib_first_field(prev_grib_path)
        diff = cur - prev
    else:
        # No predecessor (f024 has no f000 cumulative — model starts at 0).
        diff = cur

    cropped, kept_lats, kept_lons = normalize_and_crop(diff, lats, lons)
    # APCP is kg m^-2 == mm; clip tiny negative noise from subtraction.
    np.clip(cropped, 0.0, None, out=cropped)

    npy_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_path, cropped)
    _save_grid(output_dir, kept_lats, kept_lons)


def _convert_instant_to_npy(grib_path: Path, npy_path: Path, output_dir: Path, *, to_celsius: bool = False) -> None:
    """Crop one instantaneous GRIB record to CONUS, save as .npy (optionally K->degC)."""
    import numpy as np

    if npy_path.exists():
        return

    vals, lats, lons = grib_first_field(grib_path)
    cropped, kept_lats, kept_lons = normalize_and_crop(vals, lats, lons)
    if to_celsius:
        cropped = kelvin_to_celsius(cropped)

    npy_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_path, cropped.astype(np.float32))
    _save_grid(output_dir, kept_lats, kept_lons)


def _convert_wind_to_npy(ugrd_path: Path, vgrd_path: Path, npy_path: Path, output_dir: Path) -> None:
    """Combine 10 m u/v records into wind speed (hypot), crop to CONUS, save as .npy."""
    import numpy as np

    if npy_path.exists():
        return

    u, ulats, ulons = grib_first_field(ugrd_path)
    v, _, _ = grib_first_field(vgrd_path)
    uc, kept_lats, kept_lons = normalize_and_crop(u, ulats, ulons)
    vc, _, _ = normalize_and_crop(v, ulats, ulons)
    spd = wind_speed(uc, vc)

    npy_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_path, spd.astype(np.float32))
    _save_grid(output_dir, kept_lats, kept_lons)


@dataclass(frozen=True)
class DownloadTask:
    init_date: datetime
    fhour: int
    record: str  # "apcp" | "temp" | "humidity" | "ugrd" | "vgrd"
    level: str = "surface"


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

    def _paths(self, init_date: datetime, fhour: int) -> tuple[str, str, str]:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{GFS_CYCLE:02d}"
        grib_filename = f"gfs.t{cycle_str}z.pgrb2.0p25.f{fhour:03d}"
        idx_filename = f"{grib_filename}.idx"
        grib_path = f"gfs.{date_str}/{cycle_str}/atmos/{grib_filename}"
        idx_path = f"gfs.{date_str}/{cycle_str}/atmos/{idx_filename}"
        return grib_filename, grib_path, idx_path

    def _init_dir(self, init_date: datetime) -> Path:
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{GFS_CYCLE:02d}"
        out = self.output_dir / str(init_date.year) / f"{date_str}_{cycle_str}z"
        out.mkdir(parents=True, exist_ok=True)
        return out

    def _output_npy(self, init_date: datetime, fhour: int, level: str) -> Path:
        """Final daily-total .npy path (what compute_stats reads)."""
        safe_level = level.replace(" ", "_")
        return self._init_dir(init_date) / f"f{fhour:03d}_{safe_level}.npy"

    def _apcp_grib(self, init_date: datetime, fhour: int) -> Path:
        """Cumulative APCP GRIB2 path (kept for re-assembly)."""
        d = self._init_dir(init_date) / "_apcp"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"f{fhour:03d}.grib2"

    def _record_grib(self, init_date: datetime, fhour: int, record: str) -> Path:
        """Raw GRIB2 path for a record: precip in _apcp/, instant fields in _inst/."""
        if record == "apcp":
            return self._apcp_grib(init_date, fhour)
        d = self._init_dir(init_date) / "_inst"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"f{fhour:03d}_{record}.grib2"

    def _var_npy(self, init_date: datetime, fhour: int, var_suffix: str) -> Path:
        """Assembled per-variable daily .npy path (suffix: surface/temp/wind/humidity)."""
        return self._init_dir(init_date) / f"f{fhour:03d}_{var_suffix}.npy"

    def _init_complete(self, init_date: datetime, daily_fhours: list[int], level: str) -> bool:
        """True iff precip + temp + wind + humidity .npy exist for every daily fhour."""
        suffixes = (level.replace(" ", "_"), "temp", "wind", "humidity")
        return all(
            self._var_npy(init_date, fh, sfx).exists()
            for fh in daily_fhours
            for sfx in suffixes
        )

    def _find_record_byte_range(self, idx_text: str, record: str, fhour: int, level: str):
        """Dispatch idx parsing: cumulative APCP vs an instantaneous 2 m/10 m field."""
        if record == "apcp":
            return self._find_byte_range(idx_text, level, fhour)

        grib_var, lvl_desc = GFS_INSTANT_RECORDS[record]
        expected_desc = f"{fhour} hour fcst"
        lines = idx_text.strip().split("\n")
        for i, line in enumerate(lines):
            parts = line.split(":")
            if len(parts) < 6:
                continue
            if parts[3].strip() != grib_var or parts[4].strip() != lvl_desc:
                continue
            if parts[5].strip() != expected_desc:
                continue
            # Skip any extra-qualified variant (non-empty field after the time desc).
            if len(parts) > 6 and parts[6].strip() != "":
                continue
            start_byte = int(parts[1])
            end_byte = None
            if i + 1 < len(lines):
                end_byte = int(lines[i + 1].split(":")[1]) - 1
            return start_byte, end_byte
        return None, None

    def _find_byte_range(self, idx_text: str, level: str, fhour: int):
        """Locate cumulative APCP at the requested daily fhour in the .idx file.

        GFS publishes APCP as `0-N day acc fcst` at f024…f336 (matched here).
        Raises ValueError if a non-daily fhour is requested.
        """
        if fhour <= 0 or fhour % 24 != 0:
            raise ValueError(f"GFS APCP requires a daily fhour (multiple of 24, ≥24); got fhour={fhour}")
        expected_desc = f"0-{fhour // 24} day acc fcst"

        lines = idx_text.strip().split("\n")
        start_byte = None
        end_byte = None
        for i, line in enumerate(lines):
            parts = line.split(":")
            if len(parts) < 6:
                continue
            var_code = parts[3].strip()
            lvl_desc = parts[4].strip()
            time_desc = parts[5].strip()
            if var_code == GFS_VARIABLE and lvl_desc == level and time_desc == expected_desc:
                start_byte = int(parts[1])
                if i + 1 < len(lines):
                    next_parts = lines[i + 1].split(":")
                    end_byte = int(next_parts[1]) - 1
                break
        return start_byte, end_byte

    def _download_task(self, task: DownloadTask) -> tuple[DownloadTask, str]:
        init_date, fhour, record, level = task.init_date, task.fhour, task.record, task.level
        grib_file = self._record_grib(init_date, fhour, record)
        _, grib_path, idx_path = self._paths(init_date, fhour)
        status = self._byte_range_fetch(
            grib_url=f"{self.base_url}/{grib_path}",
            idx_url=f"{self.base_url}/{idx_path}",
            idx_parser=lambda txt: self._find_record_byte_range(txt, record, fhour, level),
            out_path=grib_file,
        )
        return task, status

    def _download(
        self,
        start: datetime,
        end: datetime,
        *,
        forecast_hours: list[int],
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

        # Validate: GFS only registers daily fhours, and we need cumulative descs `0-N day acc fcst`.
        daily_fhours = sorted({int(fh) for fh in forecast_hours})
        for fh in daily_fhours:
            if fh <= 0 or fh % 24 != 0:
                raise ValueError(f"GFS forecast hours must be daily multiples of 24 (≥24); got {fh}")

        # Records to fetch per daily fhour: cumulative precip + four instant fields.
        records = ("apcp", *GFS_INSTANT_RECORDS.keys())

        tasks: list[DownloadTask] = []
        skipped = 0
        for d in init_dates:
            # If every variable's daily .npy is already assembled, skip the init.
            if self._init_complete(d, daily_fhours, level):
                skipped += 1
                continue
            for fh in daily_fhours:
                for record in records:
                    # Skip download if the raw record grib is already on disk.
                    if self._record_grib(d, fh, record).exists():
                        continue
                    tasks.append(DownloadTask(d, fh, record, str(level)))

        print("\n" + "=" * 70)
        print("Parallel GFS filtered download (idx + Range)")
        print(f"Period: {start.date()} to {end.date()} | Daily")
        print(f"Cycle: {GFS_CYCLE:02d}z")
        print(f"Forecast hours: {daily_fhours}")
        print(f"Variables: precip (cumulative) + temp/wind/humidity (instant) | Level: {level}")
        print(f"Workers: {self.max_workers} | Retries: {self.max_retries}")
        print(f"Output: {self.output_dir.resolve()}")
        print(f"Tasks: {len(tasks)} grib2 records to download | {skipped} inits already complete")
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
                examples.append(f"Missing idx: {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d} ({task.record})")
            elif status == "not_found_var" and len(examples) < 5:
                examples.append(f"Var not found: {task.record} in {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d}")
            elif status.startswith("failed") and len(examples) < 5:
                examples.append(f"Failed: {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d} ({task.record}) -> {status}")
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print("-" * 70)

        print("\nAssembling daily variables (precip diff + instant snapshots)...")
        self._assemble_daily(init_dates, daily_fhours, level)


    def _assemble_daily(
        self,
        init_dates: list[datetime],
        daily_fhours: list[int],
        level: str,
    ) -> None:
        """Assemble all four variables per daily fhour. cfgrib is not thread-safe.

        precip = cumulative(fh) - cumulative(fh-24); temp/wind/humidity are
        instantaneous snapshots at fh (no diff). Each assembled .npy uses the
        variable suffix expected by ``extract_forecast``.
        """
        safe_level = level.replace(" ", "_")
        # Each work item: (kind, npy_out, *grib_inputs).
        work: list[tuple] = []
        skipped_existing = 0
        skipped_missing = 0

        for init_date in init_dates:
            for daily_fh in daily_fhours:
                # ── precip (cumulative diff) ──
                npy_out = self._var_npy(init_date, daily_fh, safe_level)
                if npy_out.exists():
                    skipped_existing += 1
                else:
                    cur_grib = self._apcp_grib(init_date, daily_fh)
                    prev_fh = daily_fh - 24
                    # GFS publishes no f000 cumulative; daily_fh=24 has no predecessor.
                    prev_grib = self._apcp_grib(init_date, prev_fh) if prev_fh >= 24 else None
                    if not cur_grib.exists() or (prev_grib is not None and not prev_grib.exists()):
                        skipped_missing += 1
                    else:
                        work.append(("precip", npy_out, cur_grib, prev_grib))

                # ── temp (instant, K -> degC) ──
                temp_out = self._var_npy(init_date, daily_fh, "temp")
                if temp_out.exists():
                    skipped_existing += 1
                else:
                    g = self._record_grib(init_date, daily_fh, "temp")
                    if g.exists():
                        work.append(("temp", temp_out, g))
                    else:
                        skipped_missing += 1

                # ── humidity (instant, % direct) ──
                hum_out = self._var_npy(init_date, daily_fh, "humidity")
                if hum_out.exists():
                    skipped_existing += 1
                else:
                    g = self._record_grib(init_date, daily_fh, "humidity")
                    if g.exists():
                        work.append(("humidity", hum_out, g))
                    else:
                        skipped_missing += 1

                # ── wind (instant, hypot of u/v) ──
                wind_out = self._var_npy(init_date, daily_fh, "wind")
                if wind_out.exists():
                    skipped_existing += 1
                else:
                    ug = self._record_grib(init_date, daily_fh, "ugrd")
                    vg = self._record_grib(init_date, daily_fh, "vgrd")
                    if ug.exists() and vg.exists():
                        work.append(("wind", wind_out, ug, vg))
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
                    _convert_grib_to_npy(item[2], item[3], npy_out, self.output_dir)
                elif kind == "temp":
                    _convert_instant_to_npy(item[2], npy_out, self.output_dir, to_celsius=True)
                elif kind == "humidity":
                    _convert_instant_to_npy(item[2], npy_out, self.output_dir, to_celsius=False)
                elif kind == "wind":
                    _convert_wind_to_npy(item[2], item[3], npy_out, self.output_dir)
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
            for sub in ("_apcp", "_inst"):
                raw_dir = self._init_dir(init_date) / sub
                if raw_dir.exists():
                    for sidecar in raw_dir.glob("*.idx"):
                        sidecar.unlink(missing_ok=True)
        print(f"\nAssembly: {done} created | {skipped_existing} existed | {skipped_missing} skipped"
              + (f" | {failed} failed" if failed else ""))


if __name__ == "__main__":
    import argparse

    sys.path.insert(0, str(_this_dir.parent))
    from model_registry import MODEL_REGISTRY

    gfs_config = MODEL_REGISTRY["gfs"]

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

    forecast_hours = gfs_config.forecast_hours

    if args.start_year:
        downloader.download_year_range(
            start_year=args.start_year,
            end_year=args.end_year or args.start_year,
            level=args.level,
            forecast_hours=forecast_hours,
        )
    else:
        start = args.start_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        end = args.end_date or start
        downloader.download_date_range(
            start_date=start,
            end_date=end,
            level=args.level,
            forecast_hours=forecast_hours,
        )
