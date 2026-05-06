#!/usr/bin/env python3
"""GFS downloader.

Downloads APCP (precipitation) GRIB2 files from the NOAA GFS S3 bucket
using byte-range requests via .idx index files.

GFS publishes APCP as a `0-N day acc fcst` cumulative bucket at every daily
forecast hour (f024, f048, ..., f336). To get a daily total ending at lead
day D we subtract:
    daily_total = cumulative(D*24) - cumulative((D-1)*24)
This mirrors the AIFS pipeline (`aifs_downloader.py`).

Output:
    model_data/gfs/{year}/{YYYYMMDD}_{HH}z/f024_surface.npy ... f336_surface.npy
    model_data/gfs/{year}/{YYYYMMDD}_{HH}z/_apcp/f024.grib2 ...   (raw cumulative APCP)
    model_data/gfs/grid_lats.npy
    model_data/gfs/grid_lons.npy
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, timezone

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from base import BaseDownloader

# Fixed 12z cycle for GFS; used in remote paths and local dir names.
GFS_CYCLE = 12
GFS_VARIABLE = "APCP"

# CONUS crop bounds (must match compute_stats / NBM target grid).
US_CROP_BOUNDS = (-130.0, 20.0, -60.0, 55.0)


def _convert_grib_to_npy(grib_path: Path, prev_grib_path: Path | None, npy_path: Path, output_dir: Path) -> None:
    """Compute APCP(fh) - APCP(fh-24), crop to CONUS, save as .npy. cfgrib is not thread-safe."""
    import numpy as np
    import xarray as xr

    if npy_path.exists():
        return

    ds = xr.open_dataset(grib_path, engine="cfgrib")
    if not ds.data_vars:
        ds.close()
        return
    var = list(ds.data_vars)[0]
    cur = ds[var].values.astype(np.float32)
    lats = ds["latitude"].values
    lons = ds["longitude"].values
    ds.close()

    if prev_grib_path is not None and prev_grib_path.exists():
        ds_prev = xr.open_dataset(prev_grib_path, engine="cfgrib")
        var_p = list(ds_prev.data_vars)[0]
        prev = ds_prev[var_p].values.astype(np.float32)
        ds_prev.close()
        diff = cur - prev
    else:
        # No predecessor (f024 has no f000 cumulative — model starts at 0).
        diff = cur

    # Normalize grid: -180..180 longitude, south-to-north latitude.
    if lons.max() > 180:
        lons = ((lons + 180) % 360) - 180
        sort_idx = np.argsort(lons)
        lons = lons[sort_idx]
        diff = diff[:, sort_idx]
    if lats[0] > lats[-1]:
        lats = lats[::-1]
        diff = diff[::-1, :]

    west, south, east, north = US_CROP_BOUNDS
    lat_mask = (lats >= south) & (lats <= north)
    lon_mask = (lons >= west) & (lons <= east)
    lat_idx = np.where(lat_mask)[0]
    lon_idx = np.where(lon_mask)[0]
    cropped = diff[np.ix_(lat_idx, lon_idx)]
    kept_lats = lats[lat_idx]
    kept_lons = lons[lon_idx]

    # APCP is kg m^-2 == mm; clip tiny negative noise from subtraction.
    np.clip(cropped, 0.0, None, out=cropped)

    npy_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_path, cropped)

    grid_lats_p = output_dir / "grid_lats.npy"
    grid_lons_p = output_dir / "grid_lons.npy"
    if not grid_lats_p.exists():
        np.save(grid_lats_p, kept_lats)
        np.save(grid_lons_p, kept_lons)


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
        init_date, fhour, level = task.init_date, task.fhour, task.level
        npy_file = self._output_npy(init_date, fhour, level)
        if npy_file.exists():
            return task, "exists"

        grib_file = self._apcp_grib(init_date, fhour)
        if grib_file.exists():
            return task, "exists"

        grib_filename, grib_path, idx_path = self._paths(init_date, fhour)
        idx_url = f"{self.base_url}/{idx_path}"
        grib_url = f"{self.base_url}/{grib_path}"
        last_err = None

        if self.polite_delay_seconds:
            time.sleep(self.polite_delay_seconds)

        for attempt in range(1, self.max_retries + 1):
            part = grib_file.with_suffix(grib_file.suffix + ".part")
            try:
                idx_resp = self.session.get(idx_url, timeout=self.timeout_seconds)
                if idx_resp.status_code == 404:
                    return task, "not_found_idx"
                idx_resp.raise_for_status()
                start_byte, end_byte = self._find_byte_range(idx_resp.text, level, fhour)
                if start_byte is None:
                    return task, "not_found_var"
                headers = {"Range": f"bytes={start_byte}-{end_byte}"} if end_byte is not None else {"Range": f"bytes={start_byte}-"}
                resp = self.session.get(grib_url, headers=headers, stream=True, timeout=self.timeout_seconds)
                resp.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                # Validate GRIB2 magic bytes before accepting.
                with open(part, "rb") as check:
                    magic = check.read(4)
                if magic != b"GRIB":
                    part.unlink(missing_ok=True)
                    last_err = ValueError(f"Invalid GRIB2 (magic={magic!r})")
                    time.sleep(1.25 * attempt)
                    continue
                part.replace(grib_file)
                size_kb = grib_file.stat().st_size / 1024
                return task, f"downloaded ({size_kb:.1f} KB)"
            except Exception as e:
                last_err = e
                if part.exists():
                    part.unlink(missing_ok=True)
                if grib_file.exists():
                    grib_file.unlink(missing_ok=True)
                time.sleep(1.25 * attempt)
        return task, f"failed: {last_err}"

    def download_date_range(
        self,
        start_date: str | datetime,
        end_date: str | datetime,
        *,
        forecast_hours: list[int],
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
        forecast_hours: list[int],
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

        tasks: list[DownloadTask] = []
        skipped = 0
        for d in init_dates:
            # If every daily total is already assembled for this init, skip the init entirely.
            if all(self._output_npy(d, fh, level).exists() for fh in daily_fhours):
                skipped += 1
                continue
            for fh in daily_fhours:
                # Skip download if cumulative grib already on disk OR daily already assembled.
                if self._apcp_grib(d, fh).exists() or self._output_npy(d, fh, level).exists():
                    continue
                tasks.append(DownloadTask(d, fh, str(level)))

        print("\n" + "=" * 70)
        print("Parallel GFS filtered download (idx + Range)")
        print(f"Period: {start.date()} to {end.date()} | Daily")
        print(f"Cycle: {GFS_CYCLE:02d}z")
        print(f"Forecast hours: {daily_fhours}")
        print(f"Variable: {GFS_VARIABLE} (cumulative 0-N day acc) | Level: {level}")
        print(f"Workers: {self.max_workers} | Retries: {self.max_retries}")
        print(f"Output: {self.output_dir.resolve()}")
        print(f"Tasks: {len(tasks)} cumulative grib2 to download | {skipped} inits already complete")
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
                examples.append(f"Var not found: {GFS_VARIABLE} @ {task.level} 0-{task.fhour//24}d in {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d}")
            elif status.startswith("failed") and len(examples) < 5:
                examples.append(f"Failed: {task.init_date:%Y-%m-%d} {GFS_CYCLE:02d}z f{task.fhour:03d} ({GFS_VARIABLE}@{task.level}) -> {status}")
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print("-" * 70)

        print("\nAssembling daily totals from cumulative APCP...")
        self._assemble_daily(init_dates, daily_fhours, level)


    def _assemble_daily(
        self,
        init_dates: list[datetime],
        daily_fhours: list[int],
        level: str,
    ) -> None:
        """Compute daily = cumulative(fh) - cumulative(fh-24); write .npy. cfgrib is not thread-safe."""
        work: list[tuple[Path, Path | None, Path]] = []
        skipped_existing = 0
        skipped_missing = 0

        for init_date in init_dates:
            for daily_fh in daily_fhours:
                npy_out = self._output_npy(init_date, daily_fh, level)
                if npy_out.exists():
                    skipped_existing += 1
                    continue
                cur_grib = self._apcp_grib(init_date, daily_fh)
                prev_fh = daily_fh - 24
                # GFS publishes no f000 cumulative; for daily_fh=24 there is no predecessor.
                prev_grib = self._apcp_grib(init_date, prev_fh) if prev_fh >= 24 else None
                if not cur_grib.exists():
                    skipped_missing += 1
                    continue
                if prev_grib is not None and not prev_grib.exists():
                    skipped_missing += 1
                    continue
                work.append((cur_grib, prev_grib, npy_out))

        if not work:
            print(f"\nAssembly: 0 created | {skipped_existing} existed | {skipped_missing} skipped (missing apcp)")
            return

        print(f"  {len(work)} daily files to assemble (sequential cfgrib)...")
        done = 0
        failed = 0
        for cur, prev, npy_out in work:
            try:
                _convert_grib_to_npy(cur, prev, npy_out, self.output_dir)
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
            apcp_dir = self._init_dir(init_date) / "_apcp"
            if apcp_dir.exists():
                for sidecar in apcp_dir.glob("*.idx"):
                    sidecar.unlink(missing_ok=True)
        print(f"\nAssembly: {done} created | {skipped_existing} existed | {skipped_missing} skipped"
              + (f" | {failed} failed" if failed else ""))


    def extract_forecast(
        self,
        init_date: datetime | None = None,
        forecast_hours: list[int] | None = None,
        lead_windows: list[tuple[int, int]] | None = None,
        output_root: Path | None = None,
    ) -> None:
        """Extract GFS .npy forecast data into stats_output/forecast/ format for tile generation."""
        import numpy as np
        import rasterio.transform
        from model_registry import window_to_key

        gfs_dir = self.output_dir
        if output_root is None:
            output_root = Path("stats_output")
        forecast_dir = output_root / "forecast"
        forecast_dir.mkdir(parents=True, exist_ok=True)

        if forecast_hours is None:
            forecast_hours = [d * 24 for d in range(1, 15)]
        if lead_windows is None:
            lead_windows = []

        # Find init directory.
        if init_date is None:
            all_inits = sorted(gfs_dir.glob(f"*/*_{GFS_CYCLE:02d}z"), reverse=True)
            for candidate in all_inits:
                if any(candidate.glob("f*_*.npy")):
                    init_date = datetime.strptime(candidate.name[:8], "%Y%m%d")
                    break
            if init_date is None:
                raise SystemExit("No GFS init directories with data found.")
            print(f"Using most recent init date: {init_date.date()}")

        date_str = init_date.strftime("%Y%m%d")
        init_dir = gfs_dir / str(init_date.year) / f"{date_str}_{GFS_CYCLE:02d}z"
        if not init_dir.exists():
            raise SystemExit(f"Init directory not found: {init_dir}")

        grid_lats_path = gfs_dir / "grid_lats.npy"
        grid_lons_path = gfs_dir / "grid_lons.npy"
        if not grid_lats_path.exists() or not grid_lons_path.exists():
            raise SystemExit("Grid coordinate files not found. Run download first.")

        lats = np.load(grid_lats_path)
        lons = np.load(grid_lons_path)

        # Build transform from the regular 1D grid coords.
        lat_res = abs(float(lats[1] - lats[0]))
        lon_res = abs(float(lons[1] - lons[0]))
        west = float(lons.min()) - lon_res / 2.0
        south = float(lats.min()) - lat_res / 2.0
        transform = rasterio.transform.Affine(lon_res, 0, west, 0, lat_res, south)

        land_mask = self._build_land_mask(lats, lons, transform)

        # Read all available lead days.
        lead_data: dict[int, np.ndarray] = {}
        for fhour in forecast_hours:
            lead_days = fhour // 24
            npy_path = init_dir / f"f{fhour:03d}_surface.npy"
            if not npy_path.exists():
                print(f"  Skipping lead {lead_days} (missing {npy_path.name})")
                continue
            lead_data[lead_days] = self._apply_land_mask(np.load(npy_path), land_mask)
            print(f"  Lead {lead_days}: {npy_path.name}")

        if not lead_data:
            raise SystemExit("No forecast data found.")

        # Write metadata.
        np.savez_compressed(
            forecast_dir / "metadata.npz",
            lats=lats,
            lons=lons,
            transform=np.array(transform),
            crs="EPSG:4326",
            init_date=init_date.strftime("%Y-%m-%d"),
        )

        # Write individual lead days.
        for lead_days, data in sorted(lead_data.items()):
            np.savez_compressed(forecast_dir / f"lead_{lead_days}.npz", precip=data)
            print(f"  Wrote lead_{lead_days}.npz")

        # Write lead windows.
        for start, end in lead_windows:
            leads_in_window = [ld for ld in sorted(lead_data) if start <= ld <= end]
            expected = end - start + 1
            if len(leads_in_window) != expected:
                print(f"  Skipping window {start}-{end} (have {len(leads_in_window)}/{expected} leads)")
                continue
            avg = np.mean([lead_data[ld] for ld in leads_in_window], axis=0)
            wkey = window_to_key(start, end)
            np.savez_compressed(forecast_dir / f"lead_{wkey}.npz", precip=avg)
            print(f"  Wrote lead_{wkey}.npz (avg of leads {start}-{end})")

        print(f"\nWrote {len(lead_data)} lead files + windows to {forecast_dir}")
        print(f"Init date: {init_date.date()} {GFS_CYCLE:02d}z")


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
