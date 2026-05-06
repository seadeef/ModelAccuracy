#!/usr/bin/env python3
"""AIFS downloader (ECMWF Open Data, aifs-single 0.25°).

Source: s3://ecmwf-forecasts/{YYYYMMDD}/{HH}z/aifs-single/0p25/oper/
        {YYYYMMDDHH0000}-{N}h-oper-fc.grib2  (every 6h, 0..360h)

`tp` is total precipitation accumulated *from forecast start*. The GRIB attr
`GRIB_units` reports `kg m**-2` (≡ mm of water), so no scaling is applied —
unlike the GraphCast pipeline whose source delivers meters.
To get a daily total ending at lead day D we subtract:
    daily_total = tp(D*24) - tp((D-1)*24)

We download tp at f0, f24, f48, ..., f336 per init via byte-range using the
sidecar .index (JSONL with _offset/_length), then assemble per-day .npy files
on a regular 0.25° lat/lon CONUS grid.

Output (matches gfs_downloader layout):
    model_data/aifs/{year}/{YYYYMMDD}_{HH}z/f024_surface.npy ... f336_surface.npy
    model_data/aifs/{year}/{YYYYMMDD}_{HH}z/_tp/f000.grib2 ...   (raw cumulative tp)
    model_data/aifs/grid_lats.npy
    model_data/aifs/grid_lons.npy
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from base import BaseDownloader

AIFS_CYCLE = 12
AIFS_VARIABLE = "tp"
AIFS_MAX_LEAD_H = 336  # 14 days, clipped from native 360h
AIFS_FIRST_DATE = datetime(2025, 3, 1)  # earliest archived AIFS-single
US_CROP_BOUNDS = (-130.0, 20.0, -60.0, 55.0)


@dataclass(frozen=True)
class DownloadTask:
    init_date: datetime
    cycle: int
    fhour: int  # raw lead hour in source (0, 24, 48, ..., 336)


def _convert_grib_to_npy(grib_path: Path, prev_grib_path: Path | None, npy_path: Path, output_dir: Path) -> None:
    """Compute tp(fh) - tp(fh-24), crop to CONUS, save as .npy. cfgrib is not thread-safe."""
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
        # No predecessor (e.g. f24 with no f0) — treat tp as already a 24h delta starting from 0.
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

    # AIFS tp units are kg m^-2 == mm; just clip tiny negative noise.
    np.clip(cropped, 0.0, None, out=cropped)

    npy_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_path, cropped)

    grid_lats_p = output_dir / "grid_lats.npy"
    grid_lons_p = output_dir / "grid_lons.npy"
    if not grid_lats_p.exists():
        np.save(grid_lats_p, kept_lats)
        np.save(grid_lons_p, kept_lons)


class AIFSDownloaderParallel(BaseDownloader):
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

    def _daily_npy(self, init_date: datetime, cycle: int, daily_fhour: int, level: str) -> Path:
        return self._init_dir(init_date, cycle) / f"f{daily_fhour:03d}_{level}.npy"

    def _find_byte_range(self, idx_text: str) -> tuple[int | None, int | None]:
        """Parse JSONL .index file; return (offset, length) for tp."""
        for line in idx_text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("param") == AIFS_VARIABLE:
                off = d.get("_offset")
                ln = d.get("_length")
                if off is None or ln is None:
                    return None, None
                return int(off), int(ln)
        return None, None

    def _download_task(self, task: DownloadTask) -> tuple[DownloadTask, str]:
        init_date, cycle, fhour = task.init_date, task.cycle, task.fhour
        out_file = self._tp_grib(init_date, cycle, fhour)
        if out_file.exists():
            return task, "exists"

        grib_url, idx_url = self._remote_paths(init_date, cycle, fhour)
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
                offset, length = self._find_byte_range(idx_resp.text)
                if offset is None:
                    return task, "not_found_var"
                end_byte = offset + length - 1
                headers = {"Range": f"bytes={offset}-{end_byte}"}
                resp = self.session.get(grib_url, headers=headers, stream=True, timeout=self.timeout_seconds)
                resp.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                with open(part, "rb") as check:
                    magic = check.read(4)
                if magic != b"GRIB":
                    part.unlink(missing_ok=True)
                    last_err = ValueError(f"Invalid GRIB2 (magic={magic!r})")
                    time.sleep(1.25 * attempt)
                    continue
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

    def _assemble_daily(
        self,
        init_dates: list[datetime],
        cycle: int,
        daily_fhours: list[int],
        level: str,
    ) -> None:
        """Compute tp(fh) - tp(fh-24) per daily fhour, write .npy. cfgrib is not thread-safe."""
        work: list[tuple[Path, Path | None, Path]] = []
        skipped_existing = 0
        skipped_missing = 0

        for init_date in init_dates:
            for daily_fh in daily_fhours:
                npy_out = self._daily_npy(init_date, cycle, daily_fh, level)
                if npy_out.exists():
                    skipped_existing += 1
                    continue
                cur_grib = self._tp_grib(init_date, cycle, daily_fh)
                prev_fh = daily_fh - 24
                prev_grib = self._tp_grib(init_date, cycle, prev_fh) if prev_fh >= 0 else None
                if not cur_grib.exists():
                    skipped_missing += 1
                    continue
                if prev_grib is not None and not prev_grib.exists():
                    skipped_missing += 1
                    continue
                work.append((cur_grib, prev_grib, npy_out))

        if not work:
            print(f"\nAssembly: 0 created | {skipped_existing} existed | {skipped_missing} skipped (missing tp)")
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
            tp_dir = self._init_dir(init_date, cycle) / "_tp"
            if tp_dir.exists():
                for sidecar in tp_dir.glob("*.idx"):
                    sidecar.unlink(missing_ok=True)
        print(f"\nAssembly: {done} created | {skipped_existing} existed | {skipped_missing} skipped"
              + (f" | {failed} failed" if failed else ""))

    # ── Public download interface ───────────────────────────────────

    def download_date_range(self, start_date, end_date, *, forecast_hours=None, level: str = "surface"):
        start = self._to_dt(start_date)
        end = self._to_dt(end_date)
        return self._download(start, end, level=level)

    def download_year_range(self, start_year: int, end_year: int, *, forecast_hours=None, level: str = "surface"):
        start = datetime(int(start_year), 1, 1)
        end = datetime(int(end_year), 12, 31)
        return self._download(start, end, level=level)

    def _download(self, start: datetime, end: datetime, *, level: str = "surface"):
        # Clip to AIFS archive availability.
        if start < AIFS_FIRST_DATE:
            print(f"AIFS archive starts {AIFS_FIRST_DATE.date()}; clipping start.")
            start = AIFS_FIRST_DATE
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
        # Need tp at f0 and at every daily_fhour.
        needed_fhours = sorted({0, *daily_fhours})

        tasks: list[DownloadTask] = []
        skipped = 0
        for d in init_dates:
            # If all dailies already exist for this init, skip the whole init.
            if all(self._daily_npy(d, cycle, fh, level).exists() for fh in daily_fhours):
                skipped += 1
                continue
            for fh in needed_fhours:
                if not self._tp_grib(d, cycle, fh).exists():
                    tasks.append(DownloadTask(d, cycle, fh))

        print("\n" + "=" * 70)
        print("Parallel AIFS (ECMWF) download")
        print(f"Period: {start.date()} to {end.date()} | Daily")
        print(f"Cycle: {cycle:02d}z | Lead 1-{AIFS_MAX_LEAD_H // 24} days")
        print(f"tp fhours per init: {needed_fhours}")
        print(f"Workers: {self.max_workers} | Retries: {self.max_retries}")
        print(f"Output: {self.output_dir.resolve()}")
        print(f"Tasks: {len(tasks)} grib2 to download | {skipped} inits already complete")
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
                examples.append(f"Missing idx: {task.init_date:%Y-%m-%d} {task.cycle:02d}z f{task.fhour:03d}")
            elif status == "not_found_var" and len(examples) < 5:
                examples.append(f"tp not found: {task.init_date:%Y-%m-%d} {task.cycle:02d}z f{task.fhour:03d}")
            elif status.startswith("failed") and len(examples) < 5:
                examples.append(f"Failed: {task.init_date:%Y-%m-%d} {task.cycle:02d}z f{task.fhour:03d} -> {status}")
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print("-" * 70)

        print("\nAssembling daily totals from cumulative tp...")
        self._assemble_daily(init_dates, cycle, daily_fhours, level)

    # ── Forecast extraction ─────────────────────────────────────────

    def extract_forecast(
        self,
        init_date: datetime | None = None,
        forecast_hours: list[int] | None = None,
        lead_windows: list[tuple[int, int]] | None = None,
        output_root: Path | None = None,
    ) -> None:
        import numpy as np
        import rasterio.transform
        from model_registry import window_to_key

        aifs_dir = self.output_dir
        if output_root is None:
            output_root = Path("stats_output")
        forecast_dir = output_root / "forecast"
        forecast_dir.mkdir(parents=True, exist_ok=True)

        if forecast_hours is None:
            forecast_hours = [d * 24 for d in range(1, AIFS_MAX_LEAD_H // 24 + 1)]
        if lead_windows is None:
            lead_windows = []

        if init_date is None:
            all_inits = sorted(aifs_dir.glob(f"*/*_{AIFS_CYCLE:02d}z"), reverse=True)
            for candidate in all_inits:
                if any(candidate.glob("f*_*.npy")):
                    init_date = datetime.strptime(candidate.name[:8], "%Y%m%d")
                    break
            if init_date is None:
                raise SystemExit("No AIFS init directories with data found.")
            print(f"Using most recent init date: {init_date.date()}")

        date_str = init_date.strftime("%Y%m%d")
        init_dir = aifs_dir / str(init_date.year) / f"{date_str}_{AIFS_CYCLE:02d}z"
        if not init_dir.exists():
            raise SystemExit(f"Init directory not found: {init_dir}")

        grid_lats_path = aifs_dir / "grid_lats.npy"
        grid_lons_path = aifs_dir / "grid_lons.npy"
        if not grid_lats_path.exists() or not grid_lons_path.exists():
            raise SystemExit("Grid coordinate files not found. Run download first.")

        lats = np.load(grid_lats_path)
        lons = np.load(grid_lons_path)
        lat_res = abs(float(lats[1] - lats[0]))
        lon_res = abs(float(lons[1] - lons[0]))
        west = float(lons.min()) - lon_res / 2.0
        south = float(lats.min()) - lat_res / 2.0
        transform = rasterio.transform.Affine(lon_res, 0, west, 0, lat_res, south)

        land_mask = self._build_land_mask(lats, lons, transform)

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

        np.savez_compressed(
            forecast_dir / "metadata.npz",
            lats=lats,
            lons=lons,
            transform=np.array(transform),
            crs="EPSG:4326",
            init_date=init_date.strftime("%Y-%m-%d"),
        )

        for lead_days, data in sorted(lead_data.items()):
            np.savez_compressed(forecast_dir / f"lead_{lead_days}.npz", precip=data)
            print(f"  Wrote lead_{lead_days}.npz")

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
        print(f"Init date: {init_date.date()} {AIFS_CYCLE:02d}z")


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
