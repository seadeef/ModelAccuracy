#!/usr/bin/env python3

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


    def extract_forecast(
        self,
        init_date: datetime | None = None,
        forecast_hours: list[int] | None = None,
        lead_windows: list[tuple[int, int]] | None = None,
        output_root: Path | None = None,
    ) -> None:
        """Extract raw GFS precipitation forecast into stats_output/forecast/ format for tile generation."""
        import numpy as np
        import rasterio.transform
        import xarray as xr
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
            all_inits = sorted(gfs_dir.glob("*/*_12z"))
            if not all_inits:
                raise SystemExit("No GFS init directories found.")
            init_date = datetime.strptime(all_inits[-1].name[:8], "%Y%m%d")
            print(f"Using most recent init date: {init_date.date()}")

        date_str = init_date.strftime("%Y%m%d")
        init_dir = gfs_dir / str(init_date.year) / f"{date_str}_{GFS_CYCLE:02d}z"
        if not init_dir.exists():
            raise SystemExit(f"Init directory not found: {init_dir}")

        # Build land mask from an existing statistic's coverage.
        land_mask = None
        for stat_dir in sorted(output_root.iterdir()):
            if stat_dir.name == "forecast" or not stat_dir.is_dir():
                continue
            for npz_file in stat_dir.glob("lead_*.npz"):
                with np.load(npz_file) as f:
                    field = list(f.keys())[0]
                    land_mask = np.isfinite(f[field])
                break
            if land_mask is not None:
                print(f"Land mask from {npz_file} ({np.count_nonzero(land_mask)} valid pixels)")
                break

        if land_mask is None:
            print("Warning: no existing statistics found for land mask. Forecast will include ocean.")

        def _read_gfs_apcp(grib_path: Path):
            npy_path = grib_path.with_suffix(".npy")
            grid_lats = gfs_dir / "grid_lats.npy"
            grid_lons = gfs_dir / "grid_lons.npy"

            if npy_path.exists() and grid_lats.exists() and grid_lons.exists():
                return np.load(npy_path), np.load(grid_lats), np.load(grid_lons)

            ds = xr.open_dataset(grib_path, engine="cfgrib")
            if not ds.data_vars:
                raise ValueError(f"No variables found in {grib_path}")
            var_name = list(ds.data_vars)[0]
            data = ds[var_name].values.astype(np.float32)
            lats = ds["latitude"].values
            lons = ds["longitude"].values

            if lons.max() > 180:
                lons = ((lons + 180) % 360) - 180
            sort_idx = np.argsort(lons)
            lons = lons[sort_idx]
            data = data[:, sort_idx]

            if lats[0] > lats[-1]:
                lats = lats[::-1]
                data = data[::-1, :]

            return data, lats, lons

        def _gfs_transform(lats, lons):
            lat_res = abs(float(lats[1] - lats[0]))
            lon_res = abs(float(lons[1] - lons[0]))
            west = float(lons.min()) - lon_res / 2.0
            if lats[0] < lats[-1]:
                south = float(lats.min()) - lat_res / 2.0
                return rasterio.transform.Affine(lon_res, 0, west, 0, lat_res, south)
            else:
                north = float(lats.max()) + lat_res / 2.0
                return rasterio.transform.from_origin(west, north, lon_res, lat_res)

        def _grid_coords_from_transform(transform, height, width):
            lons = transform.c + (np.arange(width) + 0.5) * transform.a
            lats = transform.f + (np.arange(height) + 0.5) * transform.e
            return lats.astype(np.float32), lons.astype(np.float32)

        # Read all available lead days.
        lead_data: dict[int, np.ndarray] = {}
        transform = None

        for fhour in forecast_hours:
            lead_days = fhour // 24
            grib_path = init_dir / f"f{fhour:03d}_surface.grib2"
            if not grib_path.exists():
                print(f"  Skipping lead {lead_days} (missing {grib_path.name})")
                continue

            data, lats, lons = _read_gfs_apcp(grib_path)
            if transform is None:
                transform = _gfs_transform(lats, lons)

            if land_mask is not None and data.shape == land_mask.shape:
                data[~land_mask] = np.nan

            lead_data[lead_days] = data
            print(f"  Lead {lead_days}: {grib_path.name}")

        if not lead_data or transform is None:
            raise SystemExit("No forecast hours found.")

        # Write metadata.
        height, width = next(iter(lead_data.values())).shape
        meta_lats, meta_lons = _grid_coords_from_transform(transform, height, width)
        np.savez_compressed(
            forecast_dir / "metadata.npz",
            lats=meta_lats,
            lons=meta_lons,
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
