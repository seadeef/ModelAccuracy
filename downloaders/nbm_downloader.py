#!/usr/bin/env python3
"""NBM (National Blend of Models) downloader.

Downloads 6-hour APCP bucket GRIB2 files from the NOAA NBM S3 bucket,
then assembles them into daily precipitation totals (.npy) compatible
with the compute_stats pipeline.

NBM provides deterministic 6h precipitation buckets (QPF06) rather than
running totals.  A single daily total is reconstructed by summing four
consecutive 6h buckets:

    Day N precip = QPF06(h0-6) + QPF06(h6-12) + QPF06(h12-18) + QPF06(h18-24)

where the hours are relative to (N-1)*24 from the 12z init time.

The assembled output mirrors the GFS directory layout.  Daily leads are stored
as ``fHHH_surface.npy`` (``compute_stats`` discovers ``.npy`` as well as GRIB).

    model_data/nbm/{year}/{YYYYMMDD}_12z/f024_surface.npy   (daily sum)
    model_data/nbm/grid_lats.npy
    model_data/nbm/grid_lons.npy
"""

from __future__ import annotations

import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, timezone

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from base import BaseDownloader

NBM_CYCLE = 12
NBM_VARIABLE = "APCP"


def _assemble_one(item: tuple) -> None:
    """Worker function for parallel assembly (must be at module level for pickling).

    Reads four 6h GRIB2 files, sums precipitation, reprojects from Lambert
    Conformal to a regular lat/lon grid, and saves as ``.npy``.

    Reprojection uses nearest-neighbor resampling: bilinear smooths accumulated
    precip, does not preserve cell totals, and can smear fronts/coasts when
    moving from projected grids to lat/lon.
    """
    import warnings

    import numpy as np
    import rasterio
    from rasterio.warp import Resampling, reproject
    from rasterio.transform import Affine

    try:
        from rasterio.errors import NotGeoreferencedWarning as _RioNotGeo
    except Exception:  # pragma: no cover
        _RioNotGeo = None

    sub_paths_str, npy_out_str, transform_vals, target_w, target_h = item
    npy_out = Path(npy_out_str)
    target_transform = Affine(*transform_vals)

    # Read and sum the four 6h grids.
    total = None
    src_transform = None
    src_crs = None
    for grib_path in sub_paths_str:
        with rasterio.open(grib_path) as src:
            data = src.read(1).astype(np.float32)
            if total is None:
                total = data.copy()
                src_transform = src.transform
                src_crs = src.crs
            else:
                total += data

    # Reproject from Lambert Conformal to regular lat/lon.
    # Nearest avoids bilinear smoothing of QPF totals and boundary artifacts;
    # geometry still depends on rasterio/GRIB georeferencing being correct.
    dst = np.full((target_h, target_w), np.nan, dtype=np.float32)
    with warnings.catch_warnings():
        if _RioNotGeo is not None:
            warnings.simplefilter("ignore", _RioNotGeo)
        warnings.filterwarnings(
            "ignore",
            message=r".*[Nn]o geotransform.*|.*identity matrix will be returned.*",
            category=UserWarning,
        )
        reproject(
            source=total,
            destination=dst,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=target_transform,
            dst_crs="EPSG:4326",
            resampling=Resampling.nearest,
            dst_nodata=np.nan,
        )

    npy_out.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_out, dst)


@dataclass(frozen=True)
class DownloadTask:
    init_date: datetime
    fhour: int
    level: str


class NBMDownloaderParallel(BaseDownloader):
    def __init__(
        self,
        output_dir: str = "model_data/nbm",
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
            base_url="https://noaa-nbm-grib2-pds.s3.amazonaws.com",
            user_agent="NBMDownloaderParallel/1.0",
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

    # ── URL / path helpers ──────────────────────────────────────────

    def _paths(self, init_date: datetime, fhour: int) -> tuple[str, str]:
        """Return (grib_url, idx_url) for a single 6h bucket file."""
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{NBM_CYCLE:02d}"
        filename = f"blend.t{cycle_str}z.core.f{fhour:03d}.co.grib2"
        base = f"blend.{date_str}/{cycle_str}/core/{filename}"
        return f"{self.base_url}/{base}", f"{self.base_url}/{base}.idx"

    def _sub_grib_file(self, init_date: datetime, fhour: int, level: str) -> Path:
        """Path for an individual 6h bucket GRIB2 file (in _6h/ subdirectory).

        Raw 6h files are kept separate from the assembled daily files so that
        compute_stats (which scans the init directory) only discovers daily outputs.
        """
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{NBM_CYCLE:02d}"
        safe_level = level.replace(" ", "_")
        out_dir = self.output_dir / str(init_date.year) / f"{date_str}_{cycle_str}z" / "_6h"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"f{fhour:03d}_{safe_level}.grib2"

    def _daily_npy_file(self, init_date: datetime, daily_fhour: int, level: str) -> Path:
        """Path for the assembled daily .npy file (the one compute_stats reads)."""
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{NBM_CYCLE:02d}"
        safe_level = level.replace(" ", "_")
        out_dir = self.output_dir / str(init_date.year) / f"{date_str}_{cycle_str}z"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"f{daily_fhour:03d}_{safe_level}.npy"

    # ── .idx parsing ────────────────────────────────────────────────

    def _find_byte_range(self, idx_text: str, fhour: int):
        """Find the byte range of the deterministic 6h APCP bucket.

        Matches lines like:
            255:92356838:d=2026032012:APCP:surface:0-6 hour acc fcst:

        The target accumulation window is ``{fhour-6}-{fhour} hour acc fcst``.
        Probabilistic entries (with ``prob`` qualifiers) are skipped.
        """
        target_desc = f"{fhour - 6}-{fhour} hour acc fcst"
        lines = idx_text.strip().split("\n")
        start_byte = None
        end_byte = None
        for i, line in enumerate(lines):
            parts = line.split(":")
            if len(parts) < 6:
                continue
            var_code = parts[3].strip()
            lvl_desc = parts[4].strip()
            desc = parts[5].strip()
            # Skip probabilistic entries (parts[6] contains "prob ...")
            if len(parts) > 6 and parts[6].strip().startswith("prob"):
                continue
            if var_code == NBM_VARIABLE and lvl_desc == "surface" and desc == target_desc:
                start_byte = int(parts[1])
                if i + 1 < len(lines):
                    next_parts = lines[i + 1].split(":")
                    end_byte = int(next_parts[1]) - 1
                break
        return start_byte, end_byte

    # ── Single-file download (parallel task) ────────────────────────

    def _download_task(self, task: DownloadTask) -> tuple[DownloadTask, str]:
        init_date, fhour, level = task.init_date, task.fhour, task.level
        out_file = self._sub_grib_file(init_date, fhour, level)
        if out_file.exists():
            return task, "exists"

        grib_url, idx_url = self._paths(init_date, fhour)
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
                start_byte, end_byte = self._find_byte_range(idx_resp.text, fhour)
                if start_byte is None:
                    return task, "not_found_var"
                headers = (
                    {"Range": f"bytes={start_byte}-{end_byte}"}
                    if end_byte is not None
                    else {"Range": f"bytes={start_byte}-"}
                )
                resp = self.session.get(
                    grib_url, headers=headers, stream=True, timeout=self.timeout_seconds,
                )
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

    # ── Daily assembly ──────────────────────────────────────────────

    @staticmethod
    def _sub_fhours_for_daily(daily_fhour: int) -> list[int]:
        """Return the four 6h sub-hours that make up a daily total.

        For daily_fhour=48 (lead day 2): returns [30, 36, 42, 48].
        """
        return [daily_fhour - 18, daily_fhour - 12, daily_fhour - 6, daily_fhour]

    # Target regular lat/lon grid matching GFS 0.25° resolution over CONUS.
    _TARGET_RES = 0.25
    _TARGET_BOUNDS = (-130.0, 20.0, -60.0, 55.0)  # west, south, east, north

    def _target_grid(self):
        """Return (transform, width, height, lats_1d, lons_1d) for the regular target grid.

        Row 0 is the southernmost latitude (y increases northward in row index).
        This matches ``compute_stats._gfs_transform`` when GFS latitudes are stored
        south→north (ascending ``lats``). A north-up affine would require flipping
        stored arrays and aligning the whole pipeline; do not change one side only.
        """
        import numpy as np
        import rasterio.transform

        west, south, east, north = self._TARGET_BOUNDS
        res = self._TARGET_RES
        lons_1d = np.arange(west + res / 2, east, res).astype(np.float32)
        lats_1d = np.arange(south + res / 2, north, res).astype(np.float32)
        width = len(lons_1d)
        height = len(lats_1d)
        # South edge origin, positive y step: row i centers at south + (i + 0.5) * res
        transform = rasterio.transform.Affine(res, 0, west, 0, res, south)
        return transform, width, height, lats_1d, lons_1d

    def _assemble_daily(
        self,
        init_dates: list[datetime],
        daily_fhours: list[int],
        level: str = "surface",
    ) -> None:
        """Sum four 6h GRIB2 files, reproject to regular lat/lon, save as .npy.

        NBM uses a Lambert Conformal grid that compute_stats cannot handle
        directly.  This method reprojects the summed daily precipitation onto
        a regular 0.25° lat/lon grid matching GFS resolution, so that
        compute_stats and the PRISM reprojection work correctly.

        Uses multiprocessing to parallelize the per-lead-day work (read 4
        GRIB2s, sum, reproject, save).

        Creates:
        - ``f{fhour}_surface.npy``: reprojected daily precipitation
        - ``grid_lats.npy`` / ``grid_lons.npy``: 1D grid coordinates
        """
        import numpy as np
        from concurrent.futures import ProcessPoolExecutor, as_completed

        target_transform, target_w, target_h, lats_1d, lons_1d = self._target_grid()

        # Save grid coords eagerly (all workers produce the same grid).
        if not (self.output_dir / "grid_lats.npy").exists():
            np.save(self.output_dir / "grid_lats.npy", lats_1d)
            np.save(self.output_dir / "grid_lons.npy", lons_1d)

        # Build work items: (sub_paths, npy_out, target args).
        work: list[tuple] = []
        skipped_existing = 0
        skipped_missing = 0

        for init_date in init_dates:
            for daily_fh in daily_fhours:
                npy_out = self._daily_npy_file(init_date, daily_fh, level)
                if npy_out.exists():
                    skipped_existing += 1
                    continue

                sub_fhours = self._sub_fhours_for_daily(daily_fh)
                sub_paths = [self._sub_grib_file(init_date, fh, level) for fh in sub_fhours]
                if not all(p.exists() for p in sub_paths):
                    skipped_missing += 1
                    continue

                work.append((
                    [str(p) for p in sub_paths],
                    str(npy_out),
                    tuple(target_transform)[:6],
                    target_w,
                    target_h,
                ))

        total = len(work)
        if total == 0:
            print(f"\nAssembly: 0 daily files created | "
                  f"{skipped_existing} already existed | "
                  f"{skipped_missing} skipped (missing 6h files)")
            return

        workers = min(self.max_workers, total)
        print(f"  {total} lead-days to assemble with {workers} workers...")

        assembled = 0
        failed = 0
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_assemble_one, item): item for item in work}
            for fut in as_completed(futures):
                try:
                    fut.result()
                    assembled += 1
                except Exception as exc:
                    failed += 1
                    item = futures[fut]
                    if failed <= 5:
                        print(f"  WARNING: assembly failed for {item[1]}: {exc}")
                done = assembled + failed
                if done % 500 == 0 or done == total:
                    print(f"  Assembly progress: {done}/{total}", flush=True)

        print(f"\nAssembly: {assembled} daily files created | "
              f"{skipped_existing} already existed | "
              f"{skipped_missing} skipped (missing 6h files)"
              + (f" | {failed} failed" if failed else ""))

    # ── Public download interface ───────────────────────────────────

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
        # Build init date list.
        init_dates: list[datetime] = []
        cur = datetime(start.year, start.month, start.day)
        end0 = datetime(end.year, end.month, end.day)
        while cur <= end0:
            init_dates.append(cur)
            cur += timedelta(days=1)
        if not init_dates:
            raise ValueError("No init dates selected. Check start/end dates.")

        # Expand daily forecast_hours to 6h sub-hours.
        all_sub_fhours: set[int] = set()
        for daily_fh in forecast_hours:
            all_sub_fhours.update(self._sub_fhours_for_daily(daily_fh))
        sorted_sub_fhours = sorted(all_sub_fhours)

        # Build download task list (one task per 6h file).
        tasks: list[DownloadTask] = []
        skipped = 0
        for d in init_dates:
            for fh in sorted_sub_fhours:
                t = DownloadTask(d, int(fh), str(level))
                if self._sub_grib_file(t.init_date, t.fhour, t.level).exists():
                    skipped += 1
                else:
                    tasks.append(t)

        print("\n" + "=" * 70)
        print("Parallel NBM filtered download (idx + Range)")
        print(f"Period: {start.date()} to {end.date()} | Daily")
        print(f"Cycle: {NBM_CYCLE:02d}z")
        print(f"Daily forecast hours: {forecast_hours}")
        print(f"6h sub-hours: {sorted_sub_fhours}")
        print(f"Variable: {NBM_VARIABLE} | Level: {level}")
        print(f"Workers: {self.max_workers} | Retries: {self.max_retries}")
        print(f"Output: {self.output_dir.resolve()}")
        print(f"Tasks: {len(tasks)} to download | {skipped} already exist")
        print("=" * 70)

        results = self._run_parallel(
            tasks,
            self._download_task,
            description="NBM download",
            progress_interval=50,
        )

        examples: list[str] = []
        for task, status in results:
            if status == "not_found_idx" and len(examples) < 5:
                examples.append(f"Missing idx: {task.init_date:%Y-%m-%d} {NBM_CYCLE:02d}z f{task.fhour:03d}")
            elif status == "not_found_var" and len(examples) < 5:
                examples.append(f"Var not found: {NBM_VARIABLE} @ {task.level} in {task.init_date:%Y-%m-%d} {NBM_CYCLE:02d}z f{task.fhour:03d}")
            elif status.startswith("failed") and len(examples) < 5:
                examples.append(f"Failed: {task.init_date:%Y-%m-%d} {NBM_CYCLE:02d}z f{task.fhour:03d} -> {status}")
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print("-" * 70)

        # Assemble 6h files into daily totals.
        print("\nAssembling 6h buckets into daily precipitation totals...")
        self._assemble_daily(init_dates, forecast_hours, level)

    # ── Forecast extraction ─────────────────────────────────────────

    def extract_forecast(
        self,
        init_date: datetime | None = None,
        forecast_hours: list[int] | None = None,
        lead_windows: list[tuple[int, int]] | None = None,
        output_root: Path | None = None,
    ) -> None:
        """Extract assembled NBM daily precipitation into stats_output/forecast/ format.

        The assembled .npy files are already on a regular 0.25° lat/lon grid,
        so no additional reprojection is needed.
        """
        import numpy as np
        import rasterio.transform
        from model_registry import window_to_key

        nbm_dir = self.output_dir
        if output_root is None:
            output_root = Path("stats_output")
        forecast_dir = output_root / "forecast"
        forecast_dir.mkdir(parents=True, exist_ok=True)

        if forecast_hours is None:
            forecast_hours = [d * 24 for d in range(1, 12)]
        if lead_windows is None:
            lead_windows = []

        # Find init directory.
        if init_date is None:
            all_inits = sorted(nbm_dir.glob(f"*/*_{NBM_CYCLE:02d}z"), reverse=True)
            for candidate in all_inits:
                if any(candidate.glob("f*_*.npy")):
                    init_date = datetime.strptime(candidate.name[:8], "%Y%m%d")
                    break
            if init_date is None:
                raise SystemExit("No NBM init directories with assembled data found.")
            print(f"Using most recent init date: {init_date.date()}")

        date_str = init_date.strftime("%Y%m%d")
        init_dir = nbm_dir / str(init_date.year) / f"{date_str}_{NBM_CYCLE:02d}z"
        if not init_dir.exists():
            raise SystemExit(f"Init directory not found: {init_dir}")

        grid_lats_path = nbm_dir / "grid_lats.npy"
        grid_lons_path = nbm_dir / "grid_lons.npy"
        if not grid_lats_path.exists() or not grid_lons_path.exists():
            raise SystemExit("Grid coordinate files not found. Run download + assembly first.")

        lats = np.load(grid_lats_path)
        lons = np.load(grid_lons_path)
        # Build transform from the regular 1D grid coords.
        target_transform, _, _, _, _ = self._target_grid()

        lead_data: dict[int, np.ndarray] = {}
        for fhour in forecast_hours:
            lead_days = fhour // 24
            npy_path = init_dir / f"f{fhour:03d}_surface.npy"
            if not npy_path.exists():
                print(f"  Skipping lead {lead_days} (missing {npy_path.name})")
                continue
            lead_data[lead_days] = np.load(npy_path)
            print(f"  Lead {lead_days}: {npy_path.name}")

        if not lead_data:
            raise SystemExit("No forecast data found.")

        np.savez_compressed(
            forecast_dir / "metadata.npz",
            lats=lats,
            lons=lons,
            transform=np.array(target_transform),
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
        print(f"Init date: {init_date.date()} {NBM_CYCLE:02d}z")


if __name__ == "__main__":
    import argparse

    sys.path.insert(0, str(_this_dir.parent))
    from model_registry import MODEL_REGISTRY

    nbm_config = MODEL_REGISTRY["nbm"]

    parser = argparse.ArgumentParser(description="Download NBM APCP forecasts")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD). Defaults to start date.")
    parser.add_argument("--start-year", type=int, help="Start year (downloads full years)")
    parser.add_argument("--end-year", type=int, help="End year (downloads full years)")
    parser.add_argument("--level", default="surface")
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    downloader = NBMDownloaderParallel(
        output_dir="model_data/nbm",
        max_workers=args.workers,
        max_retries=3,
        timeout_seconds=120,
        polite_delay_seconds=0.0,
    )

    forecast_hours = nbm_config.forecast_hours

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
