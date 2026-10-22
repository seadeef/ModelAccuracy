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

Besides precip, NBM exposes instantaneous 2 m / 10 m fields directly in the
daily-fhour core file (snapshot at the lead's valid time, descriptor
"{fh} hour fcst"); each is reprojected from Lambert to the same regular grid:
    TMP:2 m above ground   -> temp     (K -> degC)
    RH:2 m above ground    -> humidity (%, direct)
    WIND:10 m above ground -> wind     (m/s, DIRECT speed — not u/v)
Deterministic records only; ``ens std dev`` / probabilistic variants are skipped.

    model_data/nbm/{year}/{YYYYMMDD}_12z/f024_surface.npy   (daily precip sum)
    model_data/nbm/{year}/{YYYYMMDD}_12z/f024_temp.npy / _wind.npy / _humidity.npy
    model_data/nbm/{year}/{YYYYMMDD}_12z/_6h/   (raw 6h APCP buckets)
    model_data/nbm/{year}/{YYYYMMDD}_12z/_inst/ (raw instant records)
    model_data/nbm/grid_lats.npy
    model_data/nbm/grid_lons.npy
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, timezone

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from accumulation import daily_from_buckets
from base import BaseDownloader
from model_registry import US_CROP_BOUNDS

NBM_CYCLE = 12
NBM_VARIABLE = "APCP"

# Instantaneous records read from the daily-fhour core file:
# record key -> (grib var code, level descriptor). WIND is a direct speed field.
NBM_INSTANT_RECORDS: dict[str, tuple[str, str]] = {
    "temp": ("TMP", "2 m above ground"),
    "humidity": ("RH", "2 m above ground"),
    "wind": ("WIND", "10 m above ground"),
}


def _reproject_to_target(data, src_transform, src_crs, target_transform, target_w, target_h, resampling):
    """Reproject one Lambert-Conformal field onto the regular target grid."""
    import warnings

    import numpy as np
    from rasterio.warp import reproject

    try:
        from rasterio.errors import NotGeoreferencedWarning as _RioNotGeo
    except Exception:  # pragma: no cover
        _RioNotGeo = None

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
            source=data,
            destination=dst,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=target_transform,
            dst_crs="EPSG:4326",
            resampling=resampling,
            dst_nodata=np.nan,
        )
    return dst


def _assemble_instant_one(item: tuple) -> None:
    """Worker: reproject one instantaneous record to the target grid, save .npy.

    Single field (no summing); bilinear resampling is appropriate for smooth
    continuous fields (temp/wind/humidity). Optionally converts K -> degC.
    """
    import os

    # GDAL's GRIB driver normalizes units by default (GRIB_NORMALIZE_UNITS=YES),
    # silently converting temperature K -> degC on read. That would double-convert
    # against the `to_celsius` subtraction below (yielding ~ -260 degC). Force raw
    # GRIB units so every model's instant temp is read in Kelvin, matching the
    # cfgrib-based GFS/AIFS paths. (Wind/humidity/precip are unaffected by it.)
    # Set via the environment rather than rasterio.Env() — the latter's GDAL
    # config stack can crash GRIB reads in spawned/forked pool workers on macOS.
    os.environ["GRIB_NORMALIZE_UNITS"] = "NO"

    import numpy as np
    import rasterio
    from rasterio.warp import Resampling
    from rasterio.transform import Affine

    grib_path_str, npy_out_str, transform_vals, target_w, target_h, to_celsius = item
    npy_out = Path(npy_out_str)
    target_transform = Affine(*transform_vals)

    with rasterio.open(grib_path_str) as src:
        data = src.read(1).astype(np.float32)
        src_transform = src.transform
        src_crs = src.crs

    dst = _reproject_to_target(
        data, src_transform, src_crs, target_transform, target_w, target_h, Resampling.bilinear,
    )
    if to_celsius:
        dst = (dst - 273.15).astype(np.float32)

    npy_out.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_out, dst)


def _assemble_one(item: tuple) -> None:
    """Worker function for parallel assembly (must be at module level for pickling).

    Reads four 6h GRIB2 files, sums precipitation, reprojects from Lambert
    Conformal to a regular lat/lon grid, and saves as ``.npy``.

    Reprojection uses area-average resampling: NBM's ~2.5 km grid is downsampled
    to 0.25° (~28 km), so each coarse cell should hold the areal mean of the fine
    cells it covers. Nearest samples a single fine cell (not a total, despite the
    old "preserves totals" rationale) and adds sampling noise that inflates the
    verification error against averaged PRISM.
    """
    import numpy as np
    import rasterio
    from rasterio.warp import Resampling
    from rasterio.transform import Affine

    sub_paths_str, npy_out_str, transform_vals, target_w, target_h = item
    npy_out = Path(npy_out_str)
    target_transform = Affine(*transform_vals)

    # Read the four 6h grids; the shared core sums them into the daily total.
    buckets = []
    src_transform = None
    src_crs = None
    for grib_path in sub_paths_str:
        with rasterio.open(grib_path) as src:
            buckets.append(src.read(1).astype(np.float32))
            if src_transform is None:
                src_transform = src.transform
                src_crs = src.crs
    total = daily_from_buckets(buckets)

    # Reproject from Lambert Conformal to regular lat/lon with area-average
    # resampling (downsampling ~2.5 km → 0.25°); geometry still depends on
    # rasterio/GRIB georeferencing being correct.
    dst = _reproject_to_target(
        total, src_transform, src_crs, target_transform, target_w, target_h, Resampling.average,
    )

    npy_out.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_out, dst)


@dataclass(frozen=True)
class DownloadTask:
    init_date: datetime
    fhour: int
    level: str
    record: str = "apcp6h"  # "apcp6h" (precip bucket) | "temp" | "humidity" | "wind"


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

    def _inst_grib_file(self, init_date: datetime, daily_fhour: int, record: str) -> Path:
        """Path for a raw instantaneous record GRIB2 (in _inst/ subdirectory)."""
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{NBM_CYCLE:02d}"
        out_dir = self.output_dir / str(init_date.year) / f"{date_str}_{cycle_str}z" / "_inst"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"f{daily_fhour:03d}_{record}.grib2"

    def _var_npy_file(self, init_date: datetime, daily_fhour: int, var_suffix: str) -> Path:
        """Assembled per-variable daily .npy path (suffix: surface/temp/wind/humidity)."""
        date_str = init_date.strftime("%Y%m%d")
        cycle_str = f"{NBM_CYCLE:02d}"
        out_dir = self.output_dir / str(init_date.year) / f"{date_str}_{cycle_str}z"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"f{daily_fhour:03d}_{var_suffix}.npy"

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

    def _find_instant_byte_range(self, idx_text: str, record: str, fhour: int):
        """Find an instantaneous deterministic record (TMP/RH/WIND) at the daily fhour.

        Matches ``{var}:{level}:{fhour} hour fcst:`` with an empty trailing field;
        skips ``ens std dev`` / probabilistic variants and the wrong-level WIND
        (e.g. ``surface - 610 m above ground``) via the exact level descriptor.
        """
        grib_var, lvl_desc = NBM_INSTANT_RECORDS[record]
        target_desc = f"{fhour} hour fcst"
        lines = idx_text.strip().split("\n")
        for i, line in enumerate(lines):
            parts = line.split(":")
            if len(parts) < 6:
                continue
            if parts[3].strip() != grib_var or parts[4].strip() != lvl_desc:
                continue
            if parts[5].strip() != target_desc:
                continue
            # Deterministic record only: trailing qualifier (ens std dev / prob ...) must be empty.
            if len(parts) > 6 and parts[6].strip() != "":
                continue
            start_byte = int(parts[1])
            end_byte = None
            if i + 1 < len(lines):
                end_byte = int(lines[i + 1].split(":")[1]) - 1
            return start_byte, end_byte
        return None, None

    # ── Single-file download (parallel task) ────────────────────────

    def _download_task(self, task: DownloadTask) -> tuple[DownloadTask, str]:
        init_date, fhour, level, record = task.init_date, task.fhour, task.level, task.record
        grib_url, idx_url = self._paths(init_date, fhour)
        if record == "apcp6h":
            out_file = self._sub_grib_file(init_date, fhour, level)
            parser = lambda txt: self._find_byte_range(txt, fhour)
        else:
            out_file = self._inst_grib_file(init_date, fhour, record)
            parser = lambda txt: self._find_instant_byte_range(txt, record, fhour)
        status = self._byte_range_fetch(
            grib_url=grib_url,
            idx_url=idx_url,
            idx_parser=parser,
            out_path=out_file,
        )
        return task, status

    # ── Daily assembly ──────────────────────────────────────────────

    @staticmethod
    def _sub_fhours_for_daily(daily_fhour: int) -> list[int]:
        """Return the four 6h sub-hours that make up a daily total.

        For daily_fhour=48 (lead day 2): returns [30, 36, 42, 48].
        """
        return [daily_fhour - 18, daily_fhour - 12, daily_fhour - 6, daily_fhour]

    # Target regular lat/lon grid matching GFS 0.25° resolution over CONUS.
    _TARGET_RES = 0.25

    def _target_grid(self):
        """Return (transform, width, height, lats_1d, lons_1d) for the regular target grid.

        Row 0 is the southernmost latitude (y increases northward in row index).
        This matches ``compute_stats._gfs_transform`` when GFS latitudes are stored
        south→north (ascending ``lats``). A north-up affine would require flipping
        stored arrays and aligning the whole pipeline; do not change one side only.
        """
        import numpy as np
        import rasterio.transform

        west, south, east, north = US_CROP_BOUNDS
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

    def _assemble_instant(
        self,
        init_dates: list[datetime],
        daily_fhours: list[int],
    ) -> None:
        """Reproject instant records (temp/wind/humidity) to the target grid, save .npy.

        Temp is converted K -> degC; wind/humidity are kept as-is (m/s, %). Uses
        the same regular grid as precip so all variables align for extract_forecast.
        """
        import numpy as np
        from concurrent.futures import ProcessPoolExecutor, as_completed

        target_transform, target_w, target_h, lats_1d, lons_1d = self._target_grid()
        if not (self.output_dir / "grid_lats.npy").exists():
            np.save(self.output_dir / "grid_lats.npy", lats_1d)
            np.save(self.output_dir / "grid_lons.npy", lons_1d)

        work: list[tuple] = []
        skipped_existing = 0
        skipped_missing = 0
        for init_date in init_dates:
            for daily_fh in daily_fhours:
                for record in NBM_INSTANT_RECORDS:
                    npy_out = self._var_npy_file(init_date, daily_fh, record)
                    if npy_out.exists():
                        skipped_existing += 1
                        continue
                    grib = self._inst_grib_file(init_date, daily_fh, record)
                    if not grib.exists():
                        skipped_missing += 1
                        continue
                    work.append((
                        str(grib),
                        str(npy_out),
                        tuple(target_transform)[:6],
                        target_w,
                        target_h,
                        record == "temp",  # to_celsius
                    ))

        total = len(work)
        if total == 0:
            print(f"  Instant: 0 created | {skipped_existing} existed | "
                  f"{skipped_missing} skipped (missing inst grib)")
            return

        workers = min(self.max_workers, total)
        print(f"  {total} instant fields to assemble with {workers} workers...")
        assembled = 0
        failed = 0
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_assemble_instant_one, item): item for item in work}
            for fut in as_completed(futures):
                try:
                    fut.result()
                    assembled += 1
                except Exception as exc:
                    failed += 1
                    item = futures[fut]
                    if failed <= 5:
                        print(f"  WARNING: instant assembly failed for {item[1]}: {exc}")
                done = assembled + failed
                if done % 500 == 0 or done == total:
                    print(f"  Instant progress: {done}/{total}", flush=True)

        # Clean up cfgrib/GDAL sidecar idx files in _inst/.
        for init_date in init_dates:
            inst_dir = self._inst_grib_file(init_date, 0, "x").parent
            if inst_dir.exists():
                for sidecar in inst_dir.glob("*.idx"):
                    sidecar.unlink(missing_ok=True)

        print(f"\n  Instant: {assembled} created | {skipped_existing} existed | "
              f"{skipped_missing} skipped (missing inst grib)"
              + (f" | {failed} failed" if failed else ""))

    # ── Public download interface ───────────────────────────────────

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

        # Build download task list: one task per 6h precip bucket, plus one per
        # instant record (temp/wind/humidity) at each daily fhour.
        tasks: list[DownloadTask] = []
        skipped = 0
        for d in init_dates:
            for fh in sorted_sub_fhours:
                t = DownloadTask(d, int(fh), str(level), "apcp6h")
                if self._sub_grib_file(t.init_date, t.fhour, t.level).exists():
                    skipped += 1
                else:
                    tasks.append(t)
            for daily_fh in forecast_hours:
                for record in NBM_INSTANT_RECORDS:
                    if self._inst_grib_file(d, int(daily_fh), record).exists():
                        skipped += 1
                    else:
                        tasks.append(DownloadTask(d, int(daily_fh), str(level), record))

        print("\n" + "=" * 70)
        print("Parallel NBM filtered download (idx + Range)")
        print(f"Period: {start.date()} to {end.date()} | Daily")
        print(f"Cycle: {NBM_CYCLE:02d}z")
        print(f"Daily forecast hours: {forecast_hours}")
        print(f"6h sub-hours: {sorted_sub_fhours}")
        print(f"Variables: {NBM_VARIABLE} (6h sum) + temp/wind/humidity (instant) | Level: {level}")
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

        # Assemble 6h files into daily totals, then reproject instant fields.
        print("\nAssembling 6h buckets into daily precipitation totals...")
        self._assemble_daily(init_dates, forecast_hours, level)
        print("\nAssembling instant fields (temp/wind/humidity)...")
        self._assemble_instant(init_dates, forecast_hours)

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
