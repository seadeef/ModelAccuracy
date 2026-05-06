#!/usr/bin/env python3
"""GraphCast downloader (NOAA AIWP, GRAP_v100_GFS).

Two data paths share the same downstream output:

1. **Combined Zarr (kerchunk parquet refs)** — covers 2021-12-31 → 2025-04-01.
   `s3://noaa-oar-mlwp-data/parquet/GRAP_v100_GFS_combined_all.parq` is a
   fsspec ReferenceFileSystem with chunk pointers into the original .nc
   files. Open once, share via threads (zarr is thread-safe). Per-init
   read is ~12s (vs ~30s for direct .nc) and we avoid HDF5's GIL contention.

2. **Direct .nc lazy reads** — covers anything past the parquet ref's last init.
   Opens `s3://noaa-oar-mlwp-data/GRAP_v100_GFS/{YYYY}/{MMDD}/...nc` lazily
   per init. HDF5 is not thread-safe, so this path uses ProcessPoolExecutor.

Daily totals: sum four consecutive 6h apcp buckets, m → mm.

Output (matches gfs_downloader):
    model_data/graphcast/{year}/{YYYYMMDD}_{HH}z/f024_surface.npy ... f240_surface.npy
    model_data/graphcast/grid_lats.npy
    model_data/graphcast/grid_lons.npy
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from base import BaseDownloader
from model_registry import US_CROP_BOUNDS

GRAPHCAST_CYCLE = 12
GRAPHCAST_MAX_LEAD_H = 240  # 10 days
PARQUET_REF_URL = "s3://noaa-oar-mlwp-data/parquet/GRAP_v100_GFS_combined_all.parq"


def _sub_fhours_for_daily(daily_fhour: int) -> list[int]:
    return [daily_fhour - 18, daily_fhour - 12, daily_fhour - 6, daily_fhour]


def _conus_index_slices(lats, lons):
    """Return (lat_slc, lon_slc, kept_lats, kept_lons) for CONUS bbox.

    Source grid: lat 90→-90, lon 0→359.75. Output ascends south→north and
    converts lon to -180..180 (sorted ascending).
    """
    import numpy as np

    west, south, east, north = US_CROP_BOUNDS
    lat_keep = (lats >= south) & (lats <= north)
    lat_idx = np.where(lat_keep)[0]
    west360 = west % 360
    east360 = east % 360
    lon_keep = (lons >= west360) & (lons <= east360)
    lon_idx = np.where(lon_keep)[0]
    lat_slc = slice(int(lat_idx.min()), int(lat_idx.max()) + 1)
    lon_slc = slice(int(lon_idx.min()), int(lon_idx.max()) + 1)

    kept_lats = lats[lat_slc]
    kept_lons = lons[lon_slc]
    kept_lons = ((kept_lons + 180.0) % 360.0) - 180.0
    lon_sort = np.argsort(kept_lons)
    kept_lons_sorted = kept_lons[lon_sort]
    if kept_lats[0] > kept_lats[-1]:
        kept_lats = kept_lats[::-1]
        flip_lat = True
    else:
        flip_lat = False
    return lat_slc, lon_slc, kept_lats, kept_lons_sorted, lon_sort, flip_lat


def _write_dailies(apcp_cube, init_dir: Path, daily_fhours: list[int]) -> int:
    """Write per-fhour daily-total .npy files from a (time, lat, lon) apcp cube in METERS.

    Returns count written.
    """
    import numpy as np

    n_time = apcp_cube.shape[0]
    wrote = 0
    for daily_fh in daily_fhours:
        npy_out = init_dir / f"f{daily_fh:03d}_surface.npy"
        if npy_out.exists():
            continue
        sub_idxs = [fh // 6 for fh in _sub_fhours_for_daily(daily_fh)]
        if any(idx < 0 or idx >= n_time for idx in sub_idxs):
            continue
        arr = apcp_cube[sub_idxs].sum(axis=0).astype(np.float32)
        arr *= 1000.0  # m -> mm
        np.clip(arr, 0.0, None, out=arr)
        np.save(npy_out, arr)
        wrote += 1
    return wrote


def _process_init_nc(args: tuple) -> tuple[str, int, int, str]:
    """Worker for the direct .nc path (used past parquet-ref coverage).

    args: (init_iso, cycle, output_dir, daily_fhours, max_retries, polite_delay)
    """
    import numpy as np
    import s3fs
    import xarray as xr

    init_iso, cycle, output_dir_str, daily_fhours, max_retries, polite_delay = args
    init_date = datetime.fromisoformat(init_iso)
    output_dir = Path(output_dir_str)

    date_str = init_date.strftime("%Y%m%d")
    cycle_str = f"{cycle:02d}"
    init_dir = output_dir / str(init_date.year) / f"{date_str}_{cycle_str}z"
    init_dir.mkdir(parents=True, exist_ok=True)

    if all((init_dir / f"f{fh:03d}_surface.npy").exists() for fh in daily_fhours):
        return init_iso, cycle, 0, "exists"

    if polite_delay:
        time.sleep(polite_delay)

    mmdd = init_date.strftime("%m%d")
    fname = f"GRAP_v100_GFS_{date_str}{cycle_str}_f000_f240_06.nc"
    key = f"noaa-oar-mlwp-data/GRAP_v100_GFS/{init_date.year}/{mmdd}/{fname}"

    fs = s3fs.S3FileSystem(anon=True)
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            with fs.open(key, "rb") as fh:
                ds = xr.open_dataset(fh, engine="h5netcdf")
                try:
                    if "apcp" not in ds.data_vars:
                        return init_iso, cycle, 0, f"failed: no apcp ({list(ds.data_vars)})"
                    lats = ds["latitude"].values.astype(np.float32)
                    lons = ds["longitude"].values.astype(np.float32)
                    lat_slc, lon_slc, _, _, lon_sort, flip_lat = _conus_index_slices(lats, lons)
                    apcp = ds["apcp"].isel(latitude=lat_slc, longitude=lon_slc).values.astype(np.float32)
                    apcp = apcp[:, :, lon_sort]
                    if flip_lat:
                        apcp = apcp[:, ::-1, :]
                finally:
                    ds.close()

            wrote = _write_dailies(apcp, init_dir, daily_fhours)
            return init_iso, cycle, wrote, "processed"
        except FileNotFoundError:
            return init_iso, cycle, 0, "not_found"
        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)
    return init_iso, cycle, 0, f"failed: {last_err}"


@dataclass(frozen=True)
class DownloadTask:
    init_date: datetime
    cycle: int


class GraphCastDownloaderParallel(BaseDownloader):
    def __init__(
        self,
        output_dir: str = "model_data/graphcast",
        max_workers: int = 8,
        max_retries: int = 3,
        timeout_seconds: int = 600,
        polite_delay_seconds: float = 0.0,
    ):
        super().__init__(
            output_dir,
            max_workers=max_workers,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            polite_delay_seconds=polite_delay_seconds,
            base_url="s3://noaa-oar-mlwp-data",
            user_agent="GraphCastDownloaderParallel/1.0",
        )

    def _init_dir(self, init_date: datetime, cycle: int) -> Path:
        date_str = init_date.strftime("%Y%m%d")
        out_dir = self.output_dir / str(init_date.year) / f"{date_str}_{cycle:02d}z"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def _daily_npy(self, init_date: datetime, cycle: int, daily_fhour: int) -> Path:
        return self._init_dir(init_date, cycle) / f"f{daily_fhour:03d}_surface.npy"

    def _all_dailies_present(self, init_date: datetime, cycle: int, daily_fhours: list[int]) -> bool:
        return all(self._daily_npy(init_date, cycle, fh).exists() for fh in daily_fhours)

    # ── Public download interface ───────────────────────────────────

    def _download(self, start: datetime, end: datetime, *, forecast_hours=None, level: str = "surface"):
        import fsspec
        import numpy as np
        import xarray as xr

        cycle = GRAPHCAST_CYCLE
        daily_fhours = [d * 24 for d in range(1, GRAPHCAST_MAX_LEAD_H // 24 + 1)]

        # Build the full init-date list at the configured cycle.
        init_dates: list[datetime] = []
        cur = datetime(start.year, start.month, start.day, cycle)
        end0 = datetime(end.year, end.month, end.day, cycle)
        while cur <= end0:
            init_dates.append(cur)
            cur += timedelta(days=1)
        if not init_dates:
            raise ValueError("No init dates selected.")

        # Try to open the parquet-ref zarr; if available, use it for inits in coverage.
        zarr_init_ns: dict[int, int] = {}  # ns since epoch -> index
        zarr_init_array = None
        zarr_ds = None
        try:
            print("Opening parquet-ref zarr (combined GRAP archive)...")
            t0 = time.time()
            ref_fs = fsspec.filesystem(
                "reference",
                fo=PARQUET_REF_URL,
                remote_protocol="s3",
                remote_options={"anon": True},
            )
            zarr_ds = xr.open_zarr(ref_fs.get_mapper(""), consolidated=False)
            zarr_init_array = zarr_ds.init_time.values
            # init_time is datetime64[ns]; .view('int64') gives ns since epoch.
            ns_view = zarr_init_array.astype("datetime64[ns]").view("int64")
            zarr_init_ns = {int(ns): i for i, ns in enumerate(ns_view)}
            print(f"  opened in {time.time()-t0:.1f}s | {len(zarr_init_array)} inits "
                  f"({zarr_init_array[0]} ... {zarr_init_array[-1]})")
        except Exception as e:
            print(f"  parquet-ref zarr open failed ({e}); will fall back to .nc only")

        # Bucket inits by source.
        zarr_tasks: list[tuple[datetime, int]] = []  # (init_dt, zarr_index)
        nc_tasks: list[datetime] = []
        skipped = 0
        for d in init_dates:
            if self._all_dailies_present(d, cycle, daily_fhours):
                skipped += 1
                continue
            np_dt = np.datetime64(d.replace(tzinfo=None).isoformat(), "ns")
            ns_key = int(np_dt.view("int64"))
            zi = zarr_init_ns.get(ns_key)
            if zi is not None:
                zarr_tasks.append((d, zi))
            else:
                nc_tasks.append(d)

        print("\n" + "=" * 70)
        print("GraphCast (NOAA AIWP) download")
        print(f"Period: {start.date()} to {end.date()} | {cycle:02d}z | Lead 1-{GRAPHCAST_MAX_LEAD_H // 24}d")
        print(f"Output: {self.output_dir.resolve()}")
        print(f"Inits: {len(zarr_tasks)} via parquet-ref | {len(nc_tasks)} via direct .nc | "
              f"{skipped} already complete")
        print("=" * 70)

        # Pre-write grid_lats.npy / grid_lons.npy in the parent process so the
        # multiprocess .nc workers don't race on the write.
        sample_nc_init = nc_tasks[0] if nc_tasks else None
        self._ensure_grid_files(zarr_ds, sample_nc_init, cycle)

        # ── Path 1: parquet-ref zarr via threads ──────────────────────
        if zarr_tasks and zarr_ds is not None:
            self._run_zarr_threads(zarr_ds, zarr_tasks, cycle, daily_fhours)

        # ── Path 2: direct .nc via processes ──────────────────────────
        if nc_tasks:
            self._run_nc_processes(nc_tasks, cycle, daily_fhours)

    def _ensure_grid_files(self, zarr_ds, sample_nc_init: datetime | None, cycle: int) -> None:
        """Write grid_lats.npy/grid_lons.npy if missing.

        Runs once in the parent process. Prefers the parquet zarr if it opened;
        otherwise opens one sample .nc to derive the grid. The .nc workers then
        find the files already on disk and skip the (racy) write.
        """
        import numpy as np

        grid_lats_p = self.output_dir / "grid_lats.npy"
        grid_lons_p = self.output_dir / "grid_lons.npy"
        if grid_lats_p.exists() and grid_lons_p.exists():
            return

        lats = lons = None
        if zarr_ds is not None:
            try:
                lats = zarr_ds["latitude"].values.astype(np.float32)
                lons = zarr_ds["longitude"].values.astype(np.float32)
            except Exception as e:
                print(f"  WARNING: could not derive grid from parquet zarr: {e}")

        if (lats is None or lons is None) and sample_nc_init is not None:
            try:
                import s3fs
                import xarray as xr
                date_str = sample_nc_init.strftime("%Y%m%d")
                cycle_str = f"{cycle:02d}"
                mmdd = sample_nc_init.strftime("%m%d")
                fname = f"GRAP_v100_GFS_{date_str}{cycle_str}_f000_f240_06.nc"
                key = f"noaa-oar-mlwp-data/GRAP_v100_GFS/{sample_nc_init.year}/{mmdd}/{fname}"
                fs = s3fs.S3FileSystem(anon=True)
                with fs.open(key, "rb") as fh:
                    ds = xr.open_dataset(fh, engine="h5netcdf")
                    try:
                        lats = ds["latitude"].values.astype(np.float32)
                        lons = ds["longitude"].values.astype(np.float32)
                    finally:
                        ds.close()
            except Exception as e:
                print(f"  WARNING: could not pre-derive grid from sample .nc "
                      f"({e}); workers may race on grid file write.")

        if lats is None or lons is None:
            return

        _, _, kept_lats, kept_lons, _, _ = _conus_index_slices(lats, lons)
        if not grid_lats_p.exists():
            np.save(grid_lats_p, kept_lats)
        if not grid_lons_p.exists():
            np.save(grid_lons_p, kept_lons)

    def _run_zarr_threads(
        self,
        zarr_ds,
        tasks: list[tuple[datetime, int]],
        cycle: int,
        daily_fhours: list[int],
    ):
        import numpy as np

        # Pre-compute CONUS slices from the dataset (they're constant across inits).
        # Grid files are written in the parent process via _ensure_grid_files.
        lats = zarr_ds["latitude"].values.astype(np.float32)
        lons = zarr_ds["longitude"].values.astype(np.float32)
        lat_slc, lon_slc, _, _, lon_sort, flip_lat = _conus_index_slices(lats, lons)

        apcp = zarr_ds["apcp"].isel(latitude=lat_slc, longitude=lon_slc)

        counts = defaultdict(int)
        examples: list[str] = []
        total = len(tasks)
        done = 0
        t0 = time.time()
        print(f"\n[parquet-ref] {total} inits via {self.max_workers} threads (shared zarr)")

        def fetch_init(item: tuple[datetime, int]) -> tuple[datetime, str, int]:
            d, i = item
            init_dir = self._init_dir(d, cycle)
            try:
                cube = apcp.isel(init_time=i).values.astype(np.float32)
                cube = cube[:, :, lon_sort]
                if flip_lat:
                    cube = cube[:, ::-1, :]
                wrote = _write_dailies(cube, init_dir, daily_fhours)
                return d, "processed", wrote
            except Exception as e:
                return d, f"failed: {e}", 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(fetch_init, t): t for t in tasks}
            for fut in as_completed(futures):
                item = futures[fut]
                d = item[0]
                try:
                    _, status, wrote = fut.result()
                except Exception as e:
                    status = f"failed: pool {e}"
                    wrote = 0
                key = "processed" if status == "processed" else (status.split(":")[0] if status.startswith("failed") else status)
                counts[key] += 1
                if status.startswith("failed") and len(examples) < 5:
                    examples.append(f"{d:%Y-%m-%d} {cycle:02d}z -> {status[:120]}")
                done += 1
                if done % 10 == 0 or done == total:
                    elapsed = time.time() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    eta_min = ((total - done) / rate / 60) if rate > 0 else 0
                    parts = " | ".join(f"{k}={counts[k]}" for k in sorted(counts))
                    print(f"\r[parquet-ref] {done}/{total} | {parts} | "
                          f"{rate*60:.1f} inits/min | ETA {eta_min:.1f} min",
                          end="", flush=True)
        print()
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print(f"[parquet-ref] done in {(time.time()-t0)/60:.1f} min")

    def _run_nc_processes(self, tasks: list[datetime], cycle: int, daily_fhours: list[int]):
        import multiprocessing as mp
        ctx = mp.get_context("spawn")

        # 4 workers is enough — bandwidth limited; more workers don't help.
        nc_workers = min(self.max_workers, 4)
        print(f"\n[direct .nc] {len(tasks)} inits via {nc_workers} processes (HDF5 not thread-safe)")

        work = [
            (
                d.isoformat(),
                cycle,
                str(self.output_dir),
                daily_fhours,
                self.max_retries,
                self.polite_delay_seconds,
            )
            for d in tasks
        ]

        counts = defaultdict(int)
        examples: list[str] = []
        total = len(work)
        done = 0
        t0 = time.time()

        with ProcessPoolExecutor(max_workers=nc_workers, mp_context=ctx) as pool:
            futures = {pool.submit(_process_init_nc, w): w for w in work}
            for fut in as_completed(futures):
                w = futures[fut]
                try:
                    init_iso, cyc, wrote, status = fut.result()
                except Exception as e:
                    init_iso = w[0]
                    cyc = w[1]
                    wrote = 0
                    status = f"failed: pool {e}"
                key = "processed" if status == "processed" else (status.split(":")[0] if status.startswith("failed") else status)
                counts[key] += 1
                if status.startswith("failed") and len(examples) < 5:
                    examples.append(f"{init_iso[:10]} {cyc:02d}z -> {status[:120]}")
                done += 1
                if done % 5 == 0 or done == total:
                    elapsed = time.time() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    eta_min = ((total - done) / rate / 60) if rate > 0 else 0
                    parts = " | ".join(f"{k}={counts[k]}" for k in sorted(counts))
                    print(f"\r[direct .nc] {done}/{total} | {parts} | "
                          f"{rate*60:.1f} inits/min | ETA {eta_min:.1f} min",
                          end="", flush=True)
        print()
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print(f"[direct .nc] done in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    import argparse

    sys.path.insert(0, str(_this_dir.parent))
    from model_registry import MODEL_REGISTRY

    cfg = MODEL_REGISTRY["graphcast"]

    parser = argparse.ArgumentParser(description="Download GraphCast (NOAA AIWP) precipitation forecasts")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD).")
    parser.add_argument("--start-year", type=int, help="Start year")
    parser.add_argument("--end-year", type=int, help="End year")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    downloader = GraphCastDownloaderParallel(
        output_dir=cfg.data_dir,
        max_workers=args.workers,
        max_retries=3,
        timeout_seconds=600,
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
