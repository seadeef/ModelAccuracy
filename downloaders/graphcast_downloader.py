#!/usr/bin/env python3
"""GraphCast downloader (NOAA AIWP, GRAP_v100_GFS).

Two data paths share the same downstream output:

1. **Combined Zarr (kerchunk parquet refs)** — covers 2021-12-31 → 2025-04-01.
   `s3://noaa-oar-mlwp-data/parquet/GRAP_v100_GFS_combined_all.parq` is a
   fsspec ReferenceFileSystem with chunk pointers into the original .nc
   files. Open once, share via threads (zarr is thread-safe). Per-init
   read is ~12s.

2. **Direct .nc parallel chunk fetch** — covers anything past the parquet
   ref's last init. Each `s3://noaa-oar-mlwp-data/GRAP_v100_GFS/...nc` is
   5.77 GB but `apcp` is only ~170 MB across 41 HDF5 chunks (one global
   timestep per chunk, shuffle+deflate filtered). We open the file once
   via h5py to read chunk byte offsets (~1s metadata), then fan out
   per-chunk byte-range S3 fetches via threads (~5-7s for the full cube).
   Replaces the old xr.open_dataset+h5netcdf streaming path which took
   ~30-60s per init due to many small range requests.

Daily totals: sum four consecutive 6h apcp buckets, m → mm.

Output (matches gfs_downloader):
    model_data/graphcast/{year}/{YYYYMMDD}_{HH}z/f024_surface.npy ... f240_surface.npy
    model_data/graphcast/grid_lats.npy
    model_data/graphcast/grid_lons.npy
"""

from __future__ import annotations

import sys
import time
import zlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# NOAA AIWP publishes GraphCast inits at 00z and/or 12z, varying day-to-day.
# Forecast mode probes both, preferring 12z (later init = fresher) within a date.
GRAPHCAST_FORECAST_CYCLES: tuple[int, ...] = (12, 0)


def _graphcast_nc_key(init_date: datetime, cycle: int) -> str:
    date_str = init_date.strftime("%Y%m%d")
    cycle_str = f"{cycle:02d}"
    mmdd = init_date.strftime("%m%d")
    fname = f"GRAP_v100_GFS_{date_str}{cycle_str}_f000_f240_06.nc"
    return f"noaa-oar-mlwp-data/GRAP_v100_GFS/{init_date.year}/{mmdd}/{fname}"


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


def _unshuffle(buf: bytes, typesize: int) -> bytes:
    """Reverse the HDF5 shuffle filter (byte de-interleaving).

    For typesize=4: shuffled layout is `[byte0_v0, byte0_v1, ..., byte0_vN,
    byte1_v0, ..., byte3_vN]`. The inverse is a transpose of the (typesize, N)
    view back to interleaved bytes.
    """
    import numpy as np
    arr = np.frombuffer(buf, dtype=np.uint8).reshape(typesize, -1)
    return arr.T.tobytes()


def _decode_apcp_chunk(raw: bytes, dtype, chunk_shape):
    """Decompress one HDF5 chunk encoded with shuffle+deflate."""
    import numpy as np
    deflated = zlib.decompress(raw)
    deshuffled = _unshuffle(deflated, dtype.itemsize)
    return np.frombuffer(deshuffled, dtype=dtype).reshape(chunk_shape)


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

    # ── Forecast-mode discovery (cycle-flexible) ────────────────────

    def _list_remote_cycles_on_date(self, init_date: datetime) -> list[int]:
        """Return sorted list of cycles (e.g. [0, 12]) published on a given date,
        or [] if the date directory is missing or empty.
        """
        import s3fs
        fs = s3fs.S3FileSystem(anon=True)
        mmdd = init_date.strftime("%m%d")
        prefix = f"noaa-oar-mlwp-data/GRAP_v100_GFS/{init_date.year}/{mmdd}/"
        try:
            keys = fs.ls(prefix)
        except FileNotFoundError:
            return []
        except Exception:
            return []
        date_str = init_date.strftime("%Y%m%d")
        expected_prefix = f"GRAP_v100_GFS_{date_str}"
        cycles: set[int] = set()
        for k in keys:
            name = k.rsplit("/", 1)[-1]
            if not name.startswith(expected_prefix) or not name.endswith("_f000_f240_06.nc"):
                continue
            cc_str = name[len(expected_prefix):len(expected_prefix) + 2]
            try:
                cycles.add(int(cc_str))
            except ValueError:
                continue
        return sorted(cycles)

    def find_latest_forecast_init(
        self,
        candidates: list[datetime],
        prefer_cycles: tuple[int, ...] = GRAPHCAST_FORECAST_CYCLES,
    ) -> tuple[datetime, int] | None:
        """Find the freshest available remote init across ``candidates``.

        ``candidates`` should be in newest-to-oldest order. For each date, picks
        the latest available cycle in ``prefer_cycles`` order. Returns
        ``(init_date, cycle)`` or ``None`` if no init is published in any of
        the candidate dates.
        """
        for d in candidates:
            available = self._list_remote_cycles_on_date(d)
            if not available:
                continue
            for c in prefer_cycles:
                if c in available:
                    return d, c
            # Fallback: take whatever is published if none of the preferred
            # cycles are present.
            return d, available[-1]
        return None

    def download_forecast_init(self, init_date: datetime, cycle: int) -> bool:
        """Download a single GraphCast init at the specified cycle (00z or 12z).

        Returns True iff every daily lead .npy exists on disk afterward.
        """
        daily_fhours = [d * 24 for d in range(1, GRAPHCAST_MAX_LEAD_H // 24 + 1)]
        if self._all_dailies_present(init_date, cycle, daily_fhours):
            return True

        print("\n" + "=" * 70)
        print("GraphCast (NOAA AIWP) forecast download")
        print(f"Init: {init_date.date()} {cycle:02d}z | Lead 1-{GRAPHCAST_MAX_LEAD_H // 24}d")
        print(f"Output: {self.output_dir.resolve()}")
        print("=" * 70)

        self._ensure_grid_files(None, init_date, cycle)
        t0 = time.time()
        _, status = self._process_init_chunk_parallel(init_date, cycle, daily_fhours)
        print(f"[direct .nc] {init_date.date()} {cycle:02d}z -> {status} ({time.time()-t0:.1f}s)")
        return self._all_dailies_present(init_date, cycle, daily_fhours)

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

        # Write grid_lats.npy / grid_lons.npy once before the per-init pipelines.
        sample_nc_init = nc_tasks[0] if nc_tasks else None
        self._ensure_grid_files(zarr_ds, sample_nc_init, cycle)

        # ── Path 1: parquet-ref zarr via threads ──────────────────────
        if zarr_tasks and zarr_ds is not None:
            self._run_zarr_threads(zarr_ds, zarr_tasks, cycle, daily_fhours)

        # ── Path 2: direct .nc via parallel chunk fetch ──────────────
        if nc_tasks:
            self._run_nc_inits(nc_tasks, cycle, daily_fhours)

    def _ensure_grid_files(self, zarr_ds, sample_nc_init: datetime | None, cycle: int) -> None:
        """Write grid_lats.npy/grid_lons.npy if missing.

        Prefers the parquet zarr if it opened; otherwise opens one sample .nc
        to derive the grid. Called once before any per-init chunk fetches so
        the worker doesn't have to derive lat/lon for the on-disk grid file
        (it still reads them from each .nc for CONUS slicing).
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
                key = _graphcast_nc_key(sample_nc_init, cycle)
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

    def _process_init_chunk_parallel(
        self,
        init_date: datetime,
        cycle: int,
        daily_fhours: list[int],
    ) -> tuple[tuple[datetime, int], str]:
        """Fetch one init's apcp via parallel HDF5 chunk byte-range reads.

        Steps per init:
          1. Open .nc once via h5py to extract chunk byte offsets and lat/lon
             (~1s of small metadata reads; not parallelized).
          2. Fan out 41 chunk fetches (one per timestep) via a ThreadPoolExecutor
             of ``self.max_workers`` threads. Each chunk: byte-range fetch +
             zlib decompress + reverse shuffle filter.
          3. Slice CONUS, sort lon to -180..180, write per-fhour daily totals.

        Returns ``((init_date, cycle), status_string)``.
        """
        import h5py
        import numpy as np
        import s3fs

        init_dir = self._init_dir(init_date, cycle)
        if all((init_dir / f"f{fh:03d}_surface.npy").exists() for fh in daily_fhours):
            return (init_date, cycle), "exists"

        if self.polite_delay_seconds:
            time.sleep(self.polite_delay_seconds)

        key = _graphcast_nc_key(init_date, cycle)
        fs = s3fs.S3FileSystem(anon=True)

        # ── Phase 1: one-shot metadata read (chunk offsets + grid) ────
        try:
            with fs.open(key, "rb") as fh:
                f = h5py.File(fh, "r")
                try:
                    if "apcp" not in f:
                        return (init_date, cycle), f"failed: no apcp ({list(f.keys())})"
                    ds_apcp = f["apcp"]
                    n_chunks = ds_apcp.id.get_num_chunks()
                    chunks_info = [ds_apcp.id.get_chunk_info(i) for i in range(n_chunks)]
                    dtype = ds_apcp.dtype
                    apcp_shape = ds_apcp.shape
                    chunk_shape = ds_apcp.chunks
                    lats = f["latitude"][:].astype(np.float32)
                    lons = f["longitude"][:].astype(np.float32)
                finally:
                    f.close()
        except FileNotFoundError:
            return (init_date, cycle), "not_found"
        except Exception as e:
            return (init_date, cycle), f"failed: metadata {e}"

        lat_slc, lon_slc, _, _, lon_sort, flip_lat = _conus_index_slices(lats, lons)

        # ── Phase 2: parallel chunk fetch + decode ────────────────────
        apcp_global = np.empty(apcp_shape, dtype=dtype)

        def fetch_one(ci):
            last_err = None
            for attempt in range(1, self.max_retries + 1):
                try:
                    raw = fs.cat_file(
                        key, start=ci.byte_offset, end=ci.byte_offset + ci.size,
                    )
                    arr = _decode_apcp_chunk(raw, dtype, chunk_shape)
                    return ci.chunk_offset, arr
                except Exception as e:
                    last_err = e
                    time.sleep(0.5 * attempt)
            raise RuntimeError(f"chunk {ci.chunk_offset}: {last_err}")

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                for chunk_offset, arr in pool.map(fetch_one, chunks_info):
                    apcp_global[chunk_offset[0]] = arr[0]
        except Exception as e:
            return (init_date, cycle), f"failed: chunks {e}"

        # ── Phase 3: CONUS slice + daily totals ───────────────────────
        apcp = apcp_global[:, lat_slc, lon_slc].astype(np.float32, copy=False)
        apcp = apcp[:, :, lon_sort]
        if flip_lat:
            apcp = apcp[:, ::-1, :]

        _write_dailies(apcp, init_dir, daily_fhours)
        return (init_date, cycle), "processed"

    def _run_nc_inits(self, tasks: list[datetime], cycle: int, daily_fhours: list[int]):
        """Drive ``_process_init_chunk_parallel`` over a list of inits.

        Outer loop is sequential because the inner chunk pool already saturates
        bandwidth (8-thread chunk fetch is ~7s/init flat from 4 to 32 threads
        in benchmarks). Adding outer init-level parallelism stacks more
        concurrent S3 connections without improving wall-clock.
        """
        if not tasks:
            return

        print(f"\n[direct .nc] {len(tasks)} inits via {self.max_workers}-thread chunk fetch (sequential outer)")
        counts: dict[str, int] = defaultdict(int)
        examples: list[str] = []
        total = len(tasks)
        t0 = time.time()

        for i, d in enumerate(tasks, start=1):
            (init_d, cyc), status = self._process_init_chunk_parallel(d, cycle, daily_fhours)
            key = self._status_key(status)
            counts[key] += 1
            if status.startswith("failed") and len(examples) < 5:
                examples.append(f"{init_d:%Y-%m-%d} {cyc:02d}z -> {status[:120]}")
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta_min = ((total - i) / rate / 60) if rate > 0 else 0
            parts = " | ".join(f"{k}={counts[k]}" for k in sorted(counts))
            print(
                f"\r[direct .nc] {i}/{total} | {parts} | "
                f"{rate*60:.1f} inits/min | ETA {eta_min:.1f} min",
                end="", flush=True,
            )
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
