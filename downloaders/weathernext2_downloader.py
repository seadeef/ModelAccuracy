#!/usr/bin/env python3
"""WeatherNext 2 downloader (ensemble mean, GCS zarr).

Source switched from NOAA AIWP (.nc / parquet-ref) to Google **WeatherNext 2**,
the ensemble-mean store on GCS:

    gs://weathernext/weathernext_2_0_0_mean/zarr/2025_to_present/
        {YYYYMMDD}_{HH}hr_01_preds/predictions.zarr

These are **per-init stores** (one per cycle), 4 cycles/day (00/06/12/18z),
covering forecast + 2025→present catchup. Each store:
    sizes  : time=60 (6-hourly; time[i] valid at init + (i+1)*6h),
             lat=721 (-90..90 ascending), lon=1440 (0..359.75), level=13
    level  : [50,100,150,200,250,300,400,500,600,700,850,925,1000] hPa
    **NO `number` dim** — it is already a single mean member.

The four downstream variables (all derived from this one store):
    precip   = total_precipitation_6hr (meters) ×1000 → mm, 6h-sum over a day
    temp     = 2m_temperature (Kelvin) → °C
    wind     = 10m_wind_speed (m/s, DIRECT speed — not u/v)
    humidity = relative humidity (%) derived from pressure-level temperature (K)
               + specific_humidity (kg/kg) at 1000 hPa (Bolton)

Units confirmed live against the 12z store (2m_temperature 267..302 K,
temperature@1000 277..308 K — both sane Kelvin; specific_humidity ≤ ~0.02 kg/kg).

Time indexing (lead day D, 1..10):
    instant snapshot → time index 4D-1 (valid at init + D*24h)
    precip 6h-sum    → time indices 4D-4 … 4D-1 (the four 6h buckets of day D)

**Freshest ≠ ready.** A just-published store is written progressively (wind and
precip land first, then 2m temperature, then pressure-level fields), so the
latest cycle can read back all-NaN. Forecast selection therefore verifies each
candidate store is *complete* at the deepest lead and falls back to the latest
complete cycle — never republishing a half-written init.

Output (matches gfs_downloader layout — folder grid + per-variable daily .npy):
    model_data/weathernext2/{year}/{YYYYMMDD}_{HH}z/f024_surface.npy ...   (precip mm)
    model_data/weathernext2/{year}/{YYYYMMDD}_{HH}z/f024_temp.npy / _wind.npy / _humidity.npy
    model_data/weathernext2/grid_lats.npy
    model_data/weathernext2/grid_lons.npy

Auth: Application Default Credentials (``storage_options={"token": "google_default"}``).

Out of scope (TODO): pre-2025 historical backfill, which lives in single
*combined* stores rather than these per-init stores.
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

_this_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(0, str(_this_dir))

from base import BaseDownloader
from forecast_variables import kelvin_to_celsius, rh_from_specific_humidity
from model_registry import US_CROP_BOUNDS

# GCS location of the WeatherNext 2 ensemble-mean per-init stores.
WN2_BUCKET = "weathernext"
WN2_PREFIX = "weathernext_2_0_0_mean/zarr/2025_to_present"
WN2_ROOT = f"gs://{WN2_BUCKET}/{WN2_PREFIX}"
WN2_STORAGE_OPTIONS = {"token": "google_default"}

# Source variable names in the store.
WN2_TEMP = "2m_temperature"            # Kelvin
WN2_WIND = "10m_wind_speed"            # m/s (direct speed)
WN2_PRECIP = "total_precipitation_6hr"  # meters per 6h bucket
WN2_T_LEVEL = "temperature"            # Kelvin, on pressure levels
WN2_Q_LEVEL = "specific_humidity"      # kg/kg, on pressure levels
RH_LEVEL_HPA = 1000                    # pressure level used for surface-ish RH
RH_LEVEL_PA = 100000.0

WN2_MAX_LEAD_H = 240  # 10 days
WN2_CYCLE = 12        # cycle used for archive / catchup (date-range) mode
WN2_FIRST_DATE = datetime(2025, 1, 1)  # earliest per-init store partition

# WeatherNext 2 publishes 4 cycles/day; forecast mode prefers the latest *complete*
# cycle (18→12→06→00), verifying completeness before selecting.
WN2_FORECAST_CYCLES: tuple[int, ...] = (18, 12, 6, 0)


def _instant_time_index(daily_fhour: int) -> int:
    """Store time index of the instantaneous snapshot at lead day ``daily_fhour//24``.

    time[i] is valid at init + (i+1)*6h, so the valid time D*24h is index 4D-1.
    """
    return 4 * (daily_fhour // 24) - 1


def _precip_time_indices(daily_fhour: int) -> list[int]:
    """The four 6h precip-bucket indices making up the day ending at ``daily_fhour``."""
    d = daily_fhour // 24
    return [4 * d - 4, 4 * d - 3, 4 * d - 2, 4 * d - 1]


def _conus_index_slices(lats, lons):
    """Return (lat_slc, lon_slc, kept_lats, kept_lons, lon_sort, flip_lat) for CONUS.

    Output ascends south→north and converts lon to -180..180 (sorted ascending),
    matching ``base.normalize_and_crop`` so WeatherNext 2's grid aligns with the land
    mask in ``extract_forecast``. The WeatherNext mean grid is lat -90..90
    ascending / lon 0..359.75, so ``flip_lat`` is False and ``lon_sort`` is the
    contiguous 230..300° band remapped to -130..-60°.
    """
    import numpy as np

    west, south, east, north = US_CROP_BOUNDS
    lat_idx = np.where((lats >= south) & (lats <= north))[0]
    west360, east360 = west % 360, east % 360
    lon_idx = np.where((lons >= west360) & (lons <= east360))[0]
    lat_slc = slice(int(lat_idx.min()), int(lat_idx.max()) + 1)
    lon_slc = slice(int(lon_idx.min()), int(lon_idx.max()) + 1)

    kept_lats = lats[lat_slc]
    kept_lons = ((lons[lon_slc] + 180.0) % 360.0) - 180.0
    lon_sort = np.argsort(kept_lons)
    kept_lons_sorted = kept_lons[lon_sort]
    flip_lat = bool(kept_lats[0] > kept_lats[-1])
    if flip_lat:
        kept_lats = kept_lats[::-1]
    return lat_slc, lon_slc, kept_lats, kept_lons_sorted, lon_sort, flip_lat


def _save_grid(output_dir: Path, kept_lats, kept_lons) -> None:
    """Write grid_lats.npy/grid_lons.npy once. All variables share one grid."""
    import numpy as np

    glat = output_dir / "grid_lats.npy"
    glon = output_dir / "grid_lons.npy"
    if not glat.exists():
        np.save(glat, kept_lats)
        np.save(glon, kept_lons)


class WeatherNext2DownloaderParallel(BaseDownloader):
    def __init__(
        self,
        output_dir: str = "model_data/weathernext2",
        max_workers: int = 8,
        max_retries: int = 3,
        timeout_seconds: int = 300,
        polite_delay_seconds: float = 0.0,
    ):
        super().__init__(
            output_dir,
            max_workers=max_workers,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            polite_delay_seconds=polite_delay_seconds,
            base_url=WN2_ROOT,
            user_agent="WeatherNext2DownloaderParallel/2.0",
        )

    # ── Paths ────────────────────────────────────────────────────────

    def _store_path(self, init_date: datetime, cycle: int) -> str:
        return f"{WN2_ROOT}/{init_date:%Y%m%d}_{cycle:02d}hr_01_preds/predictions.zarr"

    def _init_dir(self, init_date: datetime, cycle: int) -> Path:
        out = self.output_dir / str(init_date.year) / f"{init_date:%Y%m%d}_{cycle:02d}z"
        out.mkdir(parents=True, exist_ok=True)
        return out

    @staticmethod
    def _var_suffixes() -> tuple[str, ...]:
        # precip keeps the historical "surface" suffix; instant vars use their key.
        return ("surface", "temp", "wind", "humidity")

    def _all_vars_present(self, init_date: datetime, cycle: int, daily_fhours: list[int]) -> bool:
        init_dir = self._init_dir(init_date, cycle)
        return all(
            (init_dir / f"f{fh:03d}_{sfx}.npy").exists()
            for fh in daily_fhours
            for sfx in self._var_suffixes()
        )

    def _open_store(self, init_date: datetime, cycle: int):
        import xarray as xr

        return xr.open_zarr(
            self._store_path(init_date, cycle),
            storage_options=WN2_STORAGE_OPTIONS,
            consolidated=True,
        )

    # ── Forecast-mode discovery (cycle-flexible, completeness-aware) ──

    def _list_remote_cycles_on_date(self, init_date: datetime) -> list[int]:
        """Sorted cycles (e.g. [0, 6, 12, 18]) with a published store on a date."""
        import gcsfs

        fs = gcsfs.GCSFileSystem(token="google_default")
        pattern = f"{WN2_BUCKET}/{WN2_PREFIX}/{init_date:%Y%m%d}_*hr_01_preds"
        try:
            hits = fs.glob(pattern)
        except Exception:
            return []
        cycles: set[int] = set()
        for h in hits:
            name = h.rstrip("/").rsplit("/", 1)[-1]  # e.g. 20260529_18hr_01_preds
            try:
                _, rest = name.split("_", 1)          # "18hr_01_preds"
                cycles.add(int(rest.split("hr", 1)[0]))
            except (ValueError, IndexError):
                continue
        return sorted(cycles)

    def _store_is_complete(self, init_date: datetime, cycle: int, daily_fhours: list[int]) -> bool:
        """True iff the store is fully written for every variable at the deepest lead.

        A freshly-published store is filled progressively (wind/precip first, then
        2 m temperature, then pressure-level temperature/humidity), so checking the
        deepest instant timestep of all five source fields catches a half-written
        store before it can be republished as a NaN forecast.
        """
        import numpy as np

        try:
            ds = self._open_store(init_date, cycle)
        except Exception:
            return False
        try:
            lats = np.ravel(ds["lat"].values)
            lons = np.ravel(ds["lon"].values)
            lat_slc, lon_slc, *_ = _conus_index_slices(lats, lons)
            lev = np.ravel(ds["level"].values)
            level_idx = int(np.where(lev == RH_LEVEL_HPA)[0][0])
            t_last = _instant_time_index(max(daily_fhours))
            checks = [
                ds[WN2_TEMP].isel(time=t_last, lat=lat_slc, lon=lon_slc).values,
                ds[WN2_WIND].isel(time=t_last, lat=lat_slc, lon=lon_slc).values,
                ds[WN2_PRECIP].isel(time=t_last, lat=lat_slc, lon=lon_slc).values,
                ds[WN2_T_LEVEL].isel(time=t_last, level=level_idx, lat=lat_slc, lon=lon_slc).values,
                ds[WN2_Q_LEVEL].isel(time=t_last, level=level_idx, lat=lat_slc, lon=lon_slc).values,
            ]
        except Exception:
            return False
        finally:
            ds.close()
        return all(np.isfinite(c).mean() > 0.99 for c in checks)

    def find_latest_forecast_init(
        self,
        candidates: list[datetime],
        prefer_cycles: tuple[int, ...] = WN2_FORECAST_CYCLES,
    ) -> tuple[datetime, int] | None:
        """Find the freshest *complete* remote init across ``candidates``.

        ``candidates`` are newest-to-oldest dates. For each, considers cycles in
        ``prefer_cycles`` order (then any others, newest first) and returns the
        first whose store passes the completeness check. Returns ``(init_date,
        cycle)`` or ``None`` if no complete init exists in the candidate dates.
        """
        daily_fhours = [d * 24 for d in range(1, WN2_MAX_LEAD_H // 24 + 1)]
        for d in candidates:
            available = self._list_remote_cycles_on_date(d)
            if not available:
                continue
            ordered = [c for c in prefer_cycles if c in available]
            ordered += [c for c in sorted(available, reverse=True) if c not in ordered]
            for c in ordered:
                if self._store_is_complete(d, c, daily_fhours):
                    return d, c
                print(f"  {d.date()} {c:02d}z store incomplete (still being written); "
                      f"trying an older cycle")
        return None

    # ── Per-init processing ──────────────────────────────────────────

    def _process_init(self, init_date: datetime, cycle: int, daily_fhours: list[int]) -> str:
        """Read one WeatherNext 2 store → write per-variable daily .npy files.

        Returns a status string: ``exists`` / ``processed`` / ``not_found`` /
        ``failed: ...`` (default ``_status_key`` collapses the last into ``failed``).
        """
        import numpy as np

        init_dir = self._init_dir(init_date, cycle)
        if self._all_vars_present(init_date, cycle, daily_fhours):
            return "exists"

        try:
            ds = self._open_store(init_date, cycle)
        except FileNotFoundError:
            return "not_found"
        except Exception as e:
            msg = str(e)
            if "not" in msg.lower() and "exist" in msg.lower():
                return "not_found"
            return f"failed: open {e}"

        try:
            lats = np.ravel(ds["lat"].values).astype(np.float32)
            lons = np.ravel(ds["lon"].values).astype(np.float32)
            lat_slc, lon_slc, kept_lats, kept_lons, lon_sort, flip_lat = _conus_index_slices(lats, lons)
            lev = np.ravel(ds["level"].values)
            level_idx = int(np.where(lev == RH_LEVEL_HPA)[0][0])

            inst_idxs = [_instant_time_index(fh) for fh in daily_fhours]
            max_t = max(_precip_time_indices(max(daily_fhours)))
            if max_t >= ds.sizes["time"]:
                return f"failed: store has {ds.sizes['time']} steps, need {max_t + 1}"

            # One read per source field (subset to the needed times + CONUS box).
            temp_k = ds[WN2_TEMP].isel(time=inst_idxs, lat=lat_slc, lon=lon_slc).values
            wind = ds[WN2_WIND].isel(time=inst_idxs, lat=lat_slc, lon=lon_slc).values
            t1000 = ds[WN2_T_LEVEL].isel(time=inst_idxs, level=level_idx, lat=lat_slc, lon=lon_slc).values
            q1000 = ds[WN2_Q_LEVEL].isel(time=inst_idxs, level=level_idx, lat=lat_slc, lon=lon_slc).values
            precip_all = ds[WN2_PRECIP].isel(time=slice(0, max_t + 1), lat=lat_slc, lon=lon_slc).values
        except Exception as e:
            return f"failed: read {e}"
        finally:
            ds.close()

        # Guard against a half-written store slipping through (forecast selection
        # already verifies completeness, but archive/catchup runs do not).
        if not np.isfinite(temp_k[-1]).any():
            return "failed: store incomplete (deepest lead all-NaN)"

        def to_grid(arr2d):
            """Reorder a CONUS-sliced (lat, lon) field to -180..180 / south→north."""
            arr2d = arr2d[:, lon_sort]
            if flip_lat:
                arr2d = arr2d[::-1, :]
            return arr2d.astype(np.float32)

        _save_grid(self.output_dir, kept_lats, kept_lons)

        for k, fh in enumerate(daily_fhours):
            # precip: sum the four 6h buckets of this day, m → mm, clip noise.
            p_idxs = _precip_time_indices(fh)
            precip = to_grid(precip_all[p_idxs].sum(axis=0)) * 1000.0
            np.clip(precip, 0.0, None, out=precip)
            np.save(init_dir / f"f{fh:03d}_surface.npy", precip)

            # temp: K → °C ; wind: direct m/s.
            np.save(init_dir / f"f{fh:03d}_temp.npy", to_grid(kelvin_to_celsius(temp_k[k])))
            np.save(init_dir / f"f{fh:03d}_wind.npy", to_grid(wind[k]))

            # humidity: RH (%) from 1000 hPa temperature (K) + specific humidity (kg/kg).
            rh = rh_from_specific_humidity(t1000[k], q1000[k], RH_LEVEL_PA)
            np.save(init_dir / f"f{fh:03d}_humidity.npy", to_grid(rh))

        return "processed"

    def download_forecast_init(self, init_date: datetime, cycle: int) -> bool:
        """Download a single WeatherNext 2 init at the specified cycle.

        Returns True iff every variable's daily lead .npy exists on disk afterward.
        """
        daily_fhours = [d * 24 for d in range(1, WN2_MAX_LEAD_H // 24 + 1)]
        if self._all_vars_present(init_date, cycle, daily_fhours):
            return True

        print("\n" + "=" * 70)
        print("WeatherNext 2 forecast download")
        print(f"Init: {init_date.date()} {cycle:02d}z | Lead 1-{WN2_MAX_LEAD_H // 24}d")
        print(f"Variables: precip + temp + wind + humidity")
        print(f"Output: {self.output_dir.resolve()}")
        print("=" * 70)

        t0 = time.time()
        status = self._process_init(init_date, cycle, daily_fhours)
        print(f"[WeatherNext 2] {init_date.date()} {cycle:02d}z -> {status} ({time.time() - t0:.1f}s)")
        return self._all_vars_present(init_date, cycle, daily_fhours)

    # ── Public download interface (archive / catchup) ────────────────

    def _download(self, start: datetime, end: datetime, *, forecast_hours=None, level: str = "surface"):
        """Archive/catchup over a date range at the fixed 12z cycle.

        forecast_hours is ignored — WeatherNext 2 always yields leads 1..10d.
        """
        cycle = WN2_CYCLE
        if start < WN2_FIRST_DATE:
            print(f"WeatherNext 2 per-init stores start {WN2_FIRST_DATE.date()}; clipping start.")
            start = WN2_FIRST_DATE

        daily_fhours = [d * 24 for d in range(1, WN2_MAX_LEAD_H // 24 + 1)]
        init_dates: list[datetime] = []
        cur = datetime(start.year, start.month, start.day)
        end0 = datetime(end.year, end.month, end.day)
        while cur <= end0:
            init_dates.append(cur)
            cur += timedelta(days=1)
        if not init_dates:
            raise ValueError("No init dates selected (post-clip).")

        print("\n" + "=" * 70)
        print("WeatherNext 2 download")
        print(f"Period: {start.date()} to {end.date()} | {cycle:02d}z | Lead 1-{WN2_MAX_LEAD_H // 24}d")
        print(f"Variables: precip + temp + wind + humidity")
        print(f"Output: {self.output_dir.resolve()}")
        print(f"Inits: {len(init_dates)}")
        print("=" * 70)

        counts: dict[str, int] = defaultdict(int)
        examples: list[str] = []
        total = len(init_dates)
        t0 = time.time()
        for i, d in enumerate(init_dates, start=1):
            if self._all_vars_present(d, cycle, daily_fhours):
                status = "exists"
            else:
                status = self._process_init(d, cycle, daily_fhours)
            counts[self._status_key(status)] += 1
            if status.startswith("failed") and len(examples) < 5:
                examples.append(f"{d:%Y-%m-%d} {cycle:02d}z -> {status[:120]}")
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta_min = ((total - i) / rate / 60) if rate > 0 else 0
            parts = " | ".join(f"{k}={counts[k]}" for k in sorted(counts))
            print(f"\r[WeatherNext 2] {i}/{total} | {parts} | "
                  f"{rate * 60:.1f} inits/min | ETA {eta_min:.1f} min", end="", flush=True)
        print()
        if examples:
            print("Examples:")
            for e in examples:
                print("  " + e)
        print(f"[WeatherNext 2] done in {(time.time() - t0) / 60:.1f} min")


if __name__ == "__main__":
    import argparse

    sys.path.insert(0, str(_this_dir.parent))
    from model_registry import MODEL_REGISTRY

    cfg = MODEL_REGISTRY["weathernext2"]

    parser = argparse.ArgumentParser(description="Download WeatherNext 2 forecasts")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD).")
    parser.add_argument("--start-year", type=int, help="Start year")
    parser.add_argument("--end-year", type=int, help="End year")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    downloader = WeatherNext2DownloaderParallel(
        output_dir=cfg.data_dir,
        max_workers=args.workers,
        max_retries=3,
        timeout_seconds=300,
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
