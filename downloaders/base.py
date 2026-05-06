"""Base downloader: shared session, parallel execution, progress reporting."""

from __future__ import annotations

import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Callable, TYPE_CHECKING, TypeVar

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
import requests
from requests.adapters import HTTPAdapter

if TYPE_CHECKING:
    from model_registry import ModelConfig

T = TypeVar("T")

PRISM_DIR = Path("prism_data")


def build_land_mask(lats: np.ndarray, lons: np.ndarray, transform) -> np.ndarray | None:
    """Reproject any PRISM GeoTIFF onto the given model grid → boolean CONUS land mask.

    PRISM covers U.S. land only, so its footprint is the land mask. Used to
    NaN-fill non-U.S. cells on map-facing outputs so the app (U.S.-only) does
    not render precipitation over ocean / outside CONUS.
    """
    prism_files = sorted(PRISM_DIR.glob("**/data.tif"))
    if not prism_files:
        print("Warning: no PRISM data found for land mask.")
        return None

    with rasterio.open(prism_files[0]) as src:
        prism_data = src.read(1).astype(np.float32)
        prism_transform = src.transform
        prism_crs = str(src.crs)
        if src.nodata is not None:
            prism_data[prism_data == src.nodata] = np.nan

    dst = np.full((lats.size, lons.size), np.nan, dtype=np.float32)
    reproject(
        source=prism_data,
        destination=dst,
        src_transform=prism_transform,
        src_crs=prism_crs,
        dst_transform=transform,
        dst_crs="EPSG:4326",
        resampling=Resampling.nearest,
        dst_nodata=np.nan,
    )
    mask = np.isfinite(dst)
    print(f"Land mask from {prism_files[0]} ({np.count_nonzero(mask)} land pixels)")
    return mask


def apply_land_mask(arr: np.ndarray, mask: np.ndarray | None) -> np.ndarray:
    """Return a copy of `arr` with non-land cells set to NaN. No-op if `mask` is None."""
    if mask is None or mask.shape != arr.shape:
        return arr
    out = arr.astype(np.float32, copy=True)
    out[~mask] = np.nan
    return out


class BaseDownloader:
    """Shared init, session, and parallel run for model downloaders."""

    def __init__(
        self,
        output_dir: str | Path,
        *,
        max_workers: int = 6,
        max_retries: int = 3,
        timeout_seconds: int = 60,
        polite_delay_seconds: float = 0.0,
        base_url: str = "",
        user_agent: str = "BaseDownloader/1.0",
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = base_url
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.polite_delay_seconds = polite_delay_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        # Match adapter pool size to worker concurrency so requests aren't serialized by small pools.
        pool_size = max(16, self.max_workers * 4)
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    # CONUS land mask: expose the module-level helpers as methods so subclasses
    # (GFS/NBM extract_forecast) can mask forecast arrays with one call.
    _build_land_mask = staticmethod(build_land_mask)
    _apply_land_mask = staticmethod(apply_land_mask)

    @staticmethod
    def _to_dt(d) -> datetime:
        """Normalize 'YYYY-MM-DD' string or datetime to datetime."""
        if isinstance(d, str):
            return datetime.strptime(d, "%Y-%m-%d")
        return d

    def _status_key(self, status: str) -> str:
        """Map raw status string to a count key. Override in subclasses for custom mappings.

        Default collapses 'downloaded ...' → 'downloaded' and 'failed: ...' → 'failed'.
        """
        if status.startswith("downloaded"):
            return "downloaded"
        if status.startswith("failed"):
            return "failed"
        return status

    def download_date_range(self, start_date, end_date, **kwargs):
        """Download a date range. Subclasses define _download(start, end, **kw)."""
        start = self._to_dt(start_date)
        end = self._to_dt(end_date)
        return self._download(start, end, **kwargs)

    def download_year_range(self, start_year: int, end_year: int, **kwargs):
        """Download every day in [start_year, end_year]. Subclasses define _download(...)."""
        start = datetime(int(start_year), 1, 1)
        end = datetime(int(end_year), 12, 31)
        return self._download(start, end, **kwargs)

    def extract_forecast(
        self,
        model_config: "ModelConfig",
        *,
        init_date: datetime | None = None,
        output_root: Path | None = None,
    ) -> None:
        """Extract per-lead forecast .npy files into stats_output/forecast/ format.

        Discovers the latest init dir for ``model_config.cycle_hour``, loads
        each daily-lead .npy, applies the CONUS land mask, writes per-lead
        ``lead_{N}.npz`` files, and writes per-window averages for
        ``model_config.lead_windows``.
        """
        import rasterio.transform
        from model_registry import window_to_key
        from stats_io import write_lead_windows

        cycle = model_config.cycle_hour
        forecast_hours = model_config.forecast_hours
        lead_windows = list(model_config.lead_windows)

        model_dir = self.output_dir
        if output_root is None:
            output_root = Path("stats_output")
        forecast_dir = output_root / "forecast"
        forecast_dir.mkdir(parents=True, exist_ok=True)

        if init_date is None:
            all_inits = sorted(model_dir.glob(f"*/*_{cycle:02d}z"), reverse=True)
            for candidate in all_inits:
                if any(candidate.glob("f*_*.npy")):
                    init_date = datetime.strptime(candidate.name[:8], "%Y%m%d")
                    break
            if init_date is None:
                raise SystemExit(f"No init directories with data found under {model_dir}.")
            print(f"Using most recent init date: {init_date.date()}")

        date_str = init_date.strftime("%Y%m%d")
        init_dir = model_dir / str(init_date.year) / f"{date_str}_{cycle:02d}z"
        if not init_dir.exists():
            raise SystemExit(f"Init directory not found: {init_dir}")

        grid_lats_path = model_dir / "grid_lats.npy"
        grid_lons_path = model_dir / "grid_lons.npy"
        if not grid_lats_path.exists() or not grid_lons_path.exists():
            raise SystemExit(
                f"Grid coordinate files not found under {model_dir}. Run download first."
            )

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

        def _write_window(start: int, end: int, arr: np.ndarray) -> None:
            wkey = window_to_key(start, end)
            np.savez_compressed(forecast_dir / f"lead_{wkey}.npz", precip=arr)
            print(f"  Wrote lead_{wkey}.npz (avg of leads {start}-{end})")

        write_lead_windows(
            lead_data,
            lead_windows,
            write_fn=_write_window,
            combine_fn=lambda arrs: np.mean(arrs, axis=0),
        )

        print(f"\nWrote {len(lead_data)} lead files + windows to {forecast_dir}")
        print(f"Init date: {init_date.date()} {cycle:02d}z")

    def _byte_range_fetch(
        self,
        *,
        grib_url: str,
        idx_url: str,
        idx_parser: Callable[[str], tuple[int | None, int | None]],
        out_path: Path,
    ) -> str:
        """Fetch one GRIB2 record via HTTP byte-range using a sidecar idx file.

        Sequence per attempt: GET .idx → parse → Range-restricted GET .grib2 →
        validate GRIB magic. Retries up to ``self.max_retries`` with linear backoff.

        ``idx_parser(idx_text)`` returns ``(start_byte, end_byte_inclusive_or_None)``
        — return ``(None, None)`` if the record is not present in the idx.

        Returns a status string compatible with the default ``_status_key``:
        ``"exists"``, ``"not_found_idx"``, ``"not_found_var"``,
        ``"downloaded (... KB)"``, or ``"failed: ..."``.
        """
        if out_path.exists():
            return "exists"

        last_err = None
        if self.polite_delay_seconds:
            time.sleep(self.polite_delay_seconds)

        for attempt in range(1, self.max_retries + 1):
            part = out_path.with_suffix(out_path.suffix + ".part")
            try:
                idx_resp = self.session.get(idx_url, timeout=self.timeout_seconds)
                if idx_resp.status_code == 404:
                    return "not_found_idx"
                idx_resp.raise_for_status()
                start_byte, end_byte = idx_parser(idx_resp.text)
                if start_byte is None:
                    return "not_found_var"
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
                with open(part, "rb") as check:
                    magic = check.read(4)
                if magic != b"GRIB":
                    part.unlink(missing_ok=True)
                    last_err = ValueError(f"Invalid GRIB2 (magic={magic!r})")
                    time.sleep(1.25 * attempt)
                    continue
                part.replace(out_path)
                size_kb = out_path.stat().st_size / 1024
                return f"downloaded ({size_kb:.1f} KB)"
            except Exception as e:
                last_err = e
                if part.exists():
                    part.unlink(missing_ok=True)
                if out_path.exists():
                    out_path.unlink(missing_ok=True)
                time.sleep(1.25 * attempt)
        return f"failed: {last_err}"

    def _run_parallel(
        self,
        tasks: list[T],
        download_fn: Callable[[T], tuple[T, str]],
        description: str = "Download",
        progress_interval: int = 25,
    ) -> list[tuple[T, str]]:
        """Run download_fn on each task in parallel; aggregate counts, print progress; return (task, status) list."""
        if not tasks:
            print(f"{description}: no tasks")
            return []
        counts: dict[str, int] = defaultdict(int)
        results: list[tuple[T, str]] = []
        total = len(tasks)
        done = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            future_to_task = {pool.submit(download_fn, t): t for t in tasks}
            for fut in as_completed(future_to_task):
                task = future_to_task[fut]
                try:
                    _task, status = fut.result()
                    results.append((_task, status))
                except Exception as e:
                    status = f"failed: {e}"
                    results.append((task, status))
                key = self._status_key(status)
                counts[key] += 1
                done += 1
                if done % progress_interval == 0 or done == total:
                    parts = " | ".join(f"{k}={counts[k]}" for k in sorted(counts))
                    print(f"\r{description} Progress: {done}/{total} | {parts}", end="", flush=True)

        print()
        summary = " | ".join(f"{k}={counts[k]}" for k in sorted(counts))
        print(f"Done | {summary}")
        return results
