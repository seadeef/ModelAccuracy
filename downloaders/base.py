"""Base downloader: shared session, parallel execution, progress reporting."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Callable, TypeVar

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
import requests
from requests.adapters import HTTPAdapter

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
