"""Direct chunk reads of ERA5 IVT from the public ARCO-ERA5 zarr on GCS.

The store keeps one full-globe field per hourly chunk (float32, blosc), so
range-less GETs of individual chunk objects are the cheapest possible
access path; no zarr/gcsfs machinery is needed and every read is a single
HTTPS request.  Grid: 721 x 1440, latitude 90..-90 descending, longitude
0..359.75 ascending — identical to the canonical tARget v4 catalog grid.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

import numpy as np
import requests
from numcodecs import Blosc

ARCO_BASE = ("https://storage.googleapis.com/gcp-public-data-arco-era5/"
             "ar/full_37-1h-0p25deg-chunk-1.zarr-v3")
IVT_EAST = "vertical_integral_of_eastward_water_vapour_flux"
IVT_NORTH = "vertical_integral_of_northward_water_vapour_flux"
# The store's time axis is hourly since this epoch (verified against the
# time coordinate chunk: time[0] == 0 with units "hours since 1900-01-01").
TIME_EPOCH = np.datetime64("1900-01-01T00", "h")
GRID_SHAPE = (721, 1440)
GRID_LATS = 90.0 - 0.25 * np.arange(721)
GRID_LONS = 0.25 * np.arange(1440)

_CODEC = Blosc()
_LOCAL = threading.local()


def _session() -> requests.Session:
    if not hasattr(_LOCAL, "session"):
        session = requests.Session()
        session.headers.update({"User-Agent": "ModelAccuracy tARget-local/1"})
        _LOCAL.session = session
    return _LOCAL.session


def hour_index(timestamp: np.datetime64 | str) -> int:
    """Chunk index of an hourly analysis time."""
    delta = np.datetime64(timestamp, "h") - TIME_EPOCH
    index = int(delta // np.timedelta64(1, "h"))
    if index < 0:
        raise ValueError(f"{timestamp} precedes the ARCO-ERA5 record")
    return index


def store_time_coverage() -> dict[str, str]:
    """Root attrs: valid_time_start/stop and the ERA5T (preliminary) stop."""
    with urllib.request.urlopen(f"{ARCO_BASE}/.zattrs", timeout=60) as response:
        attrs = json.loads(response.read())
    return {
        "valid_time_start": attrs["valid_time_start"],
        "valid_time_stop": attrs["valid_time_stop"],
        "valid_time_stop_era5t": attrs["valid_time_stop_era5t"],
    }


def fetch_field(variable: str, index: int, *, retries: int = 5,
                timeout: int = 120) -> np.ndarray:
    """One decoded (721, 1440) float32 field; retries transient failures."""
    url = f"{ARCO_BASE}/{variable}/{index}.0.0"
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = _session().get(url, timeout=timeout)
            response.raise_for_status()
            data = np.frombuffer(_CODEC.decode(response.content), dtype="<f4")
            return data.reshape(GRID_SHAPE)
        except (OSError, requests.RequestException, RuntimeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 30))
    raise IOError(f"failed to fetch {variable}[{index}] after {retries} "
                  f"attempts: {last_error}") from last_error


def iter_ivt_steps(
    timestamps: list[np.datetime64], *, workers: int = 24,
    prefetch_steps: int = 32,
) -> Iterator[tuple[np.datetime64, np.ndarray, np.ndarray]]:
    """Yield (time, ivtx, ivty) in order with bounded parallel prefetch."""
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = []
        cursor = 0

        def submit(ts):
            index = hour_index(ts)
            return (ts, executor.submit(fetch_field, IVT_EAST, index),
                    executor.submit(fetch_field, IVT_NORTH, index))

        while cursor < len(timestamps) or pending:
            while cursor < len(timestamps) and len(pending) < prefetch_steps:
                pending.append(submit(timestamps[cursor]))
                cursor += 1
            ts, east, north = pending.pop(0)
            yield ts, east.result(), north.result()
