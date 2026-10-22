"""Normalized access to local and range-backed tARget netCDF4/HDF5 data."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import re
from typing import Iterator

import h5py
import numpy as np
import requests

from atmospheric_rivers.remote import CachedMultipartFile


TIME_NAMES = ("time", "date", "datetime", "valid_time")
LAT_NAMES = ("lat", "latitude", "y")
LON_NAMES = ("lon", "longitude", "x")
_CF_TIME_RE = re.compile(
    r"^(seconds|minutes|hours|days)\s+since\s+(.+)$", re.IGNORECASE,
)


@dataclass(frozen=True)
class NormalizedFrame:
    source: Path
    index: int
    timestamp: str
    mask: np.ndarray
    lats: np.ndarray
    lons: np.ndarray


def _basename(name: str) -> str:
    return name.rsplit("/", 1)[-1]


def find_coordinate(names, candidates: tuple[str, ...], label: str) -> str:
    lowered = {_basename(str(name)).lower(): str(name) for name in names}
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    raise ValueError(f"Could not identify {label}; available names: {sorted(map(str, names))}")


def find_mask_variable(
    datasets: dict[str, h5py.Dataset], requested: str | None,
    coordinate_names: tuple[str, str, str],
) -> str:
    if requested:
        matches = [name for name in datasets if name == requested or _basename(name) == requested]
        if len(matches) != 1:
            raise ValueError(f"Variable {requested!r} is not uniquely present")
        return matches[0]
    time_name, lat_name, lon_name = coordinate_names
    sizes = (
        int(datasets[time_name].size), int(datasets[lat_name].size), int(datasets[lon_name].size),
    )
    candidates = []
    for name, variable in datasets.items():
        if name in coordinate_names or variable.ndim < 3:
            continue
        shape = tuple(int(value) for value in variable.shape)
        if all(size in shape for size in sizes):
            score = sum(token in _basename(name).lower() for token in ("ar", "mask", "shape"))
            candidates.append((score, name))
    if not candidates:
        raise ValueError("No data variable has time, latitude, and longitude dimensions")
    return max(candidates)[1]


def _attribute_text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _decode_times(values: np.ndarray, variable: h5py.Dataset) -> np.ndarray:
    values = np.asarray(values)
    if np.issubdtype(values.dtype, np.datetime64):
        return values.astype("datetime64[s]")
    if values.dtype.kind in "SUO":
        return np.asarray([np.datetime64(_attribute_text(item)) for item in values])
    units = _attribute_text(variable.attrs.get("units", ""))
    match = _CF_TIME_RE.match(units.strip())
    if not match:
        raise ValueError(f"unsupported tARget time units {units!r}")
    unit, origin_text = match.groups()
    origin_text = origin_text.strip().replace(" UTC", "").replace("Z", "")
    try:
        origin = np.datetime64(origin_text, "us")
    except ValueError as exc:
        raise ValueError(f"invalid tARget time origin {origin_text!r}") from exc
    microseconds = {
        "seconds": 1_000_000,
        "minutes": 60 * 1_000_000,
        "hours": 3600 * 1_000_000,
        "days": 86400 * 1_000_000,
    }[unit.lower()]
    offsets = np.rint(values.astype(np.float64) * microseconds).astype("timedelta64[us]")
    return (origin + offsets).astype("datetime64[s]")


def iso_time(value) -> str:
    try:
        return np.datetime_as_string(np.datetime64(value), unit="s") + "Z"
    except (TypeError, ValueError):
        return str(value).replace(" ", "T")


def normalize_grid(mask: np.ndarray, lats: np.ndarray, lons: np.ndarray):
    lats = np.asarray(lats, dtype=float)
    lons = ((np.asarray(lons, dtype=float) + 180.0) % 360.0) - 180.0
    lon_order = np.argsort(lons)
    lat_order = np.argsort(lats)[::-1]
    return np.asarray(mask)[np.ix_(lat_order, lon_order)], lats[lat_order], lons[lon_order]


def _coordinate_axes(
    data: h5py.Dataset, coordinates: tuple[h5py.Dataset, h5py.Dataset, h5py.Dataset],
) -> tuple[int, int, int]:
    """Resolve axes from HDF5 dimension scales, then unambiguous sizes/order."""
    resolved: list[int | None] = [None, None, None]
    for axis, dimension in enumerate(data.dims):
        attached = []
        try:
            attached = [scale.name for scale in dimension.values()]
        except (KeyError, RuntimeError):
            pass
        label = str(dimension.label or "").lower()
        for coordinate_index, coordinate in enumerate(coordinates):
            basename = _basename(coordinate.name).lower()
            if coordinate.name in attached or (label and label == basename):
                resolved[coordinate_index] = axis

    used = {axis for axis in resolved if axis is not None}
    for coordinate_index, coordinate in enumerate(coordinates):
        if resolved[coordinate_index] is not None:
            continue
        matches = [
            axis for axis, size in enumerate(data.shape)
            if axis not in used and size == coordinate.size
        ]
        if len(matches) == 1:
            resolved[coordinate_index] = matches[0]
            used.add(matches[0])

    # Classic tARget variables use (time, lat, lon). This deterministic fallback
    # also handles synthetic/small files where coordinate lengths happen to tie.
    remaining = [axis for axis in range(data.ndim) if axis not in used and data.shape[axis] != 1]
    for coordinate_index, coordinate in enumerate(coordinates):
        if resolved[coordinate_index] is not None:
            continue
        matches = [axis for axis in remaining if data.shape[axis] == coordinate.size]
        if not matches:
            raise ValueError(
                f"could not identify coordinate axis in mask shape {data.shape}"
            )
        resolved[coordinate_index] = matches[0]
        remaining.remove(matches[0])
    return tuple(int(axis) for axis in resolved)


def _normalized_data(root: h5py.File, variable: str | None):
    # The canonical file has many very large ancillary datasets. HDF5 object
    # headers can live far from the superblock, so recursively visiting every
    # object would itself trigger broad remote reads. Resolve the three root
    # coordinates and canonical shapemap directly; use discovery only as a
    # compatibility fallback for small/local variants.
    names = list(root.keys())
    time = find_coordinate(names, TIME_NAMES, "time coordinate")
    lat = find_coordinate(names, LAT_NAMES, "latitude coordinate")
    lon = find_coordinate(names, LON_NAMES, "longitude coordinate")
    datasets = {name: root[name] for name in (time, lat, lon)}
    if variable:
        matches = [name for name in names if name == variable or _basename(name) == variable]
        if len(matches) != 1:
            raise ValueError(f"Variable {variable!r} is not uniquely present")
        name = matches[0]
        datasets[name] = root[name]
    elif "shapemap" in root:
        name = "shapemap"
        datasets[name] = root[name]
    else:
        for candidate in names:
            value = root[candidate]
            if isinstance(value, h5py.Dataset):
                datasets[candidate] = value
        name = find_mask_variable(datasets, None, (time, lat, lon))
    data = datasets[name]
    shape = tuple(int(value) for value in data.shape)
    axes = _coordinate_axes(data, (datasets[time], datasets[lat], datasets[lon]))
    extras = [axis for axis, size in enumerate(shape) if axis not in axes and size != 1]
    if extras:
        raise ValueError(f"{name!r} has unsupported non-singleton axes {extras}")
    if datasets[time].size == 0:
        raise ValueError("dataset contains no tARget time frames")
    return datasets, (time, lat, lon), data, axes


@contextmanager
def _open_source(
    source: Path, *, session: requests.Session | None = None,
    timeout: int = 120, retries: int = 3, refresh: bool = False,
):
    source = Path(source)
    remote = None
    try:
        if source.name.endswith(".index.json"):
            remote = CachedMultipartFile(
                source, session=session, timeout=timeout, retries=retries,
                refresh=refresh,
            )
            root = h5py.File(remote, "r", driver="fileobj")
        else:
            root = h5py.File(source, "r")
        with root:
            yield root
    finally:
        if remote is not None:
            remote.close()


def validate_source(
    path: Path, *, session: requests.Session | None = None,
    timeout: int = 120, retries: int = 3,
) -> None:
    with _open_source(path, session=session, timeout=timeout, retries=retries) as root:
        _normalized_data(root, None)


def _date_bounds(start_date: str | None, end_date: str | None):
    if start_date is None and end_date is None:
        return None
    if start_date is None or end_date is None:
        raise ValueError("both start_date and end_date are required")
    start = np.datetime64(date.fromisoformat(start_date), "s")
    end = np.datetime64(date.fromisoformat(end_date) + timedelta(days=1), "s")
    if start >= end:
        raise ValueError("start_date must not be after end_date")
    return start, end


def _available_ranges(source: Path) -> list[tuple[np.datetime64, np.datetime64]]:
    if not source.name.endswith(".index.json"):
        return []
    from atmospheric_rivers.remote import read_index

    ranges = []
    for item in read_index(source).get("available_date_ranges") or []:
        start = np.datetime64(date.fromisoformat(item["start"]), "s")
        end = np.datetime64(date.fromisoformat(item["end"]) + timedelta(days=1), "s")
        ranges.append((start, end))
    return ranges


def _available_bounds(source: Path) -> tuple[float, float, float, float] | None:
    if not source.name.endswith(".index.json"):
        return None
    from atmospheric_rivers.remote import read_index

    raw = read_index(source).get("available_bounds") or []
    if len(raw) != 4:
        return None
    return tuple(float(value) for value in raw)


def _spatial_slice(values: np.ndarray, low: float, high: float, *, longitude: bool):
    comparable = np.asarray(values, dtype=float)
    if longitude:
        comparable = ((comparable + 180.0) % 360.0) - 180.0
    selected = np.flatnonzero((comparable >= low) & (comparable <= high))
    if selected.size == 0:
        raise ValueError(f"tARget grid does not intersect requested bounds {low}..{high}")
    # h5py performs efficient hyperslab reads for slices. Include any cells
    # between the selected endpoints, then normalize/crop exactly afterward.
    return slice(int(selected.min()), int(selected.max()) + 1)


def _read_mask(
    data: h5py.Dataset, axes: tuple[int, int, int], frame_index: int,
    lat_slice: slice, lon_slice: slice,
) -> np.ndarray:
    selection: list[object] = [0] * data.ndim
    time_axis, lat_axis, lon_axis = axes
    selection[time_axis] = frame_index
    selection[lat_axis] = lat_slice
    selection[lon_axis] = lon_slice
    raw = np.asarray(data[tuple(selection)])
    remaining_axes = [axis for axis in range(data.ndim) if axis != time_axis and data.shape[axis] != 1]
    raw = np.squeeze(raw)
    lat_position = remaining_axes.index(lat_axis)
    lon_position = remaining_axes.index(lon_axis)
    return np.moveaxis(raw, (lat_position, lon_position), (0, 1))


def iter_normalized_frames(
    sources: list[Path], *, variable: str | None = None,
    start_date: str | None = None, end_date: str | None = None,
    bounds: tuple[float, float, float, float] | None = None,
    session: requests.Session | None = None, timeout: int = 120, retries: int = 3,
    refresh: bool = False,
) -> Iterator[NormalizedFrame]:
    """Iterate normalized frames, constrained to downloaded virtual date windows.

    ``bounds`` is ``(west, south, east, north)``. Local netCDF inputs remain
    unrestricted when dates are omitted; virtual inputs use their recorded,
    successfully prefetched coverage.
    """
    requested = _date_bounds(start_date, end_date)
    for source in sources:
        source = Path(source)
        allowed = [requested] if requested else _available_ranges(source)
        if requested is None and source.name.endswith(".index.json") and not allowed:
            raise ValueError(
                "tARget index has no cached date ranges; run download.py --ar "
                "with --start-date/--end-date first"
            )
        with _open_source(
            source, session=session, timeout=timeout, retries=retries, refresh=refresh,
        ) as root:
            datasets, coordinate_names, data, axes = _normalized_data(root, variable)
            time_name, lat_name, lon_name = coordinate_names
            times = _decode_times(datasets[time_name][...], datasets[time_name])
            raw_lats = np.asarray(datasets[lat_name][...])
            raw_lons = np.asarray(datasets[lon_name][...])
            selected_bounds = bounds or _available_bounds(source)
            if selected_bounds:
                west, south, east, north = selected_bounds
                lat_slice = _spatial_slice(raw_lats, south, north, longitude=False)
                lon_slice = _spatial_slice(raw_lons, west, east, longitude=True)
            else:
                lat_slice = slice(None)
                lon_slice = slice(None)
            chosen_lats = raw_lats[lat_slice]
            chosen_lons = raw_lons[lon_slice]
            for index, raw_time in enumerate(times):
                if allowed and not any(start <= raw_time < end for start, end in allowed):
                    continue
                mask = _read_mask(data, axes, index, lat_slice, lon_slice) > 0
                mask, lats, lons = normalize_grid(mask, chosen_lats, chosen_lons)
                yield NormalizedFrame(
                    source=source, index=index, timestamp=iso_time(raw_time),
                    mask=mask, lats=lats, lons=lons,
                )
