"""Index and range-cache the canonical Guan--Waliser tARget v4 ERA5 database.

The UCLA Dataverse record is the sole remote authority for AR ingestion.  The
record publishes the large ERA5 netCDF catalog as numbered pieces. This module
selects only those pieces and writes a local virtual-file index. Explicit date
windows prefetch only the HDF5 byte ranges needed for those frames.
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

from atmospheric_rivers import paths
from atmospheric_rivers.dataset import iter_normalized_frames, validate_source
from atmospheric_rivers.remote import DEFAULT_BLOCK_SIZE


TARGET_AR_DATASET_DOI = "doi:10.25346/S6/ZSW7UN"
TARGET_AR_DATASET_URL = "https://doi.org/10.25346/S6/ZSW7UN"
TARGET_AR_METADATA_URL = (
    "https://dataverse.ucla.edu/api/datasets/:persistentId/"
    f"?persistentId={TARGET_AR_DATASET_DOI}"
)
_DATAFILE_URL_PREFIX = "https://dataverse.ucla.edu/api/access/datafile/"
_CATALOG_RE = re.compile(
    r"^(globalARcatalog_ERA5_\d{4}-\d{4}_v4(?:\.\d+)?\.nc)(?:\.part(\d+))?$"
)
_INDEX_SCHEMA_VERSION = 1
# The application is CONUS-only. Restricting the prefetch to this rectangle
# lets chunked netCDF storage avoid unrelated global mask chunks when possible.
PREFETCH_BOUNDS = (-130.0, 20.0, -60.0, 55.0)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _fetch_metadata(session: requests.Session, timeout: int) -> dict[str, Any]:
    try:
        with session.get(TARGET_AR_METADATA_URL, timeout=timeout) as response:
            response.raise_for_status()
            payload = response.json()
    except requests.RequestException as exc:
        raise OSError(f"could not read canonical tARget metadata: {exc}") from exc
    if payload.get("status") != "OK" or not isinstance(payload.get("data"), dict):
        raise ValueError("UCLA Dataverse returned an invalid tARget dataset record")
    data = payload["data"]
    if data.get("protocol") != "doi" or data.get("authority") != "10.25346":
        raise ValueError("UCLA Dataverse returned an unexpected dataset authority")
    if data.get("identifier") != "S6/ZSW7UN":
        raise ValueError("UCLA Dataverse returned an unexpected tARget dataset")
    return data


def _select_era5_parts(dataset: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    version = dataset.get("latestVersion")
    if not isinstance(version, dict) or version.get("versionState") != "RELEASED":
        raise ValueError("the canonical tARget dataset has no released version")

    groups: dict[str, list[dict[str, Any]]] = {}
    for entry in version.get("files", []):
        if entry.get("restricted") or entry.get("directoryLabel") != "ERA5":
            continue
        match = _CATALOG_RE.fullmatch(str(entry.get("label", "")))
        if not match:
            continue
        file_data = entry.get("dataFile")
        if not isinstance(file_data, dict):
            continue
        checksum = file_data.get("checksum") or {}
        if checksum.get("type") != "MD5" or not checksum.get("value"):
            raise ValueError(f"missing MD5 checksum for {entry.get('label')}")
        part = dict(file_data)
        part["label"] = entry["label"]
        part["part_number"] = int(match.group(2) or 1)
        groups.setdefault(match.group(1), []).append(part)

    if len(groups) != 1:
        raise ValueError(
            "expected exactly one ERA5 tARget v4 catalog in the canonical dataset; "
            f"found {sorted(groups)}"
        )
    parts = next(iter(groups.values()))
    parts.sort(key=lambda item: item["part_number"])
    expected_numbers = list(range(1, len(parts) + 1))
    if [part["part_number"] for part in parts] != expected_numbers:
        raise ValueError("canonical ERA5 tARget catalog has missing or duplicate parts")
    return version, parts


def _part_descriptor(part: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(part["id"]),
        "label": str(part["label"]),
        "size": int(part["filesize"]),
        "md5": str(part["checksum"]["value"]).lower(),
    }


def _source_fingerprint(version: dict[str, Any], parts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "dataset": TARGET_AR_DATASET_DOI,
        "dataset_url": TARGET_AR_DATASET_URL,
        "dataset_version_id": int(version["id"]),
        "version": f"{version.get('versionNumber', 0)}.{version.get('versionMinorNumber', 0)}",
        "datafile_url_prefix": _DATAFILE_URL_PREFIX,
        "parts": [_part_descriptor(part) for part in parts],
    }


def _read_existing_index(path: Path, source: dict[str, Any]) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if payload.get("schema_version") != _INDEX_SCHEMA_VERSION:
        return None
    return payload if payload.get("source") == source else None


def _merge_date_ranges(ranges: list[dict[str, str]]) -> list[dict[str, str]]:
    parsed = sorted(
        (date.fromisoformat(item["start"]), date.fromisoformat(item["end"]))
        for item in ranges
    )
    merged: list[tuple[date, date]] = []
    for start, end in parsed:
        if start > end:
            raise ValueError(f"invalid tARget cached date range {start}..{end}")
        if merged and start <= merged[-1][1] + timedelta(days=1):
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return [{"start": start.isoformat(), "end": end.isoformat()} for start, end in merged]


def _range_is_cached(ranges: list[dict[str, str]], start: date, end: date) -> bool:
    return any(
        date.fromisoformat(item["start"]) <= start
        and date.fromisoformat(item["end"]) >= end
        for item in ranges
    )


def download_default_dataset(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    force: bool = False,
    session: requests.Session | None = None,
    timeout: int = 120,
    block_size: int = DEFAULT_BLOCK_SIZE,
    retries: int = 3,
) -> list[Path]:
    """Index the fixed catalog and optionally cache one inclusive date window.

    The caller cannot supply a URL.  Dataset identity, host, product family,
    checksums, and destination are controlled here so AR ingestion cannot drift
    to an unrelated or untrusted source. With no dates this is metadata-only.
    """
    if (start_date is None) != (end_date is None):
        raise ValueError("both start_date and end_date are required for an AR range")
    if start_date is not None:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        if start > end:
            raise ValueError("AR start date must not be after end date")
    elif force:
        raise ValueError("--force requires an explicit AR date or year range")

    own_session = session is None
    client = session or requests.Session()
    client.headers.update({"User-Agent": "ModelAccuracy tARget downloader/1"})
    try:
        print(f"Checking canonical tARget v4 source: {TARGET_AR_DATASET_URL}")
        dataset = _fetch_metadata(client, timeout)
        version, raw_parts = _select_era5_parts(dataset)
        source = _source_fingerprint(version, raw_parts)

        root = paths.TARGET_AR_DATA_DIR
        root.mkdir(parents=True, exist_ok=True)
        index_path = root / paths.TARGET_AR_INDEX_FILENAME
        existing = _read_existing_index(index_path, source)
        coverage = list((existing or {}).get("available_date_ranges") or [])
        payload = {
            "schema_version": _INDEX_SCHEMA_VERSION,
            "source": source,
            "virtual_size": sum(part["size"] for part in source["parts"]),
            "block_size": int(block_size),
            "available_date_ranges": coverage,
            "available_bounds": list((existing or {}).get("available_bounds") or []),
        }
        _atomic_write_json(index_path, payload)
        print(
            f"Indexed tARget v4 {source['version']} as a virtual "
            f"{payload['virtual_size'] / (1024 ** 3):.2f} GiB netCDF: {index_path}"
        )

        if start_date is None:
            print("No AR date range requested; no netCDF data blocks downloaded.")
            return [index_path]

        cache_root = root / "cache" / f"version-{source['dataset_version_id']}"

        if not force and _range_is_cached(coverage, start, end):
            validate_source(index_path, session=client, timeout=timeout, retries=retries)
            print(
                f"tARget frames for {start_date} through {end_date} are already cached."
            )
            return [index_path]

        print(f"Caching tARget frames for {start_date} through {end_date}...")
        frame_count = 0
        frame_days: list[str] = []
        for frame in iter_normalized_frames(
            [index_path], start_date=start_date, end_date=end_date,
            bounds=PREFETCH_BOUNDS, session=client, timeout=timeout, retries=retries,
            refresh=force,
        ):
            frame_count += 1
            frame_days.append(frame.timestamp[:10])
        if frame_count == 0:
            print("The indexed catalog has no tARget frames in that date range.")
            return [index_path]

        # Clamp requested ranges to the catalog's actual temporal coverage.
        actual_start, actual_end = min(frame_days), max(frame_days)
        coverage.append({"start": actual_start, "end": actual_end})
        payload["available_date_ranges"] = _merge_date_ranges(coverage)
        payload["available_bounds"] = list(PREFETCH_BOUNDS)
        validate_source(index_path, session=client, timeout=timeout, retries=retries)
        _atomic_write_json(index_path, payload)
        cache_bytes = sum(
            item.stat().st_size for item in cache_root.rglob("*.bin") if item.is_file()
        )
        print(
            f"Cached {frame_count} frames ({cache_bytes / (1024 ** 2):.1f} MiB on disk); "
            f"available ranges: {payload['available_date_ranges']}"
        )
        return [index_path]
    finally:
        if own_session:
            client.close()
