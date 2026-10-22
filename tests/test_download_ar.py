from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import h5py
import numpy as np

from atmospheric_rivers import paths
from atmospheric_rivers.dataset import iter_normalized_frames
from atmospheric_rivers.download import (
    TARGET_AR_METADATA_URL,
    _DATAFILE_URL_PREFIX,
    download_default_dataset,
)
from atmospheric_rivers.paths import discover_target_ar_sources
from atmospheric_rivers.remote import CachedMultipartFile


class _Response:
    def __init__(self, *, payload=None, content: bytes = b"", status_code=200, headers=None):
        self._payload = payload
        self.content = content
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Session:
    def __init__(self, metadata, files):
        self.metadata = metadata
        self.files = files
        self.headers = {}
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, dict(kwargs.get("headers") or {})))
        if url == TARGET_AR_METADATA_URL:
            return _Response(payload=self.metadata)
        file_id = int(url.removeprefix(_DATAFILE_URL_PREFIX))
        content = self.files[file_id]
        range_header = kwargs["headers"]["Range"]
        start_text, end_text = range_header.removeprefix("bytes=").split("-")
        start, end = int(start_text), int(end_text)
        return _Response(
            content=content[start:end + 1], status_code=206,
            headers={"Content-Range": f"bytes {start}-{end}/{len(content)}"},
        )

    def close(self):
        return None


def _file(file_id: int, label: str, content: bytes, directory="ERA5"):
    digest = hashlib.md5(content).hexdigest()
    return {
        "label": label,
        "restricted": False,
        "directoryLabel": directory,
        "dataFile": {
            "id": file_id,
            "filesize": len(content),
            "checksum": {"type": "MD5", "value": digest},
        },
    }


def _metadata(files):
    return {
        "status": "OK",
        "data": {
            "protocol": "doi",
            "authority": "10.25346",
            "identifier": "S6/ZSW7UN",
            "latestVersion": {
                "id": 2882,
                "versionNumber": 7,
                "versionMinorNumber": 0,
                "versionState": "RELEASED",
                "files": files,
            },
        },
    }


def _catalog_bytes(root: Path) -> bytes:
    path = root / "fixture.nc"
    with h5py.File(path, "w") as output:
        time = output.create_dataset("time", data=np.array([0, 6, 24, 30], dtype="i8"))
        time.attrs["units"] = "hours since 2025-01-01 00:00:00"
        output.create_dataset("lat", data=np.array([19.0, 25.0, 40.0, 56.0]))
        output.create_dataset("lon", data=np.array([220.0, 235.0, 260.0, 305.0]))
        masks = np.zeros((4, 4, 4), dtype="u1")
        masks[0, 1:3, 1:3] = 1
        masks[1, 2, 2] = 1
        masks[2:, :, :] = 1
        output.create_dataset("shapemap", data=masks, chunks=(1, 2, 2))
    return path.read_bytes()


def _split_catalog(content: bytes):
    cuts = (len(content) // 3 + 7, 2 * len(content) // 3 - 11)
    parts = [content[:cuts[0]], content[cuts[0]:cuts[1]], content[cuts[1]:]]
    files = [
        _file(11 + index, f"globalARcatalog_ERA5_1940-2025_v4.0.nc.part{index + 1}", part)
        for index, part in enumerate(parts)
    ]
    # These products must never be selected or requested.
    files.extend([
        _file(99, "globalARcatalog_MERRA2_1980-2025_v4.0.nc.part1", b"wrong", "MERRA-2"),
        _file(100, "README.txt", b"wrong", None),
    ])
    data = {11 + index: part for index, part in enumerate(parts)}
    data.update({99: b"wrong", 100: b"wrong"})
    return files, data


class AtmosphericRiverDownloadTests(unittest.TestCase):
    def test_discovery_requires_materialized_date_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            index = root / paths.TARGET_AR_INDEX_FILENAME
            index.write_text(json.dumps({"schema_version": 1, "source": {}, "available_date_ranges": []}))
            with patch.object(paths, "TARGET_AR_DATA_DIR", root):
                self.assertEqual(discover_target_ar_sources(), [])
            payload = json.loads(index.read_text())
            payload["available_date_ranges"] = [{"start": "2025-01-01", "end": "2025-01-01"}]
            index.write_text(json.dumps(payload))
            with patch.object(paths, "TARGET_AR_DATA_DIR", root):
                self.assertEqual(discover_target_ar_sources(), [index.resolve()])

    def test_metadata_only_index_makes_no_data_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            content = _catalog_bytes(root)
            files, data = _split_catalog(content)
            session = _Session(_metadata(files), data)
            with patch.object(paths, "TARGET_AR_DATA_DIR", root / "ar"):
                result = download_default_dataset(session=session, block_size=1024)
            self.assertEqual([url for url, _ in session.requests], [TARGET_AR_METADATA_URL])
            self.assertTrue(result[0].is_file())

    def test_virtual_catalog_crosses_parts_and_filters_dates_and_bounds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            content = _catalog_bytes(root)
            files, data = _split_catalog(content)
            session = _Session(_metadata(files), data)
            with patch.object(paths, "TARGET_AR_DATA_DIR", root / "ar"):
                [index] = download_default_dataset(
                    start_date="2025-01-01", end_date="2025-01-01",
                    session=session, block_size=1024,
                )
                frames = list(iter_normalized_frames([index], session=session))
            self.assertEqual([frame.timestamp for frame in frames], [
                "2025-01-01T00:00:00Z", "2025-01-01T06:00:00Z",
            ])
            self.assertEqual(frames[0].mask.shape, (2, 2))
            requested_ids = {
                int(url.removeprefix(_DATAFILE_URL_PREFIX))
                for url, _ in session.requests if url.startswith(_DATAFILE_URL_PREFIX)
            }
            self.assertTrue(requested_ids.issubset({11, 12, 13}))
            with CachedMultipartFile(index, session=session, block_size=1024) as virtual:
                virtual.seek(len(content) // 3 - 20)
                self.assertEqual(virtual.read(100), content[len(content) // 3 - 20:len(content) // 3 + 80])

    def test_same_range_is_idempotent_and_force_redownloads_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            content = _catalog_bytes(root)
            files, data = _split_catalog(content)
            session = _Session(_metadata(files), data)
            arguments = dict(
                start_date="2025-01-01", end_date="2025-01-01",
                session=session, block_size=1024,
            )
            with patch.object(paths, "TARGET_AR_DATA_DIR", root / "ar"):
                download_default_dataset(**arguments)
                first_ranges = [headers["Range"] for url, headers in session.requests if url.startswith(_DATAFILE_URL_PREFIX)]
                session.requests.clear()
                download_default_dataset(**arguments)
                second_ranges = [headers["Range"] for url, headers in session.requests if url.startswith(_DATAFILE_URL_PREFIX)]
                session.requests.clear()
                download_default_dataset(**arguments, force=True)
                forced_ranges = [headers["Range"] for url, headers in session.requests if url.startswith(_DATAFILE_URL_PREFIX)]
            self.assertTrue(first_ranges)
            self.assertEqual(second_ranges, [])
            self.assertEqual(forced_ranges, first_ranges)


if __name__ == "__main__":
    unittest.main()
