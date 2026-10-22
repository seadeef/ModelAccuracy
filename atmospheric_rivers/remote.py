"""Seekable, cached access to the canonical multipart tARget netCDF file."""

from __future__ import annotations

import io
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
DEFAULT_BLOCK_SIZE = 8 * 1024 * 1024


@dataclass(frozen=True)
class RemotePart:
    id: int
    label: str
    size: int
    md5: str
    offset: int


def read_index(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"invalid tARget source index {path}: {exc}") from exc
    if payload.get("schema_version") != 1 or not isinstance(payload.get("source"), dict):
        raise ValueError(f"unsupported tARget source index {path}")
    return payload


def indexed_parts(index: dict[str, Any]) -> list[RemotePart]:
    offset = 0
    parts = []
    for raw in index["source"].get("parts", []):
        part = RemotePart(
            id=int(raw["id"]), label=str(raw["label"]), size=int(raw["size"]),
            md5=str(raw["md5"]), offset=offset,
        )
        if part.size <= 0:
            raise ValueError(f"invalid size for tARget part {part.label}")
        parts.append(part)
        offset += part.size
    if not parts:
        raise ValueError("tARget source index contains no netCDF parts")
    return parts


class CachedMultipartFile(io.RawIOBase):
    """Read a multipart remote object as one file using persistent range blocks.

    Blocks are aligned within each published Dataverse part, written atomically,
    and keyed by immutable dataset-version and part identifiers.  The class is a
    normal seekable binary file from h5py's perspective.
    """

    def __init__(
        self,
        index_path: Path,
        *,
        session: requests.Session | None = None,
        timeout: int = 120,
        retries: int = 3,
        block_size: int | None = None,
        refresh: bool = False,
    ) -> None:
        super().__init__()
        self.index_path = Path(index_path)
        self.index = read_index(self.index_path)
        self.parts = indexed_parts(self.index)
        self.size = sum(part.size for part in self.parts)
        self.position = 0
        self.timeout = timeout
        self.retries = retries
        self.block_size = int(block_size or self.index.get("block_size") or DEFAULT_BLOCK_SIZE)
        if self.block_size <= 0:
            raise ValueError("tARget range-cache block size must be positive")
        self._owns_session = session is None
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "ModelAccuracy tARget range reader/1"})
        self.url_prefix = str(self.index["source"]["datafile_url_prefix"])
        version_id = int(self.index["source"]["dataset_version_id"])
        self.cache_root = self.index_path.parent / "cache" / f"version-{version_id}"
        self._memory_blocks: dict[Path, bytes] = {}
        self.refresh = refresh
        self._refreshed_blocks: set[Path] = set()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            target = offset
        elif whence == os.SEEK_CUR:
            target = self.position + offset
        elif whence == os.SEEK_END:
            target = self.size + offset
        else:
            raise ValueError(f"invalid whence {whence}")
        if target < 0:
            raise OSError("negative seek position")
        self.position = min(target, self.size)
        return self.position

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def read(self, size: int = -1) -> bytes:
        self._checkClosed()
        if size is None or size < 0:
            size = self.size - self.position
        if size == 0 or self.position >= self.size:
            return b""
        remaining = min(size, self.size - self.position)
        chunks = []
        while remaining:
            part = self._part_at(self.position)
            within_part = self.position - part.offset
            block_start = (within_part // self.block_size) * self.block_size
            block = self._load_block(part, block_start)
            within_block = within_part - block_start
            take = min(remaining, len(block) - within_block)
            if take <= 0:
                raise IOError(f"empty cached block for {part.label} at {block_start}")
            chunks.append(block[within_block:within_block + take])
            self.position += take
            remaining -= take
        return b"".join(chunks)

    def close(self) -> None:
        if not self.closed and self._owns_session:
            self.session.close()
        super().close()

    def _part_at(self, offset: int) -> RemotePart:
        # Eight parts currently exist, so a linear scan is simpler and plenty fast.
        for part in self.parts:
            if part.offset <= offset < part.offset + part.size:
                return part
        raise IOError(f"tARget virtual offset {offset} is outside the catalog")

    def _cache_path(self, part: RemotePart, start: int, end: int) -> Path:
        return self.cache_root / f"part-{part.id}" / f"{start:012d}-{end:012d}.bin"

    def _load_block(self, part: RemotePart, start: int) -> bytes:
        end = min(start + self.block_size, part.size) - 1
        path = self._cache_path(part, start, end)
        expected = end - start + 1
        if self.refresh and path not in self._refreshed_blocks:
            path.unlink(missing_ok=True)
            path.with_suffix(path.suffix + ".sha256").unlink(missing_ok=True)
            self._memory_blocks.pop(path, None)
            self._refreshed_blocks.add(path)
        if path in self._memory_blocks:
            return self._memory_blocks[path]
        try:
            if path.stat().st_size == expected:
                data = path.read_bytes()
                digest_path = path.with_suffix(path.suffix + ".sha256")
                expected_digest = digest_path.read_text(encoding="ascii").strip()
                if hashlib.sha256(data).hexdigest() == expected_digest:
                    self._remember(path, data)
                    return data
        except FileNotFoundError:
            pass
        path.unlink(missing_ok=True)
        path.with_suffix(path.suffix + ".sha256").unlink(missing_ok=True)
        data = self._fetch_range(part, start, end)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".part")
        try:
            temporary.write_bytes(data)
            with temporary.open("rb") as output:
                os.fsync(output.fileno())
            temporary.replace(path)
            digest_path = path.with_suffix(path.suffix + ".sha256")
            digest_temporary = digest_path.with_name(digest_path.name + ".part")
            digest_temporary.write_text(hashlib.sha256(data).hexdigest() + "\n", encoding="ascii")
            digest_temporary.replace(digest_path)
        finally:
            temporary.unlink(missing_ok=True)
            if "digest_temporary" in locals():
                digest_temporary.unlink(missing_ok=True)
        self._remember(path, data)
        return data

    def _remember(self, path: Path, data: bytes) -> None:
        self._memory_blocks[path] = data
        while len(self._memory_blocks) > 16:
            self._memory_blocks.pop(next(iter(self._memory_blocks)))

    def _fetch_range(self, part: RemotePart, start: int, end: int) -> bytes:
        url = f"{self.url_prefix}{part.id}"
        expected = end - start + 1
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                with self.session.get(
                    url, headers={"Range": f"bytes={start}-{end}"},
                    timeout=self.timeout,
                ) as response:
                    response.raise_for_status()
                    status = getattr(response, "status_code", None)
                    content_range = response.headers.get("Content-Range", "")
                    match = _CONTENT_RANGE_RE.fullmatch(content_range)
                    if status != 206 or not match:
                        raise IOError(
                            f"server ignored byte range {start}-{end} for {part.label}"
                        )
                    actual_start, actual_end, total = map(int, match.groups())
                    if (actual_start, actual_end, total) != (start, end, part.size):
                        raise IOError(
                            f"invalid Content-Range {content_range!r} for {part.label}"
                        )
                    data = response.content
                    if len(data) != expected:
                        raise IOError(
                            f"short byte range for {part.label}: got {len(data)}, expected {expected}"
                        )
                    return data
            except (OSError, requests.RequestException) as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(min(2 ** attempt, 10))
        raise IOError(
            f"failed to fetch bytes {start}-{end} of {part.label} after "
            f"{self.retries} attempts: {last_error}"
        ) from last_error
