from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 3
MANIFEST_NAME = "manifest.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_manifest(root: Path, *, migrate: bool = True) -> dict[str, Any]:
    path = root / MANIFEST_NAME
    current = _read_json(path)
    if current and current.get("schema_version") == SCHEMA_VERSION:
        current.setdefault("stages", {})
        return current

    stages: dict[str, Any] = {}
    migrated_from: list[str] = []
    if current and current.get("schema_version") == 2 and "processed" in current:
        stages["verification"] = current
        migrated_from.append("verification-v2")
    legacy_consistency = _read_json(root / "consistency_manifest.json")
    if legacy_consistency and legacy_consistency.get("schema_version") == 1:
        stages["consistency"] = legacy_consistency
        migrated_from.append("consistency-v1")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "stages": stages,
        "migration": {"sources": migrated_from} if migrated_from else {},
    }
    if migrate and (current is not None or legacy_consistency is not None):
        atomic_write_json(path, manifest)
        (root / "consistency_manifest.json").unlink(missing_ok=True)
    return manifest


def stage_state(root: Path, stage: str) -> dict[str, Any] | None:
    value = load_manifest(root).get("stages", {}).get(stage)
    return value if isinstance(value, dict) else None


def update_stage(
    root: Path,
    stage: str,
    state: dict[str, Any],
    *,
    invalidate: tuple[str, ...] = (),
) -> None:
    manifest = load_manifest(root)
    stages = manifest.setdefault("stages", {})
    stages[stage] = state
    for downstream in invalidate:
        stages.pop(downstream, None)
    atomic_write_json(root / MANIFEST_NAME, manifest)
    (root / "consistency_manifest.json").unlink(missing_ok=True)


def fingerprint_payload(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def fingerprint_files(paths: list[Path]) -> str:
    entries = []
    for path in sorted(paths):
        try:
            stat = path.stat()
            entries.append((str(path.resolve()), stat.st_size, stat.st_mtime_ns))
        except FileNotFoundError:
            entries.append((str(path.resolve()), None, None))
    return fingerprint_payload(entries)


def digest_files_content(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(str(path.resolve()).encode("utf-8"))
        try:
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
        except FileNotFoundError:
            digest.update(b"<missing>")
    return digest.hexdigest()
