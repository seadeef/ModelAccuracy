"""Canonical paths for the atmospheric-river data pipeline.

All AR producers and consumers import these definitions so their defaults are
independent of the caller's current working directory.
"""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
TARGET_AR_DATA_DIR = PROJECT_ROOT / "target_ar_data"
TARGET_AR_INDEX_FILENAME = "globalARcatalog_ERA5_v4.index.json"
# Locally generated tARget v4 extension (atmospheric_rivers.tarv4): AR masks
# past the canonical release's end, always flagged non-canonical downstream.
TARGET_AR_EXTENSION_FILENAME = "localARextension_ERA5_v4.nc"
TARGET_AR_EXTENSION_META_FILENAME = "localARextension_ERA5_v4.meta.json"
STATIC_EXPORT_DIR = PROJECT_ROOT / "static_export"
AR_EXPORT_DIR = STATIC_EXPORT_DIR / "ar"


def target_ar_extension_meta() -> dict | None:
    """Provenance sidecar of the local extension, or None when absent."""
    import json

    extension = TARGET_AR_DATA_DIR / TARGET_AR_EXTENSION_FILENAME
    meta_path = TARGET_AR_DATA_DIR / TARGET_AR_EXTENSION_META_FILENAME
    if not (extension.is_file() and meta_path.is_file()):
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if meta.get("kind") != "local_tarv4_extension" or not meta.get("start"):
        return None
    return meta


def discover_target_ar_sources() -> list[Path]:
    """Canonical virtual catalog (when ranges are cached) plus, when present,
    the locally generated non-canonical extension file."""
    sources: list[Path] = []
    index = TARGET_AR_DATA_DIR / TARGET_AR_INDEX_FILENAME
    if index.is_file():
        try:
            from atmospheric_rivers.remote import read_index

            if read_index(index).get("available_date_ranges"):
                sources.append(index.resolve())
        except (OSError, ValueError):
            pass
    if target_ar_extension_meta() is not None:
        sources.append((TARGET_AR_DATA_DIR / TARGET_AR_EXTENSION_FILENAME)
                       .resolve())
    return sources
