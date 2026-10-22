"""Validated access to the canonical tARget atmospheric-river dataset."""

from atmospheric_rivers.dataset import NormalizedFrame, iter_normalized_frames, validate_source
from atmospheric_rivers.download import (
    TARGET_AR_DATASET_URL,
    download_default_dataset,
)
from atmospheric_rivers.paths import (
    TARGET_AR_DATA_DIR,
    discover_target_ar_sources,
    target_ar_extension_meta,
)

__all__ = [
    "NormalizedFrame",
    "TARGET_AR_DATA_DIR",
    "TARGET_AR_DATASET_URL",
    "download_default_dataset",
    "discover_target_ar_sources",
    "iter_normalized_frames",
    "target_ar_extension_meta",
    "validate_source",
]
