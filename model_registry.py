#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, field


# Shared CONUS crop bounds (west, south, east, north).
# Used by downloaders to crop model output to CONUS before saving .npy files.
# compute_tiles uses a narrower US_BOUNDS for visible-CONUS rendering.
US_CROP_BOUNDS = (-130.0, 20.0, -60.0, 55.0)


def window_to_key(start: int, end: int) -> str:
    return f"{start}_{end}"


@dataclass(frozen=True)
class ModelConfig:
    key: str
    label: str
    downloader_class_path: str  # e.g. "downloaders.gfs_downloader.GFSFilteredDownloaderParallel"
    downloader_defaults: dict = field(default_factory=dict)
    lead_days_min: int = 1
    lead_days_max: int = 14
    lead_windows: tuple[tuple[int, int], ...] = ((1, 7), (7, 14), (1, 10))
    cycle_hour: int = 12
    data_dir: str = "model_data/gfs"
    # Hours after the cycle hour at which the full forecast is available to
    # download. Used by --forecast mode to pick today vs. yesterday.
    publish_delay_hours: int = 14
    default: bool = False

    @property
    def forecast_hours(self) -> list[int]:
        return [d * 24 for d in range(self.lead_days_min, self.lead_days_max + 1)]

    def get_downloader_class(self) -> type:
        module_path, class_name = self.downloader_class_path.rsplit(".", 1)
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)


MODEL_REGISTRY: dict[str, ModelConfig] = {
    "gfs": ModelConfig(
        key="gfs",
        label="GFS",
        downloader_class_path="downloaders.gfs_downloader.GFSFilteredDownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/gfs",
            "max_workers": 16,
            "max_retries": 3,
            "timeout_seconds": 120,
        },
        lead_days_min=1,
        lead_days_max=14,
        lead_windows=((1, 7), (7, 14), (1, 10)),
        cycle_hour=12,
        data_dir="model_data/gfs",
        publish_delay_hours=14,
        default=True,
    ),
    "nbm": ModelConfig(
        key="nbm",
        label="NBM",
        downloader_class_path="downloaders.nbm_downloader.NBMDownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/nbm",
            "max_workers": 16,
            "max_retries": 3,
            "timeout_seconds": 120,
        },
        lead_days_min=1,
        lead_days_max=11,
        lead_windows=((1, 7), (7, 11), (1, 10)),
        cycle_hour=12,
        data_dir="model_data/nbm",
        publish_delay_hours=5,
    ),
    "graphcast": ModelConfig(
        key="graphcast",
        label="GraphCast",
        downloader_class_path="downloaders.graphcast_downloader.GraphCastDownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/graphcast",
            "max_workers": 8,
            "max_retries": 3,
            "timeout_seconds": 300,
        },
        lead_days_min=1,
        lead_days_max=10,
        lead_windows=((1, 7), (7, 10), (1, 10)),
        cycle_hour=12,
        data_dir="model_data/graphcast",
        publish_delay_hours=16,
    ),
    "aifs": ModelConfig(
        key="aifs",
        label="AIFS",
        downloader_class_path="downloaders.aifs_downloader.AIFSDownloaderParallel",
        downloader_defaults={
            "output_dir": "model_data/aifs",
            "max_workers": 16,
            "max_retries": 3,
            "timeout_seconds": 120,
        },
        lead_days_min=1,
        lead_days_max=14,
        lead_windows=((1, 7), (7, 14), (1, 10)),
        cycle_hour=12,
        data_dir="model_data/aifs",
        publish_delay_hours=6,
    ),
}

DEFAULT_MODEL = next(
    (k for k, v in MODEL_REGISTRY.items() if v.default),
    next(iter(MODEL_REGISTRY)),  # fallback to first if none marked
)
