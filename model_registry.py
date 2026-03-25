#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, field



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
    ),
}

DEFAULT_MODEL = next(
    (k for k, v in MODEL_REGISTRY.items() if v.default),
    next(iter(MODEL_REGISTRY)),  # fallback to first if none marked
)
