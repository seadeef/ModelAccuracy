from __future__ import annotations

from pathlib import Path

import numpy as np

METADATA_FILENAME = "metadata.npz"


def model_metadata_path(stats_root: Path) -> Path:
    return stats_root / METADATA_FILENAME


def resolve_model_metadata_path(stats_root: Path) -> Path:
    direct = model_metadata_path(stats_root)
    if direct.exists():
        return direct

    raise FileNotFoundError(f"Missing model metadata: {direct.resolve()}")


def load_model_metadata(stats_root: Path) -> dict[str, np.ndarray]:
    meta_path = resolve_model_metadata_path(stats_root)
    with np.load(meta_path, allow_pickle=False) as meta:
        return {
            "lats": meta["lats"],
            "lons": meta["lons"],
            "transform": meta["transform"],
            "crs": meta["crs"],
        }


def save_model_metadata(
    stats_root: Path,
    *,
    lats: np.ndarray,
    lons: np.ndarray,
    transform: np.ndarray,
    crs: str,
) -> Path:
    stats_root.mkdir(parents=True, exist_ok=True)
    out_path = model_metadata_path(stats_root)
    np.savez_compressed(
        out_path,
        lats=lats,
        lons=lons,
        transform=transform,
        crs=crs,
    )
    return out_path
