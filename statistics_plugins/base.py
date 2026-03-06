from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import numpy as np


@dataclass(frozen=True)
class StatisticSpec:
    name: str
    units: str
    render_field: str
    colormap: str = "diverging"
    fixed_range: tuple[float, float] | None = None
    tile_mode: str = "image"  # "image" (single PNG) or "pmtiles"


class StatisticPlugin(Protocol):
    spec: StatisticSpec

    def init_accumulator(self, shape: tuple[int, int]) -> dict[str, np.ndarray]:
        ...

    def update(
        self,
        accumulator: dict[str, np.ndarray],
        model_data: np.ndarray,
        obs_data: np.ndarray,
        valid_mask: np.ndarray,
        derived: dict[str, np.ndarray] | None = None,
    ) -> None:
        ...

    def finalize(self, accumulator: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        ...
