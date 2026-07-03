from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import numpy as np


# Ratio statistics (NMAD, NRMSE, RB, NMSE) divide by the period-mean observed
# precipitation. Where that mean is near zero — arid pixels in dry months — the
# ratio explodes (verified: June lead-7 NRMSE hit 7.4e12 %). Cells whose period
# mean observed precip is below this floor are reported as NaN (no-data) rather
# than a meaningless huge percentage. 0.1 mm/day is ~1/10 of a trace day; raise
# it to mask more of the desert Southwest, lower it to keep noisier cells.
MIN_MEAN_OBS_MM = 0.1


@dataclass(frozen=True)
class StatisticSpec:
    name: str
    label: str
    units: str
    render_field: str
    colormap: str = "diverging"
    fixed_range: tuple[float, float] | None = None
    default: bool = False


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
