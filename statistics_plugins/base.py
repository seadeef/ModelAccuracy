from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

import numpy as np


# Ratio statistics (NMAD, NRMSE, RB, NMSE) divide by the period-mean observed
# precipitation. Where that mean is near zero — arid pixels in dry months — the
# ratio explodes (verified: June lead-7 NRMSE hit 7.4e12 %). Cells whose period
# mean observed precip is below this floor are reported as NaN (no-data) rather
# than a meaningless huge percentage. 0.1 mm/day is ~1/10 of a trace day; raise
# it to mask more of the desert Southwest, lower it to keep noisier cells.
MIN_MEAN_OBS_MM = 0.1


@dataclass(frozen=True)
class StorageDescriptor:
    """How a statistic is persisted, exported, and reduced by the API."""

    kind: Literal["grid", "packed_grid", "episode_catalog", "display"] = "grid"
    source_fields: tuple[str, ...] = ("value",)
    export_dtype: str = "float32"
    path_template: str = "{stat}/{period_path}lead_{lead}.npz"
    aggregation: Literal["mean", "ratio", "catalog"] = "mean"
    finalize: Literal["identity", "ratio", "one_minus_ratio", "sqrt_ratio"] = "identity"


@dataclass(frozen=True)
class StatisticSpec:
    name: str
    label: str
    units: str
    render_field: str
    colormap: str = "diverging"
    fixed_range: tuple[float, float] | None = None
    default: bool = False
    # "region": per-lead grids averaged/pooled for any region+period.
    # "region_pooled": a single pooled scalar per region+period derived from
    #   packed accumulator planes (no per-lead entries, no stat card).
    # "ar_episode": precomputed per-basin episode/scope catalog values.
    scope: Literal["region", "region_pooled", "ar_episode", "display"] = "region"
    axis: Literal["lead", "lead_pair", "episode", "none"] = "lead"
    parameters: dict[str, Any] = field(default_factory=dict)
    periods: tuple[str, ...] = ("yearly", "monthly", "seasonal")
    storage: StorageDescriptor = field(default_factory=StorageDescriptor)
    map_support: bool = True
    winner_support: bool = True
    winner_mode: Literal["lower", "higher", "absolute_lower"] = "lower"


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


@dataclass(frozen=True)
class StatisticDefinition:
    """Registry entry for statistics produced outside verification accumulators."""

    spec: StatisticSpec
