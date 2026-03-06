from __future__ import annotations

import numpy as np

from statistics_plugins.base import StatisticSpec

EPSILON = 1e-10


class SACCPlugin:
    spec = StatisticSpec(
        name="sacc",
        units="%",
        render_field="value",
        colormap="diverging",
    )

    def init_accumulator(self, shape: tuple[int, int]) -> dict[str, np.ndarray]:
        return {
            "sum_model": np.zeros(shape, dtype=np.float64),
            "sum_obs": np.zeros(shape, dtype=np.float64),
            "sum_model_sq": np.zeros(shape, dtype=np.float64),
            "sum_obs_sq": np.zeros(shape, dtype=np.float64),
            "sum_cross": np.zeros(shape, dtype=np.float64),
            "sample_count": np.zeros(shape, dtype=np.int32),
        }

    def update(
        self,
        accumulator: dict[str, np.ndarray],
        model_data: np.ndarray,
        obs_data: np.ndarray,
        valid_mask: np.ndarray,
        derived: dict[str, np.ndarray] | None = None,
    ) -> None:
        m = model_data[valid_mask].astype(np.float64)
        o = obs_data[valid_mask].astype(np.float64)
        accumulator["sum_model"][valid_mask] += m
        accumulator["sum_obs"][valid_mask] += o
        accumulator["sum_model_sq"][valid_mask] += m * m
        accumulator["sum_obs_sq"][valid_mask] += o * o
        accumulator["sum_cross"][valid_mask] += m * o
        accumulator["sample_count"] += valid_mask.astype(np.int32)

    def finalize(self, accumulator: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        n = accumulator["sample_count"].astype(np.float64)
        sum_model = accumulator["sum_model"]
        sum_obs = accumulator["sum_obs"]
        sum_model_sq = accumulator["sum_model_sq"]
        sum_obs_sq = accumulator["sum_obs_sq"]
        sum_cross = accumulator["sum_cross"]

        numerator = n * sum_cross - sum_model * sum_obs
        denom_left = n * sum_model_sq - sum_model * sum_model
        denom_right = n * sum_obs_sq - sum_obs * sum_obs
        denominator = np.sqrt(np.maximum(denom_left, 0.0) * np.maximum(denom_right, 0.0))

        value = np.full(n.shape, np.nan, dtype=np.float32)
        valid = (n > 1.0) & (denominator > EPSILON)
        if np.any(valid):
            corr = numerator[valid] / denominator[valid]
            value[valid] = (corr * 100.0).astype(np.float32)
        value = np.clip(value, -100.0, 100.0)

        return {
            "value": value,
            "sample_count": accumulator["sample_count"],
        }
