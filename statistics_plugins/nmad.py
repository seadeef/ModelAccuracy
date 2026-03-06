from __future__ import annotations

import numpy as np

from statistics_plugins.base import StatisticSpec

EPSILON = 1e-10


class NMADPlugin:
    spec = StatisticSpec(
        name="nmad",
        units="%",
        render_field="value",
        colormap="diverging",
    )

    def init_accumulator(self, shape: tuple[int, int]) -> dict[str, np.ndarray]:
        return {
            "sum_abs_error": np.zeros(shape, dtype=np.float32),
            "sum_obs": np.zeros(shape, dtype=np.float32),
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
        abs_diff = (
            derived["abs_diff"]
            if derived is not None
            else np.abs(model_data - obs_data)
        )
        accumulator["sum_abs_error"][valid_mask] += abs_diff[valid_mask].astype(np.float32)
        accumulator["sum_obs"][valid_mask] += obs_data[valid_mask]
        accumulator["sample_count"] += valid_mask.astype(np.int32)

    def finalize(self, accumulator: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        count = accumulator["sample_count"]
        value = np.full(count.shape, np.nan, dtype=np.float32)
        valid = count > 0
        if np.any(valid):
            count_f = count[valid].astype(np.float32)
            mad = accumulator["sum_abs_error"][valid] / count_f
            average = accumulator["sum_obs"][valid] / count_f
            value[valid] = (mad / (average + EPSILON)) * 100.0

        return {
            "value": value,
            "sample_count": count,
            "sum_abs_error": accumulator["sum_abs_error"],
            "sum_obs": accumulator["sum_obs"],
        }
