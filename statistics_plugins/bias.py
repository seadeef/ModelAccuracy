from __future__ import annotations

import numpy as np

from statistics_plugins.base import StatisticSpec


class BiasPlugin:
    spec = StatisticSpec(name="bias", units="mm", render_field="value", colormap="diverging")

    def init_accumulator(self, shape: tuple[int, int]) -> dict[str, np.ndarray]:
        return {
            "sum_error": np.zeros(shape, dtype=np.float32),
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
        diff = derived["diff"] if derived is not None else (model_data - obs_data)
        accumulator["sum_error"][valid_mask] += diff[valid_mask]
        accumulator["sample_count"] += valid_mask.astype(np.int32)

    def finalize(self, accumulator: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        sample_count = accumulator["sample_count"]
        value = np.full(sample_count.shape, np.nan, dtype=np.float32)
        valid = sample_count > 0
        value[valid] = accumulator["sum_error"][valid] / sample_count[valid]
        return {
            "value": value,
            "sample_count": sample_count,
            "sum_error": accumulator["sum_error"],
        }
