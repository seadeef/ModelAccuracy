from __future__ import annotations

import numpy as np

from statistics_plugins.base import StatisticSpec, MIN_MEAN_OBS_MM


class NMSEPlugin:
    """Normalized mean squared error: MSE / mean(obs)².

    Dimensionless (mm² / mm²), so reported unitless rather than as a percent — it
    is the square of NRMSE-as-a-fraction. Lower is better. Shares the NRMSE
    accumulator shape and the same arid mask (period-mean obs below the floor →
    NaN), since it divides by mean(obs)².
    """

    spec = StatisticSpec(
        name="nmse",
        label="NMSE",
        units="",
        render_field="value",
        colormap="diverging",
    )

    def init_accumulator(self, shape: tuple[int, int]) -> dict[str, np.ndarray]:
        return {
            "sum_squared_error": np.zeros(shape, dtype=np.float64),
            "sum_obs": np.zeros(shape, dtype=np.float64),
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
        sq_diff = (
            derived["sq_diff"]
            if derived is not None
            else ((model_data - obs_data) ** 2)
        )
        accumulator["sum_squared_error"][valid_mask] += sq_diff[valid_mask].astype(np.float64)
        accumulator["sum_obs"][valid_mask] += obs_data[valid_mask].astype(np.float64)
        accumulator["sample_count"] += valid_mask.astype(np.int32)

    def finalize(self, accumulator: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        count = accumulator["sample_count"]
        value = np.full(count.shape, np.nan, dtype=np.float32)
        valid = count > 0
        if np.any(valid):
            count_f = count[valid].astype(np.float64)
            mse = accumulator["sum_squared_error"][valid] / count_f
            mean_obs = accumulator["sum_obs"][valid] / count_f
            ratio = np.full(mean_obs.shape, np.nan, dtype=np.float32)
            wet = mean_obs >= MIN_MEAN_OBS_MM
            ratio[wet] = (mse[wet] / (mean_obs[wet] ** 2)).astype(np.float32)
            value[valid] = ratio

        return {
            "value": value,
            "sample_count": count,
            "sum_squared_error": accumulator["sum_squared_error"],
            "sum_obs": accumulator["sum_obs"],
        }
