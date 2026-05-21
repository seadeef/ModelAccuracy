from __future__ import annotations

import numpy as np

from statistics_plugins.base import StatisticSpec

EPSILON = 1e-10


class NRMSEPlugin:
    # Diverging colormap with a fixed 0–800 % range.  NRMSE for daily precip
    # is routinely 100–500 %  (RMSE > mean is the norm given the variance of
    # rainfall), so the natural midpoint is well above 100 %; 400 % puts the
    # white center near the cross-lead median and lets the gradient span the
    # bulk of the data.  The fixed cap keeps localized arid-pixel outliers
    # (mean_obs ≈ 0 → values in the thousands) from pulling the percentile
    # range outward — they clamp to saturated red instead of poisoning the
    # rest of the map.
    spec = StatisticSpec(
        name="nrmse",
        label="NRMSE",
        units="%",
        render_field="value",
        colormap="diverging",
        fixed_range=(0.0, 800.0),
    )

    def init_accumulator(self, shape: tuple[int, int]) -> dict[str, np.ndarray]:
        return {
            "sum_squared_error": np.zeros(shape, dtype=np.float32),
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
        sq_diff = (
            derived["sq_diff"]
            if derived is not None
            else ((model_data - obs_data) ** 2)
        )
        accumulator["sum_squared_error"][valid_mask] += sq_diff[valid_mask].astype(np.float32)
        accumulator["sum_obs"][valid_mask] += obs_data[valid_mask]
        accumulator["sample_count"] += valid_mask.astype(np.int32)

    def finalize(self, accumulator: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        count = accumulator["sample_count"]
        value = np.full(count.shape, np.nan, dtype=np.float32)
        valid = count > 0
        if np.any(valid):
            count_f = count[valid].astype(np.float32)
            rmse = np.sqrt(accumulator["sum_squared_error"][valid] / count_f)
            average = accumulator["sum_obs"][valid] / count_f
            value[valid] = (rmse / (average + EPSILON)) * 100.0

        return {
            "value": value,
            "sample_count": count,
            "sum_squared_error": accumulator["sum_squared_error"],
            "sum_obs": accumulator["sum_obs"],
        }
