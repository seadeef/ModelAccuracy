from __future__ import annotations

import numpy as np

from statistics_plugins.base import StatisticSpec, MIN_MEAN_OBS_MM


class RBPlugin:
    """Relative bias: Σ(model − obs) / Σobs, as a percent.

    Signed like BIAS (near zero is best) but normalized by total observed precip,
    so it's comparable across wet and dry regions. Rendered ``diverging_reversed``
    so over-forecast (positive) and under-forecast (negative) read as opposite
    hues. Arid cells (period-mean obs below the floor) are NaN-masked, matching
    NMAD/NRMSE, since the ratio there is division noise.
    """

    spec = StatisticSpec(
        name="rb",
        label="RB",
        units="%",
        render_field="value",
        colormap="diverging_reversed",
        winner_mode="absolute_lower",
    )

    def init_accumulator(self, shape: tuple[int, int]) -> dict[str, np.ndarray]:
        return {
            "sum_error": np.zeros(shape, dtype=np.float64),
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
        diff = derived["diff"] if derived is not None else (model_data - obs_data)
        accumulator["sum_error"][valid_mask] += diff[valid_mask].astype(np.float64)
        accumulator["sum_obs"][valid_mask] += obs_data[valid_mask].astype(np.float64)
        accumulator["sample_count"] += valid_mask.astype(np.int32)

    def finalize(self, accumulator: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        count = accumulator["sample_count"]
        value = np.full(count.shape, np.nan, dtype=np.float32)
        valid = count > 0
        if np.any(valid):
            count_f = count[valid].astype(np.float64)
            sum_obs = accumulator["sum_obs"][valid]
            sum_err = accumulator["sum_error"][valid]
            mean_obs = sum_obs / count_f
            ratio = np.full(mean_obs.shape, np.nan, dtype=np.float32)
            wet = mean_obs >= MIN_MEAN_OBS_MM
            ratio[wet] = (sum_err[wet] / sum_obs[wet] * 100.0).astype(np.float32)
            value[valid] = ratio

        return {
            "value": value,
            "sample_count": count,
            "sum_error": accumulator["sum_error"],
            "sum_obs": accumulator["sum_obs"],
        }
