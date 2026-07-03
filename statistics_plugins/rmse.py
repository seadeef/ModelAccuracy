from __future__ import annotations

import numpy as np

from statistics_plugins.base import StatisticSpec


class RMSEPlugin:
    """Root mean squared error in mm: √(mean squared error).

    An absolute error magnitude (lower is better), not a ratio, so no arid mask
    is needed. Rendered ``diverging`` (blue low → red high), matching NMAD/NRMSE's
    "blue = low error"; the display range auto-scales since mm error varies a lot
    by region and season.
    """

    spec = StatisticSpec(
        name="rmse",
        label="RMSE",
        units="mm",
        render_field="value",
        colormap="diverging",
    )

    def init_accumulator(self, shape: tuple[int, int]) -> dict[str, np.ndarray]:
        return {
            "sum_squared_error": np.zeros(shape, dtype=np.float64),
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
        accumulator["sample_count"] += valid_mask.astype(np.int32)

    def finalize(self, accumulator: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        count = accumulator["sample_count"]
        value = np.full(count.shape, np.nan, dtype=np.float32)
        valid = count > 0
        if np.any(valid):
            count_f = count[valid].astype(np.float64)
            value[valid] = np.sqrt(
                accumulator["sum_squared_error"][valid] / count_f
            ).astype(np.float32)

        return {
            "value": value,
            "sample_count": count,
            "sum_squared_error": accumulator["sum_squared_error"],
        }
