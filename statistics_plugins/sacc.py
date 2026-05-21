from __future__ import annotations

import numpy as np

from statistics_plugins.base import StatisticSpec

EPSILON = 1e-10


class SACCPlugin:
    """Per-pixel temporal Pearson correlation between model and obs time series.

    Naming caveat: this is mathematically TCORR (temporal correlation), not a
    strict SACC.  True SACC is a *spatial* correlation that produces one scalar
    per forecast time, which can't be rendered as a per-pixel map; the metric
    here is the closest per-pixel relative we can show.  Pearson correlation is
    shift-invariant, so subtracting a per-pixel time-mean (a degenerate
    "climatology") would give an identical answer — promoting this to true ACC
    would require a *time-varying* climatology (e.g., per-pixel monthly mean
    subtracted from each day's values), which is a bigger refactor.

    Renders with ``diverging_reversed`` (red→white→blue) and a fixed 0–100 %
    range so we share the colormap family with bias and so the convention
    "blue = good skill" matches NMAD / NRMSE's "blue = low error".  A typical
    CONUS day-1 cluster (SACC 60–90 %) lands in the upper blue half with
    visible gradient; the white midpoint at 50 % is well below the cluster,
    so the map doesn't wash out the way it did with percentile-driven
    auto-ranging.
    """

    spec = StatisticSpec(
        name="sacc",
        label="SACC",
        units="%",
        render_field="value",
        colormap="diverging_reversed",
        fixed_range=(0.0, 100.0),
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
            "sum_model": sum_model,
            "sum_obs": sum_obs,
            "sum_model_sq": sum_model_sq,
            "sum_obs_sq": sum_obs_sq,
            "sum_cross": sum_cross,
        }
