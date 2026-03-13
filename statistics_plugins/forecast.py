from __future__ import annotations

from statistics_plugins.base import StatisticSpec


class ForecastPlugin:
    """Registry entry for the raw GFS precipitation forecast.

    This plugin is not used by compute_stats.py (no observation comparison).
    It exists so the tile generator and API discover the forecast statistic
    via the shared registry.
    """

    spec = StatisticSpec(
        name="forecast",
        units="mm",
        render_field="precip",
        colormap="sequential",
    )

    def init_accumulator(self, shape):
        raise NotImplementedError("Forecast plugin is display-only")

    def update(self, *args, **kwargs):
        raise NotImplementedError("Forecast plugin is display-only")

    def finalize(self, *args, **kwargs):
        raise NotImplementedError("Forecast plugin is display-only")
