from __future__ import annotations

from statistics_plugins.base import StatisticDefinition, StatisticSpec, StorageDescriptor


def forecast_definition() -> StatisticDefinition:
    """Registry entry for the raw GFS precipitation forecast.

    This plugin is not used by compute_stats.py (no observation comparison).
    It exists so the tile generator and API discover the forecast statistic
    via the shared registry.
    """

    return StatisticDefinition(StatisticSpec(
        name="forecast",
        label="Forecast",
        units="mm",
        render_field="precip",
        colormap="sequential",
        scope="display", axis="none", periods=(),
        storage=StorageDescriptor(kind="display"),
        map_support=True, winner_support=False,
    ))
