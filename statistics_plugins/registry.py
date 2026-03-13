from __future__ import annotations

from statistics_plugins.base import StatisticPlugin
from statistics_plugins.bias import BiasPlugin
from statistics_plugins.forecast import ForecastPlugin
from statistics_plugins.nmad import NMADPlugin
from statistics_plugins.nrmse import NRMSEPlugin
from statistics_plugins.sacc import SACCPlugin


# Verification statistics used by compute_stats.py (require GFS+PRISM pairs).
VERIFICATION_STATISTICS: list[StatisticPlugin] = [
    BiasPlugin(),
    SACCPlugin(),
    NRMSEPlugin(),
    NMADPlugin(),
]

# Display-only statistics (not computed by compute_stats.py).
DISPLAY_STATISTICS: list[StatisticPlugin] = [
    ForecastPlugin(),
]

# All statistics (used by tile generator, API, frontend).
# Forecast first so it's the default in the UI.
ENABLED_STATISTICS: list[StatisticPlugin] = DISPLAY_STATISTICS + VERIFICATION_STATISTICS

STATISTICS_BY_NAME = {plugin.spec.name: plugin for plugin in ENABLED_STATISTICS}
