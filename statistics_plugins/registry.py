from __future__ import annotations

from statistics_plugins.base import StatisticPlugin
from statistics_plugins.bias import BiasPlugin
from statistics_plugins.nmad import NMADPlugin
from statistics_plugins.nrmse import NRMSEPlugin
from statistics_plugins.sacc import SACCPlugin


ENABLED_STATISTICS: list[StatisticPlugin] = [
    BiasPlugin(),
    SACCPlugin(),
    NRMSEPlugin(),
    NMADPlugin(),
]

STATISTICS_BY_NAME = {plugin.spec.name: plugin for plugin in ENABLED_STATISTICS}
