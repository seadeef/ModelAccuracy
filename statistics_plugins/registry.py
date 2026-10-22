from __future__ import annotations

from statistics_plugins.base import StatisticDefinition, StatisticPlugin
from statistics_plugins.cc import CCPlugin
from statistics_plugins.consistency import (
    consistency_definitions,
    episode_definitions,
)
from statistics_plugins.forecast import forecast_definition
from statistics_plugins.nmad import NMADPlugin
from statistics_plugins.nrmse import NRMSEPlugin
from statistics_plugins.rb import RBPlugin

EPISODE_CATALOG_FILENAME = "episodes.json"

# Statistic names servable through the AR-episode/scope context (values read
# from episodes.json). Membership here — not spec.scope — is what the episode
# context validates against: revision_efficiency also has a grid-pooled
# regional surface (scope="region_pooled"), while lock_in_gap is episode-only.
EPISODE_CONTEXT_STATS: tuple[str, ...] = ("revision_efficiency", "lock_in_gap")


# Verification statistics used by compute_stats.py (require GFS+PRISM pairs).
# RB leads: it replaced the absolute Bias stat (relative bias is comparable
# across wet and dry regions) and keeps its slot in the UI ordering.
VERIFICATION_STATISTICS: list[StatisticPlugin] = [
    RBPlugin(),
    CCPlugin(),
    NRMSEPlugin(),
    NMADPlugin(),
]

# Consistency statistics computed by compute_stats.py --only consistency (run-to-run
# pairing over scorable valid dates, not the per-(init, lead) pairing of
# compute_stats.py). Panel-only: no map tiles, no legend ranges.
CONSISTENCY_STATISTICS: list[StatisticDefinition] = list(consistency_definitions())

# Predefined-basin AR-episode statistics use intact daily histories and a
# episode context; they cannot be queried as climatological arbitrary regions.
EPISODE_STATISTICS: list[StatisticDefinition] = list(episode_definitions())

# Display-only statistics (not computed by compute_stats.py).
DISPLAY_STATISTICS: list[StatisticDefinition] = [forecast_definition()]

# All statistics (used by tile generator, API, frontend).
# Forecast first so it's the default in the UI.
ENABLED_STATISTICS: list[StatisticPlugin] = (
    DISPLAY_STATISTICS + VERIFICATION_STATISTICS + CONSISTENCY_STATISTICS
)

ALL_STATISTICS = ENABLED_STATISTICS + EPISODE_STATISTICS

STATISTICS_BY_NAME = {entry.spec.name: entry for entry in ALL_STATISTICS}

DEFAULT_STATISTIC = next(
    (p.spec.name for p in ENABLED_STATISTICS if p.spec.default),
    ENABLED_STATISTICS[0].spec.name,  # fallback to first if none marked
)
