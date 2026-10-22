from __future__ import annotations

from statistics_plugins.base import StatisticDefinition, StatisticSpec, StorageDescriptor


# Parameters of the jumpiness statistic (see compute_stats.py --only consistency).
# Thresholds are daily precipitation exceedances in mm/day; scales are the
# widths of the square FSS neighborhood in kilometers (true km, converted to a
# latitude-dependent cell window at compute time — NOT a fixed cell count).
# Changing either tuple invalidates stats_output/{model}/consistency
# (the consistency manifest records them and forces a full recompute).
JUMPINESS_THRESHOLDS_MM: tuple[float, ...] = (1.0, 5.0, 10.0, 25.0)
JUMPINESS_SCALES_KM: tuple[int, ...] = (25, 50, 100)

# Defaults used when a stats query does not specify threshold/scale.
DEFAULT_THRESHOLD_MM: float = 10.0
DEFAULT_SCALE_KM: int = 50

# Fraction of the curve's own maximum used for the derived lock-in leads
# (L_stab on the J curve, L_acc on the e_L curve). Shared with the frontend
# via config.json so the displayed definition can never drift from the docs.
# 0.75, not 0.5: the e_L curve has a high floor (irreducible daily-precip
# RMSE — e_1/e_max is typically 0.5-0.7), so a 0.5 cut is below the floor and
# L_acc would be undefined for most slices. The cut must sit above the floors
# of BOTH curves for the lock-in gap to exist; 0.75 does, while still marking
# a substantial drop from the long-lead plateau.
LOCK_IN_FRACTION: float = 0.75


def threshold_tag(q: float) -> str:
    """Filename tag for a threshold: 1.0 -> 'q1', 2.5 -> 'q2p5'."""
    text = f"{q:g}".replace(".", "p")
    return f"q{text}"


def scale_tag(n: int) -> str:
    """Filename tag for a neighborhood scale: 50 -> 's50'."""
    return f"s{n:g}"


def combo_tag(q: float, n: int) -> str:
    return f"{threshold_tag(q)}_{scale_tag(n)}"


def consistency_definitions() -> tuple[StatisticDefinition, StatisticDefinition]:
    """Registry entry for run-to-run jumpiness J = 1 - FSS(f_{L+1}, f_L).

    Not computed by compute_stats.py: jumpiness pairs *two forecasts of the
    same valid date* (successive runs), not forecast vs. observation, so it is
    produced by compute_stats.py --only consistency. This entry exists so the exporter, API
    and frontend discover the statistic via the shared registry. Panel-only:
    no map tiles are rendered (a lead *pair* map per threshold x scale would
    multiply the tile tree 12x for a chart-first statistic).

    The "lead" axis of jumpiness is the lower lead of the run pair: the value
    at lead L compares the forecasts for the same valid date issued at leads
    L+1 and L (L = 1..lead_days_max-1).
    """

    common_parameters = {
        "thresholds_mm": list(JUMPINESS_THRESHOLDS_MM),
        "scales_km": list(JUMPINESS_SCALES_KM),
        "default_threshold_mm": DEFAULT_THRESHOLD_MM,
        "default_scale_km": DEFAULT_SCALE_KM,
        "lock_in_fraction": LOCK_IN_FRACTION,
    }
    jumpiness = StatisticDefinition(StatisticSpec(
        name="jumpiness",
        label="Jumpiness",
        units="",
        render_field="value",
        colormap="diverging",
        fixed_range=(0.0, 1.0),
        axis="lead_pair",
        parameters={**common_parameters, "curve_role": "stability"},
        periods=("yearly", "seasonal"),
        storage=StorageDescriptor(
            kind="packed_grid", source_fields=("num_{combo}", "den_{combo}"),
            export_dtype="float16", aggregation="ratio", finalize="ratio",
            path_template="jumpiness/{period_path}pair_{lead}.npz",
        ),
        map_support=False,
        winner_support=False,
    ))
    """Registry entry for convergence-to-truth e_L (RMSE vs PRISM by lead).

    Same math as a plain RMSE, but restricted to *scorable* valid dates —
    dates for which every lead 1..lead_days_max exists — so the e_L curve and
    the jumpiness curve are computed over the identical valid-date set and the
    lock-in gap (L_stab - L_acc) compares like with like. Computed by
    compute_stats.py --only consistency; panel-only (no map tiles).
    """

    convergence = StatisticDefinition(StatisticSpec(
        name="convergence",
        label="Convergence RMSE",
        units="mm/day",
        render_field="value",
        colormap="diverging",
        parameters={**common_parameters, "curve_role": "accuracy"},
        periods=("yearly", "seasonal"),
        storage=StorageDescriptor(
            kind="packed_grid", source_fields=("sum_squared_error", "sample_count"),
            export_dtype="float32", aggregation="ratio", finalize="sqrt_ratio",
            path_template="convergence/{period_path}lead_{lead}.npz",
        ),
        map_support=False,
        winner_support=False,
    ))
    return jumpiness, convergence


def episode_definitions() -> tuple[StatisticDefinition, StatisticDefinition]:
    """Metadata for basin-only revision efficiency R.

    R requires an intact daily forecast sequence and is therefore produced by
    compute_stats.py --only basin, not by either gridded accumulator pipeline.
    It is precomputed for catalogued basin AR episodes and exposed through
    /api/stats only after an episode is selected.
    """

    storage = StorageDescriptor(
        kind="episode_catalog", source_fields=(), export_dtype="json",
        aggregation="catalog", path_template="episodes.json",
    )
    # Revision efficiency has two production paths sharing one statistic name:
    # 1. Basin AR-episode/scope values (cluster-bootstrap CIs) precomputed by
    #    compute_stats.py --only basin/episodes and served via the episode
    #    context (registry.EPISODE_CONTEXT_STATS governs that surface).
    # 2. A grid-pooled scalar for arbitrary regions/periods: per-pair squared
    #    revision planes (rev_sse/rev_cnt) accumulated by the consistency
    #    stage, pooled over a region mask and divided by the pooled skill gain
    #    MSE(lead_max) - MSE(lead_min) from the convergence planes. This spec
    #    describes path 2; scope="region_pooled" keeps it out of per-lead stat
    #    cards, map tiles, and winner tables.
    revision = StatisticDefinition(StatisticSpec(
        name="revision_efficiency",
        label="Revision efficiency (R)",
        units="ratio",
        render_field="value",
        colormap="diverging",
        scope="region_pooled", axis="lead_pair",
        periods=("yearly", "seasonal"),
        parameters={"definition": "grid-pooled"},
        storage=StorageDescriptor(
            kind="packed_grid", source_fields=("rev_sse", "rev_cnt"),
            export_dtype="float32", aggregation="ratio", finalize="ratio",
            path_template="revision/{period_path}pair_{lead}.npz",
        ),
        map_support=False, winner_support=False,
    ))
    lock_gap = StatisticDefinition(StatisticSpec(
        name="lock_in_gap", label="Basin lock-in gap", units="days",
        render_field="value", colormap="diverging",
        scope="ar_episode", axis="episode", periods=(), storage=storage,
        parameters={"lock_in_fraction": LOCK_IN_FRACTION},
        map_support=False, winner_support=False,
    ))
    return revision, lock_gap
