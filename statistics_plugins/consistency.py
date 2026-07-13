from __future__ import annotations

from statistics_plugins.base import StatisticSpec


# Parameters of the jumpiness statistic (see compute_consistency.py).
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


class JumpinessPlugin:
    """Registry entry for run-to-run jumpiness J = 1 - FSS(f_{L+1}, f_L).

    Not computed by compute_stats.py: jumpiness pairs *two forecasts of the
    same valid date* (successive runs), not forecast vs. observation, so it is
    produced by compute_consistency.py. This entry exists so the exporter, API
    and frontend discover the statistic via the shared registry. Panel-only:
    no map tiles are rendered (a lead *pair* map per threshold x scale would
    multiply the tile tree 12x for a chart-first statistic).

    The "lead" axis of jumpiness is the lower lead of the run pair: the value
    at lead L compares the forecasts for the same valid date issued at leads
    L+1 and L (L = 1..lead_days_max-1).
    """

    spec = StatisticSpec(
        name="jumpiness",
        label="Jumpiness",
        units="",
        render_field="value",
        colormap="diverging",
        fixed_range=(0.0, 1.0),
    )

    def init_accumulator(self, shape):
        raise NotImplementedError("Jumpiness is computed by compute_consistency.py")

    def update(self, *args, **kwargs):
        raise NotImplementedError("Jumpiness is computed by compute_consistency.py")

    def finalize(self, *args, **kwargs):
        raise NotImplementedError("Jumpiness is computed by compute_consistency.py")


class ConvergencePlugin:
    """Registry entry for convergence-to-truth e_L (RMSE vs PRISM by lead).

    Same math as a plain RMSE, but restricted to *scorable* valid dates —
    dates for which every lead 1..lead_days_max exists — so the e_L curve and
    the jumpiness curve are computed over the identical valid-date set and the
    lock-in gap (L_stab - L_acc) compares like with like. Computed by
    compute_consistency.py; panel-only (no map tiles).
    """

    spec = StatisticSpec(
        name="convergence",
        label="Convergence RMSE",
        units="mm/day",
        render_field="value",
        colormap="diverging",
    )

    def init_accumulator(self, shape):
        raise NotImplementedError("Convergence is computed by compute_consistency.py")

    def update(self, *args, **kwargs):
        raise NotImplementedError("Convergence is computed by compute_consistency.py")

    def finalize(self, *args, **kwargs):
        raise NotImplementedError("Convergence is computed by compute_consistency.py")
