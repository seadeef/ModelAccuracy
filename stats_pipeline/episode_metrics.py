"""Pure calculations for HUC6 histories and episode/scope-scoped statistics.

Statistical design:

- Per-episode values are *ungated point estimates*: R = mean revision /
  mean skill gain over the episode's complete dates, null only when the
  skill gain is non-positive (flagged) or no complete date exists. Episodes
  get no bootstrap interval at all — 48% of them span a single forecast
  date, where any resampling interval is degenerate by construction.
- Inference lives at pooled scopes ("ar_all"/"non_ar") via a cluster
  bootstrap: the resampling unit is a cluster of episodes (or a pseudo-
  cluster of consecutive non-AR days), never an individual date, so serial
  dependence inside a storm sequence is respected.
- Seeds are model-independent (no model component in any seed_key), so the
  same basin/scope/catalog yields identical replicate draws across models —
  common random numbers for the paired ΔR comparison.
"""

from __future__ import annotations

import hashlib

import numpy as np


DEFAULT_BOOTSTRAP_SAMPLES = 2_000
DEFAULT_CONFIDENCE = 0.95
# A ratio CI is only trustworthy when almost every replicate has a positive
# denominator; above this dropped fraction the interval is suppressed.
MAX_DROPPED_REPLICATE_FRACTION = 0.01
CALIFORNIA_HUC2 = "18"
HUC_LEVEL = 6


def basin_history_filename(huc2: str = CALIFORNIA_HUC2) -> str:
    """Per-region basin-history artifact name (W3, 2026-08-03 amendment).

    California (HUC2 18) keeps the historical name that existing importers
    bind to; every other inferential/descriptive region gets its own file,
    so region products never overwrite one another.
    """
    return f"basin_consistency_huc{HUC_LEVEL}_{huc2}.npz"


BASIN_HISTORY_FILENAME = basin_history_filename(CALIFORNIA_HUC2)


def lock_in_lead(series: np.ndarray, lead_min: int, fraction: float) -> float:
    """Match the frontend's prefix-based lock-in definition for one curve."""
    values = np.asarray(series, dtype=np.float64)
    finite = np.isfinite(values)
    if not finite.any():
        return np.nan
    maximum = float(np.max(values[finite]))
    if maximum <= 0.0:
        return np.nan
    cut = fraction * maximum
    result = np.nan
    for offset, value in enumerate(values):
        if not np.isfinite(value):
            continue
        if value >= cut:
            break
        result = float(lead_min + offset)
    return result


def daily_lock_in(
    forecasts: np.ndarray,
    observed: np.ndarray,
    lead_min: int,
    fraction: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Daily raw-basin lock-in leads from revisions and absolute errors.

    ``forecasts`` is (date, lead, basin), ordered from shortest to longest
    lead. Stabilization uses |F_L-F_(L+1)| and accuracy uses |F_L-y|. This is
    deliberately named raw-basin lock-in: it is not the period-pooled FSS/RMSE
    diagnostic shown by the existing map panel.
    """
    forecasts = np.asarray(forecasts, dtype=np.float64)
    observed = np.asarray(observed, dtype=np.float64)
    if forecasts.ndim != 3 or observed.shape != forecasts.shape[::2]:
        raise ValueError("expected forecasts (date, lead, basin) and observed (date, basin)")
    n_dates, _, n_basins = forecasts.shape
    stab = np.full((n_dates, n_basins), np.nan, dtype=np.float32)
    acc = np.full_like(stab, np.nan)
    for di in range(n_dates):
        for bi in range(n_basins):
            f = forecasts[di, :, bi]
            stab[di, bi] = lock_in_lead(np.abs(np.diff(f)), lead_min, fraction)
            acc[di, bi] = lock_in_lead(np.abs(f - observed[di, bi]), lead_min, fraction)
    return stab, acc, stab - acc


def _stable_seed(seed_key: str) -> int:
    digest = hashlib.sha256(seed_key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little", signed=False)


# ---------------------------------------------------------------------------
# Per-date components
# ---------------------------------------------------------------------------

def per_date_components(
    forecasts: np.ndarray, observed: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Complete-date mask plus per-date revision and skill-gain terms.

    ``forecasts`` is (date, lead) for one basin, columns ordered shortest to
    longest lead; ``observed`` is (date,). A date is "complete" when the
    observation and every lead are finite — the same filter the previous
    revision_efficiency applied internally. The returned per-date arrays are
    full length and only meaningful where the mask is True:

      revision_by_date = sum_L (F_L - F_(L+1))^2
      skill_by_date    = (F_max - y)^2 - (F_min - y)^2
    """
    f = np.asarray(forecasts, dtype=np.float64)
    y = np.asarray(observed, dtype=np.float64)
    if f.ndim != 2 or y.ndim != 1 or f.shape[0] != y.size:
        raise ValueError("expected forecasts (date, lead) and observed (date,)")
    complete = np.isfinite(y) & np.all(np.isfinite(f), axis=1)
    with np.errstate(invalid="ignore"):
        revision_by_date = np.sum(np.diff(f, axis=1) ** 2, axis=1)
        skill_by_date = (f[:, -1] - y) ** 2 - (f[:, 0] - y) ** 2
    return complete, revision_by_date, skill_by_date


# ---------------------------------------------------------------------------
# Per-episode statistics (ungated point estimates, no bootstrap)
# ---------------------------------------------------------------------------

def episode_revision_efficiency(
    revision_by_date: np.ndarray, skill_by_date: np.ndarray,
) -> dict:
    """Per-episode R point estimate over the episode's complete dates.

    No bootstrap and no significance gate: value is the plain ratio whenever
    the mean skill gain is positive, null otherwise (flagged), matching the
    contract's per-episode v3 shape. Single-date episodes get a value.
    """
    revision = np.asarray(revision_by_date, dtype=np.float64)
    skill = np.asarray(skill_by_date, dtype=np.float64)
    n_dates = int(revision.size)
    result = {
        "value": None,
        "raw_value": None,
        "dates": n_dates,
        "single_event": True,
        "skill_gain": None,
        "skill_gain_nonpositive": False,
        "units": "ratio",
    }
    if n_dates == 0:
        return result
    skill_gain = float(np.mean(skill))
    result["skill_gain"] = skill_gain
    if skill_gain <= 0.0:
        result["skill_gain_nonpositive"] = True
        return result
    ratio = float(np.mean(revision)) / skill_gain
    result.update({"value": ratio, "raw_value": ratio})
    return result


def episode_lock_in_gap(daily_gaps: np.ndarray) -> dict:
    """Per-episode descriptive lock-in gap: mean/min/max of finite daily gaps.

    NaN days (a curve never dropping below the lock-in cut) are missing, not
    zero. Per-episode CIs are gone entirely — a one-date episode can only
    yield the degenerate [x, x] interval, which misleads.
    """
    gaps = np.asarray(daily_gaps, dtype=np.float64)
    finite = gaps[np.isfinite(gaps)]
    result = {
        "value": None,
        "dates": int(finite.size),
        "single_event": True,
        "daily_gap_min": None,
        "daily_gap_max": None,
        "units": "days",
    }
    if finite.size == 0:
        return result
    result.update({
        "value": float(np.mean(finite)),
        "daily_gap_min": float(np.min(finite)),
        "daily_gap_max": float(np.max(finite)),
    })
    return result


# ---------------------------------------------------------------------------
# Cluster bootstrap machinery (pooled scopes and paired ΔR)
# ---------------------------------------------------------------------------

def cluster_draws(
    n_clusters: int, bootstrap_samples: int, seed_key: str,
) -> np.ndarray:
    """(B, K) matrix of cluster indices resampled with replacement.

    The seed_key never contains a model name, so identical basin/scope/catalog
    inputs produce identical draws for every model (common random numbers).
    """
    rng = np.random.default_rng(_stable_seed(seed_key))
    return rng.integers(0, n_clusters, size=(bootstrap_samples, n_clusters))


def _cluster_sums(
    values: np.ndarray, clusters: list[np.ndarray],
) -> tuple[np.ndarray, np.ndarray]:
    """Per-cluster (sum, finite count) of ``values``; NaNs count as missing."""
    sums = np.empty(len(clusters), dtype=np.float64)
    counts = np.empty(len(clusters), dtype=np.float64)
    for ki, idx in enumerate(clusters):
        sample = values[np.asarray(idx, dtype=np.intp)]
        finite = np.isfinite(sample)
        sums[ki] = sample[finite].sum()
        counts[ki] = finite.sum()
    return sums, counts


def _replicate_means(
    sums: np.ndarray, counts: np.ndarray, draws: np.ndarray,
) -> np.ndarray:
    """Date-weighted replicate means: total sum / total count per replicate.

    Concatenating the drawn clusters' dates and averaging equals summing the
    per-cluster sums and dividing by the summed counts, so no per-replicate
    date lists are materialized. Replicates with zero dates are NaN.
    """
    rep_sum = sums[draws].sum(axis=1)
    rep_count = counts[draws].sum(axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        means = rep_sum / rep_count
    means[rep_count <= 0] = np.nan
    return means


def _percentile_interval(sample: np.ndarray, confidence: float) -> list[float]:
    alpha = (1.0 - confidence) / 2.0
    lo, hi = np.quantile(sample, [alpha, 1.0 - alpha])
    return [float(lo), float(hi)]


def scope_revision_efficiency(
    revision_by_date: np.ndarray,
    skill_by_date: np.ndarray,
    clusters: list[np.ndarray],
    *,
    seed_key: str,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    confidence: float = DEFAULT_CONFIDENCE,
) -> dict:
    """Pooled R for one scope with a cluster-bootstrap percentile interval.

    ``clusters`` partitions [0, n) index positions of the per-date arrays
    (complete dates only). The PRIMARY estimate is date-weighted; the
    episode(cluster)-weighted ratio is reported as a robustness check.
    Replicates whose mean skill gain is <= 0 are dropped from the ratio CI;
    if more than MAX_DROPPED_REPLICATE_FRACTION are dropped the interval is
    suppressed. The skill-gain CI is always reported.
    """
    revision = np.asarray(revision_by_date, dtype=np.float64)
    skill = np.asarray(skill_by_date, dtype=np.float64)
    n_dates = int(revision.size)
    result = {
        "value": None,
        "value_ci": [None, None],
        "value_episode_weighted": None,
        "dates": n_dates,
        "clusters": len(clusters),
        "dropped_replicate_fraction": 0.0,
        "interval_suppressed": False,
        "skill_gain": None,
        "skill_gain_ci": [None, None],
        "significant_skill_gain": False,
        "units": "ratio",
    }
    if n_dates == 0 or not clusters:
        return result

    skill_gain = float(np.mean(skill))
    result["skill_gain"] = skill_gain
    if skill_gain > 0.0:
        result["value"] = float(np.mean(revision)) / skill_gain

    # Robustness check: cluster means averaged with equal cluster weight,
    # numerator and denominator each cluster-mean-averaged, then the ratio.
    rev_sums, rev_counts = _cluster_sums(revision, clusters)
    skill_sums, skill_counts = _cluster_sums(skill, clusters)
    cluster_skill_mean = float(np.mean(skill_sums / skill_counts))
    if cluster_skill_mean > 0.0:
        result["value_episode_weighted"] = (
            float(np.mean(rev_sums / rev_counts)) / cluster_skill_mean)

    draws = cluster_draws(len(clusters), bootstrap_samples, seed_key)
    boot_skill = _replicate_means(skill_sums, skill_counts, draws)
    boot_revision = _replicate_means(rev_sums, rev_counts, draws)
    result["skill_gain_ci"] = _percentile_interval(boot_skill, confidence)
    result["significant_skill_gain"] = result["skill_gain_ci"][0] > 0.0

    valid = boot_skill > 0.0
    dropped = 1.0 - float(np.mean(valid))
    result["dropped_replicate_fraction"] = dropped
    if dropped > MAX_DROPPED_REPLICATE_FRACTION or not valid.any():
        result["interval_suppressed"] = dropped > MAX_DROPPED_REPLICATE_FRACTION
    else:
        result["value_ci"] = _percentile_interval(
            boot_revision[valid] / boot_skill[valid], confidence)
    return result


def scope_lock_in_gap(
    daily_gaps: np.ndarray,
    clusters: list[np.ndarray],
    *,
    seed_key: str,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    confidence: float = DEFAULT_CONFIDENCE,
) -> dict:
    """Scope-mean daily lock-in gap with a cluster-bootstrap interval.

    NaN days are missing, as in the per-episode statistic. The percentile
    interval is quantized to the statistic's native 0.5-day resolution
    (round(x*2)/2) — lock-in leads are half-day-at-best quantities and a
    finer interval would imply spurious precision.
    """
    gaps = np.asarray(daily_gaps, dtype=np.float64)
    finite = gaps[np.isfinite(gaps)]
    result = {
        "value": None,
        "value_ci": [None, None],
        "dates": int(finite.size),
        "clusters": len(clusters),
        "units": "days",
    }
    if finite.size == 0 or not clusters:
        return result
    result["value"] = float(np.mean(finite))
    sums, counts = _cluster_sums(gaps, clusters)
    draws = cluster_draws(len(clusters), bootstrap_samples, seed_key)
    boot = _replicate_means(sums, counts, draws)
    boot = boot[np.isfinite(boot)]  # replicates with no finite gap day
    if boot.size:
        lo, hi = _percentile_interval(boot, confidence)
        result["value_ci"] = [round(lo * 2.0) / 2.0, round(hi * 2.0) / 2.0]
    return result


def paired_delta_r(
    revision_a: np.ndarray,
    skill_a: np.ndarray,
    revision_b: np.ndarray,
    skill_b: np.ndarray,
    clusters: list[np.ndarray],
    *,
    seed_key: str,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    confidence: float = DEFAULT_CONFIDENCE,
) -> dict:
    """Paired ΔR = R(model_a) − R(model_b) over a shared intersected date set.

    All four arrays are aligned on the intersected complete dates and
    ``clusters`` is the shared cluster structure built from them; the shared
    model-independent seed makes both models see identical cluster draws
    (common random numbers), so ΔR replicates difference out shared weather.
    Replicates where either denominator is <= 0 are dropped; the CI is
    suppressed above MAX_DROPPED_REPLICATE_FRACTION. p_direction is the
    fraction of kept replicates with ΔR > 0.
    """
    rev_a = np.asarray(revision_a, dtype=np.float64)
    skl_a = np.asarray(skill_a, dtype=np.float64)
    rev_b = np.asarray(revision_b, dtype=np.float64)
    skl_b = np.asarray(skill_b, dtype=np.float64)
    n_dates = int(rev_a.size)
    result = {
        "intersected_dates": n_dates,
        "clusters": len(clusters),
        "delta_r": None,
        "delta_r_ci": [None, None],
        "p_direction": None,
        "dropped_replicate_fraction": 0.0,
    }
    if n_dates == 0 or not clusters:
        return result

    mean_skill_a = float(np.mean(skl_a))
    mean_skill_b = float(np.mean(skl_b))
    if mean_skill_a > 0.0 and mean_skill_b > 0.0:
        result["delta_r"] = (float(np.mean(rev_a)) / mean_skill_a
                             - float(np.mean(rev_b)) / mean_skill_b)

    draws = cluster_draws(len(clusters), bootstrap_samples, seed_key)
    boot_r_a = (_replicate_means(*_cluster_sums(rev_a, clusters), draws),
                _replicate_means(*_cluster_sums(skl_a, clusters), draws))
    boot_r_b = (_replicate_means(*_cluster_sums(rev_b, clusters), draws),
                _replicate_means(*_cluster_sums(skl_b, clusters), draws))
    keep = (boot_r_a[1] > 0.0) & (boot_r_b[1] > 0.0)
    dropped = 1.0 - float(np.mean(keep))
    result["dropped_replicate_fraction"] = dropped
    if not keep.any():
        return result
    deltas = boot_r_a[0][keep] / boot_r_a[1][keep] - boot_r_b[0][keep] / boot_r_b[1][keep]
    result["p_direction"] = float(np.mean(deltas > 0.0))
    if dropped <= MAX_DROPPED_REPLICATE_FRACTION:
        result["delta_r_ci"] = _percentile_interval(deltas, confidence)
    return result


def paired_delta_j(
    num_a: np.ndarray,
    den_a: np.ndarray,
    num_b: np.ndarray,
    den_b: np.ndarray,
    clusters: list[np.ndarray],
    *,
    seed_key: str,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    confidence: float = DEFAULT_CONFIDENCE,
) -> dict:
    """Paired ΔJ = J(model_a) − J(model_b) over a shared intersected date set
    (2026-07-24 amendment).

    J is the pooled jumpiness Σnum/Σden at one (lead pair, threshold, scale)
    combo; the four arrays hold each model's per-date num/den aligned on the
    intersected dates. Same common-random-numbers design as ``paired_delta_r``
    (shared model-independent seed, cluster draws); replicates where either
    pooled denominator is <= 0 are dropped and the interval is suppressed
    above MAX_DROPPED_REPLICATE_FRACTION. Dry dates (den == 0) participate
    with zero weight, exactly as in the plane pooling.
    """
    na = np.asarray(num_a, dtype=np.float64)
    da = np.asarray(den_a, dtype=np.float64)
    nb = np.asarray(num_b, dtype=np.float64)
    db = np.asarray(den_b, dtype=np.float64)
    n_dates = int(na.size)
    result = {
        "intersected_dates": n_dates,
        "clusters": len(clusters),
        "j_a": None,
        "j_b": None,
        "delta_j": None,
        "delta_j_ci": [None, None],
        "p_direction": None,
        "dropped_replicate_fraction": 0.0,
    }
    if n_dates == 0 or not clusters:
        return result

    den_sum_a = float(np.sum(da))
    den_sum_b = float(np.sum(db))
    if den_sum_a > 0.0 and den_sum_b > 0.0:
        result["j_a"] = float(np.sum(na)) / den_sum_a
        result["j_b"] = float(np.sum(nb)) / den_sum_b
        result["delta_j"] = result["j_a"] - result["j_b"]

    draws = cluster_draws(len(clusters), bootstrap_samples, seed_key)
    # Σnum/Σden per replicate == (mean num)/(mean den): the shared count
    # cancels, so _replicate_means composes into the pooled ratio directly.
    boot_num_a = _replicate_means(*_cluster_sums(na, clusters), draws)
    boot_den_a = _replicate_means(*_cluster_sums(da, clusters), draws)
    boot_num_b = _replicate_means(*_cluster_sums(nb, clusters), draws)
    boot_den_b = _replicate_means(*_cluster_sums(db, clusters), draws)
    keep = (boot_den_a > 0.0) & (boot_den_b > 0.0)
    dropped = 1.0 - float(np.mean(keep))
    result["dropped_replicate_fraction"] = dropped
    if not keep.any():
        return result
    deltas = (boot_num_a[keep] / boot_den_a[keep]
              - boot_num_b[keep] / boot_den_b[keep])
    result["p_direction"] = float(np.mean(deltas > 0.0))
    if dropped <= MAX_DROPPED_REPLICATE_FRACTION:
        result["delta_j_ci"] = _percentile_interval(deltas, confidence)
    return result
