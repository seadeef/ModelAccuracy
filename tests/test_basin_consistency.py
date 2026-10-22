import unittest

import numpy as np

from stats_pipeline.episode_metrics import (
    cluster_draws,
    daily_lock_in,
    episode_lock_in_gap,
    episode_revision_efficiency,
    paired_delta_r,
    per_date_components,
    scope_lock_in_gap,
    scope_revision_efficiency,
)


class PerDateComponentTests(unittest.TestCase):
    def test_complete_mask_requires_observation_and_all_leads(self):
        forecasts = np.array([[1.0, 2.0], [np.nan, 2.0], [1.0, 2.0]])
        observed = np.array([0.0, 0.0, np.nan])
        complete, revision, skill = per_date_components(forecasts, observed)
        self.assertEqual(list(complete), [True, False, False])
        self.assertEqual(revision[0], 1.0)          # (2-1)^2
        self.assertEqual(skill[0], 3.0)             # (2-0)^2 - (1-0)^2


class EpisodeRevisionEfficiencyTests(unittest.TestCase):
    def test_single_date_episode_gets_ungated_point_estimate(self):
        # The old bootstrap significance gate nulled every 1-date episode;
        # v3 reports the plain ratio whenever the skill gain is positive.
        forecasts = np.array([[1.0, 2.0, 4.0]])
        observed = np.array([0.0])
        _, revision, skill = per_date_components(forecasts, observed)
        result = episode_revision_efficiency(revision, skill)
        self.assertAlmostEqual(result["value"], 5.0 / 15.0)
        self.assertEqual(result["raw_value"], result["value"])
        self.assertEqual(result["dates"], 1)
        self.assertTrue(result["single_event"])
        self.assertFalse(result["skill_gain_nonpositive"])
        self.assertNotIn("value_ci", result)

    def test_nonpositive_skill_gain_is_null_and_flagged(self):
        forecasts = np.tile(np.array([[2.0, 1.0]]), (5, 1))
        observed = np.zeros(5)
        _, revision, skill = per_date_components(forecasts, observed)
        result = episode_revision_efficiency(revision, skill)
        self.assertIsNone(result["value"])
        self.assertTrue(result["skill_gain_nonpositive"])
        self.assertLess(result["skill_gain"], 0.0)

    def test_zero_complete_dates_is_null_without_flag(self):
        result = episode_revision_efficiency(np.empty(0), np.empty(0))
        self.assertIsNone(result["value"])
        self.assertIsNone(result["skill_gain"])
        self.assertFalse(result["skill_gain_nonpositive"])


class EpisodeLockInGapTests(unittest.TestCase):
    def test_descriptive_min_max_and_no_ci(self):
        result = episode_lock_in_gap(np.array([1.0, np.nan, -3.0]))
        self.assertEqual(result["value"], -1.0)
        self.assertEqual(result["dates"], 2)  # NaN day is missing, not zero
        self.assertEqual(result["daily_gap_min"], -3.0)
        self.assertEqual(result["daily_gap_max"], 1.0)
        self.assertNotIn("value_ci", result)

    def test_all_nan_days_yield_null(self):
        result = episode_lock_in_gap(np.array([np.nan, np.nan]))
        self.assertIsNone(result["value"])
        self.assertEqual(result["dates"], 0)


class ScopeRevisionEfficiencyTests(unittest.TestCase):
    def test_martingale_accounting_reports_one(self):
        # Four equiprobable forecast paths. Across cases, E[r1*r2]=0 and
        # errors telescope so skill gain equals cumulative squared revision.
        f = np.array([
            [0.0, -1.0, -2.0],
            [0.0, -1.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 1.0, 2.0],
        ])
        f = np.tile(f, (100, 1))
        y = np.zeros(f.shape[0])
        _, revision, skill = per_date_components(f, y)
        clusters = [np.arange(i, i + 4) for i in range(0, 400, 4)]
        result = scope_revision_efficiency(
            revision, skill, clusters, seed_key="scope:R:test",
            bootstrap_samples=500)
        self.assertAlmostEqual(result["value"], 1.0)
        self.assertAlmostEqual(result["value_episode_weighted"], 1.0)
        self.assertTrue(result["significant_skill_gain"])
        self.assertFalse(result["interval_suppressed"])
        lo, hi = result["value_ci"]
        self.assertLessEqual(lo, 1.0)
        self.assertGreaterEqual(hi, 1.0)
        self.assertEqual(result["dropped_replicate_fraction"], 0.0)

    def test_dropped_replicates_above_threshold_suppress_interval(self):
        # Half of the clusters carry a negative skill gain, so many replicate
        # denominators land <= 0: interval suppressed, value still reported.
        revision = np.full(20, 1.0)
        skill = np.array([1.0, -0.9] * 10)
        clusters = [np.array([i]) for i in range(20)]
        result = scope_revision_efficiency(
            revision, skill, clusters, seed_key="scope:R:suppress",
            bootstrap_samples=500)
        self.assertGreater(result["dropped_replicate_fraction"], 0.01)
        self.assertTrue(result["interval_suppressed"])
        self.assertEqual(result["value_ci"], [None, None])
        self.assertIsNotNone(result["value"])
        self.assertNotEqual(result["skill_gain_ci"], [None, None])

    def test_date_and_episode_weighting_differ_for_unbalanced_clusters(self):
        # Cluster A: 3 dates with revision 3 / skill 1; cluster B: 1 date, 1/3.
        revision = np.array([3.0, 3.0, 3.0, 1.0])
        skill = np.array([1.0, 1.0, 1.0, 3.0])
        clusters = [np.arange(3), np.array([3])]
        result = scope_revision_efficiency(
            revision, skill, clusters, seed_key="scope:R:weights",
            bootstrap_samples=50)
        self.assertAlmostEqual(result["value"], 10.0 / 6.0)            # date-weighted
        self.assertAlmostEqual(result["value_episode_weighted"], 1.0)  # (3+1)/2 over (1+3)/2

    def test_empty_scope_is_all_null(self):
        result = scope_revision_efficiency(
            np.empty(0), np.empty(0), [], seed_key="scope:R:empty")
        self.assertIsNone(result["value"])
        self.assertEqual(result["value_ci"], [None, None])
        self.assertEqual(result["dates"], 0)
        self.assertEqual(result["clusters"], 0)


class ScopeLockInGapTests(unittest.TestCase):
    def test_interval_is_quantized_to_half_days(self):
        rng = np.random.default_rng(7)
        gaps = rng.normal(-1.3, 2.0, size=40)
        clusters = [np.arange(i, i + 4) for i in range(0, 40, 4)]
        result = scope_lock_in_gap(gaps, clusters, seed_key="scope:gap:test",
                                   bootstrap_samples=300)
        self.assertAlmostEqual(result["value"], float(np.mean(gaps)))
        for bound in result["value_ci"]:
            self.assertEqual(round(bound * 2.0), bound * 2.0)

    def test_nan_days_are_missing(self):
        gaps = np.array([1.0, np.nan, 3.0])
        result = scope_lock_in_gap(gaps, [np.arange(3)], seed_key="scope:gap:nan",
                                   bootstrap_samples=100)
        self.assertEqual(result["value"], 2.0)
        self.assertEqual(result["dates"], 2)


class SeedPolicyTests(unittest.TestCase):
    def test_common_seed_key_produces_identical_replicate_indices(self):
        # The seed_key has no model component, so two models sharing a basin,
        # scope and episode-set digest draw identical cluster replicates.
        seed_key = "scope:R:180102:ar_all:abcdef012345"
        draws_model_a = cluster_draws(9, 250, seed_key)
        draws_model_b = cluster_draws(9, 250, seed_key)
        np.testing.assert_array_equal(draws_model_a, draws_model_b)

    def test_different_scope_changes_draws(self):
        a = cluster_draws(9, 50, "scope:R:180102:ar_all:abcdef012345")
        b = cluster_draws(9, 50, "scope:R:180102:non_ar:abcdef012345")
        self.assertFalse(np.array_equal(a, b))


class PairedDeltaRTests(unittest.TestCase):
    def test_delta_sign_matches_constructed_ordering(self):
        # Model A revises twice as much as B for the same skill gain, so
        # R_a - R_b = +1 in every replicate: tight CI, p_direction = 1.
        n = 24
        skill = np.full(n, 2.0)
        rev_b = np.full(n, 2.0)
        rev_a = np.full(n, 4.0)
        clusters = [np.arange(i, i + 3) for i in range(0, n, 3)]
        result = paired_delta_r(rev_a, skill, rev_b, skill, clusters,
                                seed_key="delta:R:test", bootstrap_samples=400)
        self.assertAlmostEqual(result["delta_r"], 1.0)
        self.assertAlmostEqual(result["delta_r_ci"][0], 1.0)
        self.assertAlmostEqual(result["delta_r_ci"][1], 1.0)
        self.assertEqual(result["p_direction"], 1.0)
        self.assertEqual(result["dropped_replicate_fraction"], 0.0)
        self.assertEqual(result["intersected_dates"], n)

    def test_unstable_denominators_suppress_interval(self):
        skill = np.array([1.0, -0.9] * 10)
        rev = np.ones(20)
        clusters = [np.array([i]) for i in range(20)]
        result = paired_delta_r(rev, skill, rev * 2.0, skill, clusters,
                                seed_key="delta:R:unstable", bootstrap_samples=400)
        self.assertGreater(result["dropped_replicate_fraction"], 0.01)
        self.assertEqual(result["delta_r_ci"], [None, None])


class DailyLockInTests(unittest.TestCase):
    def test_daily_lock_in_is_explicitly_raw_basin(self):
        forecasts = np.array([[[0.0], [0.1], [2.0], [4.0]]])
        observed = np.array([[0.0]])
        stab, acc, gap = daily_lock_in(forecasts, observed, 1, 0.75)
        self.assertEqual(stab[0, 0], 1)
        self.assertEqual(acc[0, 0], 3)
        self.assertEqual(gap[0, 0], -2)


if __name__ == "__main__":
    unittest.main()
