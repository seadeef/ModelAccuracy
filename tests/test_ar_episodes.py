from datetime import date
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
from shapely.geometry import box
import xarray as xr

from atmospheric_rivers.dataset import NormalizedFrame
from stats_pipeline.episode_metrics import BASIN_HISTORY_FILENAME, cluster_draws
from stats_pipeline.episodes import (
    assign_clusters,
    build_scope_clusters,
    cluster_ids,
    episode_set_digest,
    find_basin_episodes,
    group_pseudo_clusters,
    run_compare_stage,
    run_episode_stage,
)


def _episode(huc, start, end):
    return {
        "id": f"{huc}-{start.replace('-', '')}-{end.replace('-', '')}",
        "name": "Test basin",
        "start_date": start,
        "end_date": end,
        "active_days": (date.fromisoformat(end) - date.fromisoformat(start)).days + 1,
        "peak_basin_area_fraction": 0.5,
    }


class AtmosphericRiverEpisodeTests(unittest.TestCase):
    def test_consecutive_active_days_form_date_range(self):
        frames = [NormalizedFrame(
            Path("raw-target.nc"), index, f"2026-01-{day:02d}T00:00:00Z",
            np.ones((2, 2), dtype=bool), np.array([0.75, 0.25]), np.array([0.25, 0.75]),
        ) for index, day in enumerate((1, 2, 4))]
        with (
            patch("stats_pipeline.episodes.iter_normalized_frames", return_value=iter(frames)),
            patch("stats_pipeline.episodes.load_basins", return_value=[
                ("180101", "Test basin", box(0, 0, 1, 1)),
            ]),
        ):
            episodes = find_basin_episodes([Path("raw-target.nc")])
        self.assertEqual(len(episodes["180101"]), 2)
        self.assertEqual(episodes["180101"][0]["start_date"], "2026-01-01")
        self.assertEqual(episodes["180101"][0]["end_date"], "2026-01-02")
        self.assertEqual(episodes["180101"][1]["active_days"], 1)


class ClusterRuleTests(unittest.TestCase):
    def test_sub_72h_gap_merges_and_72h_gap_does_not(self):
        episodes = [
            _episode("180101", "2026-01-02", "2026-01-03"),
            # next.start - prev.end = 2 days (48h) < 72h: same cluster.
            _episode("180101", "2026-01-05", "2026-01-05"),
            # next.start - prev.end = 3 days (72h), not < 72h: new cluster.
            _episode("180101", "2026-01-08", "2026-01-09"),
        ]
        clusters = assign_clusters(episodes)
        self.assertEqual([len(c) for c in clusters], [2, 1])
        ids = cluster_ids("180101", episodes)
        self.assertEqual(ids["180101-20260102-20260103"], "180101-c01")
        self.assertEqual(ids["180101-20260105-20260105"], "180101-c01")
        self.assertEqual(ids["180101-20260108-20260109"], "180101-c02")

    def test_non_ar_pseudo_cluster_grouping(self):
        days = [date(2026, 1, d) for d in (1, 4, 6, 7, 11)]
        # 1 -> 4 is a 3-day break (split); 4 -> 6 is only 2 days but crosses
        # the epoch-aligned 10-day block boundary (2026-01-05|06, blocks 36|37
        # from the 2025-01-01 epoch) so it splits too (2026-07-20b amendment);
        # 6 -> 7 merges; 7 -> 11 is a 4-day break (split).
        clusters = group_pseudo_clusters(days)
        self.assertEqual(
            [[d.day for d in cluster] for cluster in clusters],
            [[1], [4], [6, 7], [11]])

    def test_pseudo_clusters_chop_long_runs_at_aligned_boundaries(self):
        # A continuous 21-day run must not form one resampling unit: it chops
        # into epoch-aligned 10-day blocks, and alignment is global — a run
        # starting mid-block shares the same boundaries (so cross-basin
        # range-merging cannot re-chain adjacent blocks).
        run_a = [date(2026, 1, 6) + (date(2026, 1, 7) - date(2026, 1, 6)) * k
                 for k in range(21)]                       # 01-06 .. 01-26
        clusters = group_pseudo_clusters(run_a)
        self.assertEqual([ (c[0].isoformat(), c[-1].isoformat()) for c in clusters],
                         [("2026-01-06", "2026-01-15"),
                          ("2026-01-16", "2026-01-25"),
                          ("2026-01-26", "2026-01-26")])
        run_b = run_a[4:]                                  # starts mid-block
        self.assertEqual(
            [c[-1] for c in group_pseudo_clusters(run_b)][:2],
            [date(2026, 1, 15), date(2026, 1, 25)])

    def test_scope_clusters_intersect_and_drop_empty(self):
        episodes = [
            _episode("180101", "2026-01-02", "2026-01-03"),
            _episode("180101", "2026-01-10", "2026-01-11"),
        ]
        # Model has no complete date inside the second episode: its cluster
        # is dropped, matching the existing zero-overlap skip behavior.
        complete = [date(2026, 1, d) for d in (1, 2, 3, 4, 5, 6)]
        scopes = build_scope_clusters(episodes, complete)
        self.assertEqual([[d.day for d in c] for c in scopes["ar_all"]], [[2, 3]])
        # [4, 5] | [6] split at the 2026-01-05|06 block boundary (2026-07-20b).
        self.assertEqual([[d.day for d in c] for c in scopes["non_ar"]],
                         [[1], [4, 5], [6]])
        # all_days (2026-07-18 amendment): both kinds coexist unchanged and
        # every complete date lands in exactly one cluster.
        self.assertEqual([[d.day for d in c] for c in scopes["all_days"]],
                         [[2, 3], [1], [4, 5], [6]])

    def test_seed_inputs_are_model_independent(self):
        # Same catalog + same complete-date set: identical digest, hence
        # identical bootstrap draws for any two models via the shared seed.
        episodes = [_episode("180101", "2026-01-02", "2026-01-03")]
        complete = [date(2026, 1, d) for d in range(1, 8)]
        digest_model_a = episode_set_digest(
            build_scope_clusters(episodes, complete)["ar_all"])
        digest_model_b = episode_set_digest(
            build_scope_clusters(episodes, list(complete))["ar_all"])
        self.assertEqual(digest_model_a, digest_model_b)
        seed_key = f"scope:R:180101:ar_all:{digest_model_a}"
        np.testing.assert_array_equal(
            cluster_draws(4, 100, seed_key), cluster_draws(4, 100, seed_key))


class EpisodeStageSchemaTests(unittest.TestCase):
    """End-to-end run_episode_stage + run_compare_stage on a synthetic basin."""

    HUC = "180101"

    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)

    def tearDown(self):
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def _write_history(self, model, scale=1.0):
        rng = np.random.default_rng(11)
        n_dates, n_leads = 20, 3
        dates = np.array([20260101 + d for d in range(n_dates)], dtype=np.int64)
        observed = rng.gamma(2.0, 2.0, size=(n_dates, 1)).astype(np.float32)
        noise = rng.normal(0.0, 1.0, size=(n_dates, n_leads, 1))
        # Error grows with lead so the skill gain is positive on average.
        forecasts = (observed[:, None, :]
                     + scale * noise * np.arange(1, n_leads + 1)[None, :, None]
                     ).astype(np.float32)
        root = Path("stats_output") / model
        root.mkdir(parents=True)
        np.savez(
            root / BASIN_HISTORY_FILENAME,
            schema_version=np.int64(2),
            huc2=np.array("18"),
            hucs=np.array([self.HUC]),
            names=np.array(["Test basin"]),
            dates=dates,
            lead_min=np.int64(1),
            lead_max=np.int64(3),
            forecasts=forecasts,
            observed=observed,
            lock_gap=rng.choice([-1.0, 0.0, np.nan], size=(n_dates, 1)),
            lock_in_fraction=np.float64(0.75),
        )

    def _catalog(self):
        return {self.HUC: [
            _episode(self.HUC, "2026-01-02", "2026-01-03"),
            _episode(self.HUC, "2026-01-05", "2026-01-05"),   # merges with above
            _episode(self.HUC, "2026-01-12", "2026-01-13"),
        ]}

    def test_v5_schema_and_comparison_artifact(self):
        for model in ("modela", "modelb"):
            self._write_history(model, scale=1.0 if model == "modela" else 1.5)
            run_episode_stage(model, self._catalog(), force=True)

        payload = json.loads(
            (Path("stats_output") / "modela" / "episodes.json").read_text())
        self.assertEqual(payload["schema_version"], 7)
        self.assertEqual(payload["analysis"]["cluster_merge_hours"], 72)
        self.assertEqual(payload["analysis"]["pseudo_cluster_block_days"], 10)
        self.assertEqual(payload["analysis"]["pseudo_cluster_epoch"], "2025-01-01")
        self.assertEqual(payload["analysis"]["bootstrap_samples"], 2000)
        # Synthetic model key: no registry entry, so no window filtering.
        self.assertEqual(payload["version_window"]["primary"], None)
        basin = payload["basins"][self.HUC]

        episodes = basin["episodes"]
        self.assertEqual([e["cluster_id"] for e in episodes],
                         ["180101-c01", "180101-c01", "180101-c02"])
        for entry in episodes:
            self.assertNotIn("value_ci", entry["revision_efficiency"])
            self.assertNotIn("value_ci", entry["lock_in_gap"])
            self.assertTrue(entry["revision_efficiency"]["single_event"])
            self.assertIn("daily_gap_min", entry["lock_in_gap"])
            self.assertIn("comparison", entry["revision_efficiency"])

        scopes = basin["scopes"]
        self.assertEqual(set(scopes), {"ar_all", "non_ar", "all_days"})
        ar_all = scopes["ar_all"]
        self.assertEqual(ar_all["dates"], 5)      # 2 + 1 + 2 episode days
        self.assertEqual(ar_all["clusters"], 2)
        self.assertEqual(ar_all["date_range"], ["2026-01-02", "2026-01-13"])
        self.assertEqual(ar_all["revision_efficiency"]["units"], "ratio")
        self.assertIn("value_episode_weighted", ar_all["revision_efficiency"])
        self.assertIn("interval_suppressed", ar_all["revision_efficiency"])
        non_ar = scopes["non_ar"]
        self.assertEqual(non_ar["dates"], 15)
        # [1], [4], [6..11], [14,15], [16..20] — gap splits plus the
        # 01-05|06 and 01-15|16 aligned block boundaries (2026-07-20b).
        self.assertEqual(non_ar["clusters"], 5)
        all_days = scopes["all_days"]
        self.assertEqual(all_days["dates"], 20)   # every complete date, once
        self.assertEqual(all_days["clusters"], 7)  # 2 episode + 5 pseudo

        # CA-pooled block: single basin, so pairs == dates. The cross-basin
        # merge is range-based, so the episode cluster (range 01-02..01-05)
        # and the pseudo-cluster [4] join into one resampling cluster:
        # 6, not the per-basin scope's 7.
        ca = payload["california_pooled"]
        self.assertEqual(ca["hucs"], [self.HUC])
        self.assertEqual(ca["scopes"]["all_days"]["pairs"], 20)
        self.assertEqual(ca["scopes"]["all_days"]["clusters"], 6)
        self.assertIn("value_ci", ca["scopes"]["ar_all"]["revision_efficiency"])

        output = run_compare_stage(["modela", "modelb"])
        self.assertIsNotNone(output)
        comparison = json.loads(output.read_text())
        self.assertEqual(comparison["schema_version"], 3)
        self.assertEqual(comparison["scopes"], ["ar_all", "non_ar", "all_days"])
        self.assertIn("modela", comparison["windows"])
        rows = {(r["model_a"], r["model_b"], r["huc"], r["scope"],
                 r["lead_span"]["matched"]): r for r in comparison["pairs"]}
        row = rows[("modela", "modelb", self.HUC, "ar_all", False)]
        self.assertEqual(row["intersected_dates"], 5)
        self.assertEqual(row["clusters"], 2)
        for key in ("delta_r", "delta_r_ci", "p_direction", "dropped_replicate_fraction"):
            self.assertIn(key, row)
        # Matched-lead-span variant: identical native bands here, so the
        # matched rows must reproduce the native values under the common band.
        matched = rows[("modela", "modelb", self.HUC, "ar_all", True)]
        self.assertEqual(matched["lead_span"], {"matched": True, "lead_min": 1,
                                                "lead_max": 3})
        self.assertEqual(matched["delta_r"], row["delta_r"])
        # CA-pooled ΔR rows exist for every scope and variant.
        ca_row = rows[("modela", "modelb", "CA", "all_days", True)]
        self.assertEqual(ca_row["intersected_dates"], 20)

    def test_compare_stage_skips_with_single_model(self):
        self._write_history("modela")
        run_episode_stage("modela", self._catalog(), force=True)
        self.assertIsNone(run_compare_stage(["modela"]))
        self.assertIsNone(run_compare_stage(["modela", "missing"]))


if __name__ == "__main__":
    unittest.main()
