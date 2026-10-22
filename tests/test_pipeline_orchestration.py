import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from stats_pipeline.consistency import (
    BoxOperator,
    MonthlyAccumulators,
    _load_existing,
    _process_valid_date,
    _write_outputs,
)
from statistics_plugins.consistency import JUMPINESS_SCALES_KM
from stats_pipeline.manifest import atomic_write_json, load_manifest
from stats_pipeline.orchestrator import run_pipeline


class PipelineOrchestrationTests(unittest.TestCase):
    @patch("stats_pipeline.orchestrator.load_manifest")
    @patch("stats_pipeline.orchestrator.run_compare_stage", return_value=None)
    @patch("stats_pipeline.orchestrator.run_consistency_stage")
    @patch("stats_pipeline.orchestrator.run_verification_stage")
    @patch("stats_pipeline.orchestrator._wbd_available", return_value=False)
    @patch("stats_pipeline.orchestrator.discover_target_ar_sources", return_value=[])
    def test_plain_run_executes_core_and_skips_optional(
        self, _sources, _wbd, verification, consistency, compare, _manifest,
    ):
        summary = run_pipeline(models=["gfs"])
        verification.assert_called_once_with("gfs", force=False)
        consistency.assert_called_once_with("gfs", force=False)
        compare.assert_called_once_with(["gfs"])
        self.assertEqual([item.stage for item in summary.skipped],
                         ["basin", "episodes", "compare"])

    @patch("stats_pipeline.orchestrator.load_manifest")
    @patch("stats_pipeline.orchestrator.run_compare_stage")
    @patch("stats_pipeline.orchestrator.run_basin_stage", return_value=Path("history.npz"))
    @patch("stats_pipeline.orchestrator._record_basin")
    @patch("stats_pipeline.orchestrator._wbd_available", return_value=True)
    @patch("stats_pipeline.orchestrator.discover_target_ar_sources", return_value=[])
    def test_only_basin_honors_model_filter(
        self, _sources, _wbd, _record, basin, compare, _manifest,
    ):
        summary = run_pipeline(models=["aifs"], only={"basin"}, force=True)
        basin.assert_called_once_with("aifs", force=True)
        compare.assert_not_called()
        self.assertEqual([(item.model, item.stage) for item in summary.results], [("aifs", "basin")])

    @patch("stats_pipeline.orchestrator.load_manifest")
    @patch("stats_pipeline.orchestrator.run_compare_stage",
           return_value=Path("episode_model_comparison.json"))
    @patch("stats_pipeline.orchestrator.discover_target_ar_sources", return_value=[])
    def test_only_compare_runs_global_pass_once(self, _sources, compare, _manifest):
        summary = run_pipeline(models=["gfs", "aifs"], only={"compare"})
        compare.assert_called_once_with(["gfs", "aifs"])
        self.assertEqual([(item.model, item.stage, item.status) for item in summary.results],
                         [("all", "compare", "completed")])

    def test_manifest_migrates_verification_and_consistency(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "manifest.json").write_text(
                '{"schema_version":2,"storage":"monthly","processed":{"x":[1]}}'
            )
            (root / "consistency_manifest.json").write_text(
                '{"schema_version":1,"processed_valid_dates":["20260101"]}'
            )
            manifest = load_manifest(root)
            self.assertEqual(manifest["schema_version"], 3)
            self.assertIn("verification", manifest["stages"])
            self.assertIn("consistency", manifest["stages"])
            self.assertFalse((root / "consistency_manifest.json").exists())

    def test_atomic_manifest_failure_preserves_previous_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            path.write_text('{"schema_version":3,"stages":{"old":{}}}')
            with patch("stats_pipeline.manifest.os.replace", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    atomic_write_json(path, {"schema_version": 3, "stages": {"new": {}}})
            self.assertIn('"old"', path.read_text())


class RevisionPlaneTests(unittest.TestCase):
    """Revision-plane accumulation (contract §5) on a tiny synthetic grid."""

    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)
        self.lats = np.array([40.0, 40.1])
        self.lons = np.array([-120.0, -119.9])
        # PRISM finite everywhere except (1, 1): that cell is not land.
        self.prism = np.array([[0.5, 0.5], [0.5, np.nan]])
        self.fields = {
            1: np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),
            2: np.array([[4.0, 2.0], [3.0, 4.0]], dtype=np.float32),
            3: np.array([[np.nan, 2.0], [5.0, 4.0]], dtype=np.float32),
        }
        self.lead_paths = {}
        for lead, field in self.fields.items():
            path = Path(f"lead_{lead}.npy")
            np.save(path, field)
            self.lead_paths[lead] = path

    def tearDown(self):
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def _process(self):
        box_ops = [BoxOperator(self.lats, self.lons, n) for n in JUMPINESS_SCALES_KM]
        with (
            patch("stats_pipeline.consistency.read_prism",
                  return_value=(self.prism, None, None)),
            patch("stats_pipeline.consistency.reproject_prism",
                  return_value=self.prism),
            patch("stats_pipeline.consistency.grid_transform", return_value=None),
        ):
            return _process_valid_date(
                self.lead_paths, Path("prism.tif"), self.lats, self.lons,
                box_ops, 1, 3)

    def test_rev_planes_hand_checked(self):
        _, _, rev_sse, rev_cnt, _, _, _, _ = self._process()
        self.assertEqual(rev_sse.shape, (2, 2, 2))
        # Pair 1 (leads 2 vs 1): (1-4)^2 = 9 at (0,0), zero elsewhere on land;
        # (1,1) is not land so it never counts.
        self.assertEqual(rev_sse[0, 0, 0], 9.0)
        self.assertEqual(rev_sse[0, 0, 1], 0.0)
        self.assertEqual(rev_cnt[0].tolist(), [[1, 1], [1, 0]])
        # Pair 2 (leads 3 vs 2): lead-3 NaN at (0,0) excludes that cell;
        # (1,0) revises 3 -> 5 so contributes (3-5)^2 = 4.
        self.assertEqual(rev_cnt[1].tolist(), [[0, 1], [1, 0]])
        self.assertEqual(rev_sse[1, 1, 0], 4.0)
        self.assertEqual(rev_sse[1, 0, 0], 0.0)

    def test_written_planes_round_trip_and_v1_files_force_recompute(self):
        jump_num, jump_den, rev_sse, rev_cnt, conv_sse, conv_cnt, _, _ = self._process()
        acc = MonthlyAccumulators((2, 2), n_pairs=2, n_leads=3)
        acc.fold(1, jump_num, jump_den, rev_sse, rev_cnt, conv_sse, conv_cnt)
        acc.fold(1, jump_num, jump_den, rev_sse, rev_cnt, conv_sse, conv_cnt)
        _write_outputs("testmodel", acc, lead_min=1)

        pair_path = Path("stats_output/testmodel/jumpiness/pair_1.npz")
        with np.load(pair_path) as data:
            np.testing.assert_allclose(data["rev_sse"], 2.0 * rev_sse[0])
            np.testing.assert_array_equal(data["rev_cnt"], 2 * rev_cnt[0])

        loaded = _load_existing("testmodel", (2, 2), 2, 3, 1)
        self.assertIsNotNone(loaded)
        np.testing.assert_allclose(loaded.rev_sse[1], 2.0 * rev_sse)
        np.testing.assert_array_equal(loaded.rev_cnt[1], 2 * rev_cnt)

        # Strip the revision planes from one monthly file (a pre-v2 layout):
        # incremental loading must refuse and demand a full recompute.
        monthly = Path("stats_output/testmodel/jumpiness/monthly/01/pair_1.npz")
        with np.load(monthly) as data:
            legacy = {name: data[name] for name in data.files
                      if name not in ("rev_sse", "rev_cnt")}
        np.savez(monthly, **legacy)
        self.assertIsNone(_load_existing("testmodel", (2, 2), 2, 3, 1))
