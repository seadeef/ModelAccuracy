"""Local tARget v4 extension: writer round-trip, discovery, and coverage."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from atmospheric_rivers import paths
from atmospheric_rivers.dataset import iter_normalized_frames
from atmospheric_rivers.tarv4.extend import write_extension


def _write_sample_extension(out_dir: Path):
    steps = [np.datetime64("2026-01-01T00", "h"),
             np.datetime64("2026-01-01T06", "h")]
    lats = 55.0 - 0.25 * np.arange(8)          # descending, like the store
    lons = 230.0 + 0.25 * np.arange(10)
    masks = np.zeros((2, lats.size, lons.size), dtype=np.uint8)
    masks[0, 2:5, 3:9] = 1
    masks[1, 0:2, 0:4] = 1
    provenance = {"kind": "local_tarv4_extension", "canonical": False,
                  "start": "2026-01-01", "end": "2026-01-01"}
    path = write_extension(steps, masks, lats, lons,
                           provenance=provenance, out_dir=out_dir)
    return path, steps, masks, lats, lons


class WriteExtensionRoundTrip(unittest.TestCase):
    def test_frames_survive_normalization(self):
        with TemporaryDirectory() as tmp:
            path, steps, masks, lats, lons = _write_sample_extension(Path(tmp))
            frames = list(iter_normalized_frames([path]))
            self.assertEqual(len(frames), 2)
            self.assertEqual(frames[0].timestamp, "2026-01-01T00:00:00Z")
            self.assertEqual(frames[1].timestamp, "2026-01-01T06:00:00Z")
            for position, frame in enumerate(frames):
                # normalize_grid maps lons into [-180, 180) and sorts; our
                # 230E..232.25E block becomes -130.. and keeps its order.
                self.assertTrue((np.diff(frame.lons) > 0).all())
                self.assertTrue((np.diff(frame.lats) < 0).all())
                np.testing.assert_array_equal(
                    frame.mask, masks[position].astype(bool))

    def test_bounds_slice(self):
        with TemporaryDirectory() as tmp:
            path, _, masks, lats, lons = _write_sample_extension(Path(tmp))
            frames = list(iter_normalized_frames(
                [path], bounds=(-130.0, 53.0, -128.5, 55.0)))
            self.assertEqual(len(frames), 2)
            self.assertLess(frames[0].mask.shape[1], lons.size)


class DiscoveryAndCoverage(unittest.TestCase):
    def test_extension_discovered_and_labeled(self):
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            original = paths.TARGET_AR_DATA_DIR
            paths.TARGET_AR_DATA_DIR = tmp_path
            try:
                self.assertEqual(paths.discover_target_ar_sources(), [])
                path, *_ = _write_sample_extension(tmp_path)
                sources = paths.discover_target_ar_sources()
                self.assertEqual([s.name for s in sources],
                                 [paths.TARGET_AR_EXTENSION_FILENAME])
                meta = paths.target_ar_extension_meta()
                self.assertEqual(meta["kind"], "local_tarv4_extension")

                from stats_pipeline.episodes import ar_catalog_coverage
                coverage = ar_catalog_coverage()
                self.assertEqual(len(coverage), 1)
                self.assertEqual(coverage[0]["source"], "local_tarv4_extension")
                self.assertIs(coverage[0]["canonical"], False)
                self.assertEqual(coverage[0]["start"], "2026-01-01")
            finally:
                paths.TARGET_AR_DATA_DIR = original

    def test_missing_sidecar_not_discovered(self):
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            original = paths.TARGET_AR_DATA_DIR
            paths.TARGET_AR_DATA_DIR = tmp_path
            try:
                _write_sample_extension(tmp_path)
                (tmp_path / paths.TARGET_AR_EXTENSION_META_FILENAME).unlink()
                self.assertEqual(paths.discover_target_ar_sources(), [])
            finally:
                paths.TARGET_AR_DATA_DIR = original


class DetectionCriteria(unittest.TestCase):
    """Synthetic-field behavior of the detection port."""

    @classmethod
    def setUpClass(cls):
        from atmospheric_rivers.tarv4.detect import build_grid
        cls.grid = build_grid()

    def _fields(self):
        grid = self.grid
        lat2d, lon2d = grid.lat2d, grid.lon2d
        t = np.clip(((lon2d - 190) + (lat2d - 25) * 2.25) / 90.0, 0, 1)
        d2 = (((lat2d - (25 + 20 * t)) * 111e3) ** 2
              + ((lon2d - (190 + 45 * t)) * 111e3
                 * np.cos(np.radians(lat2d))) ** 2)
        band = 600 * np.exp(-d2 / (2 * 300e3 ** 2))
        d2b = (((lat2d - 55) * 111e3) ** 2
               + ((lon2d - 280) * 111e3 * np.cos(np.radians(lat2d))) ** 2)
        blob = 800 * np.exp(-d2b / (2 * 150e3 ** 2))
        ivtx = (band * 0.8 + blob * 0.7).astype(np.float32)
        ivty = (band * 0.55 + blob * 0.7).astype(np.float32)
        return ivtx, ivty

    def test_band_survives_blob_rejected(self):
        from atmospheric_rivers.tarv4.detect import detect_step

        class Uniform:
            levels = 5
            def level_field(self, level, month):
                shape = self_outer.grid.lat2d.shape
                return np.full(shape, 250.0 + 12.5 * level)

        self_outer = self
        ivtx, ivty = self._fields()
        union, _ = detect_step(ivtx, ivty, 1, Uniform(), self.grid)
        lat2d, lon2d = self.grid.lat2d, self.grid.lon2d
        mid = (np.abs(lat2d - 35) < 0.5) & (np.abs(lon2d - 212.5) < 0.5)
        at_blob = (np.abs(lat2d - 55) < 0.75) & (np.abs(lon2d - 280) < 0.75)
        self.assertTrue(union[mid].any(), "elongated AR band must survive")
        self.assertFalse(union[at_blob].any(),
                         "compact blob must fail geometry criteria")


if __name__ == "__main__":
    unittest.main()
