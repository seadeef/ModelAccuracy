"""On-demand regional series from the per-cell cube (``backend.series_query``)
and the sub-cell mask fallback in ``backend.stats_query``.

A 2×2 grid with three land cells (the north-east cell is water) and a
hand-built cube: forecasts are ``cell + lead/10 + date`` so every pooled
value is checkable by hand.
"""

import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

from backend.static_store import LocalStaticStore
from unittest import mock

from backend.series_query import (
    basins_for_region,
    get_cube,
    region_rows,
    region_series_id,
    series_for_region,
    series_for_region_all_models,
)
from backend.stats_query import _build_region_mask, _get_grid

MODEL = "gfs"
LATS = [39.5, 40.5]          # rows (south → north)
LONS = [-120.5, -119.5]      # cols (west → east)
DATES = ["20260101", "20260102", "20260103"]
LEAD_MIN, LEAD_MAX = 1, 2
# Flat index = row * n_lon + col. Land: (0,0)=0, (0,1)=1, (1,0)=2; water: (1,1)=3.
CELLS = np.array([0, 1, 2], dtype=np.int32)


def _write_cube(root: Path) -> None:
    (root / "model_data" / MODEL).mkdir(parents=True)
    (root / "model_data" / MODEL / "grid.json").write_text(json.dumps(
        {"nLat": 2, "nLon": 2, "lats": LATS, "lons": LONS}))
    cube = root / "cube" / MODEL
    cube.mkdir(parents=True)
    n_leads = LEAD_MAX - LEAD_MIN + 1
    fc = np.zeros((CELLS.size, len(DATES), n_leads), dtype=np.float32)
    ob = np.zeros((CELLS.size, len(DATES)), dtype=np.float32)
    for ci, cell in enumerate(CELLS):
        for di in range(len(DATES)):
            ob[ci, di] = 10 * cell + di
            for li in range(n_leads):
                fc[ci, di, li] = cell + (LEAD_MIN + li) / 10 + di
    # One missing forecast: cell 2, date 0, lead 1 → the pooled mean skips it.
    fc[2, 0, 0] = np.nan
    np.save(cube / "cells.npy", CELLS)
    np.save(cube / "forecasts.npy", fc.astype(np.float16))
    np.save(cube / "observed.npy", ob.astype(np.float16))
    (cube / "meta.json").write_text(json.dumps({
        "schema_version": 1, "model": MODEL, "dates": DATES,
        "lead_min": LEAD_MIN, "lead_max": LEAD_MAX, "n_lat": 2, "n_lon": 2,
        "n_cells": int(CELLS.size), "dtype": "float16",
        "primary_window": "v1", "primary_valid_range": ["20260101", None],
        "weighting": "equal",
    }))


class SeriesQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        _write_cube(self.root)
        self.store = LocalStaticStore(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_cube_loads_and_matches_grid(self) -> None:
        cube = get_cube(self.store, MODEL)
        self.assertIsNotNone(cube)
        self.assertEqual(cube.forecasts.shape, (3, 3, 2))
        self.assertEqual(cube.cell_pos.tolist(), [0, 1, 2, -1])
        self.assertIsNone(get_cube(self.store, "nbm"))  # no cube exported

    def test_whole_grid_rectangle_pools_land_cells_equally(self) -> None:
        region = {"type": "rectangle", "bounds": [-121.0, 39.0, -119.0, 41.0]}
        payload = series_for_region(self.store, MODEL, region)
        self.assertEqual(payload["cells"], 3)
        self.assertFalse(payload["nearest_cell"])
        self.assertEqual(payload["dates"], DATES)
        self.assertEqual((payload["lead_min"], payload["lead_max"]), (1, 2))
        # Observed, date 1: mean of 10*cell + 1 over cells 0,1,2 = (1 + 11 + 21) / 3.
        self.assertAlmostEqual(payload["observed"][1], 11.0, places=2)
        # Forecast lead 2 (index 1), date 0: mean of cell + 0.2 = (0.2 + 1.2 + 2.2) / 3.
        self.assertAlmostEqual(payload["forecasts"][0][1], 1.2, places=2)
        # Lead 1, date 0: cell 2 is NaN → mean of (0.1, 1.1) only.
        self.assertAlmostEqual(payload["forecasts"][0][0], 0.6, places=2)
        self.assertEqual(payload["primary_valid_range"], ["20260101", None])
        self.assertEqual(payload["source"], "cube")

    def test_point_takes_nearest_land_cell(self) -> None:
        # Click on the water cell (1,1): the nearest land cell is one step away.
        payload = series_for_region(self.store, MODEL, {"type": "point", "coordinates": [-119.5, 40.5]})
        self.assertEqual(payload["cells"], 1)
        self.assertTrue(payload["nearest_cell"])
        # Point exactly on land cell 2 (row 1, col 0): observed = 20 + di.
        payload = series_for_region(self.store, MODEL, {"type": "point", "coordinates": [-120.5, 40.5]})
        self.assertEqual(payload["observed"], [20.0, 21.0, 22.0])

    def test_subcell_polygon_falls_back_to_containing_cell(self) -> None:
        # A tiny triangle inside cell 1 (row 0, col 1): no centre or corner inside
        # it, so the stats mask uses the bbox-overlap fallback → one cell.
        tiny = {"type": "polygon", "coordinates": [[-119.6, 39.4], [-119.55, 39.4], [-119.55, 39.45]]}
        lats, lons, n_lat, n_lon = _get_grid(self.store, MODEL)
        mask = _build_region_mask(lats, lons, n_lat, n_lon, tiny)
        self.assertEqual(np.flatnonzero(mask).tolist(), [1])
        payload = series_for_region(self.store, MODEL, tiny)
        self.assertEqual(payload["cells"], 1)
        self.assertFalse(payload["nearest_cell"])
        self.assertEqual(payload["observed"], [10.0, 11.0, 12.0])

    def test_offgrid_region_is_empty(self) -> None:
        far = {"type": "rectangle", "bounds": [-100.0, 20.0, -99.0, 21.0]}
        self.assertIsNone(series_for_region(self.store, MODEL, far))
        cube = get_cube(self.store, MODEL)
        rows, nearest = region_rows(cube, {"type": "point", "coordinates": [-100.0, 20.0]})
        self.assertEqual(rows.size, 0)

    def test_all_models_reports_availability(self) -> None:
        region = {"type": "rectangle", "bounds": [-121.0, 39.0, -119.0, 41.0]}
        result = series_for_region_all_models(self.store, region, [MODEL, "nbm"])
        self.assertEqual(result["available"], [MODEL])
        self.assertIsNotNone(result["models"][MODEL])
        self.assertIsNone(result["models"]["nbm"])

    def test_heavy_rain_pools_cell_days(self) -> None:
        from backend.series_query import heavy_rain_for_region
        region = {"type": "rectangle", "bounds": [-121.0, 39.0, -119.0, 41.0]}
        # Lead 1 (index 0) over 3 cells × 3 dates minus the one NaN forecast = 8 trials.
        # observed = 10*cell + di: ≥ 10 for cells 1 and 2 on every date → 6 events,
        # minus the NaN trial (cell 2, date 0) → 5 heavy trials among 8.
        r = heavy_rain_for_region(self.store, MODEL, region, q=10.0, lead=1)
        self.assertEqual(r["pooling"], "cell_days")
        self.assertEqual((r["cells"], r["dates"], r["cellDays"], r["nObs"]), (3, 3, 8, 5))
        # Trigger 0: every trial is a call → recall 1, precision 5/8.
        self.assertAlmostEqual(r["recall"][0], 1.0)
        self.assertAlmostEqual(r["precision"][0], 5 / 8, places=4)
        # Forecasts = cell + 0.1 + di. Calls at trigger 3 mm: cell 1 on date 2
        # (3.1) and cell 2 on dates 1, 2 (3.1, 4.1; date 0 is NaN) — all heavy
        # → precision 1, recall 3/5. At 2 mm cell 0 on date 2 (2.1, observed 2)
        # joins as a false alarm → precision 4/5.
        at3 = r["triggers"].index(3.0)
        self.assertAlmostEqual(r["precision"][at3], 1.0)
        self.assertAlmostEqual(r["recall"][at3], 3 / 5, places=4)
        at2 = r["triggers"].index(2.0)
        self.assertAlmostEqual(r["precision"][at2], 4 / 5, places=4)
        self.assertIsNotNone(r["point"])
        self.assertEqual(r["point"]["trigger"], 10.0)
        # Date subset: only date index 2 → 3 trials, 2 heavy.
        r2 = heavy_rain_for_region(self.store, MODEL, region, q=10.0, lead=1, dates=["20260103"])
        self.assertEqual((r2["dates"], r2["cellDays"], r2["nObs"]), (1, 3, 2))
        # A lead the model does not have is reported, not invented.
        self.assertTrue(heavy_rain_for_region(self.store, MODEL, region, q=10.0, lead=5)["unavailable"])
        self.assertIsNone(heavy_rain_for_region(self.store, MODEL, {"type": "rectangle", "bounds": [-100.0, 20.0, -99.0, 21.0]}, q=10.0, lead=1))

    def test_basins_for_region_by_shared_cell(self) -> None:
        """A region lists the catalogued HUC6 basins whose mask shares a cell with
        it; HUC regions answer by code; off-grid regions answer nothing."""
        (self.root / "ar").mkdir()
        (self.root / "ar" / "basins.json").write_text(json.dumps(
            {"huc_level": 6, "basins": {"180201": {"huc2": "18"}, "180400": {"huc2": "18"}, "170101": {"huc2": "17"}}}))
        # 180201 = the west column; 180400 = the east column; 170101 = nothing on this grid.
        fake_masks = {
            "180201": np.array([True, False, True, False]),
            "180400": np.array([False, True, False, True]),
            "170101": np.zeros(4, dtype=bool),
        }
        fake_bbox = {"180201": [-121, 39, -120, 41], "180400": [-120, 39, -119, 41], "170101": [-124, 46, -121, 49]}
        with mock.patch("backend.series_query._huc_region_mask", side_effect=lambda la, lo, nla, nlo, h: fake_masks[h]), \
             mock.patch("backend.series_query.wbd_boundaries.get_entry", side_effect=lambda h: {"bbox": fake_bbox[h]}):
            west = basins_for_region(self.store, {"type": "rectangle", "bounds": [-121.0, 39.0, -120.2, 41.0]})
            self.assertEqual(west, ["180201"])
            both = basins_for_region(self.store, {"type": "rectangle", "bounds": [-121.0, 39.0, -119.0, 41.0]})
            self.assertEqual(both, ["180201", "180400"])
            # Sub-cell polygon inside the east column → its containing cell → 180400.
            tiny = basins_for_region(self.store, {"type": "polygon", "coordinates": [[-119.52, 39.48], [-119.48, 39.48], [-119.5, 39.52]]})
            self.assertEqual(tiny, ["180400"])
            far = basins_for_region(self.store, {"type": "rectangle", "bounds": [-100.0, 30.0, -99.0, 31.0]})
            self.assertEqual(far, [])
        self.assertEqual(basins_for_region(self.store, {"type": "huc", "huc": "18"}), ["180201", "180400"])
        self.assertEqual(basins_for_region(self.store, {"type": "huc", "huc": "18020101"}), ["180201"])
        self.assertEqual(basins_for_region(self.store, {"type": "huc", "huc": "1803"}), [])

    def test_series_id_is_stable_per_region(self) -> None:
        self.assertEqual(region_series_id({"type": "huc", "huc": "18"}), "huc:18")
        self.assertEqual(region_series_id({"type": "admin", "level": "state", "fips": "06"}), "admin:state:06")
        a = region_series_id({"type": "polygon", "coordinates": [[-1.0, 2.0], [-1.5, 2.5], [-1.0, 2.5]]})
        b = region_series_id({"type": "polygon", "coordinates": [[-1.0, 2.0], [-1.5, 2.5], [-1.0, 2.5]]})
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
