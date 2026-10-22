"""Basin AR-episode / pooled-scope API + grid-pooled revision efficiency.

Fixtures follow CONTRACT §3 (episodes.json v3), §5 (revision/convergence
packed planes), and §6 (/api surface): synthetic catalogs and hand-checkable
bins on a 2x2 grid, exercised through the same service entry points the
FastAPI routes call.
"""

import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

from backend.static_store import LocalStaticStore
from backend.request_models import (
    ArEpisodeContext,
    RevisionEfficiencyRequest,
    StatsQueryRequest,
)
from backend.stats_query import episode_catalog_for_huc
from backend.stats_service import (
    query_episode_catalog_payload,
    query_revision_efficiency_payload,
    query_stats_payload,
)
from statistics_plugins.registry import EPISODE_CATALOG_FILENAME

HUC = "180101"
EPISODE_ID = f"{HUC}-20260101-20260103"
# Rectangle covering the whole 2x2 fixture grid.
RECT_REGION = {"type": "rectangle", "bounds": [-121.0, 39.0, -119.0, 41.0]}


def _episode() -> dict:
    """One v3 per-episode entry (point estimates only — no CI keys)."""
    return {
        "id": EPISODE_ID,
        "start_date": "2026-01-01",
        "end_date": "2026-01-03",
        "active_days": 3,
        "peak_basin_area_fraction": 0.5,
        "forecast_dates": 3,
        "cluster_id": f"{HUC}-c01",
        "revision_efficiency": {
            "value": 1.1, "raw_value": 1.1, "dates": 3, "single_event": True,
            "skill_gain": 2.0, "skill_gain_nonpositive": False, "units": "ratio",
        },
        "lock_in_gap": {
            "value": 2.0, "dates": 3, "single_event": True,
            "daily_gap_min": 1.0, "daily_gap_max": 3.0, "units": "days",
        },
    }


def _scope_block(value: float) -> dict:
    """One v3 pooled scope object (CONTRACT §3)."""
    return {
        "dates": 42,
        "clusters": 5,
        "date_range": ["2025-11-01", "2026-03-20"],
        "revision_efficiency": {
            "value": value, "value_ci": [round(value - 0.1, 2), round(value + 0.1, 2)],
            "value_episode_weighted": value, "dates": 42, "clusters": 5,
            "dropped_replicate_fraction": 0.0, "interval_suppressed": False,
            "skill_gain": 3.1, "skill_gain_ci": [2.0, 4.0],
            "significant_skill_gain": True, "units": "ratio",
        },
        "lock_in_gap": {
            "value": 1.5, "value_ci": [1.0, 2.0], "dates": 42, "clusters": 5,
            "units": "days",
        },
    }


def _catalog_v3(*, with_scopes: bool = True) -> dict:
    basin = {"name": "Test basin", "episode_count": 1, "episodes": [_episode()]}
    if with_scopes:
        basin["scopes"] = {
            "ar_all": _scope_block(0.62),
            "non_ar": _scope_block(0.31),
        }
    return {
        "schema_version": 3,
        "analysis": {
            "bootstrap_samples": 2000, "confidence": 0.95,
            "cluster_merge_hours": 72, "resampling_unit": "episode_cluster",
            "weighting_primary": "date", "weighting_robustness": "episode",
            "seed_policy": "basin+episode-set+statistic (model-independent)",
        },
        "lead_min": 1,
        "lead_max": 3,
        "lock_in_fraction": 0.75,
        "basins": {HUC: basin},
    }


def _catalog_v2() -> dict:
    """Pre-v3 catalog: no scopes block, no cluster_id."""
    episode = _episode()
    del episode["cluster_id"]
    return {
        "schema_version": 2,
        "lead_min": 1,
        "lead_max": 3,
        "lock_in_fraction": 0.75,
        "basins": {HUC: {
            "name": "Test basin", "episode_count": 1, "episodes": [episode],
        }},
    }


def _write_catalog(root: Path, payload: dict, model: str = "gfs",
                   filename: str = EPISODE_CATALOG_FILENAME) -> None:
    target = root / "model_data" / model / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload))


def _write_packed(path: Path, num: list[float], den: list[float]) -> None:
    """Two float32 planes, same packing convention as the exporter."""
    arr = np.stack([
        np.asarray(num, dtype=np.float32),
        np.asarray(den, dtype=np.float32),
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(arr.tobytes())


def _write_revision_fixture(root: Path, model: str = "gfs") -> None:
    """2x2 grid + revision/convergence bins with hand-checkable pooling.

    Yearly (leads 1..3 → pairs 1, 2 — CONTRACT §5):
      numer = Σ_pairs Σ(rev_sse)/Σ(rev_cnt) = 4/4 + 8/4 = 3.0
      denom = MSE(3) − MSE(1) = 28/4 − 4/4 = 6.0     →  R = 0.5
    Seasonal djf: same revision planes, but MSE(3) == MSE(1) → denom 0
    (skill_gain_nonpositive). Season jja has no bins at all (no_data).
    """
    model_dir = root / "model_data" / model
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "grid.json").write_text(json.dumps({
        "nLat": 2, "nLon": 2,
        "lats": [40.0, 40.25], "lons": [-120.0, -119.75],
    }))
    (model_dir / "consistency_meta.json").write_text(json.dumps({
        "date_counts": {"yearly": 123, "seasonal": {"djf": 30}, "monthly": {}},
        "lead_days_min": 1,
        "lead_days_max": 3,
    }))
    ones = [1.0, 1.0, 1.0, 1.0]
    _write_packed(model_dir / "revision" / "pair_1.bin", ones, ones)
    _write_packed(model_dir / "revision" / "pair_2.bin", [2.0] * 4, ones)
    _write_packed(model_dir / "convergence" / "lead_1.bin", ones, ones)
    _write_packed(model_dir / "convergence" / "lead_3.bin", [7.0] * 4, ones)
    djf = model_dir / "revision" / "seasonal" / "djf"
    _write_packed(djf / "pair_1.bin", ones, ones)
    _write_packed(djf / "pair_2.bin", [2.0] * 4, ones)
    conv_djf = model_dir / "convergence" / "seasonal" / "djf"
    _write_packed(conv_djf / "lead_1.bin", [7.0] * 4, ones)
    _write_packed(conv_djf / "lead_3.bin", [7.0] * 4, ones)


class BasinEpisodeApiTests(unittest.TestCase):
    def test_catalog_withholds_statistics_until_episode_selected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_catalog(root, _catalog_v3())
            store = LocalStaticStore(root)
            catalog = episode_catalog_for_huc(store, "gfs", HUC)
            selected = query_stats_payload(
                StatsQueryRequest(
                    model="gfs",
                    context={"type": "ar_episode", "huc": HUC, "episodeId": EPISODE_ID},
                    statistics=["revision_efficiency", "lock_in_gap"],
                ),
                store=store,
                now_month=1,
            )
        self.assertNotIn("revision_efficiency", json.dumps(catalog))
        self.assertNotIn("lock_in_gap", json.dumps(catalog))
        stats = selected["stats"]
        self.assertEqual(stats["revision_efficiency"]["value"], 1.1)
        self.assertFalse(stats["revision_efficiency"]["no_data"])
        # v3 per-episode entries carry no CI keys — pass-through, not fabricated.
        self.assertNotIn("value_ci", stats["revision_efficiency"])
        self.assertTrue(stats["revision_efficiency"]["single_event"])

    def test_scope_statistics_happy_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_catalog(root, _catalog_v3())
            store = LocalStaticStore(root)
            response = query_stats_payload(
                StatsQueryRequest(
                    model="gfs",
                    context={"type": "ar_episode", "huc": HUC, "scope": "ar_all"},
                    statistics=["revision_efficiency", "lock_in_gap"],
                ),
                store=store,
                now_month=1,
            )
        self.assertEqual(response["context"]["scope"], "ar_all")
        self.assertNotIn("episodeId", response["context"])
        rev = response["stats"]["revision_efficiency"]
        self.assertEqual(rev["value"], 0.62)
        self.assertEqual(rev["value_ci"], [0.52, 0.72])
        self.assertEqual(rev["clusters"], 5)
        self.assertEqual(rev["units"], "ratio")
        self.assertFalse(rev["no_data"])
        gap = response["stats"]["lock_in_gap"]
        self.assertEqual(gap["value_ci"], [1.0, 2.0])
        self.assertEqual(gap["units"], "days")

    def test_context_requires_exactly_one_of_episode_or_scope(self):
        with self.assertRaises(ValueError):
            ArEpisodeContext(huc=HUC, episodeId=EPISODE_ID, scope="ar_all")
        with self.assertRaises(ValueError):
            ArEpisodeContext(huc=HUC)
        with self.assertRaises(ValueError):
            ArEpisodeContext(huc=HUC, scope="everything")
        # Statistics whitelist comes from EPISODE_CONTEXT_STATS, not spec.scope.
        with self.assertRaises(ValueError):
            StatsQueryRequest(
                model="gfs",
                context={"type": "ar_episode", "huc": HUC, "scope": "ar_all"},
                statistics=["nrmse"],
            )

    def test_v2_catalog_scope_query_maps_to_404_detail(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_catalog(root, _catalog_v2())
            store = LocalStaticStore(root)
            response = query_stats_payload(
                StatsQueryRequest(
                    model="gfs",
                    context={"type": "ar_episode", "huc": HUC, "scope": "ar_all"},
                    statistics=["revision_efficiency"],
                ),
                store=store,
                now_month=1,
            )
        self.assertEqual(response.status_code, 404)
        body = json.loads(response.body)
        self.assertEqual(
            body["error"],
            "pooled AR scopes not computed; rerun compute_stats.py --only episodes",
        )

    def test_catalog_with_huc_has_cluster_id_and_scopes_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_catalog(root, _catalog_v3())
            catalog = query_episode_catalog_payload(
                "gfs", HUC, store=LocalStaticStore(root),
            )
        self.assertEqual(catalog["scopes_available"], ["ar_all", "non_ar"])
        self.assertEqual(catalog["episodes"][0]["cluster_id"], f"{HUC}-c01")
        self.assertEqual(catalog["episode_count"], 1)

    def test_catalog_v2_reports_no_scopes_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_catalog(root, _catalog_v2())
            catalog = query_episode_catalog_payload(
                "gfs", HUC, store=LocalStaticStore(root),
            )
        self.assertEqual(catalog["scopes_available"], [])
        self.assertIsNone(catalog["episodes"][0]["cluster_id"])

    def test_catalog_without_huc_lists_all_basins(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_catalog(root, _catalog_v3())
            listing = query_episode_catalog_payload(
                "gfs", store=LocalStaticStore(root),
            )
        self.assertEqual(listing["model"], "gfs")
        self.assertEqual(listing["huc_level"], 6)
        self.assertEqual(len(listing["basins"]), 1)
        basin = listing["basins"][0]
        self.assertEqual(basin["huc"], HUC)
        self.assertEqual(basin["name"], "Test basin")
        self.assertEqual(basin["episode_count"], 1)
        self.assertEqual(basin["episodes"][0]["id"], EPISODE_ID)
        self.assertEqual(basin["episodes"][0]["cluster_id"], f"{HUC}-c01")
        # Metadata only — never statistics values.
        self.assertNotIn("revision_efficiency", json.dumps(listing))

    def test_client_ar_geometry_is_rejected(self):
        with self.assertRaises(ValueError):
            StatsQueryRequest(
                model="gfs", lead=1,
                region={"type": "ar", "geometry": {"type": "Polygon", "coordinates": []}},
            )


class RevisionEfficiencyEndpointTests(unittest.TestCase):
    def test_grid_pooled_value_math(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_revision_fixture(root)
            response = query_revision_efficiency_payload(
                RevisionEfficiencyRequest(model="gfs", region=RECT_REGION),
                store=LocalStaticStore(root),
                now_month=1,
            )
        # numer = 4/4 + 8/4 = 3; denom = 7 − 1 = 6 (see _write_revision_fixture).
        self.assertEqual(response["model"], "gfs")
        self.assertEqual(response["period"], "yearly")
        self.assertAlmostEqual(response["value"], 0.5)
        self.assertFalse(response["no_data"])
        self.assertEqual(response["units"], "ratio")
        self.assertEqual(response["dates"], 123)
        self.assertEqual(response["definition"], "grid-pooled")
        self.assertNotIn("skill_gain_nonpositive", response)

    def test_monthly_period_is_rejected(self):
        with self.assertRaises(ValueError):
            RevisionEfficiencyRequest(
                model="gfs", region=RECT_REGION, period="monthly",
            )

    def test_missing_bins_are_explicit_no_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_revision_fixture(root)
            response = query_revision_efficiency_payload(
                RevisionEfficiencyRequest(
                    model="gfs", region=RECT_REGION,
                    period="seasonal", season="jja",
                ),
                store=LocalStaticStore(root),
                now_month=7,
            )
        self.assertEqual(response["season"], "jja")
        self.assertIsNone(response["value"])
        self.assertTrue(response["no_data"])
        self.assertNotIn("skill_gain_nonpositive", response)

    def test_nonpositive_skill_gain_yields_null_with_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_revision_fixture(root)
            response = query_revision_efficiency_payload(
                RevisionEfficiencyRequest(
                    model="gfs", region=RECT_REGION,
                    period="seasonal", season="djf",
                ),
                store=LocalStaticStore(root),
                now_month=1,
            )
        self.assertIsNone(response["value"])
        self.assertTrue(response["no_data"])
        self.assertTrue(response["skill_gain_nonpositive"])
        self.assertEqual(response["dates"], 30)


if __name__ == "__main__":
    unittest.main()


class RegionalCatalogDispatchTests(unittest.TestCase):
    """Episode catalogs are per HUC2 region: ``episodes.json`` for California,
    ``episodes_huc{NN}.json`` elsewhere, selected by the HUC6 prefix."""

    def test_filename_rule(self):
        from backend.stats_query import episode_catalog_filename, huc2_of
        self.assertEqual(episode_catalog_filename("18"), EPISODE_CATALOG_FILENAME)
        self.assertEqual(episode_catalog_filename("17"), "episodes_huc17.json")
        self.assertEqual(huc2_of("170101"), "17")
        self.assertEqual(huc2_of(None), "18")

    def test_pnw_basin_reads_regional_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_catalog(root, _catalog_v3())  # California only
            pnw = _catalog_v3()
            pnw_huc = "170101"
            basin = pnw["basins"].pop(HUC)
            basin["name"] = "Kootenai"
            for ep in basin["episodes"]:
                ep["id"] = ep["id"].replace(HUC, pnw_huc)
                ep["cluster_id"] = ep["cluster_id"].replace(HUC, pnw_huc)
            pnw["basins"][pnw_huc] = basin
            _write_catalog(root, pnw, filename="episodes_huc17.json")
            store = LocalStaticStore(root)
            ca = query_episode_catalog_payload("gfs", HUC, store=store)
            self.assertEqual(ca["name"], "Test basin")
            regional = query_episode_catalog_payload("gfs", pnw_huc, store=store)
            self.assertEqual(regional["name"], "Kootenai")
            self.assertEqual(regional["scopes_available"], ["ar_all", "non_ar"])
            listing = query_episode_catalog_payload("gfs", None, huc2="17", store=store)
            self.assertEqual(listing["huc2"], "17")
            self.assertEqual([b["huc"] for b in listing["basins"]], [pnw_huc])
            # Scope statistics resolve through the same regional file.
            req = StatsQueryRequest(
                model="gfs", statistics=["revision_efficiency"],
                context=ArEpisodeContext(huc=pnw_huc, scope="ar_all"),
            )
            payload = query_stats_payload(req, store=store, now_month=1)
            self.assertAlmostEqual(payload["stats"]["revision_efficiency"]["value"], 0.62)
