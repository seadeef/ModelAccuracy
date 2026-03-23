#!/usr/bin/env python3
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

from backend.static_store import LocalStaticStore, StaticStore, default_static_site_root
from model_registry import MODEL_REGISTRY
from statistics_plugins.registry import STATISTICS_BY_NAME

# ---------------------------------------------------------------------------
# In-process caches — keyed so that S3 round-trips are skipped entirely on
# cache hits.  Dict reads/writes for simple keys are atomic under CPython's
# GIL, which is sufficient for the ThreadPoolExecutor concurrency used below.
# ---------------------------------------------------------------------------
_grid_cache: dict[tuple[str, str], tuple[np.ndarray, np.ndarray, int, int]] = {}
_bin_cache: dict[tuple, np.ndarray] = {}


def _data_bin_relative_path(
    model: str,
    stat_name: str,
    lead_key: str,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
) -> Path:
    effective_period = "yearly" if stat_name == "forecast" else period
    path = Path("data") / model / stat_name
    if effective_period == "monthly" and month is not None:
        path = path / "monthly" / month
    elif effective_period == "seasonal" and season is not None:
        path = path / "seasonal" / season
    return path / f"lead_{lead_key}.bin"


def _get_grid(store: StaticStore, model: str) -> tuple[np.ndarray, np.ndarray, int, int]:
    key = (store.cache_key, model)
    cached = _grid_cache.get(key)
    if cached is not None:
        return cached
    relative_path = str(Path("data") / model / "grid.json")
    grid_text = store.read_text(relative_path)
    payload = json.loads(grid_text)
    lats = np.asarray(payload["lats"], dtype=np.float64)
    lons = np.asarray(payload["lons"], dtype=np.float64)
    n_lat = int(payload.get("nLat", lats.size))
    n_lon = int(payload.get("nLon", lons.size))
    if lats.size != n_lat or lons.size != n_lon:
        raise ValueError(f"Invalid grid.json for model {model}")
    result = (lats, lons, n_lat, n_lon)
    _grid_cache[key] = result
    return result


def _get_bin(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    *,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
) -> np.ndarray:
    cache_key = (store.cache_key, model, stat_name, lead_key, period, month, season)
    cached = _bin_cache.get(cache_key)
    if cached is not None:
        return cached
    relative_path = str(_data_bin_relative_path(
        model, stat_name, lead_key, period=period, month=month, season=season
    ))
    raw_bytes = store.read_bytes(relative_path)
    arr = np.frombuffer(raw_bytes, dtype=np.float32)
    _bin_cache[cache_key] = arr
    return arr


def _read_bin_or_none(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    *,
    period: str,
    month: str | None,
    season: str | None,
) -> np.ndarray | None:
    try:
        return _get_bin(
            store,
            model,
            stat_name,
            lead_key,
            period=period,
            month=month,
            season=season,
        )
    except FileNotFoundError:
        return None


def _bins_for_stats_parallel(
    store: StaticStore,
    model: str,
    lead_key: str,
    stat_names: list[str],
    *,
    period: str,
    month: str | None,
    season: str | None,
) -> dict[str, np.ndarray | None]:
    """Fetch all statistic layers for one lead concurrently (S3 / disk I/O bound)."""
    if not stat_names:
        return {}
    if len(stat_names) == 1:
        sn = stat_names[0]
        return {sn: _read_bin_or_none(store, model, sn, lead_key, period=period, month=month, season=season)}
    max_workers = len(stat_names)
    out: dict[str, np.ndarray | None] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_stat = {
            ex.submit(
                _read_bin_or_none,
                store,
                model,
                sn,
                lead_key,
                period=period,
                month=month,
                season=season,
            ): sn
            for sn in stat_names
        }
        for fut in as_completed(future_to_stat):
            out[future_to_stat[fut]] = fut.result()
    return out


def _point_in_polygon(lon: float, lat: float, ring: list[list[float]]) -> bool:
    if len(ring) < 3:
        return False
    inside = False
    j = len(ring) - 1
    for i, (xi, yi) in enumerate(ring):
        xj, yj = ring[j]
        if (yi > lat) != (yj > lat):
            x_int = xi + ((xj - xi) * (lat - yi)) / (yj - yi)
            if lon < x_int:
                inside = not inside
        j = i
    return inside


def _build_region_mask(
    lats: np.ndarray,
    lons: np.ndarray,
    n_lat: int,
    n_lon: int,
    region: dict,
) -> np.ndarray:
    mask = np.zeros(n_lat * n_lon, dtype=bool)
    region_type = region.get("type")

    if region_type == "rectangle":
        west, south, east, north = region["bounds"]
        lat_sel = (lats >= south) & (lats <= north)
        lon_sel = (lons >= west) & (lons <= east)
        if not np.any(lat_sel) or not np.any(lon_sel):
            return mask
        return np.outer(lat_sel, lon_sel).reshape(-1)

    if region_type == "polygon":
        ring = [[float(p[0]), float(p[1])] for p in region.get("coordinates", [])]
        if len(ring) < 3:
            return mask
        lons_ring = [p[0] for p in ring]
        lats_ring = [p[1] for p in ring]
        min_lon = min(lons_ring)
        max_lon = max(lons_ring)
        min_lat = min(lats_ring)
        max_lat = max(lats_ring)
        for r, lat in enumerate(lats):
            if lat < min_lat or lat > max_lat:
                continue
            base = r * n_lon
            for c, lon in enumerate(lons):
                if lon < min_lon or lon > max_lon:
                    continue
                if _point_in_polygon(float(lon), float(lat), ring):
                    mask[base + c] = True
    return mask


def _no_data_entry(stat_name: str) -> dict[str, float | None | str | bool]:
    plugin = STATISTICS_BY_NAME[stat_name]
    return {"value": None, "units": plugin.spec.units, "no_data": True}


def _masked_mean(values: np.ndarray, mask: np.ndarray) -> float | None:
    masked = values[mask]
    finite = masked[np.isfinite(masked)]
    if finite.size == 0:
        return None
    return float(finite.mean())


def stats_for_region(
    *,
    static_root: Path | None = None,
    store: StaticStore | None = None,
    model: str,
    lead: str | int,
    region: dict,
    stat_names: list[str] | None = None,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
) -> dict[str, dict[str, float | None | str | bool]]:
    lead_key = str(lead)
    stat_names = stat_names or list(STATISTICS_BY_NAME)
    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    lats, lons, n_lat, n_lon = _get_grid(resolved_store, model)
    region_type = region.get("type")
    result: dict[str, dict[str, float | None | str | bool]] = {}

    if region_type == "point":
        lon = float(region["coordinates"][0])
        lat = float(region["coordinates"][1])
        lat_idx = int(np.argmin(np.abs(lats - lat)))
        lon_idx = int(np.argmin(np.abs(lons - lon)))
        flat_idx = lat_idx * n_lon + lon_idx
        bins = _bins_for_stats_parallel(
            resolved_store,
            model,
            lead_key,
            stat_names,
            period=period,
            month=month,
            season=season,
        )
        for stat_name in stat_names:
            values = bins.get(stat_name)
            if values is None:
                result[stat_name] = _no_data_entry(stat_name)
                continue
            if values.size != n_lat * n_lon:
                result[stat_name] = _no_data_entry(stat_name)
                continue
            value = float(values[flat_idx])
            if not np.isfinite(value):
                result[stat_name] = _no_data_entry(stat_name)
                continue
            result[stat_name] = {
                "value": value,
                "units": STATISTICS_BY_NAME[stat_name].spec.units,
                "no_data": False,
            }
        return result

    mask = _build_region_mask(lats, lons, n_lat, n_lon, region)
    if not np.any(mask):
        return {stat_name: _no_data_entry(stat_name) for stat_name in stat_names}

    bins = _bins_for_stats_parallel(
        resolved_store,
        model,
        lead_key,
        stat_names,
        period=period,
        month=month,
        season=season,
    )
    for stat_name in stat_names:
        values = bins.get(stat_name)
        if values is None:
            result[stat_name] = _no_data_entry(stat_name)
            continue
        if values.size != n_lat * n_lon:
            result[stat_name] = _no_data_entry(stat_name)
            continue
        value = _masked_mean(values, mask)
        if value is None:
            result[stat_name] = _no_data_entry(stat_name)
            continue
        result[stat_name] = {
            "value": value,
            "units": STATISTICS_BY_NAME[stat_name].spec.units,
            "no_data": False,
        }

    return result


def _winner_metric_label(stat_name: str) -> str:
    """Same comparison semantics as ``export_static`` domain winners; values are region-aggregated."""
    if stat_name == "bias":
        return "region_mean_abs_value_lower_better"
    if stat_name == "sacc":
        return "region_mean_value_higher_better"
    return "region_mean_value_lower_better"


def _region_loss_scalar(
    values: np.ndarray,
    stat_name: str,
    *,
    mask: np.ndarray | None = None,
    flat_idx: int | None = None,
) -> float | None:
    """
    Scalar loss for winner selection; **lower is better** (SACC uses negated mean).
    Matches ``export_static._scalar_loss_from_npz`` but over a mask or single grid cell.
    """
    if flat_idx is not None:
        v = values[flat_idx]
        if not np.isfinite(v):
            return None
        if stat_name == "bias":
            return float(abs(v))
        if stat_name == "sacc":
            return float(-v)
        return float(v)

    if mask is None or not np.any(mask):
        return None
    masked = values[mask]
    finite = masked[np.isfinite(masked)]
    if finite.size == 0:
        return None
    if stat_name == "bias":
        return float(np.mean(np.abs(finite)))
    if stat_name == "sacc":
        return float(-np.mean(finite))
    return float(np.mean(finite))


def _region_loss_for_model_lead(
    store: StaticStore,
    model: str,
    stat_name: str,
    lead_key: str,
    region: dict,
    *,
    period: str,
    month: str | None,
    season: str | None,
) -> float | None:
    try:
        values = _get_bin(
            store,
            model,
            stat_name,
            lead_key,
            period=period,
            month=month,
            season=season,
        )
    except FileNotFoundError:
        return None

    lats, lons, n_lat, n_lon = _get_grid(store, model)
    if values.size != n_lat * n_lon:
        return None

    region_type = region.get("type")
    if region_type == "point":
        lon = float(region["coordinates"][0])
        lat = float(region["coordinates"][1])
        lat_idx = int(np.argmin(np.abs(lats - lat)))
        lon_idx = int(np.argmin(np.abs(lons - lon)))
        flat_idx = lat_idx * n_lon + lon_idx
        return _region_loss_scalar(values, stat_name, flat_idx=flat_idx)

    mask = _build_region_mask(lats, lons, n_lat, n_lon, region)
    return _region_loss_scalar(values, stat_name, mask=mask)


def lead_winners_for_region(
    *,
    store: StaticStore,
    region: dict,
    stat_name: str,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
    min_lead: int,
    max_lead: int,
) -> dict[str, object]:
    """
    For each lead day, pick the model with best (lowest) loss over ``region``,
    using the same formulas as static ``lead_winners.json`` (mean / mean abs / negated SACC).
    """
    model_order = tuple(sorted(MODEL_REGISTRY.keys()))

    # Prefetch grids so the parallel workers don't all race to download them.
    for mk in model_order:
        _get_grid(store, mk)

    # Fan out every (model, lead) combination in one flat pool.
    lead_range = list(range(min_lead, max_lead + 1))
    tasks: list[tuple[str, str]] = [
        (mk, str(lead)) for lead in lead_range for mk in model_order
    ]
    max_workers = min(len(tasks), 20)
    loss_results: dict[tuple[str, str], float | None] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_key = {
            ex.submit(
                _region_loss_for_model_lead,
                store, mk, stat_name, lead_key, region,
                period=period, month=month, season=season,
            ): (mk, lead_key)
            for mk, lead_key in tasks
        }
        for fut in as_completed(future_to_key):
            loss_results[future_to_key[fut]] = fut.result()

    leads_out: dict[str, str] = {}
    for lead in lead_range:
        lead_key = str(lead)
        eligible_loss: dict[str, float] = {}
        for mk in model_order:
            loss = loss_results.get((mk, lead_key))
            if loss is not None and np.isfinite(loss):
                eligible_loss[mk] = loss
        if not eligible_loss:
            continue
        if len(eligible_loss) == 1:
            winner = next(iter(eligible_loss))
        else:
            winner = min(
                eligible_loss,
                key=lambda m: (eligible_loss[m], model_order.index(m)),
            )
        leads_out[lead_key] = winner

    accumulator_key: str | None
    if period == "monthly":
        accumulator_key = month
    elif period == "seasonal":
        accumulator_key = season
    else:
        accumulator_key = None

    return {
        "schema": "best_model_by_lead/v1",
        "statistic": stat_name,
        "accumulator": period,
        "accumulator_key": accumulator_key,
        "models_considered": list(model_order),
        "metric": _winner_metric_label(stat_name),
        "aggregation": "region",
        "leads": leads_out,
    }


def stats_for_region_all_leads(
    *,
    static_root: Path | None = None,
    store: StaticStore | None = None,
    model: str,
    region: dict,
    min_lead: int,
    max_lead: int,
    stat_names: list[str] | None = None,
    period: str = "yearly",
    month: str | None = None,
    season: str | None = None,
) -> list[dict[str, object]]:
    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    lead_range = list(range(min_lead, max_lead + 1))

    # Prefetch grid once so parallel workers don't all race to S3 for it.
    _get_grid(resolved_store, model)

    if len(lead_range) <= 1:
        return [
            {
                "lead": lead,
                "stats": stats_for_region(
                    store=resolved_store,
                    model=model,
                    lead=lead,
                    region=region,
                    stat_names=stat_names,
                    period=period,
                    month=month,
                    season=season,
                ),
            }
            for lead in lead_range
        ]
    max_workers = min(len(lead_range), 20)
    by_lead: dict[int, dict[str, dict[str, float | None | str | bool]]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_lead = {
            ex.submit(
                stats_for_region,
                store=resolved_store,
                model=model,
                lead=lead,
                region=region,
                stat_names=stat_names,
                period=period,
                month=month,
                season=season,
            ): lead
            for lead in lead_range
        }
        for fut in as_completed(future_to_lead):
            lead = future_to_lead[fut]
            by_lead[lead] = fut.result()
    return [{"lead": lead, "stats": by_lead[lead]} for lead in lead_range]
