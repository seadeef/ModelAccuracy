from __future__ import annotations

from pathlib import Path

from fastapi.responses import JSONResponse

from model_registry import MODEL_REGISTRY, DEFAULT_MODEL
from statistics_plugins.registry import STATISTICS_BY_NAME, VERIFICATION_STATISTICS
from backend.request_models import ForecastAllModelsRequest, LeadWinnersRequest, StatsQueryRequest
from backend.static_store import LocalStaticStore, StaticStore, default_static_site_root
from backend.stats_query import (
    forecast_all_models,
    lead_winners_for_region,
    stats_for_region,
    stats_for_region_all_leads,
)

VERIFICATION_STAT_NAMES = frozenset(p.spec.name for p in VERIFICATION_STATISTICS)

SEASONS = {"djf": (12, 1, 2), "mam": (3, 4, 5), "jja": (6, 7, 8), "son": (9, 10, 11)}

_data_exists_cache: dict[str, bool] = {}


def _data_exists(store: StaticStore) -> bool:
    """Cache the ``exists("data")`` probe -- only positive results are cached so a
    newly-populated S3 prefix is picked up without restarting the Lambda."""
    if _data_exists_cache.get(store.cache_key):
        return True
    result = store.exists("data")
    if result:
        _data_exists_cache[store.cache_key] = True
    return result


def _stats_data_missing_message(store: StaticStore) -> str:
    base = "static_export/data is missing. Run export_static.py before serving the app."
    if store.cache_key.startswith("s3://"):
        return (
            f"No stats data found under {store.cache_key}. "
            "MODELACCURACY_DATA_S3_URI must point at the stats root in S3 (same layout as static_export/data/: "
            "<model>/grid.json, no data/ segment in keys). Check the URI, object keys, and Lambda IAM "
            "(s3:GetObject, s3:ListBucket)."
        )
    return base


def resolve_api_model(query_value: str | None, header_value: str | None) -> str:
    for raw in (query_value, header_value):
        if raw is None:
            continue
        value = str(raw).strip()
        if value:
            return value.lower()
    return DEFAULT_MODEL


def current_month_str(now_month: int) -> str:
    return f"{now_month:02d}"


def current_season_str(now_month: int) -> str:
    for name, months in SEASONS.items():
        if now_month in months:
            return name
    return "djf"


def query_stats_payload(
    payload: StatsQueryRequest,
    *,
    static_root: Path | None = None,
    store: StaticStore | None = None,
    header_model: str | None = None,
    now_month: int,
):
    model_key = resolve_api_model(payload.model, header_model)
    if model_key not in MODEL_REGISTRY:
        return JSONResponse(
            {"error": f"Unknown model: {model_key}", "valid_models": list(MODEL_REGISTRY)},
            status_code=400,
        )

    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    if not _data_exists(resolved_store):
        return JSONResponse(
            {
                "error": _stats_data_missing_message(resolved_store),
            },
            status_code=500,
        )

    stat_names = payload.statistics
    if stat_names is not None:
        invalid = [name for name in stat_names if name not in STATISTICS_BY_NAME]
        if invalid:
            return JSONResponse({"error": f"Unknown statistics: {invalid}"}, status_code=400)

    month = payload.month
    season = payload.season
    if payload.period == "monthly" and month is None:
        month = current_month_str(now_month)
    if payload.period == "seasonal" and season is None:
        season = current_season_str(now_month)

    if payload.lead is not None:
        stats = stats_for_region(
            store=resolved_store,
            model=model_key,
            lead=payload.lead,
            region=payload.region.model_dump(),
            stat_names=stat_names,
            period=payload.period,
            month=month,
            season=season,
        )
        return {
            "model": model_key,
            "lead": str(payload.lead),
            "period": payload.period,
            "month": month,
            "season": season,
            "stats": stats,
        }

    results = stats_for_region_all_leads(
        store=resolved_store,
        model=model_key,
        region=payload.region.model_dump(),
        min_lead=payload.minLead,
        max_lead=payload.maxLead,
        stat_names=stat_names,
        period=payload.period,
        month=month,
        season=season,
    )
    return {
        "model": model_key,
        "minLead": payload.minLead,
        "maxLead": payload.maxLead,
        "period": payload.period,
        "month": month,
        "season": season,
        "results": results,
    }


def query_lead_winners_payload(
    payload: LeadWinnersRequest,
    *,
    static_root: Path | None = None,
    store: StaticStore | None = None,
    now_month: int,
):
    if payload.statistic not in STATISTICS_BY_NAME:
        return JSONResponse(
            {"error": f"Unknown statistic: {payload.statistic}"},
            status_code=400,
        )
    if payload.statistic not in VERIFICATION_STAT_NAMES:
        return JSONResponse(
            {"error": "Lead winners are only computed for verification statistics (not forecast)."},
            status_code=400,
        )

    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    if not _data_exists(resolved_store):
        return JSONResponse(
            {
                "error": _stats_data_missing_message(resolved_store),
            },
            status_code=500,
        )

    month = payload.month
    season = payload.season
    if payload.period == "monthly" and month is None:
        month = current_month_str(now_month)
    if payload.period == "seasonal" and season is None:
        season = current_season_str(now_month)

    return lead_winners_for_region(
        store=resolved_store,
        region=payload.region.model_dump(),
        stat_name=payload.statistic,
        period=payload.period,
        month=month,
        season=season,
        min_lead=payload.minLead,
        max_lead=payload.maxLead,
    )


def query_forecast_all_models_payload(
    payload: ForecastAllModelsRequest,
    *,
    store: StaticStore | None = None,
    forecast_store: StaticStore | None = None,
):
    resolved_store = store or LocalStaticStore(default_static_site_root())
    if not _data_exists(resolved_store):
        return JSONResponse(
            {"error": _stats_data_missing_message(resolved_store)},
            status_code=500,
        )
    if forecast_store is None:
        return JSONResponse(
            {
                "error": "Forecast data is not configured. Run export_static.py --forecast "
                "(local dev) or ensure the stats S3 bucket has a forecast/ prefix populated "
                "(production; derived from MODELACCURACY_DATA_S3_URI)."
            },
            status_code=500,
        )

    return forecast_all_models(
        store=resolved_store,
        forecast_store=forecast_store,
        region=payload.region.model_dump(),
    )
