from __future__ import annotations

from pathlib import Path

from fastapi.responses import JSONResponse

from model_registry import MODEL_REGISTRY, DEFAULT_MODEL
from statistics_plugins.consistency import DEFAULT_SCALE_KM, DEFAULT_THRESHOLD_MM
from statistics_plugins.registry import (
    STATISTICS_BY_NAME,
)
from backend.request_models import (
    ForecastAllModelsRequest,
    LeadWinnersRequest,
    RevisionEfficiencyRequest,
    StatsQueryRequest,
)
from backend.static_store import LocalStaticStore, StaticStore, default_static_site_root
from backend.stats_query import (
    ScopesNotComputedError,
    forecast_all_models,
    CALIFORNIA_HUC2,
    episode_catalog_all_basins,
    episode_catalog_for_huc,
    episode_statistics,
    lead_winners_for_region,
    revision_efficiency_for_region,
    scope_statistics,
    stats_for_region,
    stats_for_region_all_leads,
)


def query_episode_catalog_payload(
    model: str,
    huc: str | None = None,
    *,
    huc2: str | None = None,
    static_root: Path | None = None,
    store: StaticStore | None = None,
):
    """Episode catalog: one basin's canonical catalog with *huc*, or the
    all-basin metadata listing without it (CONTRACT §6)."""
    model_key = model.strip().lower()
    if model_key not in MODEL_REGISTRY:
        return JSONResponse(
            {"error": f"Unknown model: {model_key}", "valid_models": list(MODEL_REGISTRY)},
            status_code=400,
        )
    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    try:
        if huc is None:
            return episode_catalog_all_basins(
                resolved_store, model_key, huc2 or CALIFORNIA_HUC2,
            )
        return episode_catalog_for_huc(resolved_store, model_key, huc)
    except FileNotFoundError:
        return JSONResponse(
            {"error": "AR episodes are unavailable; run compute_stats.py --only episodes "
                      "and export_static.py."},
            status_code=404,
        )
    except KeyError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)

WINNER_STAT_NAMES = frozenset(
    name for name, entry in STATISTICS_BY_NAME.items() if entry.spec.winner_support
)
REGIONAL_STAT_NAMES = frozenset(
    name for name, entry in STATISTICS_BY_NAME.items() if entry.spec.scope == "region"
)
# Packed consistency stats with per-lead regional entries (the threshold/scale
# echo applies only to these). scope=="region" excludes revision_efficiency,
# whose packed planes are served solely by the pooled revision endpoint.
PACKED_STAT_NAMES = frozenset(
    name for name, entry in STATISTICS_BY_NAME.items()
    if entry.spec.storage.kind == "packed_grid" and entry.spec.scope == "region"
)

SEASONS = {"djf": (12, 1, 2), "mam": (3, 4, 5), "jja": (6, 7, 8), "son": (9, 10, 11)}

_data_exists_cache: dict[str, bool] = {}


def _data_exists(store: StaticStore) -> bool:
    """Cache the ``exists("model_data")`` probe -- only positive results are cached so a
    newly-populated S3 prefix is picked up without restarting the Lambda."""
    if _data_exists_cache.get(store.cache_key):
        return True
    result = store.exists("model_data")
    if result:
        _data_exists_cache[store.cache_key] = True
    return result


def _stats_data_missing_message(store: StaticStore) -> str:
    base = "static_export/model_data is missing. Run export_static.py before serving the app."
    if store.cache_key.startswith("s3://"):
        return (
            f"No stats data found under {store.cache_key}. "
            "DATA_S3_URI must point at the stats root in S3 (same layout as static_export/model_data/: "
            "<model>/grid.json, no model_data/ segment in keys). Check the URI, object keys, and Lambda IAM "
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

    if payload.context is not None:
        requested = stat_names or []
        try:
            if payload.context.scope is not None:
                # Pooled scope branch (ar_all / non_ar) — mirrors the episode
                # branch but reads the basin's v3 scopes block.
                stats = scope_statistics(
                    resolved_store, model_key, payload.context.huc,
                    payload.context.scope, requested,
                )
            else:
                stats = episode_statistics(
                    resolved_store, model_key, payload.context.huc,
                    payload.context.episodeId, requested,
                )
        except FileNotFoundError:
            return JSONResponse({"error": "AR episode statistics are unavailable"}, status_code=404)
        except ScopesNotComputedError as exc:
            # Pre-v3 episodes.json: scopes exist only after an episodes rerun.
            return JSONResponse({"error": str(exc)}, status_code=404)
        except KeyError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        return {
            "model": model_key,
            # exclude_none: the unused selector (episodeId or scope) is not
            # echoed, so the episode-branch response shape is unchanged.
            "context": payload.context.model_dump(exclude_none=True),
            "stats": stats,
        }

    if stat_names is None:
        stat_names = [name for name in REGIONAL_STAT_NAMES
                      if payload.period in STATISTICS_BY_NAME[name].spec.periods]

    if stat_names is not None:
        invalid_scope = [name for name in stat_names if name not in REGIONAL_STAT_NAMES]
        if invalid_scope:
            return JSONResponse(
                {"error": f"Statistics require an episode context: {invalid_scope}"},
                status_code=400,
            )
        invalid_period = [name for name in stat_names
                          if payload.period not in STATISTICS_BY_NAME[name].spec.periods]
        if invalid_period:
            return JSONResponse(
                {"error": f"Period {payload.period!r} is unavailable for: {invalid_period}"},
                status_code=400,
            )

    month = payload.month
    season = payload.season
    if payload.period == "monthly" and month is None:
        month = current_month_str(now_month)
    if payload.period == "seasonal" and season is None:
        season = current_season_str(now_month)

    # Jumpiness parameters: resolve defaults here and echo them in the
    # response, so a client that omitted them still knows which (q, n) slice
    # the numbers describe (comparisons are only valid within one slice).
    threshold = payload.threshold if payload.threshold is not None else DEFAULT_THRESHOLD_MM
    scale = payload.scale if payload.scale is not None else DEFAULT_SCALE_KM
    requested = stat_names if stat_names is not None else list(REGIONAL_STAT_NAMES)
    consistency_echo = (
        {"threshold_mm": threshold, "scale_km": scale}
        if any(name in PACKED_STAT_NAMES for name in requested)
        else None
    )

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
            threshold=threshold,
            scale=scale,
        )
        response = {
            "model": model_key,
            "lead": str(payload.lead),
            "period": payload.period,
            "month": month,
            "season": season,
            "stats": stats,
        }
        if consistency_echo:
            response["consistency"] = consistency_echo
        return response

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
        threshold=threshold,
        scale=scale,
    )
    response = {
        "model": model_key,
        "minLead": payload.minLead,
        "maxLead": payload.maxLead,
        "period": payload.period,
        "month": month,
        "season": season,
        "results": results,
    }
    if consistency_echo:
        response["consistency"] = consistency_echo
    return response


def query_revision_efficiency_payload(
    payload: RevisionEfficiencyRequest,
    *,
    static_root: Path | None = None,
    store: StaticStore | None = None,
    header_model: str | None = None,
    now_month: int,
):
    """Grid-pooled revision efficiency R for a region (CONTRACT §5/§6).

    Response: ``{model, period, season?, value, units, no_data, dates,
    definition: "grid-pooled", skill_gain_nonpositive?}``. ``season`` is
    echoed for seasonal queries (resolved to the current season when omitted,
    mirroring query_stats_payload).
    """
    model_key = resolve_api_model(payload.model, header_model)
    if model_key not in MODEL_REGISTRY:
        return JSONResponse(
            {"error": f"Unknown model: {model_key}", "valid_models": list(MODEL_REGISTRY)},
            status_code=400,
        )
    resolved_store = store or LocalStaticStore(static_root or default_static_site_root())
    if not _data_exists(resolved_store):
        return JSONResponse(
            {"error": _stats_data_missing_message(resolved_store)},
            status_code=500,
        )
    season = payload.season
    if payload.period == "seasonal" and season is None:
        season = current_season_str(now_month)
    entry = revision_efficiency_for_region(
        store=resolved_store,
        model=model_key,
        region=payload.region.model_dump(),
        period=payload.period,
        season=season,
    )
    response: dict[str, object] = {"model": model_key, "period": payload.period}
    if payload.period == "seasonal":
        response["season"] = season
    response.update(entry)
    return response


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
    if payload.statistic not in WINNER_STAT_NAMES:
        return JSONResponse(
            {"error": "Lead winners are only computed for verification statistics "
                      "(not forecast or the consistency statistics)."},
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
                "(production; derived from DATA_S3_URI)."
            },
            status_code=500,
        )

    return forecast_all_models(
        store=resolved_store,
        forecast_store=forecast_store,
        region=payload.region.model_dump(),
        variable=payload.variable,
    )
