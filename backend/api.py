#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

_this_dir = Path(__file__).resolve().parent
_project_root = _this_dir.parent
sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv

load_dotenv(_project_root / ".env", override=True)

from fastapi import FastAPI, Header
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from model_registry import MODEL_REGISTRY
from backend.request_models import ForecastAllModelsRequest, LeadWinnersRequest, StatsQueryRequest
from backend.auth import public_auth_config
from backend.shapes_router import create_shapes_router
from backend.static_store import forecast_store_from_env, store_from_env
from backend.stats_service import query_forecast_all_models_payload, query_lead_winners_payload, query_stats_payload


STATIC_SITE_ROOT = _project_root / "static_export"
STATIC_ASSETS_ROOT = STATIC_SITE_ROOT / "static"
STATIC_DATA_ROOT = STATIC_SITE_ROOT / "data"
STATIC_STORE = store_from_env(default_local_root=STATIC_SITE_ROOT)
_FORECAST_STORE_CACHED: dict[str, object] = {}

def _get_forecast_store():
    """Lazy lookup so the local ``static_export/forecast/`` directory can appear after startup."""
    cached = _FORECAST_STORE_CACHED.get("v")
    if cached is not None:
        return cached
    store = forecast_store_from_env(default_local_root=STATIC_SITE_ROOT / "forecast")
    if store is not None:
        _FORECAST_STORE_CACHED["v"] = store
    return store


@asynccontextmanager
async def _lifespan(app: FastAPI):
    if os.getenv("MODELACCURACY_WARM_CACHE", "").strip().lower() in ("1", "true", "yes"):
        from backend.stats_query import _get_grid

        for mk in MODEL_REGISTRY:
            try:
                _get_grid(STATIC_STORE, mk)
            except Exception as e:
                print(
                    f"WARNING: MODELACCURACY_WARM_CACHE: failed to load grid for model {mk}: {e}",
                    file=sys.stderr,
                )
    yield


app = FastAPI(title="Model Statistics Query API", lifespan=_lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    """ALB / ECS health checks."""
    return {"status": "ok"}


@app.get("/api/auth/config")
def auth_config() -> dict:
    """Cognito Hosted UI hints for the SPA (safe to expose; no secrets)."""
    return public_auth_config()


class LongCacheStaticMiddleware(BaseHTTPMiddleware):
    """Set long-lived Cache-Control for versioned static assets (image tag = new deploy)."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path
        if path.startswith("/static/") or path.startswith("/data/"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


app.add_middleware(LongCacheStaticMiddleware)

# static_export/static: config, zip, tiles → /static/…
# static_export/data: .bin, grid.json → /data/…
if STATIC_ASSETS_ROOT.is_dir():
    app.mount("/static", StaticFiles(directory=str(STATIC_ASSETS_ROOT)), name="static")
else:
    print(
        "WARNING: static_export/static/ is missing — run export_static.py before serving the app.",
        file=sys.stderr,
    )
if STATIC_DATA_ROOT.is_dir():
    app.mount("/data", StaticFiles(directory=str(STATIC_DATA_ROOT)), name="data")
else:
    print(
        "WARNING: static_export/data/ is missing — run export_static.py before serving the app.",
        file=sys.stderr,
    )


@app.post("/api/stats/query")
def query_stats(
    payload: StatsQueryRequest,
    x_model: str | None = Header(
        None,
        alias="X-Model",
        description="Fallback model key when the model field is absent or empty.",
    ),
):
    return query_stats_payload(
        payload,
        store=STATIC_STORE,
        header_model=x_model,
        now_month=datetime.now(timezone.utc).month,
    )


# Saved shapes (requires authentication)
app.include_router(create_shapes_router())


@app.post("/api/stats/lead-winners")
def lead_winners(payload: LeadWinnersRequest):
    """Best verification-stat model per lead day for the given map region (not domain-wide)."""
    return query_lead_winners_payload(
        payload,
        store=STATIC_STORE,
        now_month=datetime.now(timezone.utc).month,
    )


@app.post("/api/stats/forecast")
def forecast_all_models(payload: ForecastAllModelsRequest):
    """Forecast values for all models across all leads for a region (load once, cache)."""
    return query_forecast_all_models_payload(
        payload, store=STATIC_STORE, forecast_store=_get_forecast_store(),
    )
