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
from starlette.middleware.gzip import GZipMiddleware
from starlette.requests import Request

from model_registry import MODEL_REGISTRY
from backend.request_models import ForecastAllModelsRequest, LeadWinnersRequest, StatsQueryRequest
from backend.auth import public_auth_config
from backend.shapes_router import create_shapes_router
from backend.static_store import forecast_store_from_env, store_from_env
from backend.stats_service import query_forecast_all_models_payload, query_lead_winners_payload, query_stats_payload


STATIC_SITE_ROOT = _project_root / "static_export"
STATIC_FRONTEND_ROOT = STATIC_SITE_ROOT / "frontend"
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
    if os.getenv("WARM_CACHE", "").strip().lower() in ("1", "true", "yes"):
        from backend.stats_query import _get_grid

        for mk in MODEL_REGISTRY:
            try:
                _get_grid(STATIC_STORE, mk)
            except Exception as e:
                print(
                    f"WARNING: WARM_CACHE: failed to load grid for model {mk}: {e}",
                    file=sys.stderr,
                )
    yield


app = FastAPI(title="Model Statistics Query API", lifespan=_lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    """Deploy smoke tests (deploy_api.sh); exempt from origin verification."""
    return {"status": "ok"}


@app.get("/api/auth/config")
def auth_config() -> dict:
    """Cognito Hosted UI hints for the SPA (safe to expose; no secrets)."""
    return public_auth_config()


class OriginVerifyMiddleware(BaseHTTPMiddleware):
    """Reject requests that didn't come through CloudFront.

    The Lambda Function URL is public (AuthType NONE — CloudFront OAC can't
    sign POST bodies the browser sends), so CloudFront attaches a shared
    secret in ``x-origin-verify`` and everything else gets a 403.  Active
    only when ``ORIGIN_VERIFY_SECRET`` is set (production Lambda);
    dev/uvicorn never sets it.  ``/health`` stays open for smoke tests.
    """

    _SECRET = os.getenv("ORIGIN_VERIFY_SECRET", "").strip()

    async def dispatch(self, request: Request, call_next):
        import hmac

        supplied = request.headers.get("x-origin-verify", "")
        if request.url.path != "/health" and not hmac.compare_digest(
            supplied, self._SECRET
        ):
            from fastapi.responses import JSONResponse

            return JSONResponse({"error": "Forbidden"}, status_code=403)
        return await call_next(request)


class LongCacheStaticMiddleware(BaseHTTPMiddleware):
    """Set Cache-Control on static assets, with two regimes:

    * Rebuild outputs (tiles, ranges, data binaries, config) get a short cache
      with ``must-revalidate`` so they actually refresh after a re-run of the
      compute pipeline.  These URLs are stable (``lead_9.png`` stays
      ``lead_9.png``), so ``immutable`` would be a lie that locks browsers onto
      the first bytes they ever see.
    * Vendor / external shapes (ZIP polygons, admin boundaries) get a long
      cache with ``immutable`` since they change yearly at most.

    In dev (``DEV=1``), skip caching headers entirely.  StaticFiles
    will still emit ETag/Last-Modified, so browsers do conditional GETs and
    edits on disk show up on reload without fighting the cache.
    """

    _DEV = os.getenv("DEV", "").strip().lower() in ("1", "true", "yes")

    # Data files and SPA assets are all served from the site root now.
    _MUTABLE_PREFIXES = (
        "/tiles/",
        "/ranges/",
        "/config.json",
        "/model_data/",
        "/forecast/",
    )
    _IMMUTABLE_PREFIXES = (
        "/zip/",
        "/admin/",
        "/wbd/",
        "/military/",
        "/assets/",
    )

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if self._DEV:
            return response
        path = request.url.path
        if path.startswith(self._MUTABLE_PREFIXES):
            response.headers["Cache-Control"] = "public, max-age=300, must-revalidate"
        elif path.startswith(self._IMMUTABLE_PREFIXES):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


if OriginVerifyMiddleware._SECRET:
    app.add_middleware(OriginVerifyMiddleware)
app.add_middleware(LongCacheStaticMiddleware)
# Production CDN gzips for us; this matters in dev (uvicorn behind Vite proxy) where
# large JSON like /admin/*.topojson would otherwise ship raw.
app.add_middleware(GZipMiddleware, minimum_size=1024)


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


class SiteStaticFiles(StaticFiles):
    """Serve the flat data tree and the bundled SPA from a single mount at ``/``.

    Lookups try the data root (``static_export/``: config.json, zip, tiles, ranges,
    admin, wbd, model_data, forecast) first, then the SPA root
    (``static_export/frontend/``: index.html, assets, favicons). The SPA stays
    physically under ``frontend/`` but resolves at the site root, and ``/`` returns
    its ``index.html`` (``html=True``).
    """

    def __init__(self, *, data_root: Path, spa_root: Path) -> None:
        super().__init__(directory=str(data_root), html=True, check_dir=False)
        # StaticFiles.lookup_path iterates self.all_directories (with traversal
        # guards); append the SPA root so its files resolve at the root too.
        if spa_root.is_dir():
            self.all_directories.append(spa_root)


# Mounted LAST so every /api route and /health match before this catch-all.
# Data files and the SPA are all served from the site root: /config.json, /wbd/…,
# /tiles/…, /model_data/…, plus / (index.html), /assets/…, /favicon.svg.
if STATIC_SITE_ROOT.is_dir():
    app.mount("/", SiteStaticFiles(data_root=STATIC_SITE_ROOT, spa_root=STATIC_FRONTEND_ROOT), name="site")
else:
    print(
        "WARNING: static_export/ is missing — run export_static.py before serving the app.",
        file=sys.stderr,
    )
