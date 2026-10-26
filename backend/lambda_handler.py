"""AWS Lambda entrypoint: Mangum ASGI adapter around ``backend.api:app``.

Deployed by ``deploy_api.sh`` as a zip package (handler
``backend.lambda_handler.handler``) behind a Lambda Function URL that
CloudFront fronts at ``/api/*``.  Static assets never reach this handler —
CloudFront serves them straight from S3 — so the FastAPI static mounts are
simply absent on Lambda (``static_export/`` is not in the package).

Two non-HTTP event shapes are accepted:

* ``{"warm": true}`` — EventBridge warm ping (keeps one execution environment
  resident so interactive queries skip the cold start). It also preloads every
  model's per-cell cube (``backend.series_query.get_cube``, 100–200 MB each,
  read whole from S3) so the resident environment answers ``POST /api/series``
  for a state without a multi-second cube fetch; a cache hit makes the ping a
  no-op afterwards. Errors are logged, never raised: the ping must not fail
  because one cube is missing.
"""

from __future__ import annotations

import logging

from mangum import Mangum

from backend.api import STATIC_STORE, app
from backend.series_query import get_cube
from model_registry import MODEL_REGISTRY

_asgi = Mangum(app, lifespan="auto")
_log = logging.getLogger(__name__)


def _preload_cubes() -> dict:
    loaded = {}
    for model in MODEL_REGISTRY:
        try:
            loaded[model] = get_cube(STATIC_STORE, model) is not None
        except Exception as exc:  # noqa: BLE001 — a warm ping never fails
            _log.warning("warm: cube preload failed for %s: %s", model, exc)
            loaded[model] = False
    return loaded


def handler(event, context):
    if isinstance(event, dict) and event.get("warm"):
        return {"warm": True, "cubes": _preload_cubes()}
    return _asgi(event, context)
