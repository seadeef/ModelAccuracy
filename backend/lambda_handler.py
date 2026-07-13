"""AWS Lambda entrypoint: Mangum ASGI adapter around ``backend.api:app``.

Deployed by ``deploy_api.sh`` as a zip package (handler
``backend.lambda_handler.handler``) behind a Lambda Function URL that
CloudFront fronts at ``/api/*``.  Static assets never reach this handler —
CloudFront serves them straight from S3 — so the FastAPI static mounts are
simply absent on Lambda (``static_export/`` is not in the package).

Two non-HTTP event shapes are accepted:

* ``{"warm": true}`` — EventBridge warm ping (keeps one execution environment
  resident so interactive queries skip the cold start). Returns immediately
  without touching the ASGI app.
"""

from __future__ import annotations

from mangum import Mangum

from backend.api import app

_asgi = Mangum(app, lifespan="auto")


def handler(event, context):
    if isinstance(event, dict) and event.get("warm"):
        return {"warm": True}
    return _asgi(event, context)
