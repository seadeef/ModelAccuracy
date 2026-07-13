# ModelAccuracy API + static_export (disk-backed stats, same as local dev).
# Build after: python export_static.py --image (or full export_static.py), or ./deploy_fargate.sh.
# Docker COPY below is read-only — all files are produced by export_static.py into static_export/.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Slim API deps only (no GDAL/rasterio/xarray/eccodes/s3fs); see requirements.api.txt.
COPY requirements.api.txt .
RUN pip install --no-cache-dir -r requirements.api.txt

# forecast_variables is imported by backend.stats_query at module load — it must
# be in the image or `import backend.api` fails.
COPY model_registry.py stats_grid_metadata.py forecast_variables.py ./
COPY backend ./backend
COPY statistics_plugins ./statistics_plugins
# Only bake in what the backend reads from disk:
#   model_data/                  — store_from_env → LocalStaticStore (stats + grid.json)
#   admin/boundaries.json        — backend/admin_boundaries.py reads at first use
#   wbd/chunks/                  — backend/wbd_boundaries.py lazy-loads at first use
#   military/boundaries.json     — backend/military_boundaries.py reads at first use
# Everything else under static_export/ is browser-served from S3 (via CloudFront)
# or backend-read from S3 (forecast/); see deploy_static.sh.
COPY static_export/model_data ./static_export/model_data
COPY static_export/admin/boundaries.json ./static_export/admin/boundaries.json
COPY static_export/wbd/chunks ./static_export/wbd/chunks
COPY static_export/military/boundaries.json ./static_export/military/boundaries.json

# Keep in sync with verify_static_export() in deploy_fargate.sh
RUN python3 -c "import pathlib,sys; r=pathlib.Path('static_export/model_data'); (r.is_dir() or (print('ERROR: static_export/model_data missing',file=sys.stderr), sys.exit(1))); (any((p/'grid.json').is_file() for p in r.iterdir() if p.is_dir()) or (print('ERROR: need static_export/model_data/<model>/grid.json',file=sys.stderr), sys.exit(1))); (pathlib.Path('static_export/admin/boundaries.json').is_file() or (print('ERROR: static_export/admin/boundaries.json missing — run export_static.py --image',file=sys.stderr), sys.exit(1))); (pathlib.Path('static_export/wbd/chunks/2/_root.json').is_file() or (print('ERROR: static_export/wbd/chunks missing — run export_static.py --image',file=sys.stderr), sys.exit(1))); (pathlib.Path('static_export/military/boundaries.json').is_file() or (print('ERROR: static_export/military/boundaries.json missing — run export_static.py --image',file=sys.stderr), sys.exit(1)))"

# Non-secret runtime config baked from build args (sourced from .env by deploy_fargate.sh).
# Build fails if any are missing. ECS task-def env vars still override these at runtime.
ARG DATA_S3_URI
ARG COGNITO_USER_POOL_ID
ARG COGNITO_APP_CLIENT_ID
ARG COGNITO_REGION
ARG COGNITO_DOMAIN_PREFIX
ARG COGNITO_OAUTH_BASE_URL
ARG DYNAMODB_USER_ITEMS_TABLE

RUN for v in DATA_S3_URI COGNITO_USER_POOL_ID COGNITO_APP_CLIENT_ID \
             COGNITO_REGION DYNAMODB_USER_ITEMS_TABLE; do \
      eval "val=\${$v}"; \
      [ -n "$val" ] || { echo "ERROR: build arg $v is required (set in .env or pass --build-arg)" >&2; exit 1; }; \
    done; \
    [ -n "${COGNITO_DOMAIN_PREFIX}" ] || [ -n "${COGNITO_OAUTH_BASE_URL}" ] || \
      { echo "ERROR: one of COGNITO_DOMAIN_PREFIX or COGNITO_OAUTH_BASE_URL must be set" >&2; exit 1; }

ENV DATA_S3_URI=${DATA_S3_URI} \
    COGNITO_USER_POOL_ID=${COGNITO_USER_POOL_ID} \
    COGNITO_APP_CLIENT_ID=${COGNITO_APP_CLIENT_ID} \
    COGNITO_REGION=${COGNITO_REGION} \
    COGNITO_DOMAIN_PREFIX=${COGNITO_DOMAIN_PREFIX} \
    COGNITO_OAUTH_BASE_URL=${COGNITO_OAUTH_BASE_URL} \
    DYNAMODB_USER_ITEMS_TABLE=${DYNAMODB_USER_ITEMS_TABLE}

# Container command (documented in deploy_fargate.sh header)
ENV PORT=8080
EXPOSE 8080

# No curl in slim image; urllib matches GET /health (backend.api).
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD python -c "import os,urllib.request; p=os.environ.get('PORT','8080'); urllib.request.urlopen(f'http://127.0.0.1:{p}/health', timeout=4)"

ENTRYPOINT ["/bin/sh", "-c", "exec uvicorn backend.api:app --host 0.0.0.0 --port ${PORT:-8080} --proxy-headers --forwarded-allow-ips '*'"]
