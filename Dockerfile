# ModelAccuracy API + static_export (disk-backed stats, same as local dev).
# Build after: python export_static.py --data (or full export_static.py), or ./deploy_fargate.sh.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY model_registry.py stats_grid_metadata.py ./
COPY backend ./backend
COPY statistics_plugins ./statistics_plugins
COPY static_export ./static_export

# Keep in sync with verify_static_export() in deploy_fargate.sh
RUN python3 -c "import pathlib,sys; r=pathlib.Path('static_export/data'); (r.is_dir() or (print('ERROR: static_export/data missing',file=sys.stderr), sys.exit(1))); (any((p/'grid.json').is_file() for p in r.iterdir() if p.is_dir()) or (print('ERROR: need static_export/data/<model>/grid.json',file=sys.stderr), sys.exit(1)))"

# Non-secret runtime config baked from build args (sourced from .env by deploy_fargate.sh).
# Build fails if any are missing. ECS task-def env vars still override these at runtime.
ARG MODELACCURACY_DATA_S3_URI
ARG COGNITO_USER_POOL_ID
ARG COGNITO_APP_CLIENT_ID
ARG COGNITO_REGION
ARG COGNITO_DOMAIN_PREFIX
ARG DYNAMODB_USER_ITEMS_TABLE

RUN for v in MODELACCURACY_DATA_S3_URI COGNITO_USER_POOL_ID COGNITO_APP_CLIENT_ID \
             COGNITO_REGION COGNITO_DOMAIN_PREFIX DYNAMODB_USER_ITEMS_TABLE; do \
      eval "val=\${$v}"; \
      [ -n "$val" ] || { echo "ERROR: build arg $v is required (set in .env or pass --build-arg)" >&2; exit 1; }; \
    done

ENV MODELACCURACY_DATA_S3_URI=${MODELACCURACY_DATA_S3_URI} \
    COGNITO_USER_POOL_ID=${COGNITO_USER_POOL_ID} \
    COGNITO_APP_CLIENT_ID=${COGNITO_APP_CLIENT_ID} \
    COGNITO_REGION=${COGNITO_REGION} \
    COGNITO_DOMAIN_PREFIX=${COGNITO_DOMAIN_PREFIX} \
    DYNAMODB_USER_ITEMS_TABLE=${DYNAMODB_USER_ITEMS_TABLE}

# Container command (documented in deploy_fargate.sh header)
ENV PORT=8080
EXPOSE 8080

# No curl in slim image; urllib matches GET /health (backend.api).
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD python -c "import os,urllib.request; p=os.environ.get('PORT','8080'); urllib.request.urlopen(f'http://127.0.0.1:{p}/health', timeout=4)"

ENTRYPOINT ["/bin/sh", "-c", "exec uvicorn backend.api:app --host 0.0.0.0 --port ${PORT:-8080} --proxy-headers --forwarded-allow-ips '*'"]
