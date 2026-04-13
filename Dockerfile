# ModelAccuracy API + static_export (disk-backed stats, same as local dev).
# Build after: python export_static.py --data (or full export_static.py), or ./deploy_fargate.sh.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# API stack (keep in sync with README quick start and deploy_fargate.sh header comment)
RUN pip install --no-cache-dir \
  "fastapi>=0.115.0" \
  "uvicorn[standard]>=0.32.0" \
  "numpy>=2.0.0" \
  "pydantic>=2.0.0" \
  "boto3>=1.35.0"

COPY model_registry.py stats_grid_metadata.py ./
COPY backend ./backend
COPY statistics_plugins ./statistics_plugins
COPY static_export ./static_export

# Keep in sync with verify_static_export() in deploy_fargate.sh
RUN python3 -c "import pathlib,sys; r=pathlib.Path('static_export/data'); (r.is_dir() or (print('ERROR: static_export/data missing',file=sys.stderr), sys.exit(1))); (any((p/'grid.json').is_file() for p in r.iterdir() if p.is_dir()) or (print('ERROR: need static_export/data/<model>/grid.json',file=sys.stderr), sys.exit(1)))"

# Container command (documented in deploy_fargate.sh header)
ENV PORT=8080
EXPOSE 8080

# No curl in slim image; urllib matches GET /health (backend.api).
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD python -c "import os,urllib.request; p=os.environ.get('PORT','8080'); urllib.request.urlopen(f'http://127.0.0.1:{p}/health', timeout=4)"

ENTRYPOINT ["/bin/sh", "-c", "exec uvicorn backend.api:app --host 0.0.0.0 --port ${PORT:-8080} --proxy-headers --forwarded-allow-ips '*'"]
