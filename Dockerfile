# ModelAccuracy API + static_export (disk-backed stats, same as local dev).
# Build after: python export_static.py, or ./deploy_fargate.sh (export + verify + build [+ ECR]).
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY requirements.api.txt .
RUN pip install --no-cache-dir -r requirements.api.txt

COPY model_registry.py stats_grid_metadata.py ./
COPY backend ./backend
COPY statistics_plugins ./statistics_plugins
COPY static_export ./static_export

# Keep in sync with verify_static_export() in deploy_fargate.sh
RUN python3 -c "import pathlib,sys; r=pathlib.Path('static_export/data'); (r.is_dir() or (print('ERROR: static_export/data missing',file=sys.stderr), sys.exit(1))); (any((p/'grid.json').is_file() for p in r.iterdir() if p.is_dir()) or (print('ERROR: need static_export/data/<model>/grid.json',file=sys.stderr), sys.exit(1)))"

# Container command (documented in deploy_fargate.sh header)
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/bin/sh", "-c", "exec uvicorn backend.api:app --host 0.0.0.0 --port ${PORT:-8080} --proxy-headers --forwarded-allow-ips '*'"]
