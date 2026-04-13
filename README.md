# ModelAccuracy

Precipitation forecast verification system. Downloads GFS model forecasts and PRISM observations, computes verification statistics, generates map tiles, and serves an interactive viewer.

**Current deployed version:** [https://d2375txx9cn814.cloudfront.net/](https://d2375txx9cn814.cloudfront.net/)

## Quick start

```bash
# 1. Python deps
pip install "fastapi>=0.115.0" "uvicorn[standard]>=0.32.0" "numpy>=2.0.0" "pydantic>=2.0.0" "boto3>=1.35.0"
pip install xarray cfgrib rasterio rioxarray matplotlib

# 2. Download data (all models + PRISM)
python download.py --start-date 2024-01-01 --end-date 2024-12-31

# 3. Compute statistics
python compute_stats.py

# 4. Generate map tiles
python compute_tiles.py

# 5. Build static_export/
python export_static.py

# 6. API server
uvicorn backend.api:app --reload --host 0.0.0.0 --port 8000

# 7. Viewer (separate terminal)
cd frontend && npm install && npm run dev
```

Open the URL Vite prints (default **http://localhost:5173**). Optional: put your MapTiler key in `.maptiler_key` at the repo root for a custom basemap.

## Scripts

### `download.py`

Downloads GFS model data and PRISM observations.

```bash
python download.py --start-date 2025-01-01 --end-date 2025-01-31   # all models + PRISM
python download.py --model gfs --start-date 2025-01-01              # just GFS + PRISM
python download.py --model gfs --start-year 2024 --end-year 2025    # full years
python download.py --forecast                                        # today's forecast
python download.py --catchup                                         # fill gap through yesterday
```

- `--model`: download a specific model (default: all registered models)
- `--forecast`: download and extract today's forecast (mutually exclusive with `--catchup`)
- `--catchup`: auto-detect last downloaded date and fill through yesterday

### `compute_stats.py`

Computes verification statistics from GFS forecasts and PRISM observations. Automatically preconverts GRIB2 files to `.npy` for faster reading on subsequent runs.

```bash
python compute_stats.py                    # all models
python compute_stats.py --model gfs        # just GFS
python compute_stats.py --no-preconvert    # skip GRIB2-to-npy conversion
```

Statistics are stored as monthly accumulators, with yearly and seasonal views derived by summing:

```
stats_output/{model}/
  metadata.npz
  {stat}/
    lead_{N}.npz
    lead_{window}.npz
    monthly/01/ ... 12/
      lead_{N}.npz
    seasonal/djf/ mam/ jja/ son/
      lead_{N}.npz
```

### `compute_tiles.py`

Generates PNG tile images for the map viewer.

```bash
python compute_tiles.py                # all models
python compute_tiles.py --model gfs    # just GFS
```

Generates tiles for yearly, current month, and current season. Forecast tiles are yearly only. PNG layout mirrors stats: `tiles_output/<model>/<statistic>/lead_*.png` (plus `monthly/` / `seasonal/`). Map overlay extent is fixed in `backend/tile_overlay_constants.py` and `frontend/src/lib/constants.js` — keep these in sync.

### `export_static.py`

Builds `static_export/` for disk-backed serving: Vite SPA (`index.html`, `assets/`), `static_export/static/` (config, zip lookups, tiles, ranges), and `static_export/data/` (grid metadata and `.bin` arrays). Run this before `docker build` or local API use.

- `python export_static.py` — full export (all of the above).
- `python export_static.py --static` — only `static_export/static/` (config, zip, tiles, ranges).
- `python export_static.py --data` — only `static_export/data/`.
- `python export_static.py --frontend` — only site-root SPA + `export_manifest.json`.

Use `python export_static.py --help` for `--output` and `--clean` (full export only).

## Statistics

Verification statistics (computed from GFS + PRISM pairs):

- **bias** — mean precipitation bias (mm)
- **sacc** — spatial anomaly correlation coefficient (%)
- **nrmse** — normalized root mean square error (%)
- **nmad** — normalized mean absolute difference (%)

Display statistics:

- **forecast** — latest model precipitation forecast (mm)

## API and static URLs

The FastAPI app (`backend.api:app`) does **not** serve the SPA at `/`; use Vite dev (or CloudFront/S3 + ALB in production) for HTML. The API mounts:

- `/static/…` — files under `static_export/static/`
- `/data/…` — files under `static_export/data/`

Dynamic routes:

- `GET /health` — health check (`{"status":"ok"}`)
- `POST /api/stats/query` — stats across leads for a region; optional `X-Model` header if `model` is omitted in the body
- `POST /api/stats/lead-winners` — best verification statistic per lead for the current map region

## Models

Models are registered in `model_registry.py`. Currently: **GFS** (0.25° global, 12z cycle, leads 1–14 days) and **NBM** (assembled daily `.npy` on a 0.25° US grid, 12z cycle).

## Production deployment (AWS Fargate)

| Artifact | Role |
|----------|------|
| [Dockerfile](Dockerfile) | Image build; verifies `static_export/data/<model>/grid.json` exists |
| [deploy_fargate.sh](deploy_fargate.sh) | Runs `export_static.py --data`, verifies data, `docker build`, ECR push (us-west-1 default) |

The Docker image **fails to build** if `static_export/data/<model>/grid.json` is missing. Fargate deploy refreshes **data** only; run a full `export_static.py` (or `--static` / `--frontend`) when those parts change.

```bash
chmod +x deploy_fargate.sh
./deploy_fargate.sh
```

Common flags:

- `IMAGE_TAG=v1.2.3 ./deploy_fargate.sh`
- `SKIP_EXPORT=1 ./deploy_fargate.sh` — use existing `static_export/data/`
- `SKIP_PUSH=1 ./deploy_fargate.sh` — build only, no ECR push
- `AWS_REGION=us-east-1 ECR_REPOSITORY=my-api ./deploy_fargate.sh`
- `DOCKER_PLATFORM=linux/arm64 ./deploy_fargate.sh` — only if running ARM/Graviton; default is `linux/amd64`

### ECS task environment variables

| Variable | Description |
|----------|-------------|
| `MODELACCURACY_WARM_CACHE` | `1` / `true` / `yes` — preload all models' `grid.json` at startup |
| `PORT` | Listen port (default **8080**) |

### Run the container locally

```bash
python export_static.py
docker build -t modelaccuracy-api:latest .
docker run --rm -p 8080:8080 modelaccuracy-api:latest
curl -s http://127.0.0.1:8080/health
```

### Notes

- **CORS:** The API sends no CORS headers. Use same-origin path routing (Vite proxy in dev; CloudFront in prod) to avoid browser errors.
- **Caching:** Stats are cached in-process after first read. Scale with more Fargate tasks, not multiple workers per container.
- **Archived Lambda:** Previous Lambda scripts live under [archive/lambda/](archive/lambda/README.md) for reference.