# ModelAccuracy

Precipitation forecast verification system. Downloads numerical and ML model forecasts (GFS, NBM, GraphCast, AIFS) and PRISM observations, computes verification statistics, generates map tiles, and serves an interactive viewer.

**Current deployed version:** [https://d2375txx9cn814.cloudfront.net/](https://d2375txx9cn814.cloudfront.net/)

## Quick start

```bash
# 1. Python deps
pip install -r requirements.txt

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

Open the URL Vite prints (default **http://localhost:5173**). Optional: set `MAPTILER_API_KEY` in `.env` (read by `export_static.py` and baked into `static/config.json`) for a custom basemap; without it the SPA falls back to a free OSM-style tileset.

## Scripts

### `download.py`

Downloads model forecasts (GFS, NBM, GraphCast, AIFS) and PRISM observations.

```bash
python download.py --start-date 2025-01-01 --end-date 2025-01-31   # all models + PRISM
python download.py --model gfs --start-date 2025-01-01              # just GFS + PRISM
python download.py --model gfs --start-year 2024 --end-year 2025    # full years
python download.py --forecast                                        # today's forecast (probes cycles per model)
python download.py --catchup                                         # fill gap through yesterday
python download.py --no-prism --model nbm                            # skip PRISM
```

- `--model {gfs,nbm,graphcast,aifs}`: download a specific model (default: all registered models)
- `--forecast`: download today's forecast and extract into stats format (mutually exclusive with `--catchup`)
- `--catchup`: auto-detect last downloaded date and fill through yesterday
- `--no-prism`: skip PRISM observation download

### `compute_stats.py`

Computes verification statistics from each model's forecasts against PRISM observations. Automatically preconverts GRIB2 files to `.npy` for faster reading on subsequent runs.

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

Builds `static_export/` for disk-backed serving: Vite SPA (`index.html`, `assets/`), `static_export/static/` (config, zip lookups, tiles, ranges, admin), `static_export/data/` (grid metadata and verification `.bin` arrays), and `static_export/forecast/` (forecast `.bin` arrays + `forecast_calendar.json`). Run this before `docker build` or local API use.

- `python export_static.py` — full export (all of the above).
- `python export_static.py --static` — only `static_export/static/` (config, zip, tiles, ranges).
- `python export_static.py --data` — only `static_export/data/` (verification `.bin` + `grid.json`).
- `python export_static.py --forecast` — only `static_export/forecast/` (forecast bins + `forecast_calendar.json`).
- `python export_static.py --frontend` — only site-root SPA + `export_manifest.json`.

Use `python export_static.py --help` for `--output` and `--clean` (full export only).

Admin polygon boundaries (`static_export/static/admin/boundaries.json`) are produced by `scripts/fetch_admin_boundaries.py` — not by `export_static.py`.

## Statistics

Verification statistics (computed from model + PRISM pairs):

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
- `GET /api/auth/config` — public Cognito Hosted UI hints for the SPA
- `POST /api/stats/query` — stats across leads for a region; optional `X-Model` header if `model` is omitted in the body
- `POST /api/stats/lead-winners` — best verification statistic per lead for the current map region
- `POST /api/stats/forecast` — forecast values for all models across all leads for a region
- `/api/shapes` (CRUD, requires auth) — list / create / read / update / delete saved shapes; backed by DynamoDB, authenticated via Cognito JWT

## Models

Models are registered in `model_registry.py`:

| Key | Label | Grid / source | Cycle | Leads |
|-----|-------|---------------|-------|-------|
| `gfs` | GFS | 0.25° global GRIB2 | 12z | 1–14 d |
| `nbm` | NBM | 0.25° US grid `.npy` | 12z | 1–11 d |
| `graphcast` | GraphCast | NOAA AIWP S3 (00z and/or 12z, probed per day) | 12z (default) | 1–10 d |
| `aifs` | AIFS | ECMWF open data S3 | 12z | 1–14 d |

## Production deployment (AWS)

Production splits `static_export/` across three deploy paths. Each shell script loads `.env` from the repo root and runs `python3 export_static.py --<mode>` to refresh just its subtree (or use `SKIP_BUILD=1` / `SKIP_EXPORT=1` to reuse what's already on disk).

| Artifact | Refreshes | Notes |
|----------|-----------|-------|
| [Dockerfile](Dockerfile) | API image | Bakes `static_export/data/` and `static_export/static/admin/boundaries.json`; fails if either is missing |
| [deploy_fargate.sh](deploy_fargate.sh) | API image → ECR (+ optional ECS redeploy) | Runs `export_static.py --data`, verifies data, `docker build`, ECR push (us-west-1 default) |
| [deploy_static.sh](deploy_static.sh) | `static_export/static/` and `static_export/forecast/` → S3 | Static path also creates a `/static/*` CloudFront invalidation |
| [deploy_frontend.sh](deploy_frontend.sh) | Vite SPA (`index.html`, `assets/`, favicons, `export_manifest.json`) → S3 | Invalidates non-hashed files; `assets/` URLs are content-hashed |

Routing in production: CloudFront fronts S3 (SPA shell, `/static/*`, `/assets/*`) and the ALB-backed Fargate API (`/api/*`, `/data/*`). The backend reads forecast `.bin` files directly from S3 (refreshed every ~5 min); verification data is read from disk inside the container.

### Fargate (`deploy_fargate.sh`)

The Docker image **fails to build** if `static_export/data/<model>/grid.json` or `static_export/static/admin/boundaries.json` is missing. Fargate deploy refreshes **verification data** only — run `deploy_static.sh` and/or `deploy_frontend.sh` for the other parts.

```bash
chmod +x deploy_fargate.sh
./deploy_fargate.sh
```

Common flags:

- `IMAGE_TAG=v1.2.3 ./deploy_fargate.sh`
- `SKIP_EXPORT=1 ./deploy_fargate.sh` — use existing `static_export/data/`
- `SKIP_PUSH=1 ./deploy_fargate.sh` — build only, no ECR push
- `./deploy_fargate.sh --tag-latest` (or `ECR_PUSH_LATEST=1`) — also push `:latest`
- `./deploy_fargate.sh --force-redeploy` (or `ECS_FORCE_REDEPLOY=1`) — `aws ecs update-service --force-new-deployment` after push; needs `ECS_CLUSTER`/`ECS_CLUSTER_ARN` + `ECS_SERVICE`/`ECS_SERVICE_ARN` (or interactive prompt)
- `AWS_REGION=us-east-1 ECR_REPOSITORY=my-api ./deploy_fargate.sh`
- `DOCKER_PLATFORM=linux/arm64 ./deploy_fargate.sh` — only if running ARM/Graviton; default is `linux/amd64`

### Static + forecast (`deploy_static.sh`)

```bash
./deploy_static.sh                # default: --static and --forecast
./deploy_static.sh --static       # tiles, ranges, config, zip, admin
./deploy_static.sh --forecast     # daily forecast refresh (no CF invalidation)
SKIP_BUILD=1 ./deploy_static.sh --static
SKIP_INVALIDATE=1 ./deploy_static.sh --static
```

Cache-Control mirrors `LongCacheStaticMiddleware` in `backend/api.py`: `zip/` and `admin/` are 1 yr immutable; `tiles/`, `ranges/`, and `config.json` are 300 s `must-revalidate`. The forecast S3 prefix is derived from `DATA_S3_URI` (sibling of the stats prefix); see the script header for the parsing rule.

### Frontend (`deploy_frontend.sh`)

```bash
./deploy_frontend.sh              # rebuild SPA + sync + invalidate
SKIP_BUILD=1 ./deploy_frontend.sh # upload existing static_export/
SKIP_INVALIDATE=1 ./deploy_frontend.sh
```

Uploads `index.html` (no-cache), `assets/` (1 yr immutable), `favicon.*`, and `export_manifest.json`; CloudFront invalidation covers only the non-hashed files. Requires `CLOUDFRONT_DISTRIBUTION_ID` in `.env`.

### Environment variables

Most ECS task / build-time env vars are read from `.env` at the repo root by all three deploy scripts and propagated to the Docker image via `--build-arg`:

| Variable | Description |
|----------|-------------|
| `DATA_S3_URI` | `s3://bucket[/prefix]` for stats data. Forecasts are read from a `forecast/` folder that is a sibling of `<prefix>` (keys: `forecast/{model}/lead_{n}.bin`, `forecast/forecast_calendar.json`) |
| `CLOUDFRONT_DISTRIBUTION_ID` | CloudFront distribution to invalidate (used by `deploy_static.sh` and `deploy_frontend.sh`) |
| `AWS_REGION` | Default `us-west-1` |
| `AWS_PROFILE` | Optional; exported when set |
| `COGNITO_USER_POOL_ID` | Cognito User Pool ID (required for auth) |
| `COGNITO_APP_CLIENT_ID` | Cognito App Client ID (required for auth) |
| `COGNITO_REGION` | Defaults to `AWS_REGION` |
| `COGNITO_DOMAIN_PREFIX` | Hosted UI domain label (`*.auth.<region>.amazoncognito.com`) — one of `COGNITO_DOMAIN_PREFIX` or `COGNITO_OAUTH_BASE_URL` is required |
| `COGNITO_OAUTH_BASE_URL` | Optional full https origin for Hosted UI (overrides prefix+region) |
| `DYNAMODB_USER_ITEMS_TABLE` | DynamoDB table for saved shapes (required, no default) |
| `MAPTILER_API_KEY` | Optional MapTiler key; baked into `static/config.json` by `export_static.py` |
| `WARM_CACHE` | `1` / `true` / `yes` — preload all models' `grid.json` at backend startup |
| `PORT` | Listen port (default **8080**) |
| `DEV` | `1` / `true` — backend skips production cache headers, reaches for disk over S3 |

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