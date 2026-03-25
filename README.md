# ModelAccuracy

Precipitation forecast verification system. Downloads GFS model forecasts and PRISM observations, computes verification statistics, generates map tiles, and serves an interactive viewer.

## Quick start

```bash
# 1. Python deps (API + export pipeline — same pins as Dockerfile)
pip install "fastapi>=0.115.0" "uvicorn[standard]>=0.32.0" "numpy>=2.0.0" "pydantic>=2.0.0" "boto3>=1.35.0"
# Plus processing stack for download/stats/tiles, e.g.:
pip install xarray cfgrib rasterio rioxarray matplotlib

# 2. Download data (all models + PRISM)
python download.py --start-date 2024-01-01 --end-date 2024-12-31

# 3. Compute statistics
python compute_stats.py

# 4. Generate map tiles
python compute_tiles.py

# 5. Build static_export/ (SPA, config, zip JSON, tiles, grid data for the API)
python export_static.py

# 6. API server (repository root so imports and static_export/ resolve)
uvicorn backend.api:app --reload --host 0.0.0.0 --port 8000

# 7. Viewer (separate terminal) — Vite proxies /api, /static, /data to the API
cd frontend && npm install && npm run dev
```

Open the URL Vite prints (default **http://localhost:5173**). Optional: put your MapTiler key in `.maptiler_key` at the repo root for a custom basemap; otherwise the app uses a demo MapLibre style.

## Scripts

### `download.py`

Downloads GFS model data and PRISM observations. PRISM always downloads alongside model data.

```bash
python download.py --start-date 2025-01-01 --end-date 2025-01-31   # all models + PRISM
python download.py --model gfs --start-date 2025-01-01              # just GFS + PRISM
python download.py --model gfs --start-year 2024 --end-year 2025    # full years
python download.py --forecast                                       # today's forecast
python download.py --catchup                                        # fill gap through yesterday
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

Statistics are stored as monthly accumulators (the primitive), with yearly and seasonal views derived by summing:

```
stats_output/{model}/
  metadata.npz                    # shared grid metadata for the model
  {stat}/
    lead_{N}.npz                  # yearly (sum of all months)
    lead_{window}.npz             # yearly windows
    monthly/01/ ... 12/
      lead_{N}.npz                # per-month
    seasonal/djf/ mam/ jja/ son/
      lead_{N}.npz                # sum of 3 months
```

### `compute_tiles.py`

Generates PNG tile images for the map viewer.

```bash
python compute_tiles.py                # all models
python compute_tiles.py --model gfs    # just GFS
```

Generates tiles for yearly, current month, and current season. Forecast tiles are yearly only.

PNG layout mirrors stats: `tiles_output/<model>/<statistic>/lead_*.png` (plus `monthly/` / `seasonal/`). Map overlay extent is fixed in `backend/tile_overlay_constants.py` and `frontend/src/lib/constants.js` (keep in sync).

### `export_static.py`

Builds `static_export/` for disk-backed serving: Vite SPA (`index.html`, `assets/`), `static_export/static/` (config, zip lookups, tiles, ranges), and `static_export/data/` (grid metadata and `.bin` arrays). Run this before `docker build` or local API use. See `python export_static.py --help` for flags (`--skip-frontend-build`, `--frontend-only`, etc.).

## Statistics

Verification statistics (computed from GFS + PRISM pairs):

- **bias** — mean precipitation bias (mm)
- **sacc** — spatial anomaly correlation coefficient (%)
- **nrmse** — normalized root mean square error (%)
- **nmad** — normalized mean absolute difference (%)

Display statistics:

- **forecast** — latest model precipitation forecast (mm)

## API and static URLs

The FastAPI app (`backend.api:app`) does **not** serve the SPA at `/`; use **Vite dev** (or CloudFront/S3 + ALB in production) for HTML. The API mounts:

- `/static/…` — files under `static_export/static` (e.g. `config.json`, `zip/{code}.json`, tiles)
- `/data/…` — files under `static_export/data` (per-model `grid.json`, statistic `.bin` files)

Dynamic routes:

- `GET /health` — load balancer / ECS health check (`{"status":"ok"}`)
- `POST /api/stats/query` — stats across leads for a region (JSON body); optional `X-Model` header if `model` is omitted in the body
- `POST /api/stats/lead-winners` — best verification statistic per lead for the current map region

The frontend loads **`/static/config.json`** and **`/static/zip/{5-digit}.json`** (not REST `/api/config` or `/api/zip`).

## Models

Models are registered in `model_registry.py`. Currently: **GFS** (0.25° global, 12z cycle, leads 1–14 days) and **NBM** (assembled daily `.npy` on a 0.25° US grid, 12z cycle). `compute_stats` discovers leads from real `f*_*.grib2` files and/or `f*_*.npy` files in each init directory.

## Production deployment (AWS Fargate)

The supported path is **AWS Fargate**: containerized FastAPI with `static_export/` on disk in the image (or on EFS). Stats are read from disk; the task role does **not** need S3 permissions for stats unless you add other features.

| Artifact | Role |
|----------|------|
| [Dockerfile](Dockerfile) | Image build; verifies `static_export/data/<model>/grid.json` exists |
| [deploy_fargate.sh](deploy_fargate.sh) | Runs `export_static.py`, verifies data, `docker build`, ECR push (**us-west-1** default); API deps are in the Dockerfile |

### Build pipeline order

The Docker image **fails to build** if `static_export/data/<model>/grid.json` is missing.

1. **`python export_static.py`** (or your CI equivalent).
2. **`docker build`**, or **`./deploy_fargate.sh`** for export + verify + build + push.

```bash
chmod +x deploy_fargate.sh
./deploy_fargate.sh
```

Examples:

- `IMAGE_TAG=v1.2.3 ./deploy_fargate.sh`
- `SKIP_EXPORT=1 ./deploy_fargate.sh` — use existing `static_export/`
- `SKIP_PUSH=1 ./deploy_fargate.sh` — build only, no ECR
- `AWS_REGION=us-east-1 ECR_REPOSITORY=my-api ./deploy_fargate.sh`
- `DOCKER_PLATFORM=linux/arm64 ./deploy_fargate.sh` — only if the ECS task is **ARM/Graviton**; default **`linux/amd64`** matches standard x86 Fargate (and avoids pull errors when building on Apple Silicon).

See `./deploy_fargate.sh --help` for the full header comment.

### ECS task environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MODELACCURACY_WARM_CACHE` | Optional | `1` / `true` / `yes` — preload every model’s `grid.json` into the in-process stats cache at startup (slower start, faster first request). |
| `PORT` | Optional | Listen port (default **8080**). Match the container port mapping and ALB target group. |

The container runs **one uvicorn** process per task (`Dockerfile` `ENTRYPOINT`; same command in `deploy_fargate.sh`) with `--proxy-headers` and `--forwarded-allow-ips '*'` behind an ALB. Stats caching in `backend/stats_query.py` is **in-process**; scale with more tasks or CPU, not multiple workers in one container.

### Same-origin (no CORS on the API)

The FastAPI app does **not** send CORS headers. Browsers only allow `fetch` from the **same site** as the page unless you add CORS at **CloudFront** (response headers policy) or a reverse proxy.

Recommended patterns:

- **CloudFront** with one hostname: route `/`, `/assets/*` to S3 and `/api/*`, `/static/*`, `/data/*` to the ALB so the SPA calls `/api/...` same-origin.
- **Local dev:** [frontend/vite.config.js](frontend/vite.config.js) proxies `/api`, `/static`, and `/data` to the backend.

### Caching

- **In-process:** `backend/stats_query.py` caches loaded data per process after first read.
- **HTTP:** `GET` under `/static/` and `/data/` get `Cache-Control: public, max-age=31536000, immutable` for CDN/browser caching when those paths go through CloudFront.

### Run the container locally

```bash
python export_static.py
docker build -t modelaccuracy-api:latest .
docker run --rm -p 8080:8080 modelaccuracy-api:latest
# Optional: -e MODELACCURACY_WARM_CACHE=1
curl -s http://127.0.0.1:8080/health
```

### Push to ECR (manual outline)

If not using `deploy_fargate.sh`:

```bash
export AWS_REGION=us-west-1
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
aws ecr create-repository --repository-name modelaccuracy-api --region "$AWS_REGION" 2>/dev/null || true
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
export ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/modelaccuracy-api:latest"
docker tag modelaccuracy-api:latest "$ECR_URI"
docker push "$ECR_URI"
```

### ECS + ALB (console outline)

1. **Cluster:** ECS → Fargate cluster (e.g. `modelaccuracy`).
2. **Task definition:** Fargate, 1 vCPU / 2 GiB to start; container image = your ECR URI; container port **8080**; optional env vars above; **awslogs** driver.
3. **Security groups:** ALB SG — inbound 443 (or 80 for tests); tasks SG — inbound **8080** from the ALB SG only.
4. **ALB:** Internet-facing, HTTPS recommended; target group **IP**, protocol HTTP port **8080**, health check path **`/health`**.
5. **Service:** Fargate, private subnets + NAT or public subnets + public IP as appropriate; attach to the target group; container port 8080.

**Alternatives to ALB:** NLB (L4), API Gateway + VPC Link (more moving parts). CloudFront still needs an origin (ALB, NLB, etc.); it does not replace a load balancer for ECS tasks.

### CloudFront (optional)

Typical flow: `Browser → CloudFront → ALB → Fargate`. Use one distribution with two origins (S3 or similar for SPA; ALB for `/api/*` and optionally `/static/*`, `/data/*`, `/health`). **`POST /api/stats/*`** is not cached by default at the edge (appropriate). Align cache policies with long TTLs for versioned static assets if desired. For cross-origin setups only, add CORS via CloudFront response headers.

### CI/CD example (GitHub Actions)

```yaml
name: Build and push API image
on:
  push:
    branches: [main]

jobs:
  docker:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install "fastapi>=0.115.0" "uvicorn[standard]>=0.32.0" "numpy>=2.0.0" "pydantic>=2.0.0" "boto3>=1.35.0"
      - run: python export_static.py
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          aws-region: us-west-1
      - run: chmod +x deploy_fargate.sh && SKIP_EXPORT=1 IMAGE_TAG="${{ github.sha }}" ./deploy_fargate.sh
      - run: aws ecs update-service --cluster modelaccuracy --service api --force-new-deployment
```

Adjust `pip install` / export steps if your export needs extra packages.

### Data on EFS instead of the image

If `static_export` is very large or changes often without rebuilding: create EFS, mount at e.g. `/app/static_export`, adjust the Dockerfile copy step (or use a placeholder), and populate EFS via a one-off task, DataSync, or CI. Ensure NFS (2049) security groups between tasks and EFS.

### Troubleshooting

| Symptom | Check |
|--------|--------|
| **`docker build` fails at verify** | Run `export_static.py`; ensure `static_export/data/<model>/grid.json` exists. |
| Target **unhealthy** | SG: ALB → task on **8080**; health path **`/health`**. |
| **403** from ALB | Listener rules / default action. |
| Browser **CORS** | Use same-origin path routing or CloudFront CORS headers. |
| **502** | CloudWatch Logs for the container. |
| Stale code after deploy | New image pushed? `aws ecs update-service … --force-new-deployment`. |

## Archived Lambda

Previous Lambda zip/container scripts live under **[archive/lambda/](archive/lambda/README.md)** for reference. They are **not** part of the default workflow.
