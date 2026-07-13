# ModelAccuracy

Precipitation forecast verification system. Downloads numerical and ML model forecasts (GFS, NBM, WeatherNext 2, AIFS) and PRISM observations, computes verification statistics, generates map tiles, and serves an interactive viewer.

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

Open the URL Vite prints (default **http://localhost:5173**). Optional: set `MAPTILER_API_KEY` in `.env` (read by `export_static.py` and baked into `config.json`) for a custom basemap; without it the SPA falls back to a free OSM-style tileset.

## Scripts

### `download.py`

Downloads model forecasts (GFS, NBM, WeatherNext 2, AIFS) and PRISM observations.

```bash
python download.py --start-date 2025-01-01 --end-date 2025-01-31   # all models + PRISM
python download.py --model gfs --start-date 2025-01-01              # just GFS + PRISM
python download.py --model gfs --start-year 2024 --end-year 2025    # full years
python download.py --forecast                                        # today's forecast (probes cycles per model)
python download.py --catchup                                         # fill gap through yesterday
python download.py --no-prism --model nbm                            # skip PRISM
```

- `--model {gfs,nbm,weathernext2,aifs}`: download a specific model (default: all registered models)
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

Computes the consistency statistics (**jumpiness** and **convergence** — see
the Statistics section) by pairing *successive runs for the same valid date*,
which is why it is a separate pipeline from `compute_stats.py`'s per-(init,
lead) pairing. Single-process and incremental: a full run is minutes per
model; a daily catch-up folds in only newly scorable valid dates (a date
becomes scorable once its last missing lead or PRISM day arrives).

Outputs under `stats_output/{model}/`:

```
jumpiness/
  pair_{L}.npz                      # yearly; pair_L = runs issued at leads L+1 and L
  monthly/MM/pair_{L}.npz           # the primitive (num_/den_ per q,n combo)
  seasonal/{djf,mam,jja,son}/pair_{L}.npz
convergence/
  lead_{N}.npz (+ monthly/, seasonal/)   # sum_squared_error / sample_count / value
consistency_daily.npz               # exact per-valid-date CONUS J and e_L curves
consistency_manifest.json           # processed valid dates; param changes force recompute
```

Changing the threshold or scale sets (`statistics_plugins/consistency.py`)
invalidates the manifest and forces a full recompute. Export packs the
accumulator pairs into two-plane `.bin`s (`export_static.py --data`); no tiles
are rendered for these statistics.

### `compute_tiles.py`

Generates PNG tile images for the map viewer.

```bash
python compute_tiles.py                # all models
python compute_tiles.py --model gfs    # just GFS
```

Generates tiles for yearly, current month, and current season. Forecast tiles are yearly only. PNG layout mirrors stats: `tiles_output/<model>/<statistic>/lead_*.png` (plus `monthly/` / `seasonal/`). Map overlay extent is fixed in `backend/tile_overlay_constants.py` and `frontend/src/lib/constants.js` — keep these in sync.

### `export_static.py`

Builds the flat `static_export/` tree for disk-backed serving: the bundled Vite SPA (`static_export/frontend/`: `index.html`, `assets/`, favicons), the root data assets (`config.json`, `zip/`, `tiles/`, `ranges/`, `admin/`, `wbd/`, `military/`), `static_export/model_data/` (grid metadata and verification `.bin` arrays), and `static_export/forecast/` (forecast `.bin` arrays + `forecast_calendar.json`). `config.json` is the only regular file at the root. Run this before `docker build` or local API use.

- `python export_static.py` — full export (all of the above).
- `python export_static.py --image` — Docker image: admin/HUC/military polygons + `static_export/model_data/`.
- `python export_static.py --static` — S3/CloudFront: `zip/`, admin/HUC TopoJSON, military GeoJSON, `config.json`, `tiles/`, `ranges/`, and `forecast/` (bins + `forecast_calendar.json`).
- `python export_static.py --data` — only `static_export/model_data/` (verification `.bin` + `grid.json`).
- `python export_static.py --forecast` — only `static_export/forecast/` (forecast bins + `forecast_calendar.json`).
- `python export_static.py --frontend` — only the bundled SPA under `static_export/frontend/`.

Boundary flags (combinable with each other, not with build modes): `--zip`, `--admin`, `--huc`, `--military`. Each writes both halves of its output — `--admin`/`--huc`/`--military` produce the TopoJSON/GeoJSON used by S3 *and* the polygon JSON baked into the image.

Use `python export_static.py --help` for `--output` and `--clean` (full export only).

``boundary_data/`` holds only data (like ``model_data/``): normalized ``zip/``,
``admin/``, ``wbd/layers/``, and ``military/`` outputs at the top level, plus a
``cache/`` for the large transient raw downloads (WBD GDB, MIRTA zip). It is
populated by ``load_boundaries.py``.
Export modes write the deploy tree ``static_export/``; Docker and S3 deploys only copy from there:

```bash
python load_boundaries.py                 # zip + admin + wbd + military (downloads ~2.7 GB GDB for --wbd)
python export_static.py --image           # Docker: admin/HUC/military polys + data/
./deploy_fargate.sh                       # COPY static_export/{data,static/...} into image
python export_static.py --static          # S3: zip + topos + military geojson + config/tiles/ranges + forecast
./deploy_static.sh                        # download forecast → export --static → sync S3 + invalidate
```

`load_boundaries.py` takes additive `--zip` / `--admin` / `--wbd` / `--military` selectors (all run when none are given). For local dev without the big downloads: `python load_boundaries.py --wbd-sample --military-sample` then `python export_static.py --image`.

`--military` loads the DoD **DISDI MIRTA** dataset (Military Installations, Ranges, and Training Areas — the public `installations_ranges.zip` from acq.osd.mil, a yearly FY release, ~4 MB): 820 US installation sites keyed by a `siteName + state` slug, with boundary polygons (simplified to ~100 m), the published site point, service component, and operational status. The acq.osd.mil server sends an incomplete TLS chain, so the loader completes verification with the pinned IdenTrust chain in `certs/identrust_trustid_server_ca_o1.pem` (never disables verification). Frontend: the shield tool in the draw toolbar shows every site as a marker + boundary; clicking one selects it as the analysis region (`region.type = "military"`, masked server-side from `military/boundaries.json`, with a containing-cell fallback for sites smaller than a grid cell).

## Statistics

Verification statistics (computed per (init, lead) from model + PRISM pairs by
`compute_stats.py`; rendered as map tiles and panel charts):

- **cc** — correlation coefficient (Pearson on raw values, unitless −1..1)
- **nrmse** — normalized root mean square error (%)
- **nmad** — normalized mean absolute difference (%)
- **rb** — relative bias, Σ(model−obs) / Σobs (%)

- **jumpiness** — run-to-run jumpiness J = 1 − FSS between the two successive
  runs valid on the same date, after binarizing at threshold q ∈ {1, 5, 10,
  25} mm/day and converting to neighborhood fractions over a square window of
  n ∈ {25, 50, 100} **kilometers** (latitude-aware cell windows with
  fractional weights, not fixed cell counts). J = 0 → successive runs agree;
  J → 1 → complete flip. The value at lead L compares the runs issued at
  leads L+1 and L. If neither run exceeds q anywhere in the region over the
  pooled dates the denominator is zero and the value is missing, not zero.
- **convergence** — RMSE vs PRISM per lead (mm/day), over exactly the same
  valid dates as jumpiness.

Consistency-statistics ground rules (from their definition):

- A valid date is *scorable* for a model only if **all** leads
  1..lead_days_max exist on disk **and** the PRISM day exists — so the J and
  e_L curves always describe the identical valid-date set and can be read
  jointly.
- Aggregation is **pooled**: monthly per-cell FSS numerator/denominator (and
  squared-error/count) sums are the storage primitive; seasons/years sum
  months, and a region value is the ratio of region-summed accumulators
  (standard multi-case FSS aggregation), *not* the mean of per-date values.
  The exact per-date CONUS-wide curves are kept in
  `stats_output/{model}/consistency_daily.npz` for offline analysis.
- Derived lock-in quantities (computed client-side from the two curves):
  **L_stab** — largest lead from which J stays below `lock_in_fraction`
  (0.75) of its own curve maximum; **L_acc** — the analogous lead on the e_L
  curve; **lock-in gap = L_stab − L_acc** (positive → the forecast stopped
  changing before it became accurate: false confidence).
- Cross-model comparisons are valid only on identical (q, n, lead-pair,
  region, period, valid-date set) slices; the API reports the pooled date
  count with every value. AIFS jumpiness is a property of a single model
  version — values from different AIFS versions are different statistics and
  must never be pooled.

Display statistics:

- **forecast** — latest model precipitation forecast (mm)

## API and static URLs

The FastAPI app (`backend.api:app`) serves both the SPA and the data files from the site root (handy for running the backend standalone); in normal dev you'd use Vite, and in production CloudFront/S3 fronts the static site. The API mounts:

- `/…` (site root) — data files from the flat `static_export/` root (`/config.json`, `/zip/…`, `/tiles/…`, `/ranges/…`, `/admin/…`, `/wbd/…`, `/model_data/…`, `/forecast/…`) **and** the bundled SPA (`/` → `index.html`, `/assets/…`, `/favicon.svg`) from `static_export/frontend/`. The backend serves both from one mount that searches the data root then `frontend/`, registered after all `/api` routes.

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
| `weathernext2` | WeatherNext 2 (WNXT) | GCS Zarr ensemble mean | 00/06/12/18z (latest complete) | 1–10 d |
| `aifs` | AIFS | ECMWF open data S3 | 12z | 1–14 d |

## Production deployment (AWS)

Production splits `static_export/` across three deploy paths. Each shell script loads `.env` from the repo root and runs `python3 export_static.py --<mode>` to refresh just its subtree (or use `SKIP_BUILD=1` / `SKIP_EXPORT=1` to reuse what's already on disk).

| Artifact | Refreshes | Notes |
|----------|-----------|-------|
| [Dockerfile](Dockerfile) | API image | Bakes `static_export/model_data/`, `static_export/admin/boundaries.json`, `static_export/wbd/chunks/`, and `static_export/military/boundaries.json`; fails if any is missing |
| [deploy_fargate.sh](deploy_fargate.sh) | API image → ECR (+ optional ECS redeploy) | Runs `export_static.py --image`, verifies data, `docker build`, ECR push (us-west-1 default) |
| [deploy_static.sh](deploy_static.sh) | `static_export/` root data assets → `s3://$BUCKET/` (root) and `static_export/forecast/` → S3 | No flags: downloads the latest forecast, runs `export_static.py --static`, syncs both subtrees, and invalidates the data paths (`/config.json`, `/zip/*`, `/admin/*`, …) |
| [deploy_frontend.sh](deploy_frontend.sh) | Bundled SPA (`static_export/frontend/`: `index.html`, `assets/`, favicons) → `s3://$BUCKET/` (root) | Invalidates non-hashed files; `/assets/` URLs are content-hashed |

Routing in production: CloudFront fronts S3 (the SPA at `/` and data files at the site root: `/config.json`, `/wbd/*`, …) and the ALB-backed Fargate API (`/api/*`). Both the SPA and data share the bucket root; each deploy script uses `--delete` only within its own subdirs so they don't clobber each other. The backend reads forecast `.bin` files directly from S3 (refreshed every ~5 min); verification data is read from disk inside the container.

### Fargate (`deploy_fargate.sh`)

The Docker image **fails to build** if `static_export/model_data/<model>/grid.json`, `static_export/admin/boundaries.json`, or `static_export/wbd/chunks/` is missing. Fargate deploy refreshes **verification data** only — run `deploy_static.sh` and/or `deploy_frontend.sh` for the other parts.

```bash
chmod +x deploy_fargate.sh
./deploy_fargate.sh
```

Common flags:

- `IMAGE_TAG=v1.2.3 ./deploy_fargate.sh`
- `SKIP_EXPORT=1 ./deploy_fargate.sh` — use existing `static_export/model_data/`
- `SKIP_PUSH=1 ./deploy_fargate.sh` — build only, no ECR push
- `./deploy_fargate.sh --tag-latest` (or `ECR_PUSH_LATEST=1`) — also push `:latest`
- `./deploy_fargate.sh --force-redeploy` (or `ECS_FORCE_REDEPLOY=1`) — register a new task-def revision pinned to `ECR_REPOSITORY:IMAGE_TAG` and roll the service onto it (deterministic, unlike a bare `--force-new-deployment`); needs `ECS_CLUSTER`/`ECS_CLUSTER_ARN` + `ECS_SERVICE`/`ECS_SERVICE_ARN` (or interactive prompt)
- `AWS_REGION=us-east-1 ECR_REPOSITORY=my-api ./deploy_fargate.sh`
- `DOCKER_PLATFORM=linux/arm64 ./deploy_fargate.sh` — build for ARM/Graviton; default is `linux/amd64`

The default `IMAGE_TAG` is `git-<sha>-data<digest>` (digest over the baked-in `static_export/model_data` bins), so a stats-only refresh at the same commit still produces a new immutable tag; a dirty tree (incl. untracked files) uses `git-<sha>-dirty-<ts>`. Each push (re)applies an **ECR lifecycle policy** keeping the last 10 `git-*` images and expiring untagged after 1 day, so per-build tags don't accumulate.

**Graviton (ARM64) cutover:** `--force-redeploy` aborts on an image/task-def CPU-arch mismatch. To switch: set the ECS task def's `runtimePlatform.cpuArchitecture` to `ARM64`, then deploy with `DOCKER_PLATFORM=linux/arm64` (~20% Fargate compute savings; all deps ship aarch64 wheels). Watch the first ARM deploy.

### Static + forecast (`deploy_static.sh`)

Takes no arguments. It runs `download.py --forecast`, then `export_static.py --static`
(which builds both the `static_export/` root data assets and `static_export/forecast/`), syncs both
subtrees to S3 (data at the bucket root, forecast under the forecast prefix), and invalidates the data paths.

```bash
./deploy_static.sh                       # download forecast → export --static → sync + invalidate
SKIP_BUILD=1 ./deploy_static.sh          # skip download+export, upload existing static_export/
SKIP_INVALIDATE=1 ./deploy_static.sh     # skip the CloudFront invalidation
```

Cache-Control mirrors `LongCacheStaticMiddleware` in `backend/api.py`: `zip/`, `admin/`, `wbd/`, and `military/` are 1 yr immutable; `tiles/`, `ranges/`, and `config.json` are 300 s `must-revalidate`. The forecast S3 prefix is derived from `DATA_S3_URI` (sibling of the stats prefix); see the script header for the parsing rule.

### Frontend (`deploy_frontend.sh`)

```bash
./deploy_frontend.sh              # rebuild SPA + sync + invalidate
SKIP_BUILD=1 ./deploy_frontend.sh # upload existing static_export/
SKIP_INVALIDATE=1 ./deploy_frontend.sh
```

Uploads the SPA contents to the bucket root `s3://$BUCKET/`: `index.html` (no-cache), `assets/` (1 yr immutable), and `favicon.*`; CloudFront invalidation covers only the non-hashed files. Requires `CLOUDFRONT_DISTRIBUTION_ID` in `.env`.

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
| `MAPTILER_API_KEY` | Optional MapTiler key; baked into `config.json` by `export_static.py` |
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