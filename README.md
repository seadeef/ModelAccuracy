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

Builds the flat `static_export/` tree for disk-backed serving: the bundled Vite SPA (`static_export/frontend/`: `index.html`, `assets/`, favicons), the root data assets (`config.json`, `zip/`, `tiles/`, `ranges/`, `admin/`, `wbd/`, `military/`), `static_export/model_data/` (grid metadata and verification `.bin` arrays), and `static_export/forecast/` (forecast `.bin` arrays + `forecast_calendar.json`). `config.json` is the only regular file at the root. Run this before local API use or a deploy.

- `python export_static.py` — full export (all of the above).
- `python export_static.py --static` — the whole S3 site tree except `model_data/` and the SPA: `zip/`, admin/wbd/military map TopoJSON/GeoJSON **and** the backend polygon files (`wbd/chunks/`, `boundaries.json`), `config.json`, `tiles/`, `ranges/`, and `forecast/` (bins + `forecast_calendar.json`).
- `python export_static.py --data` — only `static_export/model_data/` (verification `.bin` + `grid.json`).
- `python export_static.py --forecast` — only `static_export/forecast/` (forecast bins + `forecast_calendar.json`).
- `python export_static.py --frontend` — only the bundled SPA under `static_export/frontend/`.

Boundary flags (combinable with each other, not with build modes): `--zip`, `--admin`, `--huc`, `--military`. Each writes both halves of its output — `--admin`/`--huc`/`--military` produce the map TopoJSON/GeoJSON *and* the polygon JSON the backend reads for region masks (from disk in dev, from S3 on Lambda). WBD chunk polygons are coordinate-rounded on export (`CHUNK_COORD_DECIMALS`, ~11 m) — half the bytes, bit-identical masks.

Use `python export_static.py --help` for `--output` and `--clean` (full export only).

``boundary_data/`` holds only data (like ``model_data/``): normalized ``zip/``,
``admin/``, ``wbd/layers/``, and ``military/`` outputs at the top level, plus a
``cache/`` for the large transient raw downloads (WBD GDB, MIRTA zip). It is
populated by ``load_boundaries.py``.
Export modes write the deploy tree ``static_export/``; the S3 deploys only copy from there:

```bash
python load_boundaries.py                 # zip + admin + wbd + military (downloads ~2.7 GB GDB for --wbd)
./deploy_static.sh                        # download forecast → export --static + --data → sync S3 + invalidate
```

`load_boundaries.py` takes additive `--zip` / `--admin` / `--wbd` / `--military` selectors (all run when none are given). For local dev without the big downloads: `python load_boundaries.py --wbd-sample --military-sample` then `python export_static.py --static`.

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

## Production deployment (AWS, serverless)

Production is a single CloudFormation stack (`infra/stack.yaml`) — **no Docker, no ECR, no ALB, no Fargate, no NAT**:

- **CloudFront** serves the SPA + data files from **S3** (default behavior, OAC) and routes `/api/*` to a **Lambda Function URL**.
- **Lambda** (ARM64, zip package, FastAPI via Mangum) reads *everything* from S3 on demand: stats bins under `model_data/`, WBD chunk polygons per-parent on first use, admin/military `boundaries.json`, forecast bins. The package is code-only (~28 MB).
- The Function URL is public but CloudFront attaches an `x-origin-verify` secret header; the app 403s anything without it (OAC can't sign browser POST bodies, hence the header instead).
- **DynamoDB** (on-demand) for saved shapes, **Cognito** for auth, an **EventBridge** `rate(5 minutes)` `{"warm": true}` ping to keep one execution environment resident.

Fixed cost is a few dollars a month at light traffic (CloudFront/S3 pennies, Lambda mostly inside the free tier, DynamoDB on-demand pennies, Cognito ESSENTIALS free under 10k MAU).

There are four deploy layers; each script loads `.env` from the repo root and is idempotent:

| Script | Deploys | Notes |
|--------|---------|-------|
| [deploy_infra.sh](deploy_infra.sh) | The whole stack ([infra/stack.yaml](infra/stack.yaml)) | One command per account/region; prints the `.env` block to paste. Re-run any time |
| [deploy_api.sh](deploy_api.sh) | Lambda code | Builds the zip from manylinux/aarch64 wheels (`requirements.lambda.txt`) + `backend/`, `statistics_plugins/`, registry modules; `update-function-code` + `/health` smoke test |
| [deploy_static.sh](deploy_static.sh) | ALL data → S3 | Downloads latest forecast, `export_static.py --static` + `--data`, syncs browser assets *and* backend-read data (`model_data/`, `wbd/chunks/`, `boundaries.json`, forecast bins), invalidates mutable paths, recycles Lambda environments so warm caches drop |
| [deploy_frontend.sh](deploy_frontend.sh) | Bundled SPA → `s3://$BUCKET/` (root) | Invalidates non-hashed files; `/assets/` URLs are content-hashed |

The SPA and data share the bucket root; each sync uses `--delete` only within its own subdirs so they don't clobber each other.

### Infrastructure (`deploy_infra.sh`)

```bash
./deploy_infra.sh          # create or update the whole stack (idempotent)
```

Moving to a **different region or account** is: point `AWS_PROFILE`/`AWS_REGION` at the target, run `./deploy_infra.sh`, paste the printed block into `.env`, then run the three other deploy scripts. Details handled for you:

- the CloudFront→Lambda origin-verify secret is generated once and kept in SSM Parameter Store (`/<stack>/origin-verify-secret`, free tier);
- the Cognito OAuth callback URL needs the CloudFront domain, which doesn't exist on the first pass — the script automatically re-deploys with it (fast, client-only update);
- reuse of existing user data when rebuilding in the same account: `EXISTING_USER_POOL_ID` / `EXISTING_USER_POOL_CLIENT_ID` / `EXISTING_COGNITO_OAUTH_BASE_URL` / `EXISTING_USER_ITEMS_TABLE`;
- the bucket, DynamoDB table, and user pool are `DeletionPolicy: Retain` — a stack teardown never destroys data.

### API code (`deploy_api.sh`)

```bash
./deploy_api.sh                   # build zip (no Docker) → update-function-code → smoke test
SKIP_SMOKE=1 ./deploy_api.sh
```

Code and data deploy independently: a stats recompute needs no API deploy, and an API deploy ships no data.

### Data (`deploy_static.sh`)

Takes no arguments. Downloads the latest forecast, rebuilds `static_export/` (`--static` + `--data`), syncs every data subtree to S3, invalidates the mutable browser paths, and bumps a Lambda env var (`DATA_DEPLOYED_AT`) so warm execution environments recycle instead of serving pre-deploy cached bins.

```bash
./deploy_static.sh                       # forecast → export → sync + invalidate + cache flush
SKIP_BUILD=1 ./deploy_static.sh          # skip download+export, upload existing static_export/
SKIP_INVALIDATE=1 ./deploy_static.sh     # skip the CloudFront invalidation
SKIP_FLUSH=1 ./deploy_static.sh          # skip the Lambda cache-flush recycle
```

Cache-Control mirrors `LongCacheStaticMiddleware` in `backend/api.py`: `zip/`, `admin/`, `wbd/`, and `military/` are 1 yr immutable; `tiles/`, `ranges/`, and `config.json` are 300 s `must-revalidate`. Backend-read keys (`model_data/`, `wbd/chunks/`, `boundaries.json`, forecast bins) bypass CloudFront entirely. The forecast S3 prefix is derived from `DATA_S3_URI` (sibling of the stats prefix); see the script header for the parsing rule.

### Frontend (`deploy_frontend.sh`)

```bash
./deploy_frontend.sh              # rebuild SPA + sync + invalidate
SKIP_BUILD=1 ./deploy_frontend.sh # upload existing static_export/
SKIP_INVALIDATE=1 ./deploy_frontend.sh
```

Uploads the SPA contents to the bucket root `s3://$BUCKET/`: `index.html` (no-cache), `assets/` (1 yr immutable), and `favicon.*`; CloudFront invalidation covers only the non-hashed files. Requires `CLOUDFRONT_DISTRIBUTION_ID` in `.env`.

### Environment variables

The production Lambda gets its environment from the stack (`infra/stack.yaml`), not from `.env`. Locally, `.env` at the repo root is read by the deploy scripts and by `backend/api.py`:

| Variable | Description |
|----------|-------------|
| `DATA_S3_URI` | `s3://bucket/model_data` — the stats prefix. Forecasts are read from a `forecast/` folder that is a sibling of the prefix; the site assets (SPA, `wbd/chunks/`, `boundaries.json`, …) live at the bucket root. Printed by `deploy_infra.sh` |
| `CLOUDFRONT_DISTRIBUTION_ID` | CloudFront distribution to invalidate (`deploy_static.sh`, `deploy_frontend.sh`) |
| `LAMBDA_FUNCTION_NAME` | API function name (default `<stack>-api`); used by `deploy_api.sh` and the `deploy_static.sh` cache flush |
| `STACK_NAME` | CloudFormation stack name (default `raincheck`, lowercase) |
| `AWS_REGION` | Default `us-west-1` |
| `AWS_PROFILE` | Optional; exported when set |
| `COGNITO_USER_POOL_ID` / `COGNITO_APP_CLIENT_ID` | Cognito pool/client (local dev; the Lambda gets them from the stack) |
| `COGNITO_REGION` | Defaults to `AWS_REGION` |
| `COGNITO_DOMAIN_PREFIX` / `COGNITO_OAUTH_BASE_URL` | Hosted UI domain label, or the full https origin (one required for auth) |
| `DYNAMODB_USER_ITEMS_TABLE` | DynamoDB table for saved shapes |
| `MAPTILER_API_KEY` | Optional MapTiler key; baked into `config.json` by `export_static.py` |
| `WARM_CACHE` | `1` — preload all models' `grid.json` at startup (set on the Lambda by the stack) |
| `ORIGIN_VERIFY_SECRET` | Set by the stack on the Lambda only — enables the CloudFront origin check. Never set it locally |
| `DEV` | `1` / `true` — backend skips production cache headers, reaches for disk over S3 |

### Run the API locally

Dev mirrors production through the same store abstraction — the Lambda fetches `model_data/`, `wbd/chunks/`, and `boundaries.json` from S3 on demand; locally the identical paths resolve under `static_export/`:

```bash
pip install -r requirements.api.txt
python export_static.py                  # or --static/--data for just the parts you need
uvicorn backend.api:app --port 8000      # DEV=1 in .env keeps every read local
curl -s http://127.0.0.1:8000/health
```

### Notes

- **CORS:** The API sends no CORS headers. Use same-origin path routing (Vite proxy in dev; CloudFront in prod) to avoid browser errors.
- **Caching:** Stats are cached in-process (bounded LRUs) after first read. Lambda scales by adding execution environments, each with its own cache; `deploy_static.sh` recycles them after a data deploy so no environment serves stale bins.