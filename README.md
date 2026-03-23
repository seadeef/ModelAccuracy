# ModelAccuracy

Precipitation forecast verification system. Downloads GFS model forecasts and PRISM observations, computes verification statistics, generates map tiles, and serves an interactive viewer.

## Quick start

```bash
# 1. Install dependencies
pip install fastapi uvicorn numpy xarray cfgrib rasterio rioxarray matplotlib

# 2. Download data (all models + PRISM, defaults to today)
python download.py --start-date 2024-01-01 --end-date 2024-12-31

# 3. Compute statistics
python compute_stats.py

# 4. Generate map tiles
python compute_tiles.py

# 5. Start the API server
echo "your_key" > .maptiler_key       # optional, falls back to demo style
uvicorn backend.api:app --reload --port 8000

# 6. Open the viewer
open http://localhost:8000
```

The frontend is served on port 8000 and talks to the API on port 8001. If `.maptiler_key` is not present, the map falls back to a demo MapLibre style.

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

## Statistics

Verification statistics (computed from GFS + PRISM pairs):

- **bias** — mean precipitation bias (mm)
- **sacc** — spatial anomaly correlation coefficient (%)
- **nrmse** — normalized root mean square error (%)
- **nmad** — normalized mean absolute difference (%)

Display statistics:

- **forecast** — latest model precipitation forecast (mm)

## API

The backend serves at `http://localhost:8001` by default.

- `GET /api/config` — models, statistics, lead options, accumulation modes, current month/season
- `GET /api/stats?model=gfs&lead=7&lat=40&lon=-100` — point statistics query (model keys: `gfs`, `nbm`)
  - `model` may be omitted or empty; then `X-Model: nbm` (or another key) is used, else the server `DEFAULT_MODEL`
  - `&period=monthly&month=03` — monthly stats
  - `&period=seasonal&season=mam` — seasonal stats
- `GET /api/zip?zip=80302` — ZIP code lookup for map centering
- `GET /tiles/<model>/<statistic>/lead_<n>.png` — map overlay images (static files under `tiles_output/`)

## Models

Models are registered in `model_registry.py`. Currently: **GFS** (0.25° global, 12z cycle, leads 1–14 days) and **NBM** (assembled daily `.npy` on a 0.25° US grid, 12z cycle). `compute_stats` discovers leads from real `f*_*.grib2` files and/or `f*_*.npy` files in each init directory.

## Production deployment

Deploy the API with **Docker on AWS Fargate** (ECS + ALB). See **[DEPLOYMENT.md](DEPLOYMENT.md)** and **[docs/FARGATE.md](docs/FARGATE.md)**.

Legacy AWS Lambda packaging scripts are **archived** under [`archive/lambda/`](archive/lambda/README.md).

