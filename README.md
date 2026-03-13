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
export MAPTILER_API_KEY="your_key"   # optional, falls back to demo style
uvicorn backend.api:app --reload --port 8001

# 6. Serve the frontend (any static server works)
python -m http.server 8000 --directory frontend

# 7. Open the viewer
open http://localhost:8000
```

The frontend is served on port 8000 and talks to the API on port 8001. If `MAPTILER_API_KEY` is not set, the map falls back to a demo MapLibre style.

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
stats_output/{model}/{stat}/
  metadata.npz
  lead_{N}.npz                    # yearly (sum of all months)
  lead_{window}.npz               # yearly windows
  monthly/01/ ... 12/
    lead_{N}.npz                  # per-month
  seasonal/djf/ mam/ jja/ son/
    lead_{N}.npz                  # sum of 3 months
```

### `compute_tiles.py`

Generates PNG tile images for the map viewer.

```bash
python compute_tiles.py                # all models
python compute_tiles.py --model gfs    # just GFS
```

Generates tiles for yearly, current month, and current season. Forecast tiles are yearly only.

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
- `GET /api/stats?model=gfs&lead=7&lat=40&lon=-100` — point statistics query
  - `&period=monthly&month=03` — monthly stats
  - `&period=seasonal&season=mam` — seasonal stats
- `GET /api/zip?zip=80302` — ZIP code lookup for map centering

## Models

Models are registered in `model_registry.py`. Currently: **GFS** (0.25° global, 12z cycle, leads 1–14 days).

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `MAPTILER_API_KEY` | MapTiler API key for basemap | demo style fallback |
| `ZIP_LOOKUP_CSV` | Path to ZIP code lookup CSV | `backend/zip_lookup.csv` |
| `TILES_OUTPUT` | Tiles output directory | `tiles_output` |
