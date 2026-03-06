# ModelAccuracy

## Statistics pipeline

Statistics are computed by plugin and written under separate directories:

- `stats/<stat_name>/metadata.npz`
- `stats/<stat_name>/lead_<N>.npz`
- `stats/<stat_name>/lead_1_7.npz`
- `stats/<stat_name>/lead_7_14.npz`
- `stats/<stat_name>/lead_1_10.npz`

The compute step writes all individual lead-day files first, then writes the combined windows:

- `1-7` day average
- `7-14` day average
- `1-10` day average

Current enabled statistics are:

- `bias`
- `sacc` (spatial anomaly correlation coefficient)
- `nrmse` (normalized root mean square error)
- `nmad` (normalized mean absolute difference)

Run computations:

```
python compute_stats.py
```

Lead-day limits are centralized in `lead_config.py`:

- `LEAD_DAYS_MIN`
- `LEAD_DAYS_MAX`
- `FORECAST_HOURS`

Set `LEAD_DAYS_MAX = 14` (default) to support leads 1-14 consistently across downloader, compute, API, and frontend.

## PMTiles viewer (local)

Basemap configuration is served by the backend API (`/api/config`) from environment variables:

- `MAPTILER_API_KEY`

If `MAPTILER_API_KEY` is empty, the frontend falls back to a demo MapLibre style.

1. Serve PMTiles as ZXY tiles:

```
pmtiles serve tiles_output/pmtiles --port=8080 --public-url=http://localhost:8080 --cors="*"
```

Generate tiles for all enabled statistics:

```
python compute_stats_tiles.py --max-zoom 6
```

2. Serve the frontend (any static server works):

```
python -m http.server 8000 --directory frontend
```

3. Open the viewer:

```
http://localhost:8000/index.html
```

The viewer expects PMTiles under statistic subdirectories, e.g.:

- `tiles_output/pmtiles/bias/lead_1.pmtiles`
- `tiles_output/pmtiles/sacc/lead_1.pmtiles`

It fetches TileJSON from:

- `http://localhost:8080/<statistic>/lead_<N>.json` (single lead day)
- `http://localhost:8080/<statistic>/lead_1_7.json` (window average), etc.

### Frontend layer controls

The viewer supports:

- **View mode**: `Both`, `Landmarks only`, `Weather only`
- **Detail mode**: `Low` and `High`
  - `Low` caps basemap zoom and hides lower-priority landmark layers.
  - `High` allows higher basemap zoom and full landmark detail.
- **Opacity controls**: separate sliders for weather and landmarks.

There is no automatic API-credit tracking in the frontend; detail level is user-controlled.
Remember to keep required attribution for your chosen MapTiler style per MapTiler terms.

## Statistics query API (local)

The click-to-query popup uses a small API server:

```
pip install fastapi uvicorn
export MAPTILER_API_KEY="your_public_maptiler_key"
uvicorn bias_api:app --reload --port 8001
```

It reads statistics from `stats/`. You can override with:

```
http://localhost:8001/api/stats?lead=14&lat=40.0&lon=-100.0&stats_root=/path/to/stats
http://localhost:8001/api/stats?lead=1-7&lat=40.0&lon=-100.0&stats_root=/path/to/stats
```

Response shape:

```
{
  "lead": "14",
  "lat": 40.0,
  "lon": -100.0,
  "stats": {
    "bias": {"value": 1.23, "units": "mm", "no_data": false},
    "sacc": {"value": 72.0, "units": "%", "no_data": false},
    "nrmse": {"value": 38.0, "units": "%", "no_data": false},
    "nmad": {"value": 24.0, "units": "%", "no_data": false}
  }
}
```

Frontend lead options are discovered from:

```
http://localhost:8001/api/config
```

ZIP lookup endpoint for frontend map centering:

```
http://localhost:8001/api/zip?zip=80302
```

By default, ZIP lookups read `zip_lookup.csv` from the project root.
Override the file path with:

```
export ZIP_LOOKUP_CSV=/absolute/path/to/zip_lookup.csv
```
