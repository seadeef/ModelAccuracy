# ModelAccuracy

Lead-day limits are centralized in `lead_config.py`:

- `LEAD_DAYS_MIN`
- `LEAD_DAYS_MAX`
- `FORECAST_HOURS`

Set `LEAD_DAYS_MAX = 14` (default) to support leads 1-14 consistently across downloader, compute, API, and frontend.

## PMTiles viewer (local)

1. Serve PMTiles as ZXY tiles:

```
pmtiles serve tiles_output/pmtiles --port=8080 --public-url=http://localhost:8080 --cors="*"
```

2. Serve the frontend (any static server works):

```
python -m http.server 8000 --directory frontend
```

3. Open the viewer:

```
http://localhost:8000/index.html
```

The viewer expects PMTiles named `season_<name>_lead_Y.pmtiles` (e.g., `season_winter_lead_1.pmtiles`)
and fetches TileJSON from `http://localhost:8080/<tileset>.json`.

## Bias query API (local)

The click-to-query popup uses a small API server:

```
pip install fastapi uvicorn
uvicorn bias_api:app --reload --port 8001
```

It reads bias stats from `stats/bias/ppt`. You can override with:

```
http://localhost:8001/api/bias?season=winter&lead=14&lat=40.0&lon=-100.0&stats_dir=/path/to/stats/bias/ppt
```

Frontend lead options are discovered from:

```
http://localhost:8001/api/config
```
