# ModelAccuracy

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
http://localhost:8001/api/bias?season=winter&lead=1&lat=40.0&lon=-100.0&stats_dir=/path/to/stats/bias/ppt
```
