<script>
  import { onMount, onDestroy } from 'svelte';
  import maplibregl from 'maplibre-gl';
  import 'maplibre-gl/dist/maplibre-gl.css';
  import { appConfig, ui, accuracyStatKeys } from '../state.svelte.js';
  import { FALLBACK_STYLE_URL, TILE_IMAGE_BOUNDS_WGS84 } from '../constants.js';
  import { fetchConfig } from '../api.js';
  import {
    tileUrl, preloadLeads, clearLeadImages, getLeadImage,
    compositeToDataUrl, getModelLeadBounds,
  } from '../tile.js';
  import { drawToolGlyph } from '../appIcons.js';

  const [oW, oS, oE, oN] = TILE_IMAGE_BOUNDS_WGS84;
  const overlayCoords = [[oW, oN], [oE, oN], [oE, oS], [oW, oS]];

  const sourceId = 'bias-source';
  const layerId = 'bias-layer';

  let mapContainer;
  let map = null;
  let initialLoad = true;

  function mapStyleUrl() {
    if (!appConfig.maptilerApiKey) return FALLBACK_STYLE_URL;
    return `https://api.maptiler.com/maps/streets-v2/style.json?key=${appConfig.maptilerApiKey}`;
  }

  function removeBiasLayer() {
    if (!map) return;
    if (map.getLayer(layerId)) map.removeLayer(layerId);
    if (map.getSource(sourceId)) map.removeSource(sourceId);
  }

  function addRasterLayer() {
    // Insert below the dim mask so the cutout overlay can dim the raster.
    // Stack (bottom→top): basemap → raster → dim-mask → draw-fill → draw-line → draw-point
    const beforeLayer = map.getLayer(dimLayerId) ? dimLayerId
                      : map.getLayer(drawFillLayerId) ? drawFillLayerId
                      : undefined;
    map.addLayer({
      id: layerId, type: 'raster', source: sourceId,
      paint: { 'raster-opacity': ui.weatherOpacity },
    }, beforeLayer);
  }

  let lastShownUrl = $state('');

  /** URL or data URL of the raster currently on the map (for client-side PNG export). */
  export function getCurrentOverlayUrl() {
    return lastShownUrl;
  }

  function showOnMap(url) {
    if (!map) return;
    if (url === lastShownUrl) return;
    lastShownUrl = url;
    const src = map.getSource(sourceId);
    if (src) {
      src.updateImage({ url, coordinates: overlayCoords });
    } else {
      map.addSource(sourceId, { type: 'image', url, coordinates: overlayCoords });
      addRasterLayer();
    }
  }

  function resetOverlay() {
    lastShownUrl = '';
  }

  export function loadTilesetInterp(statistic, frac) {
    if (!map) return;
    const { min: leadMin, max: leadMax } = getModelLeadBounds(appConfig.models, ui.model);
    let lo = Math.floor(frac);
    let hi = Math.ceil(frac);
    lo = Math.max(leadMin, Math.min(leadMax, lo));
    hi = Math.max(leadMin, Math.min(leadMax, hi));
    if (hi < lo) hi = lo;
    const t = frac - lo;

    const imgLo = getLeadImage(lo);
    const imgHi = getLeadImage(hi);

    if (t === 0 || lo === hi) {
      if (imgLo) showOnMap(tileUrl(ui.model, statistic, lo, ui.period, ui.month, ui.season));
    } else if (imgLo && imgHi) {
      showOnMap(compositeToDataUrl(imgLo, imgHi, t));
    } else if (imgLo) {
      showOnMap(tileUrl(ui.model, statistic, lo, ui.period, ui.month, ui.season));
    } else if (imgHi) {
      showOnMap(tileUrl(ui.model, statistic, hi, ui.period, ui.month, ui.season));
    }
    ui.statusMessage = 'Idle';
  }

  function applyLayerState() {
    if (!map) return;
    if (map.getLayer(layerId)) {
      map.setPaintProperty(layerId, 'raster-opacity', ui.weatherOpacity);
    }
  }

  export function handleChange() {
    if (!map) return;
    loadTilesetInterp(ui.statistic, ui.leadFractional);
    if (initialLoad) {
      map.fitBounds([[oW, oS], [oE, oN]], { padding: 20 });
      initialLoad = false;
    }
  }

  export function doPreloadLeads() {
    const { min, max } = getModelLeadBounds(appConfig.models, ui.model);
    preloadLeads({
      model: ui.model,
      statistic: ui.statistic,
      period: ui.period,
      month: ui.month,
      season: ui.season,
      minLead: min,
      maxLead: max,
      onReady: (capturedLead) => {
        const lo = Math.floor(ui.leadFractional);
        const hi = Math.ceil(ui.leadFractional);
        if (capturedLead === lo || capturedLead === hi) {
          loadTilesetInterp(ui.statistic, ui.leadFractional);
        }
      },
    });
  }

  export function onModelChange() {
    resetOverlay();
    clearLeadImages();
    doPreloadLeads();
    handleChange();
  }

  export function onStatisticChange() {
    resetOverlay();
    clearLeadImages();
    doPreloadLeads();
    handleChange();
  }

  export function onPeriodChange() {
    resetOverlay();
    clearLeadImages();
    doPreloadLeads();
    handleChange();
  }

  export function onLeadSliderInput(frac) {
    loadTilesetInterp(ui.statistic, frac);
  }

  export function flyToZip(data) {
    if (!map) return;
    if (Array.isArray(data.bounds) && data.bounds.length === 4) {
      map.fitBounds(
        [[data.bounds[0], data.bounds[1]], [data.bounds[2], data.bounds[3]]],
        { padding: 24, duration: 700 },
      );
    } else {
      map.flyTo({ center: [data.lon, data.lat], zoom: 10, duration: 700 });
    }
  }

  // ── Draw interaction state ──────────────────────────────────────────
  const drawSourceId = 'draw-source';
  const drawFillLayerId = 'draw-fill';
  const drawLineLayerId = 'draw-line';
  const drawPointLayerId = 'draw-point';

  /** Dark mask with a hole over the selection (below draw layers, above basemap + raster). */
  const dimSourceId = 'selection-dim-source';
  const dimLayerId = 'selection-dim-fill';

  let rectStart = null;       // {lng, lat} for rectangle drag start
  /** `$state` so the Enter-to-finish hint can react while drawing. */
  let polyPoints = $state([]);
  let isDrawing = $state(false);

  let hasDrawnRectangle = $state(false);
  let hasDrawnPolygon = $state(false);

  let pulseFrameId = 0;

  function emptyFeatureCollection() {
    return { type: 'FeatureCollection', features: [] };
  }

  /** Signed area × 2 (lon/lat as x,y); positive ⇒ CCW exterior ring. */
  function ringSignedArea2(ring) {
    let a = 0;
    const n = ring.length;
    if (n < 3) return 0;
    for (let i = 0; i < n - 1; i++) {
      const [x1, y1] = ring[i];
      const [x2, y2] = ring[i + 1];
      a += x1 * y2 - x2 * y1;
    }
    return a;
  }

  /** RFC 7946: inner rings (holes) must be clockwise when the outer ring is CCW. */
  function asClockwiseHoleRing(ring) {
    const copy = [...ring];
    if (ringSignedArea2(copy) > 0) copy.reverse();
    return copy;
  }

  /** Rectangle corners (same as draw shape); passed through ``asClockwiseHoleRing`` for the mask. */
  function boundsHoleRing(sw, ne) {
    return [
      [sw[0], sw[1]],
      [ne[0], sw[1]],
      [ne[0], ne[1]],
      [sw[0], ne[1]],
      [sw[0], sw[1]],
    ];
  }

  /**
   * Exterior ring for the dim mask: padded map viewport merged with the selection bounds.
   * A world-spanning outer ring (-180…180) is avoided: geojson-vt’s antimeridian wrap splits
   * polygons and can drop or mis-associate the hole, so the “cutout” wrongly follows a
   * CONUS-scale footprint instead of the drawn shape.
   */
  function viewportDimOuterRing(region) {
    if (!map) {
      return [
        [-179.999, -85],
        [179.999, -85],
        [179.999, 85],
        [-179.999, 85],
        [-179.999, -85],
      ];
    }
    const b = map.getBounds();
    let w = b.getWest();
    let e = b.getEast();
    let s = b.getSouth();
    let n = b.getNorth();
    const padLon = Math.max((e - w) * 0.04, 0.75);
    const padLat = Math.max((n - s) * 0.04, 0.75);
    w -= padLon;
    e += padLon;
    s -= padLat;
    n += padLat;

    function growToInclude(lng, lat) {
      w = Math.min(w, lng - padLon);
      e = Math.max(e, lng + padLon);
      s = Math.min(s, lat - padLat);
      n = Math.max(n, lat + padLat);
    }

    if (region.type === 'rectangle') {
      const [sw, ne] = region.coordinates;
      growToInclude(sw[0], sw[1]);
      growToInclude(ne[0], ne[1]);
    } else if (region.type === 'polygon') {
      for (const [lng, lat] of region.coordinates) {
        growToInclude(lng, lat);
      }
    }

    w = Math.max(-180, w);
    e = Math.min(180, e);
    s = Math.max(-85, s);
    n = Math.min(85, n);
    if (e <= w || n <= s) {
      return [
        [-179.999, -85],
        [179.999, -85],
        [179.999, 85],
        [-179.999, 85],
        [-179.999, -85],
      ];
    }

    const ring = [
      [w, s],
      [e, s],
      [e, n],
      [w, n],
      [w, s],
    ];
    return ringSignedArea2(ring) < 0 ? [...ring].reverse() : ring;
  }

  /** Dim outside selection: viewport-sized fill with a hole. Point: no dim (pin pulse only). */
  function dimMaskGeoJson(region) {
    if (!region) return emptyFeatureCollection();
    if (region.type === 'point') return emptyFeatureCollection();

    const outer = viewportDimOuterRing(region);

    let hole;
    if (region.type === 'rectangle') {
      const [sw, ne] = region.coordinates;
      hole = asClockwiseHoleRing(boundsHoleRing(sw, ne));
    } else if (region.type === 'polygon') {
      const pts = region.coordinates;
      if (pts.length < 3) return emptyFeatureCollection();
      hole = asClockwiseHoleRing([...pts, pts[0]]);
    } else {
      return emptyFeatureCollection();
    }

    return {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          properties: {},
          geometry: {
            type: 'Polygon',
            coordinates: [outer, hole],
          },
        },
      ],
    };
  }

  function updateDimMask() {
    if (!map?.getSource(dimSourceId)) return;
    map.getSource(dimSourceId).setData(dimMaskGeoJson(ui.selectedRegion));
  }

  const DEFAULT_LINE_WIDTH = 2;
  const DEFAULT_LINE_OPACITY = 1;
  const DEFAULT_POINT_RADIUS = 6;
  const DEFAULT_POINT_OPACITY = 1;

  function stopPulseAnimation() {
    if (pulseFrameId) {
      cancelAnimationFrame(pulseFrameId);
      pulseFrameId = 0;
    }
    if (!map) return;
    if (map.getLayer(drawLineLayerId)) {
      map.setPaintProperty(drawLineLayerId, 'line-width', DEFAULT_LINE_WIDTH);
      map.setPaintProperty(drawLineLayerId, 'line-opacity', DEFAULT_LINE_OPACITY);
    }
    if (map.getLayer(drawPointLayerId)) {
      map.setPaintProperty(drawPointLayerId, 'circle-radius', DEFAULT_POINT_RADIUS);
      map.setPaintProperty(drawPointLayerId, 'circle-opacity', DEFAULT_POINT_OPACITY);
    }
  }

  function startPulseAnimation() {
    stopPulseAnimation();
    if (!map || !ui.selectedRegion) return;

    const tick = (t) => {
      if (!map || !ui.selectedRegion) {
        stopPulseAnimation();
        return;
      }
      const phase = (t / 1000) * Math.PI * 2;
      const s = 0.5 + 0.5 * Math.sin(phase);
      const lineW = 1.6 + 1.4 * s;
      const lineOp = 0.62 + 0.38 * (0.5 + 0.5 * Math.sin(phase + 0.6));
      const ptR = 5 + 4 * s;
      const ptOp = 0.72 + 0.28 * (0.5 + 0.5 * Math.sin(phase + 0.4));

      if (map.getLayer(drawLineLayerId)) {
        map.setPaintProperty(drawLineLayerId, 'line-width', lineW);
        map.setPaintProperty(drawLineLayerId, 'line-opacity', lineOp);
      }
      if (map.getLayer(drawPointLayerId)) {
        map.setPaintProperty(drawPointLayerId, 'circle-radius', ptR);
        map.setPaintProperty(drawPointLayerId, 'circle-opacity', ptOp);
      }
      pulseFrameId = requestAnimationFrame(tick);
    };
    pulseFrameId = requestAnimationFrame(tick);
  }

  function initDrawLayers() {
    if (!map) return;
    map.addSource(drawSourceId, { type: 'geojson', data: emptyFeatureCollection() });
    map.addSource(dimSourceId, { type: 'geojson', data: emptyFeatureCollection() });
    map.addLayer({
      id: drawFillLayerId, type: 'fill', source: drawSourceId,
      filter: ['any', ['==', '$type', 'Polygon']],
      paint: { 'fill-color': '#ff6b35', 'fill-opacity': 0.22 },
    });
    map.addLayer(
      {
        id: dimLayerId,
        type: 'fill',
        source: dimSourceId,
        paint: {
          'fill-color': '#02060f',
          'fill-opacity': 0.5,
        },
      },
      drawFillLayerId,
    );
    map.addLayer({
      id: drawLineLayerId, type: 'line', source: drawSourceId,
      filter: ['any', ['==', '$type', 'Polygon'], ['==', '$type', 'LineString']],
      paint: { 'line-color': '#ff4500', 'line-width': 2 },
    });
    map.addLayer({
      id: drawPointLayerId, type: 'circle', source: drawSourceId,
      filter: ['==', '$type', 'Point'],
      paint: { 'circle-radius': 6, 'circle-color': '#ff6b35', 'circle-stroke-color': '#fff', 'circle-stroke-width': 2 },
    });
    // `map` is not reactive; sync dim/pulse once layers exist.
    updateDimMask();
    if (ui.selectedRegion) startPulseAnimation();
    else stopPulseAnimation();
  }

  function updateDrawSource(geojson) {
    if (!map) return;
    const src = map.getSource(drawSourceId);
    if (src) src.setData(geojson);
  }

  function clearDrawState() {
    rectStart = null;
    polyPoints = [];
    isDrawing = false;
    updateDrawSource(emptyFeatureCollection());
  }

  /** Repaint GeoJSON from `ui.selectedRegion` (after canceling an in-progress draw). */
  function redrawStoredSelection() {
    const r = ui.selectedRegion;
    if (!r) {
      updateDrawSource(emptyFeatureCollection());
      return;
    }
    if (r.type === 'point') {
      updateDrawSource({ type: 'FeatureCollection', features: [makePointFeature(r.coordinates)] });
    } else if (r.type === 'rectangle') {
      const [sw, ne] = r.coordinates;
      updateDrawSource({ type: 'FeatureCollection', features: [makeRectFeature(sw, ne)] });
    } else if (r.type === 'polygon') {
      updateDrawSource({ type: 'FeatureCollection', features: [makePolygonFeature(r.coordinates)] });
    }
  }

  /** Abort rectangle/polygon in progress; keep `selectedRegion` and restore its map shape. */
  function cancelInProgressDraw() {
    rectStart = null;
    polyPoints = [];
    isDrawing = false;
    redrawStoredSelection();
  }

  function makeRectFeature(sw, ne) {
    return {
      type: 'Feature',
      geometry: {
        type: 'Polygon',
        coordinates: [[
          [sw[0], sw[1]], [ne[0], sw[1]], [ne[0], ne[1]], [sw[0], ne[1]], [sw[0], sw[1]],
        ]],
      },
    };
  }

  function makePolygonFeature(points) {
    const closed = [...points, points[0]];
    return {
      type: 'Feature',
      geometry: { type: 'Polygon', coordinates: [closed] },
    };
  }

  function makePointFeature(lngLat) {
    return {
      type: 'Feature',
      geometry: { type: 'Point', coordinates: lngLat },
    };
  }

  function makeLineFeature(points) {
    return {
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: points },
    };
  }

  function finishPoint(lngLat) {
    const coords = [lngLat.lng, lngLat.lat];
    ui.hasUsedPinTool = true;
    ui.selectedRegion = { type: 'point', coordinates: coords };
    updateDrawSource({ type: 'FeatureCollection', features: [makePointFeature(coords)] });
    ui.activeTool = 'point';
  }

  function finishRectangle(start, end) {
    const sw = [Math.min(start.lng, end.lng), Math.min(start.lat, end.lat)];
    const ne = [Math.max(start.lng, end.lng), Math.max(start.lat, end.lat)];
    ui.selectedRegion = {
      type: 'rectangle',
      coordinates: [sw, ne],
      bounds: [sw[0], sw[1], ne[0], ne[1]],
    };
    updateDrawSource({ type: 'FeatureCollection', features: [makeRectFeature(sw, ne)] });
    hasDrawnRectangle = true;
    ui.hasUsedAreaDrawTool = true;
    ui.activeTool = 'rectangle';
  }

  function finishPolygon() {
    if (polyPoints.length < 3) return;
    ui.selectedRegion = { type: 'polygon', coordinates: [...polyPoints] };
    updateDrawSource({ type: 'FeatureCollection', features: [makePolygonFeature(polyPoints)] });
    polyPoints = [];
    isDrawing = false;
    hasDrawnPolygon = true;
    ui.hasUsedAreaDrawTool = true;
    ui.activeTool = 'polygon';
  }

  function handleDrawClick(e) {
    if (!ui.activeTool) return;

    if (ui.activeTool === 'point') {
      finishPoint(e.lngLat);
      return;
    }

    if (ui.activeTool === 'polygon') {
      const pt = [e.lngLat.lng, e.lngLat.lat];
      polyPoints = [...polyPoints, pt];
      isDrawing = true;

      // Show progress: line + vertices
      const features = polyPoints.map(p => makePointFeature(p));
      if (polyPoints.length >= 2) {
        features.push(makeLineFeature(polyPoints));
      }
      updateDrawSource({ type: 'FeatureCollection', features });
      return;
    }
  }

  function handleDrawMouseDown(e) {
    if (ui.activeTool !== 'rectangle') return;
    rectStart = { lng: e.lngLat.lng, lat: e.lngLat.lat };
    isDrawing = true;
    map.dragPan.disable();
  }

  function handleDrawMouseMove(e) {
    // Rectangle drag preview
    if (ui.activeTool === 'rectangle' && rectStart && isDrawing) {
      const sw = [Math.min(rectStart.lng, e.lngLat.lng), Math.min(rectStart.lat, e.lngLat.lat)];
      const ne = [Math.max(rectStart.lng, e.lngLat.lng), Math.max(rectStart.lat, e.lngLat.lat)];
      updateDrawSource({ type: 'FeatureCollection', features: [makeRectFeature(sw, ne)] });
      return;
    }

    // Polygon: show closing preview line
    if (ui.activeTool === 'polygon' && polyPoints.length >= 1) {
      const preview = [...polyPoints, [e.lngLat.lng, e.lngLat.lat]];
      const features = polyPoints.map(p => makePointFeature(p));
      features.push(makeLineFeature(preview));
      if (polyPoints.length >= 3) {
        features.push(makePolygonFeature(polyPoints.concat([[e.lngLat.lng, e.lngLat.lat]])));
      }
      updateDrawSource({ type: 'FeatureCollection', features });
    }
  }

  function handleDrawMouseUp(e) {
    if (ui.activeTool === 'rectangle' && rectStart && isDrawing) {
      map.dragPan.enable();
      isDrawing = false;
      finishRectangle(rectStart, e.lngLat);
      rectStart = null;
    }
  }

  function handleDrawDblClick(e) {
    if (ui.activeTool === 'polygon' && polyPoints.length >= 3) {
      e.preventDefault();
      finishPolygon();
    }
  }

  function handleKeyDown(e) {
    if (e.key === 'Enter' && ui.activeTool === 'polygon' && polyPoints.length >= 3) {
      finishPolygon();
    }
    if (e.key === 'Escape') {
      if (isDrawing || polyPoints.length > 0 || rectStart) {
        cancelInProgressDraw();
      }
    }
  }

  // Update cursor and clear draw state when tool changes
  $effect(() => {
    if (!map) return;
    const tool = ui.activeTool;
    const canvas = map.getCanvas();
    if (tool) {
      canvas.style.cursor = 'crosshair';
      // Clear previous draw state when switching tools
      clearDrawState();
    } else {
      canvas.style.cursor = '';
    }
  });

  // Clear draw visualization when region is cleared
  $effect(() => {
    if (!ui.selectedRegion && map) {
      clearDrawState();
    }
  });

  // Dim outside selection + pulse draw outline while a region is active
  $effect(() => {
    const region = ui.selectedRegion;
    if (!map?.getSource(dimSourceId)) return;
    updateDimMask();
    if (region) startPulseAnimation();
    else stopPulseAnimation();
  });

  // Reactive opacity
  $effect(() => {
    const _ = ui.weatherOpacity;
    applyLayerState();
  });

  onMount(async () => {
    // Fetch config FIRST, then create map — same sequence as original code
    try {
      const data = await fetchConfig();

      if (Array.isArray(data.models) && data.models.length > 0) {
        appConfig.models = data.models;
      }
      if (typeof data.default_model === 'string') {
        appConfig.defaultModel = data.default_model;
      }
      if (Array.isArray(data.statistics) && data.statistics.length > 0) {
        appConfig.statistics = data.statistics;
      }
      if (typeof data.default_statistic === 'string') {
        appConfig.defaultStatistic = data.default_statistic;
      }
      if (typeof data.maptiler_api_key === 'string') {
        appConfig.maptilerApiKey = data.maptiler_api_key;
      }
      ui.model = appConfig.defaultModel;
      {
        const accKeys = accuracyStatKeys();
        let stat = appConfig.defaultStatistic;
        if (!accKeys.includes(stat)) {
          stat = accKeys[0] || 'bias';
        }
        ui.statistic = stat;
      }
      ui.period = 'yearly';
      ui.activeTool = 'point';

      const { min } = getModelLeadBounds(appConfig.models, ui.model);
      ui.leadFractional = min;
    } catch (err) {
      ui.statusMessage = 'Failed to load config';
    }

    // NOW create the map — config is loaded, style URL will be correct
    map = new maplibregl.Map({
      container: mapContainer,
      style: mapStyleUrl(),
      center: [-98.5, 39.8],
      zoom: 3,
    });
    map.on('load', () => {
      if (!appConfig.maptilerApiKey) {
        ui.statusMessage = 'Using fallback basemap; set MAPTILER_API_KEY for MapTiler.';
      }
      doPreloadLeads();
      // Draw + dim layers must exist before handleChange → showOnMap → addRasterLayer,
      // otherwise beforeLayer is undefined and the raster stacks wrong vs the selection dim cutout.
      initDrawLayers();
      handleChange();
    });

    // Draw tool events
    map.on('mousedown', handleDrawMouseDown);
    map.on('mousemove', handleDrawMouseMove);
    map.on('mouseup', handleDrawMouseUp);
    map.on('dblclick', handleDrawDblClick);
    document.addEventListener('keydown', handleKeyDown);

    map.on('click', (event) => {
      if (ui.activeTool) handleDrawClick(event);
    });

    /** Dim outer ring follows the viewport; refresh when the view changes (not a full-world polygon). */
    function refreshDimOnViewChange() {
      const r = ui.selectedRegion;
      if (r && (r.type === 'rectangle' || r.type === 'polygon')) {
        updateDimMask();
      }
    }
    map.on('moveend', refreshDimOnViewChange);
    map.on('resize', refreshDimOnViewChange);
  });

  onDestroy(() => {
    document.removeEventListener('keydown', handleKeyDown);
    stopPulseAnimation();
    if (map) {
      map.remove();
      map = null;
    }
  });
</script>

<div class="map-wrap">
  <div class="map" bind:this={mapContainer}></div>
  {#if ui.activeTool === 'polygon' && !hasDrawnPolygon}
    <div
      class="draw-hint"
      class:draw-hint--with-panel={!!ui.selectedRegion}
      role="status"
      aria-live="polite"
    >
      {#if !ui.selectedRegion}
        <div class="draw-hint-icon">
          <svg width="28" height="28" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
            {@html drawToolGlyph.polygon}
          </svg>
        </div>
      {/if}
      <div class="draw-hint-text">
        {#if polyPoints.length === 0}
          Click to add polygon corners
        {:else}
          Press <kbd>Enter</kbd> to finish
          <span class="hint-detail">
            {#if polyPoints.length < 3}
              ({3 - polyPoints.length} more point{3 - polyPoints.length === 1 ? '' : 's'} needed)
              <span class="hint-detail-sep">·</span>
            {/if}
            Press <kbd>Esc</kbd> to stop drawing
          </span>
        {/if}
      </div>
    </div>
  {/if}
  {#if ui.activeTool === 'rectangle' && !hasDrawnRectangle}
    <div
      class="draw-hint"
      class:draw-hint--with-panel={!!ui.selectedRegion}
      role="status"
      aria-live="polite"
    >
      {#if !ui.selectedRegion}
        <div class="draw-hint-icon">
          <svg width="28" height="28" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
            {@html drawToolGlyph.rectangle}
          </svg>
        </div>
      {/if}
      <div class="draw-hint-text">
        {#if !isDrawing}
          Click & drag to draw a rectangle
        {:else}
          Release to finish
          <span class="hint-detail">Press <kbd>Esc</kbd> to stop drawing</span>
        {/if}
      </div>
    </div>
  {/if}
</div>

<style>
  .map-wrap {
    position: relative;
    width: 100%;
    height: 100%;
  }
  .map {
    width: 100%;
    height: 100%;
  }
  .draw-hint {
    position: fixed;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
    z-index: 9;
    pointer-events: none;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 12px;
    padding: 24px 32px;
    text-align: center;
    background: rgba(10, 12, 18, 0.82);
    backdrop-filter: blur(16px) saturate(1.2);
    -webkit-backdrop-filter: blur(16px) saturate(1.2);
    border: 1px solid rgba(255, 255, 255, 0.12);
    border-radius: 16px;
    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.45);
    max-width: min(340px, calc(100vw - 40px));
    animation: draw-tooltip-pulse 4s ease-in-out infinite;
  }
  .draw-hint--with-panel {
    top: min(40vh, calc(100vh - 52vh - 56px));
    gap: 0;
  }
  .draw-hint-icon {
    width: 56px;
    height: 56px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    border: 2px dashed rgba(255, 255, 255, 0.25);
    border-radius: 14px;
    color: var(--accent, #6eb5ff);
  }
  @keyframes draw-tooltip-pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.8; }
  }
  .draw-hint-text {
    font-size: 16px;
    font-weight: 600;
    line-height: 1.35;
    color: #fff;
  }
  .draw-hint kbd {
    display: inline-block;
    padding: 2px 6px;
    margin: 0 2px;
    font-size: 11px;
    font-family: inherit;
    font-weight: 600;
    background: var(--surface, rgba(255, 255, 255, 0.08));
    border: 1px solid var(--panel-border, rgba(255, 255, 255, 0.12));
    border-radius: 4px;
    color: var(--accent, #6eb5ff);
  }
  .hint-detail {
    display: block;
    margin-top: 6px;
    font-size: 11px;
    font-weight: 400;
    line-height: 1.4;
    color: var(--text-secondary, #7a818c);
  }
  .hint-detail-sep {
    margin: 0 0.35em;
    opacity: 0.65;
  }
</style>
