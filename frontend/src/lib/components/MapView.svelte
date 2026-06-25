<script>
  import { onMount, onDestroy } from 'svelte';
  import maplibregl from 'maplibre-gl';
  import 'maplibre-gl/dist/maplibre-gl.css';
  import { appConfig, ui, accuracyStatKeys } from '../state.svelte.js';
  import { FALLBACK_STYLE_URL, TILE_IMAGE_BOUNDS_WGS84, STATIC_BASE } from '../constants.js';
  import { fetchConfig } from '../api.js';
  import {
    tileUrl, preloadLeads, clearLeadImages, getLeadImage,
    compositeToDataUrlCached, getModelLeadBounds,
  } from '../tile.js';
  import CenterGuide from './CenterGuide.svelte';
  import { feature as topojsonFeature } from 'topojson-client';
  import {
    geomBbox,
    setupBoundaryLayer,
    teardownBoundaryLayer,
    createBoundaryHoverHandlers,
    regionGeometryHoles,
    SELECTION_COLOR,
  } from '../mapBoundaryLayer.js';
  import {
    loadWbdIndex,
    loadHuc2Geo,
    loadHucChildren,
    hasChildren,
    nextHucLevel,
    buildPendingFromFeature,
  } from '../hucSelection.js';

  const [oW, oS, oE, oN] = TILE_IMAGE_BOUNDS_WGS84;
  const overlayCoords = [[oW, oN], [oE, oN], [oE, oS], [oW, oS]];

  const sourceId = 'bias-source';
  const layerId = 'bias-layer';

  /** Spacing between `updateImage` calls so rapid scrubbing does not abort every in-flight load (MapLibre AbortError). */
  const OVERLAY_UPDATE_MIN_MS = 56;

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
    // Stack (bottom→top):
    //   basemap → raster → admin-fill → dim-mask → admin-outline → draw-fill → draw-line → draw-point
    const beforeLayer = map.getLayer(dimLayerId) ? dimLayerId
                      : map.getLayer(drawFillLayerId) ? drawFillLayerId
                      : undefined;
    map.addLayer({
      id: layerId, type: 'raster', source: sourceId,
      paint: { 'raster-opacity': ui.weatherOpacity },
    }, beforeLayer);
  }

  let lastShownUrl = $state('');

  /** Incremented on each overlay load; stale async composites skip `showOnMap`. */
  let loadTilesetGen = 0;

  let overlayApplyTimer = null;
  let overlayPendingUrl = null;
  let overlayLastApplyTime = 0;

  /** Tile URL, data URL, or blob URL of the raster on the map (for client-side PNG export). */
  export function getCurrentOverlayUrl() {
    return lastShownUrl;
  }

  function clearOverlayApplySchedule() {
    if (overlayApplyTimer != null) {
      clearTimeout(overlayApplyTimer);
      overlayApplyTimer = null;
    }
    overlayPendingUrl = null;
  }

  function applyOverlayToMap(url) {
    if (!map) return;
    lastShownUrl = url;
    const src = map.getSource(sourceId);
    if (src) {
      src.updateImage({ url, coordinates: overlayCoords });
    } else {
      map.addSource(sourceId, { type: 'image', url, coordinates: overlayCoords });
      addRasterLayer();
    }
  }

  function showOnMap(url) {
    if (!map || url === lastShownUrl) return;
    overlayPendingUrl = url;
    const now = performance.now();
    const since = now - overlayLastApplyTime;
    if (since >= OVERLAY_UPDATE_MIN_MS && overlayApplyTimer == null) {
      applyOverlayToMap(url);
      overlayLastApplyTime = performance.now();
      overlayPendingUrl = null;
      return;
    }
    if (overlayApplyTimer != null) return;
    const delay = Math.max(0, OVERLAY_UPDATE_MIN_MS - since);
    overlayApplyTimer = window.setTimeout(() => {
      overlayApplyTimer = null;
      const u = overlayPendingUrl;
      overlayPendingUrl = null;
      if (u == null || !map || u === lastShownUrl) return;
      applyOverlayToMap(u);
      overlayLastApplyTime = performance.now();
    }, delay);
  }

  function resetOverlay() {
    clearOverlayApplySchedule();
    overlayLastApplyTime = 0;
    lastShownUrl = '';
  }

  export async function loadTilesetInterp(statistic, frac) {
    if (!map) return;
    const gen = ++loadTilesetGen;
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
      if (imgLo && gen === loadTilesetGen) {
        showOnMap(tileUrl(ui.model, statistic, lo, ui.period, ui.month, ui.season));
      }
    } else if (imgLo && imgHi) {
      const url = await compositeToDataUrlCached(
        {
          model: ui.model,
          statistic,
          period: ui.period,
          month: ui.month,
          season: ui.season,
          lo,
          hi,
        },
        imgLo,
        imgHi,
        t,
      );
      if (gen !== loadTilesetGen) return;
      showOnMap(url);
    } else if (imgLo) {
      if (gen === loadTilesetGen) {
        showOnMap(tileUrl(ui.model, statistic, lo, ui.period, ui.month, ui.season));
      }
    } else if (imgHi) {
      if (gen === loadTilesetGen) {
        showOnMap(tileUrl(ui.model, statistic, hi, ui.period, ui.month, ui.season));
      }
    }
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

  export function flyToLocation(data) {
    if (!map) return;
    const type = data.feature_type || '';
    // Addresses/streets/POIs: Mapbox's bbox is a near-zero point, so ignore it
    // and force a tight zoom. Everything else: trust the returned bbox.
    const isPinpoint = type === 'address' || type === 'street' || type === 'poi';
    if (isPinpoint) {
      map.flyTo({ center: [data.lon, data.lat], zoom: 15, duration: 700 });
      return;
    }
    if (Array.isArray(data.bounds) && data.bounds.length === 4) {
      map.fitBounds(
        [[data.bounds[0], data.bounds[1]], [data.bounds[2], data.bounds[3]]],
        { padding: 24, duration: 700 },
      );
    } else {
      map.flyTo({ center: [data.lon, data.lat], zoom: 11, duration: 700 });
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

  // ── Admin / watershed boundary selection layer ───────────────────────
  const boundarySourceId = 'admin-source';
  const boundaryFillLayerId = 'admin-fill';
  const boundaryOutlineLayerId = 'admin-outline';
  /** Cache GeoJSON per admin level so toggling tools doesn't re-fetch. */
  const adminGeoCache = new Map(); // level → FeatureCollection
  let boundaryModeLoaded = null;   // 'state' | 'county' | 'watershed-drill' | null
  const boundaryHoveredRef = { current: null };
  let adminLoadAbort = null;
  let wbdLoadAbort = null;
  /** Drill state while watershed tool is active (before finalize). */
  let wbdPending = $state(null);
  let wbdIndex = null;
  let wbdGeoCache = null;       // accumulated FeatureCollection of every shown level
  let wbdGeoLevel = null;
  /**
   * Every HUC feature shown so far this drill, keyed by huc id. Drilling *adds*
   * the clicked region's children rather than replacing the layer, so coarse
   * boundaries (e.g. all HUC2s) stay on-screen alongside the deepened region.
   */
  let wbdAccum = new Map();

  const boundaryHover = createBoundaryHoverHandlers(() => map, {
    sourceId: boundarySourceId,
    hoveredIdRef: boundaryHoveredRef,
    // While drilling, overlapping levels mean the topmost hit may be the coarse
    // parent; highlight the deepest region so hover matches what a click selects.
    pickFeature: (features) =>
      boundaryModeLoaded === 'watershed-drill' ? deepestHucFeature(features) : features?.[0],
  });

  function adminTopoUrl(level) {
    return `${STATIC_BASE}/admin/${level === 'county' ? 'counties' : 'states'}.topojson`;
  }

  /**
   * Fetch the TopoJSON for a level and decode to GeoJSON.  TopoJSON is ~4–6×
   * smaller gzipped than the equivalent GeoJSON because shared borders are
   * stored once.
   *
   * us-atlas stores geometry ids as **numbers** (e.g. California is `6`, not
   * `"06"`), so we zero-pad here to the canonical FIPS width — 2 for states,
   * 5 for counties.  Without this, states 01–09 (and every county inside
   * them, ~1300 of them) would fail backend FIPS validation.
   */
  async function loadAdminGeo(level) {
    const cached = adminGeoCache.get(level);
    if (cached) return cached;
    if (adminLoadAbort) adminLoadAbort.abort();
    adminLoadAbort = new AbortController();
    try {
      const resp = await fetch(adminTopoUrl(level), { signal: adminLoadAbort.signal });
      if (!resp.ok) throw new Error(`admin geo ${level} fetch failed (${resp.status})`);
      const topology = await resp.json();
      const objectName = level === 'county' ? 'counties' : 'states';
      const fc = topojsonFeature(topology, topology.objects[objectName]);
      const padTo = level === 'county' ? 5 : 2;
      for (const feat of fc.features) {
        const fips = String(feat.id ?? '').padStart(padTo, '0');
        feat.id = fips;
        feat.properties = { ...(feat.properties || {}), fips, level };
      }
      adminGeoCache.set(level, fc);
      return fc;
    } finally {
      adminLoadAbort = null;
    }
  }

  function geomBboxFromRegion(geom) {
    return geomBbox(geom);
  }

  async function setupAdminLayer(level) {
    if (!map) return;
    let fc;
    try {
      fc = await loadAdminGeo(level);
    } catch (err) {
      if (err?.name !== 'AbortError') {
        console.warn(`[status] Failed to load ${level} boundaries`, err);
      }
      return;
    }
    if (!map) return;
    if (ui.activeTool !== level) return; // tool switched while loading

    setupBoundaryLayer(map, {
      sourceId: boundarySourceId,
      fillLayerId: boundaryFillLayerId,
      outlineLayerId: boundaryOutlineLayerId,
      data: fc,
      dimLayerId,
      drawFillLayerId,
      onClick: handleAdminClick,
      onHoverEnter: boundaryHover.onHoverEnter,
      onHoverLeave: boundaryHover.onHoverLeave,
    });
    boundaryModeLoaded = level;
  }

  function teardownBoundarySelectionLayer() {
    if (!map) return;
    teardownBoundaryLayer(map, {
      sourceId: boundarySourceId,
      fillLayerId: boundaryFillLayerId,
      outlineLayerId: boundaryOutlineLayerId,
      hoveredIdRef: boundaryHoveredRef,
      onClick: boundaryModeLoaded === 'watershed-drill' ? handleWbdClick : handleAdminClick,
      onHoverEnter: boundaryHover.onHoverEnter,
      onHoverLeave: boundaryHover.onHoverLeave,
    });
    boundaryModeLoaded = null;
  }

  function teardownAdminLayer() {
    teardownBoundarySelectionLayer();
  }

  function handleAdminClick(e) {
    const f = e.features?.[0];
    if (!f) return;
    const props = f.properties || {};
    const level = props.level || ui.activeTool;
    const fips = String(props.fips ?? f.id ?? '');
    if (!fips) return;
    // MapLibre splits polygons across tile boundaries, so e.features[0].geometry
    // is the tile-clipped fragment under the cursor — bbox/cutout would only match
    // that fragment when zoomed in. Look up the full feature from the cache instead.
    const fc = adminGeoCache.get(level);
    const fullFeature = fc?.features?.find((feat) => feat.id === fips);
    const geometry = fullFeature?.geometry || f.geometry;
    const bounds = geomBboxFromRegion(geometry);
    ui.selectedRegion = {
      type: 'admin',
      level,
      fips,
      name: props.name || '',
      bounds,
      geometry,
    };
    ui.activeTool = level;
    ui.hasUsedAreaDrawTool = true;
    teardownBoundarySelectionLayer();
  }

  function resetWbdDrill() {
    wbdPending = null;
    ui.wbdDrillPath = [];
    wbdGeoCache = null;
    wbdGeoLevel = null;
    wbdAccum = new Map();
  }

  function finalizeWbd(pending) {
    if (!pending?.huc) return;
    ui.selectedRegion = {
      type: 'huc',
      huc: pending.huc,
      level: pending.level,
      name: pending.name,
      bounds: pending.bounds,
      geometry: pending.geometry,
    };
    ui.activeTool = 'watershed';
    ui.hasUsedAreaDrawTool = true;
    resetWbdDrill();
    teardownBoundarySelectionLayer();
  }

  async function ensureWbdIndex() {
    if (wbdIndex) return wbdIndex;
    wbdIndex = await loadWbdIndex();
    return wbdIndex;
  }

  async function setupWbdDrillLayer(fc) {
    if (!map) return;
    wbdGeoCache = fc;
    setupBoundaryLayer(map, {
      sourceId: boundarySourceId,
      fillLayerId: boundaryFillLayerId,
      outlineLayerId: boundaryOutlineLayerId,
      data: fc,
      dimLayerId,
      drawFillLayerId,
      onClick: handleWbdClick,
      onHoverEnter: boundaryHover.onHoverEnter,
      onHoverLeave: boundaryHover.onHoverLeave,
    });
    boundaryModeLoaded = 'watershed-drill';
  }

  /**
   * Load `level`-deep children of `huc`, merge them into the accumulator and
   * render. Returns the loaded child features (so the caller can detect a
   * single-child pass-through level), or null if the load was aborted/superseded
   * or failed.
   */
  async function loadWbdLevel(huc, level, fcLoader) {
    if (wbdLoadAbort) wbdLoadAbort.abort();
    wbdLoadAbort = new AbortController();
    const signal = wbdLoadAbort.signal;
    try {
      const fc = await fcLoader(signal);
      if (signal.aborted || !map || ui.activeTool !== 'watershed' || ui.selectedRegion) {
        return null;
      }
      wbdGeoLevel = { huc, level };
      // Merge the freshly loaded level into the accumulator and render the union,
      // so prior levels' boundaries persist beneath the newly revealed children.
      for (const feat of fc.features ?? []) {
        if (feat?.id != null) wbdAccum.set(feat.id, feat);
      }
      const combined = { type: 'FeatureCollection', features: [...wbdAccum.values()] };
      await setupWbdDrillLayer(combined);
      // Only reposition when drilling into a specific parent (subsequent clicks).
      // The initial HUC2 load (`huc === ''`) spans Alaska, Hawaii, the Caribbean
      // and the Pacific territories, so fitting its union flings the camera out
      // across the whole Pacific — leave the camera where the user already is.
      if (huc && Array.isArray(fc?.features?.[0]?.geometry?.coordinates)) {
        const bounds = fc.features.reduce((acc, f) => {
          const b = geomBboxFromRegion(f.geometry);
          if (!acc) return b;
          return [
            Math.min(acc[0], b[0]),
            Math.min(acc[1], b[1]),
            Math.max(acc[2], b[2]),
            Math.max(acc[3], b[3]),
          ];
        }, null);
        if (bounds) {
          map.fitBounds([[bounds[0], bounds[1]], [bounds[2], bounds[3]]], { padding: 40, maxZoom: 10 });
        }
      }
      return fc.features ?? [];
    } catch (err) {
      if (err?.name !== 'AbortError') {
        console.warn('[status] Failed to load WBD boundaries', err);
      }
      return null;
    } finally {
      wbdLoadAbort = null;
    }
  }

  async function startWbdDrill() {
    resetWbdDrill();
    await ensureWbdIndex();
    await loadWbdLevel('', 2, loadHuc2Geo);
  }

  // Levels overlap now that boundaries accumulate, so a point inside an
  // already-drilled region hits both the coarse parent and its finer child.
  // Prefer the deepest (longest huc) feature under the cursor.
  function deepestHucFeature(features) {
    return (features ?? []).reduce((best, cur) => {
      const len = String(cur?.properties?.huc ?? cur?.id ?? '').length;
      const bestLen = best ? String(best.properties?.huc ?? best.id ?? '').length : -1;
      return len > bestLen ? cur : best;
    }, null);
  }

  async function handleWbdClick(e) {
    const f = deepestHucFeature(e.features);
    if (!f) return;
    const pending = buildPendingFromFeature(f, wbdGeoCache);
    if (!pending.huc) return;
    wbdPending = pending;
    ui.wbdDrillPath = [...ui.wbdDrillPath, pending.name || pending.huc];
    // The user has engaged the watershed tool — drop the center guide hint
    // (matches the state/county tools, which clear it on their first click).
    ui.hasUsedAreaDrawTool = true;
    const index = await ensureWbdIndex();
    if (!hasChildren(index, pending.huc)) {
      finalizeWbd(pending);
      return;
    }
    // Drill, descending through "pass-through" levels. Over half of WBD units
    // have a single child that fills the same area (e.g. HUC4 1805 → lone HUC6
    // 180500 → six HUC8s), so a single drill would redraw the same outline and
    // look like nothing happened. Keep descending while a level has exactly one
    // child until it actually subdivides — so one click always reveals real
    // sub-regions. (`wbdPending`/breadcrumb stay on the clicked unit; the skipped
    // children are coextensive with it.)
    let curHuc = pending.huc;
    let curLevel = pending.level;
    while (true) {
      const childLevel = nextHucLevel(curLevel);
      const children = await loadWbdLevel(curHuc, childLevel, (signal) =>
        loadHucChildren(curHuc, childLevel, signal),
      );
      if (!children || children.length !== 1) break; // load failed/superseded, or a real split
      const only = children[0];
      const onlyHuc = String(only.properties?.huc ?? only.id ?? '');
      if (!hasChildren(index, onlyHuc)) {
        // Sole descendant is a leaf with the clicked unit's extent — just select it.
        finalizeWbd(pending);
        return;
      }
      curHuc = onlyHuc;
      curLevel = only.properties?.level ?? onlyHuc.length;
    }
  }

  async function setupWatershedTool() {
    if (ui.selectedRegion?.type === 'huc') return;
    if (boundaryModeLoaded !== 'watershed-drill') {
      teardownBoundarySelectionLayer();
      await startWbdDrill();
    }
  }

  let rectStart = null;       // {lng, lat} for rectangle drag start
  /** `$state` so the Enter-to-finish hint can react while drawing. */
  let polyPoints = $state([]);
  let isDrawing = $state(false);

  let hasDrawnRectangle = $state(false);
  let hasDrawnPolygon = $state(false);

  /** Primary input is touch / coarse pointer — show polygon “Done” (no physical Enter key). */
  let coarsePointer = $state(
    typeof window !== 'undefined' && window.matchMedia('(pointer: coarse)').matches,
  );

  let pinPulseFrameId = 0;

  /** Ignore synthetic mouse events shortly after touch (rectangle draw). */
  let suppressMouseDrawUntil = 0;

  let coarseMql = null;
  let coarseMqlHandler = null;

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
    } else if ((region.type === 'admin' || region.type === 'huc') && Array.isArray(region.bounds)) {
      const [bw, bs, be, bn] = region.bounds;
      growToInclude(bw, bs);
      growToInclude(be, bn);
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

  /** Dim outside selection: viewport-sized fill with a hole. Point: no dim. */
  function dimMaskGeoJson(region) {
    if (!region) return emptyFeatureCollection();
    if (region.type === 'point') return emptyFeatureCollection();

    const outer = viewportDimOuterRing(region);

    let holes;
    if (region.type === 'rectangle') {
      const [sw, ne] = region.coordinates;
      holes = [asClockwiseHoleRing(boundsHoleRing(sw, ne))];
    } else if (region.type === 'polygon') {
      const pts = region.coordinates;
      if (pts.length < 3) return emptyFeatureCollection();
      holes = [asClockwiseHoleRing([...pts, pts[0]])];
    } else if (region.type === 'admin' || region.type === 'huc') {
      const rings = regionGeometryHoles(region.geometry);
      holes = rings.map((ring) => asClockwiseHoleRing([...ring]));
      if (!holes.length) return emptyFeatureCollection();
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
            coordinates: [outer, ...holes],
          },
        },
      ],
    };
  }

  /**
   * Region the dim mask should cut a hole for: the finalized selection, or —
   * while drilling the watershed tool — the region whose children are currently
   * shown, so the rest of the map dims as the user drills deeper.
   */
  function activeDimRegion() {
    if (ui.selectedRegion) return ui.selectedRegion;
    if (ui.activeTool === 'watershed' && wbdPending?.geometry) {
      return { ...wbdPending, type: 'huc' };
    }
    return null;
  }

  function updateDimMask() {
    if (!map?.getSource(dimSourceId)) return;
    map.getSource(dimSourceId).setData(dimMaskGeoJson(activeDimRegion()));
  }

  const DEFAULT_LINE_WIDTH = 2;
  const DEFAULT_POINT_RADIUS = 6;
  const DEFAULT_POINT_OPACITY = 1;

  function stopPinPulse() {
    if (pinPulseFrameId) {
      cancelAnimationFrame(pinPulseFrameId);
      pinPulseFrameId = 0;
    }
    if (!map) return;
    if (map.getLayer(drawPointLayerId)) {
      map.setPaintProperty(drawPointLayerId, 'circle-radius', DEFAULT_POINT_RADIUS);
      map.setPaintProperty(drawPointLayerId, 'circle-opacity', DEFAULT_POINT_OPACITY);
    }
  }

  /** Breathing animation for the map pin only (not rectangle / polygon outlines). */
  function startPinPulse() {
    stopPinPulse();
    if (!map || ui.selectedRegion?.type !== 'point') return;

    const tick = (t) => {
      if (!map || ui.selectedRegion?.type !== 'point') {
        stopPinPulse();
        return;
      }
      const phase = (t / 1000) * Math.PI * 2;
      const s = 0.5 + 0.5 * Math.sin(phase);
      const ptR = 5 + 4 * s;
      const ptOp = 0.72 + 0.28 * (0.5 + 0.5 * Math.sin(phase + 0.4));
      if (map.getLayer(drawPointLayerId)) {
        map.setPaintProperty(drawPointLayerId, 'circle-radius', ptR);
        map.setPaintProperty(drawPointLayerId, 'circle-opacity', ptOp);
      }
      pinPulseFrameId = requestAnimationFrame(tick);
    };
    pinPulseFrameId = requestAnimationFrame(tick);
  }

  function initDrawLayers() {
    if (!map) return;
    map.addSource(drawSourceId, { type: 'geojson', data: emptyFeatureCollection() });
    map.addSource(dimSourceId, { type: 'geojson', data: emptyFeatureCollection() });
    map.addLayer({
      id: drawFillLayerId, type: 'fill', source: drawSourceId,
      filter: ['any', ['==', '$type', 'Polygon']],
      paint: { 'fill-color': SELECTION_COLOR, 'fill-opacity': 0.22 },
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
    const lineFilter = ['any', ['==', '$type', 'Polygon'], ['==', '$type', 'LineString']];
    map.addLayer({
      id: drawLineLayerId, type: 'line', source: drawSourceId,
      filter: lineFilter,
      paint: { 'line-color': SELECTION_COLOR, 'line-width': DEFAULT_LINE_WIDTH },
    });
    map.addLayer({
      id: drawPointLayerId, type: 'circle', source: drawSourceId,
      filter: ['==', '$type', 'Point'],
      paint: {
        'circle-radius': DEFAULT_POINT_RADIUS,
        'circle-color': SELECTION_COLOR,
        'circle-stroke-color': '#fff',
        'circle-stroke-width': 2,
      },
    });
    // `map` is not reactive; sync dim once layers exist.
    updateDimMask();
    if (ui.selectedRegion?.type === 'point') startPinPulse();
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
    if (performance.now() < suppressMouseDrawUntil) return;
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
    if (performance.now() < suppressMouseDrawUntil) return;
    if (ui.activeTool === 'rectangle' && rectStart && isDrawing) {
      map.dragPan.enable();
      isDrawing = false;
      finishRectangle(rectStart, e.lngLat);
      rectStart = null;
    }
  }

  function touchFingerCount(e) {
    return e.originalEvent?.touches?.length ?? 0;
  }

  function lngLatFromTouchLike(e) {
    if (e.lngLat) return e.lngLat;
    const t = e.originalEvent?.changedTouches?.[0] ?? e.originalEvent?.touches?.[0];
    if (!t || !map) return null;
    const rect = map.getContainer().getBoundingClientRect();
    return map.unproject([t.clientX - rect.left, t.clientY - rect.top]);
  }

  function handleDrawTouchStart(e) {
    if (ui.activeTool !== 'rectangle') return;
    if (touchFingerCount(e) !== 1) return;
    handleDrawMouseDown(e);
    suppressMouseDrawUntil = performance.now() + 700;
  }

  function handleDrawTouchMove(e) {
    if (ui.activeTool !== 'rectangle' || !isDrawing) return;
    if (touchFingerCount(e) !== 1) return;
    handleDrawMouseMove(e);
  }

  function handleDrawTouchEnd(e) {
    if (ui.activeTool !== 'rectangle' || !rectStart || !isDrawing) return;
    const ll = lngLatFromTouchLike(e);
    if (!ll) {
      map?.dragPan.enable();
      cancelInProgressDraw();
      return;
    }
    map.dragPan.enable();
    isDrawing = false;
    finishRectangle(rectStart, ll);
    rectStart = null;
  }

  function handleDrawTouchCancel() {
    if (ui.activeTool === 'rectangle' && (isDrawing || rectStart)) {
      map?.dragPan.enable();
      cancelInProgressDraw();
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
    if (e.key === 'Enter' && ui.activeTool === 'watershed' && wbdPending && !ui.selectedRegion) {
      finalizeWbd(wbdPending);
    }
    if (e.key === 'Escape' && ui.activeTool === 'watershed' && !ui.selectedRegion) {
      resetWbdDrill();
      teardownBoundarySelectionLayer();
      void startWbdDrill();
    }
    if (e.key === 'Escape') {
      if (isDrawing || polyPoints.length > 0 || rectStart) {
        cancelInProgressDraw();
      }
    }
  }

  // Update cursor and clear draw state when tool changes.
  // Read `ui.activeTool` BEFORE the `!map` bail-out so Svelte registers it as a
  // tracked dep on the very first run (when onMount hasn't yet created the map).
  // If we read it after the early return, the first effect call doesn't see it,
  // no dep is tracked, and later button clicks never re-run this effect.
  $effect(() => {
    const tool = ui.activeTool;
    if (!map) return;
    const canvas = map.getCanvas();
    if (tool) {
      canvas.style.cursor = 'crosshair';
      clearDrawState();
    } else {
      canvas.style.cursor = '';
    }
    if (tool === 'state' || tool === 'county') {
      if (boundaryModeLoaded !== tool) {
        teardownBoundarySelectionLayer();
        resetWbdDrill();
        setupAdminLayer(tool);
      }
    } else if (tool === 'watershed') {
      if (!ui.selectedRegion || ui.selectedRegion.type !== 'huc') {
        void setupWatershedTool();
      } else if (boundaryModeLoaded != null) {
        teardownBoundarySelectionLayer();
      }
    } else if (boundaryModeLoaded != null) {
      teardownBoundarySelectionLayer();
      if (wbdLoadAbort) wbdLoadAbort.abort();
      resetWbdDrill();
    }
  });

  // Clear draw visualization when region is cleared
  $effect(() => {
    if (!ui.selectedRegion && map) {
      clearDrawState();
      if (ui.activeTool === 'watershed' && boundaryModeLoaded !== 'watershed-drill') {
        void setupWatershedTool();
      }
    }
  });

  // Dim outside selection; pulse the pin only when the saved region is a point.
  // Read both reactive deps up front: `wbdPending` so the dim follows each drill
  // click, before the `!map` bail-out (else the dep isn't tracked on first run).
  $effect(() => {
    const region = ui.selectedRegion;
    const _pending = wbdPending;
    if (!map?.getSource(dimSourceId)) return;
    updateDimMask();
    if (region?.type === 'point') startPinPulse();
    else stopPinPulse();
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
      console.warn('[status] Failed to load config', err);
    }

    // NOW create the map — config is loaded, style URL will be correct
    map = new maplibregl.Map({
      container: mapContainer,
      style: mapStyleUrl(),
      center: [-98.5, 39.8],
      zoom: 3,
    });
    map.on('error', (e) => {
      const err = e.error;
      if (!err) return;
      if (err.name === 'AbortError') return;
      const msg = String(err.message || '');
      if (/abort/i.test(msg)) return;
    });
    map.on('load', () => {
      if (!appConfig.maptilerApiKey) {
        console.log('[status] Using fallback basemap; set MAPTILER_API_KEY for MapTiler.');
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
    map.on('touchstart', handleDrawTouchStart);
    map.on('touchmove', handleDrawTouchMove);
    map.on('touchend', handleDrawTouchEnd);
    map.on('touchcancel', handleDrawTouchCancel);
    map.on('dblclick', handleDrawDblClick);
    document.addEventListener('keydown', handleKeyDown);

    coarseMql = window.matchMedia('(pointer: coarse)');
    coarseMqlHandler = () => {
      coarsePointer = coarseMql.matches;
    };
    coarseMqlHandler();
    coarseMql.addEventListener('change', coarseMqlHandler);

    map.on('click', (event) => {
      if (ui.activeTool) handleDrawClick(event);
    });

    /**
     * Dim outer ring follows the viewport; refresh continuously during pan/zoom so newly
     * revealed areas dim immediately (otherwise the mask appears to "chunk" — only filling
     * the new viewport when the gesture ends). The dim polygon is tiny (one outer ring +
     * ≤ a few holes), so per-frame re-tiling via setData is cheap.
     */
    function refreshDimOnViewChange() {
      const r = ui.selectedRegion;
      if ((r && (r.type === 'rectangle' || r.type === 'polygon' || r.type === 'admin' || r.type === 'huc'))
          || (ui.activeTool === 'watershed' && wbdPending)) {
        updateDimMask();
      }
    }
    map.on('move', refreshDimOnViewChange);
    map.on('resize', refreshDimOnViewChange);
  });

  onDestroy(() => {
    if (coarseMql && coarseMqlHandler) {
      coarseMql.removeEventListener('change', coarseMqlHandler);
    }
    document.removeEventListener('keydown', handleKeyDown);
    stopPinPulse();
    clearOverlayApplySchedule();
    if (map) {
      map.remove();
      map = null;
    }
  });
</script>

<div class="map-wrap">
  <div class="map" bind:this={mapContainer}></div>
  {#if ui.activeTool === 'polygon' && !hasDrawnPolygon && polyPoints.length >= 3 && coarsePointer}
    <div class="poly-done-wrap">
      <button type="button" class="poly-done-btn" onclick={() => finishPolygon()}>Done — close polygon</button>
    </div>
  {/if}
  {#if ui.activeTool === 'polygon' && !hasDrawnPolygon}
    <CenterGuide>
      {#if polyPoints.length === 0}
        Tap the map to add polygon corners
      {:else}
        {#if coarsePointer}
          Tap <strong>Done</strong> below when you have at least 3 points
        {:else}
          Press <kbd class="guide-kbd">Enter</kbd> to finish
        {/if}
        <span class="guide-detail">
          {#if polyPoints.length < 3}
            ({3 - polyPoints.length} more point{3 - polyPoints.length === 1 ? '' : 's'} needed)
            <span class="guide-detail-sep">·</span>
          {/if}
          Press <kbd class="guide-kbd">Esc</kbd> to stop drawing
        </span>
      {/if}
    </CenterGuide>
  {/if}
  {#if ui.activeTool === 'rectangle' && !hasDrawnRectangle}
    <CenterGuide>
      {#if !isDrawing}
        {coarsePointer ? 'Touch and drag to draw a rectangle' : 'Click & drag to draw a rectangle'}
      {:else}
        {coarsePointer ? 'Lift finger to finish' : 'Release to finish'}
        <span class="guide-detail">Press <kbd class="guide-kbd">Esc</kbd> to stop drawing</span>
      {/if}
    </CenterGuide>
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
  @media (max-width: 640px) {
    .poly-done-wrap {
      bottom: max(100px, calc(env(safe-area-inset-bottom, 0px) + 88px));
    }
  }
  .poly-done-wrap {
    position: fixed;
    left: 50%;
    bottom: max(24px, env(safe-area-inset-bottom, 0px));
    transform: translateX(-50%);
    z-index: 12;
    pointer-events: auto;
  }
  .poly-done-btn {
    appearance: none;
    border: 1px solid rgba(110, 181, 255, 0.45);
    background: linear-gradient(180deg, rgba(110, 181, 255, 0.28), rgba(110, 181, 255, 0.12));
    color: #e8f3ff;
    font-family: inherit;
    font-size: 14px;
    font-weight: 600;
    padding: 12px 20px;
    border-radius: 12px;
    box-shadow: 0 6px 24px rgba(0, 0, 0, 0.45);
    cursor: pointer;
    min-height: 44px;
  }
  .poly-done-btn:active {
    transform: scale(0.98);
  }
  .guide-kbd {
    display: inline-block;
    padding: 2px 6px;
    margin: 0 2px;
    font-size: 11px;
    font-family: inherit;
    font-weight: 600;
    background: rgba(255, 255, 255, 0.10);
    border: 1px solid rgba(255, 255, 255, 0.18);
    border-radius: 4px;
    color: #e8f3ff;
  }
  .guide-detail {
    display: block;
    margin-top: 6px;
    font-size: 11px;
    font-weight: 400;
    line-height: 1.4;
    color: rgba(255, 255, 255, 0.7);
  }
  .guide-detail-sep {
    margin: 0 0.35em;
    opacity: 0.65;
  }
</style>
