/** Shared MapLibre boundary selection layers (admin + watershed). */

export const BOUNDARY_FILL_COLOR = '#26d9e8';
export const BOUNDARY_OUTLINE_COLOR = '#26d9e8';

/** [west, south, east, north] from a GeoJSON Polygon / MultiPolygon. */
export function geomBbox(geom) {
  let w = Infinity;
  let e = -Infinity;
  let s = Infinity;
  let n = -Infinity;
  const visit = (ring) => {
    for (const [x, y] of ring) {
      if (x < w) w = x;
      if (x > e) e = x;
      if (y < s) s = y;
      if (y > n) n = y;
    }
  };
  if (geom?.type === 'Polygon') {
    for (const ring of geom.coordinates) visit(ring);
  } else if (geom?.type === 'MultiPolygon') {
    for (const poly of geom.coordinates) {
      for (const ring of poly) visit(ring);
    }
  }
  return [w, s, e, n];
}

/**
 * Add or update fill + outline boundary layers.
 *
 * Layer stack: basemap → raster → boundary-fill → dim-mask → boundary-outline → draw-*
 */
export function setupBoundaryLayer(
  map,
  {
    sourceId,
    fillLayerId,
    outlineLayerId,
    data,
    dimLayerId,
    drawFillLayerId,
    onClick,
    onHoverEnter,
    onHoverLeave,
  },
) {
  if (map.getSource(sourceId)) {
    map.getSource(sourceId).setData(data);
  } else {
    const fillBefore = map.getLayer(dimLayerId) ? dimLayerId
      : map.getLayer(drawFillLayerId) ? drawFillLayerId
      : undefined;
    const outlineBefore = map.getLayer(drawFillLayerId) ? drawFillLayerId : undefined;
    map.addSource(sourceId, { type: 'geojson', data });
    map.addLayer({
      id: fillLayerId,
      type: 'fill',
      source: sourceId,
      paint: {
        'fill-color': BOUNDARY_FILL_COLOR,
        'fill-opacity': [
          'case',
          ['boolean', ['feature-state', 'hover'], false], 0.32,
          0.08,
        ],
      },
    }, fillBefore);
    map.addLayer({
      id: outlineLayerId,
      type: 'line',
      source: sourceId,
      paint: {
        'line-color': BOUNDARY_OUTLINE_COLOR,
        'line-width': [
          'case',
          ['boolean', ['feature-state', 'hover'], false], 2.4,
          1.2,
        ],
        'line-opacity': 0.9,
      },
    }, outlineBefore);
    if (onClick) map.on('click', fillLayerId, onClick);
    if (onHoverEnter) map.on('mousemove', fillLayerId, onHoverEnter);
    if (onHoverLeave) map.on('mouseleave', fillLayerId, onHoverLeave);
  }
}

export function teardownBoundaryLayer(
  map,
  { sourceId, fillLayerId, outlineLayerId, hoveredIdRef, onClick, onHoverEnter, onHoverLeave },
) {
  if (!map) return;
  if (hoveredIdRef?.current != null && map.getSource(sourceId)) {
    map.setFeatureState({ source: sourceId, id: hoveredIdRef.current }, { hover: false });
    hoveredIdRef.current = null;
  }
  if (map.getLayer(fillLayerId)) {
    if (onClick) map.off('click', fillLayerId, onClick);
    if (onHoverEnter) map.off('mousemove', fillLayerId, onHoverEnter);
    if (onHoverLeave) map.off('mouseleave', fillLayerId, onHoverLeave);
    map.removeLayer(fillLayerId);
  }
  if (map.getLayer(outlineLayerId)) map.removeLayer(outlineLayerId);
  if (map.getSource(sourceId)) map.removeSource(sourceId);
}

export function createBoundaryHoverHandlers(getMap, { sourceId, hoveredIdRef }) {
  function onHoverEnter(e) {
    const map = getMap();
    if (!map) return;
    map.getCanvas().style.cursor = 'pointer';
    const f = e.features?.[0];
    if (!f) return;
    if (hoveredIdRef.current != null && hoveredIdRef.current !== f.id) {
      map.setFeatureState({ source: sourceId, id: hoveredIdRef.current }, { hover: false });
    }
    hoveredIdRef.current = f.id;
    map.setFeatureState({ source: sourceId, id: hoveredIdRef.current }, { hover: true });
  }

  function onHoverLeave() {
    const map = getMap();
    if (!map) return;
    map.getCanvas().style.cursor = 'crosshair';
    if (hoveredIdRef.current != null) {
      map.setFeatureState({ source: sourceId, id: hoveredIdRef.current }, { hover: false });
      hoveredIdRef.current = null;
    }
  }

  return { onHoverEnter, onHoverLeave };
}

/** Outer rings from admin/huc geometry for dim-mask holes. */
export function regionGeometryHoles(geometry) {
  if (!geometry) return [];
  const parts = geometry.type === 'MultiPolygon' ? geometry.coordinates
    : geometry.type === 'Polygon' ? [geometry.coordinates]
    : [];
  return parts
    .map((part) => part?.[0])
    .filter((ring) => Array.isArray(ring) && ring.length >= 4);
}
