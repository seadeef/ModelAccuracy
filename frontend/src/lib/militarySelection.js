import { STATIC_BASE } from './constants.js';

// Military installation sites (DISDI MIRTA): one flat GeoJSON FeatureCollection
// (~250 KB gz) holding every site's boundary MultiPolygon, with the site's
// fly-to point in properties.point. No drill hierarchy (unlike WBD), so a
// single cached fetch covers the whole tool.
const geoCache = { data: null, promise: null };

export async function loadMilitaryGeo(signal) {
  if (geoCache.data) return geoCache.data;
  if (!geoCache.promise) {
    geoCache.promise = fetch(`${STATIC_BASE}/military/sites.geojson`, signal ? { signal } : undefined)
      .then((resp) => {
        if (!resp.ok) throw new Error(`military sites fetch failed (${resp.status})`);
        return resp.json();
      })
      .then((fc) => {
        geoCache.data = fc;
        return fc;
      })
      .finally(() => {
        geoCache.promise = null;
      });
  }
  return geoCache.promise;
}

/**
 * Point FeatureCollection derived from the polygon features (same numeric ids
 * and properties), for the circle-marker layer — sites are near-invisible as
 * polygons at national zoom.
 */
export function militaryPointsFc(fc) {
  return {
    type: 'FeatureCollection',
    features: (fc?.features ?? [])
      .filter((f) => Array.isArray(f.properties?.point) && f.properties.point.length === 2)
      .map((f) => ({
        type: 'Feature',
        id: f.id,
        properties: f.properties,
        geometry: { type: 'Point', coordinates: f.properties.point },
      })),
  };
}

export function clearMilitaryCaches() {
  geoCache.data = null;
  geoCache.promise = null;
}
