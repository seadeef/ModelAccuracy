import { feature as topojsonFeature } from 'topojson-client';
import { geomBbox } from './mapBoundaryLayer.js';
import { STATIC_BASE } from './constants.js';

// Top-level index: only the ~22 HUC2 units. Entering watershed mode loads this
// alone (~0.4 KB gz) instead of metadata for all ~125k units (~1.3 MB gz).
const indexCache = { data: null, promise: null };

// Per-HUC2 descendant metadata (HUC4..HUC12), fetched only when the user drills
// into that region. Keyed by HUC2 code → { units, promise }. Bounded by the ~22
// HUC2 regions, so no eviction needed.
const subindexCache = new Map();

// Bounded LRU for fetched TopoJSON FeatureCollections (keyed by URL). Caps memory
// for long drill sessions; insertion-order Map → first key is least-recently used.
const TOPO_CACHE_MAX = 64;
const topoCache = new Map();

function topoCacheGet(url) {
  const fc = topoCache.get(url);
  if (fc !== undefined) {
    topoCache.delete(url);
    topoCache.set(url, fc); // move to most-recently used
  }
  return fc;
}

function topoCacheSet(url, fc) {
  topoCache.delete(url);
  topoCache.set(url, fc);
  while (topoCache.size > TOPO_CACHE_MAX) {
    topoCache.delete(topoCache.keys().next().value);
  }
}

export async function loadWbdIndex() {
  if (indexCache.data) return indexCache.data;
  if (!indexCache.promise) {
    indexCache.promise = fetch(`${STATIC_BASE}/wbd/index.json`)
      .then((resp) => {
        if (!resp.ok) throw new Error(`WBD index fetch failed (${resp.status})`);
        return resp.json();
      })
      .then((data) => {
        indexCache.data = data;
        return data;
      })
      .finally(() => {
        indexCache.promise = null;
      });
  }
  return indexCache.promise;
}

/**
 * Ensure the per-HUC2 subtree index covering `huc`'s descendants is resident.
 * The top-level index only carries HUC2 units; a region's deeper metadata
 * (HUC4..HUC12, needed to know which units are drillable) lives in
 * `/wbd/index/{huc2}.json` and is fetched only on the first drill into that
 * region. A missing file (a HUC2 with no descendants) caches as empty, so
 * callers never refetch or throw. Returns the resident units map.
 */
export async function ensureHucSubtree(huc) {
  const key = String(huc).slice(0, 2);
  const cached = subindexCache.get(key);
  if (cached?.units) return cached.units;
  if (cached?.promise) return cached.promise;
  const promise = fetch(`${STATIC_BASE}/wbd/index/${key}.json`)
    .then((resp) => (resp.ok ? resp.json() : { units: {} }))
    .then((data) => {
      const units = data?.units ?? {};
      subindexCache.set(key, { units, promise: null });
      return units;
    })
    .catch(() => {
      subindexCache.set(key, { units: {}, promise: null });
      return {};
    });
  subindexCache.set(key, { units: null, promise });
  return promise;
}

function topoObjectName(level) {
  return `huc${level}`;
}

async function fetchTopoJson(url, objectName, signal) {
  const cached = topoCacheGet(url);
  if (cached) return cached;
  const resp = await fetch(url, signal ? { signal } : undefined);
  if (!resp.ok) throw new Error(`WBD topo fetch failed (${resp.status}): ${url}`);
  const topology = await resp.json();
  // The WBD exporter writes the TopoJSON object under the topojson library's
  // default key ("data") rather than `objectName`, so fall back to the sole/first
  // object when the expected name is absent.
  const objects = topology.objects || {};
  const topoObject = objects[objectName] ?? objects[Object.keys(objects)[0]];
  const fc = topology.type === 'FeatureCollection'
    ? topology
    : topojsonFeature(topology, topoObject);
  for (const feat of fc.features) {
    const huc = String(feat.properties?.huc ?? feat.id ?? '').padStart(
      feat.properties?.level ?? String(feat.id ?? '').length,
      '0',
    );
    feat.id = huc;
    feat.properties = { ...(feat.properties || {}), huc, level: feat.properties?.level };
  }
  topoCacheSet(url, fc);
  return fc;
}

export async function loadHuc2Geo(signal) {
  return fetchTopoJson(`${STATIC_BASE}/wbd/huc2.topojson`, 'huc2', signal);
}

export async function loadHucChildren(parentHuc, childLevel, signal) {
  const url = `${STATIC_BASE}/wbd/children/${childLevel}/${parentHuc}.topojson`;
  return fetchTopoJson(url, topoObjectName(childLevel), signal);
}

/**
 * Metadata for one HUC, routed to whichever resident index holds it: HUC2 codes
 * live in the top-level index; deeper codes live in their HUC2's subtree (which
 * must already be resident via `ensureHucSubtree`). Returns null if not loaded.
 */
export function wbdUnitMeta(huc) {
  const code = String(huc);
  if (code.length <= 2) {
    const units = indexCache.data?.units ?? indexCache.data;
    return units?.[code] ?? null;
  }
  return subindexCache.get(code.slice(0, 2))?.units?.[code] ?? null;
}

export function nextHucLevel(level) {
  return level + 2;
}

export function hasChildren(huc) {
  return Boolean(wbdUnitMeta(huc)?.hasChildren);
}

export function buildPendingFromFeature(feature, fc) {
  const props = feature.properties || {};
  const huc = String(props.huc ?? feature.id ?? '');
  const fullFeature = fc?.features?.find((f) => f.id === huc);
  const geometry = fullFeature?.geometry || feature.geometry;
  return {
    huc,
    level: props.level ?? huc.length,
    name: props.name || '',
    bounds: geomBbox(geometry),
    geometry,
  };
}

export function clearWbdCaches() {
  indexCache.data = null;
  indexCache.promise = null;
  subindexCache.clear();
  topoCache.clear();
}
