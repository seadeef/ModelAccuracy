import { feature as topojsonFeature } from 'topojson-client';
import { geomBbox } from './mapBoundaryLayer.js';
import { STATIC_BASE } from './constants.js';

const indexCache = { data: null, promise: null };

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

export function wbdUnitMeta(index, huc) {
  return (index?.units ?? index)?.[huc] ?? null;
}

export function nextHucLevel(level) {
  return level + 2;
}

export function hasChildren(index, huc) {
  const meta = wbdUnitMeta(index, huc);
  return Boolean(meta?.hasChildren);
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
  topoCache.clear();
}
