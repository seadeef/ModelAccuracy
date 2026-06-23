import { feature as topojsonFeature } from 'topojson-client';
import { geomBbox } from './mapBoundaryLayer.js';

const indexCache = { data: null, promise: null };
const topoCache = new Map();

export async function loadWbdIndex() {
  if (indexCache.data) return indexCache.data;
  if (!indexCache.promise) {
    indexCache.promise = fetch('/static/wbd/index.json')
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

async function fetchTopoJson(url, objectName) {
  const cached = topoCache.get(url);
  if (cached) return cached;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`WBD topo fetch failed (${resp.status}): ${url}`);
  const topology = await resp.json();
  const fc = topology.type === 'FeatureCollection'
    ? topology
    : topojsonFeature(topology, topology.objects[objectName]);
  for (const feat of fc.features) {
    const huc = String(feat.properties?.huc ?? feat.id ?? '').padStart(
      feat.properties?.level ?? String(feat.id ?? '').length,
      '0',
    );
    feat.id = huc;
    feat.properties = { ...(feat.properties || {}), huc, level: feat.properties?.level };
  }
  topoCache.set(url, fc);
  return fc;
}

export async function loadHuc2Geo() {
  return fetchTopoJson('/static/wbd/huc2.topojson', 'huc2');
}

export async function loadHucChildren(parentHuc, childLevel) {
  const url = `/static/wbd/children/${childLevel}/${parentHuc}.topojson`;
  return fetchTopoJson(url, topoObjectName(childLevel));
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
