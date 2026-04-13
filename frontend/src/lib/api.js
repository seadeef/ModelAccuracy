import { STATIC_BASE } from './constants.js';
import { appConfig } from './state.svelte.js';

const API_BASE = '';

function verificationStatisticsOnly() {
  return appConfig.statistics.filter((s) => s.key !== 'forecast');
}

function normalizeZipCode(zip) {
  const d = String(zip).replace(/\D/g, '');
  return d.length >= 5 ? d.slice(0, 5) : null;
}

/** Loads ``static_export/config.json`` (not ``/api/config``). */
export async function fetchConfig() {
  const resp = await fetch(`${STATIC_BASE}/config.json`);
  if (!resp.ok) throw new Error(`Config fetch failed (${resp.status})`);
  return resp.json();
}

/**
 * Best verification model per lead for the current map region (point / rectangle / polygon).
 * Response matches former ``lead_winners.json`` shape (`leads`, `models_considered`, …).
 */
export async function fetchLeadWinnersForRegion({
  region,
  statistic,
  period,
  month,
  season,
  minLead,
  maxLead,
}) {
  try {
    const resp = await fetch(`${API_BASE}/api/stats/lead-winners`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        region,
        statistic,
        period,
        month,
        season,
        minLead,
        maxLead,
      }),
    });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

/** Loads ``static_export/zip/{5-digit}.json``; response shape matches prior API usage. */
export async function fetchZip(zip) {
  const code = normalizeZipCode(zip);
  if (!code) {
    return { found: false, zip: String(zip).trim() || '' };
  }
  const resp = await fetch(`${STATIC_BASE}/zip/${code}.json`);
  if (!resp.ok) {
    return { found: false, zip: code };
  }
  const data = await resp.json();
  return {
    found: true,
    zip: code,
    lat: data.lat,
    lon: data.lon,
    bounds: data.bounds,
  };
}

/** Returns true when the input looks like a bare 5-digit US ZIP code. */
export function looksLikeZip(input) {
  return /^\d{5}$/.test(input.trim());
}

/**
 * Geocode a free-form US address via the Nominatim (OpenStreetMap) API.
 * Returns { found, label, lat, lon, bounds? } matching the fetchZip shape.
 */
export async function geocodeAddress(query) {
  const q = query.trim();
  if (!q) return { found: false, label: '' };
  const params = new URLSearchParams({
    q,
    format: 'jsonv2',
    countrycodes: 'us',
    limit: '1',
  });
  const resp = await fetch(
    `https://nominatim.openstreetmap.org/search?${params}`,
    { headers: { 'User-Agent': 'ModelAccuracyApp/1.0' } },
  );
  if (!resp.ok) return { found: false, label: q };
  const results = await resp.json();
  if (!results.length) return { found: false, label: q };
  const r = results[0];
  const result = {
    found: true,
    label: r.display_name,
    lat: Number(r.lat),
    lon: Number(r.lon),
  };
  if (r.boundingbox) {
    // Nominatim returns [south, north, west, east] as strings
    const [south, north, west, east] = r.boundingbox.map(Number);
    result.bounds = [west, south, east, north];
  }
  return result;
}

/**
 * Fetch stats for every lead day from min to max (all verification statistics in one request).
 * Point regions use the nearest grid cell; rectangle/polygon use the spatial mean over masked cells.
 * Returns an array of { lead, stats } objects.
 */
export async function fetchStatsAllLeads({
  model,
  region,
  period,
  month,
  season,
  minLead,
  maxLead,
}) {
  const statList = verificationStatisticsOnly();
  if (!statList?.length) {
    throw new Error('Statistics not loaded yet; wait for config.');
  }
  const resp = await fetch(`${API_BASE}/api/stats/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model,
      region,
      period,
      month,
      season,
      minLead: minLead,
      maxLead: maxLead,
      statistics: statList.map((s) => s.key),
    }),
  });
  if (!resp.ok) {
    throw new Error(`Stats query failed (${resp.status})`);
  }
  const data = await resp.json();
  return data.results ?? [];
}

