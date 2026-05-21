import { authHeaders } from './authSession.svelte.js';
import { STATIC_BASE } from './constants.js';
import { appConfig } from './state.svelte.js';

const API_BASE = '';

function headersWithAuth(base = {}) {
  return { ...base, ...authHeaders() };
}

/** Drop the (potentially large) ``geometry`` field from admin regions before sending.
 *  The backend resolves polygons from FIPS via ``backend/admin_boundaries.py``; the
 *  frontend only carries ``geometry`` for local rendering (dim mask). */
function regionPayload(region) {
  if (!region || region.type !== 'admin' || !region.geometry) return region;
  const { geometry: _g, ...rest } = region;
  return rest;
}

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
      headers: headersWithAuth({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        region: regionPayload(region),
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

/** Returns true when the input is exactly 5 digits (pure ZIP entry). */
export function looksLikeZip(input) {
  return /^\d{5}$/.test(String(input).trim());
}

const MAPBOX_TOKEN = import.meta.env.VITE_MAPBOX_TOKEN;
const MAPBOX_SEARCHBOX = 'https://api.mapbox.com/search/searchbox/v1';

/**
 * Mapbox Search Box suggest — returns autocomplete candidates for a partial query.
 * Caller passes a persistent sessionToken (UUID) to group suggest+retrieve into one
 * billable session. Returns [] on any failure so the UI stays quiet.
 */
export async function suggestPlaces(query, sessionToken, { signal } = {}) {
  const q = String(query).trim();
  if (!q || !MAPBOX_TOKEN) return [];
  const params = new URLSearchParams({
    q,
    access_token: MAPBOX_TOKEN,
    session_token: sessionToken,
    country: 'us',
    language: 'en',
    limit: '6',
    types: 'address,street,postcode,place,locality,neighborhood,poi',
  });
  try {
    const resp = await fetch(`${MAPBOX_SEARCHBOX}/suggest?${params}`, { signal });
    if (!resp.ok) return [];
    const data = await resp.json();
    return data.suggestions ?? [];
  } catch {
    return [];
  }
}

/**
 * Mapbox Search Box retrieve — resolves a suggestion's mapbox_id to coordinates and bbox.
 * Returns { found, label, lat, lon, bounds? } matching fetchZip's shape.
 */
export async function retrievePlace(mapboxId, sessionToken) {
  if (!mapboxId || !MAPBOX_TOKEN) return { found: false, label: '' };
  const params = new URLSearchParams({
    access_token: MAPBOX_TOKEN,
    session_token: sessionToken,
  });
  const resp = await fetch(
    `${MAPBOX_SEARCHBOX}/retrieve/${encodeURIComponent(mapboxId)}?${params}`,
  );
  if (!resp.ok) return { found: false, label: '' };
  const data = await resp.json();
  const feature = data.features?.[0];
  if (!feature) return { found: false, label: '' };
  const [lon, lat] = feature.geometry?.coordinates ?? [];
  const result = {
    found: true,
    label: feature.properties?.full_address || feature.properties?.name || '',
    lat,
    lon,
    feature_type: feature.properties?.feature_type || '',
  };
  const bbox = feature.properties?.bbox;
  if (Array.isArray(bbox) && bbox.length === 4) {
    // Mapbox returns [west, south, east, north] — same as our static zip bounds.
    result.bounds = bbox;
  }
  return result;
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
    feature_type: 'postcode',
  };
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
    headers: headersWithAuth({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({
      model,
      region: regionPayload(region),
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

/**
 * Region-mean forecast (precip) for every model × lead in one request.
 * Response: `{ models, forecast_calendar }` where `forecast_calendar.per_model[modelKey]`
 * has `initDate` (ISO `YYYY-MM-DD`), `leadDaysMin`, `leadDaysMax`.
 * Valid date for integer lead L = `initDate` + L calendar days.
 */
export async function fetchForecastAllModels({ region }) {
  try {
    const resp = await fetch(`${API_BASE}/api/stats/forecast`, {
      method: 'POST',
      headers: headersWithAuth({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ region: regionPayload(region) }),
    });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

/**
 * Save the current region as a named shape (authenticated).
 * Returns the created shape object or null on failure.
 */
export async function saveRegion(name, region) {
  try {
    const resp = await fetch(`${API_BASE}/api/shapes`, {
      method: 'POST',
      headers: headersWithAuth({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ name, region: regionPayload(region) }),
    });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

/**
 * List all saved shapes for the authenticated user.
 * Returns { shapes: [...] } or null on failure.
 */
export async function listSavedRegions() {
  try {
    const resp = await fetch(`${API_BASE}/api/shapes`, {
      headers: headersWithAuth(),
    });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

