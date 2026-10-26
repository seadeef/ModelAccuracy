import { authHeaders } from './authSession.svelte.js';
import { STATIC_BASE } from './constants.js';
import { appConfig } from './state.svelte.js';

const API_BASE = '';

function headersWithAuth(base = {}) {
  return { ...base, ...authHeaders() };
}

/** Drop the (potentially large) ``geometry`` field from admin/huc regions before sending.
 *  The backend resolves polygons from FIPS/HUC via boundary modules; the
 *  frontend only carries ``geometry`` for local rendering (dim mask). */
function regionPayload(region) {
  if (!region) return region;
  if (
    (region.type === 'admin' || region.type === 'huc' || region.type === 'military')
    && region.geometry
  ) {
    const { geometry: _g, ...rest } = region;
    return rest;
  }
  return region;
}

export async function fetchAtmosphericRiverManifest() {
  const url = appConfig.atmosphericRivers?.manifest_url || `${STATIC_BASE}/ar/manifest.json`;
  const resp = await fetch(url, { cache: 'no-cache' });
  if (!resp.ok) throw new Error(`Atmospheric-river manifest fetch failed (${resp.status})`);
  return resp.json();
}

export async function fetchAtmosphericRiverFrame(path, { signal } = {}) {
  const base = appConfig.atmosphericRivers?.manifest_url || `${STATIC_BASE}/ar/manifest.json`;
  const url = new URL(path, new URL(base, window.location.href));
  const resp = await fetch(url, { signal });
  if (!resp.ok) throw new Error(`Atmospheric-river frame fetch failed (${resp.status})`);
  return resp.json();
}

/** Base URL for the per-basin revision-series JSON (config-provided so the path
 *  never hardcodes here). Trailing slash trimmed. */
function basinRevisionsBase() {
  const base = appConfig.atmosphericRivers?.revisions_base || '/ar/basin_revisions';
  return base.replace(/\/+$/, '');
}

/**
 * Raw per-basin forecast revision series for one model + California HUC6, from
 * the static ``/ar/basin_revisions/{model}/{huc}.json`` asset (see
 * ``export_basin_revisions`` in export_static.py). Shape:
 * ``{ huc, name, model, lead_min, lead_max, dates:[YYYYMMDD…],
 *     forecasts:[[…lead_min..lead_max]…per date], observed:[…per date],
 *     lock:{stabilization, accuracy, gap}, lock_in_fraction }``.
 * Returns null on any failure (missing basin/model export → panel degrades).
 */
export async function fetchBasinRevisions({ model, huc, signal } = {}) {
  if (!model || !huc) return null;
  try {
    const resp = await fetch(`${basinRevisionsBase()}/${model}/${huc}.json`, { signal });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

/**
 * The per-basin revision series for EVERY model at one HUC6, fetched in parallel
 * (same fan-out idiom as ``fetchConsistencyAllModels``). Feeds the range-scoped
 * best-model-by-day ranking. Returns ``{ [modelKey]: payload }`` (models whose
 * export is absent are omitted), or null when nothing loaded.
 */
export async function fetchBasinRevisionsAllModels({ huc, signal } = {}) {
  const models = appConfig.models;
  if (!huc || !models?.length) return null;
  const entries = await Promise.all(
    models.map(async (m) => [m.key, await fetchBasinRevisions({ model: m.key, huc, signal })]),
  );
  const byModel = {};
  for (const [key, payload] of entries) if (payload) byModel[key] = payload;
  return Object.keys(byModel).length ? byModel : null;
}

/**
 * On-demand daily series for ANY region (``POST /api/series``): the backend
 * pools the per-cell cube over the region at request time and answers one
 * basin-shaped payload per model (``{ models: { [key]: payload | null } }``).
 * Models without a cube, or a region covering no land cell, come back null
 * and are omitted. Returns ``{ [modelKey]: payload }`` or null when nothing
 * loaded (404 = no cube exported at all).
 */
export async function fetchRegionSeriesAllModels({ region, signal } = {}) {
  if (!region) return null;
  try {
    const resp = await fetch(`${API_BASE}/api/series`, {
      method: 'POST',
      headers: headersWithAuth({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ region: regionPayload(region) }),
      signal,
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    const byModel = {};
    for (const [key, payload] of Object.entries(data?.models || {})) if (payload) byModel[key] = payload;
    return Object.keys(byModel).length ? byModel : null;
  } catch {
    return null;
  }
}

/**
 * The catalogued atmospheric-river basins (HUC6 codes) a region's grid cells
 * share (``POST /api/series/basins``). The storm pickers keep only the
 * curated storms whose ``basin_hucs`` meet this list. ``[]`` when the region
 * touches none; null when the request failed.
 */
export async function fetchRegionBasins({ region, signal } = {}) {
  if (!region) return null;
  try {
    const resp = await fetch(`${API_BASE}/api/series/basins`, {
      method: 'POST',
      headers: headersWithAuth({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ region: regionPayload(region) }),
      signal,
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    return Array.isArray(data?.hucs) ? data.hucs.map(String) : null;
  } catch {
    return null;
  }
}

/**
 * Heavy-rain trigger sweep for any region, judged per CELL-DAY over the
 * region (``POST /api/series/heavy-rain``): the server's counterpart of
 * ``triggerSkill`` for regions where the region-mean event degenerates.
 * ``dates`` is the YYYYMMDD population (the current scope). Returns
 * ``{ [modelKey]: result }`` (``result.unavailable`` when the model lacks the
 * lead) or null.
 */
export async function fetchHeavyRainAllModels({ region, q, lead, dates, signal } = {}) {
  if (!region) return null;
  try {
    const resp = await fetch(`${API_BASE}/api/series/heavy-rain`, {
      method: 'POST',
      headers: headersWithAuth({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ region: regionPayload(region), q, lead, dates: dates ?? null }),
      signal,
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    const byModel = {};
    for (const [key, r] of Object.entries(data?.models || {})) if (r) byModel[key] = r;
    return Object.keys(byModel).length ? byModel : null;
  } catch {
    return null;
  }
}

function verificationStatisticsOnly() {
  return appConfig.statistics.filter((s) => s.scope === 'region');
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
  threshold,
  scale,
  statistics,
}) {
  const statList = statistics?.length
    ? appConfig.statistics.filter((s) => statistics.includes(s.key))
    : verificationStatisticsOnly();
  if (!statList?.length) {
    throw new Error('Statistics not loaded yet; wait for config.');
  }
  const resp = await fetch(`${API_BASE}/api/stats`, {
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
      // Jumpiness slice parameters (ignored by the other statistics).
      ...(threshold != null ? { threshold } : {}),
      ...(scale != null ? { scale } : {}),
    }),
  });
  if (!resp.ok) {
    throw new Error(`Stats query failed (${resp.status})`);
  }
  const data = await resp.json();
  return data.results ?? [];
}

/** Precomputed AR episode catalog (``GET /api/ar/episodes``).
 * With ``huc``: one CA HUC6's episodes (+ ``scopes_available``, basin ``name``).
 * Without ``huc``: episode-first entry — ``{ basins: [{huc, name, episode_count, episodes}] }``. */
export async function fetchArEpisodes({ model, huc, signal }) {
  const params = new URLSearchParams({ model });
  if (huc) params.set('huc', huc);
  const resp = await fetch(`${API_BASE}/api/ar/episodes?${params}`, { signal });
  if (!resp.ok) throw new Error(`AR episode catalog failed (${resp.status})`);
  return resp.json();
}

/** Episode- or scope-context statistics (``POST /api/stats`` with an ``ar_episode``
 * context). Exactly one of ``episodeId`` (single episode) or ``scope``
 * (``'ar_all'`` | ``'non_ar'`` pooled date population) must be provided.
 * Returns ``{ stats: { revision_efficiency: {...}, lock_in_gap: {...} } }`` —
 * per-episode entries carry no CI (``single_event: true``); scope entries carry
 * ``value_ci`` / ``clusters`` / ``interval_suppressed``. */
export async function fetchArEpisodeStats({ model, huc, episodeId, scope, statistics, signal }) {
  const context = { type: 'ar_episode', huc };
  if (episodeId) context.episodeId = episodeId;
  else context.scope = scope;
  const resp = await fetch(`${API_BASE}/api/stats`, {
    method: 'POST',
    headers: headersWithAuth({ 'Content-Type': 'application/json' }),
    signal,
    body: JSON.stringify({
      model,
      context,
      statistics,
    }),
  });
  if (!resp.ok) throw new Error(`AR episode statistics failed (${resp.status})`);
  return resp.json();
}

/**
 * Grid-pooled revision efficiency R for an arbitrary region
 * (``POST /api/stats/revision-efficiency``; period yearly|seasonal only).
 * Response: ``{ value, units, no_data, dates, definition: 'grid-pooled',
 * skill_gain_nonpositive? }`` — no CI (no per-date data online).
 * Returns null on any failure so the panel card degrades to its em-dash state.
 */
export async function fetchRevisionEfficiency({ model, region, period, season, signal }) {
  const endpoint = appConfig.consistency?.revision_endpoint || '/api/stats/revision-efficiency';
  try {
    const resp = await fetch(`${API_BASE}${endpoint}`, {
      method: 'POST',
      headers: headersWithAuth({ 'Content-Type': 'application/json' }),
      signal,
      body: JSON.stringify({
        model,
        region: regionPayload(region),
        period,
        ...(period === 'seasonal' && season ? { season } : {}),
      }),
    });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

/**
 * One consistency statistic's per-lead curve for EVERY model over the same
 * region/period slice (the multi-model overlay). Fires one ``POST /api/stats``
 * per model in parallel (same fan-out pattern as ``fetchForecastAllVariables``),
 * each over that model's own lead bounds.
 * Returns ``{ [modelKey]: [{ lead, stats }, ...] }`` (models that fail are
 * omitted), or null when nothing succeeded.
 */
export async function fetchConsistencyAllModels({
  region,
  period,
  month,
  season,
  threshold,
  scale,
  statistic,
}) {
  try {
    const models = appConfig.models;
    if (!models?.length) return null;
    const entries = await Promise.all(
      models.map(async (m) => {
        try {
          const rows = await fetchStatsAllLeads({
            model: m.key,
            region,
            period,
            month,
            season,
            minLead: m.lead_days_min ?? 1,
            maxLead: m.lead_days_max ?? 14,
            threshold,
            scale,
            statistics: [statistic],
          });
          return [m.key, rows?.length ? rows : null];
        } catch {
          return [m.key, null];
        }
      }),
    );
    const byModel = {};
    for (const [key, rows] of entries) if (rows) byModel[key] = rows;
    return Object.keys(byModel).length ? byModel : null;
  } catch {
    return null;
  }
}

/**
 * Forecast init dates per model, fetched directly from the static asset
 * `${STATIC_BASE}/forecast/forecast_calendar.json` (NOT the API) so the SPA
 * always reflects the latest exported forecast independently of the backend.
 * Shape: `{ per_model: { [modelKey]: { initDate, leadDaysMin, leadDaysMax } } }`.
 * Cached in-memory for `FORECAST_CALENDAR_TTL_MS`; the asset itself is served
 * with a short Cache-Control so CloudFront/browser also refresh it.
 */
const FORECAST_CALENDAR_TTL_MS = 5 * 60 * 1000;
let _calendarCache = { data: null, at: 0 };

export async function fetchForecastCalendar() {
  const now = Date.now();
  if (_calendarCache.data && now - _calendarCache.at < FORECAST_CALENDAR_TTL_MS) {
    return _calendarCache.data;
  }
  try {
    const resp = await fetch(`${STATIC_BASE}/forecast/forecast_calendar.json`, { cache: 'no-cache' });
    if (!resp.ok) return _calendarCache.data;
    const data = await resp.json();
    _calendarCache = { data, at: now };
    return data;
  } catch {
    return _calendarCache.data;
  }
}

/**
 * Region-mean forecast (precip) for every model × lead in one request.
 * Returns `{ models, forecast_calendar }`. The `models` values come from the API
 * (`POST /api/stats/forecast`); `forecast_calendar` is fetched directly from the
 * static asset so init dates stay current with new forecast exports without the
 * backend echoing them. `forecast_calendar.per_model[modelKey]` has `initDate`
 * (ISO `YYYY-MM-DD`), `leadDaysMin`, `leadDaysMax`; valid date for lead L =
 * `initDate` + L calendar days.
 */
export async function fetchForecastAllModels({ region }) {
  try {
    const [resp, calendar] = await Promise.all([
      fetch(`${API_BASE}/api/stats/forecast`, {
        method: 'POST',
        headers: headersWithAuth({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ region: regionPayload(region) }),
      }),
      fetchForecastCalendar(),
    ]);
    if (!resp.ok) return null;
    const data = await resp.json();
    if (calendar) data.forecast_calendar = calendar;
    return data;
  } catch {
    return null;
  }
}

/**
 * Region-mean forecast for EVERY variable (precip/temp/wind/humidity) × every
 * model × lead. Fires one `POST /api/stats/forecast` per variable in parallel so
 * the panel's variable selector can show each variable's current value and switch
 * between them with no refetch. The variable list comes from the forecast
 * calendar's `variables` catalog (falls back to precip).
 * Returns `{ [variable]: { models, variable, forecast_calendar } }`, or null.
 */
export async function fetchForecastAllVariables({ region }) {
  try {
    const calendar = await fetchForecastCalendar();
    const variables = calendar?.variables?.map((v) => v.key) || ['precip'];
    const region_ = regionPayload(region);
    const entries = await Promise.all(
      variables.map(async (variable) => {
        try {
          const resp = await fetch(`${API_BASE}/api/stats/forecast`, {
            method: 'POST',
            headers: headersWithAuth({ 'Content-Type': 'application/json' }),
            body: JSON.stringify({ region: region_, variable }),
          });
          if (!resp.ok) return [variable, null];
          const data = await resp.json();
          if (!data?.models) return [variable, null];
          if (calendar) data.forecast_calendar = calendar;
          return [variable, data];
        } catch {
          return [variable, null];
        }
      }),
    );
    const byVar = {};
    for (const [variable, data] of entries) if (data) byVar[variable] = data;
    return Object.keys(byVar).length ? byVar : null;
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

// ── Dashboard additions ──────────────────────────────────────────────────

/**
 * Index of HUC6 basins with an exported forecast-series history
 * (``/ar/basins.json``, written by ``export_static.py --basins``):
 * ``{ huc_level, regions:[{huc2,name,basins:[huc]}], basins:{[huc]:{name,huc2,region,models}} }``.
 * Cached for the session; null when the export is absent.
 */
let _basinsIndex = null;
export async function fetchBasinsIndex() {
  if (_basinsIndex !== null) return _basinsIndex;
  try {
    const resp = await fetch(`${STATIC_BASE}/ar/basins.json`);
    _basinsIndex = resp.ok ? await resp.json() : false;
  } catch {
    _basinsIndex = false;
  }
  return _basinsIndex;
}

/** Curated storm catalog for one HUC2 region (``/ar/storms/{huc2}.json``), or null. */
const _stormCache = new Map();
export async function fetchStormCatalog(huc2) {
  if (!huc2) return null;
  if (_stormCache.has(huc2)) return _stormCache.get(huc2);
  const p = (async () => {
    try {
      const resp = await fetch(`${STATIC_BASE}/ar/storms/${huc2}.json`);
      return resp.ok ? await resp.json() : null;
    } catch {
      return null;
    }
  })();
  _stormCache.set(huc2, p);
  return p;
}

/**
 * Per-lead statistics for EVERY model over the same region/period slice, one
 * ``POST /api/stats`` per model in parallel, each over that model's own lead
 * bounds. Returns ``{ [modelKey]: [{lead, stats}] }`` (failed models omitted)
 * or null when nothing succeeded.
 */
export async function fetchStatsAllLeadsAllModels({
  region, period, month, season, statistics, threshold, scale, signal,
}) {
  const models = appConfig.models;
  if (!models?.length) return null;
  const entries = await Promise.all(
    models.map(async (m) => {
      try {
        const rows = await fetchStatsAllLeads({
          model: m.key, region, period, month, season,
          minLead: m.lead_days_min ?? 1, maxLead: m.lead_days_max ?? 14,
          threshold, scale, statistics, signal,
        });
        return [m.key, rows?.length ? rows : null];
      } catch {
        return [m.key, null];
      }
    }),
  );
  const byModel = {};
  for (const [key, rows] of entries) if (rows) byModel[key] = rows;
  return Object.keys(byModel).length ? byModel : null;
}
