/**
 * Shared data for the one-screen dashboard. Fetch orchestration lives in
 * Dashboard.svelte (effects need a component context); every pane, the
 * header and the accuracy tiles read from here.
 */
import { appConfig, ui } from '../state.svelte.js';
import { MONTH_NAMES, SEASON_NAMES } from '../constants.js';
import { modelPalette } from '../palette.js';
import { getModelLeadBounds } from '../tile.js';

export const dash = $state({
  /** { [model]: [{lead, stats}] } — verification stats per lead, every model. */
  accuracy: null,
  accuracyLoading: false,
  /** { [model]: [{lead, stats}] } — jumpiness + convergence per lead at the current q/n. */
  consistency: null,
  consistencyLoading: false,
  /** { [stat]: lead-winners payload } for the current region/period. */
  winners: {},
  winnersLoading: false,
  /** Exported HUC6 behind the series (null when the series is pooled from the per-cell cube). */
  basinHuc: null,
  /** { name, region, source: 'basin' | 'cube', cells?, nearestCell? } for the series below. */
  basinInfo: null,
  /** { [model]: daily series } — the exported basin JSON or the on-demand regional pool (same shape). */
  basinSeries: null,
  basinLoading: false,
  /** Episode catalog for the basin (model-independent metadata; taken from ui.model). */
  episodes: null,
  /** Curated storm catalog for the basin's HUC2 region (exported HUC6 basins only; the Storms tab's ledger). */
  storms: null,
  /** Analysed curated storms that reached the selected region — any region type — oldest first,
   *  each tagged with its catalog's ``huc2`` and ``regionName``; null while unknown. */
  regionStorms: null,
  /** { [variable]: forecast payload } for the Forecast tab. */
  forecastByVar: null,
  /** Live map overlay value range for the key. */
  valueRange: null,
});

/** Stable key of a region for fetch de-duplication and range ownership. */
export function regionKey(r) {
  if (!r) return '';
  if (r.type === 'point') return `pt:${r.coordinates.join(',')}`;
  if (r.type === 'rectangle') return `rect:${(r.bounds || r.coordinates || []).join(',')}`;
  if (r.type === 'polygon') return `poly:${JSON.stringify(r.coordinates)}`;
  if (r.type === 'admin') return `admin:${r.level}:${r.fips}`;
  if (r.type === 'huc') return `huc:${r.huc}`;
  if (r.type === 'military') return `mil:${r.site}`;
  return JSON.stringify(r);
}

/** Who set the inspected range: the region key when it was a storm or episode
 *  picked for that region (storm choices are region-bound: the next region
 *  opens on its own storm unless the same storm reached it), null when the
 *  user typed or scrubbed a custom range (custom ranges travel between regions). */
export const rangeOrigin = { regionKey: null };

export function resetDash() {
  dash.accuracy = null;
  dash.consistency = null;
  dash.winners = {};
  dash.basinHuc = null;
  dash.basinInfo = null;
  dash.basinSeries = null;
  dash.episodes = null;
  dash.storms = null;
  dash.regionStorms = null;
  dash.forecastByVar = null;
}

/** Integer lead the dashboard inspects everywhere (map, tiles, cursors). */
export function selectedLead() {
  return Math.max(1, Math.round(ui.leadFractional));
}

/** Map callback for lead changes (registered by Dashboard; the map owns the tile lead). */
export const leadSink = { fn: null };

/** Inspect lead ``l`` everywhere: lead bar, tiles, chart cursors and the map overlay. */
export function pickLead(l) {
  const lead = Math.max(1, Math.round(l));
  ui.leadFractional = lead;
  ui.activeWindow = null;
  leadSink.fn?.(lead);
}

export function modelLabel(key) {
  return appConfig.models.find((m) => m.key === key)?.label ?? key;
}

export function palette() {
  return modelPalette(appConfig.models);
}

export function modelKeys() {
  return appConfig.models.map((m) => m.key);
}

/** Global lead axis: 1 … max lead over every configured model. */
export function leadAxis() {
  let max = 1;
  for (const m of appConfig.models) max = Math.max(max, m.lead_days_max ?? 14);
  return Array.from({ length: max }, (_, i) => i + 1);
}

export function leadBoundsOf(model) {
  return getModelLeadBounds(appConfig.models, model);
}

/** True when the inspected lead lies past the selected model's longest lead
 *  (the lead axis is global, so NBM / WeatherNext 2 can be asked for day 14). */
export function leadBeyondModel(lead = selectedLead(), model = ui.model) {
  return lead > leadBoundsOf(model).max;
}

/** The lead the map can actually show for the selected model (MapView clamps the same way). */
export function mapLead(lead = selectedLead(), model = ui.model) {
  const { min, max } = leadBoundsOf(model);
  return Math.max(min, Math.min(max, lead));
}

/** Any finite value for ``stat`` in any model's rows? (A payload can be present yet all no_data.) */
export function hasAnyValue(byModel, stat) {
  if (!byModel) return false;
  for (const rows of Object.values(byModel)) {
    if (rows?.some((r) => { const s = r?.stats?.[stat]; return s && !s.no_data && s.value != null; })) return true;
  }
  return false;
}

/** Verification grid extent (each model's grid.json): 0.25° cells over 20–55°N, 130–60°W. */
const GRID = { w: -130, s: 20, e: -60, n: 55, step: 0.25 };

/** [west, south, east, north] of a region, or null for a point. */
export function regionBounds(region) {
  if (!region || region.type === 'point') return null;
  if (region.bounds?.length === 4) return region.bounds;
  if (region.type === 'polygon' && Array.isArray(region.coordinates)) {
    const xs = region.coordinates.map((c) => c[0]);
    const ys = region.coordinates.map((c) => c[1]);
    return [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)];
  }
  return null;
}

/**
 * Why a pane has nothing to draw although the request succeeded. The backend
 * scores a region from the 0.25° cells whose centres fall inside it; a
 * sub-cell unit takes the cell containing it, but places off the grid, cells
 * over water and dry slices still answer honestly with no_data.
 * ``periodSpecific`` = the message is for a season or month (the full record
 * may still score here).
 */
export function emptyReason(region, { periodSpecific = false } = {}) {
  const b = regionBounds(region);
  // Centre of the region (a state's bbox can clip the grid corner, e.g. Alaska's panhandle).
  const c = b ? [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2] : region?.type === 'point' ? region.coordinates : null;
  const offGrid = !!c && (c[0] < GRID.w || c[0] > GRID.e || c[1] < GRID.s || c[1] > GRID.n);
  if (offGrid) return 'Outside the verification grid: forecasts are scored over the contiguous United States only (20–55°N, 130–60°W).';
  if (b && (b[2] - b[0]) < GRID.step && (b[3] - b[1]) < GRID.step && !periodSpecific) {
    return 'Smaller than one 0.25° grid cell, and the cell containing it is not scored (over water or outside the land mask).';
  }
  if (periodSpecific) return `Too few scored wet days in ${periodPhrase()} at this location. Normalised statistics are undefined for a dry slice; the full record or another period may still score here.`;
  if (region?.type === 'point') return 'No scored grid cell at this point. Cells over water and outside the land mask are not verified.';
  return 'No 0.25° cell centre falls inside this region (cell centres are about 25 km apart). Try a slightly larger unit.';
}

/** Value of one stat at one lead for one model from a rows payload. */
export function statAt(rows, lead, stat) {
  const row = rows?.find((r) => r.lead === lead);
  const s = row?.stats?.[stat];
  if (!s || s.no_data || s.value == null) return null;
  return s;
}

/** Series over the global lead axis (null where the model has no lead). */
export function statSeries(rows, stat, axis) {
  return axis.map((lead) => statAt(rows, lead, stat)?.value ?? null);
}

export function statUnits(key) {
  return appConfig.statistics.find((s) => s.key === key)?.units ?? '';
}

export function periodLabel() {
  if (ui.period === 'seasonal') return { djf: 'Winter (DJF)', mam: 'Spring (MAM)', jja: 'Summer (JJA)', son: 'Autumn (SON)' }[ui.season] || 'Seasonal';
  if (ui.period === 'monthly') return MONTH_NAMES[Number(ui.month)] || `Month ${ui.month}`;
  return 'Full record';
}

/** The period as a phrase inside a sentence: "winter", "March", "the full record". */
export function periodPhrase() {
  if (ui.period === 'seasonal') return (SEASON_NAMES[ui.season] || 'this season').toLowerCase();
  if (ui.period === 'monthly') return MONTH_NAMES[Number(ui.month)] || 'this month';
  return 'the full record';
}

/** Region display name for the header. */
export function regionTitle(region) {
  if (!region) return '';
  if (region.type === 'point') {
    const [lon, lat] = region.coordinates;
    return `Point ${lat.toFixed(2)}°, ${lon.toFixed(2)}°`;
  }
  if (region.type === 'rectangle') return 'Drawn rectangle';
  if (region.type === 'polygon') return 'Drawn polygon';
  if (region.type === 'admin') return region.name || (region.level === 'state' ? 'State' : 'County');
  if (region.type === 'huc') return region.name || `HUC${String(region.huc).length} ${region.huc}`;
  if (region.type === 'military') return region.name || 'Military installation';
  return 'Selected region';
}

export function regionKind(region) {
  if (!region) return '';
  if (region.type === 'huc') return 'Watershed';
  if (region.type === 'admin') return region.level === 'state' ? 'State' : 'County';
  if (region.type === 'military') return 'Military installation';
  if (region.type === 'point') return 'Grid cell';
  return 'Drawn area';
}
