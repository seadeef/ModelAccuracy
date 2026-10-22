/**
 * Shared data for the one-screen dashboard. Fetch orchestration lives in
 * Dashboard.svelte (effects need a component context); every pane, the
 * header and the accuracy tiles read from here.
 */
import { appConfig, ui } from '../state.svelte.js';
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
  /** { [model]: grid-pooled R payload } (yearly/seasonal). */
  revisionGrid: null,
  /** Basin analysis context (null when the region is not an exported HUC6 basin). */
  basinHuc: null,
  basinInfo: null,
  /** { [model]: basin revision series } */
  basinSeries: null,
  basinLoading: false,
  /** Episode catalog for the basin (model-independent metadata; taken from ui.model). */
  episodes: null,
  /** Curated storm catalog for the basin's HUC2 region. */
  storms: null,
  /** { [variable]: forecast payload } for the Forecast tab. */
  forecastByVar: null,
  /** Live map overlay value range for the key. */
  valueRange: null,
});

export function resetDash() {
  dash.accuracy = null;
  dash.consistency = null;
  dash.winners = {};
  dash.revisionGrid = null;
  dash.basinHuc = null;
  dash.basinInfo = null;
  dash.basinSeries = null;
  dash.episodes = null;
  dash.storms = null;
  dash.forecastByVar = null;
  dash.valueRange = null;
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
  if (ui.period === 'monthly') return `Month ${ui.month}`;
  return 'Full record';
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
