// App config (loaded once from /config.json); all data files load from the site root (see constants.js).
export const appConfig = $state({
  models: [],
  defaultModel: 'gfs',
  statistics: [],        // [{key, label, units?}, ...]
  defaultStatistic: 'nmad',
  maptilerApiKey: '',
  consistency: {},
  arEpisodes: {},
  atmosphericRivers: {},
});

export function applyAppConfig(data) {
  appConfig.models = data.models;
  appConfig.defaultModel = data.default_model;
  appConfig.statistics = data.statistics;
  appConfig.defaultStatistic = data.default_statistic;
  appConfig.maptilerApiKey = data.maptiler_api_key || '';
  appConfig.consistency = data.consistency || {};
  appConfig.arEpisodes = data.ar_episodes || {};
  appConfig.atmosphericRivers = data.atmospheric_rivers || {};
  ui.model = appConfig.defaultModel;
  ui.statistic = accuracyStatKeys().includes(appConfig.defaultStatistic)
    ? appConfig.defaultStatistic : accuracyStatKeys()[0];
  ui.consistencyThreshold = appConfig.consistency.default_threshold_mm;
  ui.consistencyScale = appConfig.consistency.default_scale_km;
}

// UI state — single object so components can mutate properties
export const ui = $state({
  /** accuracy | atmospheric-rivers — map PRESENTATION only (AR footprint layer,
   *  timeline, instruction text). Mode changes must never clear the selected
   *  region, statistic, period, tools, or episode: mode changes presentation;
   *  period changes data; neither destroys the other's state. */
  mode: 'accuracy',
  model: 'gfs',
  statistic: 'nmad',
  /** yearly | monthly | seasonal | ar-episode.
   *  AR mode uses the single 'ar-episode' period; its data scope is the
   *  date RANGE (arRangeStart..arRangeEnd), not a climatology population. */
  period: 'yearly',
  month: '01',
  season: 'djf',
  weatherOpacity: 0.85,
  leadFractional: 1.0,
  activeWindow: null,

  /** AR mode date-range selection (ISO YYYY-MM-DD). The range is written by the
   *  panel timeline's drill level (year → season → month → week) or by an
   *  episode pick — never dragged directly. */
  arRangeStart: null,
  arRangeEnd: null,
  /** The single inspected day (ISO) — always set while AR data is loaded.
   *  Defaults to the middle of the current range; drives the revision graph.
   *  Independent of the footprint frame scrubber (arFrameIndex). */
  arTargetDate: null,

  /** Selected 6-hour tARget v4 frame (ISO timestamp + manifest frame index),
   *  driven by the timeline's frame scrubber within the current range. */
  arTime: null,
  arFrameIndex: 0,
  arFramePath: null,
  /** Set when the current range came from a precomputed episode pick — the
   *  timeline shows the episode's days; null = range follows the drill level. */
  arEpisodeId: null,
  /** Episode hovered in the panel's episode dropdown (highlights its marker on
   *  the timeline while the pointer rests on the option). */
  arHoverEpisodeId: null,
  /** One-shot visible note (e.g. "episode not available for this model"). */
  arEpisodeNote: '',

  /** Forecast variable shown on the Ensemble Forecast chart: precip|temp|wind|humidity. */
  forecastVariable: 'precip',

  /** Jumpiness parameters: exceedance threshold q (mm/day) and neighborhood
   *  scale n (km). Must be one of appConfig.consistency's computed values. */
  consistencyThreshold: 10,
  consistencyScale: 50,

  // Draw tools: 'point' | 'rectangle' | 'polygon' | 'state' | 'county' | 'watershed' | 'military' | null
  activeTool: 'point',

  // Selected region: null or
  //   { type: 'point'|'rectangle'|'polygon', coordinates, bounds? }
  //   { type: 'admin', level: 'state'|'county', fips, name, bounds, geometry }
  //   { type: 'huc', huc, level, name, bounds, geometry }
  //   { type: 'military', site, name, bounds, geometry }
  selectedRegion: null,

  /** Default pin guide stays until the user places a point selection at least once. */
  hasUsedPinTool: false,
  /** After rectangle/polygon/watershed, pin-only guide omits the “draw an area” line. */
  hasUsedAreaDrawTool: false,
  /** Watershed drill breadcrumb (cleared on finalize or Escape). */
  wbdDrillPath: [],

  // ── Dashboard (one-screen analysis view) ──
  /** Bottom pane tab: revision | storms | efficiency | heavyrain | quadrant | forecast | timeline */
  dashTab: 'revision',
  /** Date population for the basin analyses: all_days | ar_all | non_ar | range. */
  analysisScope: 'all_days',
  /** Observed-event threshold (mm/day) for the heavy-rain trigger sweep. */
  eventThresholdMm: 10,
  /** Which consistency curve the consistency pane draws: jumpiness | convergence. */
  consistencyCurve: 'jumpiness',
  /** Revision tab view: revisions (run-to-run change) | path (raw forecast by lead vs observed). */
  revisionView: 'revisions',
});

/** Look up a statistic's display label from the config-provided list. */
export function statLabel(key) {
  const s = appConfig.statistics.find(s => s.key === key);
  return s ? s.label : key;
}

/** Statistic keys used by the accuracy UI (excludes archived forecast). */
export function accuracyStatKeys() {
  return appConfig.statistics.filter(s => s.scope === 'region').map(s => s.key);
}

export function winnerStatKeys() {
  return appConfig.statistics.filter(s => s.winner_support).map(s => s.key);
}

/** Consistency statistics (jumpiness/convergence): panel-only — no map tiles,
 *  no cross-model winners table (interpretation needs the full curve pair). */
export function isConsistencyStat(key) {
  return appConfig.consistency.stats.includes(key);
}

/** AR mode uses a single period; its data scope is the chosen date range. */
export const AR_PERIODS = ['ar-episode'];

export function isArPeriod(period) {
  return AR_PERIODS.includes(period);
}
