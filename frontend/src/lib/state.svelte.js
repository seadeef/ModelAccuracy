// App config (loaded once from /config.json); all data files load from the site root (see constants.js).
export const appConfig = $state({
  models: [],
  defaultModel: 'gfs',
  statistics: [],        // [{key, label, units?}, ...]
  defaultStatistic: 'nmad',
  maptilerApiKey: '',
  // Consistency-statistics parameters (jumpiness/convergence); fallbacks match
  // statistics_plugins/consistency.py and are overwritten from config.json.
  consistency: {
    stats: ['convergence', 'jumpiness'],
    thresholds_mm: [1, 5, 10, 25],
    scales_km: [25, 50, 100],
    default_threshold_mm: 10,
    default_scale_km: 50,
    lock_in_fraction: 0.5,
  },
});

// UI state — single object so components can mutate properties
export const ui = $state({
  model: 'gfs',
  statistic: 'nmad',
  period: 'yearly',
  month: '01',
  season: 'djf',
  weatherOpacity: 0.85,
  leadFractional: 1.0,
  activeWindow: null,

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
});

/** Look up a statistic's display label from the config-provided list. */
export function statLabel(key) {
  const s = appConfig.statistics.find(s => s.key === key);
  return s ? s.label : key;
}

/** Statistic keys used by the accuracy UI (excludes archived forecast). */
export function accuracyStatKeys() {
  return appConfig.statistics.filter(s => s.key !== 'forecast').map(s => s.key);
}

/** Consistency statistics (jumpiness/convergence): panel-only — no map tiles,
 *  no cross-model winners table (interpretation needs the full curve pair). */
export function isConsistencyStat(key) {
  return appConfig.consistency.stats.includes(key);
}
