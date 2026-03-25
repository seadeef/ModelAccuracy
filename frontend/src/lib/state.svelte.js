// App config (loaded once from /static/config.json); data layers use /data (see constants.js).
export const appConfig = $state({
  models: [],
  defaultModel: 'gfs',
  statistics: [],        // [{key, label, units?}, ...]
  defaultStatistic: 'bias',
  maptilerApiKey: '',
});

// UI state — single object so components can mutate properties
export const ui = $state({
  model: 'gfs',
  statistic: 'bias',
  period: 'yearly',
  month: '01',
  season: 'djf',
  weatherOpacity: 0.85,
  leadFractional: 1.0,
  statusMessage: 'Idle',
  activeWindow: null,

  // Draw tools: 'point' | 'rectangle' | 'polygon' | null
  activeTool: 'point',

  // Selected region: null or { type, coordinates, bounds }
  selectedRegion: null,

  /** Default pin guide stays until the user places a point selection at least once. */
  hasUsedPinTool: false,
  /** After rectangle/polygon, pin-only guide omits the “draw an area” line. */
  hasUsedAreaDrawTool: false,
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
