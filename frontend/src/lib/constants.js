export const MONTH_NAMES = [
  '', 'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];

export const SEASON_NAMES = {
  djf: 'Winter',
  mam: 'Spring',
  jja: 'Summer',
  son: 'Autumn',
};

export const FALLBACK_STYLE_URL = 'https://demotiles.maplibre.org/style.json';

/** Config, zip, tiles → ``static_export/static`` (``/static``). */
export const STATIC_BASE = '/static';

/** Grid and .bin layers → ``static_export/data`` (``/data``). */
export const DATA_BASE = '/data';

// Keep in sync with backend/tile_overlay_constants.py TILE_IMAGE_BOUNDS_WGS84.
export const TILE_IMAGE_BOUNDS_WGS84 = [
  -125.0,
  24.095327339179846,
  -65.94784325726698,
  50.0,
];
