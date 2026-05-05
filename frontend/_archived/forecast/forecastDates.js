/** Valid date = model cycle init (YYYY-MM-DD) + lead day offset (UTC calendar days). */

function parseInitUtc(iso) {
  if (!iso || typeof iso !== 'string') return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso.trim());
  if (!m) return null;
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
}

export function addCalendarDaysUtc(isoYmd, n) {
  const d = parseInitUtc(isoYmd);
  if (!d) return null;
  d.setUTCDate(d.getUTCDate() + n);
  return d;
}

const intlUtcShort = new Intl.DateTimeFormat('en-US', {
  month: 'numeric',
  day: 'numeric',
  timeZone: 'UTC',
});

const intlUtcMedium = new Intl.DateTimeFormat('en-US', {
  month: 'long',
  day: 'numeric',
  year: 'numeric',
  timeZone: 'UTC',
});

const intlUtcFull = new Intl.DateTimeFormat('en-US', {
  weekday: 'long',
  month: 'long',
  day: 'numeric',
  year: 'numeric',
  timeZone: 'UTC',
});

/** Compact calendar date (UTC), e.g. 3/21 — top bar / chips. */
export function formatShortDate(d) {
  if (!d) return '';
  return intlUtcShort.format(d);
}

/** e.g. March 21, 2026 (UTC) — popup header. */
export function formatMediumForecastDateUtc(d) {
  if (!d) return '';
  return intlUtcMedium.format(d);
}

/** e.g. Saturday, March 21, 2026 (UTC) — bottom status pill. */
export function formatFullForecastDateUtc(d) {
  if (!d) return '';
  return intlUtcFull.format(d);
}

/**
 * Slider / top bar: one short date at integer lead, or "3/1–3/2" when interpolating between days.
 */
export function forecastLeadSliderLabel(modelKey, leadFrac, initByModel) {
  const init = initByModel?.[modelKey];
  if (!init) {
    if (leadFrac === Math.floor(leadFrac)) return `Day ${leadFrac}`;
    return leadFrac.toFixed(1);
  }
  const lo = Math.floor(leadFrac);
  const hi = Math.ceil(leadFrac);
  const dLo = addCalendarDaysUtc(init, lo);
  const dHi = addCalendarDaysUtc(init, hi);
  if (!dLo || !dHi) {
    if (leadFrac === Math.floor(leadFrac)) return `Day ${leadFrac}`;
    return leadFrac.toFixed(1);
  }
  if (lo === hi) return formatShortDate(dLo);
  return `${formatShortDate(dLo)}–${formatShortDate(dHi)}`;
}

/** Bottom pill: "Showing forecast: Saturday, March 21, 2026" (rounded lead). */
export function forecastStatusPillLabel(modelKey, leadFrac, initByModel) {
  const r = Math.round(leadFrac);
  const init = initByModel?.[modelKey];
  if (!init) return `Showing forecast: day ${r}`;
  const d = addCalendarDaysUtc(init, r);
  return d
    ? `Showing forecast: ${formatFullForecastDateUtc(d)}`
    : `Showing forecast: day ${r}`;
}

/** Bottom pill while cross-fading two lead days. */
export function forecastStatusPillRangeLabel(modelKey, leadFrac, initByModel) {
  const init = initByModel?.[modelKey];
  if (!init) return `Showing forecast: lead ${leadFrac.toFixed(1)}`;
  const lo = Math.floor(leadFrac);
  const hi = Math.ceil(leadFrac);
  const dLo = addCalendarDaysUtc(init, lo);
  const dHi = addCalendarDaysUtc(init, hi);
  if (!dLo || !dHi) return `Showing forecast: lead ${leadFrac.toFixed(1)}`;
  if (lo === hi) return `Showing forecast: ${formatFullForecastDateUtc(dLo)}`;
  return `Showing forecast: ${formatFullForecastDateUtc(dLo)} \u2013 ${formatFullForecastDateUtc(dHi)}`;
}

/** Bottom pill for lead window chip, full dates. */
export function forecastWindowStatusPillLabel(modelKey, start, end, initByModel) {
  const init = initByModel?.[modelKey];
  if (!init) return `Showing forecast: days ${start}\u2013${end}`;
  const d1 = addCalendarDaysUtc(init, start);
  const d2 = addCalendarDaysUtc(init, end);
  if (!d1 || !d2) return `Showing forecast: days ${start}\u2013${end}`;
  return `Showing forecast: ${formatFullForecastDateUtc(d1)} \u2013 ${formatFullForecastDateUtc(d2)}`;
}

function _loadingFromShowing(s) {
  return s.replace(/^Showing forecast: /, 'Loading forecast: ').replace(/\s*$/, '') + '\u2026';
}

export function forecastLoadingPillLabel(modelKey, leadFrac, initByModel) {
  return _loadingFromShowing(forecastStatusPillLabel(modelKey, leadFrac, initByModel));
}

export function forecastLoadingPillRangeLabel(modelKey, leadFrac, initByModel) {
  return _loadingFromShowing(forecastStatusPillRangeLabel(modelKey, leadFrac, initByModel));
}

/** Popup header: date for the lead day used in the query (rounded). */
export function forecastPopupLeadLabel(modelKey, leadFrac, initByModel) {
  const r = Math.round(leadFrac);
  const init = initByModel?.[modelKey];
  if (!init) return `Day ${r}`;
  const d = addCalendarDaysUtc(init, r);
  return d ? formatMediumForecastDateUtc(d) : `Day ${r}`;
}

/** Lead window chip: e.g. 3/1–3/7 */
export function forecastWindowChipLabel(modelKey, start, end, initByModel) {
  const init = initByModel?.[modelKey];
  if (!init) return `${start}–${end}d`;
  const d1 = addCalendarDaysUtc(init, start);
  const d2 = addCalendarDaysUtc(init, end);
  if (!d1 || !d2) return `${start}–${end}d`;
  return `${formatShortDate(d1)}–${formatShortDate(d2)}`;
}
