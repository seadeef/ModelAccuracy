import { MONTH_NAMES } from './constants.js';

/** Hover copy: full name + whether higher or lower is better (RB: near zero). */
const STATISTIC_HELP = {
  nrmse: 'Normalized root mean square error. Lower is better.',
  nmad: 'Normalized mean absolute deviation. Lower is better.',
  cc: 'Correlation coefficient (Pearson, raw values). Higher is better.',
  rb: 'Relative bias (sum of errors / sum of obs). Near zero is best.',
  forecast: 'Forecast metric. See documentation for interpretation.',
  jumpiness:
    'Run-to-run jumpiness J = 1 − FSS between successive runs for the same valid '
    + 'date, at the chosen precip threshold (mm/day) and neighborhood scale (km). '
    + '0 = consecutive runs agree; →1 = complete flip. The value at lead L compares '
    + 'the runs issued at leads L+1 and L. Low J alone is ambiguous (early skill or '
    + 'frozen hedging) — read it jointly with Convergence RMSE. '
    + 'Compare models only on identical threshold/scale/region/period slices.',
  convergence:
    'Convergence-to-truth: RMSE vs observed (PRISM) precip at each lead, pooled over '
    + 'the same complete-curve valid dates as Jumpiness. Lower is better; a curve that '
    + 'falls toward short leads means the forecast is closing in on the outcome.',
};

export function statisticTooltip(statKey) {
  return STATISTIC_HELP[statKey] ?? 'Verification metric for the map and charts.';
}

export const PERIOD_HELP = {
  yearly: 'Accumulate over the full year.',
  monthlyIntro: 'Accumulate over one calendar month each year.',
  seasonalIntro: 'Accumulate over a three-month season.',
};

/** Season keys match ui.season / static paths (djf, mam, jja, son). */
export const SEASON_PERIOD_HELP = {
  djf: 'Accumulate over Winter months',
  mam: 'Accumulate over Spring months',
  jja: 'Accumulate over Summer months',
  son: 'Accumulate over Autumn months',
};

export function monthlyPeriodTooltip(monthTwoDigits) {
  const m = parseInt(monthTwoDigits, 10);
  const name = MONTH_NAMES[m] || monthTwoDigits;
  return `Accumulate over ${name}.`;
}

/**
 * Tooltip for the active period/month/season (toolbar or panel context).
 */
export function currentPeriodTooltip(period, month, season) {
  if (period === 'yearly') return PERIOD_HELP.yearly;
  if (period === 'monthly') return monthlyPeriodTooltip(month);
  if (period === 'seasonal' && season && SEASON_PERIOD_HELP[season]) {
    return SEASON_PERIOD_HELP[season];
  }
  if (period === 'seasonal') return PERIOD_HELP.seasonalIntro;
  return PERIOD_HELP.yearly;
}
