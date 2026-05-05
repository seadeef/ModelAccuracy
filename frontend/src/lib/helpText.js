import { MONTH_NAMES } from './constants.js';

/** Hover copy: full name + whether higher or lower is better (bias: near zero). */
const STATISTIC_HELP = {
  bias: 'Bias (mean model minus observation). Near zero is best.',
  nrmse: 'Normalized root mean square error. Lower is better.',
  nmad: 'Normalized mean absolute deviation. Lower is better.',
  sacc: 'Structured anomaly correlation. Higher is better.',
  forecast: 'Forecast metric. See documentation for interpretation.',
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
