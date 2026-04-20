import { statLabel } from './state.svelte.js';

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const s = String(value);
  if (/[\r\n",]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function safeFilenamePart(s) {
  return String(s)
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'export';
}

function periodToken(period, month, season) {
  if (period === 'yearly') return 'yearly';
  if (period === 'seasonal') return `season-${season || ''}`;
  if (period === 'monthly') return `month-${month || ''}`;
  return safeFilenamePart(period);
}

function regionToken(region) {
  if (!region) return 'region';
  if (region.type === 'point') {
    const [lon, lat] = region.coordinates;
    return `pt-${lat.toFixed(3)}_${lon.toFixed(3)}`;
  }
  if (region.type === 'rectangle') {
    return 'bbox';
  }
  if (region.type === 'polygon') {
    return `poly-${region.coordinates?.length || 0}`;
  }
  return region.type || 'region';
}

function headerForStat(statKey, leadRows) {
  const label = statLabel(statKey);
  let units = '';
  for (const row of leadRows) {
    const u = row.stats?.[statKey]?.units;
    if (u) {
      units = u;
      break;
    }
  }
  return units ? `${label} (${units})` : label;
}

function modelDisplayName(models, key) {
  return models?.find((m) => m.key === key)?.label ?? key;
}

/**
 * Per-model forecast columns from the `/api/stats/forecast` payload.
 * Returns `{ columns: [{ modelKey, header }], valueAt(modelKey, leadKey) }`.
 * `columns` is empty if no model has any forecast data for the relevant leads.
 */
function forecastColumnsFromPayload(forecastPayload, models) {
  if (!forecastPayload?.models) return { columns: [], valueAt: () => null };
  const columns = [];
  const leadsByModel = new Map();
  for (const [modelKey, entry] of Object.entries(forecastPayload.models)) {
    const leadsObj = entry?.leads;
    if (!leadsObj || typeof leadsObj !== 'object') continue;
    let units = '';
    let hasAny = false;
    for (const cell of Object.values(leadsObj)) {
      if (!cell || cell.no_data || cell.value == null) continue;
      hasAny = true;
      if (!units && cell.units) units = cell.units;
    }
    if (!hasAny) continue;
    const label = modelDisplayName(models, modelKey);
    const init = forecastPayload.forecast_calendar?.per_model?.[modelKey]?.initDate;
    const suffix = units
      ? init
        ? ` (${units}, init ${init})`
        : ` (${units})`
      : init
      ? ` (init ${init})`
      : '';
    columns.push({ modelKey, header: `Forecast ${label}${suffix}` });
    leadsByModel.set(modelKey, leadsObj);
  }
  return {
    columns,
    valueAt(modelKey, leadKey) {
      const cell = leadsByModel.get(modelKey)?.[leadKey];
      if (!cell || cell.no_data || cell.value == null) return '';
      const v = Number(cell.value);
      return Number.isFinite(v) ? v : '';
    },
  };
}

/**
 * Build a CSV string from panel lead/stat rows (no network).
 * Optional `bestModelByLead` maps lead day string → model key (from in-memory winners payload).
 * Optional `forecastPayload` adds one `Forecast <Model>` column per model with forecast data.
 */
export function buildPanelStatsCsv({
  leadData,
  statKeys,
  modelKey,
  models,
  period,
  month,
  season,
  region,
  bestModelByLead = null,
  bestModelColumnHeader = null,
  forecastPayload = null,
}) {
  const forecast = forecastColumnsFromPayload(forecastPayload, models);

  const headers = ['lead_day'];
  for (const k of statKeys) {
    headers.push(headerForStat(k, leadData));
  }
  if (bestModelByLead && bestModelColumnHeader) {
    headers.push(bestModelColumnHeader);
  }
  for (const c of forecast.columns) {
    headers.push(c.header);
  }

  const lines = [headers.map(csvEscape).join(',')];

  for (const row of leadData) {
    const stats = row.stats || {};
    const leadKey = String(row.lead);
    const cells = [csvEscape(leadKey)];
    for (const k of statKeys) {
      const s = stats[k];
      if (!s || s.no_data || s.value === null || s.value === undefined) {
        cells.push('');
      } else {
        cells.push(csvEscape(s.value));
      }
    }
    if (bestModelByLead && bestModelColumnHeader) {
      const w = bestModelByLead.get(leadKey);
      cells.push(w != null ? csvEscape(modelDisplayName(models, w)) : '');
    }
    for (const c of forecast.columns) {
      cells.push(csvEscape(forecast.valueAt(c.modelKey, leadKey)));
    }
    lines.push(cells.join(','));
  }

  // UTF-8 BOM helps Excel on Windows recognize encoding.
  return `\uFEFF${lines.join('\r\n')}\r\n`;
}

export function downloadPanelStatsCsv(opts) {
  const {
    leadData,
    statKeys,
    modelKey,
    models,
    period,
    month,
    season,
    region,
    bestModelByLead,
    bestModelColumnHeader,
    forecastPayload,
  } = opts;

  const csv = buildPanelStatsCsv({
    leadData,
    statKeys,
    modelKey,
    models,
    period,
    month,
    season,
    region,
    bestModelByLead,
    bestModelColumnHeader,
    forecastPayload,
  });

  const name = [
    'raincheck-stats',
    safeFilenamePart(modelKey),
    periodToken(period, month, season),
    regionToken(region),
  ].join('_');

  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${name}.csv`;
  a.rel = 'noopener';
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
