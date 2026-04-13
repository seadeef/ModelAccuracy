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
 * Build a CSV string from panel lead/stat rows (no network).
 * Optional `bestModelByLead` maps lead day string → model key (from in-memory winners payload).
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
}) {
  const headers = ['lead_day'];
  for (const k of statKeys) {
    headers.push(headerForStat(k, leadData));
  }
  if (bestModelByLead && bestModelColumnHeader) {
    headers.push(bestModelColumnHeader);
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
  });

  const name = [
    'modelaccuracy-stats',
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
