/** CSV export of everything the dashboard shows per lead: every model × accuracy stats, consistency, best model. */
import { appConfig } from '../state.svelte.js';

function esc(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[\r\n",]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

export function buildDashboardCsv({ accuracy, consistency, winners, statKeys, axis, region, period, month, season, threshold, scale, basinSeries, basinHuc }) {
  const lines = [];
  lines.push(`# region,${esc(JSON.stringify({ type: region?.type, name: region?.name, huc: region?.huc, fips: region?.fips, site: region?.site }))}`);
  lines.push(`# period,${period}${period === 'monthly' ? `,${month}` : period === 'seasonal' ? `,${season}` : ''}`);
  lines.push(`# consistency_threshold_mm,${threshold},consistency_scale_km,${scale}`);
  if (basinHuc) lines.push(`# basin,${basinHuc}`);
  const header = ['lead_day', 'model'];
  for (const k of statKeys) header.push(k);
  for (const k of appConfig.consistency.stats || []) header.push(k);
  for (const k of statKeys) header.push(`best_model_${k}`);
  lines.push(header.map(esc).join(','));
  for (const lead of axis) {
    for (const m of appConfig.models) {
      const row = [lead, m.key];
      const acc = accuracy?.[m.key]?.find((r) => r.lead === lead)?.stats || {};
      for (const k of statKeys) row.push(acc[k] && !acc[k].no_data ? acc[k].value : '');
      const con = consistency?.[m.key]?.find((r) => r.lead === lead)?.stats || {};
      for (const k of appConfig.consistency.stats || []) row.push(con[k] && !con[k].no_data ? con[k].value : '');
      for (const k of statKeys) row.push(winners?.[k]?.leads?.[String(lead)] ?? '');
      lines.push(row.map(esc).join(','));
    }
  }
  if (basinSeries) {
    lines.push('');
    lines.push('# basin forecast series: date, model, observed_mm, forecast_mm by lead (shortest first)');
    const leadMax = Math.max(...Object.values(basinSeries).map((r) => r.lead_max));
    lines.push(['date', 'model', 'observed_mm', ...Array.from({ length: leadMax }, (_, i) => `lead_${i + 1}_mm`)].join(','));
    for (const [key, rev] of Object.entries(basinSeries)) {
      for (let d = 0; d < rev.dates.length; d++) {
        lines.push([rev.dates[d], key, rev.observed[d] ?? '', ...rev.forecasts[d].map((v) => v ?? '')].map(esc).join(','));
      }
    }
  }
  return `﻿${lines.join('\r\n')}\r\n`;
}

export function downloadText(name, text, type = 'text/csv;charset=utf-8') {
  const blob = new Blob([text], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.rel = 'noopener';
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
