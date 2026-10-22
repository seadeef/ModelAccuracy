/**
 * Scope resolution shared by the basin analysis panes, plus the storm /
 * day context that every basin tab reads (set once in the tab strip).
 */
import { ui } from '../state.svelte.js';
import { dash } from './dashData.svelte.js';
import { scopeIndices, scopeClusters, datesInRange, wettestDay } from '../basinAnalysis.js';
import { isoToYmd, ymdToIso } from '../bootstrap.js';
import { fmt, fmtDate } from './chartSetup.js';

export function scopeOptions() {
  return {
    scope: ui.analysisScope,
    range: [isoToYmd(ui.arRangeStart), isoToYmd(ui.arRangeEnd)],
    episodes: dash.episodes?.episodes ?? [],
  };
}

/** Indices + cluster structure for one model's series under the current scope. */
export function scopeFor(rev) {
  if (!rev) return null;
  const opts = scopeOptions();
  const idx = scopeIndices(rev, opts);
  const info = scopeClusters(rev, idx, opts.episodes, opts.scope);
  return { idx, info, opts };
}

/** Human label of the active date population. */
export function scopeLabel() {
  const s = ui.analysisScope;
  if (s === 'ar_all') return 'AR days';
  if (s === 'non_ar') return 'Non-AR days';
  if (s === 'range') return fmtRange();
  return 'All days';
}

export function fmtRange() {
  if (!ui.arRangeStart || !ui.arRangeEnd) return '';
  const a = fmtDate(ui.arRangeStart);
  const b = fmtDate(ui.arRangeEnd);
  if (ui.arRangeStart === ui.arRangeEnd) return a;
  // Same month: "Dec 15 – 27"
  const sameMonth = ui.arRangeStart.slice(0, 7) === ui.arRangeEnd.slice(0, 7);
  return sameMonth ? `${a} – ${fmtDate(ui.arRangeEnd, { day: 'numeric' })}` : `${a} – ${b}`;
}

/** Catalogued storms touching the basin (analysed first), the basin's own AR episodes, then the full record. */
export function stormItems() {
  const items = [];
  const huc = dash.basinHuc;
  const rev = dash.basinSeries?.[ui.model] ?? Object.values(dash.basinSeries ?? {})[0] ?? null;
  const storms = (dash.storms?.events || []).filter((e) => e.in_analysis && e.basin_hucs?.includes(huc));
  for (const e of [...storms].reverse()) {
    items.push({
      key: `storm:${e.event_id}`, group: items.length === 0 ? 'Storms' : undefined,
      label: `${fmtDate(e.start_date)} – ${fmtDate(e.end_date)}`,
      sub: `${e.span_days} d · ${e.basins} basins${e.volume_km3 != null ? ` · ${fmt(e.volume_km3, 1)} km³` : ''}${e.volume_rank ? ` · #${e.volume_rank} by volume` : ''}`,
      start: e.start_date, end: e.end_date, peak: e.peak?.basin_huc === huc ? e.peak?.basin_date : null, eventId: e.event_id,
    });
  }
  const eps = dash.episodes?.episodes || [];
  eps.slice().reverse().forEach((ep, i) => items.push({
    key: `ep:${ep.id}`, group: i === 0 ? 'AR episodes in this basin' : undefined,
    label: `${fmtDate(ep.start_date)} – ${fmtDate(ep.end_date)}`, sub: `${ep.active_days} active d`,
    start: ep.start_date, end: ep.end_date, episodeId: ep.id,
  }));
  if (rev) {
    items.push({ key: 'all', group: 'Record', label: 'Full record', sub: `${rev.dates.length} dates`, start: ymdToIso(rev.dates[0]), end: ymdToIso(rev.dates[rev.dates.length - 1]) });
  }
  return items;
}

/** Set the storm / date range everywhere and move the inspected day to its peak. */
export function applyStormRange(item) {
  if (!item) return;
  ui.arRangeStart = item.start;
  ui.arRangeEnd = item.end;
  ui.arEpisodeId = item.episodeId ?? null;
  const rev = dash.basinSeries?.[ui.model] ?? Object.values(dash.basinSeries ?? {})[0] ?? null;
  if (!rev) return;
  const r = [isoToYmd(item.start), isoToYmd(item.end)];
  const positions = datesInRange(rev, r);
  const pk = item.peak ? rev.dates.indexOf(isoToYmd(item.peak)) : -1;
  const peak = pk >= 0 ? pk : wettestDay(rev, positions);
  if (peak != null) ui.arTargetDate = ymdToIso(rev.dates[peak]);
}
