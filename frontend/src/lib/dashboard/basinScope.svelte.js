/**
 * Scope resolution shared by the basin analysis panes, plus the storm /
 * day context that every basin tab reads (set once in the tab strip).
 */
import { ui } from '../state.svelte.js';
import { dash, regionKey, rangeOrigin } from './dashData.svelte.js';
import { scopeIndices, scopeClusters, datesInRange, wettestDay, wettestSpells } from '../basinAnalysis.js';
import { isoToYmd, ymdToIso } from '../bootstrap.js';
import { fmt, fmtDate } from './chartSetup.js';

/** AR / non-AR populations need the basin's episode catalog; regions served
 *  from the per-cell cube have none (yet), so those scopes are unavailable there. */
export function arScopesAvailable() {
  return (dash.episodes?.episodes?.length ?? 0) > 0;
}

/** The scope actually in force: an AR scope with no catalog falls back to all days. */
export function activeScope() {
  const s = ui.analysisScope;
  return (s === 'ar_all' || s === 'non_ar') && !arScopesAvailable() ? 'all_days' : s;
}

export function scopeOptions() {
  return {
    scope: activeScope(),
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
  const s = activeScope();
  if (s === 'ar_all') return 'AR days';
  if (s === 'non_ar') return 'Non-AR days';
  if (s === 'range') return fmtRange();
  return 'All days';
}

/** One date span, the same way everywhere: "Dec 15 – 27", "Mar 30 – Apr 3",
 *  "Jan 1, 2025 – Aug 6, 2026" (years only when they differ), "Apr 15" for one day.
 *  Takes ISO or YYYYMMDD. */
export function fmtSpan(start, end) {
  if (!start || !end) return '';
  const s = String(start).replace(/-/g, '');
  const e = String(end).replace(/-/g, '');
  const a = fmtDate(s);
  if (s === e) return a;
  if (s.slice(0, 4) !== e.slice(0, 4)) {
    const y = { month: 'short', day: 'numeric', year: 'numeric' };
    return `${fmtDate(s, y)} – ${fmtDate(e, y)}`;
  }
  return s.slice(0, 6) === e.slice(0, 6) ? `${a} – ${fmtDate(e, { day: 'numeric' })}` : `${a} – ${fmtDate(e)}`;
}

export function fmtRange() {
  return fmtSpan(ui.arRangeStart, ui.arRangeEnd);
}

/** Regions with a curated storm catalog (``ar/storms/{huc2}.json``). */
export const STORM_REGIONS = { 18: 'California', 17: 'Pacific Northwest' };
/** PRISM water volume over the catalog region above which a storm outside the curated windows is listed in the pickers. */
export const NOTABLE_KM3 = 0.5;

/** The catalogued storms that reached any of ``hucs`` (HUC6 codes; null = every
 *  storm), oldest first, each tagged with ``huc2`` and ``regionName``. Storms
 *  outside the curated version windows (``in_analysis`` false) are kept: the
 *  dashboard computes their ledger from the region's own series, so the latest
 *  storms on record stay reachable. Both storm pickers and the default inspected
 *  range read this list. */
export function regionStormsFor(hucs, catalogs) {
  const want = hucs ? new Set(hucs.map(String)) : null;
  const out = [];
  const seen = new Set();
  for (const cat of catalogs || []) {
    if (!cat) continue;
    const huc2 = String(cat.huc2 ?? '');
    const regionName = cat.region || STORM_REGIONS[huc2] || '';
    for (const e of cat.events || []) {
      if (seen.has(e.event_id)) continue;
      const touched = want ? (e.basin_hucs || []).filter((h) => want.has(String(h))).length : null;
      if (want && !touched) continue;
      seen.add(e.event_id);
      // Pickers list the analysed storms plus any later storm that carried real
      // water (≥ 0.5 km³ over its region); the Storms tab's table keeps every event.
      const notable = !!e.in_analysis || (e.volume_km3 ?? 0) >= NOTABLE_KM3;
      out.push({ ...e, huc2, regionName, touched, notable });
    }
  }
  return dedupeOverlapping(out).sort((a, b) => (a.start_date < b.start_date ? -1 : a.start_date > b.start_date ? 1 : 0));
}

/** One row per synoptic storm: the California and Pacific Northwest catalogs
 *  both record a storm that crossed their boundary, so overlapping events from
 *  different catalogs collapse onto the one that reached more of the selected
 *  region (then the larger volume). */
function dedupeOverlapping(events) {
  const score = (e) => [e.touched ?? 0, e.volume_km3 ?? 0];
  const better = (a, b) => { const [x1, y1] = score(a); const [x2, y2] = score(b); return x1 !== x2 ? x1 > x2 : y1 >= y2; };
  const kept = [];
  for (const e of events.slice().sort((a, b) => (a.start_date < b.start_date ? -1 : 1))) {
    const i = kept.findIndex((k) => k.huc2 !== e.huc2 && e.start_date <= k.end_date && e.end_date >= k.start_date);
    if (i < 0) kept.push(e);
    else if (better(e, kept[i])) kept[i] = e;
  }
  return kept;
}

/** One picker item per storm: label = dates, sub = region · span · basins · volume, where
 *  "3 of 16 basins" says how many of the storm's basins lie in the selected region (a storm
 *  catalogued for California can reach Oregon through the Klamath). The peak day is the
 *  basin's own when the series is a catalogued basin, else the storm's region-wide peak. */
export function stormItem(e, huc = null) {
  const peak = huc && e.peak?.basin_huc === huc ? e.peak?.basin_date : e.peak?.date;
  const basins = e.touched != null && e.touched < (e.basins ?? 0) && !huc ? `${e.touched} of ${e.basins} basins` : `${e.basins} basins`;
  return {
    key: `storm:${e.event_id}`,
    label: fmtSpan(e.start_date, e.end_date),
    sub: `${e.regionName ? `${e.regionName} · ` : ''}${e.span_days} d · ${basins}${e.volume_km3 != null ? ` · ${fmt(e.volume_km3, 1)} km³` : ''}`,
    start: e.start_date, end: e.end_date, peak: peak ?? null, eventId: e.event_id,
  };
}

/** Catalogued storms that reached the region (newest first), the basin's own
 *  AR episodes, the region's wettest five-day spells (PRISM only, any region),
 *  then the full record. */
export function stormItems() {
  const items = [];
  const huc = dash.basinHuc;
  const rev = dash.basinSeries?.[ui.model] ?? Object.values(dash.basinSeries ?? {})[0] ?? null;
  for (const e of [...(dash.regionStorms || [])].filter((s) => s.notable).reverse()) {
    items.push({ ...stormItem(e, huc), group: items.length === 0 ? 'Storms that reached this region' : undefined });
  }
  const eps = dash.episodes?.episodes || [];
  eps.slice().reverse().forEach((ep, i) => items.push({
    key: `ep:${ep.id}`, group: i === 0 ? 'AR episodes in this basin' : undefined,
    label: fmtSpan(ep.start_date, ep.end_date), sub: `${ep.active_days} active d`,
    start: ep.start_date, end: ep.end_date, episodeId: ep.id,
  }));
  if (rev) {
    wettestSpells(rev, { span: 5, count: 6 }).forEach((sp, i) => items.push({
      key: `spell:${rev.dates[sp.start]}`, group: i === 0 ? 'Wettest 5-day spells' : undefined,
      label: fmtSpan(rev.dates[sp.start], rev.dates[sp.end]),
      sub: `${fmt(sp.total, 0)} mm observed${i === 0 ? ' · wettest on record' : ''}`,
      start: ymdToIso(rev.dates[sp.start]), end: ymdToIso(rev.dates[sp.end]), peak: sp.peak != null ? ymdToIso(rev.dates[sp.peak]) : null,
    }));
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
  rangeOrigin.regionKey = regionKey(ui.selectedRegion) || null;
  const rev = dash.basinSeries?.[ui.model] ?? Object.values(dash.basinSeries ?? {})[0] ?? null;
  if (!rev) return;
  const r = [isoToYmd(item.start), isoToYmd(item.end)];
  const positions = datesInRange(rev, r);
  const pk = item.peak ? rev.dates.indexOf(isoToYmd(item.peak)) : -1;
  const peak = pk >= 0 ? pk : wettestDay(rev, positions);
  if (peak != null) ui.arTargetDate = ymdToIso(rev.dates[peak]);
}
