/** Scope resolution shared by the basin analysis panes. */
import { ui } from '../state.svelte.js';
import { dash } from './dashData.svelte.js';
import { scopeIndices, scopeClusters } from '../basinAnalysis.js';
import { isoToYmd } from '../bootstrap.js';

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

export function scopeLabel() {
  const s = ui.analysisScope;
  if (s === 'ar_all') return 'AR days';
  if (s === 'non_ar') return 'Non-AR days';
  if (s === 'range') return 'Selected range';
  return 'All days';
}

export function fmtRange() {
  const f = (iso) => (iso ? new Date(`${iso}T00:00:00Z`).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }) : '');
  if (!ui.arRangeStart || !ui.arRangeEnd) return '';
  return ui.arRangeStart === ui.arRangeEnd ? f(ui.arRangeStart) : `${f(ui.arRangeStart)} – ${f(ui.arRangeEnd)}`;
}
