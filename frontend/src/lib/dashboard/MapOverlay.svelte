<script>
  /**
   * Colour key for the raster overlay, naming what the map shows (model,
   * statistic, lead day, period). ``bare`` = the full-screen map before a
   * region is picked; otherwise the narrow dashboard map column.
   */
  import { ui, statLabel } from '../state.svelte.js';
  import { dash, selectedLead, modelLabel, periodLabel } from './dashData.svelte.js';
  let { bare = false } = $props();
  const lead = $derived(selectedLead());
  const range = $derived(dash.valueRange);
  const reversed = $derived(range?.colormap?.includes('reversed'));
  const units = $derived(ui.statistic === 'cc' ? '' : '%');
  const keyTitle = $derived({ nmad: 'Normalized error, % of observed', nrmse: 'Normalized RMS error, %', cc: 'Correlation', rb: 'Relative bias, %' }[ui.statistic] || statLabel(ui.statistic));
  const polarity = $derived(ui.statistic === 'cc' ? 'higher is better' : ui.statistic === 'rb' ? 'closer to 0 is better' : 'lower is better');
</script>

{#if range}
  <div class="map-key" class:bare>
    <div class="what"><b>{modelLabel(ui.model)}</b> · {statLabel(ui.statistic)} · lead day {lead} · {periodLabel()}</div>
    <span>{keyTitle} <em>· {polarity}</em></span>
    <i class:reversed></i>
    <div class="ticks"><b>{range.vmin}{units}</b><b>{((range.vmin + range.vmax) / 2).toFixed(range.vmax <= 1 ? 1 : 0)}{units}</b><b>{range.vmax}{units}</b></div>
  </div>
{/if}

<style>
  .map-key { position: absolute; left: 12px; right: 12px; bottom: 34px; padding: 8px 11px; background: #10161fe6; border: 1px solid var(--panel-border); border-radius: 8px; font-size: 11.5px; z-index: 5; pointer-events: none; }
  .what { font-size: 12.5px; color: var(--text-primary); margin-bottom: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .what b { font-weight: 600; }
  .map-key > span { color: #c0c7d1; }
  .map-key em { font-style: normal; color: var(--text-secondary); }
  .map-key i { display: block; height: 6px; margin-top: 5px; border-radius: 3px; background: linear-gradient(90deg, #3877ac, #e9edf2, #db584d); }
  .map-key i.reversed { background: linear-gradient(90deg, #db584d, #e9edf2, #3877ac); }
  .ticks { display: flex; justify-content: space-between; margin-top: 3px; }
  .ticks b { font-weight: 400; color: #c0c7d1; font-variant-numeric: tabular-nums; }
  /* Bare map: a compact card in the corner, clear of the toolbar and the attribution. */
  .map-key.bare { left: max(72px, calc(env(safe-area-inset-left, 0px) + 72px)); right: auto; bottom: max(14px, env(safe-area-inset-bottom, 0px)); width: 300px; padding: 9px 12px; background: var(--panel-bg); backdrop-filter: blur(20px) saturate(1.4); -webkit-backdrop-filter: blur(20px) saturate(1.4); box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4); }
  @media (max-width: 640px) { .map-key.bare { display: none; } }
  /* Short map column (laptop heights): the toolbar reaches the key's row, so start the key right of it. */
  @container map-box (max-height: 560px) { .map-key:not(.bare) { left: 72px; } }
</style>
