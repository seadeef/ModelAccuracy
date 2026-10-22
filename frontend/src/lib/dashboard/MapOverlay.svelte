<script>
  /**
   * Map-box chrome: what the raster overlay shows and its colour key.
   * ``bare`` = the full-screen map before a region is picked (key only, with
   * the model · statistic · lead line folded into it); otherwise the dashboard
   * title card plus the key.
   */
  import { ui, statLabel } from '../state.svelte.js';
  import { dash, selectedLead, modelLabel, regionKind, periodLabel } from './dashData.svelte.js';
  let { bare = false } = $props();
  const lead = $derived(selectedLead());
  const range = $derived(dash.valueRange);
  const reversed = $derived(range?.colormap?.includes('reversed'));
  const units = $derived(ui.statistic === 'cc' ? '' : '%');
  const keyTitle = $derived({ nmad: 'Normalized error (% of observed)', nrmse: 'Normalized RMS error (%)', cc: 'Correlation', rb: 'Relative bias (%)' }[ui.statistic] || statLabel(ui.statistic));
  const polarity = $derived(ui.statistic === 'cc' ? 'higher is better' : ui.statistic === 'rb' ? 'closer to 0 is better' : 'lower is better');
</script>

{#if !bare}
  <div class="map-title">
    Selected {regionKind(ui.selectedRegion).toLowerCase()}
    <small>{modelLabel(ui.model)} · {statLabel(ui.statistic)} · lead day {lead}</small>
  </div>
{/if}
{#if range}
  <div class="map-key" class:bare>
    {#if bare}
      <div class="what"><b>{modelLabel(ui.model)}</b> · {statLabel(ui.statistic)} · lead day {lead} · {periodLabel()}</div>
    {/if}
    <span>{keyTitle} <em>· {polarity}</em></span>
    <i class:reversed></i>
    <div><b>{range.vmin}{units}</b><b>{((range.vmin + range.vmax) / 2).toFixed(range.vmax <= 1 ? 1 : 0)}{units}</b><b>{range.vmax}{units}</b></div>
  </div>
{/if}

<style>
  .map-title { position: absolute; left: 10px; right: 56px; top: 10px; padding: 8px 11px; border-radius: 7px; background: #111a26e8; font-size: 14px; font-weight: 600; z-index: 5; pointer-events: none; }
  .map-title small { display: block; color: var(--text-secondary); font-size: 11.5px; font-weight: 400; margin-top: 2px; }
  .map-key { position: absolute; left: 10px; right: 10px; bottom: 34px; padding: 6px 10px; background: #151d27ed; border-radius: 7px; font-size: 11px; z-index: 5; pointer-events: none; }
  .map-key em { font-style: normal; color: var(--text-secondary); }
  .map-key i { display: block; height: 6px; margin-top: 4px; border-radius: 3px; background: linear-gradient(90deg, #3877ac, #e9edf2, #db584d); }
  .map-key i.reversed { background: linear-gradient(90deg, #db584d, #e9edf2, #3877ac); }
  .map-key > div { display: flex; justify-content: space-between; margin-top: 3px; }
  .map-key b { font-weight: 400; color: #c0c7d1; }
  /* Bare map: a compact card in the corner, clear of the toolbar and the attribution. */
  .map-key.bare { left: max(72px, calc(env(safe-area-inset-left, 0px) + 72px)); right: auto; bottom: max(14px, env(safe-area-inset-bottom, 0px)); width: 300px; padding: 8px 11px; background: var(--panel-bg); backdrop-filter: blur(20px) saturate(1.4); -webkit-backdrop-filter: blur(20px) saturate(1.4); border: 1px solid var(--panel-border); box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4); }
  .map-key.bare .what { display: block; margin: 0 0 5px; font-size: 12px; color: var(--text-primary); }
  .map-key.bare .what b { font-weight: 600; color: var(--text-primary); }
  @media (max-width: 640px) { .map-key.bare { display: none; } }
  /* Short map column (laptop heights): the toolbar reaches the key's row, so start the key right of it. */
  @container map-box (max-height: 560px) { .map-key:not(.bare) { left: 72px; } }
</style>
