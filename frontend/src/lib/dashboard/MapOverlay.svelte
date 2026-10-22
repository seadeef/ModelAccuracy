<script>
  /** Map-box chrome for the dashboard: what the overlay shows and its color key. */
  import { ui, statLabel } from '../state.svelte.js';
  import { dash, selectedLead, modelLabel, regionKind } from './dashData.svelte.js';

  const lead = $derived(selectedLead());
  const range = $derived(dash.valueRange);
  const reversed = $derived(range?.colormap?.includes('reversed'));
  const units = $derived(ui.statistic === 'cc' ? '' : '%');
  const keyTitle = $derived({ nmad: 'Normalized error (% of observed)', nrmse: 'Normalized RMS error (%)', cc: 'Correlation', rb: 'Relative bias (%)' }[ui.statistic] || statLabel(ui.statistic));
</script>

<div class="map-title">
  Selected {regionKind(ui.selectedRegion).toLowerCase()}
  <small>{modelLabel(ui.model)} · {statLabel(ui.statistic)} · lead day {lead}</small>
</div>
{#if range}
  <div class="map-key">
    <span>{keyTitle}</span>
    <i class:reversed></i>
    <div><b>{range.vmin}{units}</b><b>{((range.vmin + range.vmax) / 2).toFixed(range.vmax <= 1 ? 1 : 0)}{units}</b><b>{range.vmax}{units}</b></div>
  </div>
{/if}

<style>
  .map-title { position: absolute; left: 10px; right: 56px; top: 10px; padding: 8px 11px; border-radius: 7px; background: #111a26e8; font-size: 14px; font-weight: 600; z-index: 5; pointer-events: none; }
  .map-title small { display: block; color: var(--text-secondary); font-size: 11.5px; font-weight: 400; margin-top: 2px; }
  .map-key { position: absolute; left: 10px; right: 10px; bottom: 34px; padding: 6px 10px; background: #151d27ed; border-radius: 7px; font-size: 11px; z-index: 5; pointer-events: none; }
  .map-key i { display: block; height: 6px; margin-top: 4px; border-radius: 3px; background: linear-gradient(90deg, #3877ac, #e9edf2, #db584d); }
  .map-key i.reversed { background: linear-gradient(90deg, #db584d, #e9edf2, #3877ac); }
  .map-key > div { display: flex; justify-content: space-between; margin-top: 3px; }
  .map-key b { font-weight: 400; color: #c0c7d1; }
</style>
