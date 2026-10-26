<script>
  /**
   * Colour key for the raster overlay, naming what the map shows (model,
   * statistic, lead day, period). ``bare`` = the full-screen map before a
   * region is picked; otherwise the narrow dashboard map column.
   */
  import { ui, statLabel } from '../state.svelte.js';
  import { dash, selectedLead, modelLabel, periodLabel, mapLead, leadBeyondModel } from './dashData.svelte.js';
  import { arData } from '../arManifest.svelte.js';
  let { bare = false } = $props();
  /** AR mode: name the purple footprint layer too, and say when the frame is the local 2026 extension rather than the canonical release. */
  const arOn = $derived(ui.mode === 'atmospheric-rivers');
  const arFrame = $derived(arData.frames[ui.arFrameIndex] ?? null);
  /** What the raster shows: MapView clamps the inspected lead to the model's range. */
  const lead = $derived(mapLead());
  const clamped = $derived(leadBeyondModel());
  const range = $derived(dash.valueRange);
  const reversed = $derived(range?.colormap?.includes('reversed'));
  const units = $derived(ui.statistic === 'cc' ? '' : '%');
  const keyTitle = $derived({ nmad: 'Normalized error, % of observed', nrmse: 'Normalized RMS error, %', cc: 'Correlation', rb: 'Relative bias, %' }[ui.statistic] || statLabel(ui.statistic));
  const polarity = $derived(ui.statistic === 'cc' ? 'higher is better' : ui.statistic === 'rb' ? 'closer to 0 is better' : 'lower is better');
</script>

{#if range}
  <div class="map-key" class:bare>
    <div class="what"><b>{modelLabel(ui.model)}</b> · {statLabel(ui.statistic)} · lead day {lead}{clamped ? ' (its longest)' : ''} · {periodLabel()}</div>
    <span>{keyTitle} <em>· {polarity}</em></span>
    <i class:reversed></i>
    <div class="ticks"><b>{range.vmin}{units}</b><b>{((range.vmin + range.vmax) / 2).toFixed(range.vmax <= 1 ? 1 : 0)}{units}</b><b>{range.vmax}{units}</b></div>
    {#if arOn}
      <div class="ar-line"><i class="ar-swatch"></i><span>Atmospheric-river footprint · tARget v4{arFrame?.non_canonical ? ' · local ERA5 extension, not the canonical release' : ' · canonical release'}</span></div>
    {/if}
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
  .ar-line { display: flex; align-items: center; gap: 6px; margin-top: 7px; padding-top: 6px; border-top: 1px solid var(--rule); color: #c0c7d1; min-width: 0; }
  .ar-line span { white-space: normal; line-height: 1.3; }
  .ar-swatch { flex: none; width: 14px; height: 10px; border-radius: 2px; background: #a78bfa66; border: 1px solid #c4b5fd99; }
  /* Bare map: a compact card in the corner, clear of the toolbar and the attribution. */
  /* On the bare map the key sits bottom-left; when the AR panel floats at the
     bottom (App sets --ar-panel-h on .map-fill) the key moves up to clear it. */
  .map-key.bare { left: max(72px, calc(env(safe-area-inset-left, 0px) + 72px)); right: auto; bottom: calc(max(14px, env(safe-area-inset-bottom, 0px)) + var(--ar-panel-h, 0px)); width: 300px; padding: 9px 12px; background: var(--panel-bg); backdrop-filter: blur(20px) saturate(1.4); -webkit-backdrop-filter: blur(20px) saturate(1.4); box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4); }
  @media (max-width: 640px) { .map-key.bare { display: none; } }
  /* Short map column (laptop heights): the toolbar reaches the key's row, so start the key right of it. */
  @container map-box (max-height: 560px) { .map-key:not(.bare) { left: 72px; } }
</style>
