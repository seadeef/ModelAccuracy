<script>
  /** Accuracy at the inspected lead for the selected model; click a tile to switch the statistic everywhere. */
  import { ui, appConfig, accuracyStatKeys, statLabel } from '../state.svelte.js';
  import { statisticTooltip } from '../helpText.js';
  import { statColor } from '../palette.js';
  import { dash, selectedLead, statAt } from './dashData.svelte.js';
  import { fmt } from './chartSetup.js';

  let { onstatchange } = $props();
  const NAMES = { nmad: 'Error (NMAD)', cc: 'Correlation', nrmse: 'RMS error', rb: 'Bias' };
  const ORDER = ['nmad', 'cc', 'nrmse', 'rb'];
  const keys = $derived.by(() => {
    const k = accuracyStatKeys().filter((x) => !appConfig.consistency.stats.includes(x));
    return [...ORDER.filter((x) => k.includes(x)), ...k.filter((x) => !ORDER.includes(x))];
  });
  const lead = $derived(selectedLead());
  function pick(k) {
    ui.statistic = k;
    ui.activeWindow = null;
    onstatchange?.(k);
  }
</script>

<div class="tiles">
  <div class="block-title">Accuracy at lead day <b>{lead}</b></div>
  <div class="grid">
    {#each keys as k (k)}
      {@const s = statAt(dash.accuracy?.[ui.model], lead, k)}
      <button type="button" class="tile" class:on={ui.statistic === k} style:--stat-color={statColor(k)} title={statisticTooltip(k)} onclick={() => pick(k)}>
        <span class="label">{NAMES[k] || statLabel(k)}</span>
        <span class="val">{s ? fmt(s.value, 2) : '—'}</span>
        <span class="unit">{s?.units || ''}</span>
      </button>
    {/each}
  </div>
</div>

<style>
  .tiles { flex: none; padding: 9px 10px 10px; border: 1px solid var(--panel-border); border-radius: 10px; background: var(--pane-bg); }
  .block-title { font-size: 14px; font-weight: 600; margin-bottom: 7px; }
  .block-title b { font-weight: 600; color: var(--accent); }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
  .tile {
    display: flex; flex-direction: column; align-items: center; gap: 1px; padding: 6px 4px; font: inherit; cursor: pointer;
    background: #ffffff05; border: 1px solid var(--panel-border); border-radius: 8px; color: var(--text-primary); position: relative; overflow: hidden;
  }
  .tile::after { content: ''; position: absolute; left: 0; right: 0; top: 0; height: 2px; background: var(--stat-color); opacity: .35; }
  .tile.on { border-color: var(--stat-color); background: color-mix(in srgb, var(--stat-color) 12%, transparent); }
  .tile.on::after { opacity: 1; }
  .tile:hover { background: var(--hover-bg); }
  .label { font-size: 11px; color: var(--text-secondary); }
  .val { font-size: 22px; font-weight: 600; color: var(--stat-color); line-height: 1.1; font-variant-numeric: tabular-nums; }
  .unit { font-size: 10px; color: var(--text-secondary); min-height: 12px; }
</style>
