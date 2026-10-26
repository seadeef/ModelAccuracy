<script>
  /** Accuracy at the inspected lead for the selected model; click a tile to switch the statistic everywhere. */
  import { ui, appConfig, accuracyStatKeys, statLabel } from '../state.svelte.js';
  import { statisticTooltip } from '../helpText.js';
  import { statColor } from '../palette.js';
  import { dash, selectedLead, statAt, modelLabel, palette, leadBeyondModel, leadBoundsOf } from './dashData.svelte.js';
  import { fmt } from './chartSetup.js';
  import { fitText } from '../fitText.js';

  let { onstatchange } = $props();
  const NAMES = { nmad: 'Error · NMAD', cc: 'Correlation', nrmse: 'RMS error', rb: 'Bias' };
  const ORDER = ['nmad', 'cc', 'nrmse', 'rb'];
  const keys = $derived.by(() => {
    const k = accuracyStatKeys().filter((x) => !appConfig.consistency.stats.includes(x));
    return [...ORDER.filter((x) => k.includes(x)), ...k.filter((x) => !ORDER.includes(x))];
  });
  const lead = $derived(selectedLead());
  const pal = $derived(palette());
  function pick(k) {
    ui.statistic = k;
    ui.activeWindow = null;
    onstatchange?.(k);
  }
</script>

<div class="tiles">
  <div class="eyebrow" use:fitText={{ min: 9 }} title={leadBeyondModel() ? `${modelLabel(ui.model)} forecasts ${leadBoundsOf(ui.model).max} days ahead at most; pick an earlier day or another model` : undefined}><em style:color={pal[ui.model]?.text}>{modelLabel(ui.model)}</em> at lead day <em>{lead}</em>{#if leadBeyondModel()}<span class="cap note">· beyond its {leadBoundsOf(ui.model).max}-day range</span>{/if}</div>
  <div class="grid">
    {#each keys as k (k)}
      {@const s = statAt(dash.accuracy?.[ui.model], lead, k)}
      <button type="button" class="tile" class:on={ui.statistic === k} style:--stat-color={statColor(k)} title={`${statisticTooltip(k)} Click to show this statistic on the map and the lead bar.`} onclick={() => pick(k)}>
        <span class="label">{NAMES[k] || statLabel(k)}</span>
        <span class="val">{s ? fmt(s.value, 2) : '—'}</span>
        <span class="unit">{s?.units || ' '}</span>
      </button>
    {/each}
  </div>
</div>

<style>
  .tiles { flex: none; padding: 12px 14px 14px; border: 1px solid var(--panel-border); border-radius: 12px; background: var(--pane-bg); display: flex; flex-direction: column; gap: 10px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
  .note { margin-left: 6px; text-transform: none; letter-spacing: 0; }
  .tile {
    display: flex; flex-direction: column; align-items: center; gap: 2px; padding: 10px 6px 8px; font: inherit; cursor: pointer;
    background: #ffffff05; border: 1px solid var(--panel-border); border-radius: 10px; color: var(--text-primary); position: relative; overflow: hidden;
  }
  .tile::after { content: ''; position: absolute; left: 0; right: 0; bottom: 0; height: 2px; background: var(--stat-color); opacity: 0; }
  .tile.on { border-color: color-mix(in srgb, var(--stat-color) 60%, transparent); background: color-mix(in srgb, var(--stat-color) 10%, transparent); }
  .tile.on::after { opacity: 1; }
  .tile:hover { background: var(--hover-bg); }
  .label { font-size: 11.5px; font-weight: 600; letter-spacing: .06em; text-transform: uppercase; color: var(--text-secondary); }
  .val { font-size: 27px; font-weight: 600; color: var(--stat-color); line-height: 1.1; font-variant-numeric: tabular-nums; letter-spacing: -.01em; }
  .unit { font-size: 11.5px; color: var(--text-secondary); min-height: 14px; }
</style>
