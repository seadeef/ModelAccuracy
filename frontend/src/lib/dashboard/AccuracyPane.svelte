<script>
  /** Forecast accuracy vs lead day for every model on the selected statistic. */
  import { ui, statLabel, appConfig } from '../state.svelte.js';
  import { statisticTooltip } from '../helpText.js';
  import { dash, selectedLead, leadAxis, palette, statSeries, statUnits, modelLabel, pickLead } from './dashData.svelte.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, onClickIndex, fmt } from './chartSetup.js';

  const axis = $derived(leadAxis());
  const pal = $derived(palette());
  const lead = $derived(selectedLead());
  const units = $derived(statUnits(ui.statistic));
  const cursor = cursorPlugin(() => selectedLead() - 1);

  const config = $derived.by(() => {
    const stat = ui.statistic;
    const datasets = appConfig.models.map((m) => ({
      label: m.label,
      data: statSeries(dash.accuracy?.[m.key], stat, axis),
      borderColor: pal[m.key]?.text,
      borderWidth: m.key === ui.model ? 2.6 : 1.4,
      spanGaps: false,
    }));
    const opts = leadAxisOptions({
      yTitle: units ? (units === '%' ? '% of observed' : units) : statLabel(stat),
      tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y)}${units}`,
    });
    opts.plugins.tooltip.callbacks.title = (items) => `Lead day ${items[0]?.label}`;
    if (stat === 'cc') { opts.scales.y.min = 0; opts.scales.y.max = 1; }
    opts.onClick = onClickIndex((i) => pickLead(axis[i]));
    return { type: 'line', data: { labels: axis, datasets }, options: opts, plugins: [cursor] };
  });

  const atLead = $derived.by(() => {
    const rows = dash.accuracy?.[ui.model];
    const s = rows?.find((r) => r.lead === lead)?.stats?.[ui.statistic];
    return s && !s.no_data ? s.value : null;
  });
</script>

<article class="pane">
  <div class="pane-title" title={`${statisticTooltip(ui.statistic)} Click the chart to inspect a lead day.`}>
    Forecast accuracy
    <span>{statLabel(ui.statistic)}{units ? ` (${units})` : ''}</span>
  </div>
  <div class="plot">
    {#if dash.accuracy}
      <ChartCanvas {config} />
    {:else}
      <div class="empty">{dash.accuracyLoading ? 'Loading accuracy curves…' : 'No accuracy data for this slice'}</div>
    {/if}
  </div>
  <div class="model-key">
    {#each appConfig.models as m}
      <span><i style:background={pal[m.key]?.text}></i>{m.label}</span>
    {/each}
    <span class="note">{modelLabel(ui.model)} at lead day {lead}: <b>{fmt(atLead)}{atLead != null ? units : ''}</b></span>
  </div>
</article>

<style>
  .pane { display: flex; flex-direction: column; min-width: 0; min-height: 0; padding: 9px 11px 7px; background: var(--pane-bg); border: 1px solid var(--panel-border); border-radius: 10px; }
  .pane-title { display: flex; justify-content: space-between; align-items: baseline; gap: 10px; font-size: 15px; font-weight: 600; margin-bottom: 2px; }
  .pane-title > span { font-size: 12px; color: var(--text-secondary); font-weight: 400; white-space: nowrap; }
  .plot { position: relative; flex: 1; min-height: 70px; cursor: pointer; }
  .empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--text-secondary); font-size: 12px; }
  .model-key { display: flex; gap: 12px; align-items: center; margin-top: 4px; color: #bec6d2; font-size: 11.5px; flex-wrap: wrap; }
  .model-key span { display: flex; align-items: center; gap: 5px; }
  .model-key i { width: 14px; height: 3px; border-radius: 2px; }
  .model-key .note { margin-left: auto; color: var(--text-secondary); }
  .model-key b { color: var(--text-primary); font-weight: 600; }
</style>
