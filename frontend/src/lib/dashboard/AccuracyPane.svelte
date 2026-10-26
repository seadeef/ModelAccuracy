<script>
  /** Forecast accuracy vs lead day for every model on the selected statistic. */
  import { ui, statLabel, appConfig } from '../state.svelte.js';
  import { statisticTooltip } from '../helpText.js';
  import { statColor } from '../palette.js';
  import { dash, leadAxis, palette, statSeries, statUnits, pickLead, hasAnyValue, emptyReason } from './dashData.svelte.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, endLabelPlugin, onClickIndex, fmt, alpha } from './chartSetup.js';

  const axis = $derived(leadAxis());
  const pal = $derived(palette());
  const units = $derived(statUnits(ui.statistic));
  const cursor = cursorPlugin(() => Math.max(1, Math.round(ui.leadFractional)) - 1);
  const ends = endLabelPlugin((ds) => ds.ident);
  const meta = $derived(appConfig.statistics.find((s) => s.key === ui.statistic) ?? {});
  const polarity = $derived(meta.winner_mode === 'higher' ? 'higher is better' : meta.winner_mode === 'absolute_lower' ? 'closer to 0 is better' : 'lower is better');
  /** The request answered, but every model is no_data for this statistic: say why instead of drawing empty axes. */
  const scored = $derived(hasAnyValue(dash.accuracy, ui.statistic));
  const why = $derived(emptyReason(ui.selectedRegion, { periodSpecific: ui.period === 'seasonal' || ui.period === 'monthly' }));

  const config = $derived.by(() => {
    const stat = ui.statistic;
    const datasets = appConfig.models.map((m) => ({
      label: m.label,
      ident: pal[m.key]?.ident,
      data: statSeries(dash.accuracy?.[m.key], stat, axis),
      borderColor: m.key === ui.model ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.8),
      endLabelColor: pal[m.key]?.text,
      borderWidth: m.key === ui.model ? 2.8 : 1.5,
      spanGaps: false,
      order: m.key === ui.model ? 0 : 1,
    }));
    const opts = leadAxisOptions({
      yTitle: units ? (units === '%' ? '% of observed' : units) : statLabel(stat),
      xTitle: 'Lead day',
      endLabels: true,
      tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y)}${units}`,
    });
    opts.plugins.tooltip.callbacks.title = (items) => `Lead day ${items[0]?.label}`;
    if (stat === 'cc') { opts.scales.y.min = 0; opts.scales.y.max = 1; }
    opts.onClick = onClickIndex((i) => pickLead(axis[i]));
    return { type: 'line', data: { labels: axis, datasets }, options: opts, plugins: [cursor, ends] };
  });
</script>

<article class="pane">
  <div class="head">
    <div class="eyebrow" title={`${statisticTooltip(ui.statistic)} Click the chart to inspect a lead day.`}>
      Forecast accuracy — <em style:color={statColor(ui.statistic)}>{statLabel(ui.statistic)}</em>
    </div>
    <span class="cap">{polarity}</span>
  </div>
  <div class="plot clickable">
    {#if dash.accuracy && scored}
      <ChartCanvas {config} />
    {:else if dash.accuracyLoading}
      <div class="plot-empty">Loading accuracy curves…</div>
    {:else if !dash.accuracy}
      <div class="plot-empty">The accuracy request failed; change the period or region to retry</div>
    {:else}
      <div class="plot-empty"><span class="reason"><b>No scored data for this {ui.period === 'yearly' ? 'region' : 'slice'}</b>{why}</span></div>
    {/if}
  </div>
</article>

<style>
  .pane { display: flex; flex-direction: column; min-width: 0; min-height: 0; padding: 12px 16px 10px; }
  .head { display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-bottom: 6px; min-width: 0; }
  .clickable { cursor: pointer; }
  .reason b { display: block; margin-bottom: 4px; font-weight: 600; color: var(--text-primary); }
</style>
