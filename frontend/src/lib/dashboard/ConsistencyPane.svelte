<script>
  /**
   * Run-to-run consistency: jumpiness J (1 − FSS between successive runs) or
   * convergence RMSE vs lead for every model at the chosen threshold/scale.
   */
  import { ui, appConfig, isArPeriod } from '../state.svelte.js';
  import { statisticTooltip } from '../helpText.js';
  import { statColor } from '../palette.js';
  import { dash, leadAxis, palette, statSeries, pickLead, hasAnyValue, emptyReason } from './dashData.svelte.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, endLabelPlugin, onClickIndex, fmt, alpha } from './chartSetup.js';

  const axis = $derived(leadAxis());
  const pal = $derived(palette());
  const stabKey = $derived(appConfig.consistency.curve_roles?.stability ?? 'jumpiness');
  const accKey = $derived(appConfig.consistency.curve_roles?.accuracy ?? 'convergence');
  const curve = $derived(ui.consistencyCurve === 'convergence' ? accKey : stabKey);
  const isJ = $derived(curve === stabKey);
  const cursor = cursorPlugin(() => Math.max(1, Math.round(ui.leadFractional)) - 1);
  const ends = endLabelPlugin((ds) => ds.ident);
  const supported = $derived(ui.period === 'yearly' || ui.period === 'seasonal' || isArPeriod(ui.period));
  /** Answered but all no_data for the drawn curve (dry cells leave J undefined; small units cover no cell). */
  const scored = $derived(hasAnyValue(dash.consistency, curve));
  const why = $derived(emptyReason(ui.selectedRegion, { periodSpecific: ui.period === 'seasonal' }));

  const config = $derived.by(() => {
    const datasets = appConfig.models.map((m) => ({
      label: m.label,
      ident: pal[m.key]?.ident,
      data: statSeries(dash.consistency?.[m.key], curve, axis),
      borderColor: m.key === ui.model ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.8),
      endLabelColor: pal[m.key]?.text,
      borderWidth: m.key === ui.model ? 2.8 : 1.5,
      spanGaps: false,
      order: m.key === ui.model ? 0 : 1,
    }));
    const opts = leadAxisOptions({
      yTitle: isJ ? 'J = 1 − FSS between runs' : 'mm/day',
      xTitle: isJ ? 'Lead day of the newer run' : 'Lead day',
      yMin: 0,
      ...(isJ ? { yMax: 1 } : {}),
      endLabels: true,
      tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, isJ ? 3 : 2)}`,
    });
    opts.plugins.tooltip.callbacks.title = (items) => (isJ
      ? `Runs at lead ${Number(items[0]?.label) + 1} → ${items[0]?.label}` : `Lead day ${items[0]?.label}`);
    opts.onClick = onClickIndex((i) => pickLead(axis[i]));
    return { type: 'line', data: { labels: axis, datasets }, options: opts, plugins: [cursor, ends] };
  });
</script>

<article class="pane">
  <div class="head">
    <div class="eyebrow" title={`${statisticTooltip(curve)} Click the chart to inspect a lead day.`}>
      Forecast consistency — <em style:color={statColor(curve)}>{isJ ? 'J' : 'RMSE to PRISM'}</em>
    </div>
    <span class="seg">
      <button type="button" class:active={isJ} title={statisticTooltip(stabKey)} onclick={() => { ui.consistencyCurve = 'jumpiness'; }}>J</button>
      <button type="button" class:active={!isJ} title={statisticTooltip(accKey)} onclick={() => { ui.consistencyCurve = 'convergence'; }}>RMSE</button>
    </span>
  </div>
  <div class="params">
    <span class="cap">{isJ ? 'lower is steadier' : 'lower is closer to PRISM'}</span>
    <div class="controls">
      <span class="cap" title="Exceedance threshold q: rain heavier than this counts as an event.">mm/d</span>
      <span class="chips">
        {#each appConfig.consistency.thresholds_mm ?? [] as q}
          <button type="button" class="chip" class:active={ui.consistencyThreshold === q} onclick={() => { ui.consistencyThreshold = q; }}>{q}</button>
        {/each}
      </span>
      <span class="cap" title="Neighborhood scale n: width of the window over which event fractions are compared.">km</span>
      <span class="chips">
        {#each appConfig.consistency.scales_km ?? [] as n}
          <button type="button" class="chip" class:active={ui.consistencyScale === n} onclick={() => { ui.consistencyScale = n; }}>{n}</button>
        {/each}
      </span>
    </div>
  </div>
  <div class="plot clickable">
    {#if dash.consistency && scored}
      <ChartCanvas {config} />
    {:else if !supported}
      <div class="plot-empty">Consistency is computed for the full record and the four seasons</div>
    {:else if dash.consistencyLoading}
      <div class="plot-empty">Loading consistency curves…</div>
    {:else if !dash.consistency}
      <div class="plot-empty">The consistency request failed; change the period or region to retry</div>
    {:else}
      <div class="plot-empty"><span class="reason"><b>No scored {isJ ? 'J' : 'RMSE'} for this {ui.period === 'yearly' ? 'region' : 'season'}</b>{isJ && ui.period === 'seasonal' ? `J needs runs that both forecast rain above ${ui.consistencyThreshold} mm/d; a dry season leaves it undefined. ` : ''}{why}</span></div>
    {/if}
  </div>
</article>

<style>
  .pane { display: flex; flex-direction: column; min-width: 0; min-height: 0; padding: 12px 16px 10px; }
  .head { display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-bottom: 4px; min-width: 0; }
  .params { display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-bottom: 4px; min-width: 0; }
  .controls { display: flex; align-items: center; gap: 6px; flex: none; }
  .controls .cap { margin-left: 4px; }
  .controls .chip { padding: 2px 8px; font-size: 11.5px; }
  .clickable { cursor: pointer; }
  .reason b { display: block; margin-bottom: 4px; font-weight: 600; color: var(--text-primary); }
  @media (max-width: 1250px) { .params > .cap { display: none; } }
</style>
