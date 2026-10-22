<script>
  /**
   * Run-to-run consistency: jumpiness J (1 − FSS between successive runs) or
   * convergence RMSE vs lead for every model at the chosen threshold/scale,
   * with every model's value at the inspected lead ranked steadiest first.
   */
  import { ui, appConfig, isArPeriod } from '../state.svelte.js';
  import { statisticTooltip } from '../helpText.js';
  import { dash, selectedLead, leadAxis, palette, statSeries, modelLabel, pickLead } from './dashData.svelte.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, onClickIndex, fmt } from './chartSetup.js';

  const axis = $derived(leadAxis());
  const pal = $derived(palette());
  const lead = $derived(selectedLead());
  const stabKey = $derived(appConfig.consistency.curve_roles?.stability ?? 'jumpiness');
  const accKey = $derived(appConfig.consistency.curve_roles?.accuracy ?? 'convergence');
  const curve = $derived(ui.consistencyCurve === 'convergence' ? accKey : stabKey);
  const isJ = $derived(curve === stabKey);
  const cursor = cursorPlugin(() => selectedLead() - 1);
  const supported = $derived(ui.period === 'yearly' || ui.period === 'seasonal' || isArPeriod(ui.period));

  const config = $derived.by(() => {
    const datasets = appConfig.models.map((m) => ({
      label: m.label,
      data: statSeries(dash.consistency?.[m.key], curve, axis),
      borderColor: pal[m.key]?.text,
      borderWidth: m.key === ui.model ? 2.6 : 1.4,
      spanGaps: false,
    }));
    const opts = leadAxisOptions({
      yTitle: isJ ? 'J = 1 − FSS' : 'mm/day',
      yMin: 0,
      ...(isJ ? { yMax: 1 } : {}),
      tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, isJ ? 3 : 2)}`,
    });
    opts.plugins.tooltip.callbacks.title = (items) => (isJ
      ? `Runs at lead ${Number(items[0]?.label) + 1} → ${items[0]?.label}` : `Lead day ${items[0]?.label}`);
    opts.onClick = onClickIndex((i) => pickLead(axis[i]));
    return { type: 'line', data: { labels: axis, datasets }, options: opts, plugins: [cursor] };
  });

  /** Every model at the inspected lead on the drawn curve, best (lowest) first. */
  const ranked = $derived.by(() => appConfig.models.map((m) => {
    const s = dash.consistency?.[m.key]?.find((r) => r.lead === lead)?.stats?.[curve];
    return { key: m.key, label: m.label, value: s && !s.no_data ? s.value : null, dates: s?.dates ?? null };
  }).filter((r) => r.value != null).sort((a, b) => a.value - b.value));
  const sel = $derived(ranked.find((r) => r.key === ui.model) ?? null);
</script>

<article class="pane">
  <div class="pane-title">
    <span class="title-main">
      Forecast consistency
      <span class="seg">
        <button type="button" class:active={ui.consistencyCurve !== 'convergence'} title={statisticTooltip(stabKey)} onclick={() => { ui.consistencyCurve = 'jumpiness'; }}>J</button>
        <button type="button" class:active={ui.consistencyCurve === 'convergence'} title={statisticTooltip(accKey)} onclick={() => { ui.consistencyCurve = 'convergence'; }}>RMSE</button>
      </span>
    </span>
    <span title="Click the chart to inspect a lead day">{isJ ? 'J · lower is steadier' : 'Convergence to PRISM · lower is closer'}</span>
  </div>
  <div class="plot">
    {#if dash.consistency}
      <ChartCanvas {config} />
    {:else}
      <div class="empty">{!supported ? 'Consistency is computed for the full record and seasons' : dash.consistencyLoading ? 'Loading consistency curves…' : 'No consistency data for this slice'}</div>
    {/if}
  </div>
  <div class="params">
    <span class="cap" title="Exceedance threshold q: rain heavier than this counts as an event (mm/day).">Threshold mm/d</span>
    <div class="chips">
      {#each appConfig.consistency.thresholds_mm ?? [] as q}
        <button type="button" class="chip" class:active={ui.consistencyThreshold === q} onclick={() => { ui.consistencyThreshold = q; }}>{q}</button>
      {/each}
    </div>
    <span class="cap" title="Neighborhood scale n: width of the square window (km) over which event fractions are compared.">Scale km</span>
    <div class="chips">
      {#each appConfig.consistency.scales_km ?? [] as n}
        <button type="button" class="chip" class:active={ui.consistencyScale === n} onclick={() => { ui.consistencyScale = n; }}>{n}</button>
      {/each}
    </div>
  </div>
  <div class="foot">
    <span class="cons-note" title={sel?.dates ? `${sel.dates} forecast dates pooled` : ''}>
      {#if sel}{modelLabel(ui.model)} at lead day {lead}: <b>{isJ ? `J ${fmt(sel.value, 2)}` : `${fmt(sel.value, 1)} mm/d`}</b>{:else}{modelLabel(ui.model)} at lead day {lead}: no value{/if}
    </span>
    <span class="rank" title={isJ ? `Steadiest at lead day ${lead}, lowest J first` : `Closest to PRISM at lead day ${lead}, lowest RMSE first`}>
      {#each ranked as r, i (r.key)}
        <span class:first={i === 0}><b style:color={pal[r.key]?.text}>{pal[r.key]?.ident}</b> {fmt(r.value, isJ ? 2 : 1)}</span>
      {/each}
    </span>
  </div>
</article>

<style>
  .pane { display: flex; flex-direction: column; min-width: 0; min-height: 0; padding: 9px 11px 7px; background: var(--pane-bg); border: 1px solid var(--panel-border); border-radius: 10px; }
  .pane-title { display: flex; justify-content: space-between; align-items: center; gap: 10px; font-size: 15px; font-weight: 600; margin-bottom: 2px; }
  .pane-title > span:last-child { font-size: 12px; color: var(--text-secondary); font-weight: 400; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .title-main { display: flex; align-items: center; gap: 8px; white-space: nowrap; }
  .seg { display: inline-flex; border: 1px solid var(--panel-border); border-radius: 6px; overflow: hidden; }
  .seg button { font: inherit; font-size: 10.5px; font-weight: 600; padding: 2px 7px; background: none; border: 0; color: var(--text-secondary); cursor: pointer; }
  .seg button.active { background: var(--accent-glow); color: var(--accent); }
  .plot { position: relative; flex: 1; min-height: 60px; cursor: pointer; }
  .empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--text-secondary); font-size: 12px; text-align: center; padding: 0 20px; }
  .params { display: flex; align-items: center; gap: 6px; margin-top: 5px; min-width: 0; }
  .cap { flex: none; font-size: 10.5px; color: var(--text-secondary); white-space: nowrap; }
  .cap + .chips { margin-right: 6px; }
  .chips { display: flex; gap: 3px; flex: 1; min-width: 0; }
  .chip { flex: 1; min-width: 0; font: inherit; font-size: 11px; font-weight: 600; padding: 1px 4px; background: #ffffff06; border: 1px solid var(--panel-border); border-radius: 5px; color: var(--text-secondary); cursor: pointer; }
  .chip:hover { background: var(--hover-bg); color: var(--text-primary); }
  .chip.active { background: #8b93ff33; border-color: #8b93ff; color: #e3e5ff; }
  .foot { display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-top: 5px; font-size: 11px; color: var(--text-secondary); min-width: 0; }
  .foot b { color: var(--text-primary); font-weight: 600; }
  .cons-note { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; }
  .rank { display: flex; gap: 8px; flex: none; white-space: nowrap; font-variant-numeric: tabular-nums; }
  .rank span.first { color: var(--text-primary); }
  .rank b { font-weight: 600; }
  @media (max-width: 1100px) { .rank { display: none; } }
</style>
