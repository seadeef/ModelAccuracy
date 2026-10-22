<script>
  /**
   * Forecast revision by lead for one storm day. Two views of the same
   * series: "Revisions" — bars = the selected model's run-to-run change
   * (F_L − F_(L+1)), lines = the other models; "Forecast path" — every
   * model's raw forecast for the day at each lead against the PRISM
   * observation. The side ranks models by total revision on the way in
   * (steadiest first) and by final-run error (closest first).
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, selectedLead, palette, modelLabel } from './dashData.svelte.js';
  import { revisionPath, datesInRange, wettestDay } from '../basinAnalysis.js';
  import { ymdToIso, isoToYmd } from '../bootstrap.js';
  import { fmtRange } from './basinScope.svelte.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import Dropdown from './Dropdown.svelte';
  import BasinNotice from './BasinNotice.svelte';
  import { leadAxisOptions, cursorPlugin, hlinePlugin, fmt, fmtDate, alpha } from './chartSetup.js';

  const pal = $derived(palette());
  const series = $derived(dash.basinSeries);
  const rev = $derived(series?.[ui.model] ?? null);
  const targetYmd = $derived(isoToYmd(ui.arTargetDate));
  const range = $derived([isoToYmd(ui.arRangeStart), isoToYmd(ui.arRangeEnd)]);
  const pathView = $derived(ui.revisionView === 'path');

  /** Lead axes, longest lead first (the storm approaches left → right). */
  const leadMax = $derived(Math.max(...appConfig.models.map((m) => m.lead_days_max ?? 14)));
  const pairLeads = $derived(Array.from({ length: leadMax - 1 }, (_, i) => leadMax - 1 - i)); // 13..1
  const runLeads = $derived(Array.from({ length: leadMax }, (_, i) => leadMax - i)); // 14..1

  const paths = $derived.by(() => {
    if (!series) return {};
    const out = {};
    for (const m of appConfig.models) {
      const r = series[m.key];
      const d = r?.dates?.indexOf(targetYmd) ?? -1;
      out[m.key] = r && d >= 0 ? revisionPath(r, d) : null;
    }
    return out;
  });
  const observed = $derived.by(() => {
    for (const m of appConfig.models) {
      const o = paths[m.key]?.observed;
      if (o != null) return o;
    }
    return null;
  });

  function revisionsFor(key) {
    const p = paths[key];
    const r = series?.[key];
    if (!p || !r) return pairLeads.map(() => null);
    return pairLeads.map((L) => {
      const i = L - r.lead_min;
      return i >= 0 && i < p.revisions.length ? p.revisions[i] : null;
    });
  }
  function forecastsFor(key) {
    const p = paths[key];
    const r = series?.[key];
    if (!p?.forecasts || !r) return runLeads.map(() => null);
    return runLeads.map((L) => {
      const i = L - r.lead_min;
      return i >= 0 && i < p.forecasts.length ? p.forecasts[i] : null;
    });
  }

  const cursorPairs = cursorPlugin(() => pairLeads.indexOf(selectedLead()));
  const cursorRuns = cursorPlugin(() => runLeads.indexOf(selectedLead()));
  const zero = hlinePlugin(() => 0, '#ffffff40');
  const obsLine = hlinePlugin(() => observed, '#7fd6a8', [5, 3]);

  const config = $derived.by(() => {
    const sel = ui.model;
    if (pathView) {
      const datasets = appConfig.models.map((m) => ({
        type: 'line', label: m.label, data: forecastsFor(m.key),
        borderColor: m.key === sel ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.73),
        borderWidth: m.key === sel ? 2.6 : 1.3, pointRadius: m.key === sel ? 2.5 : 1.5, tension: 0, spanGaps: false,
        order: m.key === sel ? 0 : 1,
      }));
      const vals = [...datasets.flatMap((d) => d.data), observed].filter((v) => v != null);
      const top = Math.max(5, ...vals) * 1.12;
      const opts = leadAxisOptions({
        yTitle: 'mm', yMin: 0, yMax: Math.ceil(top / 5) * 5, xTitle: 'Lead day of the run',
        tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 1)} mm`,
      });
      opts.plugins.tooltip.callbacks.title = (items) => `Run at lead ${items[0]?.label}${observed != null ? ` · observed ${fmt(observed, 1)} mm` : ''}`;
      return { type: 'line', data: { labels: runLeads, datasets }, options: opts, plugins: [obsLine, cursorRuns] };
    }
    const datasets = [
      {
        type: 'bar', label: `${modelLabel(sel)} revision`, data: revisionsFor(sel),
        backgroundColor: alpha(pal[sel]?.text, 0.8), borderRadius: 2, barPercentage: 0.6, order: 2,
      },
      ...appConfig.models.filter((m) => m.key !== sel).map((m) => ({
        type: 'line', label: `${m.label} revision`, data: revisionsFor(m.key),
        borderColor: alpha(pal[m.key]?.text, 0.73), borderWidth: 1.2, pointRadius: 1.5, tension: 0, order: 1, spanGaps: false,
      })),
    ];
    const vals = datasets.flatMap((d) => d.data).filter((v) => v != null);
    const amp = Math.max(10, ...vals.map(Math.abs));
    const lim = Math.ceil((amp * 1.12) / 10) * 10;
    const opts = leadAxisOptions({
      yTitle: 'mm', yMin: -lim, yMax: lim, xTitle: 'Lead day of the run',
      tooltipLabel: (c) => ` ${c.dataset.label}: ${c.parsed.y > 0 ? '+' : ''}${fmt(c.parsed.y, 1)} mm`,
    });
    opts.plugins.tooltip.callbacks.title = (items) => `Run at lead ${items[0]?.label} vs the run before it`;
    return { type: 'bar', data: { labels: pairLeads, datasets }, options: opts, plugins: [zero, cursorPairs] };
  });

  /** Ranking rows: total revision (steadiest first) and final-run error (closest first). */
  const rows = $derived.by(() => appConfig.models.map((m) => {
    const p = paths[m.key];
    const fc = p?.forecasts;
    const first = fc ? fc[fc.length - 1] : null;
    const final = fc ? fc[0] : null;
    const err = final != null && p.observed != null ? Math.abs(final - p.observed) : null;
    return { key: m.key, label: m.label, total: p?.total ?? null, first, final, err, leadMax: series?.[m.key]?.lead_max };
  }));
  const totals = $derived.by(() => {
    const r = rows.filter((x) => x.total != null).sort((a, b) => a.total - b.total);
    const most = Math.max(1, ...r.map((x) => x.total));
    return r.map((x) => ({ ...x, pct: (100 * x.total) / most }));
  });
  const errors = $derived.by(() => {
    const r = rows.filter((x) => x.err != null).sort((a, b) => a.err - b.err);
    const most = Math.max(1, ...r.map((x) => x.err));
    return r.map((x) => ({ ...x, pct: (100 * x.err) / most }));
  });
  const rowTitle = (r) => `${r.label}: first run (lead ${r.leadMax ?? '—'}) ${fmt(r.first, 1)} mm → final run (lead 1) ${fmt(r.final, 1)} mm${observed != null ? ` · observed ${fmt(observed, 1)} mm` : ''}`;

  /** Day picker: dates inside the range, with the basin observation. */
  const dayItems = $derived.by(() => {
    if (!rev) return [];
    return datesInRange(rev, range).map((d) => ({
      key: rev.dates[d],
      label: fmtDate(rev.dates[d]),
      sub: rev.observed[d] != null ? `${fmt(rev.observed[d], 1)} mm observed` : 'no observation',
      color: rev.observed[d] != null && rev.observed[d] >= ui.eventThresholdMm ? '#7fd6a8' : undefined,
    }));
  });

  /** Range picker: analysed storms touching this basin, then the basin's own AR episodes, then the full record. */
  const rangeItems = $derived.by(() => {
    const items = [];
    const huc = dash.basinHuc;
    const storms = (dash.storms?.events || []).filter((e) => e.in_analysis && e.basin_hucs?.includes(huc));
    for (const e of [...storms].reverse()) {
      items.push({
        key: `storm:${e.event_id}`, group: items.length === 0 ? 'Storms (region-wide AR events)' : undefined,
        label: `${fmtDate(e.start_date)} – ${fmtDate(e.end_date)}`,
        sub: `${e.span_days} d · ${e.basins} basins${e.volume_km3 != null ? ` · ${fmt(e.volume_km3, 1)} km³` : ''}${e.volume_rank ? ` · #${e.volume_rank} by volume` : ''}`,
        start: e.start_date, end: e.end_date, peak: e.peak?.basin_huc === huc ? e.peak?.basin_date : null,
      });
    }
    const eps = dash.episodes?.episodes || [];
    eps.slice().reverse().forEach((ep, i) => items.push({
      key: `ep:${ep.id}`, group: i === 0 ? 'AR episodes in this basin' : undefined,
      label: `${fmtDate(ep.start_date)} – ${fmtDate(ep.end_date)}`, sub: `${ep.active_days} active d`,
      start: ep.start_date, end: ep.end_date, episodeId: ep.id,
    }));
    if (rev) {
      items.push({ key: 'all', group: 'Record', label: 'Full record', sub: `${rev.dates.length} dates`, start: ymdToIso(rev.dates[0]), end: ymdToIso(rev.dates[rev.dates.length - 1]) });
    }
    return items;
  });
  const rangeValue = $derived.by(() => {
    const hit = rangeItems.find((i) => i.start === ui.arRangeStart && i.end === ui.arRangeEnd);
    return hit?.key ?? 'custom';
  });
  const rangeItemsShown = $derived(rangeValue === 'custom' && ui.arRangeStart
    ? [{ key: 'custom', label: fmtRange(), sub: 'from the timeline' }, ...rangeItems] : rangeItems);

  function pickRange(key) {
    const it = rangeItems.find((i) => i.key === key);
    if (!it) return;
    ui.arRangeStart = it.start;
    ui.arRangeEnd = it.end;
    ui.arEpisodeId = it.episodeId ?? null;
    if (rev) {
      const r = [isoToYmd(it.start), isoToYmd(it.end)];
      const positions = datesInRange(rev, r);
      const peak = it.peak && rev.dates.indexOf(isoToYmd(it.peak)) >= 0 ? rev.dates.indexOf(isoToYmd(it.peak)) : wettestDay(rev, positions);
      if (peak != null) ui.arTargetDate = ymdToIso(rev.dates[peak]);
    }
  }
  /** Hovering an episode option highlights its marker on the AR timeline. */
  function hoverRange(key) {
    ui.arHoverEpisodeId = key ? rangeItems.find((i) => i.key === key)?.episodeId ?? null : null;
  }
</script>

{#if !series}
  <div class="fallback">
    <BasinNotice />
    {#if dash.revisionGrid && Object.keys(dash.revisionGrid).length}
      <div class="grid-r">
        <div class="rev-title">Revision efficiency R, grid-pooled over this region <small>Σ mean squared run-to-run revision ÷ skill gain from the longest to the shortest lead · R = 1 is the martingale reference · lowest first</small></div>
        {#each appConfig.models.map((m) => ({ key: m.key, label: m.label, ...(dash.revisionGrid[m.key] || {}) })).filter((r) => r.value != null).sort((a, b) => a.value - b.value) as r (r.key)}
          <div class="rev-row" class:on={r.key === ui.model} style:--model={pal[r.key]?.text}>
            <b style:color={pal[r.key]?.text}>{pal[r.key]?.ident ?? r.label}</b>
            <i style:width="{Math.min(100, (100 * r.value) / Math.max(1, ...Object.values(dash.revisionGrid).map((x) => x.value || 0))).toFixed(1)}%"></i>
            <span>{fmt(r.value, 2)} <small>· {r.dates} dates</small></span>
          </div>
        {/each}
      </div>
    {/if}
  </div>
{:else}
  <div class="head">
    <div class="title">
      <span class="seg">
        <button type="button" class:active={!pathView} title="Run-to-run change in the forecast for the day: bars = selected model, lines = the others" onclick={() => { ui.revisionView = 'revisions'; }}>Revisions</button>
        <button type="button" class:active={pathView} title="Raw forecast for the day at every lead, against the PRISM observation" onclick={() => { ui.revisionView = 'path'; }}>Forecast path</button>
      </span>
      <span>— <b>{fmtDate(ui.arTargetDate)}</b> — <b style:color={pal[ui.model]?.text}>{modelLabel(ui.model)}</b></span>
    </div>
    <div class="pickers">
      <Dropdown items={dayItems} value={targetYmd} ariaLabel="Storm day" title="Choose the day to inspect (green = observed at or above the heavy-rain threshold)" compact width="112px"
        onselect={(k) => { ui.arTargetDate = ymdToIso(k); }} />
      <Dropdown items={rangeItemsShown} value={rangeValue} ariaLabel="Storm or date range" title="Storm, AR episode or full record: sets the days the picker offers and the range used by the analyses" compact width="230px" align="right"
        onselect={pickRange} onhover={hoverRange} />
    </div>
  </div>
  <div class="main">
    <div class="revision">
      <div class="chart-note">
        {#if pathView}
          Basin-mean forecast for <b>{fmtDate(ui.arTargetDate, { month: 'short', day: 'numeric', year: 'numeric' })}</b> by the run at each lead · dashed = PRISM observation{observed != null ? ` ${fmt(observed, 1)} mm` : ''}
        {:else}
          Change in the forecast for <b>{fmtDate(ui.arTargetDate, { month: 'short', day: 'numeric', year: 'numeric' })}</b> from the previous run · above zero = raised, below = lowered{observed != null ? ` · observed ${fmt(observed, 1)} mm` : ''}
        {/if}
      </div>
      <div class="plot">
        {#if rev && paths[ui.model]}
          <ChartCanvas {config} />
        {:else}
          <div class="empty">No forecast series for {modelLabel(ui.model)} on this day — pick another day</div>
        {/if}
      </div>
      <div class="model-key">
        {#each appConfig.models as m}<span class:absent={rev && !paths[m.key]} title={rev && !paths[m.key] ? `${m.label}: no complete run sequence for this day` : ''}><i style:background={pal[m.key]?.text}></i>{m.label}</span>{/each}
        {#if pathView && observed != null}<span><i class="obs"></i>Observed</span>{/if}
      </div>
    </div>
    <aside class="rev-summary">
      <div class="block">
        <div class="rev-title">Total revision, lead {leadMax} → 1 <small>Σ |change| across adjacent runs · steadiest first</small></div>
        <div class="rev-rows">
          {#each totals as t (t.key)}
            <div class="rev-row" class:on={t.key === ui.model} style:--model={pal[t.key]?.text} title={rowTitle(t)}>
              <b style:color={pal[t.key]?.text}>{pal[t.key]?.ident ?? t.label}</b>
              <i style:width="{t.pct.toFixed(1)}%"></i>
              <span>{fmt(t.total, 0)} mm</span>
            </div>
          {/each}
          {#if !totals.length}<div class="empty-rows">No complete revision path on this day</div>{/if}
        </div>
      </div>
      <div class="block">
        <div class="rev-title">Final-run error <small>|lead-1 forecast − observed| · closest first</small></div>
        <div class="rev-rows">
          {#each errors as e (e.key)}
            <div class="rev-row" class:on={e.key === ui.model} style:--model={pal[e.key]?.text} title={rowTitle(e)}>
              <b style:color={pal[e.key]?.text}>{pal[e.key]?.ident ?? e.label}</b>
              <i style:width="{e.pct.toFixed(1)}%"></i>
              <span>{fmt(e.err, 1)} mm</span>
            </div>
          {/each}
          {#if !errors.length}<div class="empty-rows">No observation for this day</div>{/if}
        </div>
      </div>
    </aside>
  </div>
{/if}

<style>
  .fallback { flex: 1; display: flex; flex-direction: column; min-height: 0; }
  .grid-r { flex: none; border-top: 1px solid var(--panel-border); padding-top: 8px; display: flex; flex-direction: column; gap: 6px; max-width: 520px; margin: 0 auto 4px; width: 100%; }
  .head { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 2px; flex: none; }
  .title { display: flex; align-items: center; gap: 8px; font-size: 15px; font-weight: 600; color: #d1c0ff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; }
  .title > span:last-child { color: var(--text-secondary); font-size: 13px; font-weight: 400; }
  .title b { font-weight: 600; color: var(--text-primary); }
  .seg { display: inline-flex; border: 1px solid var(--panel-border); border-radius: 6px; overflow: hidden; flex: none; }
  .seg button { font: inherit; font-size: 11px; font-weight: 600; padding: 3px 9px; background: none; border: 0; color: var(--text-secondary); cursor: pointer; }
  .seg button.active { background: #9d7bff33; color: #e8e0ff; }
  .pickers { display: flex; gap: 8px; flex: none; }
  .main { display: flex; flex: 1; min-height: 0; gap: 14px; }
  .revision { flex: 1; min-width: 0; display: flex; flex-direction: column; min-height: 0; }
  .chart-note { font-size: 11.5px; color: var(--text-secondary); margin: 1px 0 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .chart-note b { color: var(--text-primary); font-weight: 500; }
  .plot { position: relative; flex: 1; min-height: 70px; }
  .empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--text-secondary); font-size: 12px; }
  .model-key { display: flex; gap: 10px; align-items: center; color: #bec6d2; font-size: 11px; }
  .model-key span { display: flex; align-items: center; gap: 5px; }
  .model-key span.absent { opacity: 0.38; text-decoration: line-through; }
  .model-key i { width: 14px; height: 3px; border-radius: 2px; }
  .model-key i.obs { background: repeating-linear-gradient(90deg, #7fd6a8 0 4px, transparent 4px 7px); }
  .rev-summary { width: 250px; flex: none; border-left: 1px solid var(--panel-border); padding-left: 13px; display: flex; flex-direction: column; justify-content: center; gap: 14px; min-height: 0; overflow: auto; }
  .block { display: flex; flex-direction: column; gap: 7px; }
  .rev-title { font-size: 12.5px; font-weight: 600; color: #d5dae2; }
  .rev-title small { display: block; font-weight: 400; color: var(--text-secondary); font-size: 10px; margin-top: 2px; line-height: 1.3; }
  .rev-rows { display: flex; flex-direction: column; gap: 6px; }
  .rev-row { display: grid; grid-template-columns: 42px 1fr 60px; align-items: center; gap: 7px; font-size: 12px; color: #bec6d2; }
  .grid-r .rev-row { grid-template-columns: 42px 1fr 110px; }
  .rev-row b { font-weight: 600; }
  .rev-row i { display: block; height: 7px; border-radius: 4px; background: var(--model); opacity: .55; }
  .rev-row.on i { opacity: 1; height: 9px; }
  .rev-row.on { color: #eef2f7; }
  .rev-row span:last-child { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
  .rev-row small { color: var(--text-secondary); font-size: 10px; }
  .empty-rows { font-size: 11.5px; color: var(--text-secondary); }
  @media (max-width: 1100px) { .rev-summary { width: 200px; } }
</style>
