<script>
  /**
   * Forecast revision by lead for one storm day. "Revisions": bars = the
   * selected model's cumulative |run-to-run change| from the longest lead
   * in, lines = every model's signed change per run (F_L − F_(L+1)).
   * "Forecast path": every model's raw forecast for the day at each lead
   * against the PRISM observation.
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, selectedLead, palette, modelLabel } from './dashData.svelte.js';
  import { revisionPath } from '../basinAnalysis.js';
  import { isoToYmd } from '../bootstrap.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import BasinNotice from './BasinNotice.svelte';
  import { leadAxisOptions, cursorPlugin, hlinePlugin, endLabelPlugin, fmt, fmtDate, alpha } from './chartSetup.js';

  const pal = $derived(palette());
  const series = $derived(dash.basinSeries);
  const rev = $derived(series?.[ui.model] ?? null);
  const targetYmd = $derived(isoToYmd(ui.arTargetDate));
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

  function byPair(key, field) {
    const p = paths[key];
    const r = series?.[key];
    if (!p || !r) return pairLeads.map(() => null);
    return pairLeads.map((L) => {
      const i = L - r.lead_min;
      return i >= 0 && i < p[field].length ? p[field][i] : null;
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
  const ends = endLabelPlugin((ds) => ds.ident);

  const config = $derived.by(() => {
    const sel = ui.model;
    if (pathView) {
      const datasets = appConfig.models.map((m) => ({
        type: 'line', label: m.label, ident: pal[m.key]?.ident, data: forecastsFor(m.key),
        borderColor: m.key === sel ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.75), endLabelColor: pal[m.key]?.text,
        borderWidth: m.key === sel ? 2.8 : 1.4, pointRadius: m.key === sel ? 2.5 : 1.5, tension: 0, spanGaps: false,
        order: m.key === sel ? 0 : 1,
      }));
      const vals = [...datasets.flatMap((d) => d.data), observed].filter((v) => v != null);
      const top = Math.max(5, ...vals) * 1.12;
      const opts = leadAxisOptions({
        yTitle: 'mm', yMin: 0, yMax: Math.ceil(top / 5) * 5, xTitle: 'Lead day of the run', endLabels: true,
        tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 1)} mm`,
      });
      opts.plugins.tooltip.callbacks.title = (items) => `Run at lead ${items[0]?.label}${observed != null ? ` · observed ${fmt(observed, 1)} mm` : ''}`;
      return { type: 'line', data: { labels: runLeads, datasets }, options: opts, plugins: [obsLine, cursorRuns, ends] };
    }
    const datasets = [
      {
        type: 'bar', label: `${modelLabel(sel)} cumulative |revision|`, data: byPair(sel, 'cumulative'),
        backgroundColor: alpha(pal[sel]?.text, 0.22), borderColor: alpha(pal[sel]?.text, 0.45), borderWidth: 1, borderRadius: 2, barPercentage: 0.62, order: 3,
      },
      ...appConfig.models.map((m) => ({
        type: 'line', label: `${m.label} revision`, ident: pal[m.key]?.ident, data: byPair(m.key, 'revisions'),
        borderColor: m.key === sel ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.75), endLabelColor: pal[m.key]?.text,
        borderWidth: m.key === sel ? 2.6 : 1.3, pointRadius: m.key === sel ? 2.4 : 1.5, tension: 0, order: m.key === sel ? 0 : 1, spanGaps: false,
      })),
    ];
    const signed = datasets.slice(1).flatMap((d) => d.data).filter((v) => v != null);
    const neg = Math.min(0, ...signed);
    const pos = Math.max(0, ...signed, paths[sel]?.total ?? 0);
    const step = pos + Math.abs(neg) > 60 ? 20 : 10;
    const opts = leadAxisOptions({
      yTitle: 'mm', yMin: -Math.ceil((Math.abs(neg) * 1.15 + 1) / step) * step, yMax: Math.ceil((pos * 1.12 + 1) / step) * step, xTitle: 'Lead day of the run', endLabels: true,
      tooltipLabel: (c) => (c.dataset.type === 'bar' ? ` ${c.dataset.label}: ${fmt(c.parsed.y, 1)} mm` : ` ${c.dataset.label}: ${c.parsed.y > 0 ? '+' : ''}${fmt(c.parsed.y, 1)} mm`),
    });
    opts.plugins.tooltip.callbacks.title = (items) => `Run at lead ${items[0]?.label} vs the run before it`;
    return { type: 'bar', data: { labels: pairLeads, datasets }, options: opts, plugins: [zero, cursorPairs, ends] };
  });

  /** Ranking rows: total revision (steadiest first) or final-run error (closest first). */
  const rows = $derived.by(() => appConfig.models.map((m) => {
    const p = paths[m.key];
    const fc = p?.forecasts;
    const first = fc ? fc[fc.length - 1] : null;
    const final = fc ? fc[0] : null;
    const err = final != null && p.observed != null ? Math.abs(final - p.observed) : null;
    return { key: m.key, label: m.label, total: p?.total ?? null, first, final, err, leadMax: series?.[m.key]?.lead_max };
  }));
  const ranked = $derived.by(() => {
    const field = pathView ? 'err' : 'total';
    const r = rows.filter((x) => x[field] != null).sort((a, b) => a[field] - b[field]);
    const most = Math.max(1, ...r.map((x) => x[field]));
    return r.map((x) => ({ ...x, value: x[field], pct: (100 * x[field]) / most }));
  });
  const rowTitle = (r) => `${r.label}: first run (lead ${r.leadMax ?? '—'}) ${fmt(r.first, 1)} mm → final run (lead 1) ${fmt(r.final, 1)} mm${observed != null ? ` · observed ${fmt(observed, 1)} mm` : ''}`;
  const absent = $derived(appConfig.models.filter((m) => series?.[m.key] && !paths[m.key]).map((m) => m.label));
</script>

<BasinNotice>
  <div class="head">
    <div class="eyebrow">
      {pathView ? 'Forecast path for' : 'Revisions of the forecast for'} <em>{fmtDate(ui.arTargetDate, { month: 'short', day: 'numeric', year: 'numeric' })}</em>{observed != null ? ` · observed ${fmt(observed, 1)} mm` : ''}
    </div>
    <span class="seg">
      <button type="button" class:active={!pathView} title="Bars: the selected model's cumulative |change| from the longest lead in. Lines: each model's change per run, above zero = raised, below = lowered" onclick={() => { ui.revisionView = 'revisions'; }}>Revisions</button>
      <button type="button" class:active={pathView} title="Raw basin-mean forecast for the day at every lead, against the PRISM observation" onclick={() => { ui.revisionView = 'path'; }}>Forecast path</button>
    </span>
  </div>
  <div class="main">
    <div class="chart">
      <div class="plot">
        {#if rev && paths[ui.model]}
          <ChartCanvas {config} />
        {:else}
          <div class="plot-empty">No complete run sequence for {modelLabel(ui.model)} on this day — pick another day or model</div>
        {/if}
      </div>
      <div class="legend">
        {#if pathView}
          <span><i class="obs"></i>PRISM observed</span>
        {:else}
          <span><i class="bar" style:--c={pal[ui.model]?.text}></i>{modelLabel(ui.model)} cumulative |revision|</span>
          <span><i class="line"></i>Change per run, every model</span>
        {/if}
        {#if absent.length}<span class="muted">no complete sequence: {absent.join(', ')}</span>{/if}
      </div>
    </div>
    <aside class="side">
      <div class="eyebrow">{pathView ? 'Final-run error' : `Total revision, lead ${leadMax} → 1`}</div>
      <div class="cap">{pathView ? 'lead-1 forecast against observed · closest first' : 'sum of |change| across runs · steadiest first'}</div>
      <div class="rows">
        {#each ranked as r (r.key)}
          <div class="row" class:on={r.key === ui.model} style:--model={pal[r.key]?.text} title={rowTitle(r)}>
            <b style:color={pal[r.key]?.text}>{pal[r.key]?.ident ?? r.label}</b>
            <i style:width="{r.pct.toFixed(1)}%"></i>
            <span>{fmt(r.value, pathView ? 1 : 0)} mm</span>
          </div>
        {/each}
        {#if !ranked.length}<div class="cap">{pathView ? 'No observation for this day' : 'No complete revision path on this day'}</div>{/if}
      </div>
    </aside>
  </div>
</BasinNotice>

<style>
  .head { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 6px; flex: none; min-width: 0; }
  .main { display: flex; flex: 1; min-height: 0; gap: 18px; }
  .chart { flex: 1; min-width: 0; display: flex; flex-direction: column; min-height: 0; }
  .legend { display: flex; gap: 16px; align-items: center; color: #bec6d2; font-size: 12px; margin-top: 4px; flex-wrap: wrap; }
  .legend span { display: inline-flex; align-items: center; gap: 6px; }
  .legend i { display: inline-block; width: 14px; height: 3px; border-radius: 2px; }
  .legend i.bar { height: 10px; background: color-mix(in srgb, var(--c) 28%, transparent); border: 1px solid color-mix(in srgb, var(--c) 50%, transparent); }
  .legend i.line { background: #bec6d2; }
  .legend i.obs { background: repeating-linear-gradient(90deg, #7fd6a8 0 4px, transparent 4px 7px); }
  .legend .muted { color: var(--text-secondary); }
  .side { width: 270px; flex: none; border-left: 1px solid var(--rule); padding-left: 16px; display: flex; flex-direction: column; justify-content: center; gap: 4px; min-height: 0; overflow: auto; }
  .side > .cap { white-space: normal; line-height: 1.35; }
  .rows { display: flex; flex-direction: column; gap: 9px; margin-top: 10px; }
  .row { display: grid; grid-template-columns: 46px 1fr 62px; align-items: center; gap: 8px; font-size: 13px; color: #bec6d2; }
  .row b { font-weight: 600; }
  .row i { display: block; height: 8px; border-radius: 4px; background: var(--model); opacity: .5; }
  .row.on i { opacity: 1; height: 10px; }
  .row.on { color: #eef2f7; }
  .row span { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
  @media (max-width: 1100px) { .side { width: 210px; } }
</style>
