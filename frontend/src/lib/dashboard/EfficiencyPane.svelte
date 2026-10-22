<script>
  /**
   * Revision efficiency R on the chosen date scope:
   * the five-model R forest with cluster-bootstrap intervals, the martingale
   * waterfall for the selected model, and paired ΔR contrasts against it
   * (common random numbers, p_direction).
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, modelLabel } from './dashData.svelte.js';
  import { revisionEfficiency, waterfall, pairedDeltaR } from '../basinAnalysis.js';
  import { scopeFor, scopeOptions, scopeLabel } from './basinScope.svelte.js';
  import BasinNotice from './BasinNotice.svelte';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, hlinePlugin, fmt, fmtCi, alpha } from './chartSetup.js';

  const pal = $derived(palette());
  const series = $derived(dash.basinSeries);

  const forest = $derived.by(() => {
    if (!series) return [];
    return appConfig.models.map((m) => {
      const rev = series[m.key];
      const sc = scopeFor(rev);
      if (!sc) return null;
      const r = revisionEfficiency(rev, sc.idx, sc.info);
      return { key: m.key, label: m.label, ...r, arClusters: sc.info.arClusterCount, window: rev.primary_window };
    }).filter(Boolean).sort((a, b) => (a.value ?? Infinity) - (b.value ?? Infinity));
  });
  const forestMax = $derived(Math.max(1.5, ...forest.map((f) => f.ci?.[1] ?? f.value ?? 0)));

  const wf = $derived.by(() => {
    const rev = series?.[ui.model];
    const sc = scopeFor(rev);
    return sc && sc.idx.length ? waterfall(rev, sc.idx) : null;
  });
  const skillLine = hlinePlugin(() => wf?.skillGain ?? null, '#7fd6a8', [5, 3]);
  const wfConfig = $derived.by(() => {
    if (!wf) return null;
    const cum = [];
    let run = 0;
    for (const v of wf.meanSquaredRevisionByPair) { run += v; cum.push(run); }
    const opts = leadAxisOptions({
      yTitle: 'mm²', yMin: 0, xTitle: 'Run pair (lead L → L−1)',
      tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 1)} mm²`,
    });
    opts.plugins.tooltip.callbacks.title = (items) => `Pair ${items[0]?.label}`;
    return {
      type: 'bar',
      data: {
        labels: wf.pairLeads,
        datasets: [
          { type: 'line', label: 'Cumulative revision', data: cum, borderColor: '#9d7bff', borderWidth: 1.6, pointRadius: 2, order: 1 },
          { type: 'bar', label: 'Mean squared revision', data: wf.meanSquaredRevisionByPair, backgroundColor: alpha(pal[ui.model]?.text, 0.67), borderRadius: 2, order: 2 },
        ],
      },
      options: opts,
      plugins: [skillLine],
    };
  });

  const deltas = $derived.by(() => {
    const a = series?.[ui.model];
    if (!a) return [];
    const eps = dash.episodes?.episodes ?? [];
    return appConfig.models.filter((m) => m.key !== ui.model && series[m.key]).map((m) => ({
      key: m.key, label: m.label, ...pairedDeltaR(a, series[m.key], eps, scopeOptions()),
    }));
  });
  const fmtP = (p) => (p == null ? '' : p === 0 || p === 1 ? (p === 1 ? 'p(Δ>0) > 0.9995' : 'p(Δ>0) < 0.0005') : `p(Δ>0) = ${fmt(p, 3)}`);
</script>

<BasinNotice>
  <div class="grid">
    <section class="col forest">
      <div class="h">Revision efficiency R <small>{scopeLabel()} · Σ revision² ÷ skill gained from the longest to the shortest lead · R = 1 is the martingale reference; above it is unproductive revision variance</small></div>
      <div class="rows">
        {#each forest as f (f.key)}
          <div class="row" class:on={f.key === ui.model} title={`${f.label}: ${f.dates} complete dates in ${f.clusters} clusters (${f.arClusters} AR)${f.window ? ` · version window ${f.window}` : ''}${f.reason ? ` · ${f.reason}` : ''}`}>
            <b style:color={pal[f.key]?.text}>{pal[f.key]?.ident}</b>
            <div class="track">
              <i class="ref" style:left="{(100 * 1) / forestMax}%"></i>
              {#if f.ci}<i class="ci" style:left="{(100 * f.ci[0]) / forestMax}%" style:width="{(100 * (f.ci[1] - f.ci[0])) / forestMax}%"></i>{/if}
              {#if f.value != null}<i class="dot" style:left="{(100 * f.value) / forestMax}%" style:background={pal[f.key]?.text}></i>{/if}
            </div>
            <span>{f.value == null ? (f.skillGainNonpositive ? 'no skill gain' : '—') : fmt(f.value, 2)} <small>{f.ci ? fmtCi(f.ci) : f.reason}</small></span>
          </div>
        {/each}
      </div>
      <div class="foot">Whiskers: 95% cluster-bootstrap interval, B = 2000, draws shared across models. Vertical line: R = 1.</div>
    </section>
    <section class="col wf">
      <div class="h">Where the revisions happen — <span style:color={pal[ui.model]?.text}>{modelLabel(ui.model)}</span> <small>mean squared revision per run pair; dashed line = pooled skill gain{wf ? ` (${fmt(wf.skillGain, 1)} mm²) · R ${fmt(wf.r, 2)} over ${wf.units} dates` : ''}</small></div>
      <div class="plot">{#if wfConfig}<ChartCanvas config={wfConfig} />{:else}<div class="empty">No complete dates in this scope</div>{/if}</div>
    </section>
    <section class="col deltas">
      <div class="h">Paired ΔR vs <span style:color={pal[ui.model]?.text}>{modelLabel(ui.model)}</span> <small>R(model) − R({pal[ui.model]?.ident}) on the dates both models score, same draws</small></div>
      <div class="rows">
        {#each deltas as d (d.key)}
          <div class="drow" title={`${d.dates} shared dates · ${d.clusters} clusters`}>
            <b style:color={pal[d.key]?.text}>{pal[d.key]?.ident}</b>
            <span class="dv" class:pos={d.delta > 0} class:neg={d.delta < 0}>{d.delta == null ? '—' : `${d.delta > 0 ? '+' : ''}${fmt(d.delta, 2)}`}</span>
            <small>{d.ci ? `CI ${fmt(d.ci[0], 2)} to ${fmt(d.ci[1], 2)} · ${fmtP(d.pDirection)}` : d.suppressed ? 'interval suppressed' : d.delta == null ? 'skill gain not positive' : 'single cluster'}</small>
          </div>
        {/each}
      </div>
      <div class="foot">Positive = {pal[ui.model]?.ident} revises more efficiently. Intervals crossing zero do not separate the models.</div>
    </section>
  </div>
</BasinNotice>

<style>
  .grid { display: grid; grid-template-columns: 1.1fr 1.2fr 1fr; gap: 14px; flex: 1; min-height: 0; }
  .col { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
  .col + .col { border-left: 1px solid var(--panel-border); padding-left: 13px; }
  .h { font-size: 12.5px; font-weight: 600; color: #d5dae2; margin-bottom: 4px; }
  .h small { display: block; font-weight: 400; color: var(--text-secondary); font-size: 10px; line-height: 1.3; }
  .rows { display: flex; flex-direction: column; gap: 6px; flex: 1; justify-content: center; }
  .row { display: grid; grid-template-columns: 40px 1fr 110px; align-items: center; gap: 8px; font-size: 12px; color: #bec6d2; }
  .row.on { color: #eef2f7; }
  .row b { font-weight: 600; }
  .track { position: relative; height: 14px; }
  .track .ref { position: absolute; top: 0; bottom: 0; width: 1px; background: #ffffff50; }
  .track .ci { position: absolute; top: 5px; height: 4px; background: #ffffff33; border-radius: 2px; }
  .track .dot { position: absolute; top: 3px; width: 8px; height: 8px; border-radius: 50%; margin-left: -4px; }
  .row span { white-space: nowrap; font-variant-numeric: tabular-nums; }
  .row small, .drow small { color: var(--text-secondary); font-size: 10px; }
  .foot { font-size: 10px; color: var(--text-secondary); margin-top: 4px; line-height: 1.3; }
  .plot { position: relative; flex: 1; min-height: 70px; }
  .empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--text-secondary); font-size: 12px; }
  .drow { display: grid; grid-template-columns: 40px 60px 1fr; align-items: center; gap: 6px; font-size: 12px; }
  .dv { font-weight: 600; font-variant-numeric: tabular-nums; }
  .dv.pos { color: #7fd6a8; } .dv.neg { color: #f0a07a; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr 1fr; } .deltas { display: none; } }
</style>
