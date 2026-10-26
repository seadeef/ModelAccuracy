<script>
  /**
   * Revision efficiency R on the chosen date scope: the R forest with
   * cluster-bootstrap intervals (paired ΔR against the selected model in
   * each row's tooltip) and the martingale waterfall for the selected model.
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, modelLabel } from './dashData.svelte.js';
  import { revisionEfficiency, waterfall, pairedDeltaR } from '../basinAnalysis.js';
  import { scopeFor, scopeOptions } from './basinScope.svelte.js';
  import BasinNotice from './BasinNotice.svelte';
  import ScopeChips from './ScopeChips.svelte';
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
  const deltas = $derived.by(() => {
    const a = series?.[ui.model];
    if (!a) return {};
    const eps = dash.episodes?.episodes ?? [];
    const out = {};
    for (const m of appConfig.models) {
      if (m.key === ui.model || !series[m.key]) continue;
      out[m.key] = pairedDeltaR(a, series[m.key], eps, scopeOptions());
    }
    return out;
  });
  const fmtP = (p) => (p == null ? '' : p === 0 || p === 1 ? (p === 1 ? 'p(Δ>0) > 0.9995' : 'p(Δ>0) < 0.0005') : `p(Δ>0) = ${fmt(p, 3)}`);
  function rowTitle(f) {
    const base = `${f.label}: ${f.dates} complete dates in ${f.clusters} clusters (${f.arClusters} AR)${f.window ? ` · version window ${f.window}` : ''}${f.reason ? ` · ${f.reason}` : ''}`;
    const d = deltas[f.key];
    if (!d) return base;
    const dv = d.delta == null ? 'no paired ΔR' : `ΔR vs ${pal[ui.model]?.ident} ${d.delta > 0 ? '+' : ''}${fmt(d.delta, 2)}${d.ci ? ` [${fmt(d.ci[0], 2)}, ${fmt(d.ci[1], 2)}] · ${fmtP(d.pDirection)}` : ''} on ${d.dates} shared dates`;
    return `${base}\n${dv}`;
  }

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
      yTitle: 'mm²', yMin: 0, xTitle: 'Run pair, lead L → L−1',
      tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 1)} mm²`,
    });
    opts.plugins.tooltip.callbacks.title = (items) => `Pair ${items[0]?.label}`;
    return {
      type: 'bar',
      data: {
        labels: wf.pairLeads,
        datasets: [
          { type: 'line', label: 'Cumulative revision', data: cum, borderColor: '#9d7bff', borderWidth: 1.8, pointRadius: 2, order: 1 },
          { type: 'bar', label: 'Mean squared revision', data: wf.meanSquaredRevisionByPair, backgroundColor: alpha(pal[ui.model]?.text, 0.6), borderRadius: 2, order: 2 },
        ],
      },
      options: opts,
      plugins: [skillLine],
    };
  });
</script>

<BasinNotice>
  <div class="grid">
    <section class="col forest">
      <div class="head stack">
        <div class="eyebrow" title="R = Σ revision² ÷ skill gained from the longest to the shortest lead, over the chosen days. R = 1 is the martingale reference; above it is unproductive revision variance. Whiskers: 95% cluster-bootstrap interval, B = 2000, draws shared across models. Hover a row for the paired ΔR against the selected model.">Revision efficiency R</div>
        <ScopeChips />
      </div>
      <div class="rows">
        {#each forest as f (f.key)}
          <div class="row" class:on={f.key === ui.model} title={rowTitle(f)}>
            <b style:color={pal[f.key]?.text}>{pal[f.key]?.ident}</b>
            <div class="track">
              <i class="ref" style:left="{(100 * 1) / forestMax}%"></i>
              {#if f.ci}<i class="ci" style:left="{(100 * f.ci[0]) / forestMax}%" style:width="{(100 * (f.ci[1] - f.ci[0])) / forestMax}%"></i>{/if}
              {#if f.value != null}<i class="dot" style:left="{(100 * f.value) / forestMax}%" style:background={pal[f.key]?.text}></i>{/if}
            </div>
            <span>{f.value == null ? (f.skillGainNonpositive ? 'no skill gain' : '—') : fmt(f.value, 2)} <small>{f.ci ? fmtCi(f.ci) : f.suppressed ? 'no interval' : f.reason}</small></span>
          </div>
        {/each}
      </div>
      <div class="cap">Dot: R · bar: 95% interval · line: R = 1 · lower is more efficient</div>
    </section>
    <section class="col wf">
      <div class="head">
        <div class="eyebrow" title="Mean squared run-to-run revision at each run pair for the selected model; the dashed line is the pooled skill gain the revisions bought. The ratio of the two totals is R.">Where <em style:color={pal[ui.model]?.text}>{modelLabel(ui.model)}</em> revises</div>
        <span class="cap">{wf ? `R ${fmt(wf.r, 2)} over ${wf.units} dates` : ''}</span>
      </div>
      <div class="plot">{#if wfConfig}<ChartCanvas config={wfConfig} />{:else}<div class="plot-empty">No complete dates in this scope</div>{/if}</div>
      <div class="cap">Bars: mean squared revision per pair · purple line: cumulative · dashed: skill gain{wf ? ` ${fmt(wf.skillGain, 1)} mm²` : ''}</div>
    </section>
  </div>
</BasinNotice>

<style>
  .grid { display: grid; grid-template-columns: 1fr 1.25fr; gap: 18px; flex: 1; min-height: 0; }
  .col { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
  .col + .col { border-left: 1px solid var(--rule); padding-left: 16px; }
  .head { display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-bottom: 6px; min-width: 0; }
  .head .eyebrow { flex: 1; }
  .head.stack { flex-direction: column; align-items: flex-start; gap: 6px; }
  .rows { display: flex; flex-direction: column; gap: 14px; flex: 1; justify-content: flex-start; padding-top: 10px; }
  .row { display: grid; grid-template-columns: 46px 1fr 120px; align-items: center; gap: 10px; font-size: 13px; color: #bec6d2; }
  .row.on { color: #eef2f7; }
  .row b { font-weight: 600; }
  .track { position: relative; height: 16px; }
  .track .ref { position: absolute; top: 0; bottom: 0; width: 1px; background: #ffffff55; }
  .track .ci { position: absolute; top: 6px; height: 4px; background: #ffffff33; border-radius: 2px; }
  .track .dot { position: absolute; top: 3px; width: 10px; height: 10px; border-radius: 50%; margin-left: -5px; }
  .row span { white-space: nowrap; font-variant-numeric: tabular-nums; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
  .row small { color: var(--text-secondary); font-size: 11px; }
  .col > .cap { margin-top: 6px; white-space: normal; line-height: 1.35; }
</style>
