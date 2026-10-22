<script>
  /**
   * Heavy-rain trigger sweep: event = observed ≥ q;
   * sweep the forecast trigger at the inspected lead to trace precision–recall,
   * and report the t = q slice (hit rate, frequency bias) per model.
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, modelLabel, selectedLead } from './dashData.svelte.js';
  import { triggerSkill } from '../basinAnalysis.js';
  import { scopeFor, scopeLabel } from './basinScope.svelte.js';
  import BasinNotice from './BasinNotice.svelte';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, fmt } from './chartSetup.js';

  const pal = $derived(palette());
  const series = $derived(dash.basinSeries);
  const lead = $derived(selectedLead());
  const results = $derived.by(() => {
    if (!series) return [];
    return appConfig.models.map((m) => {
      const rev = series[m.key];
      const sc = scopeFor(rev);
      if (!sc) return null;
      const li = lead - rev.lead_min;
      if (li < 0 || li >= rev.lead_max - rev.lead_min + 1) return { key: m.key, label: m.label, unavailable: true };
      return { key: m.key, label: m.label, ...triggerSkill(rev, sc.idx, ui.eventThresholdMm, li) };
    }).filter(Boolean);
  });
  const config = $derived.by(() => {
    const datasets = results.filter((r) => !r.unavailable).map((r) => ({
      label: r.label,
      data: r.recall.map((x, i) => ({ x, y: r.precision[i] })),
      borderColor: pal[r.key]?.text, borderWidth: r.key === ui.model ? 2.4 : 1.3, pointRadius: 0, tension: 0, showLine: true, order: 1,
    }));
    for (const r of results.filter((x) => !x.unavailable && x.point)) {
      datasets.push({ label: `${r.label} at ${r.q} mm`, data: [{ x: r.point.recall, y: r.point.precision }], borderColor: pal[r.key]?.text, backgroundColor: pal[r.key]?.text, pointRadius: 4, pointStyle: 'rectRot', showLine: false, order: 0 });
    }
    const opts = {
      responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: false },
      layout: { padding: { top: 6, right: 8 } },
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => ` ${c.dataset.label}: recall ${fmt(c.parsed.x, 2)} · precision ${fmt(c.parsed.y, 2)}` } } },
      scales: {
        x: { type: 'linear', min: 0, max: 1, grid: { color: '#ffffff0b' }, border: { display: false }, ticks: { font: { size: 10.5 }, maxTicksLimit: 6 }, title: { display: true, text: 'Recall (share of heavy days caught)', color: '#919daf', font: { size: 10 } } },
        y: { type: 'linear', min: 0, max: 1, grid: { color: '#ffffff0b' }, border: { display: false }, ticks: { font: { size: 10.5 }, maxTicksLimit: 5 }, title: { display: true, text: 'Precision', color: '#919daf', font: { size: 10.5 } } },
      },
    };
    return { type: 'scatter', data: { datasets }, options: opts, plugins: [] };
  });
</script>

<BasinNotice>
  <div class="grid">
    <section class="col">
      <div class="h">Precision–recall of the wet-day trigger at lead day {lead} <small>{scopeLabel()} · event = basin-mean PRISM ≥ {ui.eventThresholdMm} mm; each curve sweeps the forecast trigger from 0 to 80 mm; ◆ = trigger at the event threshold</small></div>
      <div class="plot"><ChartCanvas {config} /></div>
    </section>
    <section class="side">
      <div class="h">At the event threshold <small>the published single-threshold numbers: hit rate = recall, frequency bias = forecast ÷ observed heavy days</small></div>
      <div class="chips">
        <span>Event ≥</span>
        {#each [5, 10, 25] as q}<button type="button" class:active={ui.eventThresholdMm === q} onclick={() => { ui.eventThresholdMm = q; }}>{q} mm</button>{/each}
      </div>
      <table>
        <thead><tr><th></th><th>hits</th><th>miss</th><th>false</th><th>recall</th><th>precis.</th><th>F1</th><th>bias</th><th>AP</th><th>best F1 @</th></tr></thead>
        <tbody>
          {#each results as r (r.key)}
            <tr class:on={r.key === ui.model}>
              <td><b style:color={pal[r.key]?.text}>{pal[r.key]?.ident}</b></td>
              {#if r.unavailable || !r.point}
                <td colspan="9" class="na">no lead {lead} for this model</td>
              {:else}
                <td>{r.point.hits}</td><td>{r.point.misses}</td><td>{r.point.falseAlarms}</td>
                <td>{fmt(r.point.recall, 2)}</td><td>{fmt(r.point.precision, 2)}</td><td>{fmt(r.point.f1, 2)}</td>
                <td class:c-bad={r.point.frequencyBias != null && (r.point.frequencyBias < 0.7 || r.point.frequencyBias > 1.4)}>{fmt(r.point.frequencyBias, 2)}</td>
                <td>{fmt(r.ap, 2)}</td>
                <td>{fmt(r.bestF1.trigger, 1)} mm ({fmt(r.bestF1.f1, 2)})</td>
              {/if}
            </tr>
          {/each}
        </tbody>
      </table>
      <div class="foot">{results[0] && !results[0].unavailable ? `${results[0].nObs} heavy days of ${results[0].dates} (base rate ${(100 * results[0].baseRate).toFixed(0)}%) for ${modelLabel(results[0].key)}; counts differ by model with its scored dates.` : ''} Bias below 1 = the model refuses heavy days; above 1 = it over-calls them. AP = area under the precision–recall curve.</div>
    </section>
  </div>
</BasinNotice>

<style>
  .grid { display: grid; grid-template-columns: 1fr 1.25fr; gap: 14px; flex: 1; min-height: 0; }
  .col, .side { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
  .side { border-left: 1px solid var(--panel-border); padding-left: 13px; }
  .h { font-size: 12.5px; font-weight: 600; color: #d5dae2; margin-bottom: 4px; }
  .h small { display: block; font-weight: 400; color: var(--text-secondary); font-size: 10px; line-height: 1.3; }
  .plot { position: relative; flex: 1; min-height: 70px; }
  .chips { display: flex; gap: 4px; align-items: center; margin-bottom: 6px; font-size: 11px; color: var(--text-secondary); }
  .chips button { font: inherit; font-size: 11px; font-weight: 600; padding: 2px 7px; background: #ffffff06; border: 1px solid var(--panel-border); border-radius: 5px; color: var(--text-secondary); cursor: pointer; }
  .chips button.active { background: #9d7bff33; border-color: #9d7bff; color: #e8e0ff; }
  table { border-collapse: collapse; font-size: 11.5px; width: 100%; }
  th { text-align: right; font-weight: 500; color: var(--text-secondary); font-size: 10px; padding: 1px 4px; white-space: nowrap; }
  th:first-child { text-align: left; }
  td { text-align: right; padding: 2px 4px; font-variant-numeric: tabular-nums; border-top: 1px solid #ffffff08; white-space: nowrap; }
  td:first-child { text-align: left; }
  td.na { text-align: left; color: var(--text-secondary); }
  tr.on td { color: #fff; }
  .c-bad { color: #f0a07a; }
  .foot { font-size: 10px; color: var(--text-secondary); margin-top: 6px; line-height: 1.3; }
</style>
