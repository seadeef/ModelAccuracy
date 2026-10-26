<script>
  /**
   * Heavy-rain trigger sweep: event = observed ≥ q;
   * sweep the forecast trigger at the inspected lead to trace precision–recall,
   * and report the t = q slice (hit rate, frequency bias) per model.
   *
   * Two estimands, one display. An exported river basin judges the BASIN-MEAN
   * day (a daily basin total crossing q), computed here from the series. Any
   * other region is judged per CELL-DAY on the server (``/api/series/heavy-rain``):
   * the mean over a state almost never reaches 25 mm, so the day-level curve
   * collapses to a few steps, while hundreds of thousands of cell-days give the
   * per-cell verification idiom the accuracy maps already use.
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, modelLabel, selectedLead } from './dashData.svelte.js';
  import { triggerSkill } from '../basinAnalysis.js';
  import { fetchHeavyRainAllModels } from '../api.js';
  import { scopeFor, scopeLabel } from './basinScope.svelte.js';
  import BasinNotice from './BasinNotice.svelte';
  import ScopeChips from './ScopeChips.svelte';
  import ChartCanvas from './ChartCanvas.svelte';
  import { endLabelPlugin, fmt, alpha } from './chartSetup.js';

  const pal = $derived(palette());
  const series = $derived(dash.basinSeries);
  const lead = $derived(selectedLead());
  /** Cell-day pooling for regions served from the per-cell cube; basin-mean days for exported basins. */
  const cellDays = $derived(dash.basinInfo?.source === 'cube');

  /** The scope's date population, as the union over models (each model's complete dates differ slightly). */
  const scopeDates = $derived.by(() => {
    if (!series) return [];
    const set = new Set();
    for (const m of appConfig.models) {
      const rev = series[m.key];
      const sc = scopeFor(rev);
      if (sc) for (const d of sc.idx) set.add(rev.dates[d]);
    }
    return [...set].sort();
  });

  // ── Server sweep (cell-days), generation-counter idiom ──
  let server = $state(null);
  let serverLoading = $state(false);
  let srvGen = 0;
  let srvKey = '';
  $effect(() => {
    const region = ui.selectedRegion;
    const q = ui.eventThresholdMm;
    const l = lead;
    const dates = scopeDates;
    const key = cellDays && region && series ? `${JSON.stringify(region.huc ?? region.fips ?? region.site ?? region.bounds ?? region.coordinates)}|${q}|${l}|${dates.length}:${dates[0]}:${dates[dates.length - 1]}|${scopeLabel()}` : '';
    if (key === srvKey) return;
    srvKey = key;
    const gen = ++srvGen;
    if (!key) { server = null; serverLoading = false; return; }
    serverLoading = true;
    queueMicrotask(async () => {
      const data = await fetchHeavyRainAllModels({ region, q, lead: l, dates });
      if (gen !== srvGen) return;
      server = data;
      serverLoading = false;
    });
  });

  const results = $derived.by(() => {
    if (!series) return [];
    if (cellDays) {
      if (!server) return [];
      return appConfig.models.map((m) => {
        const r = server[m.key];
        if (!r) return null;
        if (r.unavailable) return { key: m.key, label: m.label, unavailable: true };
        return { key: m.key, label: m.label, ...r };
      }).filter(Boolean);
    }
    return appConfig.models.map((m) => {
      const rev = series[m.key];
      const sc = scopeFor(rev);
      if (!sc) return null;
      const li = lead - rev.lead_min;
      if (li < 0 || li >= rev.lead_max - rev.lead_min + 1) return { key: m.key, label: m.label, unavailable: true };
      return { key: m.key, label: m.label, ...triggerSkill(rev, sc.idx, ui.eventThresholdMm, li) };
    }).filter(Boolean);
  });
  const ends = endLabelPlugin((ds) => ds.ident);
  const config = $derived.by(() => {
    // Consecutive sweep steps often land on the same (recall, precision) point; keep one.
    const dedupe = (r) => r.recall.map((x, i) => ({ x, y: r.precision[i], t: r.triggers?.[i] })).filter((pt, i, arr) => i === 0 || pt.x !== arr[i - 1].x || pt.y !== arr[i - 1].y);
    const datasets = results.filter((r) => !r.unavailable).map((r) => ({
      label: r.label, ident: pal[r.key]?.ident, modelKey: r.key,
      data: dedupe(r),
      borderColor: r.key === ui.model ? pal[r.key]?.text : alpha(pal[r.key]?.text, 0.75), endLabelColor: pal[r.key]?.text,
      borderWidth: r.key === ui.model ? 2.6 : 1.4, pointRadius: 0, tension: 0, showLine: true, order: r.key === ui.model ? 0 : 1,
    }));
    for (const r of results.filter((x) => !x.unavailable && x.point)) {
      datasets.push({ label: `${r.label} at ${r.q} mm`, modelKey: r.key, isPoint: true, data: [{ x: r.point.recall, y: r.point.precision, t: r.q }], borderColor: pal[r.key]?.text, backgroundColor: pal[r.key]?.text, pointRadius: 4.5, pointStyle: 'rectRot', showLine: false, order: 0, endLabel: false });
    }
    const opts = {
      responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: false },
      layout: { padding: { top: 8, right: 10 } },
      plugins: { legend: { display: false }, tooltip: {
        // One row per model: drop repeated curve hits, and the diamond when its curve row is already listed.
        filter: (item, idx, items) => items.findIndex((o) => o.datasetIndex === item.datasetIndex) === idx
          && !(item.dataset.isPoint && items.some((o) => !o.dataset.isPoint && o.dataset.modelKey === item.dataset.modelKey)),
        callbacks: { title: () => '', label: (c) => ` ${c.dataset.label}${c.raw?.t != null ? ` at trigger ${fmt(c.raw.t, 1)} mm` : ''}: recall ${fmt(c.parsed.x, 2)} · precision ${fmt(c.parsed.y, 2)}` },
      } },
      scales: {
        x: { type: 'linear', min: 0, max: 1, grid: { color: '#ffffff0b' }, border: { display: false }, ticks: { font: { size: 11.5 }, maxTicksLimit: 6 }, title: { display: true, text: 'Recall · share of heavy days caught', color: '#8f9aab', font: { size: 11 } } },
        y: { type: 'linear', min: 0, max: 1, grid: { color: '#ffffff0b' }, border: { display: false }, ticks: { font: { size: 11.5 }, maxTicksLimit: 5 }, title: { display: true, text: 'Precision · share of calls that verified', color: '#8f9aab', font: { size: 11 } } },
      },
    };
    return { type: 'scatter', data: { datasets }, options: opts, plugins: [] };
  });
  const lead0 = $derived(results.find((r) => !r.unavailable) ?? null);
</script>

<BasinNotice>
  <div class="grid">
    <section class="col">
      <div class="head stack">
        <div class="eyebrow" title={cellDays
          ? 'Event = PRISM at or above the threshold in one 0.25° grid cell on one day; every cell-day in the region is a trial, so a state has hundreds of thousands. Each curve sweeps the model\'s forecast trigger from 0 to 80 mm; the diamond is the operating point where the trigger equals the event threshold.'
          : 'Event = basin-mean PRISM at or above the threshold. Each curve sweeps the model\'s forecast trigger from 0 to 80 mm; the diamond is the operating point where the trigger equals the event threshold.'}>
          Heavy-rain calls at lead day <em>{lead}</em>{#if cellDays}<span class="cap note">· per grid cell and day</span>{/if}
        </div>
        <ScopeChips />
      </div>
      <div class="plot">
        {#if cellDays && !server}
          <div class="plot-empty">{serverLoading ? 'Pooling heavy-rain calls over the region\'s grid cells…' : 'The heavy-rain request failed; change the lead or threshold to retry'}</div>
        {:else}
          <ChartCanvas {config} />
        {/if}
      </div>
      <div class="cap">◆ trigger = event threshold{lead0 ? (cellDays
        ? ` · ${fmt(lead0.nObs, 0)} heavy cell-days of ${fmt(lead0.cellDays, 0)} (${lead0.cells} cells × ${lead0.dates} days) for ${modelLabel(lead0.key)}`
        : ` · ${lead0.nObs} heavy days of ${lead0.dates} for ${modelLabel(lead0.key)}`) : ''}</div>
    </section>
    <section class="side">
      <div class="head">
        <div class="eyebrow">At the event threshold</div>
        <span class="cap">Event ≥</span>
        <span class="chips">
          {#each [5, 10, 25] as q}<button type="button" class="chip" class:active={ui.eventThresholdMm === q} onclick={() => { ui.eventThresholdMm = q; }}>{q} mm</button>{/each}
        </span>
      </div>
      <table>
        <thead><tr>
          <th></th>
          <th title={cellDays ? 'Heavy cell-days the model called' : 'Heavy days the model called'}>Hits</th>
          <th title={cellDays ? 'Heavy cell-days the model missed' : 'Heavy days the model missed'}>Misses</th>
          <th title={cellDays ? 'Calls on cell-days that stayed below the threshold' : 'Calls on days that stayed below the threshold'}>False alarms</th>
          <th title="Share of calls that verified">Precision</th>
          <th title={cellDays ? 'Share of heavy cell-days caught (hit rate)' : 'Share of heavy days caught (hit rate)'}>Recall</th>
          <th title={cellDays ? 'Forecast heavy cell-days ÷ observed heavy cell-days. Below 1 the model under-calls heavy rain; above 1 it over-calls.' : 'Forecast heavy days ÷ observed heavy days. Below 1 the model under-calls heavy rain; above 1 it over-calls.'}>Bias</th>
          <th title="Average precision: area under the precision–recall curve">AP</th>
        </tr></thead>
        <tbody>
          {#each results as r (r.key)}
            <tr class:on={r.key === ui.model}>
              <td><b style:color={pal[r.key]?.text}>{pal[r.key]?.ident}</b></td>
              {#if r.unavailable || !r.point}
                <td colspan="7" class="na">no lead {lead} for this model</td>
              {:else}
                <td>{r.point.hits}</td><td>{r.point.misses}</td><td>{r.point.falseAlarms}</td>
                <td>{fmt(r.point.precision, 2)}</td><td>{fmt(r.point.recall, 2)}</td>
                <td class:c-bad={r.point.frequencyBias != null && (r.point.frequencyBias < 0.7 || r.point.frequencyBias > 1.4)}>{fmt(r.point.frequencyBias, 2)}</td>
                <td>{fmt(r.ap, 2)}</td>
              {/if}
            </tr>
          {/each}
        </tbody>
      </table>
    </section>
  </div>
</BasinNotice>

<style>
  .grid { display: grid; grid-template-columns: 1fr 1.15fr; gap: 18px; flex: 1; min-height: 0; }
  .col, .side { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
  .side { border-left: 1px solid var(--rule); padding-left: 16px; }
  .head { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; min-width: 0; }
  .head .eyebrow { flex: 1; }
  .head.stack { flex-direction: column; align-items: flex-start; gap: 6px; }
  .note { margin-left: 6px; text-transform: none; letter-spacing: 0; }
  .col > .cap { margin-top: 4px; white-space: normal; line-height: 1.35; }
  table { border-collapse: collapse; font-size: 13px; width: 100%; margin-top: 4px; }
  th { text-align: right; font-weight: 600; color: var(--text-secondary); font-size: 11px; letter-spacing: .06em; text-transform: uppercase; padding: 2px 6px 6px; white-space: nowrap; }
  th:first-child { text-align: left; padding-left: 0; }
  td { text-align: right; padding: 6px 6px; font-variant-numeric: tabular-nums; border-top: 1px solid var(--rule); white-space: nowrap; }
  td:first-child { text-align: left; padding-left: 0; }
  td.na { text-align: left; color: var(--text-secondary); }
  tr.on td { color: #fff; }
  .c-bad { color: #f0a07a; }
  @media (max-width: 1360px) {
    .grid { grid-template-columns: 1fr 1.3fr; gap: 14px; }
    .side { padding-left: 12px; }
    table { font-size: 12px; }
    th, td { padding-left: 4px; padding-right: 4px; }
  }
</style>
