<script>
  /**
   * Storm catalog for the basin's region:
   * every catalogued AR storm with its span, basins, PRISM volume and peak;
   * selecting one sets the storm context. The detail shows the per-model
   * storm ledger (steadiness J at pair 3, R, lead-3 error, FSS).
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, selectedLead } from './dashData.svelte.js';
  import { applyStormRange } from './basinScope.svelte.js';
  import BasinNotice from './BasinNotice.svelte';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, endLabelPlugin, fmt, fmtDate, alpha, ordinal } from './chartSetup.js';

  const pal = $derived(palette());
  const huc = $derived(dash.basinHuc);
  const touching = $derived((dash.storms?.events || []).filter((e) => e.basin_hucs?.includes(huc)));
  /** Column sort: newest first by default; click a header to rank by size, span or peak. */
  const COLS = [
    { key: 'date', label: 'Storm', title: 'Sort by date', get: (e) => e.start_date },
    { key: 'days', label: 'Days', title: 'Sort by span', get: (e) => e.span_days ?? 0 },
    { key: 'basins', label: 'Basins', title: 'Sort by basins touched', get: (e) => e.basins ?? 0 },
    { key: 'volume', label: 'Water, km³', title: 'Sort by PRISM water volume over the region', get: (e) => e.volume_km3 ?? -1 },
    { key: 'bar', label: '', title: '', get: null },
    { key: 'peak', label: 'Peak day', title: 'Sort by the peak day\'s PRISM percentile (1981–2024 baseline)', get: (e) => e.peak?.prism_pct ?? -1 },
  ];
  let sortKey = $state('date');
  let sortDesc = $state(true);
  function sortBy(col) {
    if (!col.get) return;
    if (sortKey === col.key) sortDesc = !sortDesc;
    else { sortKey = col.key; sortDesc = true; }
  }
  const events = $derived.by(() => {
    const col = COLS.find((c) => c.key === sortKey) ?? COLS[0];
    const dir = sortDesc ? -1 : 1;
    return touching.slice().sort((a, b) => {
      const x = col.get(a), y = col.get(b);
      const d = x < y ? -1 : x > y ? 1 : 0;
      return d !== 0 ? dir * d : (a.start_date < b.start_date ? 1 : -1);
    });
  });
  const maxVol = $derived(Math.max(1, ...events.map((e) => e.volume_km3 || 0)));
  const selected = $derived(events.find((e) => e.start_date === ui.arRangeStart && e.end_date === ui.arRangeEnd) ?? null);
  const detail = $derived(selected ?? events.find((e) => e.in_analysis) ?? null);

  function pick(e) {
    applyStormRange({ start: e.start_date, end: e.end_date, peak: e.peak?.basin_huc === huc ? e.peak?.basin_date : null });
  }

  const LEDGER = [
    { key: 'j3', label: 'J, pair 3', digits: 2, note: 'storm-pooled jumpiness between the lead-4 and lead-3 runs over CONUS · lower is steadier', lower: true },
    { key: 'j3_ca', label: 'J, pair 3, region', digits: 2, note: 'the same, pooled over the region mask', lower: true },
    { key: 'r_eff', label: 'R', digits: 2, note: 'revision efficiency over the storm’s active basin-days · lower is more efficient', lower: true },
    { key: 'mae3', label: 'Error, lead 3', digits: 1, note: 'basin-mean absolute error of the lead-3 run (mm)', lower: true },
    { key: 'fss3', label: 'FSS, lead 3', digits: 2, note: 'fractions skill score of the lead-3 run against PRISM · higher is better', lower: false },
    { key: 'r3', label: 'Corr, lead 3', digits: 2, note: 'Pearson r between lead-3 forecast and observed basin totals · higher is better', lower: false },
  ];
  const pairCursor = cursorPlugin(() => selectedLead() - 1);
  const ends = endLabelPlugin((ds) => ds.ident, { minGap: 11, font: "600 10px 'DM Sans', system-ui, sans-serif" });
  const jConfig = $derived.by(() => {
    const jb = detail?.j_by_pair;
    if (!jb) return null;
    const pairs = Array.from({ length: 13 }, (_, i) => i + 1);
    const datasets = appConfig.models.filter((m) => jb[m.key]).map((m) => ({
      label: m.label, ident: pal[m.key]?.ident,
      data: pairs.map((pr) => { const i = jb[m.key].pairs.indexOf(pr); return i >= 0 ? jb[m.key].values[i] : null; }),
      borderColor: m.key === ui.model ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.75), endLabelColor: pal[m.key]?.text,
      borderWidth: m.key === ui.model ? 2.4 : 1.2, pointRadius: 0, tension: 0.18, spanGaps: false, order: m.key === ui.model ? 0 : 1,
    }));
    if (!datasets.length) return null;
    const opts = leadAxisOptions({ yTitle: 'J', yMin: 0, yMax: 1, ticksLimit: 3, endLabels: true, tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 2)}` });
    opts.plugins.tooltip.callbacks.title = (items) => `Runs at lead ${Number(items[0]?.label) + 1} → ${items[0]?.label}`;
    opts.layout.padding = { top: 4, right: 40 };
    opts.scales.x.ticks.font = { size: 10.5 };
    opts.scales.y.ticks.font = { size: 10.5 };
    return { type: 'line', data: { labels: pairs, datasets }, options: opts, plugins: [pairCursor, ends] };
  });

  function best(e, field, lower) {
    const vals = e.models?.[field];
    if (!vals) return null;
    let bk = null;
    for (const m of appConfig.models) {
      const v = vals[m.key];
      if (v == null) continue;
      if (bk === null || (lower ? v < vals[bk] : v > vals[bk])) bk = m.key;
    }
    return bk;
  }
</script>

<BasinNotice>
  {#if !dash.storms}
    <div class="plot-empty">No storm catalog for this region</div>
  {:else}
  <div class="wrap">
    <div class="table-wrap">
      <table>
        <thead><tr>
          {#each COLS as c (c.key)}
            <th class:sortable={!!c.get} class:sorted={sortKey === c.key} aria-sort={sortKey === c.key ? (sortDesc ? 'descending' : 'ascending') : 'none'} title={c.title} onclick={() => sortBy(c)}>{c.label}{#if sortKey === c.key}<span class="arrow">{sortDesc ? '▾' : '▴'}</span>{/if}</th>
          {/each}
        </tr></thead>
        <tbody>
          {#each events as e (e.event_id)}
            <tr class:on={e === selected} class:muted={!e.in_analysis} onclick={() => pick(e)} title={e.in_analysis ? 'Make this the storm every storm pane reads' : 'Outside the analysed record (incomplete observations)'}>
              <td>{fmtDate(e.start_date)}{e.start_date !== e.end_date ? ` – ${fmtDate(e.end_date)}` : ''}{e.flagship ? ' ★' : ''}</td>
              <td>{e.span_days}</td>
              <td>{e.basins}/{dash.storms.record?.basins ?? '—'}</td>
              <td>{e.volume_km3 != null ? fmt(e.volume_km3, e.volume_km3 >= 10 ? 0 : 1) : '—'}</td>
              <td class="bar"><i style:width="{e.volume_km3 ? (100 * e.volume_km3) / maxVol : 0}%"></i></td>
              <td>{e.peak?.prism_pct != null ? `${ordinal(e.peak.prism_pct)} pct` : '—'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <aside class="detail">
      {#if detail}
        <div class="eyebrow"><em>{fmtDate(detail.start_date)} – {fmtDate(detail.end_date)}</em>{detail.volume_rank ? ` · #${detail.volume_rank} by water volume` : ''}</div>
        <div class="cap">{detail.span_days} d · {detail.basins} basins{detail.volume_km3 != null ? ` · ${fmt(detail.volume_km3, 1)} km³, ${fmt(detail.record_share_pct, 0)}% of the record` : ''}{detail.peak?.date ? ` · peak ${fmtDate(detail.peak.date)}, ${ordinal(detail.peak.prism_pct)} PRISM percentile` : ''}</div>
        {#if jConfig}
          <div class="j-head"><span class="eyebrow">Steadiness through the storm</span><span class="cap">J by run pair, over the storm's days · lower is steadier</span></div>
          <div class="j-plot"><ChartCanvas config={jConfig} /></div>
        {/if}
        {#if detail.models}
          <table class="ledger">
            <thead><tr><th></th>{#each appConfig.models as m}<th style:color={pal[m.key]?.text}>{pal[m.key]?.ident}</th>{/each}</tr></thead>
            <tbody>
              {#each LEDGER as row}
                {#if detail.models[row.key]}
                  {@const bk = best(detail, row.key, row.lower)}
                  <tr title={row.note}><td>{row.label}</td>
                    {#each appConfig.models as m}
                      {@const v = detail.models[row.key]?.[m.key]}
                      <td class:best={m.key === bk}>{v == null ? '—' : fmt(v, row.digits)}</td>
                    {/each}
                  </tr>
                {/if}
              {/each}
            </tbody>
          </table>
        {/if}
      {/if}
    </aside>
  </div>
  {/if}
</BasinNotice>

<style>
  .wrap { display: flex; gap: 18px; flex: 1; min-height: 0; }
  .table-wrap { flex: 1; min-width: 0; min-height: 0; overflow: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; font-weight: 600; color: var(--text-secondary); font-size: 11px; letter-spacing: .06em; text-transform: uppercase; padding: 2px 8px 6px 0; position: sticky; top: 0; background: var(--pane-bg); white-space: nowrap; }
  th.sortable { cursor: pointer; user-select: none; }
  th.sortable:hover, th.sorted { color: var(--text-primary); }
  th .arrow { margin-left: 3px; font-size: 9px; }
  td { padding: 5px 8px 5px 0; white-space: nowrap; border-top: 1px solid var(--rule); font-variant-numeric: tabular-nums; }
  tbody tr { cursor: pointer; }
  tbody tr:hover { background: #ffffff06; }
  tr.on { color: #fff; background: #6eb5ff1c; }
  tr.on td:first-child { font-weight: 600; }
  tr.muted { color: var(--text-secondary); opacity: .55; }
  td.bar { width: 24%; }
  td.bar i { display: block; height: 8px; border-radius: 3px; background: #6f8fc9; opacity: .6; }
  tr.on td.bar i { background: var(--accent); opacity: 1; }
  .detail { width: 360px; flex: none; border-left: 1px solid var(--rule); padding-left: 16px; display: flex; flex-direction: column; gap: 4px; min-height: 0; overflow: auto; }
  .detail .eyebrow em { font-size: 13px; letter-spacing: 0; text-transform: none; }
  .detail > .cap { white-space: normal; line-height: 1.4; }
  .j-head { display: flex; flex-direction: column; gap: 1px; margin-top: 10px; }
  .j-plot { position: relative; height: 128px; flex: none; }
  .ledger { margin-top: 8px; font-size: 12.5px; }
  .ledger th, .ledger td { padding: 3px 5px; text-align: right; font-size: 12px; letter-spacing: 0; text-transform: none; position: static; }
  .ledger th:first-child, .ledger td:first-child { text-align: left; color: var(--text-secondary); }
  .ledger td.best { font-weight: 700; color: #fff; }
  @media (max-width: 1100px) { .detail { width: 250px; } }
</style>
