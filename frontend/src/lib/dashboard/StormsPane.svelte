<script>
  /**
   * Catalogued AR storms for the selected region, with a per-model ledger.
   *
   * Exported HUC6 basins read the curated catalog of their region
   *: every storm with its span,
   * basins, PRISM volume and peak, and the published ledger (J at pair 3,
   * R, lead-3 error, FSS). Every other region lists the analysed storms
   * that reached it (``dash.regionStorms``) and computes the ledger from
   * the region's own daily series: rain observed here, RMS revision by run
   * pair, R, lead-1 / lead-3 error and the lead-3 correlation.
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, selectedLead } from './dashData.svelte.js';
  import { applyStormRange, fmtSpan } from './basinScope.svelte.js';
  import { datesInRange, stormLedger } from '../basinAnalysis.js';
  import { isoToYmd, ymdToIso } from '../bootstrap.js';
  import BasinNotice from './BasinNotice.svelte';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, endLabelPlugin, fmt, fmtDate, alpha, ordinal } from './chartSetup.js';

  const pal = $derived(palette());
  const huc = $derived(dash.basinHuc);
  /** Curated mode: the basin's catalog; region mode: the storms that reached the region. */
  const curated = $derived(!!dash.storms);
  const rev = $derived(dash.basinSeries?.[ui.model] ?? Object.values(dash.basinSeries ?? {})[0] ?? null);

  /** Region-mode storm rows: the region's own rain and wettest day over the storm's days. */
  const regionRows = $derived.by(() => {
    if (curated || !rev) return [];
    return (dash.regionStorms || []).map((e) => {
      const pos = datesInRange(rev, [isoToYmd(e.start_date), isoToYmd(e.end_date)]);
      let rain = 0; let wet = null; let any = false;
      for (const d of pos) {
        const y = rev.observed[d];
        if (y == null) continue;
        any = true; rain += y;
        if (wet === null || y > rev.observed[wet]) wet = d;
      }
      return { ...e, rain: any ? rain : null, wettest: wet != null ? rev.dates[wet] : null, wettestMm: wet != null ? rev.observed[wet] : null };
    });
  });
  const touching = $derived(curated ? (dash.storms?.events || []).filter((e) => e.basin_hucs?.includes(huc)) : regionRows);

  /** Column sort: newest first by default; click a header to rank by size, span or peak. */
  const COLS = $derived(curated ? [
    { key: 'date', label: 'Storm', title: 'Sort by date', get: (e) => e.start_date },
    { key: 'days', label: 'Days', title: 'Sort by span', get: (e) => e.span_days ?? 0 },
    { key: 'basins', label: 'Basins', title: 'Sort by basins touched', get: (e) => e.basins ?? 0 },
    { key: 'volume', label: 'Water, km³', title: 'Sort by PRISM water volume over the region', get: (e) => e.volume_km3 ?? -1 },
    { key: 'bar', label: '', title: '', get: null },
    { key: 'peak', label: 'Peak day', title: 'Sort by the peak day\'s PRISM percentile (1981–2024 baseline)', get: (e) => e.peak?.prism_pct ?? -1 },
  ] : [
    { key: 'date', label: 'Storm', title: 'Sort by date', get: (e) => e.start_date },
    { key: 'days', label: 'Days', title: 'Sort by span', get: (e) => e.span_days ?? 0 },
    { key: 'basins', label: 'Basins here', title: 'Sort by how many of the storm\'s catalogued basins lie in this region', get: (e) => e.touched ?? 0 },
    { key: 'rain', label: 'Rain here, mm', title: 'Sort by PRISM rain observed over this region through the storm (region mean, summed over its days)', get: (e) => e.rain ?? -1 },
    { key: 'bar', label: '', title: '', get: null },
    { key: 'peak', label: 'Wettest day', title: 'Sort by the wettest day\'s observed rain here', get: (e) => e.wettestMm ?? -1 },
  ]);
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
  const barOf = (e) => (curated ? e.volume_km3 : e.rain) || 0;
  const maxBar = $derived(Math.max(1, ...events.map(barOf)));
  const selected = $derived(events.find((e) => e.start_date === ui.arRangeStart && e.end_date === ui.arRangeEnd) ?? null);
  /** The focused storm: the one the strip holds, else the top row of the current sort. */
  const detail = $derived(selected ?? events[0] ?? null);

  function pick(e) {
    const peak = curated ? (e.peak?.basin_huc === huc ? e.peak?.basin_date : null) : (e.wettest ? ymdToIso(e.wettest) : null);
    applyStormRange({ start: e.start_date, end: e.end_date, peak });
  }

  // ── Curated ledger (published numbers) ──
  const LEDGER = [
    { key: 'j3', label: 'J, pair 3', digits: 2, note: 'storm-pooled jumpiness between the lead-4 and lead-3 runs over CONUS · lower is steadier', lower: true },
    { key: 'j3_ca', label: 'J, pair 3, region', digits: 2, note: 'the same, pooled over the region mask', lower: true },
    { key: 'r_eff', label: 'R', digits: 2, note: 'revision efficiency over the storm’s active basin-days · lower is more efficient', lower: true },
    { key: 'mae3', label: 'Error, lead 3', digits: 1, note: 'basin-mean absolute error of the lead-3 run (mm)', lower: true },
    { key: 'fss3', label: 'FSS, lead 3', digits: 2, note: 'fractions skill score of the lead-3 run against PRISM · higher is better', lower: false },
    { key: 'r3', label: 'Corr, lead 3', digits: 2, note: 'Pearson r between lead-3 forecast and observed basin totals · higher is better', lower: false },
  ];
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

  // ── Region ledger (computed here from the region's series) ──
  const LEAD = 3;
  const REGION_LEDGER = [
    { key: 'revision', label: 'Revision / day', digits: 1, note: 'mean over the storm\'s complete days of the summed |run-to-run change|, lead 14 → 1 (mm) · lower is steadier', lower: true },
    { key: 'r', label: 'R', digits: 2, note: 'revision efficiency: Σ revision² ÷ skill gained from the longest to the shortest lead over the storm\'s days · lower is more efficient', lower: true },
    { key: 'mae1', label: 'Error, lead 1', digits: 1, note: 'region-mean absolute error of the lead-1 run (mm)', lower: true },
    { key: 'maeL', label: `Error, lead ${LEAD}`, digits: 1, note: `region-mean absolute error of the lead-${LEAD} run (mm)`, lower: true },
    { key: 'corrL', label: `Corr, lead ${LEAD}`, digits: 2, note: `Pearson r between the lead-${LEAD} forecast and the observed region mean over the storm's days (3+ days) · higher is better`, lower: false },
  ];
  const ledgers = $derived.by(() => {
    // Every region off the exported basins (the published ledger is the basins', not this
    // region's), and inside a basin the storms the curated windows exclude.
    if (!detail || !dash.basinSeries || (curated && detail.models)) return null;
    const out = {};
    for (const m of appConfig.models) {
      const r = dash.basinSeries[m.key];
      if (!r) continue;
      out[m.key] = stormLedger(r, datesInRange(r, [isoToYmd(detail.start_date), isoToYmd(detail.end_date)]), { lead: LEAD });
    }
    return out;
  });
  function bestRegion(field, lower) {
    let bk = null;
    for (const m of appConfig.models) {
      const v = ledgers?.[m.key]?.[field];
      if (v == null) continue;
      if (bk === null || (lower ? v < ledgers[bk][field] : v > ledgers[bk][field])) bk = m.key;
    }
    return bk;
  }

  const pairCursor = cursorPlugin(() => selectedLead() - 1);
  const ends = endLabelPlugin((ds) => ds.ident, { minGap: 11, font: "600 10px 'DM Sans', system-ui, sans-serif" });
  const jConfig = $derived.by(() => {
    const pairs = Array.from({ length: 13 }, (_, i) => i + 1);
    let datasets;
    let yTitle = 'J';
    let yMax = 1;
    const published = curated && !!detail?.j_by_pair;
    if (published) {
      const jb = detail.j_by_pair;
      datasets = appConfig.models.filter((m) => jb[m.key]).map((m) => ({
        label: m.label, ident: pal[m.key]?.ident,
        data: pairs.map((pr) => { const i = jb[m.key].pairs.indexOf(pr); return i >= 0 ? jb[m.key].values[i] : null; }),
      }));
    } else {
      if (!ledgers) return null;
      yTitle = 'mm';
      yMax = undefined;
      datasets = appConfig.models.filter((m) => ledgers[m.key]?.rmsByPair).map((m) => {
        const l = ledgers[m.key];
        return {
          label: m.label, ident: pal[m.key]?.ident,
          data: pairs.map((pr) => { const i = l.pairLeads.indexOf(pr); return i >= 0 ? l.rmsByPair[i] : null; }),
        };
      });
    }
    if (!datasets.length) return null;
    for (const ds of datasets) {
      const key = appConfig.models.find((m) => m.label === ds.label)?.key;
      Object.assign(ds, {
        borderColor: key === ui.model ? pal[key]?.text : alpha(pal[key]?.text, 0.75), endLabelColor: pal[key]?.text,
        borderWidth: key === ui.model ? 2.4 : 1.2, pointRadius: 0, tension: 0.18, spanGaps: false, order: key === ui.model ? 0 : 1,
      });
    }
    const opts = leadAxisOptions({ yTitle, yMin: 0, yMax, ticksLimit: 3, endLabels: true, tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, published ? 2 : 1)}${published ? '' : ' mm'}` });
    opts.plugins.tooltip.callbacks.title = (items) => `Runs at lead ${Number(items[0]?.label) + 1} → ${items[0]?.label}`;
    opts.layout.padding = { top: 10, right: 40 };
    opts.scales.x.ticks.font = { size: 10.5 };
    opts.scales.y.ticks.font = { size: 10.5 };
    return { type: 'line', data: { labels: pairs, datasets }, options: opts, plugins: [pairCursor, ends] };
  });
  const detailLedger = $derived(ledgers ? ledgers[ui.model] ?? Object.values(ledgers)[0] ?? null : null);
  const published = $derived(curated && !!detail?.j_by_pair);
</script>

<BasinNotice>
  {#if !curated && !(dash.regionStorms?.length)}
    <div class="plot-empty">No catalogued atmospheric-river storm reached this region (storm catalogs cover the California and Pacific Northwest river basins)</div>
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
            <tr class:on={e === selected} class:focus={e === detail && !selected} class:muted={!e.in_analysis} onclick={() => pick(e)} title={e.in_analysis ? 'Make this the storm every storm pane reads' : 'Not in the published analysis: no storm day inside the curated version windows has a complete run sequence from every model (a gap in one model\'s archive or a version change). Its ledger is computed here from the region\'s daily series instead. Click to make it the storm every storm pane reads.'}>
              <td>{fmtSpan(e.start_date, e.end_date)}{e.flagship ? ' ★' : ''}</td>
              <td>{e.span_days}</td>
              {#if curated}
                <td>{e.basins}/{dash.storms.record?.basins ?? '—'}</td>
                <td>{e.volume_km3 != null ? fmt(e.volume_km3, e.volume_km3 >= 10 ? 0 : 1) : '—'}</td>
              {:else}
                <td title={`${e.touched} of the ${e.basins} ${e.regionName} basins this storm reached lie in this region`}>{e.touched} of {e.basins}</td>
                <td>{e.rain != null ? fmt(e.rain, e.rain >= 10 ? 0 : 1) : '—'}</td>
              {/if}
              <td class="bar"><i style:width="{(100 * barOf(e)) / maxBar}%"></i></td>
              {#if curated}
                <td>{e.peak?.prism_pct != null ? `${ordinal(e.peak.prism_pct)} pct` : '—'}</td>
              {:else}
                <td>{e.wettest ? `${fmtDate(e.wettest)} · ${fmt(e.wettestMm, 1)} mm` : '—'}</td>
              {/if}
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <aside class="detail">
      {#if detail}
        <div class="eyebrow"><em>{fmtSpan(detail.start_date, detail.end_date)}</em>{curated && detail.volume_rank ? ` · #${detail.volume_rank} by water volume` : !curated && detail.regionName ? ` · ${detail.regionName} catalog` : ''}</div>
        {#if curated}
          <div class="cap" title={detail.in_analysis ? 'Peak day percentile against the 1981–2024 PRISM baseline' : 'Not in the published analysis (no storm day with a complete run sequence from every model inside the curated windows): the steadiness curve and ledger below are computed from the daily series instead'}>{detail.span_days} d · {detail.basins} basins{detail.volume_km3 != null ? ` · ${fmt(detail.volume_km3, 1)} km³, ${fmt(detail.record_share_pct, 0)}% of record` : ''}{detail.peak?.date ? ` · peak ${fmtDate(detail.peak.date)}, ${ordinal(detail.peak.prism_pct)} pct` : ''}</div>
        {:else}
          <div class="cap">{detail.span_days} d · reached {detail.touched} of its {detail.basins} basins here{detail.rain != null ? ` · ${fmt(detail.rain, 0)} mm observed here` : ''}{detail.wettest ? ` · wettest ${fmtDate(detail.wettest)}, ${fmt(detail.wettestMm, 1)} mm` : ''}{detailLedger && detailLedger.complete < detailLedger.days ? ` · ${detailLedger.complete} of ${detailLedger.days} days with a complete run sequence` : ''}</div>
        {/if}
        {#if jConfig}
          <div class="j-head"><span class="eyebrow">Steadiness through the storm</span><span class="cap" title={published ? 'Storm-pooled jumpiness J between each pair of consecutive runs, from the published curated ledger' : `Root-mean-square run-to-run change of the ${curated ? 'basin' : 'region'}-mean forecast at each pair of consecutive runs, over the storm's days with a complete run sequence`}>{published ? 'J by run pair, over the storm\'s days · lower is steadier' : `RMS revision by run pair${curated && !detail.in_analysis ? ', from the basin series' : ''} · lower is steadier`}</span></div>
          <div class="j-plot"><ChartCanvas config={jConfig} /></div>
        {/if}
        {#if curated && detail.models}
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
        {:else if ledgers}
          <table class="ledger">
            <thead><tr><th></th>{#each appConfig.models as m}<th style:color={pal[m.key]?.text}>{pal[m.key]?.ident}</th>{/each}</tr></thead>
            <tbody>
              {#each REGION_LEDGER as row}
                {@const bk = bestRegion(row.key, row.lower)}
                <tr title={row.note}><td>{row.label}</td>
                  {#each appConfig.models as m}
                    {@const v = ledgers[m.key]?.[row.key]}
                    <td class:best={m.key === bk}>{v == null ? '—' : fmt(v, row.digits)}</td>
                  {/each}
                </tr>
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
  tr.focus { background: #ffffff08; }
  tr.on td:first-child { font-weight: 600; }
  tr.muted { color: var(--text-secondary); opacity: .55; }
  td.bar { width: 24%; }
  td.bar i { display: block; height: 8px; border-radius: 3px; background: #6f8fc9; opacity: .6; }
  tr.on td.bar i { background: var(--accent); opacity: 1; }
  .detail { width: 360px; flex: none; border-left: 1px solid var(--rule); padding-left: 16px; display: flex; flex-direction: column; gap: 4px; min-height: 0; overflow: auto; }
  .detail .eyebrow em { font-size: 13px; letter-spacing: 0; text-transform: none; }
  .detail > .cap { white-space: normal; line-height: 1.4; }
  .j-head { display: flex; flex-direction: column; gap: 1px; margin-top: 10px; }
  .j-head .cap { white-space: normal; line-height: 1.35; }
  .j-plot { position: relative; height: clamp(72px, 11.5vh, 104px); flex: none; }
  .ledger { margin-top: 6px; font-size: 12.5px; }
  .ledger th, .ledger td { padding: 2px 5px; text-align: right; font-size: 12px; letter-spacing: 0; text-transform: none; position: static; }
  .ledger th:first-child, .ledger td:first-child { text-align: left; color: var(--text-secondary); }
  .ledger td.best { font-weight: 700; color: #fff; }
  @media (max-width: 1360px) {
    .detail { width: 336px; padding-left: 12px; }
    .table-wrap > table > thead th:nth-child(5), td.bar { display: none; }
    .table-wrap > table { font-size: 12px; }
    .table-wrap > table th, .table-wrap > table td { padding-right: 4px; }
    .ledger { font-size: 11.5px; }
    .ledger th, .ledger td { padding: 2px 3px; font-size: 11.5px; }
  }
  @media (max-width: 1100px) { .detail { width: 250px; } }
</style>
