<script>
  /**
   * Storm catalog for the basin's region:
   * every catalogued AR storm with its span, basins, PRISM volume and rank;
   * selecting one sets the inspected range/day. The detail card shows the
   * per-model storm ledger (steadiness J at pair 3, R, lead-3 error, FSS).
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, modelLabel } from './dashData.svelte.js';
  import { datesInRange, wettestDay } from '../basinAnalysis.js';
  import { ymdToIso, isoToYmd } from '../bootstrap.js';
  import BasinNotice from './BasinNotice.svelte';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, fmt, fmtDate, alpha, ordinal } from './chartSetup.js';
  import { selectedLead } from './dashData.svelte.js';

  const pal = $derived(palette());
  const huc = $derived(dash.basinHuc);
  const touching = $derived((dash.storms?.events || []).filter((e) => e.basin_hucs?.includes(huc)));
  /** Column sort: newest first by default; click a header to rank by size, span or peak. */
  const COLS = [
    { key: 'date', label: 'storm', title: 'Sort by date', get: (e) => e.start_date },
    { key: 'days', label: 'days', title: 'Sort by span', get: (e) => e.span_days ?? 0 },
    { key: 'basins', label: 'basins', title: 'Sort by basins touched', get: (e) => e.basins ?? 0 },
    { key: 'volume', label: 'km³', title: 'Sort by PRISM water volume', get: (e) => e.volume_km3 ?? -1 },
    { key: 'bar', label: '', title: '', get: null },
    { key: 'peak', label: 'peak', title: 'Sort by peak-day PRISM percentile', get: (e) => e.peak?.prism_pct ?? -1 },
    { key: 'rank', label: 'rank', title: 'Sort by volume rank (1 = largest of the record)', get: (e) => e.volume_rank ?? Infinity },
  ];
  let sortKey = $state('date');
  let sortDesc = $state(true);
  function sortBy(col) {
    if (!col.get) return;
    if (sortKey === col.key) sortDesc = !sortDesc;
    else { sortKey = col.key; sortDesc = col.key !== 'rank'; }
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
  let detailId = $state(null);
  const detail = $derived(events.find((e) => e.event_id === detailId) ?? selected ?? events.find((e) => e.in_analysis) ?? null);

  function pick(e) {
    detailId = e.event_id;
    ui.arRangeStart = e.start_date;
    ui.arRangeEnd = e.end_date;
    ui.arEpisodeId = null;
    const rev = dash.basinSeries?.[ui.model];
    if (rev) {
      const r = [isoToYmd(e.start_date), isoToYmd(e.end_date)];
      const peak = e.peak?.basin_huc === huc && e.peak?.basin_date && rev.dates.indexOf(isoToYmd(e.peak.basin_date)) >= 0
        ? rev.dates.indexOf(isoToYmd(e.peak.basin_date)) : wettestDay(rev, datesInRange(rev, r));
      if (peak != null) ui.arTargetDate = ymdToIso(rev.dates[peak]);
    }
  }

  const LEDGER = [
    { key: 'j3', label: 'J (pair 3)', digits: 2, note: 'storm-pooled jumpiness between the lead-4 and lead-3 runs, CONUS · lower is steadier', lower: true },
    { key: 'j3_ca', label: 'J (pair 3, region)', digits: 2, note: 'same, pooled over the region mask', lower: true },
    { key: 'r_eff', label: 'R', digits: 2, note: 'revision efficiency over the storm’s active basin-days (Σ revision² / Σ skill gain) · lower is more efficient', lower: true },
    { key: 'mae3', label: 'MAE lead 3', digits: 1, note: 'basin-mean absolute error of the lead-3 run (mm)', lower: true },
    { key: 'fss3', label: 'FSS lead 3', digits: 2, note: 'fractions skill score of the lead-3 run vs PRISM · higher is better', lower: false },
    { key: 'r3', label: 'Corr lead 3', digits: 2, note: 'Pearson r between lead-3 forecast and observed basin totals · higher is better', lower: false },
  ];
  /** J by run pair for the focused storm (the per-storm steadiness curves), every configured model. */
  const pairCursor = cursorPlugin(() => selectedLead() - 1);
  const jConfig = $derived.by(() => {
    const jb = detail?.j_by_pair;
    if (!jb) return null;
    const pairs = Array.from({ length: 13 }, (_, i) => i + 1);
    const datasets = appConfig.models.filter((m) => jb[m.key]).map((m) => ({
      label: m.label,
      data: pairs.map((pr) => { const i = jb[m.key].pairs.indexOf(pr); return i >= 0 ? jb[m.key].values[i] : null; }),
      borderColor: m.key === ui.model ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.7),
      borderWidth: m.key === ui.model ? 2.4 : 1.2, pointRadius: 0, tension: 0.18, spanGaps: false,
    }));
    if (!datasets.length) return null;
    const opts = leadAxisOptions({ yTitle: 'J = 1 − FSS', yMin: 0, yMax: 1, ticksLimit: 3, tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 2)}` });
    opts.plugins.tooltip.callbacks.title = (items) => `Runs at lead ${Number(items[0]?.label) + 1} → ${items[0]?.label}`;
    opts.layout.padding = { top: 2, right: 6 };
    return { type: 'line', data: { labels: pairs, datasets }, options: opts, plugins: [pairCursor] };
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
    <div class="notice">No storm catalog for this region yet — the AR episode catalog still drives the range picker.</div>
  {:else}
  <div class="wrap">
    <div class="table-wrap">
      <div class="caption">{dash.storms.region} · {events.length} storms touching {dash.basinInfo?.name}{dash.storms.record ? ` · record ${fmtDate(dash.storms.record.start, { month: 'short', year: 'numeric' })} – ${fmtDate(dash.storms.record.end, { month: 'short', year: 'numeric' })}` : ''} · click a storm to inspect it</div>
      <table>
        <thead><tr>
          {#each COLS as c (c.key)}
            <th class:sortable={!!c.get} class:sorted={sortKey === c.key} aria-sort={sortKey === c.key ? (sortDesc ? 'descending' : 'ascending') : 'none'} title={c.title} onclick={() => sortBy(c)}>{c.label}{#if sortKey === c.key}<span class="arrow">{sortDesc ? '▾' : '▴'}</span>{/if}</th>
          {/each}
        </tr></thead>
        <tbody>
          {#each events as e (e.event_id)}
            <tr class:on={e === selected} class:focus={e === detail} class:muted={!e.in_analysis} onclick={() => pick(e)} title={e.in_analysis ? 'Set the range and day to this storm' : 'Not in the analysed population (observation coverage incomplete)'}>
              <td>{fmtDate(e.start_date)}{e.start_date !== e.end_date ? ` – ${fmtDate(e.end_date)}` : ''}{e.flagship ? ' ★' : ''}</td>
              <td>{e.span_days}</td>
              <td>{e.basins}/{dash.storms.record?.basins ?? '—'}</td>
              <td>{e.volume_km3 != null ? fmt(e.volume_km3, e.volume_km3 >= 10 ? 0 : 1) : '—'}</td>
              <td class="bar"><i style:width="{e.volume_km3 ? (100 * e.volume_km3) / maxVol : 0}%"></i></td>
              <td>{e.peak?.prism_pct != null ? `${ordinal(e.peak.prism_pct)} pct` : '—'}</td>
              <td>{e.volume_rank ? `#${e.volume_rank}` : '—'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <aside class="detail">
      {#if detail}
        <div class="d-title">{fmtDate(detail.start_date)} – {fmtDate(detail.end_date)} <small>{detail.span_days} d · {detail.basins} basins{detail.volume_km3 != null ? ` · ${fmt(detail.volume_km3, 1)} km³ (${fmt(detail.record_share_pct, 1)}% of record)` : ''}</small></div>
        {#if detail.peak?.date}
          <div class="d-line">Peak {fmtDate(detail.peak.date)}: {fmt(detail.peak.mm, 1)} mm region-mean ({ordinal(detail.peak.prism_pct)} PRISM pct){detail.peak.basin_name ? ` · wettest basin ${detail.peak.basin_name} ${fmt(detail.peak.basin_mm, 0)} mm` : ''}</div>
        {/if}
        {#if jConfig}
          <div class="j-title">Steadiness through the storm <small>J by run pair, pooled over the storm's days on the region mask · lower is steadier</small></div>
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
          <div class="d-foot">Storm ledger from the curated pipeline (five-model population; models not configured here are omitted). Bold = best per row. R is gated at non-positive skill gain.</div>
        {:else}
          <div class="d-foot">No per-model ledger for this storm (outside the scored population).</div>
        {/if}
      {/if}
    </aside>
  </div>
  {/if}
</BasinNotice>

<style>
  .notice { flex: 1; display: flex; align-items: center; justify-content: center; color: var(--text-secondary); font-size: 12.5px; }
  .wrap { display: flex; gap: 14px; flex: 1; min-height: 0; }
  .table-wrap { flex: 1; min-width: 0; min-height: 0; overflow: auto; }
  .caption { font-size: 11px; color: var(--text-secondary); margin-bottom: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
  th { text-align: left; font-weight: 500; color: var(--text-secondary); font-size: 10.5px; padding: 2px 6px 4px 0; position: sticky; top: 0; background: var(--pane-bg); }
  th.sortable { cursor: pointer; user-select: none; }
  th.sortable:hover, th.sorted { color: var(--text-primary); }
  th .arrow { margin-left: 3px; font-size: 9px; }
  td { padding: 3px 6px 3px 0; white-space: nowrap; border-top: 1px solid #ffffff08; font-variant-numeric: tabular-nums; }
  tbody tr { cursor: pointer; }
  tbody tr:hover { background: #ffffff06; }
  tr.on { color: #fff; background: #a78bfa22; }
  tr.focus td:first-child { font-weight: 600; }
  tr.muted { color: var(--text-secondary); opacity: .6; }
  td.bar { width: 22%; }
  td.bar i { display: block; height: 7px; border-radius: 3px; background: #8b93ff; opacity: .7; }
  tr.on td.bar i { background: #f0a07a; opacity: 1; }
  .detail { width: 340px; flex: none; border-left: 1px solid var(--panel-border); padding-left: 13px; display: flex; flex-direction: column; gap: 5px; min-height: 0; overflow: auto; }
  .d-title { font-size: 13px; font-weight: 600; }
  .d-title small { display: block; font-weight: 400; font-size: 10.5px; color: var(--text-secondary); }
  .d-line { font-size: 11px; color: var(--text-secondary); line-height: 1.35; }
  .j-title { font-size: 11.5px; font-weight: 600; color: #d5dae2; margin-top: 2px; }
  .j-title small { display: block; font-weight: 400; color: var(--text-secondary); font-size: 10px; line-height: 1.3; }
  .j-plot { position: relative; height: 118px; flex: none; }
  .ledger th, .ledger td { padding: 2px 4px; text-align: right; }
  .ledger th:first-child, .ledger td:first-child { text-align: left; color: var(--text-secondary); }
  .ledger td.best { font-weight: 700; color: #fff; }
  .d-foot { font-size: 10px; color: var(--text-secondary); line-height: 1.35; }
  @media (max-width: 1100px) { .detail { width: 240px; } }
</style>
