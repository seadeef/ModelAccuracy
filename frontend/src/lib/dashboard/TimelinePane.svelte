<script>
  /**
   * AR mode: the date range + one time slider (the map's footprints and the
   * storm panes' day), and under it the range day by day — PRISM observed
   * over the region as bars, every model's forecast for each day at the
   * inspected lead as lines. Click a day to inspect it everywhere.
   */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, selectedLead, modelLabel, leadBeyondModel } from './dashData.svelte.js';
  import { ensureArManifest, selectDay } from '../arManifest.svelte.js';
  import { datesInRange } from '../basinAnalysis.js';
  import { isoToYmd, ymdToIso } from '../bootstrap.js';
  import ArRangeControl from '../components/ArRangeControl.svelte';
  import ChartCanvas from './ChartCanvas.svelte';
  import { fmtRange } from './basinScope.svelte.js';
  import { leadAxisOptions, cursorPlugin, endLabelPlugin, onClickIndex, fmt, fmtDate, alpha } from './chartSetup.js';

  $effect(() => { ensureArManifest(); });

  const pal = $derived(palette());
  const series = $derived(dash.basinSeries);
  const lead = $derived(selectedLead());
  const rev = $derived(series?.[ui.model] ?? Object.values(series ?? {})[0] ?? null);
  /** Days of the range on the x-axis (the selected model's dates; other models map onto them). */
  const days = $derived.by(() => {
    if (!rev || !ui.arRangeStart || !ui.arRangeEnd) return [];
    return datesInRange(rev, [isoToYmd(ui.arRangeStart), isoToYmd(ui.arRangeEnd)]).map((d) => rev.dates[d]);
  });
  const targetYmd = $derived(isoToYmd(ui.arTargetDate));
  const cursor = cursorPlugin(() => days.indexOf(targetYmd));
  const ends = endLabelPlugin((ds) => ds.ident);
  const config = $derived.by(() => {
    if (!rev || !days.length) return null;
    const sel = ui.model;
    const observed = days.map((ymd) => { const d = rev.dates.indexOf(ymd); return d >= 0 ? rev.observed[d] : null; });
    const lines = appConfig.models.map((m) => {
      const r = series[m.key];
      const li = r ? lead - r.lead_min : -1;
      const data = days.map((ymd) => {
        if (!r || li < 0 || li >= (r.lead_max - r.lead_min + 1)) return null;
        const d = r.dates.indexOf(ymd);
        const v = d >= 0 ? r.forecasts[d]?.[li] : null;
        return v != null && Number.isFinite(v) ? v : null;
      });
      return {
        type: 'line', label: `${m.label} at lead ${lead}`, ident: pal[m.key]?.ident, data,
        borderColor: m.key === sel ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.7), endLabelColor: pal[m.key]?.text,
        borderWidth: m.key === sel ? 2.6 : 1.3, pointRadius: days.length > 60 ? 0 : m.key === sel ? 2.4 : 1.5, tension: 0.2, spanGaps: false,
        order: m.key === sel ? 0 : 1,
      };
    });
    const datasets = [
      { type: 'bar', label: 'PRISM observed', data: observed, backgroundColor: alpha('#7fd6a8', 0.38), borderColor: alpha('#7fd6a8', 0.6), borderWidth: 1, borderRadius: 2, barPercentage: 0.7, categoryPercentage: 0.9, order: 3 },
      ...lines,
    ];
    const vals = datasets.flatMap((d) => d.data).filter((v) => v != null);
    const top = Math.max(5, ...vals) * 1.12;
    const opts = leadAxisOptions({ yTitle: 'mm', yMin: 0, yMax: Math.ceil(top / 5) * 5, endLabels: true, tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 1)} mm` });
    opts.plugins.tooltip.callbacks.title = (items) => fmtDate(items[0]?.label, { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });
    opts.scales.x.ticks = { ...opts.scales.x.ticks, autoSkip: true, maxTicksLimit: 14, callback: (v) => fmtDate(days[v], days.length > 200 ? { month: 'short', year: '2-digit' } : { month: 'short', day: 'numeric' }) };
    opts.onClick = onClickIndex((i, ymd) => selectDay(ymdToIso(ymd)));
    return { type: 'bar', data: { labels: days, datasets }, options: opts, plugins: [cursor, ends] };
  });
  const beyond = $derived(appConfig.models.filter((m) => series?.[m.key] && leadBeyondModel(lead, m.key)).map((m) => m.label));
</script>

<div class="wrap">
  <div class="head">
    <div class="eyebrow" title="Pick a date range; the slider is the time — the map shows its footprints and the storm panes read its day. Purple ticks are catalogued AR episodes for this basin.">Atmospheric-river timeline · <em class="accent">{fmtRange() || '…'}</em></div>
  </div>
  <ArRangeControl episodes={dash.episodes?.episodes ?? []} />
  <div class="daily">
    <div class="head">
      <div class="eyebrow" title="Each day of the range: the PRISM observation over the region as a bar and every model's forecast for that day from the run at the inspected lead as a line. Change the lead in the lead bar; click a day to inspect it in every pane.">Day by day at lead <em>{lead}</em> · <em>{modelLabel(ui.model)}</em> and the others against observed</div>
      <span class="cap">{days.length ? `${days.length} days · click a day to inspect it` : ''}{beyond.length ? ` · no lead ${lead} run: ${beyond.join(', ')}` : ''}</span>
    </div>
    <div class="plot">
      {#if config}
        <ChartCanvas {config} />
      {:else}
        <div class="plot-empty">{series ? 'No scored days inside this range' : 'Loading the region\'s daily forecast history…'}</div>
      {/if}
    </div>
    <div class="legend"><span><i class="obs"></i>PRISM observed, region mean</span><span><i class="line"></i>Forecast for the day at lead {lead}, every model</span></div>
  </div>
</div>

<style>
  .wrap { display: flex; flex-direction: column; flex: 1; min-height: 0; gap: 8px; }
  .head { display: flex; align-items: center; justify-content: space-between; gap: 10px; min-width: 0; flex: none; }
  .head .cap { overflow: hidden; text-overflow: ellipsis; }
  .accent { color: #b9a6ff; }
  .daily { display: flex; flex-direction: column; flex: 1; min-height: 0; margin-top: 6px; padding-top: 10px; border-top: 1px solid var(--rule); gap: 4px; }
  .legend { display: flex; gap: 16px; align-items: center; color: #bec6d2; font-size: 12px; flex-wrap: wrap; flex: none; }
  .legend span { display: inline-flex; align-items: center; gap: 6px; }
  .legend i { display: inline-block; width: 14px; height: 3px; border-radius: 2px; }
  .legend i.obs { height: 10px; background: #7fd6a861; border: 1px solid #7fd6a899; }
  .legend i.line { background: #bec6d2; }
</style>
