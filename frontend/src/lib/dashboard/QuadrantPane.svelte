<script>
  /**
   * Consistency-by-accuracy plane (the quadrant figure, generalised):
   * every model at the inspected lead, x = jumpiness J for the run pair
   * ending at that lead (reversed: steadier to the right), y = the selected
   * accuracy statistic (better upward). The selected model is pinned as the
   * origin, so the quadrants read as "steadier / more accurate than it".
   */
  import { ui, appConfig, statLabel, isArPeriod } from '../state.svelte.js';
  import { dash, selectedLead, palette, modelLabel, statAt, statUnits, hasAnyValue, emptyReason } from './dashData.svelte.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import { fmt, alpha } from './chartSetup.js';

  const pal = $derived(palette());
  const lead = $derived(selectedLead());
  const stabKey = $derived(appConfig.consistency.curve_roles?.stability ?? 'jumpiness');
  const meta = $derived(appConfig.statistics.find((s) => s.key === ui.statistic) ?? {});
  const mode = $derived(meta.winner_mode || 'lower'); // lower | higher | absolute_lower
  const lowerBetter = $derived(mode !== 'higher');
  const units = $derived(statUnits(ui.statistic));
  const supported = $derived(ui.period === 'yearly' || ui.period === 'seasonal' || isArPeriod(ui.period));
  /** Nothing scored at any lead (not just this one): explain the region or season rather than the lead. */
  const nothingScored = $derived(!!dash.accuracy && !!dash.consistency
    && !(hasAnyValue(dash.accuracy, ui.statistic) && hasAnyValue(dash.consistency, stabKey)));

  const points = $derived.by(() => appConfig.models.map((m) => {
    const j = statAt(dash.consistency?.[m.key], lead, stabKey)?.value ?? null;
    const raw = statAt(dash.accuracy?.[m.key], lead, ui.statistic)?.value ?? null;
    const acc = raw == null ? null : mode === 'absolute_lower' ? Math.abs(raw) : raw;
    return { key: m.key, label: m.label, j, raw, acc };
  }).filter((p) => p.j != null && p.acc != null));
  /** The selected model is the origin; when it has no value at this lead (NBM / WeatherNext 2 past
   *  their range) the first scoring model stands in, and the pane says so. */
  const originFallback = $derived(!points.some((p) => p.key === ui.model) && points.length > 0);
  const origin = $derived(points.find((p) => p.key === ui.model) ?? points[0] ?? null);
  const originKey = $derived(origin?.key ?? ui.model);
  /** J for the pair ending at the longest lead needs a run that does not exist: say so instead of "not enough models". */
  const longest = $derived(Math.max(...appConfig.models.map((m) => m.lead_days_max ?? 14)));
  const better = (p) => origin && p.key !== origin.key && p.j < origin.j && (lowerBetter ? p.acc < origin.acc : p.acc > origin.acc);
  const steadier = (p) => origin && p.key !== origin.key && p.j < origin.j;
  const moreAccurate = (p) => origin && p.key !== origin.key && (lowerBetter ? p.acc < origin.acc : p.acc > origin.acc);

  /** Quadrant tints, origin crosshair and point labels. */
  const decor = {
    id: 'quadrant-decor',
    beforeDatasetsDraw(chart) {
      const { ctx, chartArea: a, scales: { x, y } } = chart;
      if (!origin) return;
      const ox = x.getPixelForValue(origin.j);
      const oy = y.getPixelForValue(origin.acc);
      ctx.save();
      // Trade-off quadrants raised; the "beats the origin on both" quadrant is top-right
      // because x is reversed (steadier right) and y is oriented better-up.
      ctx.fillStyle = '#7fd6a812';
      ctx.fillRect(ox, a.top, a.right - ox, oy - a.top);
      ctx.fillStyle = '#f0a07a0f';
      ctx.fillRect(a.left, oy, ox - a.left, a.bottom - oy);
      ctx.strokeStyle = '#ffffff35';
      ctx.setLineDash([4, 4]);
      ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(ox, a.top); ctx.lineTo(ox, a.bottom); ctx.moveTo(a.left, oy); ctx.lineTo(a.right, oy); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#919daf';
      ctx.font = '600 11px "DM Sans", system-ui, sans-serif';
      ctx.textBaseline = 'top';
      ctx.textAlign = 'right'; ctx.fillText('steadier · more accurate', a.right - 4, a.top + 3);
      ctx.textAlign = 'left'; ctx.fillText('jumpier · more accurate', a.left + 4, a.top + 3);
      ctx.textBaseline = 'bottom';
      ctx.textAlign = 'right'; ctx.fillText('steadier · less accurate', a.right - 4, a.bottom - 3);
      ctx.textAlign = 'left'; ctx.fillText('jumpier · less accurate', a.left + 4, a.bottom - 3);
      ctx.restore();
    },
    afterDatasetsDraw(chart) {
      const { ctx, scales: { x, y }, chartArea: a } = chart;
      ctx.save();
      ctx.font = '600 12px "DM Sans", system-ui, sans-serif';
      ctx.textBaseline = 'middle';
      for (const p of points) {
        const px = x.getPixelForValue(p.j);
        const py = y.getPixelForValue(p.acc);
        const right = px + 60 < a.right;
        ctx.textAlign = right ? 'left' : 'right';
        ctx.fillStyle = pal[p.key]?.text || '#fff';
        ctx.fillText(pal[p.key]?.ident ?? p.label, right ? px + 9 : px - 9, py);
      }
      ctx.restore();
    },
  };

  const config = $derived.by(() => {
    const js = points.map((p) => p.j);
    const as = points.map((p) => p.acc);
    const pad = (lo, hi) => { const s = Math.max(1e-6, (hi - lo) * 0.35); return [lo - s, hi + s]; };
    const [jx0, jx1] = pad(Math.min(...js), Math.max(...js));
    const [ay0, ay1] = pad(Math.min(...as), Math.max(...as));
    const datasets = [{
      label: 'models',
      data: points.map((p) => ({ x: p.j, y: p.acc, key: p.key, label: p.label, raw: p.raw })),
      pointRadius: points.map((p) => (p.key === originKey ? 7 : 5.5)),
      pointHoverRadius: 8,
      pointBackgroundColor: points.map((p) => alpha(pal[p.key]?.text, 0.9)),
      pointBorderColor: points.map((p) => (p.key === originKey ? '#fff' : alpha(pal[p.key]?.text, 1))),
      pointBorderWidth: points.map((p) => (p.key === originKey ? 2 : 1)),
      showLine: false,
    }];
    const yLabel = `${statLabel(ui.statistic)}${units ? ` (${units})` : ''}${mode === 'absolute_lower' ? ', magnitude' : ''}`;
    const opts = {
      responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: true },
      layout: { padding: { top: 4, right: 8 } },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: {
          title: (items) => items[0]?.raw?.label ?? '',
          label: (c) => [` J at pair ${lead}: ${fmt(c.raw.x, 3)}`, ` ${statLabel(ui.statistic)} at lead ${lead}: ${fmt(c.raw.raw, 2)}${units}`],
        } },
      },
      scales: {
        x: { type: 'linear', reverse: true, min: Math.max(0, jx0), max: Math.min(1, jx1), grid: { color: '#ffffff0b' }, border: { display: false }, ticks: { font: { size: 11.5 }, maxTicksLimit: 6, includeBounds: false }, title: { display: true, text: `Jumpiness J, runs at lead ${lead + 1} → ${lead} · steadier →`, color: '#8f9aab', font: { size: 11 } } },
        y: { type: 'linear', reverse: lowerBetter, min: ay0, max: ay1, grid: { color: '#ffffff0b' }, border: { display: false }, ticks: { font: { size: 11.5 }, maxTicksLimit: 5, includeBounds: false }, title: { display: true, text: `${yLabel} · more accurate ↑`, color: '#8f9aab', font: { size: 11 } } },
      },
    };
    return { type: 'scatter', data: { datasets }, options: opts, plugins: [decor] };
  });

  const beats = $derived(points.filter(better));
  const verdict = $derived.by(() => {
    if (!origin) {
      if (lead >= longest) return `Jumpiness compares each run with the run before it, so there is no pair ending at lead day ${lead}, the longest lead. Pick lead day ${longest - 1} or shorter.`;
      return `No model has both a jumpiness and a ${statLabel(ui.statistic)} value at lead day ${lead} here.`;
    }
    const o = pal[originKey]?.ident ?? origin.label;
    const pre = originFallback ? `${modelLabel(ui.model)} has no lead ${lead}, so ${o} is the origin. ` : '';
    if (!beats.length) return `${pre}No model is both steadier and more accurate than ${o} at lead day ${lead}.`;
    const names = beats.map((p) => pal[p.key]?.ident ?? p.label);
    const list = names.length > 1 ? `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}` : names[0];
    const verb = names.length === 1 ? 'is' : names.length === 2 ? 'are both' : 'are all';
    return `${pre}${list} ${verb} steadier and more accurate than ${o} at lead day ${lead}.`;
  });
</script>

<div class="grid">
  <section class="col">
    <div class="head">
      <div class="eyebrow" title="Every model at the inspected lead. x = jumpiness J for the run pair ending at this lead (threshold and scale from the consistency pane); y = the selected accuracy statistic at this lead. The selected model is the origin, so the shaded quadrant holds models that beat it on both.">
        Steadiness against accuracy at lead day <em>{lead}</em>
      </div>
      <span class="cap">origin = <b style:color={pal[originKey]?.text}>{modelLabel(originKey)}</b></span>
    </div>
    <div class="plot">
      {#if !supported}
        <div class="plot-empty">Consistency is computed for the full record and the four seasons</div>
      {:else if points.length >= 2}
        <ChartCanvas {config} />
      {:else}
        <div class="plot-empty">{dash.consistencyLoading || dash.accuracyLoading ? 'Loading curves…' : nothingScored ? `Nothing scored here. ${emptyReason(ui.selectedRegion, { periodSpecific: ui.period === 'seasonal' })}` : lead >= longest ? `No run pair ends at lead day ${lead}, the longest lead — pick lead day ${longest - 1} or shorter` : `Fewer than two models score at lead day ${lead}`}</div>
      {/if}
    </div>
  </section>
  <section class="side">
    <div class="verdict">{verdict}</div>
    <table>
      <thead><tr><th></th><th title="Jumpiness for the run pair ending at this lead">J</th><th title={statLabel(ui.statistic)}>{statLabel(ui.statistic)}</th><th>Steadier</th><th>More accurate</th></tr></thead>
      <tbody>
        {#each points.slice().sort((a, b) => a.j - b.j) as p (p.key)}
          <tr class:on={p.key === originKey}>
            <td><b style:color={pal[p.key]?.text}>{pal[p.key]?.ident}</b></td>
            <td>{fmt(p.j, 3)}</td>
            <td>{fmt(p.raw, 2)}{units}</td>
            <td class:yes={steadier(p)}>{p.key === originKey ? '·' : steadier(p) ? 'yes' : 'no'}</td>
            <td class:yes={moreAccurate(p)}>{p.key === originKey ? '·' : moreAccurate(p) ? 'yes' : 'no'}</td>
          </tr>
        {/each}
      </tbody>
    </table>
    <div class="cap">Lower J = steadier · {lowerBetter ? 'lower' : 'higher'} {statLabel(ui.statistic)} = more accurate{mode === 'absolute_lower' ? ' (magnitude)' : ''}</div>
  </section>
</div>

<style>
  .grid { display: grid; grid-template-columns: 1.45fr 1fr; gap: 18px; flex: 1; min-height: 0; }
  .col, .side { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
  .side { border-left: 1px solid var(--rule); padding-left: 16px; justify-content: center; gap: 10px; }
  .head { display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-bottom: 6px; min-width: 0; }
  .head .eyebrow { flex: 1; }
  .cap b { font-weight: 600; }
  .verdict { font-size: 15px; line-height: 1.45; color: var(--text-primary); }
  table { border-collapse: collapse; font-size: 13px; width: 100%; }
  th { text-align: right; font-weight: 600; color: var(--text-secondary); font-size: 11px; letter-spacing: .06em; text-transform: uppercase; padding: 2px 6px 6px; white-space: nowrap; }
  th:first-child { text-align: left; padding-left: 0; }
  td { text-align: right; padding: 6px 6px; font-variant-numeric: tabular-nums; border-top: 1px solid var(--rule); white-space: nowrap; color: var(--text-secondary); }
  td:first-child { text-align: left; padding-left: 0; }
  td.yes { color: #7fd6a8; font-weight: 600; }
  tr.on td { color: #fff; }
</style>
