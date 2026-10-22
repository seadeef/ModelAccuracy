<script>
  /**
   * Consistency-by-accuracy plane (the quadrant figure, generalised):
   * every model at the inspected lead, x = jumpiness J for the run pair
   * ending at that lead (reversed: steadier to the right), y = the selected
   * accuracy statistic (better upward). The selected model is pinned as the
   * origin, so the quadrants read as "steadier / more accurate than it".
   */
  import { ui, appConfig, statLabel, isArPeriod } from '../state.svelte.js';
  import { dash, selectedLead, palette, modelLabel, statAt, statUnits } from './dashData.svelte.js';
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

  const points = $derived.by(() => appConfig.models.map((m) => {
    const j = statAt(dash.consistency?.[m.key], lead, stabKey)?.value ?? null;
    const raw = statAt(dash.accuracy?.[m.key], lead, ui.statistic)?.value ?? null;
    const acc = raw == null ? null : mode === 'absolute_lower' ? Math.abs(raw) : raw;
    return { key: m.key, label: m.label, j, raw, acc };
  }).filter((p) => p.j != null && p.acc != null));
  const origin = $derived(points.find((p) => p.key === ui.model) ?? null);
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
      ctx.font = '600 10px "DM Sans", system-ui, sans-serif';
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
      ctx.font = '600 11px "DM Sans", system-ui, sans-serif';
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
      pointRadius: points.map((p) => (p.key === ui.model ? 7 : 5.5)),
      pointHoverRadius: 8,
      pointBackgroundColor: points.map((p) => alpha(pal[p.key]?.text, 0.9)),
      pointBorderColor: points.map((p) => (p.key === ui.model ? '#fff' : alpha(pal[p.key]?.text, 1))),
      pointBorderWidth: points.map((p) => (p.key === ui.model ? 2 : 1)),
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
        x: { type: 'linear', reverse: true, min: Math.max(0, jx0), max: Math.min(1, jx1), grid: { color: '#ffffff0b' }, border: { display: false }, ticks: { font: { size: 10.5 }, maxTicksLimit: 6, includeBounds: false }, title: { display: true, text: `Jumpiness J, runs at lead ${lead + 1} → ${lead} · steadier →`, color: '#919daf', font: { size: 10 } } },
        y: { type: 'linear', reverse: lowerBetter, min: ay0, max: ay1, grid: { color: '#ffffff0b' }, border: { display: false }, ticks: { font: { size: 10.5 }, maxTicksLimit: 5, includeBounds: false }, title: { display: true, text: `${yLabel} · more accurate ↑`, color: '#919daf', font: { size: 10 } } },
      },
    };
    return { type: 'scatter', data: { datasets }, options: opts, plugins: [decor] };
  });

  const beats = $derived(points.filter(better));
  const verdict = $derived.by(() => {
    if (!origin) return `${modelLabel(ui.model)} has no value at lead day ${lead} on this slice.`;
    const o = pal[ui.model]?.ident ?? origin.label;
    if (!beats.length) return `No model is both steadier and more accurate than ${o} at lead day ${lead}.`;
    const names = beats.map((p) => pal[p.key]?.ident ?? p.label);
    const list = names.length > 1 ? `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}` : names[0];
    const verb = names.length === 1 ? 'is' : names.length === 2 ? 'are both' : 'are all';
    return `${list} ${verb} steadier and more accurate than ${o} at lead day ${lead}.`;
  });
</script>

<div class="grid">
  <section class="col">
    <div class="h">Steadiness against accuracy at lead day {lead} <small>every model on the region's pooled curves · x = J for the run pair ending at this lead ({ui.consistencyThreshold} mm/d, {ui.consistencyScale} km) · y = {statLabel(ui.statistic)} at this lead · origin = <span style:color={pal[ui.model]?.text}>{modelLabel(ui.model)}</span></small></div>
    <div class="plot">
      {#if !supported}
        <div class="empty">Consistency is computed for the full record and seasons — pick one in the period menu</div>
      {:else if points.length >= 2}
        <ChartCanvas {config} />
      {:else}
        <div class="empty">{dash.consistencyLoading || dash.accuracyLoading ? 'Loading curves…' : `Not enough models score at lead day ${lead}`}</div>
      {/if}
    </div>
  </section>
  <section class="side">
    <div class="h">Relative to <span style:color={pal[ui.model]?.text}>{modelLabel(ui.model)}</span> <small>{verdict}</small></div>
    <table>
      <thead><tr><th></th><th>J</th><th>{statLabel(ui.statistic)}</th><th>steadier</th><th>more accurate</th></tr></thead>
      <tbody>
        {#each points.slice().sort((a, b) => a.j - b.j) as p (p.key)}
          <tr class:on={p.key === ui.model}>
            <td><b style:color={pal[p.key]?.text}>{pal[p.key]?.ident}</b></td>
            <td>{fmt(p.j, 3)}</td>
            <td>{fmt(p.raw, 2)}{units}</td>
            <td class:yes={steadier(p)}>{p.key === ui.model ? '·' : steadier(p) ? 'yes' : 'no'}</td>
            <td class:yes={moreAccurate(p)}>{p.key === ui.model ? '·' : moreAccurate(p) ? 'yes' : 'no'}</td>
          </tr>
        {/each}
      </tbody>
    </table>
    <div class="foot">Lower J = steadier run-to-run; {lowerBetter ? `lower ${statLabel(ui.statistic)}` : `higher ${statLabel(ui.statistic)}`} = more accurate{mode === 'absolute_lower' ? ' (magnitude)' : ''}. Switch the lead on the lead bar, the statistic on the tiles, and the origin model in the header. Pooled climatology, no uncertainty bands.</div>
  </section>
</div>

<style>
  .grid { display: grid; grid-template-columns: 1.4fr 1fr; gap: 14px; flex: 1; min-height: 0; }
  .col, .side { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
  .side { border-left: 1px solid var(--panel-border); padding-left: 13px; }
  .h { font-size: 12.5px; font-weight: 600; color: #d5dae2; margin-bottom: 4px; }
  .h small { display: block; font-weight: 400; color: var(--text-secondary); font-size: 10px; line-height: 1.3; }
  .plot { position: relative; flex: 1; min-height: 70px; }
  .empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--text-secondary); font-size: 12px; text-align: center; padding: 0 10%; }
  table { border-collapse: collapse; font-size: 11.5px; width: 100%; margin-top: 4px; }
  th { text-align: right; font-weight: 500; color: var(--text-secondary); font-size: 10px; padding: 1px 4px; white-space: nowrap; }
  th:first-child { text-align: left; }
  td { text-align: right; padding: 2px 4px; font-variant-numeric: tabular-nums; border-top: 1px solid #ffffff08; white-space: nowrap; color: var(--text-secondary); }
  td:first-child { text-align: left; }
  td.yes { color: #7fd6a8; font-weight: 600; }
  tr.on td { color: #fff; }
  .foot { font-size: 10px; color: var(--text-secondary); margin-top: auto; padding-top: 6px; line-height: 1.35; }
</style>
