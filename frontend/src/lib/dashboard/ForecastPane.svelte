<script>
  /** Current forecast by lead for every model (precip / wind / humidity / temp), from the live forecast bins. */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, selectedLead, leadAxis, modelLabel } from './dashData.svelte.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, endLabelPlugin, fmt, alpha } from './chartSetup.js';

  const VARS = [
    { key: 'precip', label: 'Precip', color: '#4a90d9' },
    { key: 'wind', label: 'Wind', color: '#2bb3a3' },
    { key: 'humidity', label: 'Humidity', color: '#5fae46' },
    { key: 'temp', label: 'Temp', color: '#e8743b' },
  ];
  const pal = $derived(palette());
  const axis = $derived(leadAxis());
  const lead = $derived(selectedLead());
  const payload = $derived(dash.forecastByVar?.[ui.forecastVariable] ?? null);
  const available = $derived(VARS.filter((v) => dash.forecastByVar?.[v.key]));
  const cursor = cursorPlugin(() => selectedLead() - 1);
  const ends = endLabelPlugin((ds) => ds.ident);
  /** Models with a run on file for this variable. */
  const present = $derived(appConfig.models.filter((m) => payload?.models?.[m.key] && Object.values(payload.models[m.key].leads ?? {}).some((e) => e && !e.no_data)));
  const initDate = $derived(payload?.forecast_calendar?.per_model?.[ui.model]?.initDate ?? '');
  const units = $derived.by(() => {
    const m = payload?.models?.[ui.model] ?? Object.values(payload?.models ?? {})[0];
    return Object.values(m?.leads ?? {})[0]?.units ?? '';
  });
  function validDate(modelKey, l) {
    const cal = payload?.forecast_calendar?.per_model?.[modelKey];
    if (!cal?.initDate) return '';
    const d = new Date(`${cal.initDate}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() + l);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  }
  const config = $derived.by(() => {
    const datasets = present.map((m) => ({
      label: m.label, ident: pal[m.key]?.ident,
      data: axis.map((l) => { const e = payload?.models?.[m.key]?.leads?.[String(l)]; return e && !e.no_data ? e.value : null; }),
      borderColor: m.key === ui.model ? pal[m.key]?.text : alpha(pal[m.key]?.text, 0.8), endLabelColor: pal[m.key]?.text, borderWidth: m.key === ui.model ? 2.8 : 1.5, spanGaps: false, order: m.key === ui.model ? 0 : 1,
    }));
    const opts = leadAxisOptions({ yTitle: units, xTitle: 'Lead day', endLabels: true, tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 1)} ${units}` });
    opts.plugins.tooltip.callbacks.title = (items) => `Lead day ${items[0]?.label}${validDate(ui.model, Number(items[0]?.label)) ? ` · valid ${validDate(ui.model, Number(items[0]?.label))}` : ''}`;
    return { type: 'line', data: { labels: axis, datasets }, options: opts, plugins: [cursor, ends] };
  });
</script>

<div class="wrap">
  <div class="head">
    <div class="eyebrow" title="Region-mean value of the most recent run on file, for each model and lead day">Latest run on file{initDate ? ` · ${modelLabel(ui.model)} initialised ${initDate}` : ''}</div>
    <span class="chips">
      {#each VARS as v}
        <button type="button" class="chip" class:active={ui.forecastVariable === v.key} disabled={!dash.forecastByVar?.[v.key]} onclick={() => { ui.forecastVariable = v.key; }}>{v.label}</button>
      {/each}
    </span>
  </div>
  <div class="plot">
    {#if payload}<ChartCanvas {config} />{:else}<div class="plot-empty">{dash.forecastByVar === null ? 'Loading forecast…' : 'No forecast on file for this region'}</div>{/if}
  </div>
  <div class="legend">
    {#each present as m}
      {@const e = payload?.models?.[m.key]?.leads?.[String(lead)]}
      <span><i style:background={pal[m.key]?.text}></i>{m.label} <b>{e && !e.no_data ? `${fmt(e.value, 1)} ${units}` : '—'}</b></span>
    {/each}
    <span class="note">at lead day {lead}{validDate(ui.model, lead) ? ` · valid ${validDate(ui.model, lead)}` : ''}</span>
  </div>
</div>

<style>
  .wrap { display: flex; flex-direction: column; flex: 1; min-height: 0; }
  .head { display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-bottom: 6px; }
  .legend { display: flex; gap: 16px; align-items: center; margin-top: 4px; color: #bec6d2; font-size: 12.5px; flex-wrap: wrap; }
  .legend span { display: flex; align-items: center; gap: 6px; }
  .legend i { width: 14px; height: 3px; border-radius: 2px; }
  .legend b { color: var(--text-primary); font-weight: 600; }
  .note { margin-left: auto; color: var(--text-secondary); }
</style>
