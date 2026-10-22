<script>
  /** Current forecast by lead for every model (precip / wind / humidity / temp), from the live forecast bins. */
  import { ui, appConfig } from '../state.svelte.js';
  import { dash, palette, selectedLead, leadAxis } from './dashData.svelte.js';
  import ChartCanvas from './ChartCanvas.svelte';
  import { leadAxisOptions, cursorPlugin, fmt } from './chartSetup.js';

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
    const datasets = appConfig.models.map((m) => ({
      label: m.label,
      data: axis.map((l) => { const e = payload?.models?.[m.key]?.leads?.[String(l)]; return e && !e.no_data ? e.value : null; }),
      borderColor: pal[m.key]?.text, borderWidth: m.key === ui.model ? 2.6 : 1.4, spanGaps: false,
    }));
    const opts = leadAxisOptions({ yTitle: units, tooltipLabel: (c) => ` ${c.dataset.label}: ${fmt(c.parsed.y, 1)} ${units}` });
    opts.plugins.tooltip.callbacks.title = (items) => `Lead day ${items[0]?.label}${validDate(ui.model, Number(items[0]?.label)) ? ` · valid ${validDate(ui.model, Number(items[0]?.label))}` : ''}`;
    return { type: 'line', data: { labels: axis, datasets }, options: opts, plugins: [cursor] };
  });
</script>

<div class="wrap">
  <div class="head">
    <div class="h">Current forecast <small>region-mean value of the latest run for each model and lead day{payload?.forecast_calendar?.per_model?.[ui.model]?.initDate ? ` · ${ui.model.toUpperCase()} initialised ${payload.forecast_calendar.per_model[ui.model].initDate}` : ''}</small></div>
    <div class="chips">
      {#each VARS as v}
        <button type="button" class:active={ui.forecastVariable === v.key} disabled={!dash.forecastByVar?.[v.key]} style:--c={v.color} onclick={() => { ui.forecastVariable = v.key; }}>{v.label}</button>
      {/each}
    </div>
  </div>
  <div class="plot">
    {#if payload}<ChartCanvas {config} />{:else}<div class="empty">{dash.forecastByVar === null ? 'Loading forecast…' : 'No forecast available for this region'}</div>{/if}
  </div>
  <div class="model-key">
    {#each appConfig.models as m}
      {@const e = payload?.models?.[m.key]?.leads?.[String(lead)]}
      <span><i style:background={pal[m.key]?.text}></i>{m.label} <b>{e && !e.no_data ? `${fmt(e.value, 1)} ${units}` : '—'}</b></span>
    {/each}
    <span class="note">at lead day {lead}{validDate(ui.model, lead) ? ` · valid ${validDate(ui.model, lead)}` : ''}</span>
  </div>
</div>

<style>
  .wrap { display: flex; flex-direction: column; flex: 1; min-height: 0; }
  .head { display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; }
  .h { font-size: 12.5px; font-weight: 600; color: #d5dae2; margin-bottom: 4px; }
  .h small { display: block; font-weight: 400; color: var(--text-secondary); font-size: 10px; }
  .chips { display: flex; gap: 4px; }
  .chips button { font: inherit; font-size: 11px; font-weight: 600; padding: 2px 8px; background: #ffffff06; border: 1px solid var(--panel-border); border-radius: 5px; color: var(--text-secondary); cursor: pointer; }
  .chips button.active { color: #fff; border-color: var(--c); background: color-mix(in srgb, var(--c) 30%, transparent); }
  .chips button:disabled { opacity: .4; cursor: default; }
  .plot { position: relative; flex: 1; min-height: 70px; }
  .empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--text-secondary); font-size: 12px; }
  .model-key { display: flex; gap: 14px; align-items: center; margin-top: 4px; color: #bec6d2; font-size: 11.5px; flex-wrap: wrap; }
  .model-key span { display: flex; align-items: center; gap: 5px; }
  .model-key i { width: 14px; height: 3px; border-radius: 2px; }
  .model-key b { color: var(--text-primary); font-weight: 600; }
  .note { margin-left: auto; color: var(--text-secondary); }
</style>
