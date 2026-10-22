<script>
  /** Dashboard header: region identity, model + period controls, export menu, close. */
  import { ui, appConfig, accuracyStatKeys } from '../state.svelte.js';
  import { MONTH_NAMES } from '../constants.js';
  import { dash, palette, regionTitle, regionKind, periodLabel, leadAxis } from './dashData.svelte.js';
  import { buildDashboardCsv, downloadText } from './dashboardCsv.js';
  import Dropdown from './Dropdown.svelte';

  let { onmodelchange, onperiodchange, onexportmap, onclose } = $props();
  const pal = $derived(palette());
  const modelItems = $derived(appConfig.models.map((m) => ({
    key: m.key, label: m.label, color: pal[m.key]?.text, sub: `leads ${m.lead_days_min ?? 1}–${m.lead_days_max ?? 14}`,
  })));
  const periodItems = [
    { key: 'yearly', label: 'Full record', sub: 'every scored date', group: 'Period' },
    { key: 'seasonal:djf', label: 'Winter', sub: 'Dec – Feb' },
    { key: 'seasonal:mam', label: 'Spring', sub: 'Mar – May' },
    { key: 'seasonal:jja', label: 'Summer', sub: 'Jun – Aug' },
    { key: 'seasonal:son', label: 'Autumn', sub: 'Sep – Nov' },
    ...Array.from({ length: 12 }, (_, i) => ({
      key: `monthly:${String(i + 1).padStart(2, '0')}`, label: MONTH_NAMES[i + 1], sub: 'accuracy only', group: i === 0 ? 'Month' : undefined,
    })),
  ];
  const periodValue = $derived(ui.period === 'seasonal' ? `seasonal:${ui.season}` : ui.period === 'monthly' ? `monthly:${ui.month}` : 'yearly');
  function pickPeriod(key) {
    const [p, sub] = key.split(':');
    ui.period = p;
    if (p === 'seasonal') ui.season = sub;
    if (p === 'monthly') ui.month = sub;
    ui.activeWindow = null;
    onperiodchange?.();
  }
  const exportItems = [
    { key: 'csv', label: 'Download CSV', sub: 'every model × lead, best model, basin series' },
    { key: 'json', label: 'Basin series JSON', sub: 'raw forecast history for this basin' },
    { key: 'map', label: 'Map image', sub: 'current overlay with the region' },
  ];
  function doExport(key) {
    if (key === 'map') { onexportmap?.(); return; }
    const stem = (ui.selectedRegion?.huc || ui.selectedRegion?.fips || ui.selectedRegion?.site || regionTitle(ui.selectedRegion)).toString().replace(/[^A-Za-z0-9._-]+/g, '-');
    if (key === 'json') {
      if (!dash.basinSeries) return;
      downloadText(`basin-series_${stem}.json`, JSON.stringify(dash.basinSeries), 'application/json');
      return;
    }
    const csv = buildDashboardCsv({
      accuracy: dash.accuracy, consistency: dash.consistency, winners: dash.winners,
      statKeys: accuracyStatKeys().filter((k) => !appConfig.consistency.stats.includes(k)), axis: leadAxis(),
      region: ui.selectedRegion, period: ui.period, month: ui.month, season: ui.season,
      threshold: ui.consistencyThreshold, scale: ui.consistencyScale,
      basinSeries: dash.basinSeries, basinHuc: dash.basinHuc,
    });
    downloadText(`forecast-analysis_${stem}_${ui.period}.csv`, csv);
  }
</script>

<header class="dash-header">
  <div class="brand">
    <span class="name">{regionTitle(ui.selectedRegion)}</span>
    <small>{regionKind(ui.selectedRegion)}{ui.selectedRegion?.type === 'huc' ? ` HUC${String(ui.selectedRegion.huc).length}` : ''} · {periodLabel()}{dash.basinInfo ? ` · ${dash.basinInfo.region} basin` : ''}</small>
  </div>
  <div class="controls">
    <Dropdown items={modelItems} value={ui.model} ariaLabel="Forecast model" title="Model driving the map overlay, tiles and the highlighted curve" width="170px"
      onselect={(k) => onmodelchange?.(k)} />
    <Dropdown items={periodItems} value={periodValue} ariaLabel="Period" title="Accumulation period for the accuracy and consistency products" width="150px"
      onselect={pickPeriod} />
    <Dropdown items={exportItems} value={null} placeholder="Export" ariaLabel="Export" compact width="96px" align="right" onselect={doExport} />
    <button type="button" class="close" title="Close the dashboard (keeps the map)" aria-label="Close dashboard" onclick={() => onclose?.()}>×</button>
  </div>
</header>

<style>
  .dash-header { height: 56px; flex: none; display: flex; align-items: center; justify-content: space-between; gap: 16px; border-bottom: 1px solid var(--panel-border); min-width: 0; }
  .brand { display: flex; flex-direction: column; min-width: 0; }
  .name { font-size: 19px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .brand small { font-size: 12px; color: var(--text-secondary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .controls { display: flex; align-items: center; gap: 8px; flex: none; }
  .close { width: 34px; height: 34px; border-radius: 8px; background: var(--surface); border: 1px solid var(--panel-border); color: var(--text-secondary); font: inherit; font-size: 20px; line-height: 1; cursor: pointer; }
  .close:hover { color: var(--text-primary); background: var(--hover-bg); }
</style>
