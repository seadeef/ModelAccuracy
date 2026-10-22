<script>
  /**
   * Bottom pane: the storm analyses as tabs. The storm picker in the strip
   * is the one context every basin tab shares (revision range, storm
   * highlight, the "this storm" date population); the day picker only
   * matters for the revision tab and so only shows there.
   */
  import { ui } from '../state.svelte.js';
  import { dash, palette } from './dashData.svelte.js';
  import { datesInRange } from '../basinAnalysis.js';
  import { isoToYmd, ymdToIso } from '../bootstrap.js';
  import { stormItems, applyStormRange, fmtRange } from './basinScope.svelte.js';
  import { fmt, fmtDate } from './chartSetup.js';
  import Dropdown from './Dropdown.svelte';
  import RevisionPane from './RevisionPane.svelte';
  import StormsPane from './StormsPane.svelte';
  import EfficiencyPane from './EfficiencyPane.svelte';
  import HeavyRainPane from './HeavyRainPane.svelte';
  import QuadrantPane from './QuadrantPane.svelte';
  import ForecastPane from './ForecastPane.svelte';
  import TimelinePane from './TimelinePane.svelte';

  const TABS = [
    { key: 'revision', label: 'Forecast revision', basin: true, title: 'How each run changed its forecast for one storm day, and the raw forecast path against the observation' },
    { key: 'storms', label: 'Storms', basin: true, title: 'Catalogued atmospheric-river storms that touched this basin, with each storm\'s steadiness and accuracy ledger' },
    { key: 'heavyrain', label: 'Heavy rain', basin: true, title: 'Precision, recall and frequency bias of each model\'s heavy-rain calls on the chosen days' },
    { key: 'efficiency', label: 'Efficiency', basin: true, title: 'Revision efficiency R: revision variance against the skill it bought' },
    { key: 'quadrant', label: 'Steady vs accurate', title: 'Every model on consistency and accuracy at the inspected lead, relative to the selected model' },
    { key: 'forecast', label: 'Forecast', title: 'Latest run on file, by model and lead day' },
    { key: 'timeline', label: 'AR timeline', ar: true, title: 'Atmospheric-river footprint scrubber and date-range drill' },
  ];
  const pal = $derived(palette());
  const basinReady = $derived(!!dash.basinSeries);
  const basinTabsOk = $derived(basinReady || dash.basinLoading);
  const visibleTabs = $derived(TABS.filter((t) => (!t.basin || basinTabsOk) && (!t.ar || ui.mode === 'atmospheric-rivers')));
  const tab = $derived(visibleTabs.some((t) => t.key === ui.dashTab) ? ui.dashTab : (visibleTabs[0]?.key ?? 'quadrant'));
  const showStorm = $derived(basinReady && ['revision', 'storms', 'efficiency', 'heavyrain'].includes(tab));
  const showDay = $derived(basinReady && tab === 'revision');

  const items = $derived(stormItems());
  const stormValue = $derived.by(() => {
    const hit = items.find((i) => i.start === ui.arRangeStart && i.end === ui.arRangeEnd);
    return hit?.key ?? 'custom';
  });
  const stormShown = $derived(stormValue === 'custom' && ui.arRangeStart
    ? [{ key: 'custom', label: fmtRange(), sub: 'from the timeline' }, ...items] : items);
  function pickStorm(key) { applyStormRange(items.find((i) => i.key === key)); }
  function hoverStorm(key) { ui.arHoverEpisodeId = key ? items.find((i) => i.key === key)?.episodeId ?? null : null; }

  const rev = $derived(dash.basinSeries?.[ui.model] ?? Object.values(dash.basinSeries ?? {})[0] ?? null);
  const dayItems = $derived.by(() => {
    if (!rev) return [];
    const range = [isoToYmd(ui.arRangeStart), isoToYmd(ui.arRangeEnd)];
    return datesInRange(rev, range).map((d) => ({
      key: rev.dates[d],
      label: fmtDate(rev.dates[d]),
      sub: rev.observed[d] != null ? `${fmt(rev.observed[d], 1)} mm observed` : 'no observation',
      color: rev.observed[d] != null && rev.observed[d] >= ui.eventThresholdMm ? '#7fd6a8' : undefined,
    }));
  });
  /** The basin behind the storm panes, when it is wider than the drawn region (e.g. a HUC8 inside it). */
  const basinNote = $derived(dash.basinInfo && ui.selectedRegion?.type === 'huc' && String(ui.selectedRegion.huc).length > 6 ? dash.basinInfo.name : null);
</script>

<article class="pane">
  <div class="strip">
    <div class="tabs">
    {#each visibleTabs as t (t.key)}
      <button
        type="button"
        class="tab"
        class:active={tab === t.key}
        class:ar={t.ar}
        title={t.title}
        onclick={() => { ui.dashTab = t.key; }}
      >{t.label}</button>
    {/each}
    </div>
    <div class="context">
      {#if basinNote}<span class="cap" title="Storm analyses use the HUC6 basin's forecast history">{basinNote} basin</span>{/if}
      {#if showStorm}
        <span class="cap">Storm</span>
        <Dropdown items={stormShown} value={stormValue} ariaLabel="Storm or date range" title="Storm, AR episode or the full record. Sets the revision day, the highlighted storm and the days behind the storm population." compact width="196px" align="right"
          onselect={pickStorm} onhover={hoverStorm} />
      {/if}
      {#if showDay}
        <span class="cap">Day</span>
        <Dropdown items={dayItems} value={isoToYmd(ui.arTargetDate)} ariaLabel="Storm day" title="The day whose forecasts are traced (green = observed at or above the heavy-rain threshold)" compact width="98px" align="right"
          onselect={(k) => { ui.arTargetDate = ymdToIso(k); }} />
      {/if}
      {#if !basinTabsOk && ui.selectedRegion}
        <span class="cap" title="The per-basin forecast history covers the HUC6 river basins of California, the Pacific Northwest and the Upper Colorado">Storm analyses: pick a river basin</span>
      {/if}
    </div>
  </div>
  <div class="tab-body">
    {#if tab === 'revision'}<RevisionPane />
    {:else if tab === 'storms'}<StormsPane />
    {:else if tab === 'efficiency'}<EfficiencyPane />
    {:else if tab === 'heavyrain'}<HeavyRainPane />
    {:else if tab === 'quadrant'}<QuadrantPane />
    {:else if tab === 'forecast'}<ForecastPane />
    {:else if tab === 'timeline'}<TimelinePane />
    {/if}
  </div>
</article>

<style>
  .pane { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
  .strip { display: flex; align-items: center; gap: 2px; padding: 4px 10px 0 10px; border-bottom: 1px solid var(--rule); flex: none; min-width: 0; }
  .tabs { display: flex; align-items: center; gap: 2px; flex: 1; min-width: 0; overflow-x: auto; scrollbar-width: none; }
  .tab { font: inherit; font-size: 13px; font-weight: 600; padding: 9px 9px 10px; background: none; border: 0; border-bottom: 2px solid transparent; color: var(--text-secondary); cursor: pointer; white-space: nowrap; margin-bottom: -1px; }
  .tab:hover { color: var(--text-primary); }
  .tab.active { color: var(--text-primary); border-bottom-color: var(--accent); }
  .tab.ar.active { border-bottom-color: #b9a6ff; }
  .context { flex: none; display: flex; align-items: center; gap: 6px; padding: 0 2px 4px 10px; }
  .context .cap { margin-left: 6px; }
  .tab-body { flex: 1; min-height: 0; padding: 10px 16px 12px; display: flex; flex-direction: column; }
  @media (max-width: 1250px) { .context .cap { display: none; } }
</style>
