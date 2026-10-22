<script>
  /** Bottom pane: the revision view plus the basin analyses as tabs. */
  import { ui } from '../state.svelte.js';
  import { dash } from './dashData.svelte.js';
  import { SCOPES } from '../basinAnalysis.js';
  import RevisionPane from './RevisionPane.svelte';
  import StormsPane from './StormsPane.svelte';
  import EfficiencyPane from './EfficiencyPane.svelte';
  import HeavyRainPane from './HeavyRainPane.svelte';
  import QuadrantPane from './QuadrantPane.svelte';
  import ForecastPane from './ForecastPane.svelte';
  import TimelinePane from './TimelinePane.svelte';

  const TABS = [
    { key: 'revision', label: 'Forecast revision', title: 'How each run changed the forecast for one day, and the raw forecast path against the observation' },
    { key: 'storms', label: 'Storms', title: 'Catalogued atmospheric-river storms for this region, with each storm\'s steadiness and accuracy ledger' },
    { key: 'efficiency', label: 'Efficiency', title: 'Revision efficiency R: revision variance against the skill it bought' },
    { key: 'heavyrain', label: 'Heavy rain', title: 'Trigger sweep: precision, recall and frequency bias for heavy-rain days' },
    { key: 'quadrant', label: 'Steady vs accurate', title: 'Every model on consistency and accuracy at the inspected lead, relative to the selected model' },
    { key: 'forecast', label: 'Forecast', title: 'Current forecast by model and lead' },
    { key: 'timeline', label: 'AR timeline', ar: true, title: 'Atmospheric-river footprint scrubber and date-range drill' },
  ];
  const basinReady = $derived(!!dash.basinSeries);
  const visibleTabs = $derived(TABS.filter((t) => !t.ar || ui.mode === 'atmospheric-rivers'));
  const tab = $derived(visibleTabs.some((t) => t.key === ui.dashTab) ? ui.dashTab : 'revision');
  const showScope = $derived(['efficiency', 'heavyrain'].includes(tab) && basinReady);
</script>

<article class="pane">
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
    {#if showScope}
      <div class="scope" title="Date population for this analysis. AR / non-AR membership comes from the basin's tARget episode catalog; intervals resample AR episode clusters and 10-day pseudo-clusters.">
        <span>Dates</span>
        {#each SCOPES as s}
          <button type="button" class="scope-btn" class:active={ui.analysisScope === s.key} title={s.title} onclick={() => { ui.analysisScope = s.key; }}>{s.label}</button>
        {/each}
      </div>
    {/if}
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
  .pane { display: flex; flex-direction: column; min-width: 0; min-height: 0; background: var(--pane-bg); border: 1px solid var(--panel-border); border-radius: 10px; }
  .tabs { display: flex; align-items: center; gap: 2px; padding: 6px 8px 0; border-bottom: 1px solid var(--panel-border); flex: none; overflow-x: auto; scrollbar-width: none; }
  .tab { font: inherit; font-size: 12px; font-weight: 600; padding: 5px 10px 7px; background: none; border: 0; border-bottom: 2px solid transparent; color: var(--text-secondary); cursor: pointer; white-space: nowrap; margin-bottom: -1px; }
  .tab:hover { color: var(--text-primary); }
  .tab.active { color: var(--text-primary); border-bottom-color: var(--accent); }
  .tab.ar.active { border-bottom-color: #b9a6ff; }
  .scope { margin-left: auto; display: flex; align-items: center; gap: 3px; padding: 0 4px 5px 10px; font-size: 10.5px; color: var(--text-secondary); white-space: nowrap; }
  .scope span { margin-right: 4px; }
  .scope-btn { font: inherit; font-size: 10.5px; font-weight: 600; padding: 2px 7px; background: #ffffff06; border: 1px solid var(--panel-border); border-radius: 5px; color: var(--text-secondary); cursor: pointer; }
  .scope-btn.active { background: #9d7bff33; border-color: #9d7bff; color: #e8e0ff; }
  .tab-body { flex: 1; min-height: 0; padding: 7px 11px 9px; display: flex; flex-direction: column; }
</style>
