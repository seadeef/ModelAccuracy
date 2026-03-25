<script>
  import { appConfig, ui, statLabel, accuracyStatKeys } from '../state.svelte.js';
  import LeadControl from './LeadControl.svelte';
  import PeriodMenu from './PeriodMenu.svelte';

  let { onmodelchange, onstatchange, onperiodchange, onsliderinput, onwindowselect, onmodechange } = $props();

  const accuracyStats = $derived(accuracyStatKeys());

  function handleModelChange(e) {
    ui.model = e.target.value;
    ui.activeWindow = null;
    onmodelchange?.();
  }

  function handleStatChange(e) {
    ui.statistic = e.target.value;
    ui.activeWindow = null;
    onstatchange?.();
  }

  function switchMode(mode) {
    if (ui.mode === mode) return;
    ui.mode = mode;
    ui.selectedRegion = null;
    ui.activeTool = null;
    ui.activeWindow = null;

    if (mode === 'forecast') {
      ui.statistic = 'forecast';
      ui.period = 'yearly';
    } else {
      // Default to first accuracy stat
      ui.statistic = accuracyStats[0] || 'bias';
    }
    onmodechange?.();
  }

  function handleExport() {
    /* Map PNG export lives in the active app (client-side canvas + /static/ranges). */
  }
</script>

<div class="controls-row">
  <div class="mode-toggle">
    <button
      class="mode-btn"
      class:active={ui.mode === 'forecast'}
      onclick={() => switchMode('forecast')}
    >Forecast</button>
    <button
      class="mode-btn"
      class:active={ui.mode === 'accuracy'}
      onclick={() => switchMode('accuracy')}
    >Accuracy</button>
  </div>

  <label>Model
    <select value={ui.model} onchange={handleModelChange}>
      {#each appConfig.models as m}
        <option value={m.key}>{m.label}</option>
      {/each}
    </select>
  </label>

  {#if ui.mode === 'accuracy'}
    <label>Statistic
      <select value={ui.statistic} onchange={handleStatChange}>
        {#each accuracyStats as statName}
          <option value={statName}>{statLabel(statName)}</option>
        {/each}
      </select>
    </label>
  {/if}

  <LeadControl {onsliderinput} {onwindowselect} />

  {#if ui.mode === 'accuracy'}
    <PeriodMenu onchange={onperiodchange} />
  {/if}

  <button class="export-btn" type="button" title="Save the current map as a PNG with title and legend" onclick={handleExport}>
    Export Image
  </button>

  <div class="status">{ui.statusMessage}</div>
</div>

<style>
  .controls-row {
    display: flex;
    gap: 16px;
    align-items: center;
    padding: 8px 16px;
  }
  .mode-toggle {
    display: flex;
    background: #2a2d35;
    border-radius: 6px;
    overflow: hidden;
    border: 1px solid #3a3d45;
  }
  .mode-btn {
    padding: 5px 14px;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    border: none;
    background: transparent;
    color: var(--text-secondary);
    transition: all 0.15s;
  }
  .mode-btn.active {
    background: rgba(138, 180, 248, 0.15);
    color: var(--accent);
  }
  .mode-btn:hover:not(.active) {
    color: var(--text-primary);
  }
  .controls-row label {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    color: var(--text-secondary);
    white-space: nowrap;
  }
  .controls-row select {
    background: #2a2d35;
    color: var(--text-primary);
    border: 1px solid #3a3d45;
    border-radius: 4px;
    padding: 4px 8px;
    font-size: 13px;
    outline: none;
  }
  .controls-row select:focus {
    border-color: var(--accent);
  }
  .export-btn {
    background: #2a2d35;
    color: var(--text-primary);
    border: 1px solid #3a3d45;
    border-radius: 4px;
    padding: 4px 12px;
    font-size: 13px;
    cursor: pointer;
  }
  .export-btn:hover {
    background: #3a3d45;
  }
  .status {
    margin-left: auto;
    font-size: 11px;
    color: var(--text-secondary);
  }
</style>
