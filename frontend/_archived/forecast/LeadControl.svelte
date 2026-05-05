<script>
  import { appConfig, ui } from '../state.svelte.js';
  import { getModelLeadBounds } from '../tile.js';
  import { forecastLeadSliderLabel, forecastWindowChipLabel } from '../forecastDates.js';

  let { onsliderinput, onwindowselect } = $props();

  const leadBounds = $derived(getModelLeadBounds(appConfig.models, ui.model));
  const sliderStep = $derived(ui.statistic === 'forecast' ? '1' : '0.1');
  const sliderLabel = $derived(
    ui.mode === 'forecast'
      ? forecastLeadSliderLabel(ui.model, ui.leadFractional, appConfig.forecastInitDate)
      : ui.leadFractional === Math.floor(ui.leadFractional)
        ? `Day ${ui.leadFractional}`
        : ui.leadFractional.toFixed(1)
  );

  const modelConfig = $derived(appConfig.models.find(m => m.key === ui.model));
  const leadWindows = $derived(modelConfig?.lead_windows || []);

  function chipLabel(start, end) {
    if (ui.mode !== 'forecast') return `${start}\u2013${end}`;
    return forecastWindowChipLabel(ui.model, start, end, appConfig.forecastInitDate);
  }

  function handleSliderInput(e) {
    const frac = parseFloat(e.target.value);
    ui.leadFractional = frac;
    if (ui.activeWindow) ui.activeWindow = null;
    onsliderinput?.(frac);
  }

  function handleWindowClick(key) {
    if (ui.activeWindow === key) {
      ui.activeWindow = null;
      onwindowselect?.(null);
    } else {
      ui.activeWindow = key;
      onwindowselect?.(key);
    }
  }
</script>

<div class="lead-group">
  <span class="lead-value">
    {#if ui.activeWindow}
      {@const parts = ui.activeWindow.split('_')}
      {#if ui.mode === 'forecast'}
        {forecastWindowChipLabel(ui.model, Number(parts[0]), Number(parts[1]), appConfig.forecastInitDate)}
      {:else}
        {parts[0]}&ndash;{parts[1]}d
      {/if}
    {:else}
      {sliderLabel}
    {/if}
  </span>
  <input
    type="range"
    class="lead-slider"
    min={leadBounds.min}
    max={leadBounds.max}
    step={sliderStep}
    value={ui.leadFractional}
    oninput={handleSliderInput}
  />
  {#if leadWindows.length > 0}
    <div class="window-chips">
      {#each leadWindows as [start, end]}
        {@const key = `${start}_${end}`}
        <button
          class="chip"
          class:active={ui.activeWindow === key}
          onclick={() => handleWindowClick(key)}
        >{chipLabel(start, end)}</button>
      {/each}
    </div>
  {/if}
</div>

<style>
  .lead-group {
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .lead-value {
    font-size: 12px;
    font-weight: 500;
    color: var(--text-secondary);
    min-width: max(34px, max-content);
    white-space: nowrap;
  }
  .lead-slider {
    width: 100px;
    accent-color: var(--accent);
    height: 3px;
  }
  .window-chips {
    display: flex;
    gap: 3px;
  }
  .chip {
    font-size: 11px;
    font-family: inherit;
    padding: 2px 7px;
    border-radius: 8px;
    border: 1px solid var(--panel-border);
    background: transparent;
    color: var(--text-secondary);
    cursor: pointer;
    white-space: nowrap;
    transition: all 0.15s;
  }
  .chip:hover {
    background: var(--hover-bg);
    border-color: rgba(255,255,255,0.12);
  }
  .chip.active {
    background: var(--accent-glow);
    color: var(--accent);
    border-color: rgba(110,181,255,0.3);
  }
</style>
