<script>
  import { ui, appConfig } from './lib/state.svelte.js';
  import { drawToolGlyph } from './lib/appIcons.js';
  import { fetchZip } from './lib/api.js';
  import { getModelLeadBounds, tileUrl } from './lib/tile.js';
  import { exportMapImage } from './lib/exportMapImage.js';
  import MapView from './lib/components/MapView.svelte';
  import MapToolbar from './lib/components/MapToolbar.svelte';
  import Panel from './lib/components/Panel.svelte';
  import DrawTools from './lib/components/DrawTools.svelte';
  let mapView;

  let zipValue = $state('');

  function handleModelChange(e) {
    ui.model = e.target.value;
    ui.activeWindow = null;
    const { min, max } = getModelLeadBounds(appConfig.models, ui.model);
    if (ui.leadFractional > max) {
      ui.leadFractional = max;
    } else if (ui.leadFractional < min) {
      ui.leadFractional = min;
    }
    mapView?.onModelChange();
  }

  function handleStatChange(e) {
    ui.statistic = e.target.value;
    ui.activeWindow = null;
    mapView?.onStatisticChange();
  }

  function handlePeriodChange() {
    mapView?.onPeriodChange();
  }

  function handleOpacityInput(e) {
    ui.weatherOpacity = Number(e.target.value);
  }

  async function handleExport() {
    const lead = ui.activeWindow || String(Math.round(ui.leadFractional));
    const overlayUrl = mapView?.getCurrentOverlayUrl?.() ?? '';
    const fallbackTileUrl = tileUrl(
      ui.model,
      ui.statistic,
      lead,
      ui.period,
      ui.month,
      ui.season,
    );
    ui.statusMessage = 'Preparing download…';
    try {
      await exportMapImage({
        overlayUrl,
        fallbackTileUrl,
        model: ui.model,
        statistic: ui.statistic,
        lead,
        period: ui.period,
        month: ui.month,
        season: ui.season,
        models: appConfig.models,
        statisticsMeta: appConfig.statistics,
      });
      ui.statusMessage = 'Idle';
    } catch {
      ui.statusMessage = 'Map download failed';
    }
  }

  async function goToZip() {
    const raw = zipValue.trim();
    if (!raw) return;
    ui.statusMessage = `Looking up ${raw}...`;
    try {
      const data = await fetchZip(raw);
      if (!data.found) { ui.statusMessage = `ZIP not found: ${raw}`; return; }
      mapView?.flyToZip(data);
      ui.statusMessage = `Centered on ${data.zip}`;
    } catch { ui.statusMessage = 'ZIP lookup error'; }
  }

  /** Show even when a region is selected so the panel can stay open underneath. */
  const showGuide = $derived(
    ui.activeTool === 'point' && !ui.hasUsedPinTool,
  );
</script>

<div class="app">
  <div class="map-fill">
    <MapView bind:this={mapView} />

    <MapToolbar bind:zipValue onOpacityInput={handleOpacityInput} onExport={handleExport} goToZip={goToZip} />

    {#if ui.statusMessage && ui.statusMessage !== 'Idle'}
      <div class="status-pill">{ui.statusMessage}</div>
    {/if}

    <DrawTools />

    {#if showGuide}
      <div class="draw-guide" class:draw-guide--with-panel={!!ui.selectedRegion}>
        {#if !ui.selectedRegion}
          <div class="guide-icon">
            <svg width="28" height="28" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
              {@html drawToolGlyph.point}
            </svg>
          </div>
        {/if}
        <div class="guide-text">Click anywhere to analyze</div>
        {#if !ui.hasUsedAreaDrawTool}
          <div class="guide-hint">Or draw an area with the tools on the left</div>
        {/if}
      </div>
    {/if}

    <Panel
      onleadchange={(frac) => mapView?.onLeadSliderInput(frac)}
      onstatchange={handleStatChange}
      onperiodchange={handlePeriodChange}
      onmodelchange={handleModelChange}
    />

    <footer class="app-credit">Kevin Toren 2026</footer>
  </div>
</div>

<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&display=swap');

  :root {
    color-scheme: dark;
    --panel-bg: rgba(14, 17, 23, 0.88);
    --panel-solid: #0e1117;
    --panel-border: rgba(255,255,255,0.08);
    --text-primary: #e2e5ea;
    --text-secondary: #7a818c;
    --accent: #6eb5ff;
    --accent-glow: rgba(110, 181, 255, 0.12);
    --hover-bg: rgba(255,255,255,0.05);
    --surface: rgba(255,255,255,0.04);
    --radius: 10px;
    --fs-base: 15px;
  }
  :global(*) { box-sizing: border-box; }
  :global(body) {
    margin: 0;
    font-family: 'DM Sans', system-ui, -apple-system, sans-serif;
    font-size: var(--fs-base);
    background: #000;
    color: var(--text-primary);
    -webkit-font-smoothing: antialiased;
  }
  .app {
    height: 100vh;
    display: flex;
  }
  .map-fill {
    flex: 1;
    position: relative;
    min-height: 0;
  }

  .app-credit {
    position: absolute;
    top: 14px;
    right: 14px;
    z-index: 11;
    margin: 0;
    padding: 8px 14px;
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 0.04em;
    color: var(--text-primary);
    background: var(--panel-bg);
    backdrop-filter: blur(16px) saturate(1.2);
    -webkit-backdrop-filter: blur(16px) saturate(1.2);
    border: 1px solid var(--panel-border);
    border-radius: 12px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.35);
    pointer-events: none;
  }

  .status-pill {
    position: absolute;
    bottom: 20px;
    left: 50%;
    transform: translateX(-50%);
    z-index: 8;
    padding: 6px 14px;
    font-size: 12px;
    color: var(--text-secondary);
    background: var(--panel-bg);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border: 1px solid var(--panel-border);
    border-radius: 20px;
    white-space: nowrap;
    pointer-events: none;
    animation: fadeIn 0.3s;
  }
  @keyframes fadeIn {
    from { opacity: 0; transform: translateX(-50%) translateY(4px); }
    to { opacity: 1; transform: translateX(-50%) translateY(0); }
  }

  .draw-guide {
    /* Match MapView .draw-hint; centered in viewport */
    position: fixed;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
    z-index: 25;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
    pointer-events: none;
    padding: 24px 32px;
    text-align: center;
    max-width: min(340px, calc(100vw - 40px));
    background: rgba(10, 12, 18, 0.82);
    backdrop-filter: blur(16px) saturate(1.2);
    -webkit-backdrop-filter: blur(16px) saturate(1.2);
    border: 1px solid rgba(255, 255, 255, 0.12);
    border-radius: 16px;
    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.45);
    animation: draw-tooltip-pulse 4s ease-in-out infinite;
  }
  /* Panel open: no icon, sit above bottom sheet */
  .draw-guide--with-panel {
    top: min(40vh, calc(100vh - 52vh - 56px));
    gap: 8px;
  }
  .guide-icon {
    width: 56px;
    height: 56px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    border: 2px dashed rgba(255, 255, 255, 0.25);
    border-radius: 14px;
    color: var(--accent);
  }
  @keyframes draw-tooltip-pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.8; }
  }
  .guide-text {
    font-size: 16px;
    font-weight: 600;
    line-height: 1.35;
    color: #fff;
  }
  .guide-hint {
    display: block;
    margin-top: 4px;
    font-size: 11px;
    font-weight: 400;
    line-height: 1.35;
    color: var(--text-secondary);
  }
</style>
