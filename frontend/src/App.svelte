<script>
  import { onMount } from 'svelte';
  import { initAuth, authSession, beginCognitoSignIn } from './lib/authSession.svelte.js';
  import { ui, appConfig, applyAppConfig } from './lib/state.svelte.js';
  import { fetchConfig, fetchAtmosphericRiverFrame } from './lib/api.js';
  import { getModelLeadBounds, tileUrl } from './lib/tile.js';
  import { exportMapImage } from './lib/exportMapImage.js';
  import MapView from './lib/components/MapView.svelte';
  import MapToolbar from './lib/components/MapToolbar.svelte';
  import DrawTools from './lib/components/DrawTools.svelte';
  import CenterGuide from './lib/components/CenterGuide.svelte';
  import AtmosphericRiverControl from './lib/components/AtmosphericRiverControl.svelte';
  import Dashboard from './lib/dashboard/Dashboard.svelte';
  import DashHeader from './lib/dashboard/DashHeader.svelte';
  import AccuracyTiles from './lib/dashboard/AccuracyTiles.svelte';
  import MapOverlay from './lib/dashboard/MapOverlay.svelte';
  let mapView = $state();
  let configReady = $state(false);
  /** The one-screen dashboard opens whenever a region is selected. */
  const dashActive = $derived(!!ui.selectedRegion);

  function handleModelChange(e) {
    ui.model = typeof e === 'string' ? e : e.target.value;
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
    ui.statistic = typeof e === 'string' ? e : e.target.value;
    ui.activeWindow = null;
    mapView?.onStatisticChange();
  }

  function handlePeriodChange() {
    mapView?.onPeriodChange();
  }

  function handleOpacityInput(e) {
    ui.weatherOpacity = Number(e.target.value);
  }

  /** Frame timestamp for the export title, e.g. "2023-01-05 12:00 UTC". */
  function arExportTimeLabel(time) {
    if (!time) return '';
    const date = new Date(time);
    if (Number.isNaN(date.valueOf())) return String(time);
    return `${date.toISOString().slice(0, 16).replace('T', ' ')} UTC`;
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
    console.log('[status] Preparing download…');
    // In AR mode the composed image also carries the on-map footprints; if the
    // frame fetch fails, degrade to the plain export rather than aborting.
    let arFootprints = null;
    let arFrameLabel = '';
    if (ui.mode === 'atmospheric-rivers' && ui.arFramePath) {
      try {
        arFootprints = await fetchAtmosphericRiverFrame(ui.arFramePath);
        const timeLabel = arExportTimeLabel(ui.arTime);
        arFrameLabel = timeLabel ? `tARget v4 · ${timeLabel}` : 'tARget v4 footprints';
      } catch (err) {
        console.warn('[status] AR footprints unavailable; exporting without them', err);
        arFootprints = null;
        arFrameLabel = '';
      }
    }
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
        region: ui.selectedRegion,
        arFootprints,
        arFrameLabel,
      });
    } catch {
      console.warn('[status] Map download failed');
    }
  }

  function closeDashboard() {
    ui.selectedRegion = null;
    ui.wbdDrillPath = [];
  }

  function handleLocationSelect(data) {
    mapView?.flyToLocation(data);
  }

  function handleSearchStatus(msg) {
    if (msg) console.log('[status]', msg);
  }

  /** Show even when a region is selected so the panel can stay open underneath. */
  const showPointGuide = $derived(
    ui.activeTool === 'point' && !ui.hasUsedPinTool,
  );
  const showBoundaryGuide = $derived(
    (ui.activeTool === 'state' || ui.activeTool === 'county' || ui.activeTool === 'watershed'
      || ui.activeTool === 'military')
    && !ui.hasUsedAreaDrawTool,
  );
  const boundaryGuideText = $derived.by(() => {
    if (ui.activeTool === 'county') return 'Click a county to analyze';
    if (ui.activeTool === 'state') return 'Click a state to analyze';
    if (ui.activeTool === 'military') return 'Click a military installation marker to analyze';
    const crumb = ui.wbdDrillPath.length ? `${ui.wbdDrillPath.join(' → ')} — ` : '';
    return `${crumb}Click a watershed to drill down. Press Enter to analyze at the current level.`;
  });

  /**
   * Session-scoped latch: the point guide pulses on its very first display
   * this session and never again — switching away from the pin tool or
   * actually clicking the map both permanently disable subsequent pulsing.
   */
  let pinGuidePulseAllowed = $state(
    ui.activeTool === 'point' && !ui.hasUsedPinTool,
  );
  $effect(() => {
    if (ui.hasUsedPinTool || ui.activeTool !== 'point') {
      pinGuidePulseAllowed = false;
    }
  });

  onMount(async () => {
    try {
      applyAppConfig(await fetchConfig());
      configReady = true;
    } catch (error) {
      console.error('[config] bootstrap failed', error);
    }
    void initAuth().catch((e) => console.error('[auth] initAuth failed', e));
  });
</script>

{#if configReady}<div class="app" class:dash={dashActive}>
  {#if dashActive}
    <DashHeader
      onmodelchange={handleModelChange}
      onperiodchange={handlePeriodChange}
      onexportmap={handleExport}
      onclose={closeDashboard}
    />
  {/if}
  <div class="workspace">
    <aside class="geography">
      <div class="map-box">
        <div class="map-fill">
          <MapView bind:this={mapView} />

          {#if !dashActive}
            <MapToolbar
              onOpacityInput={handleOpacityInput}
              onExport={handleExport}
              onLocationSelect={handleLocationSelect}
              onSearchStatus={handleSearchStatus}
            />
          {:else}
            <MapOverlay />
          {/if}

          <AtmosphericRiverControl />
          <DrawTools />

          {#if !dashActive && ui.mode === 'accuracy' && showPointGuide}
            <CenterGuide
              pulse={pinGuidePulseAllowed}
              hint={!ui.hasUsedAreaDrawTool ? 'Or analyze an area with the tools on the left' : null}
            >
              Click anywhere to compare model accuracy
            </CenterGuide>
          {/if}

          {#if !dashActive && ui.mode === 'accuracy' && showBoundaryGuide}
            <CenterGuide>
              {boundaryGuideText}
            </CenterGuide>
          {/if}

          {#if ui.mode === 'atmospheric-rivers' && !ui.selectedRegion}
            <CenterGuide>
              AR footprints are shown for the scrubbed time. Click a highlighted basin
              on the map to open the storm analysis dashboard.
            </CenterGuide>
          {/if}

          {#if !dashActive && authSession.ready && authSession.mode === 'cognito'}
            {#if authSession.hasSession}
              <div class="auth-floating auth-floating--email" title={authSession.userLabel ?? 'Signed in'}>
                {authSession.userLabel ?? 'Signed in'}
              </div>
            {:else}
              <button type="button" class="auth-floating auth-floating--cta" onclick={() => beginCognitoSignIn()}>
                Sign in
              </button>
            {/if}
          {/if}

          {#if !dashActive}<footer class="app-credit">Kevin Toren 2026</footer>{/if}
        </div>
      </div>
      {#if dashActive}
        <AccuracyTiles onstatchange={handleStatChange} />
      {/if}
    </aside>
    {#if dashActive}
      <Dashboard onleadchange={(lead) => mapView?.onLeadSliderInput(lead)} />
    {/if}
  </div>
  {#if dashActive}
    <footer class="dash-footer">
      <span>Kevin Toren 2026{authSession.ready && authSession.mode === 'cognito' && authSession.hasSession ? ` · ${authSession.userLabel ?? 'signed in'}` : ''}</span>
      <span>{ui.selectedRegion?.type === 'huc' ? `HUC${String(ui.selectedRegion.huc).length} ${ui.selectedRegion.huc}` : ''}</span>
    </footer>
  {/if}
</div>{:else}<div class="app-loading">Loading configuration…</div>{/if}

<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&display=swap');

  :root {
    color-scheme: dark;
    --panel-bg: rgba(14, 17, 23, 0.88);
    --panel-solid: #0e1117;
    --panel-border: rgba(255,255,255,0.08);
    --text-primary: #e2e5ea;
    --text-secondary: #a3aab6;
    --accent: #6eb5ff;
    --accent-glow: rgba(110, 181, 255, 0.12);
    --hover-bg: rgba(255,255,255,0.05);
    --surface: rgba(255,255,255,0.04);
    --radius: 10px;
    --fs-base: 15px;
    --pane-bg: #171e28;
  }
  :global(*) { box-sizing: border-box; }
  :global(html) {
    height: 100%;
    overflow: hidden;
    overscroll-behavior: none;
  }
  :global(body) {
    margin: 0;
    height: 100%;
    min-height: 100dvh;
    font-family: 'DM Sans', system-ui, -apple-system, sans-serif;
    font-size: var(--fs-base);
    background: #000;
    color: var(--text-primary);
    -webkit-font-smoothing: antialiased;
    overflow: hidden;
    overscroll-behavior: none;
    padding: env(safe-area-inset-top, 0) env(safe-area-inset-right, 0) env(safe-area-inset-bottom, 0)
      env(safe-area-inset-left, 0);
  }
  .app {
    /* Viewport units, not 100%: a percentage height resolves to auto here and
       lets a tall table in the analysis column push the page past the fold. */
    height: 100dvh;
    min-height: 100dvh;
    overflow: hidden;
    display: flex;
    flex-direction: column;
  }
  .workspace {
    flex: 1;
    min-height: 0;
    display: flex;
  }
  .geography {
    flex: 1;
    min-width: 0;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }
  .map-box {
    flex: 1;
    position: relative;
    min-height: 0;
    display: flex;
  }
  .map-fill {
    flex: 1;
    position: relative;
    min-height: 0;
  }

  /* ── One-screen dashboard: map column + analysis column ── */
  .app.dash {
    background: #0e1117;
    padding: 0 14px;
  }
  .app.dash .workspace {
    display: grid;
    grid-template-columns: minmax(270px, 29%) minmax(0, 1fr);
    grid-template-rows: minmax(0, 1fr);
    gap: 12px;
    padding-top: 12px;
  }
  .app.dash .geography {
    gap: 12px;
  }
  .app.dash .map-box {
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid var(--panel-border);
  }
  /* The draw toolbar sits under the map title in the narrow map column. */
  .app.dash .map-box :global(.toolbar) {
    top: 66px;
    left: 10px;
  }
  .app.dash .map-box :global(.map-wrap) {
    height: 100%;
  }
  .dash-footer {
    height: 24px;
    flex: none;
    display: flex;
    align-items: center;
    justify-content: space-between;
    color: #929dae;
    font-size: 11px;
  }
  @media (max-width: 900px) {
    .app.dash .workspace {
      grid-template-columns: 1fr;
      grid-template-rows: 300px minmax(0, 1fr);
      overflow-y: auto;
    }
  }

  .app-credit {
    position: absolute;
    bottom: max(14px, env(safe-area-inset-bottom, 0px));
    left: 50%;
    transform: translateX(-50%);
    z-index: 11;
    margin: 0;
    padding: 6px 14px;
    font-size: 12px;
    font-weight: 400;
    letter-spacing: 0.02em;
    color: rgba(255, 255, 255, 0.7);
    background: rgba(18, 22, 30, 0.62);
    backdrop-filter: blur(14px) saturate(1.2);
    -webkit-backdrop-filter: blur(14px) saturate(1.2);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 999px;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
    pointer-events: none;
  }

  .auth-floating {
    position: absolute;
    top: max(14px, env(safe-area-inset-top, 0px));
    right: max(14px, env(safe-area-inset-right, 0px));
    z-index: 11;
    margin: 0;
    display: inline-flex;
    align-items: center;
    padding: 10px 18px;
    font-family: inherit;
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 0.02em;
    color: var(--text-primary);
    background: var(--panel-bg);
    backdrop-filter: blur(20px) saturate(1.4);
    -webkit-backdrop-filter: blur(20px) saturate(1.4);
    border: 1px solid var(--panel-border);
    border-radius: 14px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
    max-width: min(260px, 40vw);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .auth-floating--email {
    pointer-events: none;
  }
  .auth-floating--cta {
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s, color 0.15s;
  }
  .auth-floating--cta:hover {
    background: rgba(28, 34, 46, 0.92);
    border-color: rgba(255, 255, 255, 0.18);
    color: var(--accent);
  }
  .auth-floating--cta:active {
    background: rgba(20, 24, 32, 0.95);
  }

  @media (max-width: 640px) {
    :global(body) {
      font-size: 14px;
    }
    .app-credit {
      bottom: max(10px, env(safe-area-inset-bottom, 0px));
      font-size: 11px;
      padding: 5px 12px;
      gap: 6px;
    }
    .auth-floating {
      top: max(10px, env(safe-area-inset-top, 0px));
      right: max(10px, env(safe-area-inset-right, 0px));
      font-size: 12px;
      padding: 7px 12px;
      max-width: min(180px, 50vw);
    }
  }
</style>
