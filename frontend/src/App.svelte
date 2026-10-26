<script>
  import { onMount } from 'svelte';
  import { initAuth, authSession, beginCognitoSignIn } from './lib/authSession.svelte.js';
  import { ui, appConfig, applyAppConfig, isArPeriod } from './lib/state.svelte.js';
  import { fetchConfig, fetchAtmosphericRiverFrame } from './lib/api.js';
  import { getModelLeadBounds, tileUrl } from './lib/tile.js';
  import { exportMapImage } from './lib/exportMapImage.js';
  import MapView from './lib/components/MapView.svelte';
  import MapToolbar from './lib/components/MapToolbar.svelte';
  import DrawTools from './lib/components/DrawTools.svelte';
  import CenterGuide from './lib/components/CenterGuide.svelte';
  import AtmosphericRiverControl from './lib/components/AtmosphericRiverControl.svelte';
  import ArPanel from './lib/components/ArPanel.svelte';
  import Dashboard from './lib/dashboard/Dashboard.svelte';
  import DashHeader from './lib/dashboard/DashHeader.svelte';
  import AccuracyTiles from './lib/dashboard/AccuracyTiles.svelte';
  import MapOverlay from './lib/dashboard/MapOverlay.svelte';
  import { dash } from './lib/dashboard/dashData.svelte.js';
  import { STATIC_BASE } from './lib/constants.js';
  let mapView = $state();
  /** Height of the floating AR panel (bare map, AR mode) so the map key clears it. */
  let arPanelHeight = $state(0);

  // Value range of the raster overlay for the colour key (bare map and dashboard alike).
  $effect(() => {
    const model = ui.model;
    const stat = ui.statistic;
    const period = isArPeriod(ui.period) ? 'yearly' : ui.period;
    const month = ui.month;
    const season = ui.season;
    const sub = period === 'monthly' ? `monthly/${month}` : period === 'seasonal' ? `seasonal/${season}` : 'yearly';
    queueMicrotask(async () => {
      try {
        const resp = await fetch(`${STATIC_BASE}/ranges/${model}/${stat}/${sub}.json`);
        dash.valueRange = resp.ok ? await resp.json() : null;
      } catch {
        dash.valueRange = null;
      }
    });
  });
  let configReady = $state(false);
  /** The one-screen dashboard opens whenever a region is selected. */
  /** The dashboard shell stays up after the first Escape with its data blanked and the
   *  picker re-armed; a second Escape (or ×) drops back to the full map. */
  let dashShell = $state(false);
  const dashActive = $derived(!!ui.selectedRegion || dashShell);
  const dashBlank = $derived(dashShell && !ui.selectedRegion);
  $effect(() => { if (ui.selectedRegion) dashShell = false; });

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
    dashShell = false;
    ui.selectedRegion = null;
    ui.wbdDrillPath = [];
  }

  /** Escape, first press: drop the region but keep the dashboard shell, blank, with the
   *  active tool re-armed for the next pick. Second press: back to the full map. Anything
   *  that owns Escape itself — an open menu or popover, the search list, a focused text
   *  field, a draw in progress (MapView cancels it and marks the event handled) — keeps it. */
  function onWindowKeyDown(e) {
    if (e.key !== 'Escape' || e.defaultPrevented || !dashActive) return;
    if (document.querySelector('.dd-menu, .popover, .suggestions')) return;
    const t = e.target;
    if (t instanceof HTMLElement && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.isContentEditable)) {
      t.blur();
      return;
    }
    if (ui.selectedRegion) {
      ui.selectedRegion = null;
      ui.wbdDrillPath = [];
      dashShell = true;
    } else {
      closeDashboard();
    }
  }

  const toolNoun = $derived({ point: 'point', state: 'state', county: 'county', watershed: 'watershed', military: 'installation', rectangle: 'rectangle', polygon: 'polygon' }[ui.activeTool] ?? 'area');

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
  // The first-use hint hides once an area tool has been used, except for the
  // watershed tool, whose hint carries the Enter / Escape instructions and so
  // stays up until a basin is selected. The body is static; only the hint
  // names the basin currently drilled into (one name, never the whole path —
  // the bubble must not grow with the drill, and a long name shrinks or
  // ellipsizes inside it).
  const showBoundaryGuide = $derived(
    (ui.activeTool === 'state' || ui.activeTool === 'county' || ui.activeTool === 'watershed'
      || ui.activeTool === 'military')
    && (!ui.hasUsedAreaDrawTool || ui.activeTool === 'watershed'),
  );
  const boundaryGuideText = $derived.by(() => {
    if (ui.activeTool === 'county') return 'Click a county to analyze';
    if (ui.activeTool === 'state') return 'Click a state to analyze';
    if (ui.activeTool === 'military') return 'Click a military installation marker to analyze';
    return ui.wbdDrillPath.length
      ? 'Click a sub-basin to drill down · Enter analyzes this level · Esc backs out'
      : 'Click a watershed to drill down · Enter analyzes the current level';
  });
  const boundaryGuideHint = $derived(ui.activeTool === 'watershed' && ui.wbdDrillPath.length
    ? `Inside ${ui.wbdDrillPath[ui.wbdDrillPath.length - 1]}`
    : null);

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

<svelte:window onkeydown={onWindowKeyDown} />


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
        <div class="map-fill" style:--ar-panel-h="{!dashActive && ui.mode === 'atmospheric-rivers' && arPanelHeight ? arPanelHeight + 12 : 0}px">
          <MapView bind:this={mapView} />

          {#if !dashActive}
            <MapToolbar
              onOpacityInput={handleOpacityInput}
              onExport={handleExport}
              onLocationSelect={handleLocationSelect}
              onSearchStatus={handleSearchStatus}
            />
            <MapOverlay bare />
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

          {#if !dashActive && showBoundaryGuide}
            <CenterGuide hint={boundaryGuideHint}>
              {boundaryGuideText}
            </CenterGuide>
          {/if}

          <!-- AR mode on the bare map: the timeline floats over the map, so the
               footprints can be scrubbed and the range set before any region is
               picked; the dashboard's AR tab takes over once one is. -->
          {#if !dashActive && ui.mode === 'atmospheric-rivers'}
            <ArPanel bind:height={arPanelHeight} />
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

          {#if !dashActive && ui.mode !== 'atmospheric-rivers'}<footer class="app-credit">Kevin Toren 2026</footer>{/if}
        </div>
      </div>
      {#if dashBlank}
        <div class="blank-tiles" aria-hidden="true"></div>
      {:else if dashActive}
        <AccuracyTiles onstatchange={handleStatChange} />
      {/if}
    </aside>
    {#if dashBlank}
      <section class="blank-results">
        <div class="blank-msg">
          <b>Pick another area on the map</b>
          <span>The {toolNoun} tool is ready · Esc again for the full map</span>
        </div>
      </section>
    {:else if dashActive}
      <Dashboard onleadchange={(lead) => mapView?.onLeadSliderInput(lead)} />
    {/if}
  </div>
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
    --pane-bg: #121822;
    --rule: rgba(255,255,255,0.07);
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
  /* The floating AR panel sits over the bottom edge: the map's attribution moves up with the key. */
  .map-fill :global(.maplibregl-ctrl-bottom-right) { bottom: var(--ar-panel-h, 0px); transition: bottom .15s; }

  /* ── One-screen dashboard: map column + analysis column ── */
  .app.dash {
    background: #0b0f16;
    padding: 0 14px 14px;
  }
  .app.dash .workspace {
    display: grid;
    grid-template-columns: minmax(290px, 30%) minmax(0, 1fr);
    grid-template-rows: minmax(0, 1fr);
    gap: 14px;
    padding-top: 12px;
  }
  .app.dash .geography {
    gap: 14px;
  }
  .app.dash .map-box {
    container: map-box / size;
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid var(--panel-border);
  }
  /* The draw toolbar sits at the top of the narrow map column. */
  .app.dash .map-box :global(.toolbar) {
    top: 12px;
    left: 12px;
  }
  .app.dash .map-box :global(.map-wrap) {
    height: 100%;
  }
  .blank-tiles { flex: none; height: 118px; border: 1px dashed var(--panel-border); border-radius: 12px; opacity: .6; }
  .blank-results { display: flex; align-items: center; justify-content: center; min-width: 0; min-height: 0; border: 1px dashed var(--panel-border); border-radius: 12px; }
  .blank-msg { display: flex; flex-direction: column; align-items: center; gap: 6px; text-align: center; padding: 16px; }
  .blank-msg b { font-size: 16px; font-weight: 600; color: var(--text-primary); }
  .blank-msg span { font-size: 13px; color: var(--text-secondary); }
  @media (max-width: 900px) {
    .app.dash .workspace {
      grid-template-columns: 1fr;
      grid-template-rows: 300px minmax(0, 1fr);
      overflow-y: auto;
    }
  }

  /* ── Dashboard-wide idiom (shared by every pane) ── */
  :global(.eyebrow) {
    font-size: 11.5px; font-weight: 600; letter-spacing: .09em; text-transform: uppercase;
    color: #8f9aab; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0;
  }
  :global(.eyebrow em) { font-style: normal; color: var(--text-primary); }
  :global(.chips) { display: inline-flex; align-items: center; gap: 4px; }
  :global(.chip) {
    font: inherit; font-size: 12px; font-weight: 600; padding: 3px 9px; line-height: 1.2;
    background: transparent; border: 1px solid var(--panel-border); border-radius: 6px;
    color: var(--text-secondary); cursor: pointer; white-space: nowrap;
  }
  :global(.chip:hover) { color: var(--text-primary); background: var(--hover-bg); }
  :global(.chip.active) { color: #fff; border-color: #ffffff38; background: #ffffff12; }
  :global(.chip:disabled) { opacity: .4; cursor: default; }
  :global(.seg) { display: inline-flex; border: 1px solid var(--panel-border); border-radius: 7px; overflow: hidden; }
  :global(.seg button) { font: inherit; font-size: 12px; font-weight: 600; padding: 3px 10px; background: none; border: 0; color: var(--text-secondary); cursor: pointer; white-space: nowrap; }
  :global(.seg button.active) { background: #ffffff14; color: #fff; }
  :global(.cap) { font-size: 12px; color: var(--text-secondary); white-space: nowrap; }
  :global(.plot) { position: relative; flex: 1; min-height: 60px; }
  :global(.plot-empty) { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; text-align: center; color: var(--text-secondary); font-size: 13px; padding: 0 12%; line-height: 1.5; }

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
