<script>
  import { ui } from '../state.svelte.js';
  import {
    glyphDownloadMap,
    glyphOpacityMoon,
  } from '../appIcons.js';
  import AddressSearch from './AddressSearch.svelte';

  let {
    onOpacityInput,
    onExport,
    onLocationSelect,
    onSearchStatus,
  } = $props();
</script>

<div class="map-toolbar" role="toolbar" aria-label="Map tools">
  <button type="button" class="btn-download" onclick={() => onExport?.()} aria-label="Download map image">
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">{@html glyphDownloadMap}</svg>
    <span class="btn-download-label">Download map</span>
  </button>

  <div class="toolbar-sep" aria-hidden="true"></div>

  <div class="opacity-block">
    <span class="ctrl-label">Map opacity</span>
    <div class="opacity-row">
      <svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" class="opacity-icon" aria-hidden="true">{@html glyphOpacityMoon}</svg>
      <input
        type="range"
        min="0"
        max="1"
        step="0.05"
        value={ui.weatherOpacity}
        oninput={onOpacityInput}
        aria-label="Map opacity"
      />
    </div>
  </div>

  <div class="toolbar-sep" aria-hidden="true"></div>

  <div class="search-block">
    <span class="ctrl-label">Fly to</span>
    <AddressSearch onSelect={onLocationSelect} onStatus={onSearchStatus} />
  </div>

</div>

<style>
  .map-toolbar {
    --toolbar-control-height: 32px;
    position: absolute;
    top: max(14px, env(safe-area-inset-top, 0px));
    left: 50%;
    transform: translateX(-50%);
    z-index: 11;
    display: flex;
    /* Stretch so the download button, the two label+control stacks, and the
       dividers all share one height — the button becomes a full-height pill whose
       top aligns with the labels and bottom with the controls (symmetric boxes). */
    align-items: stretch;
    flex-wrap: wrap;
    justify-content: center;
    gap: 10px 12px;
    padding: 8px 14px 10px;
    max-width: calc(100vw - 120px - env(safe-area-inset-left, 0px) - env(safe-area-inset-right, 0px));
    background: var(--panel-bg);
    backdrop-filter: blur(20px) saturate(1.4);
    -webkit-backdrop-filter: blur(20px) saturate(1.4);
    border: 1px solid var(--panel-border);
    border-radius: 14px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
  }
  /* Subtle, neutral styling that matches the area-selector tool buttons — surface
     fill and a faint border rather than the accent gradient, so it doesn't compete
     with the controls beside it. Accent only shows up on hover. */
  .btn-download {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    padding: 0 16px;
    border-radius: 9px;
    background: var(--surface);
    color: var(--text-secondary);
    font-size: 13px;
    font-weight: 600;
    font-family: inherit;
    cursor: pointer;
    border: 1px solid var(--panel-border);
    white-space: nowrap;
    transition: background 0.15s, color 0.15s, border-color 0.15s;
  }
  .btn-download:hover {
    background: var(--hover-bg);
    color: var(--text-primary);
    border-color: rgba(110, 181, 255, 0.3);
  }
  @media (max-width: 640px) {
    .map-toolbar {
      --toolbar-control-height: 40px;
      left: max(56px, calc(env(safe-area-inset-left, 0px) + 48px));
      right: max(8px, env(safe-area-inset-right, 0px));
      transform: none;
      max-width: none;
      /* Controls wrap and use fixed heights on mobile — center them per row rather
         than stretching to the tallest item in that row. */
      align-items: center;
      justify-content: flex-start;
      row-gap: 8px;
    }
    .toolbar-sep {
      display: none;
    }
    .opacity-row input[type='range'] {
      width: min(120px, 28vw);
    }
    .btn-download-label {
      display: none;
    }
    .btn-download {
      min-width: 40px;
      min-height: 40px;
      padding: 10px;
      justify-content: center;
    }
    .search-block {
      flex: 1 1 200px;
      min-width: 160px;
    }
  }
  .toolbar-sep {
    width: 1px;
    /* Full-height divider so it spans the label+control stacks, not just the control row. */
    align-self: stretch;
    background: var(--panel-border);
    flex-shrink: 0;
  }
  .opacity-block {
    display: flex;
    flex-direction: column;
    /* Label pinned to the top, control to the bottom, so every section shares the
       same top and bottom edges as the full-height download button. */
    justify-content: space-between;
    gap: 4px;
  }
  .search-block {
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    gap: 4px;
    align-items: stretch;
    min-width: 240px;
  }
  .ctrl-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: rgba(202, 210, 222, 0.9);
    text-align: left;
  }
  .opacity-row {
    display: flex;
    align-items: center;
    gap: 7px;
    box-sizing: border-box;
    height: var(--toolbar-control-height);
    padding: 0 9px;
    background: var(--surface);
    border: 1px solid var(--panel-border);
    border-radius: 9px;
  }
  .opacity-icon {
    flex-shrink: 0;
    opacity: 0.55;
    color: var(--text-secondary);
  }
  .opacity-row input[type="range"] {
    width: 80px;
    height: 4px;
    accent-color: var(--accent);
  }
</style>
