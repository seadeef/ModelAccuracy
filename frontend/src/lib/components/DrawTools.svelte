<script>
  import { ui } from '../state.svelte.js';
  import { authSession } from '../authSession.svelte.js';
  import { drawToolGlyph, glyphSave, glyphLoad } from '../appIcons.js';
  import { saveRegion, listSavedRegions } from '../api.js';

  function selectTool(tool) {
    if (ui.activeTool === tool) {
      ui.activeTool = 'point';
    } else {
      ui.activeTool = tool;
    }
  }

  const tools = [
    { id: 'point', label: 'Click point', icon: drawToolGlyph.point },
    { id: 'rectangle', label: 'Draw rectangle', icon: drawToolGlyph.rectangle },
    { id: 'polygon', label: 'Draw polygon', icon: drawToolGlyph.polygon },
  ];

  function toolTitle(tool) {
    if (ui.selectedRegion) return tool.label;
    if (tool.id === 'polygon') {
      return `${tool.label} — Click map to add corners, then press Enter to finish (at least 3). Double-click also finishes.`;
    }
    return `${tool.label} — Draw to get started`;
  }

  const isLoggedIn = $derived(authSession.ready && authSession.mode === 'cognito' && authSession.hasSession);
  const canSave = $derived(isLoggedIn && ui.selectedRegion != null);

  // ---- Save flow ----
  let showSavePrompt = $state(false);
  let saveName = $state('');
  let saving = $state(false);
  let saveFlash = $state('');

  async function handleSave() {
    const name = saveName.trim();
    if (!name || !ui.selectedRegion) return;
    saving = true;
    saveFlash = '';
    const result = await saveRegion(name, ui.selectedRegion);
    saving = false;
    if (result) {
      saveFlash = 'Saved!';
      showSavePrompt = false;
      saveName = '';
      setTimeout(() => { saveFlash = ''; }, 1800);
    } else {
      saveFlash = 'Save failed';
      setTimeout(() => { saveFlash = ''; }, 2500);
    }
  }

  function openSavePrompt() {
    showSavePrompt = true;
    saveName = '';
    saveFlash = '';
  }

  // ---- Load flow ----
  let showLoadPopover = $state(false);
  let savedShapes = $state([]);
  let loadingShapes = $state(false);
  let loadError = $state('');

  async function openLoadPopover() {
    showLoadPopover = !showLoadPopover;
    if (!showLoadPopover) return;
    loadingShapes = true;
    loadError = '';
    const data = await listSavedRegions();
    loadingShapes = false;
    if (!data) {
      loadError = 'Could not load saved regions';
      savedShapes = [];
      return;
    }
    savedShapes = data.shapes ?? [];
    if (!savedShapes.length) {
      loadError = 'No saved regions yet';
    }
  }

  function loadShape(shape) {
    ui.selectedRegion = shape.region;
    if (shape.region?.type) {
      ui.activeTool = shape.region.type;
    }
    showLoadPopover = false;
  }

  function handleClickOutside(e) {
    if (showSavePrompt || showLoadPopover) {
      const toolbar = e.target.closest('.toolbar');
      if (!toolbar) {
        showSavePrompt = false;
        showLoadPopover = false;
      }
    }
  }

  function shapeTypeIcon(type) {
    if (type === 'rectangle') return '▭';
    if (type === 'polygon') return '⬠';
    return '📍';
  }
</script>

<svelte:window onclick={handleClickOutside} />

<div class="toolbar">
  {#each tools as tool}
    <button
      class="tool-btn"
      class:active={ui.activeTool === tool.id}
      title={toolTitle(tool)}
      onclick={() => selectTool(tool.id)}
    >
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
        {@html tool.icon}
      </svg>
    </button>
  {/each}

  {#if isLoggedIn}
    <div class="toolbar-sep" aria-hidden="true"></div>

    {#if canSave}
      <div class="save-wrap">
        <button
          class="tool-btn"
          class:active={showSavePrompt}
          title="Save current region"
          onclick={openSavePrompt}
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
            {@html glyphSave}
          </svg>
        </button>

        {#if showSavePrompt}
          <!-- svelte-ignore a11y_click_events_have_key_events -->
          <div class="popover save-popover" onclick={(e) => e.stopPropagation()}>
            <div class="popover-title">Save region</div>
            <form class="save-form" onsubmit={(e) => { e.preventDefault(); handleSave(); }}>
              <input
                type="text"
                class="save-input"
                placeholder="Region name…"
                bind:value={saveName}
                maxlength="200"
                autofocus
              />
              <button type="submit" class="save-submit" disabled={saving || !saveName.trim()}>
                {saving ? '…' : 'Save'}
              </button>
            </form>
          </div>
        {/if}
      </div>
    {/if}

    <div class="load-wrap">
      <button
        class="tool-btn"
        class:active={showLoadPopover}
        title="Load saved region"
        onclick={openLoadPopover}
      >
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
          {@html glyphLoad}
        </svg>
      </button>

      {#if showLoadPopover}
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <div class="popover load-popover" onclick={(e) => e.stopPropagation()}>
          <div class="popover-title">Saved regions</div>
          {#if loadingShapes}
            <div class="popover-msg">Loading…</div>
          {:else if loadError}
            <div class="popover-msg">{loadError}</div>
          {:else}
            <ul class="shape-list">
              {#each savedShapes as shape}
                <li>
                  <button class="shape-item" onclick={() => loadShape(shape)}>
                    <span class="shape-icon">{shapeTypeIcon(shape.region?.type)}</span>
                    <span class="shape-name">{shape.name}</span>
                  </button>
                </li>
              {/each}
            </ul>
          {/if}
        </div>
      {/if}
    </div>
  {/if}

  {#if saveFlash}
    <div class="flash" class:flash--error={saveFlash.includes('fail')}>
      {saveFlash}
    </div>
  {/if}
</div>

<style>
  .toolbar {
    position: absolute;
    top: max(14px, env(safe-area-inset-top, 0px));
    left: max(14px, env(safe-area-inset-left, 0px));
    z-index: 10;
    display: flex;
    flex-direction: column;
    gap: 2px;
    padding: 4px;
    background: var(--panel-bg);
    backdrop-filter: blur(20px) saturate(1.4);
    -webkit-backdrop-filter: blur(20px) saturate(1.4);
    border: 1px solid var(--panel-border);
    border-radius: 10px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.4);
  }
  .toolbar-sep {
    width: 100%;
    height: 1px;
    background: var(--panel-border);
    margin: 2px 0;
    flex-shrink: 0;
  }
  .tool-btn {
    width: 34px;
    height: 34px;
    border: 1px solid transparent;
    border-radius: 7px;
    background: transparent;
    color: var(--text-secondary);
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.15s;
    position: relative;
  }
  .tool-btn.active {
    background: rgba(110, 181, 255, 0.12);
    color: var(--accent);
    border-color: rgba(110, 181, 255, 0.2);
    box-shadow: 0 0 8px rgba(110, 181, 255, 0.1);
  }
  .tool-btn:hover:not(.active) {
    background: var(--hover-bg);
    color: var(--text-primary);
    border-color: var(--panel-border);
  }

  .save-wrap, .load-wrap {
    position: relative;
  }

  .popover {
    position: absolute;
    left: calc(100% + 10px);
    top: 0;
    min-width: 200px;
    background: var(--panel-bg);
    backdrop-filter: blur(20px) saturate(1.4);
    -webkit-backdrop-filter: blur(20px) saturate(1.4);
    border: 1px solid var(--panel-border);
    border-radius: 10px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.45);
    padding: 10px;
    z-index: 20;
    animation: popIn 0.15s ease-out;
  }
  @keyframes popIn {
    from { opacity: 0; transform: translateX(-4px); }
    to { opacity: 1; transform: translateX(0); }
  }
  .popover-title {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    color: var(--text-secondary);
    margin-bottom: 8px;
  }
  .popover-msg {
    font-size: 12px;
    color: var(--text-secondary);
    padding: 4px 0;
  }

  .save-form {
    display: flex;
    gap: 6px;
  }
  .save-input {
    flex: 1;
    min-width: 0;
    height: 30px;
    padding: 0 8px;
    background: var(--surface);
    color: var(--text-primary);
    border: 1px solid var(--panel-border);
    border-radius: 7px;
    font-size: 12px;
    font-family: inherit;
    outline: none;
  }
  .save-input:focus {
    border-color: var(--accent);
  }
  .save-submit {
    flex-shrink: 0;
    height: 30px;
    padding: 0 12px;
    border-radius: 7px;
    border: 1px solid rgba(110, 181, 255, 0.35);
    background: linear-gradient(180deg, rgba(110, 181, 255, 0.2), rgba(110, 181, 255, 0.08));
    color: #b8d9ff;
    font-size: 12px;
    font-weight: 600;
    font-family: inherit;
    cursor: pointer;
    transition: background 0.15s, color 0.15s;
  }
  .save-submit:hover:not(:disabled) {
    background: linear-gradient(180deg, rgba(110, 181, 255, 0.3), rgba(110, 181, 255, 0.14));
    color: #e8f3ff;
  }
  .save-submit:disabled {
    opacity: 0.4;
    cursor: default;
  }

  .shape-list {
    list-style: none;
    margin: 0;
    padding: 0;
    max-height: 240px;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .shape-item {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 7px 8px;
    border: none;
    border-radius: 7px;
    background: transparent;
    color: var(--text-primary);
    font-size: 13px;
    font-family: inherit;
    cursor: pointer;
    text-align: left;
    transition: background 0.12s, color 0.12s;
  }
  .shape-item:hover {
    background: var(--hover-bg);
    color: var(--accent);
  }
  .shape-icon {
    flex-shrink: 0;
    font-size: 13px;
    width: 18px;
    text-align: center;
    opacity: 0.7;
  }
  .shape-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .flash {
    position: absolute;
    left: calc(100% + 10px);
    bottom: 0;
    white-space: nowrap;
    font-size: 12px;
    font-weight: 600;
    color: #7be69a;
    padding: 6px 12px;
    background: var(--panel-bg);
    backdrop-filter: blur(16px);
    border: 1px solid var(--panel-border);
    border-radius: 8px;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
    animation: flashIn 0.2s ease-out;
    pointer-events: none;
  }
  .flash--error {
    color: #ff7b7b;
  }
  @keyframes flashIn {
    from { opacity: 0; transform: translateX(-4px); }
    to { opacity: 1; transform: translateX(0); }
  }

  @media (max-width: 640px) {
    .toolbar {
      top: max(12px, env(safe-area-inset-top, 0px));
      left: max(8px, env(safe-area-inset-left, 0px));
      padding: 5px;
      gap: 3px;
    }
    .tool-btn {
      width: 44px;
      height: 44px;
      border-radius: 10px;
    }
    .tool-btn svg {
      width: 18px;
      height: 18px;
    }
    .popover {
      min-width: 180px;
    }
  }
</style>
