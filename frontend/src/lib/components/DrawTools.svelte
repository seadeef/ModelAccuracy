<script>
  import { ui } from '../state.svelte.js';
  import { drawToolGlyph } from '../appIcons.js';

  function selectTool(tool) {
    // Toggle between area tool and point (default)
    if (ui.activeTool === tool) {
      ui.activeTool = 'point'; // back to default click mode
    } else {
      ui.activeTool = tool;
    }
  }

  const tools = [
    { id: 'point', label: 'Click point', icon: drawToolGlyph.point },
    { id: 'rectangle', label: 'Draw rectangle', icon: drawToolGlyph.rectangle },
    { id: 'polygon', label: 'Draw polygon', icon: drawToolGlyph.polygon },
  ];

  /** Native tooltip on toolbar buttons. */
  function toolTitle(tool) {
    if (ui.selectedRegion) return tool.label;
    if (tool.id === 'polygon') {
      return `${tool.label} — Click map to add corners, then press Enter to finish (at least 3). Double-click also finishes.`;
    }
    return `${tool.label} — Draw to get started`;
  }
</script>

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
</div>

<style>
  .toolbar {
    position: absolute;
    top: 14px;
    left: 14px;
    z-index: 10;
    display: flex;
    flex-direction: column;
    gap: 2px;
    padding: 5px;
    background: var(--panel-bg);
    backdrop-filter: blur(20px) saturate(1.4);
    -webkit-backdrop-filter: blur(20px) saturate(1.4);
    border: 1px solid var(--panel-border);
    border-radius: var(--radius);
    box-shadow: 0 4px 16px rgba(0,0,0,0.35);
  }
  .tool-btn {
    width: 36px;
    height: 36px;
    border: none;
    border-radius: 7px;
    background: transparent;
    color: var(--text-secondary);
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.15s;
  }
  .tool-btn.active {
    background: var(--accent-glow);
    color: var(--accent);
  }
  .tool-btn:hover:not(.active) {
    background: var(--hover-bg);
    color: var(--text-primary);
  }
</style>
