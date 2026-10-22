<script>
  /** Shared lead-day bar: best model per lead for the selected statistic; click = inspect that lead everywhere. */
  import { ui, statLabel, winnerStatKeys } from '../state.svelte.js';
  import { statColor } from '../palette.js';
  import { dash, selectedLead, leadAxis, palette, modelLabel, pickLead } from './dashData.svelte.js';

  const axis = $derived(leadAxis());
  const pal = $derived(palette());
  const lead = $derived(selectedLead());
  const payload = $derived(dash.winners[ui.statistic] ?? null);
  const supported = $derived(winnerStatKeys().includes(ui.statistic));

</script>

<div class="lead-bar">
  <div class="lead-title">
    <div>Best model by lead day — <span style:color={statColor(ui.statistic)}>{statLabel(ui.statistic)}</span></div>
    <small>{supported ? 'Click a lead day (or a chart) to inspect it everywhere' : 'Rankings apply to accuracy statistics'}</small>
  </div>
  <div class="lead-strip" role="radiogroup" aria-label="Lead day">
    {#each axis as l}
      {@const w = payload?.leads?.[String(l)]}
      {@const pc = w ? pal[w] : null}
      <button
        type="button"
        class="ls-cell"
        class:selected={l === lead}
        role="radio"
        aria-checked={l === lead}
        title={`Lead day ${l}${w ? ` · best on ${statLabel(ui.statistic)}: ${modelLabel(w)}${pc ? '' : ' (scored but not in the configured model list)'}` : ''}`}
        onclick={() => pickLead(l)}
      >
        <span class="ls-day">{l}</span>
        <span class="ls-pill" class:foreign={w && !pc} style:background={pc?.bg} style:color={pc?.fg} style:border-color={pc?.border}>{pc?.ident ?? (w ? String(w).slice(0, 4).toUpperCase() : dash.winnersLoading && !payload ? '…' : '—')}</span>
      </button>
    {/each}
  </div>
</div>

<style>
  .lead-bar {
    display: flex; align-items: center; gap: 14px; padding: 5px 11px; min-width: 0;
    background: var(--pane-bg); border: 1px solid var(--panel-border); border-radius: 10px;
  }
  .lead-title { flex: none; font-size: 13px; font-weight: 600; color: #d5dae2; line-height: 1.25; white-space: nowrap; }
  .lead-title small { display: block; font-weight: 400; color: var(--text-secondary); font-size: 10.5px; }
  .lead-strip { flex: 1; display: grid; grid-auto-flow: column; grid-auto-columns: minmax(0, 1fr); gap: 4px; min-width: 0; }
  .ls-cell {
    display: flex; flex-direction: column; align-items: center; gap: 3px; padding: 3px 0; min-width: 0;
    border: 1px solid #ffffff14; border-radius: 5px; background: #ffffff04; color: var(--text-primary); cursor: pointer; font: inherit;
  }
  .ls-cell:hover { background: #ffffff0c; }
  .ls-cell.selected { background: #6eb5ff2e; border-color: #6eb5ff; box-shadow: 0 0 0 1px #6eb5ff66; }
  .ls-day { font-size: 11px; font-weight: 600; line-height: 1; }
  .ls-pill.foreign { color: var(--text-secondary); background: #ffffff08; }
  .ls-pill { font-size: 9px; font-weight: 600; line-height: 1; padding: 3px 2px; border: 1px solid #ffffff1a; border-radius: 3px; width: 32px; text-align: center; overflow: hidden; }
  @media (max-width: 900px) {
    .lead-title small { display: none; }
  }
</style>
