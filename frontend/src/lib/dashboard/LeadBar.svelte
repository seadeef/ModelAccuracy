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
  <div class="eyebrow" title="For each lead day, the model with the best value of the selected statistic over this region and period. Click a day to inspect it on the map, the tiles and every chart.">
    Best model by lead day — <em style:color={statColor(ui.statistic)}>{statLabel(ui.statistic)}</em>
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
        title={`Lead day ${l}${w ? ` · best on ${statLabel(ui.statistic)}: ${modelLabel(w)}` : ''}`}
        onclick={() => pickLead(l)}
      >
        <span class="ls-day">{l}</span>
        <span class="ls-pill" class:foreign={w && !pc} style:background={pc?.bg} style:color={pc?.fg} style:border-color={pc?.border}>{pc?.ident ?? (w ? String(w).slice(0, 4).toUpperCase() : dash.winnersLoading && !payload ? '…' : supported ? '—' : '')}</span>
      </button>
    {/each}
  </div>
</div>

<style>
  .lead-bar { display: flex; align-items: center; gap: 18px; min-width: 0; }
  .eyebrow { flex: none; }
  .lead-strip { flex: 1; display: grid; grid-auto-flow: column; grid-auto-columns: minmax(0, 1fr); gap: 5px; min-width: 0; }
  .ls-cell {
    display: flex; flex-direction: column; align-items: center; gap: 4px; padding: 4px 0 5px; min-width: 0;
    border: 1px solid transparent; border-radius: 7px; background: #ffffff06; color: var(--text-primary); cursor: pointer; font: inherit;
  }
  .ls-cell:hover { background: #ffffff0e; }
  .ls-cell.selected { background: #6eb5ff24; border-color: #6eb5ff; }
  .ls-day { font-size: 12px; font-weight: 600; line-height: 1; color: #c9d0da; }
  .ls-cell.selected .ls-day { color: #fff; }
  .ls-pill.foreign { color: var(--text-secondary); background: #ffffff08; }
  .ls-pill { font-size: 10px; font-weight: 600; line-height: 1; padding: 4px 2px; border: 1px solid #ffffff1a; border-radius: 4px; width: 38px; text-align: center; overflow: hidden; }
</style>
