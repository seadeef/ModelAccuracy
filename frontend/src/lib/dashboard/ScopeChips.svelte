<script>
  /** Date population for the pooled basin analyses (R, heavy rain): all / AR / non-AR days, or the storm picked in the strip. */
  import { ui } from '../state.svelte.js';
  import { SCOPES } from '../basinAnalysis.js';
  import { fmtRange } from './basinScope.svelte.js';
  const label = (s) => (s.key === 'range' ? (fmtRange() || 'Range') : s.label);
</script>

<span class="chips" title="Days pooled for this analysis. AR membership comes from the basin's tARget episode catalog; intervals resample whole AR episodes.">
  {#each SCOPES as s}
    <button type="button" class="chip" class:active={ui.analysisScope === s.key} title={s.key === 'range' ? 'The storm or range picked in the tab strip' : s.title} onclick={() => { ui.analysisScope = s.key; }}>{label(s)}</button>
  {/each}
</span>
