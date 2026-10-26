<script>
  /** Date population for the pooled basin analyses (R, heavy rain): all / AR / non-AR days, or the storm picked in the strip. */
  import { ui } from '../state.svelte.js';
  import { SCOPES } from '../basinAnalysis.js';
  import { fmtRange, arScopesAvailable, activeScope } from './basinScope.svelte.js';
  const label = (s) => (s.key === 'range' ? (fmtRange() || 'Range') : s.label);
  /** AR / non-AR need the basin's episode catalog; hide them where there is none. */
  const shown = $derived(SCOPES.filter((s) => arScopesAvailable() || (s.key !== 'ar_all' && s.key !== 'non_ar')));
</script>

<span class="chips" title={arScopesAvailable()
  ? 'Days pooled for this analysis. AR membership comes from the basin\'s tARget episode catalog; intervals resample whole AR episodes.'
  : 'Days pooled for this analysis. AR / non-AR populations need an episode catalog, which exists for the exported river basins only; intervals resample weekly blocks of dates.'}>
  {#each shown as s (s.key)}
    <button type="button" class="chip" class:active={activeScope() === s.key} title={s.key === 'range' ? 'The storm or range picked in the tab strip' : s.title} onclick={() => { ui.analysisScope = s.key; }}>{label(s)}</button>
  {/each}
</span>
