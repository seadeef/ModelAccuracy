<script>
  /** Empty state for basin-only analyses. */
  import { ui } from '../state.svelte.js';
  import { dash } from './dashData.svelte.js';
  let { children } = $props();
</script>

{#if dash.basinSeries}
  {@render children?.()}
{:else}
  <div class="notice">
    {#if dash.basinLoading}
      Loading basin forecast series…
    {:else if ui.selectedRegion?.type === 'huc'}
      Basin analyses use the per-basin forecast history, which is computed for the HUC6 basins of California, the Pacific Northwest and the Upper Colorado. This watershed is outside those regions.
    {:else}
      Basin analyses need a HUC6 basin: pick one with the watershed tool (drill to the six-digit level) or the storm-mode basin picker. Any basin in California, the Pacific Northwest or the Upper Colorado works.
    {/if}
  </div>
{/if}

<style>
  .notice { flex: 1; display: flex; align-items: center; justify-content: center; text-align: center; color: var(--text-secondary); font-size: 12.5px; padding: 0 12%; line-height: 1.5; }
</style>
