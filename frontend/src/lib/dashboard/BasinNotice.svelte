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
      Loading the basin's forecast history…
    {:else}
      Storm analyses cover the river basins of California, the Pacific Northwest and the Upper Colorado. Pick one with the watershed tool.
    {/if}
  </div>
{/if}

<style>
  .notice { flex: 1; display: flex; align-items: center; justify-content: center; text-align: center; color: var(--text-secondary); font-size: 13.5px; padding: 0 12%; line-height: 1.5; }
</style>
