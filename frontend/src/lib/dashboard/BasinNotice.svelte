<script>
  /** Empty state for the analyses that need a region's daily forecast history. */
  import { dash } from './dashData.svelte.js';
  let { children } = $props();
</script>

{#if dash.basinSeries}
  {@render children?.()}
{:else}
  <div class="notice">
    {#if dash.basinLoading}
      Loading the region's daily forecast history…
    {:else}
      No daily forecast history here: the region lies off the verification grid (20–55°N, 130–60°W) or covers only water.
    {/if}
  </div>
{/if}

<style>
  .notice { flex: 1; display: flex; align-items: center; justify-content: center; text-align: center; color: var(--text-secondary); font-size: 13.5px; padding: 0 12%; line-height: 1.5; }
</style>
