<script module>
  /** Once per page load: the panel seeds the range on the first AR-mode entry only. */
  let seeded = false;
</script>

<script>
  /**
   * Standalone atmospheric-river panel for the bare map: the same drillable
   * timeline the dashboard's AR tab shows, floating over the map so AR mode
   * works without a region selected. The needle scrubs the footprints on the
   * map; picking any region with the toolbar opens the dashboard on the same
   * range (the dashboard's AR tab then takes over this timeline).
   */
  import { untrack } from 'svelte';
  import { ui } from '../state.svelte.js';
  import { arData, spanBounds, isoFromMs, selectDay } from '../arManifest.svelte.js';
  import { transitionAnalysisMode } from '../modeController.js';
  import { fmtRange, regionStormsFor, stormItem, STORM_REGIONS } from '../dashboard/basinScope.svelte.js';
  import { rangeOrigin } from '../dashboard/dashData.svelte.js';
  import { fitText } from '../fitText.js';
  import { glyphStorm } from '../appIcons.js';
  import ArRangeControl from './ArRangeControl.svelte';
  import { fetchStormCatalog } from '../api.js';

  /** Every notable AR storm from every region with a catalog, newest first. With no region
   *  selected there is nothing to limit them to; picking a region opens the dashboard, whose
   *  picker keeps only the storms that reached it. */
  let storms = $state([]);
  $effect(() => {
    queueMicrotask(async () => {
      const cats = await Promise.all(Object.keys(STORM_REGIONS).map((h) => fetchStormCatalog(h)));
      storms = regionStormsFor(null, cats).filter((e) => e.notable).reverse().map((e) => stormItem(e));
    });
  });
  /** First entry into AR mode lands on the latest storm, not on frame one of the record
   *  (Jan 2025 with two footprints): the range control's "Full record" chip is one click away.
   *  Only when the range is still the untouched default span — a range the user set is kept. */
  $effect(() => {
    const first = storms[0];
    const frames = arData.frames;
    if (seeded || !first || !frames.length) return;
    const span = spanBounds();
    const untouched = !!span && ui.arRangeStart === isoFromMs(span.startMs) && ui.arRangeEnd === isoFromMs(span.endMs) && !ui.arEpisodeId;
    seeded = true;
    if (!untouched) return;
    untrack(() => {
      ui.arRangeStart = first.start;
      ui.arRangeEnd = first.end;
      rangeOrigin.regionKey = 'panel';
      selectDay(first.peak && first.peak >= first.start && first.peak <= first.end ? first.peak : first.start);
    });
  });

  /** Rendered height, bound by the app so the map key can sit above the panel. */
  let { height = $bindable(0) } = $props();
  let collapsed = $state(false);

  const range = $derived(fmtRange());
  const subLine = 'Drag the slider to move through time; pick any region with the tools on the left to open its storm analysis for this range';
</script>

<section class="ar-panel" class:collapsed aria-label="Atmospheric-river timeline" bind:clientHeight={height}>
  <header class="head">
    <svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" class="glyph" aria-hidden="true">{@html glyphStorm}</svg>
    <div class="titles">
      <div class="eyebrow" use:fitText={{ min: 9 }}>Atmospheric rivers · <em class="accent">{range || 'loading…'}</em></div>
      <div class="cap sub" use:fitText={{ min: 9 }}>{subLine}</div>
    </div>
    <button type="button" class="ctl" title={collapsed ? 'Show the timeline' : 'Hide the timeline (footprints stay on the map)'} aria-expanded={!collapsed} onclick={() => { collapsed = !collapsed; }}>
      <svg class="chev" class:flip={collapsed} width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 4.5L6 7.5L9 4.5"/></svg>
    </button>
    <button type="button" class="ctl" title="Leave atmospheric-river mode" aria-label="Leave atmospheric-river mode" onclick={() => transitionAnalysisMode('accuracy')}>✕</button>
  </header>
  {#if !collapsed}
    <div class="body">
      <ArRangeControl episodes={[]} {storms} />
    </div>
  {/if}
</section>

<style>
  .ar-panel {
    position: absolute;
    left: 50%;
    bottom: max(14px, env(safe-area-inset-bottom, 0px));
    transform: translateX(-50%);
    z-index: 12;
    width: min(960px, calc(100% - 28px));
    max-height: min(46vh, 420px);
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 10px 14px 12px;
    background: var(--panel-bg);
    backdrop-filter: blur(20px) saturate(1.4);
    -webkit-backdrop-filter: blur(20px) saturate(1.4);
    border: 1px solid var(--panel-border);
    border-radius: 14px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
    overflow: hidden;
  }
  .ar-panel.collapsed { max-height: none; }
  .head { display: flex; align-items: center; gap: 10px; min-width: 0; flex: none; }
  .glyph { flex: none; color: #b9a6ff; }
  .titles { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
  .titles .eyebrow, .titles .sub { max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .accent { color: #b9a6ff; }
  .ctl {
    flex: none; font: inherit; font-size: 13px; line-height: 1; width: 26px; height: 26px; border-radius: 7px;
    background: var(--surface); border: 1px solid var(--panel-border); color: var(--text-secondary); cursor: pointer;
  }
  .ctl:hover { color: var(--text-primary); background: var(--hover-bg); }
  .ctl .chev { display: block; margin: 0 auto; transition: transform .15s; }
  .ctl .chev.flip { transform: rotate(180deg); }
  .body { flex: 1; min-height: 0; overflow: auto; }
  @media (max-width: 640px) {
    .ar-panel { width: calc(100% - 16px); padding: 8px 10px 10px; max-height: 52vh; }
  }
</style>
