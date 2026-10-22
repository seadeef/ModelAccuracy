<script>
  import { untrack } from 'svelte';
  import { ui } from '../state.svelte.js';
  import {
    arData,
    ensureArManifest,
    spanBounds,
    dayMs,
    isoFromMs,
    DAY_MS,
    windowIndices,
    firstWithFootprints,
    selectFrame,
  } from '../arManifest.svelte.js';

  /**
   * Headless AR coordinator. The visible timeline (range drill, day cells,
   * frame scrubber, episode markers) lives in the stats panel (ArTimeline);
   * this component owns the invariants that must hold even when the panel is
   * closed (no basin selected yet):
   *  - the manifest is loaded on first AR-mode entry,
   *  - a range always exists (defaults to the full data span),
   *  - a single target day is always selected (defaults to the range middle),
   *  - the map's footprint frame stays inside the current range.
   */

  $effect(() => {
    if (ui.mode === 'atmospheric-rivers') ensureArManifest();
  });

  // Range + target-day invariants. Frame/target writes happen inside untrack so
  // scrubbing and day picks never re-trigger this effect through their own writes.
  $effect(() => {
    const frames = arData.frames;
    const start = ui.arRangeStart;
    const end = ui.arRangeEnd;
    const target = ui.arTargetDate;
    if (!frames.length) return;
    const span = spanBounds();
    if (!span) return;
    untrack(() => {
      if (!start || !end) {
        ui.arRangeStart = isoFromMs(span.startMs);
        ui.arRangeEnd = isoFromMs(span.endMs);
        return; // this write re-runs the effect with a valid range
      }
      if (!target || target < start || target > end) {
        const days = Math.round((dayMs(end) - dayMs(start)) / DAY_MS);
        ui.arTargetDate = isoFromMs(dayMs(start) + Math.floor(days / 2) * DAY_MS);
      }
    });
  });

  // Keep the map's footprint frame inside the current range. The scrubber is
  // user-driven; this only re-lands the frame when the range excludes it.
  $effect(() => {
    const frames = arData.frames;
    const start = ui.arRangeStart;
    const end = ui.arRangeEnd;
    const idx = ui.arFrameIndex;
    const path = ui.arFramePath;
    if (!frames.length || !start || !end) return;
    untrack(() => {
      const win = windowIndices({ start, end });
      if (!win) return;
      if (!path || idx < win.lo || idx > win.hi) {
        selectFrame(firstWithFootprints(win.lo, win.hi));
      }
    });
  });
</script>
