<script>
  import { untrack } from 'svelte';
  import { appConfig, ui } from '../state.svelte.js';
  import { modelPalette } from '../palette.js';
  import {
    arData,
    spanBounds,
    dayMs,
    isoFromMs,
    DAY_MS,
    windowIndices,
    selectFrame,
  } from '../arManifest.svelte.js';

  /**
   * Hierarchical AR date-range timeline (replaces the map-floating range
   * slider). One drill path — years → seasons → months → weeks → days — where
   * every drill level IS the selected range; at week/episode depth the cells
   * become individual days and clicking one moves the inspected day
   * (ui.arTargetDate) while the range stays put.
   *
   * Every row (cells, episode lane, scrubber) shares the same width and maps
   * positions through CELL space — a timestamp lands at (cellIndex + fraction
   * inside that cell) / cellCount — so the episode markers, the scrub needle,
   * and the full-height position line all line up exactly with the cells above
   * them even when cells cover unequal time spans (months, seasons). To keep
   * that mapping total, a "year" is the meteorological year (Dec 1 Y-1 …
   * Nov 30 Y), which DJF/MAM/JJA/SON tile with no remainder.
   */
  let { episodes = [], dayWinners = null, winnerLabel = '' } = $props();

  const palette = $derived(modelPalette(appConfig.models));

  /** Drill path: each deeper field only meaningful when the previous is set. */
  let drill = $state({ year: null, season: null, monthMs: null, weekMs: null });

  const span = $derived.by(() => {
    void arData.frames;
    return spanBounds();
  });

  // ── Calendar math (all UTC) ──
  const SEASONS = [
    { key: 'djf', label: 'DJF', months: [-1, 0, 1] },  // Dec (Y-1), Jan, Feb
    { key: 'mam', label: 'MAM', months: [2, 3, 4] },
    { key: 'jja', label: 'JJA', months: [5, 6, 7] },
    { key: 'son', label: 'SON', months: [8, 9, 10] },
  ];
  const MONTH_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  function monthStartMs(year, monthIdx) {
    return Date.UTC(year, monthIdx, 1);
  }
  function monthEndMs(year, monthIdx) {
    return Date.UTC(year, monthIdx + 1, 1) - DAY_MS; // last day, day-floored
  }
  function seasonBounds(year, key) {
    const s = SEASONS.find((x) => x.key === key);
    const first = s.months[0];
    const last = s.months[s.months.length - 1];
    return {
      startMs: Date.UTC(year, first, 1),
      endMs: Date.UTC(year, last + 1, 1) - DAY_MS,
    };
  }
  /** Meteorological year Y = Dec 1 (Y-1) … Nov 30 (Y). */
  function metYearOf(ms) {
    const d = new Date(ms);
    return d.getUTCMonth() === 11 ? d.getUTCFullYear() + 1 : d.getUTCFullYear();
  }
  function metYearBounds(year) {
    return {
      startMs: Date.UTC(year - 1, 11, 1),
      endMs: Date.UTC(year, 11, 1) - DAY_MS,
    };
  }
  /** Monday-start week containing `ms`. */
  function weekStartOf(ms) {
    const dow = new Date(ms).getUTCDay(); // 0=Sun
    const back = (dow + 6) % 7;
    return ms - back * DAY_MS;
  }
  function overlaps(aStart, aEnd, bStart, bEnd) {
    return aStart <= bEnd && aEnd >= bStart;
  }
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

  function fmtDay(ms) {
    return new Date(ms).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  }
  function fmtWeekLabel(startMs, endMs) {
    const a = new Date(startMs);
    const b = new Date(endMs);
    return a.getUTCMonth() === b.getUTCMonth()
      ? `${MONTH_SHORT[a.getUTCMonth()]} ${a.getUTCDate()}–${b.getUTCDate()}`
      : `${fmtDay(startMs)} – ${fmtDay(endMs)}`;
  }
  function fmtFrameTime(value) {
    if (!value) return '';
    const d = new Date(value);
    if (Number.isNaN(d.valueOf())) return String(value);
    return `${fmtDay(d.valueOf())} · ${String(d.getUTCHours()).padStart(2, '0')}:00 UTC`;
  }

  // ── Active episode (constrains the range and takes over the leaf level) ──
  const activeEpisode = $derived(
    ui.arEpisodeId ? (episodes ?? []).find((e) => e.id === ui.arEpisodeId) ?? null : null,
  );

  const level = $derived(
    activeEpisode ? 'episode'
      : drill.weekMs != null ? 'days'
      : drill.monthMs != null ? 'weeks'
      : drill.season != null ? 'months'
      : drill.year != null ? 'seasons'
      : 'years',
  );
  const dayLevel = $derived(level === 'days' || level === 'episode');

  /** Bounds of the current drill node, clipped to the data span. */
  const nodeBounds = $derived.by(() => {
    if (!span) return null;
    let b;
    if (activeEpisode) {
      b = { startMs: dayMs(activeEpisode.start_date), endMs: dayMs(activeEpisode.end_date) };
    } else if (drill.weekMs != null) {
      b = { startMs: drill.weekMs, endMs: drill.weekMs + 6 * DAY_MS };
      if (drill.monthMs != null) {
        const m = new Date(drill.monthMs);
        b.startMs = Math.max(b.startMs, drill.monthMs);
        b.endMs = Math.min(b.endMs, monthEndMs(m.getUTCFullYear(), m.getUTCMonth()));
      }
    } else if (drill.monthMs != null) {
      const m = new Date(drill.monthMs);
      b = { startMs: drill.monthMs, endMs: monthEndMs(m.getUTCFullYear(), m.getUTCMonth()) };
    } else if (drill.season != null) {
      b = seasonBounds(drill.year, drill.season);
    } else if (drill.year != null) {
      b = metYearBounds(drill.year);
    } else {
      b = { startMs: span.startMs, endMs: span.endMs };
    }
    return {
      startMs: clamp(b.startMs, span.startMs, span.endMs),
      endMs: clamp(b.endMs, span.startMs, span.endMs),
    };
  });

  // The drill node owns the range whenever no episode is active ("going back to
  // no episode deconstrains the range" = the range snaps back to the node).
  $effect(() => {
    const node = nodeBounds;
    const epId = ui.arEpisodeId;
    if (!node || epId) return;
    untrack(() => {
      const s = isoFromMs(node.startMs);
      const e = isoFromMs(node.endMs);
      if (ui.arRangeStart !== s) ui.arRangeStart = s;
      if (ui.arRangeEnd !== e) ui.arRangeEnd = e;
    });
  });

  // ── Cells for the current level ──
  const cells = $derived.by(() => {
    if (!span || !nodeBounds) return [];
    const out = [];
    if (level === 'years') {
      const y0 = metYearOf(span.startMs);
      const y1 = metYearOf(span.endMs);
      for (let y = y0; y <= y1; y++) {
        const b = metYearBounds(y);
        out.push({
          key: `y${y}`,
          label: String(y),
          startMs: b.startMs,
          endMs: b.endMs,
          onpick: () => { clearEpisode(); drill = { year: y, season: null, monthMs: null, weekMs: null }; },
        });
      }
    } else if (level === 'seasons') {
      for (const s of SEASONS) {
        const b = seasonBounds(drill.year, s.key);
        out.push({
          key: s.key,
          label: s.label,
          startMs: b.startMs,
          endMs: b.endMs,
          onpick: () => { clearEpisode(); drill = { ...drill, season: s.key, monthMs: null, weekMs: null }; },
        });
      }
    } else if (level === 'months') {
      const s = SEASONS.find((x) => x.key === drill.season);
      for (const mOff of s.months) {
        const start = monthStartMs(drill.year, mOff);
        const d = new Date(start);
        out.push({
          key: `m${start}`,
          label: `${MONTH_SHORT[d.getUTCMonth()]} ${d.getUTCFullYear()}`,
          startMs: start,
          endMs: monthEndMs(drill.year, mOff),
          onpick: () => { clearEpisode(); drill = { ...drill, monthMs: start, weekMs: null }; },
        });
      }
    } else if (level === 'weeks') {
      const m = new Date(drill.monthMs);
      const mEnd = monthEndMs(m.getUTCFullYear(), m.getUTCMonth());
      let w = weekStartOf(drill.monthMs);
      while (w <= mEnd) {
        const cellStart = Math.max(w, drill.monthMs);
        const cellEnd = Math.min(w + 6 * DAY_MS, mEnd);
        const wk = w;
        out.push({
          key: `w${wk}`,
          label: fmtWeekLabel(cellStart, cellEnd),
          startMs: cellStart,
          endMs: cellEnd,
          onpick: () => { clearEpisode(); drill = { ...drill, weekMs: wk }; },
        });
        w += 7 * DAY_MS;
      }
    } else {
      // days | episode — one cell per day; clicking moves the inspected day.
      for (let t = nodeBounds.startMs; t <= nodeBounds.endMs; t += DAY_MS) {
        const iso = isoFromMs(t);
        out.push({
          key: iso,
          label: String(new Date(t).getUTCDate()),
          sub: new Date(t).toLocaleDateString('en-US', { weekday: 'short', timeZone: 'UTC' }),
          startMs: t,
          endMs: t,
          iso,
          onpick: () => { ui.arTargetDate = iso; },
        });
      }
    }
    for (const c of out) {
      c.enabled = overlaps(c.startMs, c.endMs, span.startMs, span.endMs);
    }
    return out;
  });

  // A level with a single reachable cell carries no information (the lone
  // "2025" row): drill straight through it. Terminates at day depth.
  $effect(() => {
    const list = cells;
    const lvl = level;
    if (!span || activeEpisode || lvl === 'days' || lvl === 'episode') return;
    const enabled = list.filter((c) => c.enabled);
    if (enabled.length !== 1) return;
    untrack(() => enabled[0].onpick());
  });

  function clearEpisode() {
    if (ui.arEpisodeId) ui.arEpisodeId = null;
    ui.arEpisodeNote = '';
  }

  /** Episode overlap summary for one coarse cell (count + AR-active days). */
  function cellEpisodeInfo(cell) {
    let count = 0;
    let days = 0;
    for (const ep of episodes ?? []) {
      const s = dayMs(ep.start_date);
      const e = dayMs(ep.end_date);
      if (s <= cell.endMs && e >= cell.startMs) {
        count += 1;
        const lo = Math.max(s, cell.startMs);
        const hi = Math.min(e, cell.endMs);
        days += Math.round((hi - lo) / DAY_MS) + 1;
      }
    }
    return { count, days };
  }

  /** ISO days covered by any episode (marks AR days on the day cells). */
  const episodeDaySet = $derived.by(() => {
    const set = new Set();
    for (const ep of episodes ?? []) {
      for (let t = dayMs(ep.start_date); t <= dayMs(ep.end_date); t += DAY_MS) {
        set.add(isoFromMs(t));
      }
    }
    return set;
  });

  // ── Cell-space position mapping (shared by episode lane + scrubber) ──
  /** 0..1 across the cell row for a UTC ms instant; exact per-cell alignment. */
  function cellFracForMs(ms) {
    const list = cells;
    const n = list.length;
    if (!n) return 0;
    for (let i = 0; i < n; i++) {
      const c = list[i];
      const end = c.endMs + DAY_MS; // exclusive
      if (ms < c.startMs) return i / n;
      if (ms < end) return (i + (ms - c.startMs) / (end - c.startMs)) / n;
    }
    return 1;
  }
  /** Inverse of cellFracForMs (0..1 → UTC ms inside the matching cell). */
  function msForCellFrac(frac) {
    const list = cells;
    const n = list.length;
    if (!n) return NaN;
    const x = clamp(frac, 0, 1) * n;
    const i = Math.min(n - 1, Math.floor(x));
    const c = list[i];
    const end = c.endMs + DAY_MS;
    return c.startMs + (x - i) * (end - c.startMs);
  }

  // ── Breadcrumb ──
  const crumbs = $derived.by(() => {
    const list = [{ label: 'All', onpick: () => { clearEpisode(); drill = { year: null, season: null, monthMs: null, weekMs: null }; } }];
    if (drill.year != null) {
      const y = drill.year;
      list.push({ label: String(y), onpick: () => { clearEpisode(); drill = { year: y, season: null, monthMs: null, weekMs: null }; } });
    }
    if (drill.season != null) {
      const s = drill.season;
      list.push({
        label: SEASONS.find((x) => x.key === s)?.label ?? s,
        onpick: () => { clearEpisode(); drill = { ...drill, season: s, monthMs: null, weekMs: null }; },
      });
    }
    if (drill.monthMs != null) {
      const mMs = drill.monthMs;
      const d = new Date(mMs);
      list.push({
        label: MONTH_SHORT[d.getUTCMonth()],
        onpick: () => { clearEpisode(); drill = { ...drill, monthMs: mMs, weekMs: null }; },
      });
    }
    if (drill.weekMs != null && !activeEpisode) {
      list.push({ label: 'Week', onpick: null });
    }
    if (activeEpisode) {
      list.push({
        label: `Episode ${fmtDay(dayMs(activeEpisode.start_date))} – ${fmtDay(dayMs(activeEpisode.end_date))}`,
        onpick: null,
        episode: true,
      });
    }
    return list;
  });

  // ── Episode markers over the current range (cell-space positions) ──
  const rangeStartMs = $derived(ui.arRangeStart ? dayMs(ui.arRangeStart) : NaN);
  const rangeEndMs = $derived(ui.arRangeEnd ? dayMs(ui.arRangeEnd) + DAY_MS : NaN); // exclusive

  const visibleEpisodes = $derived.by(() => {
    if (!Number.isFinite(rangeStartMs) || !Number.isFinite(rangeEndMs) || !cells.length) return [];
    return (episodes ?? [])
      .map((ep) => {
        const s = dayMs(ep.start_date);
        const e = dayMs(ep.end_date) + DAY_MS;
        if (!(s < rangeEndMs && e > rangeStartMs)) return null;
        const leftFrac = cellFracForMs(clamp(s, rangeStartMs, rangeEndMs));
        const rightFrac = cellFracForMs(clamp(e, rangeStartMs, rangeEndMs));
        const widthPct = Math.max((rightFrac - leftFrac) * 100, 1.2);
        return {
          ep,
          leftPct: leftFrac * 100,
          widthPct,
          activeDays: ep.active_days,
          wide: widthPct > 6,
        };
      })
      .filter(Boolean);
  });

  function pickEpisode(ep) {
    ui.arEpisodeId = ep.id;
    ui.arRangeStart = ep.start_date;
    ui.arRangeEnd = ep.end_date;
    ui.arEpisodeNote = '';
  }

  // ── Frame scrubber: one needle, cell-aligned, independent of the day pick ──
  const scrubWin = $derived.by(() => {
    void arData.frames;
    if (!ui.arRangeStart || !ui.arRangeEnd) return null;
    return windowIndices({ start: ui.arRangeStart, end: ui.arRangeEnd });
  });
  const scrubFrame = $derived(arData.frames[ui.arFrameIndex] ?? null);
  const scrubFrac = $derived.by(() => {
    const t = Date.parse(scrubFrame?.time);
    if (!Number.isFinite(t) || !cells.length) return 0;
    return cellFracForMs(t);
  });
  /** ISO day the scrubbed frame falls on (day-cell highlight at week depth). */
  const scrubDayIso = $derived.by(() => {
    const t = Date.parse(scrubFrame?.time);
    return Number.isFinite(t) ? isoFromMs(Math.floor(t / DAY_MS) * DAY_MS) : null;
  });

  let scrubTrackEl = $state();
  let scrubbing = $state(false);

  function scrubToClientX(clientX) {
    const win = scrubWin;
    if (!win || !scrubTrackEl) return;
    const rect = scrubTrackEl.getBoundingClientRect();
    const frac = clamp((clientX - rect.left) / Math.max(1, rect.width), 0, 1);
    const t = msForCellFrac(frac);
    if (!Number.isFinite(t)) return;
    let bestIdx = win.lo;
    let bestDist = Infinity;
    for (let i = win.lo; i <= win.hi; i++) {
      const ft = Date.parse(arData.frames[i]?.time);
      const dist = Math.abs(ft - t);
      if (dist < bestDist) { bestDist = dist; bestIdx = i; }
    }
    selectFrame(bestIdx);
  }
  function startScrub(e) {
    scrubbing = true;
    try { e.currentTarget.setPointerCapture?.(e.pointerId); } catch { /* ignore */ }
    scrubToClientX(e.clientX);
  }
  function moveScrub(e) {
    if (scrubbing) scrubToClientX(e.clientX);
  }
  function endScrub() {
    scrubbing = false;
  }

  function modelLabel(key) {
    return appConfig.models.find((m) => m.key === key)?.label ?? key;
  }
</script>

<svelte:window
  onpointermove={scrubbing ? moveScrub : null}
  onpointerup={scrubbing ? endScrub : null}
/>

<div class="ar-timeline">
  {#if !arData.frames.length}
    <div class="tl-msg">
      {arData.error ? `${arData.error}. Run export_static.py --ar to publish the dataset.`
        : 'Loading atmospheric-river dates…'}
    </div>
  {:else}
    <div class="tl-crumbs">
      {#each crumbs as crumb, i}
        {#if i > 0}<span class="crumb-sep">›</span>{/if}
        {#if crumb.onpick && i < crumbs.length - 1}
          <button type="button" class="crumb" class:crumb-episode={crumb.episode} onclick={crumb.onpick}>{crumb.label}</button>
        {:else}
          <span class="crumb crumb-current" class:crumb-episode={crumb.episode}>{crumb.label}</span>
        {/if}
      {/each}
      <span class="tl-hint">
        {#if dayLevel}
          Click a day to inspect its revisions · drag the needle to scrub footprints
        {:else}
          Click a period to narrow the range
        {/if}
      </span>
    </div>

    <div class="tl-body">
      <div class="tl-cells" class:tl-cells-days={dayLevel}>
        {#each cells as cell (cell.key)}
          {@const info = dayLevel ? null : cellEpisodeInfo(cell)}
          <button
            type="button"
            class="tl-cell"
            class:tl-cell-day={dayLevel}
            class:selected={dayLevel && cell.iso === ui.arTargetDate}
            class:scrub-day={dayLevel && cell.iso === scrubDayIso}
            class:ar-day={dayLevel && episodeDaySet.has(cell.iso)}
            disabled={!cell.enabled}
            title={dayLevel
              ? `Inspect ${fmtDay(cell.startMs)}${episodeDaySet.has(cell.iso) ? ' (AR episode day)' : ''}`
              : `${fmtDay(cell.startMs)} – ${fmtDay(cell.endMs)}${info?.count ? ` · ${info.count} episode${info.count === 1 ? '' : 's'}` : ' · no AR episodes'}`}
            onclick={cell.onpick}
          >
            {#if dayLevel}
              <span class="tl-cell-sub">{cell.sub}</span>
              <span class="tl-cell-label">{cell.label}</span>
              {#if dayWinners?.get(cell.iso)}
                {@const wk = dayWinners.get(cell.iso)}
                {@const pc = palette[wk]}
                <span
                  class="tl-winner"
                  style:background={pc?.bg}
                  style:color={pc?.fg}
                  style:border-color={pc?.border}
                  title={`Best model this day${winnerLabel ? ` (${winnerLabel})` : ''}: ${modelLabel(wk)}`}
                >{pc?.ident ?? wk}</span>
              {:else}
                <span class="tl-winner tl-winner-empty">&mdash;</span>
              {/if}
            {:else}
              <span class="tl-cell-label">{cell.label}</span>
              {#if cell.enabled}
                <span class="tl-cell-info" class:tl-cell-info-none={!info.count}>
                  {info.count ? `${info.count} ep · ${info.days} d AR` : 'no AR'}
                </span>
              {/if}
            {/if}
          </button>
        {/each}
      </div>

      <div class="tl-episodes" class:tl-episodes-empty={!visibleEpisodes.length}>
        {#each visibleEpisodes as v (v.ep.id)}
          <button
            type="button"
            class="ep-marker"
            class:active={v.ep.id === ui.arEpisodeId}
            class:hovered={v.ep.id === ui.arHoverEpisodeId}
            style:left="{v.leftPct}%"
            style:width="{v.widthPct}%"
            title={`AR episode ${fmtDay(dayMs(v.ep.start_date))} – ${fmtDay(dayMs(v.ep.end_date))} · ${v.ep.active_days} active d — click to focus`}
            onclick={() => pickEpisode(v.ep)}
          >{#if v.wide}<span class="ep-marker-label">{v.activeDays}d</span>{/if}</button>
        {/each}
        {#if !visibleEpisodes.length}
          <span class="tl-episodes-none">no episodes in range</span>
        {/if}
      </div>

      {#if scrubWin}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div class="tl-scrub" bind:this={scrubTrackEl} onpointerdown={startScrub}>
          <div class="tl-scrub-grip" style:left="{scrubFrac * 100}%"></div>
        </div>
        <!-- Full-height position needle: ties the scrubbed instant to the cells
             and episode lane above (all rows share cell-space x). -->
        <div class="tl-needle" style:left="{scrubFrac * 100}%" aria-hidden="true"></div>
      {/if}
    </div>

    <div class="tl-caption">
      <span class="tl-caption-left">
        <span class="ep-swatch" aria-hidden="true"></span>
        {episodes?.length ? `${episodes.length} AR episode${episodes.length === 1 ? '' : 's'} for this basin` : 'AR episode lane'}
      </span>
      {#if scrubFrame}
        <span class="tl-caption-right" title="Map footprint frame at the needle (independent of the inspected day).">
          ▮ {fmtFrameTime(scrubFrame.time)} · {scrubFrame.count ?? 0} footprint{(scrubFrame.count ?? 0) === 1 ? '' : 's'}
        </span>
      {/if}
    </div>
  {/if}
</div>

<style>
  .ar-timeline {
    --ar-accent: #b9a6ff;
    --ar-glow: rgba(150, 130, 255, 0.14);
    display: flex;
    flex-direction: column;
    gap: 6px;
    flex: 1 1 auto;
    min-height: 0;
  }
  .tl-msg {
    font-size: 12px;
    color: var(--text-secondary);
    padding: 6px 0;
  }
  .tl-crumbs {
    display: flex;
    align-items: baseline;
    gap: 4px;
    flex-wrap: wrap;
    min-width: 0;
  }
  .crumb {
    font-family: inherit;
    font-size: 11px;
    font-weight: 600;
    color: var(--text-secondary);
    background: transparent;
    border: none;
    border-radius: 4px;
    padding: 1px 4px;
    cursor: pointer;
  }
  .crumb:hover { color: var(--ar-accent); background: var(--ar-glow); }
  .crumb-current {
    color: var(--text-primary);
    cursor: default;
  }
  .crumb-episode { color: var(--ar-accent); }
  .crumb-sep { color: var(--text-secondary); font-size: 10px; opacity: 0.7; }
  .tl-hint {
    margin-left: auto;
    font-size: 10px;
    color: var(--text-secondary);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  /* Rows stack inside one positioned body so the needle can span them all and
     every row shares the exact same width (cell-space alignment). */
  .tl-body {
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  /* Contiguous table-profile cells: no gaps (positions map exactly), separators
     between cells, rounded outer corners on the row. */
  .tl-cells {
    display: flex;
    border-radius: 7px;
    overflow: hidden;
    background: var(--surface);
    box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--panel-border) 80%, transparent);
  }
  .tl-cell {
    flex: 1 1 0;
    min-width: 0;
    min-height: 46px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 3px;
    font-family: inherit;
    padding: 7px 2px;
    border: none;
    border-right: 1px solid color-mix(in srgb, var(--panel-border) 70%, transparent);
    background: transparent;
    color: var(--text-primary);
    cursor: pointer;
    transition: background-color 0.12s, box-shadow 0.12s;
  }
  .tl-cell:last-child { border-right: none; }
  .tl-cell:hover:not(:disabled) {
    background: var(--ar-glow);
    box-shadow: inset 0 -2px 0 0 color-mix(in srgb, var(--ar-accent) 55%, transparent);
  }
  .tl-cell:disabled {
    opacity: 0.28;
    cursor: default;
  }
  .tl-cell-label { font-size: 12px; font-weight: 600; line-height: 1.2; }
  .tl-cell-info {
    font-size: 9px;
    font-weight: 600;
    letter-spacing: 0.3px;
    color: var(--ar-accent);
    white-space: nowrap;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .tl-cell-info-none {
    color: var(--text-secondary);
    font-weight: 400;
    opacity: 0.7;
  }
  .tl-cell-sub {
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    color: var(--text-secondary);
    line-height: 1;
  }
  .tl-cells-days .tl-cell { min-height: 56px; }
  .tl-cell-day.ar-day .tl-cell-sub { color: var(--ar-accent); }
  .tl-cell-day.selected {
    background: color-mix(in srgb, var(--ar-accent) 14%, transparent);
    box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--ar-accent) 65%, transparent);
  }
  /* Day the scrubbed footprint frame falls on: purple underline, distinct from
     the inspected-day box so the two selections read separately. */
  .tl-cell-day.scrub-day {
    box-shadow: inset 0 -2px 0 0 var(--ar-accent);
  }
  .tl-cell-day.selected.scrub-day {
    box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--ar-accent) 65%, transparent),
                inset 0 -2px 0 0 var(--ar-accent);
  }
  .tl-winner {
    display: inline-block;
    margin-top: 1px;
    padding: 1px 5px;
    border-radius: 4px;
    border: 1px solid transparent;
    font-size: 10px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .tl-winner-empty { color: rgba(255,255,255,0.32); }

  /* Episode lane: same width as the cells; markers positioned in cell space. */
  .tl-episodes {
    position: relative;
    height: 16px;
    border-radius: 5px;
    background: color-mix(in srgb, var(--surface) 70%, transparent);
    box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--panel-border) 55%, transparent);
  }
  .tl-episodes-none {
    position: absolute;
    inset: 0;
    display: grid;
    place-items: center;
    font-size: 9px;
    color: var(--text-secondary);
    opacity: 0.7;
    pointer-events: none;
  }
  .ep-marker {
    position: absolute;
    top: 2px;
    bottom: 2px;
    display: grid;
    place-items: center;
    padding: 0;
    border: 1px solid rgba(150, 130, 255, 0.5);
    border-radius: 4px;
    background: rgba(150, 130, 255, 0.3);
    cursor: pointer;
    transition: background-color 0.12s, border-color 0.12s;
    overflow: hidden;
  }
  .ep-marker-label {
    font-size: 8px;
    font-weight: 700;
    color: #e6ddff;
    line-height: 1;
    pointer-events: none;
  }
  .ep-marker:hover,
  .ep-marker.hovered {
    background: rgba(150, 130, 255, 0.6);
    border-color: var(--ar-accent);
  }
  .ep-marker.active {
    background: var(--ar-accent);
    border-color: var(--ar-accent);
  }
  .ep-marker.active .ep-marker-label { color: #17131f; }

  /* Frame scrubber: a single needle on a quiet rail — no range fill. */
  .tl-scrub {
    position: relative;
    height: 12px;
    border-radius: 6px;
    background: var(--surface);
    box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--panel-border) 55%, transparent);
    cursor: ew-resize;
    touch-action: none;
  }
  .tl-scrub-grip {
    position: absolute;
    top: 1px;
    bottom: 1px;
    width: 6px;
    transform: translateX(-50%);
    border-radius: 3px;
    background: var(--ar-accent);
    box-shadow: 0 0 6px rgba(150, 130, 255, 0.5);
    pointer-events: none;
  }
  /* Position needle spanning cells + episode lane + rail. */
  .tl-needle {
    position: absolute;
    top: 0;
    bottom: 0;
    width: 1.5px;
    transform: translateX(-50%);
    background: color-mix(in srgb, var(--ar-accent) 55%, transparent);
    pointer-events: none;
  }

  .tl-caption {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 10px;
    min-width: 0;
  }
  .tl-caption-left,
  .tl-caption-right {
    font-size: 10px;
    color: var(--text-secondary);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .tl-caption-right { color: var(--ar-accent); }
  .ep-swatch {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 2px;
    background: rgba(150, 130, 255, 0.45);
    border: 1px solid rgba(150, 130, 255, 0.6);
    vertical-align: baseline;
    margin-right: 3px;
  }
</style>
