<script>
  /**
   * Atmospheric-river range control: a date range and one slider.
   *
   *   From [date]  –  To [date]                         Full record
   *   [═══════════●══════════════]   Dec 23 · 12:00 UTC · 3 footprints
   *
   * The slider runs over the 6-hourly tARget frames inside the range. Its
   * position is THE time: the map shows that frame's footprints and the storm
   * panes read that day (ui.arTargetDate follows the slider). The track is
   * tinted where footprints exist; purple ticks are the basin's catalogued
   * AR episodes when the caller passes them.
   *
   * State contract: ui.arRangeStart/End (ISO), ui.arTargetDate, ui.arEpisodeId
   * (cleared on a manual range change), the frame via selectFrame().
   */
  import { untrack } from 'svelte';
  import { ui } from '../state.svelte.js';
  import { arData, spanBounds, dayMs, isoFromMs, DAY_MS, windowIndices, selectFrame, selectDay } from '../arManifest.svelte.js';
  import { fitText } from '../fitText.js';
  import { regionKey, rangeOrigin } from '../dashboard/dashData.svelte.js';
  import Dropdown from '../dashboard/Dropdown.svelte';

  /** ``storms``: optional picker items [{ key, label, sub?, start, end, peak? }] (ISO dates) — the
   *  bare-map panel passes the catalogued AR storms; the dashboard's tab strip has its own picker. */
  let { episodes = [], storms = [] } = $props();

  const span = $derived.by(() => { void arData.frames; return spanBounds(); });
  const spanStart = $derived(span ? isoFromMs(span.startMs) : null);
  const spanEnd = $derived(span ? isoFromMs(span.endMs) : null);
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

  function fmtDay(iso) {
    return new Date(dayMs(iso)).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
  }
  function fmtFrame(value) {
    const d = new Date(value ?? '');
    if (Number.isNaN(d.valueOf())) return '';
    return `${d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', timeZone: 'UTC' })} · ${String(d.getUTCHours()).padStart(2, '0')}:00 UTC`;
  }

  function setRange(start, end) {
    if (!span) return;
    let s = clamp(dayMs(start), span.startMs, span.endMs);
    let e = clamp(dayMs(end), span.startMs, span.endMs);
    if (s > e) [s, e] = [e, s];
    ui.arRangeStart = isoFromMs(s);
    ui.arRangeEnd = isoFromMs(e);
    ui.arEpisodeId = null;
    ui.arEpisodeNote = '';
    rangeOrigin.regionKey = null; // a typed range travels to the next region
  }
  const isAll = $derived(!!spanStart && ui.arRangeStart === spanStart && ui.arRangeEnd === spanEnd);
  const rangeDays = $derived(ui.arRangeStart && ui.arRangeEnd
    ? Math.round((dayMs(ui.arRangeEnd) - dayMs(ui.arRangeStart)) / DAY_MS) + 1 : 0);

  // ── Storm picker (standalone panel) ──
  const stormValue = $derived(storms.find((i) => i.start === ui.arRangeStart && i.end === ui.arRangeEnd)?.key ?? 'custom');
  function pickStorm(key) {
    const it = storms.find((i) => i.key === key);
    if (!it) return;
    setRange(it.start, it.end);
    // A storm choice is region-bound even from the bare-map panel: the next region keeps it
    // only if that storm reached it (the 'panel' origin never matches a region key).
    rangeOrigin.regionKey = regionKey(ui.selectedRegion) || 'panel';
    goToDay(it.peak && it.peak >= it.start && it.peak <= it.end ? it.peak : it.start);
  }

  // ── Individual days of a storm-length range ──
  const dayChips = $derived.by(() => {
    if (!rangeDays || rangeDays > 31) return [];
    const out = [];
    for (let t = dayMs(ui.arRangeStart); t <= dayMs(ui.arRangeEnd); t += DAY_MS) {
      const d = new Date(t);
      out.push({ iso: isoFromMs(t), label: `${d.toLocaleDateString('en-US', { weekday: 'short', timeZone: 'UTC' })} ${d.getUTCDate()}` });
    }
    return out;
  });
  /** Move the slider onto a day (its first frame with footprints, else its first frame). */
  const goToDay = selectDay;

  // Frames inside the range; the slider index is clamped into it.
  const win = $derived.by(() => {
    void arData.frames;
    return ui.arRangeStart && ui.arRangeEnd ? windowIndices({ start: ui.arRangeStart, end: ui.arRangeEnd }) : null;
  });
  const frameIdx = $derived(win ? clamp(ui.arFrameIndex, win.lo, win.hi) : 0);
  const frame = $derived(arData.frames[frameIdx] ?? null);
  const frameDay = $derived.by(() => {
    const t = Date.parse(frame?.time);
    return Number.isFinite(t) ? isoFromMs(Math.floor(t / DAY_MS) * DAY_MS) : null;
  });
  // A range set elsewhere (the tab strip's storm picker, the date inputs, a
  // storm pane) can leave the frame outside the window: re-seat it on the
  // inspected day when that day is inside the range, else on the first day.
  // Only then does the slider become the day (the effect below).
  $effect(() => {
    const w = win;
    if (!w) return;
    const idx = ui.arFrameIndex;
    const start = ui.arRangeStart;
    const end = ui.arRangeEnd;
    untrack(() => {
      if (idx >= w.lo && idx <= w.hi) return;
      const t = ui.arTargetDate;
      goToDay(t && t >= start && t <= end ? t : start);
    });
  });
  // The slider is the day: the inspected day follows the frame.
  $effect(() => {
    const day = frameDay;
    if (!day) return;
    untrack(() => { if (ui.arTargetDate !== day) ui.arTargetDate = day; });
  });
  function onSlide(e) { selectFrame(Number(e.target.value)); }

  /** Smooth footprint-activity curve over the range: one point per frame
   *  (3-frame moving average), drawn as an SVG area so it reads as a shape,
   *  not as per-frame bands. */
  const curve = $derived.by(() => {
    if (!win) return null;
    const n = win.hi - win.lo + 1;
    const raw = [];
    for (let i = win.lo; i <= win.hi; i++) raw.push(arData.frames[i]?.count ?? 0);
    const sm = raw.map((_, i) => {
      let s = 0; let k = 0;
      for (let j = Math.max(0, i - 1); j <= Math.min(n - 1, i + 1); j++) { s += raw[j]; k++; }
      return s / k;
    });
    const max = Math.max(1, ...sm);
    const pts = sm.map((v, i) => `${(((i + 0.5) / n) * 100).toFixed(2)},${(100 - (v / max) * 88).toFixed(2)}`);
    return { d: `M0,100 L0,${pts[0].split(',')[1]} L${pts.join(' L')} L100,${pts[pts.length - 1].split(',')[1]} L100,100 Z`, any: max > 1 || raw.some(Boolean) };
  });
  const thumbPct = $derived(win ? (((frameIdx - win.lo) + 0.5) / (win.hi - win.lo + 1)) * 100 : 0);
  /** Episode ticks under the track, as % of the range width. */
  const ticks = $derived.by(() => {
    if (!ui.arRangeStart || !ui.arRangeEnd) return [];
    const s0 = dayMs(ui.arRangeStart);
    const e0 = dayMs(ui.arRangeEnd) + DAY_MS;
    const w = e0 - s0;
    return (episodes ?? []).map((ep) => {
      const s = clamp(dayMs(ep.start_date), s0, e0);
      const e = clamp(dayMs(ep.end_date) + DAY_MS, s0, e0);
      if (e <= s) return null;
      return { ep, left: ((s - s0) / w) * 100, width: Math.max(((e - s) / w) * 100, 0.6) };
    }).filter(Boolean);
  });
  function pickEpisode(ep) {
    ui.arRangeStart = ep.start_date;
    ui.arRangeEnd = ep.end_date;
    ui.arEpisodeId = ep.id;
    ui.arEpisodeNote = '';
    rangeOrigin.regionKey = regionKey(ui.selectedRegion) || null;
  }
</script>

<div class="arc">
  {#if !arData.frames.length}
    <div class="msg">{arData.error ? `${arData.error}. Run export_static.py --ar to publish the dataset.` : 'Loading atmospheric-river dates…'}</div>
  {:else}
    <div class="row">
      {#if storms.length}
        <Dropdown items={storms} value={stormValue} placeholder={isAll ? 'Full record' : 'Custom range'} ariaLabel="Storm" title="Catalogued atmospheric-river storms (California and Pacific Northwest records); picking one sets the range and jumps to its peak day" compact width="212px" onselect={pickStorm} />
      {/if}
      <span class="cap">From</span>
      <input class="date" type="date" value={ui.arRangeStart ?? ''} min={spanStart} max={spanEnd} aria-label="Range start"
        onchange={(e) => { if (e.target.value) setRange(e.target.value, ui.arRangeEnd || e.target.value); }} />
      <span class="cap">to</span>
      <input class="date" type="date" value={ui.arRangeEnd ?? ''} min={spanStart} max={spanEnd} aria-label="Range end"
        onchange={(e) => { if (e.target.value) setRange(ui.arRangeStart || e.target.value, e.target.value); }} />
      {#if !isAll}<button type="button" class="chip" onclick={() => setRange(spanStart, spanEnd)}>Full record</button>{/if}
      <span class="cap tail" use:fitText={{ min: 9 }}>data {fmtDay(spanStart)} – {fmtDay(spanEnd)}</span>
    </div>
    <div class="row">
      <div class="track">
        {#if curve}
          <svg class="area" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">
            <path d={curve.d} />
          </svg>
        {/if}
        <div class="needle" style:left="{thumbPct}%" aria-hidden="true"></div>
        <input class="slider" type="range" min={win?.lo ?? 0} max={win?.hi ?? 0} step="1" value={frameIdx} disabled={!win} oninput={onSlide} aria-label="Time within the range" />
        {#if ticks.length}
          <div class="lane">
            {#each ticks as t (t.ep.id)}
              <button type="button" class="tick" class:active={t.ep.id === ui.arEpisodeId} class:hovered={t.ep.id === ui.arHoverEpisodeId}
                style:left="{t.left}%" style:width="{t.width}%"
                title={`AR episode ${fmtDay(t.ep.start_date)} – ${fmtDay(t.ep.end_date)} — click to make it the range`}
                onclick={() => pickEpisode(t.ep)}></button>
            {/each}
          </div>
        {/if}
      </div>
      <span class="when" use:fitText={{ min: 10 }}>{frame ? `${fmtFrame(frame.time)} · ${frame.count ?? 0} footprint${(frame.count ?? 0) === 1 ? '' : 's'}` : win ? '' : 'No footprint frames in this range'}</span>
    </div>
    {#if dayChips.length}
      <div class="days" aria-label="Days in the range">
        {#each dayChips as d (d.iso)}
          <button type="button" class="chip day" class:active={d.iso === ui.arTargetDate} title={`Inspect ${fmtDay(d.iso)}`} onclick={() => goToDay(d.iso)}>{d.label}</button>
        {/each}
      </div>
    {/if}
  {/if}
</div>

<style>
  .arc { --ar-accent: #b9a6ff; display: flex; flex-direction: column; gap: 10px; min-width: 0; }
  .msg { font-size: 12px; color: var(--text-secondary); padding: 6px 0; }
  .row { display: flex; align-items: center; gap: 8px; min-width: 0; }
  .cap { flex: none; }
  .tail { flex: 1; min-width: 0; text-align: right; overflow: hidden; text-overflow: ellipsis; }
  .date {
    flex: none; font: inherit; font-size: 13px; padding: 4px 8px; height: 28px; border-radius: 7px;
    background: var(--surface); border: 1px solid var(--panel-border); color: var(--text-primary); color-scheme: dark;
  }
  .date:focus { outline: none; border-color: var(--ar-accent); }
  .track { flex: 1; min-width: 0; position: relative; height: 40px; border-radius: 8px; background: #ffffff08; border: 1px solid var(--panel-border); }
  .track, .area { border-radius: 8px; }
  .area { position: absolute; inset: 0; width: 100%; height: 100%; display: block; }
  .area path { fill: rgba(185, 166, 255, 0.28); stroke: rgba(185, 166, 255, 0.85); stroke-width: 1.2; vector-effect: non-scaling-stroke; stroke-linejoin: round; }
  .needle { position: absolute; top: 0; bottom: 0; width: 1px; background: #ffffffb8; transform: translateX(-0.5px); pointer-events: none; }
  .slider { position: absolute; inset: 0; width: 100%; height: 100%; margin: 0; background: transparent; -webkit-appearance: none; appearance: none; cursor: pointer; }
  .slider:focus { outline: none; }
  .slider::-webkit-slider-runnable-track { background: transparent; height: 100%; }
  .slider::-moz-range-track { background: transparent; height: 100%; }
  .slider::-webkit-slider-thumb { -webkit-appearance: none; appearance: none; width: 14px; height: 14px; border-radius: 50%; background: #fff; border: 2px solid var(--ar-accent); box-shadow: 0 1px 4px rgba(0,0,0,.5); margin-top: 12px; }
  .slider::-moz-range-thumb { width: 14px; height: 14px; border-radius: 50%; background: #fff; border: 2px solid var(--ar-accent); box-shadow: 0 1px 4px rgba(0,0,0,.5); }
  .slider:focus-visible::-webkit-slider-thumb { box-shadow: 0 0 0 3px rgba(185,166,255,.45); }
  .lane { position: absolute; left: 0; right: 0; bottom: 0; height: 4px; pointer-events: none; }
  .tick { position: absolute; top: 0; height: 4px; min-width: 3px; padding: 0; border: 0; border-radius: 2px 2px 0 0; background: var(--ar-accent); opacity: .9; cursor: pointer; pointer-events: auto; }
  .tick.active, .tick.hovered { opacity: 1; height: 6px; top: -2px; }
  .days { display: flex; flex-wrap: wrap; gap: 4px; }
  .chip.day { padding: 2px 7px; font-size: 11.5px; }
  .when { flex: none; width: 250px; text-align: right; font-size: 13px; color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  @media (max-width: 760px) { .tail { display: none; } .when { width: 150px; font-size: 12px; } }
</style>
