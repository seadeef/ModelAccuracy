/**
 * Shared tARget v4 manifest store + frame-selection helpers.
 *
 * The manifest (6-hour AR footprint frames) is loaded once per session and read
 * by both the headless AR coordinator (map frame defaults) and the panel's
 * timeline (drill cells + frame scrubber), so it lives in a runes module
 * instead of either component.
 */

import { fetchAtmosphericRiverManifest } from './api.js';
import { ui } from './state.svelte.js';

export const DAY_MS = 86_400_000;

export const arData = $state({
  frames: [],
  loading: false,
  loaded: false,
  error: '',
});

export function ensureArManifest() {
  if (arData.loaded || arData.loading) return;
  arData.loading = true;
  fetchAtmosphericRiverManifest()
    .then((data) => {
      arData.frames = data?.frames ?? [];
      arData.loaded = true;
    })
    .catch((e) => {
      arData.error = e?.message || 'AR data unavailable';
    })
    .finally(() => {
      arData.loading = false;
    });
}

export function dayMs(iso) {
  return Date.parse(`${iso}T00:00:00Z`);
}

export function isoFromMs(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

/** UTC-day bounds of the manifest: {startMs, endMs} (both day-floored), or null. */
export function spanBounds() {
  const frames = arData.frames;
  if (!frames.length) return null;
  const a = Date.parse(frames[0]?.time);
  const b = Date.parse(frames[frames.length - 1]?.time);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  return {
    startMs: Math.floor(a / DAY_MS) * DAY_MS,
    endMs: Math.floor(b / DAY_MS) * DAY_MS,
  };
}

/** Frame indices {lo, hi} inside [start 00:00Z, end+1d 00:00Z), or null. */
export function windowIndices(range) {
  const list = arData.frames;
  if (!range?.start || !range?.end || !list.length) return null;
  const startMs = dayMs(range.start);
  const endMs = dayMs(range.end) + DAY_MS; // end day inclusive
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return null;
  let lo = -1;
  let hi = -1;
  for (let i = 0; i < list.length; i++) {
    const t = Date.parse(list[i]?.time);
    if (t >= startMs && t < endMs) {
      if (lo === -1) lo = i;
      hi = i;
    }
  }
  return lo === -1 ? null : { lo, hi };
}

/** First frame in [lo, hi] with visualized footprints, else lo. */
export function firstWithFootprints(lo, hi) {
  const list = arData.frames;
  for (let i = lo; i <= hi; i++) {
    if ((list[i]?.count ?? 0) > 0) return i;
  }
  return lo;
}

export function selectFrame(index) {
  const list = arData.frames;
  const next = Math.max(0, Math.min(Math.max(0, list.length - 1), Number(index)));
  ui.arFrameIndex = next;
  ui.arTime = list[next]?.time ?? null;
  ui.arFramePath = list[next]?.path ?? null;
}

/** Inspect a day (ISO): the frame moves onto its first frame with footprints
 *  (else its first frame) and the inspected day follows. With no frames for
 *  the day (outside the manifest) only the day changes. */
export function selectDay(iso) {
  ui.arTargetDate = iso;
  const w = windowIndices({ start: iso, end: iso });
  if (w) selectFrame(firstWithFootprints(w.lo, w.hi));
}
