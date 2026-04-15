/**
 * Single source of truth for all UI colors tied to statistics, models, and
 * value-quality tiers.  Every component that color-codes a model or statistic
 * must import from here — no ad-hoc palette constants elsewhere.
 *
 * Constraint: no green, red, or yellow hues (they conflict with the
 * good / ok / bad quality tier semantics).
 */

/* ── Statistic accent colors ── */

export const STAT_COLORS = {
  nrmse: '#6eb5ff',   // blue
  bias:  '#e0a4ff',   // lavender
  sacc:  '#5ce0d6',   // teal
  nmad:  '#f0a07a',   // peach
};

export function statColor(key) {
  return STAT_COLORS[key] || '#888';
}

/* ── Model palette ── */

/**
 * Curated hues that avoid the red (0–30, 330–360), yellow (40–70),
 * and green (80–155) zones.  Entries cycle if there are more models
 * than slots.
 */
const MODEL_HUES = [210, 275, 185, 315, 240, 170, 295, 225];

/**
 * Deterministic display colors keyed by model.  Each model receives
 * bg / fg / border variants for use in pills, chart lines, etc.
 */
export function modelPalette(models) {
  const arr = Array.isArray(models) ? models : [];
  const out = {};
  for (let i = 0; i < arr.length; i++) {
    const hue = MODEL_HUES[i % MODEL_HUES.length];
    out[arr[i].key] = {
      bg:     `hsl(${hue} 52% 32%)`,
      fg:     `hsl(${hue} 30% 96%)`,
      border: `hsl(${hue} 45% 48%)`,
    };
  }
  return out;
}
