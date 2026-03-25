/**
 * Deterministic display colors from the order of models in config.json (not hardcoded keys).
 * Golden-angle hue steps keep 2–4 models visually distinct.
 */
export function modelPalette(models) {
  const arr = Array.isArray(models) ? models : [];
  const out = {};
  const n = arr.length;
  if (n === 0) return out;
  const golden = 137.508;
  for (let i = 0; i < n; i++) {
    const hue = Math.round((i * golden) % 360);
    out[arr[i].key] = {
      bg: `hsl(${hue} 52% 32%)`,
      fg: `hsl(${hue} 30% 96%)`,
      border: `hsl(${hue} 45% 48%)`,
    };
  }
  return out;
}
