import { STATIC_BASE } from './constants.js';

// Offscreen canvas for compositing interpolated tiles
const interpCanvas = document.createElement('canvas');
const interpCtx = interpCanvas.getContext('2d');

// Cache of loaded Image objects keyed by integer lead day
const leadImages = {};

/** LRU of composited lead-blend blob URLs (see compositeToDataUrlCached). */
const compositeDataUrlCache = new Map();
const MAX_COMPOSITE_CACHE = 128;
/** Quantize blend factor so scrubbing reuses canvas work (invisible at ~64 steps). */
const COMPOSITE_T_STEPS = 64;

export function clearLeadImages() {
  for (const key of Object.keys(leadImages)) delete leadImages[key];
  for (const url of compositeDataUrlCache.values()) {
    URL.revokeObjectURL(url);
  }
  compositeDataUrlCache.clear();
}

export function getLeadImage(lead) {
  const entry = leadImages[lead];
  return entry && entry.ready ? entry.img : null;
}

/**
 * Build the tile URL for a given statistic and lead.
 */
export function tileUrl(model, statistic, lead, period, month, season) {
  let sub = null;
  if (period === 'monthly') sub = `monthly/${month}`;
  else if (period === 'seasonal') sub = `seasonal/${season}`;
  const base = `${STATIC_BASE}/tiles/${model}/${statistic}`;
  return sub ? `${base}/${sub}/lead_${lead}.png` : `${base}/lead_${lead}.png`;
}

/**
 * Preload all lead images for the current model/statistic/period.
 * Calls onReady(lead) when an image finishes loading.
 */
export function preloadLeads({ model, statistic, period, month, season, minLead, maxLead, onReady }) {
  for (let lead = minLead; lead <= maxLead; lead++) {
    if (leadImages[lead]) continue;
    const url = tileUrl(model, statistic, lead, period, month, season);
    const img = new Image();
    const entry = { img, ready: false };
    const capturedLead = lead;
    leadImages[lead] = entry;
    img.onload = () => {
      entry.ready = true;
      if (onReady) onReady(capturedLead);
    };
    img.src = url;
  }
}

/**
 * Linear crossfade between two images via canvas "lighter" (premultiplied add).
 * Returns a blob: URL from async PNG encoding (no base64 data: URL).
 */
export function compositeToDataUrl(imgA, imgB, t) {
  const w = imgA.naturalWidth;
  const h = imgA.naturalHeight;
  if (imgB.naturalWidth !== w || imgB.naturalHeight !== h) {
    console.warn(
      'compositeToDataUrl: size mismatch',
      `${imgA.naturalWidth}x${imgA.naturalHeight}`,
      'vs',
      `${imgB.naturalWidth}x${imgB.naturalHeight}`,
    );
  }
  if (!interpCtx) {
    return Promise.reject(new Error('Canvas 2D context unavailable'));
  }
  if (interpCanvas.width !== w || interpCanvas.height !== h) {
    interpCanvas.width = w;
    interpCanvas.height = h;
  }
  interpCtx.imageSmoothingEnabled = false;
  interpCtx.clearRect(0, 0, w, h);
  interpCtx.globalCompositeOperation = 'source-over';
  interpCtx.globalAlpha = 1.0 - t;
  interpCtx.drawImage(imgA, 0, 0, w, h);
  interpCtx.globalCompositeOperation = 'lighter';
  interpCtx.globalAlpha = t;
  interpCtx.drawImage(imgB, 0, 0, w, h);
  interpCtx.globalCompositeOperation = 'source-over';
  interpCtx.globalAlpha = 1.0;

  return new Promise((resolve, reject) => {
    interpCanvas.toBlob((blob) => {
      if (!blob) {
        reject(new Error('compositeToDataUrl: toBlob failed'));
        return;
      }
      resolve(URL.createObjectURL(blob));
    });
  });
}

function quantizeBlendT(t) {
  const n = COMPOSITE_T_STEPS;
  const q = Math.round(Math.min(1, Math.max(0, t)) * n) / n;
  return q;
}

function compositeCacheKey(meta, tq) {
  const { model, statistic, period, month, season, lo, hi } = meta;
  const pk =
    period === 'monthly' ? `m:${month}` : period === 'seasonal' ? `s:${season}` : 'a';
  return `${model}|${statistic}|${pk}|${lo}|${hi}|${tq}`;
}

/**
 * Same pixels as compositeToDataUrl, but memoized by (model context, leads, quantized t).
 * Interpolated frames are blob: URLs; this avoids redoing canvas encode when revisiting
 * the same fractional lead.
 */
export async function compositeToDataUrlCached(meta, imgA, imgB, t) {
  const tq = quantizeBlendT(t);
  const key = compositeCacheKey(meta, tq);
  const hit = compositeDataUrlCache.get(key);
  if (hit !== undefined) {
    compositeDataUrlCache.delete(key);
    compositeDataUrlCache.set(key, hit);
    return hit;
  }
  const blobUrl = await compositeToDataUrl(imgA, imgB, tq);
  compositeDataUrlCache.set(key, blobUrl);
  while (compositeDataUrlCache.size > MAX_COMPOSITE_CACHE) {
    const oldestKey = compositeDataUrlCache.keys().next().value;
    const evicted = compositeDataUrlCache.get(oldestKey);
    compositeDataUrlCache.delete(oldestKey);
    if (evicted) URL.revokeObjectURL(evicted);
  }
  return blobUrl;
}

/**
 * Build lead options (for dropdown) from a model config object.
 */
export function buildLeadOptions(modelConfig) {
  if (!modelConfig) return [{ key: '1' }];
  const options = [];
  for (let i = modelConfig.lead_days_min; i <= modelConfig.lead_days_max; i++) {
    options.push({ key: String(i) });
  }
  for (const [start, end] of modelConfig.lead_windows || []) {
    if (start >= modelConfig.lead_days_min && end <= modelConfig.lead_days_max) {
      options.push({ key: `${start}_${end}` });
    }
  }
  return options;
}

/**
 * Get min/max lead day bounds for a model config.
 */
export function getModelLeadBounds(models, modelKey) {
  const m = models.find((x) => x.key === modelKey);
  return { min: m ? m.lead_days_min : 1, max: m ? m.lead_days_max : 14 };
}
