import { STATIC_BASE } from './constants.js';

// Offscreen canvas for compositing interpolated tiles
const interpCanvas = document.createElement('canvas');
const interpCtx = interpCanvas.getContext('2d');

// Cache of loaded Image objects keyed by integer lead day
const leadImages = {};

export function clearLeadImages() {
  for (const key of Object.keys(leadImages)) delete leadImages[key];
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
 * Per-pixel linear crossfade between two images.
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
  if (interpCanvas.width !== w || interpCanvas.height !== h) {
    interpCanvas.width = w;
    interpCanvas.height = h;
  }
  interpCtx.imageSmoothingEnabled = false;
  interpCtx.globalAlpha = 1.0;
  interpCtx.clearRect(0, 0, w, h);
  interpCtx.drawImage(imgA, 0, 0, w, h);
  const dataA = interpCtx.getImageData(0, 0, w, h);
  interpCtx.clearRect(0, 0, w, h);
  interpCtx.drawImage(imgB, 0, 0, w, h);
  const dataB = interpCtx.getImageData(0, 0, w, h);
  const pA = dataA.data;
  const pB = dataB.data;
  const s = 1.0 - t;
  // Where only one side has coverage, keep that pixel (avoids halos at land/ocean).
  // Where both have alpha > 0, cross-fade in premultiplied space then unpremultiply —
  // straight RGB * s + RGB * t is wrong when alpha varies (e.g. sequential tiles).
  for (let i = 0, n = pA.length; i < n; i += 4) {
    const aA = pA[i + 3];
    const aB = pB[i + 3];
    if (aA > 0 && aB > 0) {
      const pmAr = (pA[i] * aA) / 255;
      const pmAg = (pA[i + 1] * aA) / 255;
      const pmAb = (pA[i + 2] * aA) / 255;
      const pmBr = (pB[i] * aB) / 255;
      const pmBg = (pB[i + 1] * aB) / 255;
      const pmBb = (pB[i + 2] * aB) / 255;
      const aOut = aA * s + aB * t;
      if (aOut <= 0) {
        pA[i] = pA[i + 1] = pA[i + 2] = pA[i + 3] = 0;
        continue;
      }
      const pmOr = pmAr * s + pmBr * t;
      const pmOg = pmAg * s + pmBg * t;
      const pmOb = pmAb * s + pmBb * t;
      const inv = 255 / aOut;
      pA[i] = Math.min(255, (pmOr * inv + 0.5) | 0);
      pA[i + 1] = Math.min(255, (pmOg * inv + 0.5) | 0);
      pA[i + 2] = Math.min(255, (pmOb * inv + 0.5) | 0);
      pA[i + 3] = Math.min(255, (aOut + 0.5) | 0);
    } else if (aB > 0) {
      pA[i] = pB[i];
      pA[i + 1] = pB[i + 1];
      pA[i + 2] = pB[i + 2];
      pA[i + 3] = pB[i + 3];
    }
    // else: only A or neither — keep pA
  }
  interpCtx.putImageData(dataA, 0, 0);
  return interpCanvas.toDataURL();
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
