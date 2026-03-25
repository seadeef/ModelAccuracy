import { STATIC_BASE, MONTH_NAMES } from './constants.js';

const SEASON_LABELS = {
  djf: 'Winter (DJF)',
  mam: 'Spring (MAM)',
  jja: 'Summer (JJA)',
  son: 'Autumn (SON)',
};

function rangeJsonUrl(model, statistic, period, month, season) {
  const b = `${STATIC_BASE}/ranges/${model}/${statistic}`;
  if (period === 'monthly') return `${b}/monthly/${month}.json`;
  if (period === 'seasonal') return `${b}/seasonal/${season}.json`;
  return `${b}/yearly.json`;
}

function loadImage(src) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.decoding = 'async';
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error('Tile image failed to load'));
    img.src = src;
  });
}

function formatLegendValue(v) {
  const av = Math.abs(v);
  if (av >= 100) return v.toFixed(0);
  if (av >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

function drawColorBar(ctx, x, y, w, h, colormap) {
  const g = ctx.createLinearGradient(x, 0, x + w, 0);
  if (colormap === 'diverging') {
    g.addColorStop(0, 'rgb(44,123,182)');
    g.addColorStop(0.5, 'rgb(255,255,255)');
    g.addColorStop(1, 'rgb(215,25,28)');
  } else if (colormap === 'diverging_reversed') {
    g.addColorStop(0, 'rgb(215,25,28)');
    g.addColorStop(0.5, 'rgb(255,255,255)');
    g.addColorStop(1, 'rgb(44,123,182)');
  } else {
    g.addColorStop(0, 'rgb(255,255,255)');
    g.addColorStop(1, 'rgb(44,123,182)');
  }
  ctx.fillStyle = g;
  ctx.fillRect(x, y, w, h);
  ctx.strokeStyle = 'rgb(120,120,120)';
  ctx.lineWidth = 1;
  ctx.strokeRect(x - 0.5, y - 0.5, w + 1, h + 1);
}

/**
 * Build a titled PNG (map + legend) using the overlay already shown on the map
 * (browser cache / decoded pixels) and static range JSON from export_static.
 */
export async function exportMapImage({
  overlayUrl,
  fallbackTileUrl,
  model,
  statistic,
  lead,
  period,
  month,
  season,
  models,
  statisticsMeta,
}) {
  const src = overlayUrl || fallbackTileUrl;
  if (!src) throw new Error('No tile URL for export');

  const rangeUrl = rangeJsonUrl(model, statistic, period, month, season);
  const [rangeRes, tileImg] = await Promise.all([
    fetch(rangeUrl),
    loadImage(src),
  ]);
  if (!rangeRes.ok) {
    throw new Error(`Legend range file missing (${rangeRes.status}). Re-run export_static.py.`);
  }
  const range = await rangeRes.json();
  const { vmin, vmax, colormap } = range;
  if (typeof vmin !== 'number' || typeof vmax !== 'number' || !colormap) {
    throw new Error('Invalid range JSON');
  }

  const modelLabel = models.find((m) => m.key === model)?.label ?? model;
  const statEntry = statisticsMeta.find((s) => s.key === statistic);
  const statLabel = statEntry?.label ?? statistic;
  const units = statEntry?.units ?? '';

  const leadStr = String(lead);
  let leadLabel;
  if (leadStr.includes('_')) {
    const parts = leadStr.split('_');
    leadLabel = `${parts[0]}\u2013${parts[1]} Day Average`;
  } else {
    leadLabel = `Day ${leadStr}`;
  }

  let periodLabel = 'Yearly';
  if (period === 'monthly' && month) {
    const mi = parseInt(month, 10);
    periodLabel = MONTH_NAMES[mi] && mi >= 1 && mi <= 12 ? MONTH_NAMES[mi] : month;
  } else if (period === 'seasonal' && season) {
    periodLabel = SEASON_LABELS[season] ?? season.toUpperCase();
  }

  const title = `${modelLabel} ${statLabel} (${units})  \u2014  ${leadLabel}  \u2014  ${periodLabel}`;

  const MIN_WIDTH = 800;
  const TITLE_H = 48;
  const LEGEND_H = 70;
  const PADDING = 16;

  const tileW = tileImg.naturalWidth;
  const tileH = tileImg.naturalHeight;
  const scale = Math.max(1, MIN_WIDTH / (tileW + 2 * PADDING));
  const scaledW = Math.round(tileW * scale);
  const scaledH = Math.round(tileH * scale);

  const imgW = scaledW + 2 * PADDING;
  const imgH = TITLE_H + scaledH + LEGEND_H;

  const canvas = document.createElement('canvas');
  canvas.width = imgW;
  canvas.height = imgH;
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error('Canvas unsupported');

  ctx.fillStyle = '#fff';
  ctx.fillRect(0, 0, imgW, imgH);

  ctx.strokeStyle = 'rgb(200,200,200)';
  ctx.beginPath();
  ctx.moveTo(0, TITLE_H - 0.5);
  ctx.lineTo(imgW, TITLE_H - 0.5);
  ctx.stroke();

  ctx.fillStyle = 'rgb(240,240,240)';
  ctx.fillRect(PADDING, TITLE_H, scaledW, scaledH);
  ctx.imageSmoothingEnabled = scale > 1;
  ctx.imageSmoothingQuality = 'high';
  ctx.drawImage(tileImg, PADDING, TITLE_H, scaledW, scaledH);

  ctx.beginPath();
  ctx.moveTo(0, TITLE_H + scaledH + 0.5);
  ctx.lineTo(imgW, TITLE_H + scaledH + 0.5);
  ctx.stroke();

  let titleFontPx = 20;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  for (; titleFontPx >= 12; titleFontPx -= 2) {
    ctx.font = `600 ${titleFontPx}px system-ui, -apple-system, sans-serif`;
    const tw = ctx.measureText(title).width;
    if (tw <= imgW - 2 * PADDING) break;
  }
  ctx.fillStyle = '#000';
  ctx.fillText(title, imgW / 2, TITLE_H / 2);

  const barW = Math.min(Math.floor(imgW * 0.5), 400);
  const barH = 16;
  const barX = (imgW - barW) >> 1;
  const barY = TITLE_H + scaledH + 12;

  drawColorBar(ctx, barX, barY, barW, barH, colormap);

  const labelY = barY + barH + 4;
  ctx.font = '12px system-ui, -apple-system, sans-serif';
  ctx.fillStyle = 'rgb(60,60,60)';
  ctx.textBaseline = 'top';

  const vminStr = formatLegendValue(vmin);
  const vmaxStr = formatLegendValue(vmax);

  ctx.textAlign = 'center';
  ctx.fillText(vminStr, barX, labelY);
  ctx.fillText(vmaxStr, barX + barW, labelY);

  if (colormap === 'diverging' || colormap === 'diverging_reversed') {
    const mid = (vmin + vmax) / 2;
    const midStr = formatLegendValue(mid);
    ctx.fillText(midStr, barX + barW / 2, labelY);
  }

  ctx.textAlign = 'center';
  ctx.fillStyle = 'rgb(120,120,120)';
  ctx.fillText(units, imgW / 2, labelY + 16);

  const leadTag = leadStr.replaceAll('_', '-');
  const periodTag =
    period === 'monthly' && month
      ? `month${month}`
      : period === 'seasonal' && season
        ? season
        : 'yearly';
  const filename = `${model}_${statistic}_lead${leadTag}_${periodTag}.png`;

  await new Promise((resolve, reject) => {
    canvas.toBlob(
      (blob) => {
        if (!blob) {
          reject(new Error('PNG export failed'));
          return;
        }
        const a = document.createElement('a');
        const href = URL.createObjectURL(blob);
        a.href = href;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(href);
        resolve();
      },
      'image/png',
      0.92,
    );
  });
}
