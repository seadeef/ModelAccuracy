import { STATIC_BASE, MONTH_NAMES, SEASON_NAMES, TILE_IMAGE_BOUNDS_WGS84 } from './constants.js';

function rangeJsonUrl(model, statistic, period, month, season) {
  const b = `${STATIC_BASE}/ranges/${model}/${statistic}`;
  if (period === 'monthly') return `${b}/monthly/${month}.json`;
  if (period === 'seasonal') return `${b}/seasonal/${season}.json`;
  return `${b}/yearly.json`;
}

/** Source tile PNGs are EPSG:3857. Lat must use the Mercator y mapping;
 *  lng is linear. Returns [0,1] fraction along the source bounds. */
function mercY(latDeg) {
  return Math.log(Math.tan(Math.PI / 4 + (latDeg * Math.PI) / 360));
}

function geometryParts(geometry) {
  if (!geometry) return [];
  if (geometry.type === 'MultiPolygon') return geometry.coordinates;
  if (geometry.type === 'Polygon') return [geometry.coordinates];
  return [];
}

function buildAdminTitleSuffix(region) {
  if (!region || region.type !== 'admin' || !region.name) return '';
  return `  —  ${region.name}`;
}

function adminFilenameTag(region) {
  if (!region || region.type !== 'admin' || !region.fips) return '';
  return `_${region.level}${region.fips}`;
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
  region,
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
    periodLabel = SEASON_NAMES[season] ? `${SEASON_NAMES[season]} (${season.toUpperCase()})` : season.toUpperCase();
  }

  const title = `${modelLabel} ${statLabel} (${units})  \u2014  ${leadLabel}  \u2014  ${periodLabel}${buildAdminTitleSuffix(region)}`;

  const MIN_IMG_WIDTH = 800;
  const TITLE_H = 48;
  const LEGEND_H = 70;
  const PADDING = 16;
  const MAP_AREA_WIDTH = 900;
  const MAP_AREA_HEIGHT = 600;

  const tileW = tileImg.naturalWidth;
  const tileH = tileImg.naturalHeight;

  const [oW, oS, oE, oN] = TILE_IMAGE_BOUNDS_WGS84;
  const oMercN = mercY(oN);
  const oMercS = mercY(oS);

  // Map a WGS84 point to source-pixel coords in the (Mercator) tile PNG.
  const lngToSrcX = (lng) => ((lng - oW) / (oE - oW)) * tileW;
  const latToSrcY = (lat) => ((oMercN - mercY(lat)) / (oMercN - oMercS)) * tileH;

  // Crop the source tile to an admin region's bbox when one is selected.
  const adminGeometry = region?.type === 'admin' ? region.geometry : null;
  const adminBounds = region?.type === 'admin' && Array.isArray(region.bounds) ? region.bounds : null;

  let srcX = 0, srcY = 0, srcW = tileW, srcH = tileH;
  if (adminGeometry && adminBounds && adminBounds.length === 4) {
    const [bw, bs, be, bn] = adminBounds;
    const xL = lngToSrcX(bw);
    const xR = lngToSrcX(be);
    const yT = latToSrcY(bn);
    const yB = latToSrcY(bs);
    // Snap each edge outward so the cropped source pixels fully contain the bbox.
    // ceil(xR)-floor(xL) is correct; ceil(xR-xL) under-sizes when the bbox crosses a pixel boundary.
    srcX = Math.max(0, Math.floor(xL));
    srcY = Math.max(0, Math.floor(yT));
    const srcXEnd = Math.min(tileW, Math.ceil(xR));
    const srcYEnd = Math.min(tileH, Math.ceil(yB));
    srcW = Math.max(1, srcXEnd - srcX);
    srcH = Math.max(1, srcYEnd - srcY);
  }

  // Fit the source crop into a fixed map-area box, preserving aspect ratio.
  // Tiny crops (single states, counties) get scaled up; CONUS-wide crops fit within MAP_AREA_WIDTH.
  const scale = Math.min(MAP_AREA_WIDTH / srcW, MAP_AREA_HEIGHT / srcH);
  const scaledW = Math.round(srcW * scale);
  const scaledH = Math.round(srcH * scale);

  // Image always has at least MIN_IMG_WIDTH so title and legend have room.
  const imgW = Math.max(MIN_IMG_WIDTH, scaledW + 2 * PADDING);
  const imgH = TITLE_H + MAP_AREA_HEIGHT + LEGEND_H;

  // Center the scaled map both horizontally and vertically inside the map area.
  const mapOffsetX = Math.round((imgW - scaledW) / 2);
  const mapOffsetY = TITLE_H + Math.round((MAP_AREA_HEIGHT - scaledH) / 2);
  const mapAreaBottom = TITLE_H + MAP_AREA_HEIGHT;

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

  // Convert a (lng, lat) WGS84 vertex to canvas pixels inside the cropped/scaled tile area.
  const lngLatToCanvas = (lng, lat) => {
    const sx = lngToSrcX(lng);
    const sy = latToSrcY(lat);
    return [
      mapOffsetX + (sx - srcX) * scale,
      mapOffsetY + (sy - srcY) * scale,
    ];
  };

  ctx.fillStyle = 'rgb(240,240,240)';
  ctx.fillRect(mapOffsetX, mapOffsetY, scaledW, scaledH);
  ctx.imageSmoothingEnabled = scale > 1;
  ctx.imageSmoothingQuality = 'high';

  if (adminGeometry) {
    ctx.save();
    // Build a clip path from the admin polygon(s); even-odd handles holes.
    ctx.beginPath();
    for (const poly of geometryParts(adminGeometry)) {
      for (const ring of poly) {
        if (!Array.isArray(ring) || ring.length < 3) continue;
        for (let i = 0; i < ring.length; i++) {
          const [lng, lat] = ring[i];
          const [px, py] = lngLatToCanvas(lng, lat);
          if (i === 0) ctx.moveTo(px, py);
          else ctx.lineTo(px, py);
        }
        ctx.closePath();
      }
    }
    ctx.clip('evenodd');
    ctx.drawImage(tileImg, srcX, srcY, srcW, srcH, mapOffsetX, mapOffsetY, scaledW, scaledH);
    ctx.restore();

    // Outline the admin shape so the boundary is legible against the white background.
    ctx.save();
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.strokeStyle = 'rgba(40,40,40,0.85)';
    ctx.lineWidth = 1.4;
    for (const poly of geometryParts(adminGeometry)) {
      for (const ring of poly) {
        if (!Array.isArray(ring) || ring.length < 3) continue;
        ctx.beginPath();
        for (let i = 0; i < ring.length; i++) {
          const [lng, lat] = ring[i];
          const [px, py] = lngLatToCanvas(lng, lat);
          if (i === 0) ctx.moveTo(px, py);
          else ctx.lineTo(px, py);
        }
        ctx.closePath();
        ctx.stroke();
      }
    }
    ctx.restore();
  } else {
    ctx.drawImage(tileImg, srcX, srcY, srcW, srcH, mapOffsetX, mapOffsetY, scaledW, scaledH);
  }

  ctx.beginPath();
  ctx.moveTo(0, mapAreaBottom + 0.5);
  ctx.lineTo(imgW, mapAreaBottom + 0.5);
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
  const barY = mapAreaBottom + 12;

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
  const filename = `${model}_${statistic}_lead${leadTag}_${periodTag}${adminFilenameTag(region)}.png`;

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
