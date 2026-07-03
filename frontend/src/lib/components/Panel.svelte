<script>
  import { appConfig, ui, statLabel, accuracyStatKeys } from '../state.svelte.js';
  import {
    statisticTooltip,
    PERIOD_HELP,
    SEASON_PERIOD_HELP,
  } from '../helpText.js';
  import { fetchStatsAllLeads, fetchLeadWinnersForRegion, fetchForecastAllVariables } from '../api.js';
  import { downloadPanelStatsCsv } from '../exportPanelStatsCsv.js';
  import { getModelLeadBounds } from '../tile.js';
  import { modelPalette, statColor } from '../palette.js';
  import { glyphPanelClose } from '../appIcons.js';

  let { onleadchange, onstatchange, onperiodchange, onmodelchange } = $props();

  let canvasEl = $state();
  let ensembleForecastCanvasEl = $state();
  /** Wraps forecast + accuracy chart sections; shared X for lead scrub / hover. */
  let chartTrackRegionEl = $state();
  let loading = $state(false);
  let leadData = $state([]);
  /** Chart / card highlight; must match `ui.statistic` when the panel opens (map layer). */
  let activeStat = $state('bias');
  let hoverLead = $state(null);
  /** When true, chart scrubbing does not move the map lead; click the chart again to unlock. */
  let leadScrubLocked = $state(false);
  /** For distinguishing a click from a drag when toggling lock (pointer capture on chart). */
  let scrubPointerDown = $state(null);

  /** Best model per lead from ``POST /api/stats/lead-winners`` (region-scoped). */
  let winnersPayload = $state(null);
  let winnersLoading = $state(false);
  let winnersGen = 0;
  let leadFetchGen = 0;

  /** ``POST /api/stats/forecast`` per variable — { [variable]: { models, ... } } for the
   *  map region (all models × leads). All variables are fetched together so the
   *  selector buttons show each variable's live value and switching is instant. */
  let forecastByVar = $state(null);
  /** Payload for the currently selected forecast variable (drives the top chart). */
  const forecastPayload = $derived(forecastByVar?.[ui.forecastVariable] ?? null);
  let lastForecastGeoKey = '';
  let forecastFetchGen = 0;

  /** Forecast-variable selector buttons (top chart), in display order. */
  const FORECAST_VAR_ORDER = ['precip', 'wind', 'humidity', 'temp'];
  const FORECAST_VAR_LABEL = { precip: 'Precip', temp: 'Temp', wind: 'Wind', humidity: 'Humidity' };
  /** Accent color per forecast variable (mirrors the data colormaps: blue/teal/green/orange). */
  const FORECAST_VAR_COLOR = { precip: '#4a90d9', temp: '#e8743b', wind: '#2bb3a3', humidity: '#5fae46' };

  /** Custom model dropdown (replaces the native <select> so only the model NAME is
   *  colored by its model color while the date range stays neutral — native
   *  <option> text can only be a single color). */
  let modelMenuOpen = $state(false);
  let modelSelectEl = $state();
  function selectModel(key) {
    modelMenuOpen = false;
    if (key === ui.model) return;
    onmodelchange?.({ target: { value: key } });
  }
  /** One window handler for both click-outside and Escape (matches the toolbar idiom). */
  function handleModelMenuWindow(e) {
    if (!modelMenuOpen) return;
    if (e.type === 'keydown') {
      if (e.key === 'Escape') modelMenuOpen = false;
      return;
    }
    if (modelSelectEl && !modelSelectEl.contains(e.target)) modelMenuOpen = false;
  }

  /** Coalesce chart scrub → map updates to one per frame (reduces overlapping MapLibre `updateImage` / AbortError). */
  let chartScrubRafId = 0;
  let pendingScrubLead = null;

  /** Plot area insets; y-labels drawn inline over chart, no reserved margins needed.
   *  The chart title now lives in its own header row above the canvas (see .chart-label),
   *  so PLOT_TOP only has to reserve room for the floating hover-value pill and the
   *  topmost y-axis tick — it no longer shares this band with the title text. */
  const PAD = { left: 4, right: 4, bottom: 6 };
  const PLOT_TOP = 26;
  const HOVER_LABEL_Y = 15;

  function regionCenter() {
    const r = ui.selectedRegion;
    if (!r) return null;
    if (r.type === 'point') return { lat: r.coordinates[1], lon: r.coordinates[0] };
    if (r.type === 'rectangle') return {
      lat: (r.coordinates[0][1] + r.coordinates[1][1]) / 2,
      lon: (r.coordinates[0][0] + r.coordinates[1][0]) / 2,
    };
    if (r.type === 'polygon') {
      const lats = r.coordinates.map(c => c[1]);
      const lons = r.coordinates.map(c => c[0]);
      return { lat: lats.reduce((a, b) => a + b, 0) / lats.length, lon: lons.reduce((a, b) => a + b, 0) / lons.length };
    }
    if (r.type === 'admin' && Array.isArray(r.bounds) && r.bounds.length === 4) {
      const [w, s, e, n] = r.bounds;
      return { lat: (s + n) / 2, lon: (w + e) / 2 };
    }
    return null;
  }

  function regionLabel() {
    const r = ui.selectedRegion;
    const c = regionCenter();
    if (!r || !c) return '';
    const coord = `${c.lat.toFixed(2)}\u00b0N, ${Math.abs(c.lon).toFixed(2)}\u00b0W`;
    if (r.type === 'polygon') return `Polygon centered at: ${coord}`;
    if (r.type === 'rectangle') return `Rectangle centered at: ${coord}`;
    if (r.type === 'admin') return r.name ? `${r.name} \u2014 ${coord}` : coord;
    if (r.type === 'huc') {
      const tag = r.huc ? `HUC-${r.level} ${r.huc}` : `HUC-${r.level}`;
      return r.name ? `${r.name} (${tag}) \u2014 ${coord}` : `${tag} \u2014 ${coord}`;
    }
    return coord;
  }

  /** Panel chart + stat grid order: NMAD and Bias swapped vs config (map toolbar unchanged). */
  const chartStats = $derived.by(() => {
    const keys = accuracyStatKeys();
    const out = [...keys];
    const ib = out.indexOf('bias');
    const inm = out.indexOf('nmad');
    if (ib !== -1 && inm !== -1) {
      [out[ib], out[inm]] = [out[inm], out[ib]];
    }
    return out;
  });
  const isAccuracyStatistic = $derived(chartStats.includes(ui.statistic));
  const bounds = $derived(getModelLeadBounds(appConfig.models, ui.model));
  const leadMin = $derived(bounds.min);
  const leadMax = $derived(bounds.max);

  const palette = $derived(modelPalette(appConfig.models));
  /** Number of lead-day columns; shared by charts and winners table for alignment. */
  const nLeadCols = $derived(leadMax - leadMin + 1);

  const chartLead = $derived(hoverLead ?? ui.leadFractional);

  const winnerLeadKey = $derived.by(() => {
    // A pinned integer lead overrides the chart's fractional lead.
    if (ui.activeWindow && /^\d+$/.test(String(ui.activeWindow))) {
      return String(ui.activeWindow);
    }
    const hl = chartLead;
    const r = Math.round(Number(hl));
    const clamped = Math.max(leadMin, Math.min(leadMax, r));
    return String(clamped);
  });

  function modelLabel(key) {
    return appConfig.models.find((m) => m.key === key)?.label ?? key;
  }

  /** Rows from ``leads`` sorted by day (matches static JSON). */
  const winnerTableRows = $derived.by(() => {
    const p = winnersPayload;
    if (!p?.leads || typeof p.leads !== 'object') return [];
    return Object.keys(p.leads)
      .filter((k) => /^\d+$/.test(k))
      .map((day) => ({ day, winner: p.leads[day] }))
      .sort((a, b) => Number(a.day) - Number(b.day));
  });

  /** Map lead day string → winning model key (from in-memory winners payload; optional CSV column). */
  const winnerByLeadMap = $derived.by(() => {
    const rows = winnerTableRows;
    if (!rows.length) return null;
    return new Map(rows.map((r) => [String(r.day), r.winner]));
  });

  function seriesFor(statName) {
    return leadData.map(d => {
      if (!d.stats?.[statName]) return null;
      const s = d.stats[statName];
      return (s.no_data || s.value === null) ? null : s.value;
    });
  }

  function interpValue(data, frac) {
    const i = frac - leadMin;
    const lo = Math.max(0, Math.min(data.length - 1, Math.floor(i)));
    const hi = Math.min(data.length - 1, lo + 1);
    if (data[lo] === null) return data[hi];
    if (data[hi] === null) return data[lo];
    return data[lo] + (data[hi] - data[lo]) * (i - lo);
  }

  /** Per-lead forecast values for one model (aligned to leadMin..leadMax). */
  function forecastSeriesArray(modelKey, payload = forecastPayload) {
    const leadsObj = payload?.models?.[modelKey]?.leads;
    if (!leadsObj) return null;
    const n = leadMax - leadMin + 1;
    const arr = [];
    for (let i = 0; i < n; i++) {
      const cell = leadsObj[String(leadMin + i)];
      if (!cell || cell.no_data || cell.value == null) arr.push(null);
      else {
        const v = Number(cell.value);
        arr.push(Number.isFinite(v) ? v : null);
      }
    }
    return arr;
  }

  function forecastUnitsForModel(modelKey, payload = forecastPayload) {
    const leadsObj = payload?.models?.[modelKey]?.leads;
    if (!leadsObj) return '';
    for (let d = leadMin; d <= leadMax; d++) {
      const u = leadsObj[String(d)]?.units;
      if (u) return u;
    }
    return '';
  }

  function forecastAtChartLead(modelKey, hl) {
    const arr = forecastSeriesArray(modelKey);
    if (!arr) return null;
    const v = interpValue(arr, hl);
    if (v === null) return null;
    return { value: v, units: forecastUnitsForModel(modelKey) };
  }

  /** Forecast-variable selector buttons: those we have data for, in display order. */
  const forecastVars = $derived.by(() => {
    const keys = forecastByVar ? Object.keys(forecastByVar) : [];
    if (!keys.length) return FORECAST_VAR_ORDER.slice();  // show buttons before first load
    const ordered = FORECAST_VAR_ORDER.filter((k) => keys.includes(k));
    for (const k of keys) if (!ordered.includes(k)) ordered.push(k);  // any extras after
    return ordered;
  });

  /** Current value of forecast variable ``varKey`` at the hover lead for the active model.
   *  Mirrors ``statAtHoverLead`` so the top-chart buttons read like the bottom-chart ones. */
  function forecastVarAtHoverLead(varKey) {
    const payload = forecastByVar?.[varKey];
    const arr = forecastSeriesArray(ui.model, payload);
    if (!arr) return null;
    const v = interpValue(arr, chartLead);
    if (v === null || v === undefined) return null;
    return { value: v, units: forecastUnitsForModel(ui.model, payload) };
  }

  function selectForecastVariable(varKey) {
    ui.forecastVariable = varKey;
  }

  /** Validity date for forecast lead (API rule: ``initDate`` + ``lead`` calendar days, UTC). */
  function forecastValidDateIso(modelKey, leadDay) {
    const pm = forecastPayload?.forecast_calendar?.per_model?.[modelKey];
    const init = pm?.initDate;
    if (!init || typeof init !== 'string') return null;
    const parts = init.trim().split('-').map((p) => parseInt(p, 10));
    if (parts.length !== 3 || parts.some((n) => !Number.isFinite(n))) return null;
    const [yy, mm, dd] = parts;
    const lead = Math.round(Number(leadDay));
    const ms = Date.UTC(yy, mm - 1, dd) + lead * 86400000;
    return new Date(ms).toISOString().slice(0, 10);
  }

  function forecastValidDateDisplay(modelKey, leadDay) {
    const iso = forecastValidDateIso(modelKey, leadDay);
    if (!iso) return null;
    const [y, m, d] = iso.split('-').map((x) => parseInt(x, 10));
    const dt = new Date(Date.UTC(y, m - 1, d));
    return dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
  }

  /** Short date without year (e.g. "Mar 15"). */
  function forecastDateShort(modelKey, leadDay) {
    const iso = forecastValidDateIso(modelKey, leadDay);
    if (!iso) return null;
    const [y, m, d] = iso.split('-').map((x) => parseInt(x, 10));
    const dt = new Date(Date.UTC(y, m - 1, d));
    return dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric', timeZone: 'UTC' });
  }

  /**
   * Whole days between today (UTC) and the forecast valid date for ``leadDay``.
   * Not the same as ``leadDay`` once the app stays open past a model cycle: e.g.
   * a 12Z run from "yesterday" with lead=2 is only T+1d from today.
   */
  function leadFromToday(modelKey, leadDay) {
    const iso = forecastValidDateIso(modelKey, leadDay);
    if (!iso) return null;
    const [y, m, d] = iso.split('-').map((x) => parseInt(x, 10));
    const validMs = Date.UTC(y, m - 1, d);
    const now = new Date();
    const todayMs = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
    return Math.round((validMs - todayMs) / 86400000);
  }

  function formatLeadFromToday(modelKey, leadDay, fallbackRoundLead) {
    const n = leadFromToday(modelKey, leadDay);
    if (n == null) return `Day ${fallbackRoundLead}`;
    return `T${n >= 0 ? '+' : ''}${n}d`;
  }

  /** Date range string for a model's lead bounds (e.g. "Mar 15 – Mar 29"). */
  function forecastDateRange(modelKey) {
    const lb = getModelLeadBounds(appConfig.models, modelKey);
    const start = forecastDateShort(modelKey, lb.min);
    const end = forecastDateShort(modelKey, lb.max);
    if (start && end) return `${start} – ${end}`;
    return null;
  }

  function redrawLeadCharts() {
    drawChart();
    drawEnsembleForecastChart();
  }

  /**
   * Y-axis uses the highlighted metric only so its variation fills the plot.
   * Using min/max across all stats on one axis often makes every line look flat
   * when magnitudes differ (e.g. bias vs NRMSE).
   */
  function getYRange() {
    let min = Infinity;
    let max = -Infinity;
    for (const v of seriesFor(activeStat)) {
      if (v !== null) {
        if (v < min) min = v;
        if (v > max) max = v;
      }
    }
    if (!isFinite(min)) return { min: 0, max: 100 };
    let span = max - min;
    if (span < 1e-12) {
      const c = min;
      const inflate = Math.max(Math.abs(c) * 0.04, 0.01);
      min = c - inflate;
      max = c + inflate;
      span = max - min;
    }
    const pad = Math.max(span * 0.05, 1e-9);
    return { min: min - pad, max: max + pad };
  }

  function drawSmoothLine(ctx, points) {
    if (points.length < 2) return;
    if (points.length === 2) { ctx.moveTo(points[0].x, points[0].y); ctx.lineTo(points[1].x, points[1].y); return; }
    const n = points.length;
    const dx = [], dy = [], m = [];
    for (let i = 0; i < n - 1; i++) { dx.push(points[i+1].x - points[i].x); dy.push(points[i+1].y - points[i].y); m.push(dy[i] / dx[i]); }
    const tangent = [m[0]];
    for (let i = 1; i < n - 1; i++) tangent.push(m[i-1] * m[i] <= 0 ? 0 : (m[i-1] + m[i]) / 2);
    tangent.push(m[n - 2]);
    ctx.moveTo(points[0].x, points[0].y);
    for (let i = 0; i < n - 1; i++) {
      const p0 = points[i], p1 = points[i+1], d = p1.x - p0.x;
      ctx.bezierCurveTo(p0.x + d/3, p0.y + tangent[i]*d/3, p1.x - d/3, p1.y - tangent[i+1]*d/3, p1.x, p1.y);
    }
  }

  function drawChart() {
    if (!canvasEl || leadData.length === 0) return;
    const rect = canvasEl.parentElement.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvasEl.width = rect.width * dpr;
    canvasEl.height = rect.height * dpr;
    canvasEl.style.width = rect.width + 'px';
    canvasEl.style.height = rect.height + 'px';
    const ctx = canvasEl.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const w = rect.width, h = rect.height;
    const { min: yMin, max: yMax } = getYRange();
    const plotBottom = h - PAD.bottom;
    const ySpan = yMax - yMin;
    const yTickFmt =
      ySpan < 0.01 ? (v) => v.toFixed(4)
      : ySpan < 0.1 ? (v) => v.toFixed(3)
      : ySpan < 2 ? (v) => v.toFixed(2)
      : (v) => v.toFixed(1);

    // Column-aligned x: each lead day maps to center of its table column
    const nCols = leadMax - leadMin + 1;
    const colW = w / nCols;
    const plotLeft = colW / 2;
    const plotRight = colW / 2;
    const xForLead = lead => (lead - leadMin + 0.5) * colW;
    const yForValue = val => PLOT_TOP + (1 - (val - yMin) / (yMax - yMin)) * (plotBottom - PLOT_TOP);

    ctx.clearRect(0, 0, w, h);

    // Full-width grid
    ctx.strokeStyle = 'rgba(255,255,255,0.035)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const y = yForValue(yMin + (yMax - yMin) * i / 4);
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
    }
    ctx.save();
    ctx.beginPath();
    ctx.rect(0, PLOT_TOP, w, plotBottom - PLOT_TOP);
    ctx.clip();

    const stat = activeStat;
    const color = statColor(stat);
    const data = seriesFor(stat);
    const points = [];
    let firstDataVal = null;
    let lastDataVal = null;
    for (let i = 0; i < data.length; i++) {
      if (data[i] !== null) {
        points.push({ x: xForLead(leadMin + i), y: yForValue(data[i]) });
        if (firstDataVal === null) firstDataVal = data[i];
        lastDataVal = data[i];
      }
    }
    if (points.length >= 2) {
      ctx.beginPath(); drawSmoothLine(ctx, points);
      ctx.lineTo(points[points.length - 1].x, yForValue(yMin)); ctx.lineTo(points[0].x, yForValue(yMin)); ctx.closePath();
      ctx.fillStyle = color + '15'; ctx.fill();
      ctx.beginPath(); drawSmoothLine(ctx, points);
      ctx.strokeStyle = color; ctx.lineWidth = 2.6; ctx.stroke();
      for (const p of points) {
        ctx.beginPath(); ctx.arc(p.x, p.y, 2.5, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
      }
    }

    ctx.restore();

    // Y-axis labels: left margin at x=0
    ctx.font = '10px "DM Sans", system-ui';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= 4; i++) {
      const v = yMin + (yMax - yMin) * i / 4;
      const y = yForValue(v);
      const txt = yTickFmt(v);
      ctx.fillStyle = 'rgba(188,195,206,0.92)';
      ctx.fillText(txt, 2, y);
    }

    // Right margin: spread bar between first and last values
    if (firstDataVal !== null && lastDataVal !== null) {
      const y1 = yForValue(firstDataVal);
      const y2 = yForValue(lastDataVal);
      const yTop = Math.min(y1, y2);
      const yBot = Math.max(y1, y2);
      const barX = w - plotRight / 2;

      // Vertical spread bar
      ctx.strokeStyle = color + '44';
      ctx.lineWidth = 2;
      ctx.beginPath(); ctx.moveTo(barX, yTop); ctx.lineTo(barX, yBot); ctx.stroke();

      // End caps
      const capW = 4;
      ctx.strokeStyle = color + '88';
      ctx.lineWidth = 1.5;
      ctx.beginPath(); ctx.moveTo(barX - capW, yTop); ctx.lineTo(barX + capW, yTop); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(barX - capW, yBot); ctx.lineTo(barX + capW, yBot); ctx.stroke();

      // Labels at top and bottom
      ctx.font = '600 9px "DM Sans", system-ui';
      ctx.textAlign = 'center';
      const topVal = firstDataVal > lastDataVal ? firstDataVal : lastDataVal;
      const botVal = firstDataVal > lastDataVal ? lastDataVal : firstDataVal;
      ctx.textBaseline = 'bottom';
      ctx.fillStyle = color + 'cc';
      ctx.fillText(yTickFmt(topVal), barX, yTop - 2);
      ctx.textBaseline = 'top';
      ctx.fillText(yTickFmt(botVal), barX, yBot + 2);

      // Per-day rate in the middle of the bar
      const nDays = leadMax - leadMin;
      if (nDays > 0 && yBot - yTop > 20) {
        const delta = lastDataVal - firstDataVal;
        const perDay = delta / nDays;
        const sign = perDay >= 0 ? '+' : '';
        const arrow = perDay > 0 ? '\u2191' : perDay < 0 ? '\u2193' : '';
        const rateTxt = `${arrow}${sign}${perDay.toFixed(2)}/d`;
        ctx.font = '600 8px "DM Sans", system-ui';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        const midY = (yTop + yBot) / 2;
        // Backdrop pill
        const tw = ctx.measureText(rateTxt).width;
        ctx.fillStyle = 'rgba(12,15,22,0.85)';
        ctx.fillRect(barX - tw / 2 - 3, midY - 6, tw + 6, 12);
        ctx.fillStyle = color + 'cc';
        ctx.fillText(rateTxt, barX, midY);
      }
    }


    ctx.textBaseline = 'alphabetic';

    // Lead cursor (scrub or locked position)
    const hl = chartLead;
    if (hl >= leadMin && hl <= leadMax) {
      const hx = xForLead(hl);
      ctx.setLineDash([3, 3]); ctx.strokeStyle = 'rgba(255,255,255,0.15)'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(hx, PLOT_TOP); ctx.lineTo(hx, plotBottom); ctx.stroke(); ctx.setLineDash([]);

      {
        const st = activeStat;
        const val = interpValue(seriesFor(st), hl);
        if (val !== null) {
          const c = statColor(st);
          const y = yForValue(val);
          ctx.beginPath(); ctx.arc(hx, y, 7, 0, Math.PI * 2); ctx.fillStyle = c + '20'; ctx.fill();
          ctx.beginPath(); ctx.arc(hx, y, 4, 0, Math.PI * 2); ctx.fillStyle = c; ctx.fill();
          const units = leadData[0]?.stats?.[st]?.units || '';
          const label = units ? `${val.toFixed(2)} ${units}` : val.toFixed(2);
          ctx.save();
          ctx.font = '600 12px "DM Sans", system-ui';
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          const tw = ctx.measureText(label).width;
          const padX = 9;
          const padY = 10;
          const halfW = tw / 2 + padX;
          let lx = hx;
          const xMin = plotLeft + halfW + 2;
          const xMax = w - plotRight - halfW - 2;
          if (lx < xMin) lx = xMin;
          if (lx > xMax) lx = xMax;
          const bx = lx - tw / 2 - padX;
          const by = HOVER_LABEL_Y - padY;
          const bw = tw + padX * 2;
          const bh = padY * 2;
          ctx.fillStyle = 'rgba(12, 14, 20, 0.94)';
          ctx.strokeStyle = c + '66';
          ctx.lineWidth = 1;
          ctx.beginPath();
          if (typeof ctx.roundRect === 'function') {
            ctx.roundRect(bx, by, bw, bh, 6);
          } else {
            ctx.rect(bx, by, bw, bh);
          }
          ctx.fill();
          ctx.stroke();
          ctx.fillStyle = c;
          ctx.fillText(label, lx, HOVER_LABEL_Y);
          ctx.restore();
        }
      }
    }
  }

  function leadFromClientX(clientX) {
    const trackEl = chartTrackRegionEl ?? canvasEl?.parentElement;
    if (!trackEl) return ui.leadFractional;
    const rect = trackEl.getBoundingClientRect();
    const mx = clientX - rect.left;
    const nCols = leadMax - leadMin + 1;
    const colW = rect.width / nCols;
    let lead = leadMin + mx / colW - 0.5;
    lead = Math.max(leadMin, Math.min(leadMax, lead));
    return Math.round(lead * 10) / 10;
  }

  function flushChartScrubLead() {
    chartScrubRafId = 0;
    const L = pendingScrubLead;
    pendingScrubLead = null;
    if (L == null || L === hoverLead) return;
    hoverLead = L;
    ui.leadFractional = L;
    onleadchange?.(L);
    redrawLeadCharts();
  }

  function handleChartsTrackMouseMove(e) {
    if (!canvasEl || leadData.length === 0 || leadScrubLocked) return;
    const lead = leadFromClientX(e.clientX);
    if (lead === hoverLead) return;
    pendingScrubLead = lead;
    if (!chartScrubRafId) {
      chartScrubRafId = requestAnimationFrame(flushChartScrubLead);
    }
  }

  function handleChartsTrackMouseLeave() {
    if (chartScrubRafId) {
      cancelAnimationFrame(chartScrubRafId);
      chartScrubRafId = 0;
    }
    pendingScrubLead = null;
    if (leadScrubLocked) return;
    hoverLead = null;
    redrawLeadCharts();
  }

  function handleChartsTrackPointerDown(e) {
    if (e.button !== 0 || leadData.length === 0) return;
    scrubPointerDown = { x: e.clientX, y: e.clientY, t: Date.now() };
    try {
      e.currentTarget.setPointerCapture(e.pointerId);
    } catch {
      /* ignore */
    }
  }

  function handleChartsTrackPointerUp(e) {
    if (e.button !== 0 || leadData.length === 0) return;
    const down = scrubPointerDown;
    scrubPointerDown = null;
    try {
      e.currentTarget.releasePointerCapture(e.pointerId);
    } catch {
      /* ignore */
    }
    if (!down) return;
    const dist = Math.hypot(e.clientX - down.x, e.clientY - down.y);
    const dt = Date.now() - down.t;
    if (dt > 450 || dist > 12) return;

    if (leadScrubLocked) {
      leadScrubLocked = false;
      redrawLeadCharts();
      return;
    }
    const lead = leadFromClientX(e.clientX);
    leadScrubLocked = true;
    hoverLead = null;
    ui.leadFractional = lead;
    onleadchange?.(lead);
    redrawLeadCharts();
  }

  function handleChartsTrackPointerCancel(e) {
    scrubPointerDown = null;
    try {
      e.currentTarget.releasePointerCapture(e.pointerId);
    } catch {
      /* ignore */
    }
  }

  function statAtHoverLead(statName) {
    const hl = chartLead;
    const val = interpValue(seriesFor(statName), hl);
    if (val === null) return null;
    return { value: val, units: leadData[0]?.stats?.[statName]?.units || '' };
  }

  /**
   * Good / ok / bad tiers for panel values (same units as API: % for NRMSE/NMAD,
   * unitless -1..1 for CC, mm for bias).
   * Cutoffs rounded from a one-off analysis of static_export/ranges (legend vmin/vmax per
   * model/stat/period) so tiers sit near the scale of real outputs instead of mistaken 0–1 bounds.
   */
  function colorClass(statName, value) {
    if (value == null) return '';
    if (statName === 'cc') return value > 0.42 ? 'c-good' : value > 0.20 ? 'c-ok' : 'c-bad';
    if (statName === 'nmad') return value < 100 ? 'c-good' : value < 125 ? 'c-ok' : 'c-bad';
    if (statName === 'nrmse') return value < 350 ? 'c-good' : value < 510 ? 'c-ok' : 'c-bad';
    if (statName === 'bias') {
      const a = Math.abs(value);
      return a < 2 ? 'c-good' : a < 4 ? 'c-ok' : 'c-bad';
    }
    return '';
  }

  let panelEntering = $state(false);
  /** Only true right after we transition from no region → region (not on every reactive run). */
  let hadSelectedRegion = $state(false);

  // Keep panel emphasis aligned with the map's current statistic (avoids stale default e.g. NRMSE vs bias).
  $effect(() => {
    if (!ui.selectedRegion) return;
    const keys = accuracyStatKeys();
    if (!keys.length) return;
    const s = ui.statistic;
    activeStat = keys.includes(s) ? s : keys[0];
  });

  // Entrance animation only when the panel opens, not when statistic/period/etc. change while open.
  $effect(() => {
    const has = !!ui.selectedRegion;
    if (has && !hadSelectedRegion) {
      panelEntering = true;
      setTimeout(() => { panelEntering = false; }, 450);
    }
    hadSelectedRegion = has;
  });

  $effect(() => {
    if (!ui.selectedRegion) {
      leadScrubLocked = false;
      scrubPointerDown = null;
      hoverLead = null;
    }
  });

  function closePanel() {
    ui.selectedRegion = null;
  }

  function handleExportStatsCsv() {
    if (!leadData.length) return;
    const keys = chartStats;
    const includeWinners =
      isAccuracyStatistic && winnerByLeadMap && winnerByLeadMap.size > 0;
    downloadPanelStatsCsv({
      leadData,
      statKeys: keys,
      modelKey: ui.model,
      models: appConfig.models,
      period: ui.period,
      month: ui.month,
      season: ui.season,
      region: ui.selectedRegion,
      bestModelByLead: includeWinners ? winnerByLeadMap : null,
      bestModelColumnHeader: includeWinners
        ? `Best model (${statLabel(ui.statistic)})`
        : null,
      forecastPayload,
    });
  }

  function selectPeriod(period, month, season) {
    ui.period = period;
    if (month) ui.month = month;
    if (season) ui.season = season;
    onperiodchange?.();
  }

  const periodOptions = [
    { key: 'yearly', label: 'Yearly', title: PERIOD_HELP.yearly },
    { key: 'djf', label: 'DJF', period: 'seasonal', season: 'djf', title: SEASON_PERIOD_HELP.djf },
    { key: 'mam', label: 'MAM', period: 'seasonal', season: 'mam', title: SEASON_PERIOD_HELP.mam },
    { key: 'jja', label: 'JJA', period: 'seasonal', season: 'jja', title: SEASON_PERIOD_HELP.jja },
    { key: 'son', label: 'SON', period: 'seasonal', season: 'son', title: SEASON_PERIOD_HELP.son },
  ];

  function currentPeriodKey() {
    if (ui.period === 'yearly') return 'yearly';
    if (ui.period === 'seasonal') return ui.season;
    return 'yearly';
  }

  // Build a key from the fetch dependencies — only refetch when this changes
  let lastFetchKey = '';

  function regionGeometryKey(r) {
    if (r.type === 'point') {
      return `p:${r.coordinates[1].toFixed(5)},${r.coordinates[0].toFixed(5)}`;
    }
    if (r.type === 'rectangle') {
      return `r:${r.bounds.map((x) => x.toFixed(5)).join(',')}`;
    }
    if (r.type === 'polygon') {
      return `g:${r.coordinates.map(([lng, lat]) => `${lng.toFixed(5)},${lat.toFixed(5)}`).join(';')}`;
    }
    if (r.type === 'admin' && r.level && r.fips) {
      return `a:${r.level}:${r.fips}`;
    }
    if (r.type === 'huc' && r.huc) {
      return `h:${r.huc}`;
    }
    return '';
  }

  function fetchKey() {
    const r = ui.selectedRegion;
    if (!r) return '';
    const geo = regionGeometryKey(r);
    if (!geo) return '';
    return `${geo}|${ui.model}|${ui.period}|${ui.month}|${ui.season}`;
  }

  $effect(() => {
    // Read reactive deps to track them
    const region = ui.selectedRegion;
    const model = ui.model;
    const period = ui.period;
    const month = ui.month;
    const season = ui.season;

    // Compute key from deps
    const key = fetchKey();

    if (!key) {
      lastFetchKey = '';
      leadFetchGen += 1;
      if (leadData.length > 0) leadData = [];
      return;
    }

    if (key === lastFetchKey) return; // no change
    lastFetchKey = key;

    const b = getModelLeadBounds(appConfig.models, model);
    const isFirstLoad = leadData.length === 0;
    const lid = leadFetchGen + 1;
    leadFetchGen = lid;

    // Use untracked timeout to avoid writing $state inside tracked $effect
    setTimeout(() => {
      if (isFirstLoad) loading = true;

      fetchStatsAllLeads({
        model,
        region,
        period,
        month,
        season,
        minLead: b.min,
        maxLead: b.max,
      }).then((results) => {
        if (lid !== leadFetchGen) return;
        leadData = results;
        loading = false;
      });
    }, 0);
  });

  $effect(() => {
    const region = ui.selectedRegion;
    const stat = ui.statistic;
    const period = ui.period;
    const month = ui.month;
    const season = ui.season;
    const accKeys = accuracyStatKeys();
    const b = getModelLeadBounds(appConfig.models, ui.model);

    if (!region || !accKeys.includes(stat)) {
      winnersGen += 1;
      winnersPayload = null;
      winnersLoading = false;
      return;
    }

    const my = winnersGen + 1;
    winnersGen = my;
    winnersLoading = true;
    // Keep showing previous rankings while fetching so the panel does not jump to "Loading…".

    queueMicrotask(async () => {
      const data = await fetchLeadWinnersForRegion({
        region,
        statistic: stat,
        period,
        month,
        season,
        minLead: b.min,
        maxLead: b.max,
      });
      if (my !== winnersGen) return;
      winnersPayload = data;
      winnersLoading = false;
    });
  });

  $effect(() => {
    const region = ui.selectedRegion;
    if (!region) {
      lastForecastGeoKey = '';
      forecastFetchGen += 1;
      forecastByVar = null;
      return;
    }
    const geo = regionGeometryKey(region);
    if (!geo) {
      lastForecastGeoKey = '';
      forecastFetchGen += 1;
      forecastByVar = null;
      return;
    }
    if (geo === lastForecastGeoKey) return;
    lastForecastGeoKey = geo;
    const fid = forecastFetchGen + 1;
    forecastFetchGen = fid;
    queueMicrotask(async () => {
      const data = await fetchForecastAllVariables({ region });
      if (fid !== forecastFetchGen) return;
      forecastByVar = data;  // { [variable]: payload } or null
    });
  });

  /** Multi-model forecast curves from ``POST /api/stats/forecast`` only (no synthetic data). */
  function drawEnsembleForecastChart() {
    if (!ensembleForecastCanvasEl) return;
    const rect = ensembleForecastCanvasEl.parentElement.getBoundingClientRect();
    if (!rect.width) return;
    const dpr = window.devicePixelRatio || 1;
    ensembleForecastCanvasEl.width = rect.width * dpr;
    ensembleForecastCanvasEl.height = rect.height * dpr;
    ensembleForecastCanvasEl.style.width = rect.width + 'px';
    ensembleForecastCanvasEl.style.height = rect.height + 'px';
    const ctx = ensembleForecastCanvasEl.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const w = rect.width, h = rect.height;
    const plotBottom = h - PAD.bottom;

    const models = appConfig.models;
    if (!models.length) return;
    const n = leadMax - leadMin + 1;
    if (n < 2) return;

    const byModel = forecastPayload?.models;
    if (!byModel || typeof byModel !== 'object') {
      ctx.clearRect(0, 0, w, h);
      ctx.strokeStyle = 'rgba(255,255,255,0.035)';
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const y = PLOT_TOP + ((plotBottom - PLOT_TOP) * i) / 4;
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
      }
      return;
    }

    const allSeries = models.map((m) => {
      const leadsObj = byModel[m.key]?.leads;
      return Array.from({ length: n }, (_, i) => {
        const leadDay = leadMin + i;
        const cell = leadsObj?.[String(leadDay)];
        if (!cell || cell.no_data || cell.value == null) return null;
        const v = Number(cell.value);
        return Number.isFinite(v) ? v : null;
      });
    });

    let gMin = Infinity;
    let gMax = -Infinity;
    for (const s of allSeries) {
      for (const v of s) {
        if (v != null && Number.isFinite(v)) {
          if (v < gMin) gMin = v;
          if (v > gMax) gMax = v;
        }
      }
    }
    if (!Number.isFinite(gMin) || !Number.isFinite(gMax)) {
      ctx.clearRect(0, 0, w, h);
      ctx.strokeStyle = 'rgba(255,255,255,0.035)';
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const y = PLOT_TOP + ((plotBottom - PLOT_TOP) * i) / 4;
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
      }
      return;
    }
    const pad = Math.max((gMax - gMin) * 0.06, 1e-6);
    gMin -= pad;
    gMax += pad;
    const yMin = gMin;
    const yMax = gMax;
    const ySpan = yMax - yMin || 1e-9;
    const yTickFmt = ySpan < 2 ? v => v.toFixed(2) : v => v.toFixed(1);

    // Column-aligned x: matches primary chart and table columns
    const colW = w / n;
    const plotLeft = colW / 2;
    const plotRight = colW / 2;
    const xForLead = lead => (lead - leadMin + 0.5) * colW;
    const yForValue = val => PLOT_TOP + (1 - (val - yMin) / ySpan) * (plotBottom - PLOT_TOP);

    ctx.clearRect(0, 0, w, h);

    // Full-width grid
    ctx.strokeStyle = 'rgba(255,255,255,0.035)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const y = yForValue(yMin + ySpan * i / 4);
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
    }
    ctx.save();
    ctx.beginPath();
    ctx.rect(0, PLOT_TOP, w, plotBottom - PLOT_TOP);
    ctx.clip();

    /** Match stats chart area fill: ``fillStyle = hex + '15'`` (~8.2% alpha). Model colors are `hsl()`, so use alpha here. */
    const FORECAST_AREA_FILL_ALPHA = 0x15 / 255;

    const activeModelKey = ui.model;
    const termLabels = []; // { y, label, color, active }

    // Draw dimmed lines first, then highlighted on top
    const drawOrder = models.map((m, mi) => ({ m, mi, active: m.key === activeModelKey }));
    drawOrder.sort((a, b) => (a.active ? 1 : 0) - (b.active ? 1 : 0));

    for (const { m, mi, active } of drawOrder) {
      const color = palette[m.key]?.border || '#888';
      const values = allSeries[mi];
      const points = [];
      for (let i = 0; i < values.length; i++) {
        const v = values[i];
        if (v != null) points.push({ x: xForLead(leadMin + i), y: yForValue(v) });
      }
      if (points.length === 0) continue;

      const alpha = active ? 1.0 : 0.48;
      const lineW = active ? 2.6 : 1.85;
      const dotR = active ? 2.5 : 0;

      if (active && points.length >= 2) {
        ctx.beginPath(); drawSmoothLine(ctx, points);
        ctx.lineTo(points[points.length - 1].x, yForValue(yMin));
        ctx.lineTo(points[0].x, yForValue(yMin));
        ctx.closePath();
        ctx.fillStyle = color;
        ctx.globalAlpha = FORECAST_AREA_FILL_ALPHA;
        ctx.fill();
        ctx.globalAlpha = 1;
      }

      ctx.globalAlpha = alpha;
      if (points.length >= 2) {
        ctx.beginPath(); drawSmoothLine(ctx, points);
        ctx.strokeStyle = color; ctx.lineWidth = lineW; ctx.stroke();
      } else if (points.length === 1) {
        ctx.beginPath(); ctx.arc(points[0].x, points[0].y, active ? 2.5 : 1.5, 0, Math.PI * 2);
        ctx.fillStyle = color; ctx.fill();
      }

      if (dotR > 0 && points.length >= 2) {
        for (const p of points) {
          ctx.beginPath(); ctx.arc(p.x, p.y, dotR, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
        }
      }
      ctx.globalAlpha = 1.0;

      const lastPt = points[points.length - 1];
      const shortLabel = palette[m.key]?.ident ?? m.label;
      termLabels.push({ y: lastPt.y, label: shortLabel, color, active });
    }

    ctx.restore();

    // Y-axis labels: left margin at x=0
    ctx.font = '10px "DM Sans", system-ui';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= 4; i++) {
      const v = yMin + ySpan * i / 4;
      const y = yForValue(v);
      const txt = yTickFmt(v);
      ctx.fillStyle = 'rgba(188,195,206,0.92)';
      ctx.fillText(txt, 2, y);
    }

    // Right margin: terminal labels with collision resolution
    termLabels.sort((a, b) => a.y - b.y);
    const minGap = 10;
    for (let pass = 0; pass < 4; pass++) {
      for (let i = 1; i < termLabels.length; i++) {
        const gap = termLabels[i].y - termLabels[i - 1].y;
        if (gap < minGap) {
          const nudge = (minGap - gap) / 2;
          termLabels[i - 1].y -= nudge;
          termLabels[i].y += nudge;
        }
      }
    }
    // Spreading can push the stack past the canvas when end values converge; shift the
    // whole block back so the 9px text stays fully on-canvas (top and bottom).
    if (termLabels.length) {
      const labelMargin = 7;
      const topOver = labelMargin - termLabels[0].y;
      if (topOver > 0) for (const t of termLabels) t.y += topOver;
      const botOver = termLabels[termLabels.length - 1].y - (h - labelMargin);
      if (botOver > 0) for (const t of termLabels) t.y -= botOver;
    }
    const marginCenterX = w - plotRight / 2;
    ctx.font = '600 9px "DM Sans", system-ui';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    for (const t of termLabels) {
      ctx.fillStyle = t.color;
      ctx.globalAlpha = t.active ? 1 : 0.5;
      ctx.fillText(t.label, marginCenterX, t.y);
    }
    ctx.globalAlpha = 1;

    ctx.textBaseline = 'alphabetic';

    const hlFc = chartLead;
    if (hlFc >= leadMin && hlFc <= leadMax) {
      const hx = xForLead(hlFc);
      ctx.setLineDash([3, 3]);
      ctx.strokeStyle = 'rgba(255,255,255,0.15)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(hx, PLOT_TOP);
      ctx.lineTo(hx, plotBottom);
      ctx.stroke();
      ctx.setLineDash([]);

      const fc = forecastAtChartLead(ui.model, hlFc);
      if (fc !== null) {
        const c = palette[ui.model]?.border || '#6eb5ff';
        const yPt = yForValue(fc.value);
        ctx.beginPath();
        ctx.arc(hx, yPt, 7, 0, Math.PI * 2);
        ctx.fillStyle = c;
        ctx.globalAlpha = 0x20 / 255;
        ctx.fill();
        ctx.globalAlpha = 1;
        ctx.beginPath();
        ctx.arc(hx, yPt, 4, 0, Math.PI * 2);
        ctx.fillStyle = c;
        ctx.fill();

        const u = fc.units ? ` ${fc.units}` : '';
        const leadStr = formatLeadFromToday(ui.model, hlFc, Math.round(hlFc));
        const label = `${leadStr} · ${fc.value.toFixed(2)}${u}`;
        ctx.save();
        ctx.font = '600 12px "DM Sans", system-ui';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        const tw = ctx.measureText(label).width;
        const padX = 9;
        const padY = 10;
        const halfW = tw / 2 + padX;
        let lx = hx;
        const xMin = plotLeft + halfW + 2;
        const xMax = w - plotRight - halfW - 2;
        if (lx < xMin) lx = xMin;
        if (lx > xMax) lx = xMax;
        const bx = lx - tw / 2 - padX;
        const by = HOVER_LABEL_Y - padY;
        const bw = tw + padX * 2;
        const bh = padY * 2;
        ctx.fillStyle = 'rgba(12, 14, 20, 0.94)';
        ctx.strokeStyle = c;
        ctx.globalAlpha = 1;
        ctx.lineWidth = 1;
        ctx.beginPath();
        if (typeof ctx.roundRect === 'function') {
          ctx.roundRect(bx, by, bw, bh, 6);
        } else {
          ctx.rect(bx, by, bw, bh);
        }
        ctx.fill();
        ctx.globalAlpha = 0x66 / 255;
        ctx.stroke();
        ctx.globalAlpha = 1;
        ctx.fillStyle = c;
        ctx.fillText(label, lx, HOVER_LABEL_Y);
        ctx.restore();
      }
    }
  }

  $effect(() => {
    // `ui.model` must be read here so the ensemble forecast chart re-highlights when the map model changes
    // (forecast payload is keyed by region only, so it does not update on model-only changes).
    const _ = [leadData, activeStat, winnerLeadKey, leadScrubLocked, chartLead, forecastPayload, ui.model];
    requestAnimationFrame(() => { drawChart(); drawEnsembleForecastChart(); });
  });
</script>

<svelte:window onclick={handleModelMenuWindow} onkeydown={handleModelMenuWindow} />

{#if ui.selectedRegion}
  <div class="panel" class:entering={panelEntering}>
    {#if loading}
      <div class="panel-loading">
        <div class="loading-dot"></div>
        Loading stats...
      </div>
    {:else if leadData.length > 0}
      <div class="panel-body">
        <button class="close-btn" type="button" title="Close panel" aria-label="Close panel" onclick={closePanel}>
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2">{@html glyphPanelClose}</svg>
        </button>
        <div class="panel-content">
          <div class="chart-col">
            <!-- svelte-ignore a11y_no_static_element_interactions -->
            <div
              class="charts-track-region"
              class:lead-locked={leadScrubLocked}
              bind:this={chartTrackRegionEl}
              onmousemove={handleChartsTrackMouseMove}
              onmouseleave={handleChartsTrackMouseLeave}
              onpointerdown={handleChartsTrackPointerDown}
              onpointerup={handleChartsTrackPointerUp}
              onpointercancel={handleChartsTrackPointerCancel}
              role="presentation"
            >
              <div class="chart-section">
                <div class="section-label chart-label" style:padding-left="{50 / nLeadCols}%">Ensemble Forecast — <span style:color={FORECAST_VAR_COLOR[ui.forecastVariable] || '#6eb5ff'}>{FORECAST_VAR_LABEL[ui.forecastVariable] || ui.forecastVariable}</span></div>
                <div class="lead-chart">
                  <canvas bind:this={ensembleForecastCanvasEl}></canvas>
                </div>
              </div>

              <div class="chart-section">
                <div class="section-label chart-label" style:padding-left="{50 / nLeadCols}%">Accuracy vs time — <span style:color={palette[ui.model]?.text || '#6eb5ff'}>{modelLabel(ui.model)}</span></div>
                <div class="lead-chart">
                  <canvas bind:this={canvasEl}></canvas>
                </div>
              </div>
            </div>

            <div class="compare-section">
          <div
            class="section-label compare-section-label"
            style:padding-left="{50 / nLeadCols}%"
            title={`For each lead day, which model wins on ${statLabel(ui.statistic)} for this drawn region and accumulation window (use the period selector next to the chart). The map and this table both use the statistic you pick in the stat cards.`}
          >
            <span>Best model by day — <span style:color={statColor(ui.statistic)}>{statLabel(ui.statistic)}</span></span>
          </div>
          {#if !isAccuracyStatistic}
            <div class="winners-loading">Domain winners apply to accuracy metrics only.</div>
          {:else if winnersLoading && winnersPayload === null}
            <div class="winners-loading">Loading rankings…</div>
          {:else if winnerTableRows.length === 0}
            <div class="winners-loading">No winner data for this slice.</div>
          {:else}
            <div class="winners-table-wrap">
              <table class="compare-table winners-table winners-table-cols">
                <thead>
                  <tr>
                    {#each winnerTableRows as row}
                      {@const dayDate = forecastDateShort(ui.model, row.day)}
                      <th
                        class="winners-day-head"
                        class:winners-col-current={row.day === winnerLeadKey}
                        title={dayDate ? `Lead ${row.day} — ${dayDate}` : `Lead ${row.day}`}
                      >{dayDate ?? row.day}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    {#each winnerTableRows as row}
                      {@const pc = row.winner ? palette[row.winner] : null}
                      {@const id = pc?.ident ?? ''}
                      <td class:winners-col-current={row.day === winnerLeadKey}>
                        {#if row.winner}
                          {#if pc}
                            <span
                              class="winner-pill winner-pill-compact"
                              style:background={pc.bg}
                              style:color={pc.fg}
                              style:border-color={pc.border}
                              title={modelLabel(row.winner)}
                            ><span class="winner-ident">{id}</span></span>
                          {:else}
                            <span class="winner-pill winner-pill-compact winner-pill-fallback" title={modelLabel(row.winner)}><span class="winner-ident">{id}</span></span>
                          {/if}
                        {:else}
                          <span class="compare-nodata">&mdash;</span>
                        {/if}
                      </td>
                    {/each}
                  </tr>
                </tbody>
              </table>
            </div>
          {/if}
            </div>
          </div>

          <div class="side-strip">
            <!-- Selector blocks sit at the same vertical layer as the charts:
                 forecast variables anchored to the top (Ensemble) chart, accuracy
                 stats anchored to the bottom (Accuracy) chart. -->
            <div class="selector-stack">
              <!-- svelte-ignore a11y_no_static_element_interactions -->
              <div class="selector-block">
                {#each forecastVars as v}
                  {@const fc = forecastVarAtHoverLead(v)}
                  <!-- svelte-ignore a11y_click_events_have_key_events -->
                  <!-- svelte-ignore a11y_no_static_element_interactions -->
                  <div
                    class="stat-card"
                    class:highlighted={ui.forecastVariable === v}
                    style="--stat-color: {FORECAST_VAR_COLOR[v] || '#6eb5ff'}"
                    title={`Show ${FORECAST_VAR_LABEL[v] || v} forecast`}
                    onclick={() => selectForecastVariable(v)}
                  >
                    <div class="stat-label">{FORECAST_VAR_LABEL[v] || v}</div>
                    <div class="stat-val">{fc ? fc.value.toFixed(1) : '—'}</div>
                    <div class="stat-unit">{fc?.units || ''}</div>
                  </div>
                {/each}
              </div>
              <!-- svelte-ignore a11y_no_static_element_interactions -->
              <div class="selector-block">
                {#each chartStats as stat}
                  {@const s = statAtHoverLead(stat)}
                  <!-- svelte-ignore a11y_click_events_have_key_events -->
                  <!-- svelte-ignore a11y_no_static_element_interactions -->
                  <div
                    class="stat-card"
                    class:highlighted={activeStat === stat}
                    style="--stat-color: {statColor(stat)}"
                    title={statisticTooltip(stat)}
                    onclick={() => { activeStat = stat; ui.statistic = stat; ui.activeWindow = null; onstatchange?.({ target: { value: stat } }); }}
                  >
                    <div class="stat-label">{statLabel(stat)}</div>
                    <div class="stat-val {s ? colorClass(stat, s.value) : ''}">{s ? s.value.toFixed(2) : '—'}</div>
                    <div class="stat-unit">{s?.units || ''}</div>
                  </div>
                {/each}
              </div>
            </div>
            <div class="strip-controls">
              <div class="model-select" bind:this={modelSelectEl}>
                <button
                  type="button"
                  class="model-select-trigger"
                  class:open={modelMenuOpen}
                  aria-haspopup="listbox"
                  aria-expanded={modelMenuOpen}
                  aria-label="Forecast model"
                  title={`Model driving the map overlay and lead slider. Leads ${leadMin}–${leadMax}.`}
                  onclick={() => (modelMenuOpen = !modelMenuOpen)}
                >
                  <span class="model-name" style:color={palette[ui.model]?.text || 'var(--text-primary)'}>{modelLabel(ui.model)}</span>
                  <span class="model-range">{forecastDateRange(ui.model) ?? `leads ${leadMin}–${leadMax}`}</span>
                  <svg class="model-caret" width="11" height="11" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 4.5L6 7.5L9 4.5"/></svg>
                </button>
                {#if modelMenuOpen}
                  <div class="model-menu" role="listbox" aria-label="Forecast model">
                    {#each appConfig.models as m}
                      {@const dateRange = forecastDateRange(m.key)}
                      {@const lb = getModelLeadBounds(appConfig.models, m.key)}
                      <button
                        type="button"
                        class="model-option"
                        class:active={m.key === ui.model}
                        role="option"
                        aria-selected={m.key === ui.model}
                        onclick={() => selectModel(m.key)}
                      >
                        <span class="model-name" style:color={palette[m.key]?.text || 'var(--text-primary)'}>{m.label}</span>
                        <span class="model-range">{dateRange ?? `leads ${lb.min}–${lb.max}`}</span>
                      </button>
                    {/each}
                  </div>
                {/if}
              </div>
              <div class="period-seg">
                {#each periodOptions as opt}
                  <button
                    type="button"
                    class="period-btn"
                    class:active={currentPeriodKey() === opt.key}
                    title={opt.title}
                    onclick={() => selectPeriod(opt.period || 'yearly', null, opt.season || null)}
                  >{opt.label}</button>
                {/each}
              </div>
              <button type="button" class="export-btn" title="Download statistics as CSV" onclick={handleExportStatsCsv}>
                <svg class="export-icon" width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v2h8v-2"/><path d="M8 2v9"/><path d="M5 8l3 3 3-3"/></svg>
                Export CSV
              </button>
            </div>
          </div>
        </div>
      </div>
    {:else}
      <div class="panel-loading">No data available</div>
    {/if}
  </div>
{/if}

<style>
  /* ── Panel shell ── */
  .panel {
    /* Shared chart height: each lead-chart and each selector block use this so the
       two 2×2 selector blocks line up exactly with the two stacked charts. Bump it
       to make both the graphs and the anchored cards larger / easier to read. */
    --chart-h: 150px;
    /* Height of each chart's title row. The canvas shrinks by this much so the
       section (title + canvas) still equals --chart-h and stays aligned with the
       selector blocks; the title therefore never overlaps the chart or its pill. */
    --chart-title-h: 16px;
    position: absolute;
    bottom: 0;
    left: 0;
    right: 0;
    padding-bottom: env(safe-area-inset-bottom, 0px);
    background: var(--panel-bg);
    backdrop-filter: blur(24px) saturate(1.4);
    -webkit-backdrop-filter: blur(24px) saturate(1.4);
    border-top: 1px solid rgba(255,255,255,0.06);
    border-radius: 14px 14px 0 0;
    max-height: min(62vh, 100dvh - 12%);
    overflow-y: auto;
    z-index: 20;
    -webkit-overflow-scrolling: touch;
  }
  .panel::before {
    content: '';
    position: absolute;
    top: 0;
    left: 40px;
    right: 40px;
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--accent) 20%, var(--accent) 80%, transparent);
    opacity: 0.2;
    border-radius: 1px;
    pointer-events: none;
  }
  .panel.entering {
    animation: slideUp 0.4s cubic-bezier(0.16, 1, 0.3, 1);
  }
  @keyframes slideUp {
    from { transform: translateY(100%); opacity: 0.5; }
    to   { transform: translateY(0);    opacity: 1; }
  }
  /* ── Body layout ── */
  .panel-body {
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 0;
    padding: 6px 16px 10px;
  }

  /* ── Content row: chart column + stat cards ── */
  .panel-content {
    display: flex;
    gap: 10px;
    min-height: 0;
  }
  .chart-col {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .charts-track-region {
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
    cursor: crosshair;
    touch-action: none;
    border-radius: 6px;
  }
  .charts-track-region.lead-locked {
    box-shadow: inset 0 0 0 1px rgba(110, 181, 255, 0.35);
    cursor: pointer;
  }
  .chart-section {
    min-width: 0;
    position: relative;
  }
  /* Dedicated header row above the canvas (no longer floated over it), so the
     persistent hover-value pill can never sit on top of the title text. */
  .chart-label {
    height: var(--chart-title-h);
    line-height: var(--chart-title-h);
    margin-bottom: 0;
    pointer-events: none;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .lead-chart {
    width: 100%;
    height: calc(var(--chart-h) - var(--chart-title-h));
    position: relative;
    cursor: crosshair;
    touch-action: none;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .lead-chart canvas { position: absolute; inset: 0; width: 100%; height: 100%; }

  /* ── Side strip: stats + controls ── */
  .side-strip {
    flex-shrink: 0;
    width: 224px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  /* Sits flush in the panel's top-left corner and shares its 14px radius so the two
     curves melt together (the panel clips content to that same corner). The top/left
     borders are dropped so the button's outer edges blend into the panel edge — only
     the inner (right/bottom) edges are drawn, reading as a corner tab. */
  .close-btn {
    position: absolute;
    top: 0;
    left: 0;
    z-index: 30;
    width: 26px;
    height: 26px;
    display: flex;
    align-items: center;
    justify-content: center;
    border: 1px solid var(--panel-border);
    border-top-color: transparent;
    border-left-color: transparent;
    border-radius: 14px 0 9px 0;
    background: var(--surface);
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.15s;
    flex-shrink: 0;
  }
  .close-btn:hover {
    color: #ff8a8a;
    background: rgba(255, 107, 107, 0.12);
    border-right-color: rgba(255, 107, 107, 0.28);
    border-bottom-color: rgba(255, 107, 107, 0.28);
  }
  .stat-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 5px;
  }
  /* Selector blocks anchored beside the charts at the same vertical layer:
     forecast variables next to the top (Ensemble) chart, accuracy stats next to
     the bottom (Accuracy) chart. The stack mirrors the charts-track-region height
     (two 100px chart-sections + a 4px gap) so each 2×2 block lines up with its
     graph. Both blocks share .stat-card styling so they look/behave identically. */
  .selector-stack {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .selector-block {
    height: var(--chart-h);
    display: grid;
    grid-template-columns: 1fr 1fr;
    grid-template-rows: 1fr 1fr;
    gap: 6px;
  }
  .selector-block .stat-card {
    min-width: 0;
    padding: 6px 6px;
    gap: 4px;
  }
  .selector-block .stat-label {
    font-size: 11px;
  }
  .selector-block .stat-val {
    font-size: 23px;
  }
  .selector-block .stat-unit {
    font-size: 11px;
  }
  .strip-controls {
    display: flex;
    flex-direction: column;
    gap: 5px;
  }

  /* ── Model selector ── */
  /* ── Model selector (custom dropdown) ──
     Native <select> can't color only the model name and leave the date range
     neutral, so this is a button trigger + popup list. */
  .model-select {
    position: relative;
    width: 100%;
  }
  .model-select-trigger {
    position: relative;
    width: 100%;
    box-sizing: border-box;
    display: flex;
    align-items: baseline;
    gap: 6px;
    text-align: left;
    background: var(--surface);
    border: 1px solid color-mix(in srgb, var(--panel-border) 85%, transparent);
    border-radius: 6px;
    font-family: inherit;
    padding: 6px 26px 6px 8px;
    cursor: pointer;
    outline: none;
    transition: border-color 0.15s, background-color 0.15s;
  }
  .model-select-trigger:hover {
    border-color: var(--panel-border);
    background-color: color-mix(in srgb, var(--surface) 92%, #fff 8%);
  }
  .model-select-trigger.open { border-color: var(--accent); }
  .model-caret {
    position: absolute;
    right: 8px;
    top: 50%;
    transform: translateY(-50%);
    color: var(--text-secondary);
    transition: transform 0.15s;
    pointer-events: none;
  }
  .model-select-trigger.open .model-caret { transform: translateY(-50%) rotate(180deg); }
  .model-name {
    font-size: 13px;
    font-weight: 600;
    white-space: nowrap;
  }
  .model-range {
    font-size: 11px;
    font-weight: 400;
    color: var(--text-secondary);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  /* Opens upward: the panel is pinned to the screen bottom, so a downward menu
     would fall outside the panel's scroll area. */
  .model-menu {
    position: absolute;
    left: 0;
    right: 0;
    bottom: calc(100% + 4px);
    z-index: 40;
    background: var(--panel-solid);
    border: 1px solid var(--panel-border);
    border-radius: 8px;
    padding: 4px;
    display: flex;
    flex-direction: column;
    gap: 2px;
    box-shadow: 0 10px 28px rgba(0,0,0,0.5);
    max-height: 260px;
    overflow-y: auto;
  }
  .model-option {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 1px;
    width: 100%;
    text-align: left;
    background: transparent;
    border: 1px solid transparent;
    border-radius: 6px;
    padding: 6px 8px;
    cursor: pointer;
    font-family: inherit;
    transition: background-color 0.12s, border-color 0.12s;
  }
  .model-option:hover { background: var(--hover-bg); }
  .model-option.active {
    background: var(--accent-glow);
    border-color: color-mix(in srgb, var(--accent) 30%, transparent);
  }

  /* ── Period segmented control ── */
  .period-seg {
    display: flex;
    flex-wrap: wrap;
    gap: 1px;
    background: var(--surface);
    border: 1px solid var(--panel-border);
    border-radius: 6px;
    padding: 2px;
  }
  .period-btn {
    flex: 1 1 auto;
    padding: 5px 0;
    font-size: 11px;
    font-family: inherit;
    font-weight: 500;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.12s ease;
    white-space: nowrap;
    text-align: center;
    min-width: 0;
  }
  .period-btn.active {
    background: var(--accent);
    color: #0a0e14;
    font-weight: 600;
    box-shadow: 0 1px 4px rgba(110, 181, 255, 0.25);
  }
  .period-btn:hover:not(.active) {
    color: var(--text-primary);
    background: rgba(255,255,255,0.04);
  }

  /* ── Export button ── */
  .export-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    font-family: inherit;
    font-size: 12px;
    font-weight: 600;
    padding: 7px 10px;
    border-radius: 6px;
    border: 1px solid var(--panel-border);
    background: var(--surface);
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.15s;
    white-space: nowrap;
    width: 100%;
  }
  .export-btn:hover {
    color: var(--accent);
    border-color: rgba(110, 181, 255, 0.3);
    background: var(--accent-glow);
  }
  .export-icon { flex-shrink: 0; }
  .stat-card {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
    padding: 8px 6px;
    border-radius: 8px;
    background: var(--surface);
    border: 1px solid transparent;
    cursor: pointer;
    transition: all 0.15s;
    gap: 3px;
    position: relative;
    overflow: hidden;
  }
  .stat-card::after {
    content: '';
    position: absolute;
    bottom: 0;
    left: 25%;
    right: 25%;
    height: 2px;
    background: var(--stat-color);
    opacity: 0;
    transition: all 0.15s;
    border-radius: 1px;
  }
  .stat-card:hover {
    border-color: var(--panel-border);
  }
  .stat-card:hover::after {
    opacity: 0.4;
    left: 15%;
    right: 15%;
  }
  .stat-card.highlighted {
    border-color: color-mix(in srgb, var(--stat-color) 35%, transparent);
    background: color-mix(in srgb, var(--stat-color) 7%, transparent);
  }
  .stat-card.highlighted::after {
    opacity: 1;
    left: 10%;
    right: 10%;
  }
  .stat-label {
    font-size: 9px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-secondary);
    margin: 0;
    line-height: 1.2;
  }
  .stat-val {
    font-size: 18px;
    font-weight: 600;
    line-height: 1.1;
    margin: 0;
  }
  .stat-unit {
    font-size: 9px;
    color: rgba(255,255,255,0.55);
    margin: 0;
    line-height: 1.2;
  }
  .c-good { color: #4ade80; }
  .c-ok   { color: #fbbf24; }
  .c-bad  { color: #f87171; }

  /* ── Model comparison table (inside chart column) ── */
  /* Grows to absorb the chart column's leftover height so the left column matches
     the side strip exactly (no empty gap under either). The selector stack and the
     charts-track-region are the same height, so this fills the same remainder as
     .strip-controls does on the right. */
  .compare-section {
    min-width: 0;
    flex: 1 1 auto;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }
  .section-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    /* Brighter than --text-secondary: these name what each chart/table shows, so
       the neutral lead-in ("Ensemble Forecast —", "Best model by day —") should
       read clearly even though the colored variable/model word carries the accent. */
    color: rgba(206, 213, 224, 0.92);
    margin-bottom: 5px;
  }
  .compare-section-label {
    display: flex;
    align-items: baseline;
    gap: 8px;
    flex-wrap: wrap;
  }
  .winners-loading {
    font-size: 12px;
    color: var(--text-secondary);
    padding: 4px 0;
  }
  .winners-table-wrap {
    flex: 1 1 auto;
    overflow-x: auto;
    overflow-y: hidden;
    border-radius: 6px;
    border: 1px solid var(--panel-border);
  }
  /* Fill the wrap's full height (rather than centering with gaps) so the cells —
     and the current-day column highlight on them — span the entire box. */
  .compare-table {
    width: 100%;
    height: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  .compare-table th {
    text-align: center;
    vertical-align: middle;
    padding: 7px 4px;
    color: var(--text-secondary);
    font-weight: 600;
    font-size: 11px;
    letter-spacing: 0.2px;
    border-bottom: 1px solid var(--panel-border);
    min-width: 3rem;
  }
  .compare-table td {
    text-align: center;
    vertical-align: middle;
    padding: 8px 4px;
    min-width: 3rem;
  }
  .compare-table tr:last-child td { border-bottom: none; }
  .compare-nodata { color: rgba(255,255,255,0.32); }
  /* Highlight the current-day column as one continuous box spanning the header and
     body cells (top edge on the th, bottom edge on the td, sides on both, and the
     header divider dropped within the column) instead of outlining each separately. */
  .winners-table-cols th.winners-col-current,
  .winners-table-cols td.winners-col-current {
    background: rgba(110, 181, 255, 0.1);
  }
  .winners-table-cols th.winners-col-current {
    box-shadow: inset 1px 0 0 0 rgba(110, 181, 255, 0.4),
                inset -1px 0 0 0 rgba(110, 181, 255, 0.4),
                inset 0 1px 0 0 rgba(110, 181, 255, 0.4);
    border-bottom-color: transparent;
  }
  .winners-table-cols td.winners-col-current {
    box-shadow: inset 1px 0 0 0 rgba(110, 181, 255, 0.4),
                inset -1px 0 0 0 rgba(110, 181, 255, 0.4),
                inset 0 -1px 0 0 rgba(110, 181, 255, 0.4);
  }
  .winner-pill {
    display: inline-block;
    padding: 3px 7px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 600;
    border: 1px solid transparent;
    white-space: nowrap;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .winner-pill-compact {
    padding: 3px 4px;
    font-size: 12px;
    border-radius: 4px;
    width: 3.75rem;
    box-sizing: border-box;
    text-align: center;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .winner-ident {
    display: inline-block;
    transform-origin: center center;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
  }
  .winner-pill-fallback {
    background: var(--surface);
    color: var(--text-primary);
    border-color: var(--panel-border);
  }

  /* ── Loading state ── */
  .panel-loading {
    padding: 20px;
    text-align: center;
    color: var(--text-secondary);
    font-size: 13px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
  }
  .loading-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--accent);
    animation: pulse 1s ease-in-out infinite;
  }
  @keyframes pulse {
    0%, 100% { opacity: 0.3; }
    50%      { opacity: 1; }
  }

  /* ── Mobile ── */
  @media (max-width: 720px) {
    .panel {
      max-height: min(72dvh, 100dvh - 10%);
    }
    .panel-body {
      padding: 4px 12px calc(12px + env(safe-area-inset-bottom, 0px));
      gap: 10px;
    }
    .panel-content {
      flex-direction: column;
      gap: 10px;
    }
    .chart-col {
      gap: 8px;
    }
    .side-strip {
      width: auto;
      order: -1;
      /* The strip stacks first on mobile, so reserve room for the larger
         (44px) floated close button instead of letting it cover a card. */
      padding-top: 52px;
    }
    .stat-grid {
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .strip-controls {
      flex-direction: row;
      flex-wrap: wrap;
      gap: 6px;
    }
    .model-select {
      flex: 1 1 auto;
      min-width: 0;
      max-width: min(220px, 100%);
    }
    .model-select-trigger {
      min-height: 40px;
      padding-top: 6px;
      padding-bottom: 6px;
    }
    .period-seg {
      flex: 1;
      min-width: 0;
    }
    .period-btn {
      padding: 8px 0;
      min-height: 40px;
      font-size: 12px;
    }
    .export-btn {
      min-height: 40px;
      padding: 8px 12px;
      flex: 1;
    }
    .lead-chart { height: 130px; }
    /* Columns stack on mobile, so the blocks no longer need to mirror chart height;
       let the cards size to their content with a comfortable tap target. */
    .selector-block {
      height: auto;
    }
    .selector-block .stat-card {
      min-height: 58px;
    }
    .close-btn {
      min-width: 44px;
      min-height: 44px;
    }
  }
</style>
