<script>
  import { appConfig, ui, statLabel, accuracyStatKeys } from '../state.svelte.js';
  import {
    statisticTooltip,
    PERIOD_HELP,
    SEASON_PERIOD_HELP,
  } from '../helpText.js';
  import { fetchStatsAllLeads, fetchLeadWinnersForRegion } from '../api.js';
  import { downloadPanelStatsCsv } from '../exportPanelStatsCsv.js';
  import { getModelLeadBounds } from '../tile.js';
  import { modelPalette } from '../modelPalette.js';
  import { glyphPanelClose } from '../appIcons.js';

  let { onleadchange, onstatchange, onperiodchange, onmodelchange } = $props();

  const STAT_COLORS = {
    nrmse: '#6eb5ff',
    bias: '#e0a4ff',
    sacc: '#5ce0d6',
    nmad: '#f0a07a',
  };

  let canvasEl;
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

  /** Coalesce chart scrub → map updates to one per frame (reduces overlapping MapLibre `updateImage` / AbortError). */
  let chartScrubRafId = 0;
  let pendingScrubLead = null;

  /** Plot area insets; `PLOT_TOP` reserves space above the grid for the hover readout. */
  const PAD = { left: 48, right: 18, bottom: 26 };
  const PLOT_TOP = 34;
  const HOVER_LABEL_Y = 16;

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
    return null;
  }

  function regionLabel() {
    const r = ui.selectedRegion;
    const c = regionCenter();
    if (!r || !c) return '';
    const coord = `${c.lat.toFixed(2)}\u00b0N, ${Math.abs(c.lon).toFixed(2)}\u00b0W`;
    if (r.type === 'polygon') return `Polygon centered at: ${coord}`;
    if (r.type === 'rectangle') return `Rectangle centered at: ${coord}`;
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

  const chartLead = $derived(hoverLead ?? ui.leadFractional);

  const winnerLeadKey = $derived.by(() => {
    if (ui.activeWindow && /^\d+$/.test(String(ui.activeWindow))) {
      return String(ui.activeWindow);
    }
    if (ui.activeWindow && /^\d+_\d+$/.test(String(ui.activeWindow))) {
      return String(ui.activeWindow).split('_')[0];
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

    const xForLead = lead => PAD.left + ((lead - leadMin) / (leadMax - leadMin)) * (w - PAD.left - PAD.right);
    const yForValue = val => PLOT_TOP + (1 - (val - yMin) / (yMax - yMin)) * (plotBottom - PLOT_TOP);

    ctx.clearRect(0, 0, w, h);

    // Grid
    ctx.strokeStyle = 'rgba(255,255,255,0.035)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const y = yForValue(yMin + (yMax - yMin) * i / 4);
      ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(w - PAD.right, y); ctx.stroke();
    }
    ctx.fillStyle = '#444';
    ctx.font = '10px "DM Sans", system-ui';
    ctx.textAlign = 'right';
    for (let i = 0; i <= 4; i++) {
      const v = yMin + (yMax - yMin) * i / 4;
      ctx.fillText(yTickFmt(v), PAD.left - 6, yForValue(v) + 3);
    }
    ctx.textAlign = 'center';
    ctx.font = '10px "DM Sans", system-ui';
    for (let d = leadMin; d <= leadMax; d++) ctx.fillText(d, xForLead(d), h - 6);

    ctx.save();
    ctx.beginPath();
    ctx.rect(PAD.left, PLOT_TOP, w - PAD.left - PAD.right, plotBottom - PLOT_TOP);
    ctx.clip();

    const stat = activeStat;
    const color = STAT_COLORS[stat] || '#888';
    const data = seriesFor(stat);
    const points = [];
    for (let i = 0; i < data.length; i++) if (data[i] !== null) points.push({ x: xForLead(leadMin + i), y: yForValue(data[i]) });
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
          const c = STAT_COLORS[st] || '#888';
          const y = yForValue(val);
          ctx.beginPath(); ctx.arc(hx, y, 7, 0, Math.PI * 2); ctx.fillStyle = c + '20'; ctx.fill();
          ctx.beginPath(); ctx.arc(hx, y, 4, 0, Math.PI * 2); ctx.fillStyle = c; ctx.fill();
          const units = leadData[0]?.stats?.[st]?.units || '';
          const label = `${val.toFixed(2)} ${units}`;
          ctx.save();
          ctx.font = '600 11px "DM Sans", system-ui';
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          const tw = ctx.measureText(label).width;
          const padX = 9;
          const padY = 10;
          const halfW = tw / 2 + padX;
          let lx = hx;
          const xMin = PAD.left + halfW + 2;
          const xMax = w - PAD.right - halfW - 2;
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
    if (!canvasEl) return ui.leadFractional;
    const rect = canvasEl.parentElement.getBoundingClientRect();
    const mx = clientX - rect.left;
    let lead = leadMin + ((mx - PAD.left) / (rect.width - PAD.left - PAD.right)) * (leadMax - leadMin);
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
    drawChart();
  }

  function handleChartMouseMove(e) {
    if (!canvasEl || leadData.length === 0 || leadScrubLocked) return;
    const lead = leadFromClientX(e.clientX);
    if (lead === hoverLead) return;
    pendingScrubLead = lead;
    if (!chartScrubRafId) {
      chartScrubRafId = requestAnimationFrame(flushChartScrubLead);
    }
  }

  function handleChartMouseLeave() {
    if (chartScrubRafId) {
      cancelAnimationFrame(chartScrubRafId);
      chartScrubRafId = 0;
    }
    pendingScrubLead = null;
    if (leadScrubLocked) return;
    hoverLead = null;
    drawChart();
  }

  function handleLeadChartPointerDown(e) {
    if (e.button !== 0 || leadData.length === 0) return;
    scrubPointerDown = { x: e.clientX, y: e.clientY, t: Date.now() };
    try {
      e.currentTarget.setPointerCapture(e.pointerId);
    } catch {
      /* ignore */
    }
  }

  function handleLeadChartPointerUp(e) {
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
      drawChart();
      return;
    }
    const lead = leadFromClientX(e.clientX);
    leadScrubLocked = true;
    hoverLead = null;
    ui.leadFractional = lead;
    onleadchange?.(lead);
    drawChart();
  }

  function handleLeadChartPointerCancel(e) {
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
   * Good / ok / bad tiers for panel values (same units as API: % for SACC/NRMSE/NMAD, mm for bias).
   * Cutoffs rounded from a one-off analysis of static_export/static/ranges (legend vmin/vmax per
   * model/stat/period) so tiers sit near the scale of real outputs instead of mistaken 0–1 bounds.
   */
  function colorClass(statName, value) {
    if (value == null) return '';
    if (statName === 'sacc') return value > 42 ? 'c-good' : value > 20 ? 'c-ok' : 'c-bad';
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
    const _ = [leadData, activeStat, winnerLeadKey, leadScrubLocked, chartLead];
    requestAnimationFrame(() => drawChart());
  });
</script>

{#if ui.selectedRegion}
  <div class="panel" class:entering={panelEntering}>
    <div class="panel-handle"></div>

    {#if loading}
      <div class="panel-loading">
        <div class="loading-dot"></div>
        Loading stats...
      </div>
    {:else if leadData.length > 0}
      <div class="panel-body">
        <div class="panel-toolbar">
          <div class="toolbar-title">
            <span class="toolbar-heading" title="The chart shows only the highlighted metric. Click a stat card to switch.">Accuracy vs. lead time</span>
            <span class="toolbar-location">{regionLabel()}</span>
          </div>
          <button class="close-btn" type="button" title="Close panel" aria-label="Close panel" onclick={closePanel}>
            <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2">{@html glyphPanelClose}</svg>
          </button>
        </div>

        <div class="panel-content">
          <div class="chart-col">
            <div class="chart-section">
              <!-- svelte-ignore a11y_no_static_element_interactions -->
              <div
                class="lead-chart"
                class:lead-locked={leadScrubLocked}
                onmousemove={handleChartMouseMove}
                onmouseleave={handleChartMouseLeave}
                onpointerdown={handleLeadChartPointerDown}
                onpointerup={handleLeadChartPointerUp}
                onpointercancel={handleLeadChartPointerCancel}
              >
                <canvas bind:this={canvasEl}></canvas>
              </div>
            </div>

            <div class="compare-section">
          <div
            class="section-label compare-section-label"
            title={`For each lead day, which model wins on ${statLabel(ui.statistic)} for this drawn region and accumulation window (use the period selector next to the chart). The map and this table both use the statistic you pick in the stat cards.`}
          >
            <span>Best model by day — {statLabel(ui.statistic)}</span>
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
                      <th
                        class="winners-day-head"
                        class:winners-col-current={row.day === winnerLeadKey}
                      >{row.day}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    {#each winnerTableRows as row}
                      {@const pc = row.winner ? palette[row.winner] : null}
                      <td class:winners-col-current={row.day === winnerLeadKey}>
                        {#if row.winner}
                          {#if pc}
                            <span
                              class="winner-pill winner-pill-compact"
                              style:background={pc.bg}
                              style:color={pc.fg}
                              style:border-color={pc.border}
                            >{modelLabel(row.winner)}</span>
                          {:else}
                            <span class="winner-pill winner-pill-compact winner-pill-fallback">{modelLabel(row.winner)}</span>
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
            <div class="stat-grid">
              {#each chartStats as stat}
                {@const s = statAtHoverLead(stat)}
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_static_element_interactions -->
                <div
                  class="stat-card"
                  class:highlighted={activeStat === stat}
                  style="--stat-color: {STAT_COLORS[stat] || '#888'}"
                  title={statisticTooltip(stat)}
                  onclick={() => { activeStat = stat; ui.statistic = stat; ui.activeWindow = null; onstatchange?.({ target: { value: stat } }); }}
                >
                  <div class="stat-label">{statLabel(stat)}</div>
                  <div class="stat-val {s ? colorClass(stat, s.value) : ''}">{s ? s.value.toFixed(2) : '\u2014'}</div>
                  <div class="stat-unit">{s?.units || ''}</div>
                </div>
              {/each}
            </div>
            <div class="strip-controls">
              <select class="panel-model-select" value={ui.model} onchange={onmodelchange} aria-label="Model">
                {#each appConfig.models as m}
                  <option value={m.key}>{m.label}</option>
                {/each}
              </select>
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
    max-height: min(52vh, 100dvh - 20%);
    overflow-y: auto;
    z-index: 20;
    -webkit-overflow-scrolling: touch;
  }
  .panel::before {
    content: '';
    position: absolute;
    top: 0;
    left: 32px;
    right: 32px;
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--accent) 30%, var(--accent) 70%, transparent);
    opacity: 0.25;
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
  .panel-handle {
    width: 28px;
    height: 3px;
    background: rgba(255,255,255,0.1);
    border-radius: 2px;
    margin: 7px auto 0;
  }

  /* ── Body layout ── */
  .panel-body {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 4px 16px 12px;
  }

  /* ── Toolbar (minimal: title + close) ── */
  .panel-toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    min-height: 24px;
    padding: 0 0 2px;
  }
  .toolbar-title {
    display: flex;
    align-items: baseline;
    gap: 8px;
    min-width: 0;
    flex-shrink: 1;
    overflow: hidden;
  }
  .toolbar-heading {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-secondary);
    white-space: nowrap;
    flex-shrink: 0;
  }
  .toolbar-location {
    font-size: 12px;
    font-weight: 500;
    color: var(--text-primary);
    opacity: 0.55;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    min-width: 0;
  }
  .close-btn {
    width: 26px;
    height: 26px;
    display: flex;
    align-items: center;
    justify-content: center;
    border: 1px solid var(--panel-border);
    border-radius: 6px;
    background: var(--surface);
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.15s;
    flex-shrink: 0;
  }
  .close-btn:hover {
    color: #ff8a8a;
    background: rgba(255, 107, 107, 0.1);
    border-color: rgba(255, 107, 107, 0.28);
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
  .chart-section {
    min-width: 0;
  }
  .lead-chart {
    width: 100%;
    height: 150px;
    position: relative;
    cursor: crosshair;
    touch-action: none;
    border-radius: 8px;
  }
  .lead-chart.lead-locked {
    box-shadow: inset 0 0 0 1px rgba(110, 181, 255, 0.35);
    cursor: pointer;
  }
  .lead-chart canvas { width: 100%; height: 100%; }

  /* ── Side strip: stats + controls ── */
  .side-strip {
    flex-shrink: 0;
    width: 224px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .stat-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 5px;
  }
  .strip-controls {
    display: flex;
    flex-direction: column;
    gap: 5px;
  }

  /* ── Model selector ── */
  .panel-model-select {
    width: 100%;
    background: var(--surface);
    color: var(--text-primary);
    border: 1px solid var(--panel-border);
    border-radius: 6px;
    font-size: 13px;
    font-family: inherit;
    font-weight: 500;
    padding: 7px 26px 7px 10px;
    cursor: pointer;
    outline: none;
    -webkit-appearance: none;
    appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 12 12'%3E%3Cpath fill='%237a818c' d='M3 4.5L6 7.5L9 4.5'/%3E%3C/svg%3E");
    background-repeat: no-repeat;
    background-position: right 8px center;
    transition: border-color 0.15s;
  }
  .panel-model-select:focus { border-color: var(--accent); }
  .panel-model-select option {
    background: var(--panel-solid);
    color: var(--text-primary);
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
    color: rgba(255,255,255,0.22);
    margin: 0;
    line-height: 1.2;
  }
  .c-good { color: #4ade80; }
  .c-ok   { color: #fbbf24; }
  .c-bad  { color: #f87171; }

  /* ── Model comparison table (inside chart column) ── */
  .compare-section {
    min-width: 0;
  }
  .section-label {
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-secondary);
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
    overflow-x: auto;
    border-radius: 6px;
    border: 1px solid var(--panel-border);
  }
  .compare-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }
  .compare-table th {
    text-align: center;
    vertical-align: middle;
    padding: 5px 4px;
    color: var(--text-secondary);
    font-weight: 600;
    font-size: 10px;
    letter-spacing: 0.2px;
    border-bottom: 1px solid var(--panel-border);
    min-width: 3rem;
  }
  .compare-table td {
    text-align: center;
    vertical-align: middle;
    padding: 5px 4px;
    min-width: 3rem;
  }
  .compare-table tr:last-child td { border-bottom: none; }
  .compare-nodata { color: rgba(255,255,255,0.12); }
  .winners-table-cols th.winners-col-current,
  .winners-table-cols td.winners-col-current {
    background: rgba(110, 181, 255, 0.08);
    box-shadow: inset 0 0 0 1px rgba(110, 181, 255, 0.15);
  }
  .winner-pill {
    display: inline-block;
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    border: 1px solid transparent;
    white-space: nowrap;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .winner-pill-compact {
    padding: 2px 6px;
    font-size: 11px;
    border-radius: 4px;
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
      max-height: min(62dvh, 100dvh - 15%);
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
    .panel-model-select {
      width: auto;
      flex: 1;
      min-width: 120px;
      min-height: 40px;
      font-size: 14px;
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
    .lead-chart { height: 140px; }
    .close-btn {
      min-width: 44px;
      min-height: 44px;
    }
  }
</style>
