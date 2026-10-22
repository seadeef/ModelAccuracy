/** Chart.js registration + dashboard-wide defaults and shared plugins. */
import {
  Chart, LineController, BarController, ScatterController, LineElement, PointElement, BarElement,
  CategoryScale, LinearScale, Filler, Tooltip, Legend,
} from 'chart.js';

Chart.register(LineController, BarController, ScatterController, LineElement, PointElement, BarElement,
  CategoryScale, LinearScale, Filler, Tooltip, Legend);

Chart.defaults.font.family = "'DM Sans', system-ui, sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.color = '#a3aab6';
Chart.defaults.animation = false;
Chart.defaults.plugins.legend.display = false;
Chart.defaults.plugins.tooltip.backgroundColor = '#0b111b';
Chart.defaults.plugins.tooltip.padding = 8;
Chart.defaults.plugins.tooltip.titleFont = { size: 11, weight: '600' };
Chart.defaults.plugins.tooltip.bodyFont = { size: 11 };
Chart.defaults.elements.point.radius = 0;
Chart.defaults.elements.point.hitRadius = 8;
Chart.defaults.elements.line.tension = 0.18;

export { Chart };

export const GRID = '#ffffff0b';
export const CURSOR = '#a4b9d4';

/** Base options for a lead-axis line chart. */
export function leadAxisOptions({ yTitle = '', yMin, yMax, xTitle = '', ticksLimit = 4, tooltipLabel } = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    layout: { padding: { top: 6, right: 8 } },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: tooltipLabel ? { label: tooltipLabel } : {},
      },
    },
    scales: {
      x: {
        grid: { display: false },
        border: { display: false },
        ticks: { autoSkip: false, maxRotation: 0, font: { size: 10.5 } },
        title: { display: !!xTitle, text: xTitle, color: '#919daf', font: { size: 10 }, padding: 0 },
      },
      y: {
        grid: { color: GRID },
        border: { display: false },
        ticks: { maxTicksLimit: ticksLimit, font: { size: 10.5 } },
        title: { display: !!yTitle, text: yTitle, color: '#919daf', font: { size: 10.5 } },
        ...(yMin != null ? { min: yMin } : {}),
        ...(yMax != null ? { max: yMax } : {}),
      },
    },
  };
}

/**
 * Click handler that maps a click anywhere in the plot to the nearest
 * category index (so a click between points still lands on a lead).
 */
/** 1 → "1st", 92 → "92nd", 100 → "100th". */
export function ordinal(n) {
  const v = Math.round(Number(n));
  if (!Number.isFinite(v)) return '—';
  const r100 = v % 100;
  const suf = r100 >= 11 && r100 <= 13 ? 'th' : ({ 1: 'st', 2: 'nd', 3: 'rd' }[v % 10] || 'th');
  return `${v}${suf}`;
}

export function onClickIndex(fn) {
  return (evt, _elements, chart) => {
    const x = chart.scales?.x;
    if (!x || evt?.x == null) return;
    const v = x.getValueForPixel(evt.x);
    if (!Number.isFinite(v)) return;
    const idx = Math.max(0, Math.min(chart.data.labels.length - 1, Math.round(v)));
    fn(idx, chart.data.labels[idx]);
  };
}

/** Dashed vertical cursor at a category index (``getIndex()`` evaluated per draw). */
export function cursorPlugin(getIndex, color = CURSOR) {
  return {
    id: 'leadCursor',
    afterDatasetsDraw(chart) {
      const idx = getIndex();
      if (idx == null || idx < 0) return;
      const { ctx, scales: { x, y } } = chart;
      if (!x || !y) return;
      const xx = x.getPixelForValue(idx);
      if (!Number.isFinite(xx)) return;
      ctx.save();
      ctx.strokeStyle = color;
      ctx.setLineDash([4, 4]);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(xx, y.top);
      ctx.lineTo(xx, y.bottom);
      ctx.stroke();
      ctx.restore();
    },
  };
}

/** Solid horizontal reference line at y = value. */
export function hlinePlugin(getValue, color = '#ffffff40', dash = []) {
  return {
    id: `hline-${Math.random().toString(36).slice(2, 7)}`,
    beforeDatasetsDraw(chart) {
      const v = getValue();
      if (v == null || !Number.isFinite(v)) return;
      const { ctx, chartArea: a, scales: { y } } = chart;
      if (!y) return;
      const yy = y.getPixelForValue(v);
      if (yy < a.top || yy > a.bottom) return;
      ctx.save();
      ctx.strokeStyle = color;
      ctx.setLineDash(dash);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(a.left, yy);
      ctx.lineTo(a.right, yy);
      ctx.stroke();
      ctx.restore();
    },
  };
}

/**
 * Apply an alpha to a palette color. The model palette uses the modern
 * space-separated ``hsl(H S% L%)`` form, which canvas accepts but which
 * cannot take a hex alpha suffix; rewrite it as ``hsla``.
 */
export function alpha(color, a) {
  if (!color) return color;
  const m = /^hsl\(\s*([\d.]+)\s+([\d.]+)%\s+([\d.]+)%\s*\)$/.exec(color);
  if (m) return `hsla(${m[1]}, ${m[2]}%, ${m[3]}%, ${a})`;
  const h = /^#([0-9a-f]{6})$/i.exec(color);
  if (h) return `rgba(${parseInt(h[1].slice(0, 2), 16)}, ${parseInt(h[1].slice(2, 4), 16)}, ${parseInt(h[1].slice(4, 6), 16)}, ${a})`;
  return color;
}

export function fmt(v, digits = 2) {
  if (v == null || !Number.isFinite(v)) return '—';
  return Number(v).toFixed(digits);
}

export function fmtCi(ci, digits = 2) {
  if (!ci || ci[0] == null || ci[1] == null) return '';
  return `${fmt(ci[0], digits)}–${fmt(ci[1], digits)}`;
}

export function fmtDate(ymdOrIso, opts = { month: 'short', day: 'numeric' }) {
  if (!ymdOrIso) return '';
  const s = String(ymdOrIso);
  const iso = s.includes('-') ? s : `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  return new Date(`${iso}T00:00:00Z`).toLocaleDateString('en-US', { ...opts, timeZone: 'UTC' });
}
