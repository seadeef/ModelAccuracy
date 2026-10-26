/**
 * Client-side basin analyses on the exported per-basin forecast series
 * (``/ar/basin_revisions/{model}/{huc}.json``: ``forecasts[date][lead]``
 * shortest → longest lead, ``observed[date]``). Each function mirrors the production
 * ``stats_pipeline/episode_metrics.py`` primitives, evaluated on the user's
 * chosen scope.
 *
 * Every estimator takes the series ``rev`` plus an index set ``idx`` (date
 * positions, sorted) and, where an interval is reported, the cluster
 * structure built by ``scopeClusters``.
 */

import {
  BOOTSTRAP_SAMPLES,
  clusterDigest,
  drawMatrix,
  episodeClusters,
  finiteMean,
  quantile,
  ratioBootstrap,
} from './bootstrap.js';

export const SCOPES = [
  { key: 'all_days', label: 'All days', title: 'Every complete date in the model\'s primary version window' },
  { key: 'ar_all', label: 'AR days', title: 'Dates inside a catalogued atmospheric-river episode for this basin' },
  { key: 'non_ar', label: 'Non-AR', title: 'Dates outside every catalogued episode' },
  { key: 'range', label: 'Range', title: 'The storm or date range picked in the revision tab' },
];

export const TRIGGER_GRID_MM = (() => {
  const g = [];
  for (let t = 0; t <= 30; t += 0.5) g.push(t);
  for (let t = 31; t <= 80; t += 1) g.push(t);
  return g;
})();

/** Number of leads in the series. */
export function nLeads(rev) {
  return rev.lead_max - rev.lead_min + 1;
}

/** True when the observation and every lead are finite for date ``d``. */
export function isComplete(rev, d) {
  const fc = rev.forecasts[d];
  const y = rev.observed[d];
  if (y == null || !Number.isFinite(y) || !fc || fc.length !== nLeads(rev)) return false;
  for (const v of fc) if (v == null || !Number.isFinite(v)) return false;
  return true;
}

/**
 * Date positions for a scope. ``range`` is [startYmd, endYmd] inclusive;
 * ``episodes`` is the basin's catalog (for AR / non-AR membership).
 * Only complete dates are returned (the pooled-product rule).
 */
export function scopeIndices(rev, { scope = 'all_days', range = null, episodes = [], windowed = true } = {}) {
  const complete = [];
  const win = windowed ? rev.primary_valid_range : null;
  for (let d = 0; d < rev.dates.length; d++) {
    if (!isComplete(rev, d)) continue;
    if (win && ((win[0] && rev.dates[d] < win[0]) || (win[1] && rev.dates[d] > win[1]))) continue;
    complete.push(d);
  }
  if (scope === 'range') {
    if (!range || !range[0] || !range[1]) return [];
    return complete.filter((d) => rev.dates[d] >= range[0] && rev.dates[d] <= range[1]);
  }
  if (scope === 'ar_all' || scope === 'non_ar') {
    const dates = complete.map((d) => rev.dates[d]);
    const { arDays } = episodeClusters(dates, episodes);
    return complete.filter((_, i) => (scope === 'ar_all' ? arDays.has(i) : !arDays.has(i)));
  }
  return complete;
}

/**
 * Cluster structure (index arrays into ``idx``) + the common-random-number
 * draw set for the scope. ``seedExtra`` lets a paired contrast rebuild the
 * draws on the intersected population.
 */
export function scopeClusters(rev, idx, episodes, seedExtra = '') {
  const dates = idx.map((d) => rev.dates[d]);
  const { clusters, arClusterCount } = episodeClusters(dates, episodes);
  const digest = clusterDigest(dates, clusters);
  const draws = clusters.length > 1
    ? drawMatrix(clusters.length, BOOTSTRAP_SAMPLES, `${rev.huc}|${seedExtra}|${digest}`)
    : null;
  return { clusters, draws, arClusterCount, dates };
}

// ── Per-date terms (episode_metrics.per_date_components) ──

export function perDateComponents(fc, y) {
  let revision = 0;
  for (let i = 0; i < fc.length - 1; i++) revision += (fc[i] - fc[i + 1]) ** 2;
  const skill = (fc[fc.length - 1] - y) ** 2 - (fc[0] - y) ** 2;
  return { revision, skill };
}

/**
 * Run-to-run revision path for one date: ``revisions[p]`` is the signed
 * change F_L − F_(L+1) made by the run at lead ``lead_min + p`` (positive =
 * raised); ``cumulative`` the running Σ|revision| from the longest lead in.
 */
export function revisionPath(rev, d) {
  const fc = rev.forecasts[d];
  const n = nLeads(rev);
  const revisions = new Array(n - 1).fill(null);
  for (let i = 0; i < n - 1; i++) {
    const a = fc?.[i];
    const b = fc?.[i + 1];
    revisions[i] = a != null && b != null ? a - b : null;
  }
  // Cumulative from the longest lead (index n−2) down to lead_min.
  const cumulative = new Array(n - 1).fill(null);
  let run = 0;
  let any = false;
  for (let i = n - 2; i >= 0; i--) {
    if (revisions[i] == null) { cumulative[i] = any ? run : null; continue; }
    run += Math.abs(revisions[i]);
    any = true;
    cumulative[i] = run;
  }
  return { revisions, cumulative, total: any ? run : null, forecasts: fc, observed: rev.observed[d] };
}

// ── Revision efficiency R (episode_metrics.scope_revision_efficiency) ──

/**
 * Pooled R = Σ revision / Σ skill gain over ``idx`` with a cluster-bootstrap
 * interval. Returns value null when the pooled skill gain is non-positive.
 */
export function revisionEfficiency(rev, idx, clusterInfo) {
  const revByDate = [];
  const skillByDate = [];
  for (const d of idx) {
    const { revision, skill } = perDateComponents(rev.forecasts[d], rev.observed[d]);
    revByDate.push(revision);
    skillByDate.push(skill);
  }
  const out = {
    value: null, ci: null, dates: idx.length, clusters: clusterInfo?.clusters?.length ?? 0,
    skillGain: null, skillGainNonpositive: false, suppressed: false, reason: '',
    revisionMean: null,
  };
  if (!idx.length) return out;
  const r = finiteMean(revByDate);
  const s = finiteMean(skillByDate);
  out.skillGain = s.mean;
  out.revisionMean = r.mean;
  if (!(s.mean > 0)) { out.skillGainNonpositive = true; return out; }
  out.value = r.mean / s.mean;
  if (clusterInfo?.draws) {
    const b = ratioBootstrap(revByDate, skillByDate, clusterInfo.clusters, clusterInfo.draws);
    out.ci = b.ci;
    out.suppressed = b.suppressed;
    out.reason = b.reason;
  } else if ((clusterInfo?.clusters?.length ?? 0) < 2) {
    out.reason = 'single cluster';
  }
  return out;
}

/**
 * Martingale waterfall: per adjacent run
 * pair the mean squared revision, against the pooled skill gain. The stack
 * sums to R × skill gain; R = 1 is the martingale reference.
 */
export function waterfall(rev, idx) {
  const n = nLeads(rev);
  const byPair = new Array(n - 1).fill(0);
  let skill = 0;
  for (const d of idx) {
    const fc = rev.forecasts[d];
    for (let i = 0; i < n - 1; i++) byPair[i] += (fc[i] - fc[i + 1]) ** 2;
    skill += (fc[n - 1] - rev.observed[d]) ** 2 - (fc[0] - rev.observed[d]) ** 2;
  }
  const units = idx.length || 1;
  const mean = byPair.map((v) => v / units);
  const skillGain = skill / units;
  const total = mean.reduce((a, b) => a + b, 0);
  return {
    pairLeads: Array.from({ length: n - 1 }, (_, i) => rev.lead_min + i),
    meanSquaredRevisionByPair: mean,
    skillGain,
    revisionTotal: total,
    r: skillGain > 0 ? total / skillGain : null,
    units: idx.length,
  };
}

/**
 * Paired ΔR = R_a − R_b on the intersected date population with shared draws
 * (episode_metrics.paired_delta_r). ``p_direction`` is the fraction of
 * replicates with ΔR > 0.
 */
export function pairedDeltaR(revA, revB, episodes, scopeOpts) {
  const idxA = scopeIndices(revA, scopeOpts);
  const datesB = new Set(scopeIndices(revB, scopeOpts).map((d) => revB.dates[d]));
  const common = idxA.filter((d) => datesB.has(revA.dates[d]));
  const commonDates = common.map((d) => revA.dates[d]);
  const idxB = commonDates.map((ymd) => revB.dates.indexOf(ymd));
  const info = scopeClusters(revA, common, episodes, 'paired');
  const termsA = common.map((d) => perDateComponents(revA.forecasts[d], revA.observed[d]));
  const termsB = idxB.map((d) => perDateComponents(revB.forecasts[d], revB.observed[d]));
  const ratio = (terms) => {
    const r = finiteMean(terms.map((t) => t.revision));
    const s = finiteMean(terms.map((t) => t.skill));
    return s.mean > 0 ? r.mean / s.mean : null;
  };
  const rA = ratio(termsA);
  const rB = ratio(termsB);
  const out = { dates: common.length, clusters: info.clusters.length, rA, rB, delta: null, ci: null, pDirection: null, suppressed: false };
  if (rA == null || rB == null) return out;
  out.delta = rA - rB;
  if (!info.draws) return out;
  let positive = 0;
  let kept = 0;
  const reps = [];
  const sumsA = info.clusters.map((c) => c.reduce((acc, i) => {
    acc[0] += termsA[i].revision; acc[1] += termsA[i].skill; return acc;
  }, [0, 0]));
  const sumsB = info.clusters.map((c) => c.reduce((acc, i) => {
    acc[0] += termsB[i].revision; acc[1] += termsB[i].skill; return acc;
  }, [0, 0]));
  for (const draw of info.draws) {
    let ra = 0; let sa = 0; let rb = 0; let sb = 0;
    for (const k of draw) { ra += sumsA[k][0]; sa += sumsA[k][1]; rb += sumsB[k][0]; sb += sumsB[k][1]; }
    if (sa > 0 && sb > 0) {
      const dlt = ra / sa - rb / sb;
      reps.push(dlt);
      kept += 1;
      if (dlt > 0) positive += 1;
    }
  }
  const dropped = 1 - kept / info.draws.length;
  if (!kept || dropped > 0.01) { out.suppressed = true; return out; }
  const sorted = reps.sort((a, b) => a - b);
  out.ci = [quantile(sorted, 0.025), quantile(sorted, 0.975)];
  out.pDirection = positive / kept;
  return out;
}

// ── Heavy-rain trigger skill ──

function averagePrecision(precision, recall) {
  // Step-function AP over descending trigger (ascending recall).
  let ap = 0;
  let prevR = 0;
  for (let i = precision.length - 1; i >= 0; i--) {
    ap += (recall[i] - prevR) * precision[i];
    prevR = recall[i];
  }
  return ap;
}

/**
 * Event = observed ≥ q. Sweep the forecast trigger t at lead index ``li``:
 * precision/recall/F1/frequency bias per trigger, AP, and the t = q slice
 * (the published single-threshold hit rate / bias).
 */
export function triggerSkill(rev, idx, q, li) {
  const f = idx.map((d) => rev.forecasts[d][li]);
  const y = idx.map((d) => rev.observed[d]);
  const obs = y.map((v) => v >= q);
  const nObs = obs.filter(Boolean).length;
  const precision = [];
  const recall = [];
  const f1 = [];
  const bias = [];
  const nFc = [];
  const hitsArr = [];
  for (const t of TRIGGER_GRID_MM) {
    let hits = 0; let fc = 0;
    for (let i = 0; i < f.length; i++) {
      if (f[i] >= t) { fc += 1; if (obs[i]) hits += 1; }
    }
    nFc.push(fc);
    hitsArr.push(hits);
    precision.push(fc > 0 ? hits / fc : 1);
    recall.push(nObs > 0 ? hits / nObs : 0);
    f1.push((fc + nObs) > 0 ? (2 * hits) / (fc + nObs) : 0);
    bias.push(nObs > 0 ? fc / nObs : null);
  }
  const at = TRIGGER_GRID_MM.indexOf(q);
  const point = at >= 0 ? {
    trigger: q,
    hits: hitsArr[at],
    misses: nObs - hitsArr[at],
    falseAlarms: nFc[at] - hitsArr[at],
    precision: precision[at],
    recall: recall[at],
    f1: f1[at],
    frequencyBias: bias[at],
  } : null;
  // Best-F1 trigger: the decision threshold that balances misses and false alarms.
  let bestI = 0;
  for (let i = 1; i < f1.length; i++) if (f1[i] > f1[bestI]) bestI = i;
  return {
    q, lead: rev.lead_min + li, dates: idx.length, nObs, baseRate: idx.length ? nObs / idx.length : null,
    triggers: TRIGGER_GRID_MM, precision, recall, f1, bias,
    ap: nObs > 0 ? averagePrecision(precision, recall) : null,
    point,
    bestF1: { trigger: TRIGGER_GRID_MM[bestI], f1: f1[bestI], precision: precision[bestI], recall: recall[bestI] },
  };
}

// ── Storm ledger for any region ──

function pearson(xs, ys) {
  const n = xs.length;
  let mx = 0; let my = 0;
  for (let i = 0; i < n; i++) { mx += xs[i]; my += ys[i]; }
  mx /= n; my /= n;
  let sxy = 0; let sxx = 0; let syy = 0;
  for (let i = 0; i < n; i++) { const a = xs[i] - mx; const b = ys[i] - my; sxy += a * b; sxx += a * a; syy += b * b; }
  return sxx > 0 && syy > 0 ? sxy / Math.sqrt(sxx * syy) : null;
}

/**
 * Per-model ledger over a storm's days for a region served from the per-cell
 * cube (the curated ledger exists for the exported basins only): rain
 * observed (region-mean mm summed over the days), the wettest day, the mean
 * total |run-to-run revision| per day, pooled R, lead-1 and lead-``lead``
 * absolute error and the lead-``lead`` correlation, plus RMS revision by run
 * pair for the steadiness curve. ``positions`` = date positions in the storm;
 * revision and error terms use the complete dates only.
 */
export function stormLedger(rev, positions, { lead = 3 } = {}) {
  const n = nLeads(rev);
  const li = lead - rev.lead_min;
  let rain = 0;
  let wettest = null;
  let obsDays = 0;
  const complete = [];
  for (const d of positions) {
    const y = rev.observed[d];
    if (y != null && Number.isFinite(y)) {
      rain += y; obsDays++;
      if (wettest === null || y > rev.observed[wettest]) wettest = d;
    }
    if (isComplete(rev, d)) complete.push(d);
  }
  const out = {
    days: positions.length, obsDays, rain, wettest, complete: complete.length,
    revision: null, r: null, mae1: null, maeL: null, corrL: null, rmsByPair: null, pairLeads: null, lead,
  };
  if (!complete.length) return out;
  const wf = waterfall(rev, complete);
  out.rmsByPair = wf.meanSquaredRevisionByPair.map(Math.sqrt);
  out.pairLeads = wf.pairLeads;
  out.r = revisionEfficiency(rev, complete, null).value;
  let revTot = 0; let e1 = 0; let eL = 0;
  const xs = []; const ys = [];
  for (const d of complete) {
    const fc = rev.forecasts[d];
    const y = rev.observed[d];
    for (let i = 0; i < n - 1; i++) revTot += Math.abs(fc[i] - fc[i + 1]);
    e1 += Math.abs(fc[0] - y);
    if (li >= 0 && li < n) { eL += Math.abs(fc[li] - y); xs.push(fc[li]); ys.push(y); }
  }
  out.revision = revTot / complete.length;
  out.mae1 = e1 / complete.length;
  if (xs.length) out.maeL = eL / xs.length;
  if (xs.length >= 3) out.corrL = pearson(xs, ys);
  return out;
}

// ── Day ranking helpers ──

/** Dates (positions) inside a range with an observation, newest last. */
export function datesInRange(rev, range) {
  const out = [];
  for (let d = 0; d < rev.dates.length; d++) {
    const ymd = rev.dates[d];
    if (range && (ymd < range[0] || ymd > range[1])) continue;
    out.push(d);
  }
  return out;
}

/** Position of the date with the largest observation in ``positions``. */
export function wettestDay(rev, positions) {
  let best = null;
  for (const d of positions) {
    const y = rev.observed[d];
    if (y == null) continue;
    if (best === null || y > rev.observed[best]) best = d;
  }
  return best;
}

function dayNumber(ymd) {
  return Date.UTC(Number(ymd.slice(0, 4)), Number(ymd.slice(4, 6)) - 1, Number(ymd.slice(6, 8))) / 86400000;
}

/**
 * The ``count`` wettest non-overlapping ``span``-day spells on record, wettest
 * first: ``{ start, end, total, peak }`` as positions (``total`` in mm summed
 * over the spell, ``peak`` its wettest day). Windows must be consecutive
 * calendar days with an observation on each; this is pure PRISM, so it ranks
 * rain, not atmospheric-river attribution — the storm catalog does that.
 */
export function wettestSpells(rev, { span = 5, count = 6 } = {}) {
  const n = rev.dates.length;
  if (n < span) return [];
  const sums = [];
  for (let s = 0; s + span <= n; s++) {
    if (dayNumber(rev.dates[s + span - 1]) - dayNumber(rev.dates[s]) !== span - 1) continue;
    let tot = 0;
    let ok = true;
    for (let d = s; d < s + span; d++) {
      const y = rev.observed[d];
      if (y == null) { ok = false; break; }
      tot += y;
    }
    if (ok) sums.push({ s, tot });
  }
  sums.sort((a, b) => b.tot - a.tot);
  const taken = new Uint8Array(n);
  const out = [];
  for (const { s, tot } of sums) {
    if (out.length >= count) break;
    let clash = false;
    for (let d = s; d < s + span; d++) if (taken[d]) { clash = true; break; }
    if (clash) continue;
    for (let d = s; d < s + span; d++) taken[d] = 1;
    const positions = Array.from({ length: span }, (_, i) => s + i);
    out.push({ start: s, end: s + span - 1, total: tot, peak: wettestDay(rev, positions) });
  }
  return out;
}
