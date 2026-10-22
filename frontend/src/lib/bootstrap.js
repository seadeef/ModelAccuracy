/**
 * Resampling machinery shared by every client-side basin analysis — a port
 * of the production design in ``stats_pipeline/episode_metrics.py`` and
 * ``stats_pipeline/episodes.py``:
 *
 * - Resampling units are CLUSTERS of dates, never single dates. AR episodes
 *   of the basin less than 72 h apart merge into one cluster
 *   (``assign_clusters``); the remaining dates form pseudo-clusters split at
 *   >= 3-day gaps and at 10-day calendar blocks aligned to 2025-01-01
 *   (``group_pseudo_clusters``).
 * - B = 2000 percentile intervals at 95%.
 * - Draws are seeded from the scope/cluster digest only — never from a model
 *   name — so paired contrasts share one draw set (common random numbers).
 * - Ratio replicates with a non-positive denominator are dropped; the
 *   interval is suppressed when more than 1% drop.
 */

export const BOOTSTRAP_SAMPLES = 2000;
export const CONFIDENCE = 0.95;
export const MAX_DROPPED_FRACTION = 0.01;
export const CLUSTER_MERGE_HOURS = 72;
export const NON_AR_BREAK_DAYS = CLUSTER_MERGE_HOURS / 24;
export const PSEUDO_CLUSTER_BLOCK_DAYS = 10;
const PSEUDO_CLUSTER_EPOCH_MS = Date.UTC(2025, 0, 1);
const DAY_MS = 86_400_000;

/** Deterministic 32-bit PRNG (mulberry32) so intervals are stable per slice. */
export function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function hashSeed(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

export function quantile(sorted, q) {
  if (!sorted.length) return null;
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

export function percentileInterval(sample, confidence = CONFIDENCE) {
  const sorted = Array.from(sample).filter(Number.isFinite).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const alpha = (1 - confidence) / 2;
  return [quantile(sorted, alpha), quantile(sorted, 1 - alpha)];
}

/** (B × K) cluster-index draws with replacement, seeded by ``seedKey``. */
export function drawMatrix(nClusters, samples, seedKey) {
  const rng = mulberry32(hashSeed(seedKey));
  const draws = new Array(samples);
  for (let b = 0; b < samples; b++) {
    const row = new Int32Array(nClusters);
    for (let k = 0; k < nClusters; k++) row[k] = Math.floor(rng() * nClusters);
    draws[b] = row;
  }
  return draws;
}

// ── Date helpers (YYYYMMDD strings, UTC) ──

export function ymdToMs(ymd) {
  const s = String(ymd);
  return Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8));
}

export function ymdToIso(ymd) {
  const s = String(ymd);
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

export function isoToYmd(iso) {
  return iso ? String(iso).replaceAll('-', '') : '';
}

function pseudoBlock(ms) {
  return Math.floor((ms - PSEUDO_CLUSTER_EPOCH_MS) / DAY_MS / PSEUDO_CLUSTER_BLOCK_DAYS);
}

/**
 * Partition a sorted list of dates (YYYYMMDD strings) into bootstrap
 * clusters: AR clusters from the basin's episode catalog (72 h merge rule,
 * projected onto the date set) followed by non-AR pseudo-clusters. Returns
 * index arrays into ``dates`` plus the AR-day membership for scope filters.
 *
 * ``episodes`` are catalog entries ``{start_date, end_date}`` (ISO).
 */
export function episodeClusters(dates, episodes) {
  const ordered = [...(episodes || [])].sort((a, b) =>
    (a.start_date + a.end_date).localeCompare(b.start_date + b.end_date));
  // Merge episodes into clusters by the 72 h rule; a cluster's membership is
  // the union of its episodes' ACTIVE days (gap days between merged episodes
  // stay non-AR, as in stats_pipeline.episodes.build_scope_clusters).
  const clusterDays = [];
  let clusterEnd = null;
  for (const ep of ordered) {
    const startMs = ymdToMs(isoToYmd(ep.start_date));
    const endMs = ymdToMs(isoToYmd(ep.end_date));
    const days = new Set();
    for (let ms = startMs; ms <= endMs; ms += DAY_MS) days.add(ms);
    if (clusterEnd !== null && ((startMs - clusterEnd) / DAY_MS) * 24 < CLUSTER_MERGE_HOURS) {
      const last = clusterDays[clusterDays.length - 1];
      for (const ms of days) last.add(ms);
      clusterEnd = Math.max(clusterEnd, endMs);
    } else {
      clusterDays.push(days);
      clusterEnd = endMs;
    }
  }
  const msOf = dates.map(ymdToMs);
  const arClusters = [];
  const arDays = new Set();
  for (const days of clusterDays) {
    const hit = [];
    for (let i = 0; i < dates.length; i++) if (days.has(msOf[i])) hit.push(i);
    if (hit.length) {
      arClusters.push(hit);
      for (const i of hit) arDays.add(i);
    }
  }
  const pseudo = [];
  for (let i = 0; i < dates.length; i++) {
    if (arDays.has(i)) continue;
    const last = pseudo.length ? pseudo[pseudo.length - 1] : null;
    if (last) {
      const prevMs = msOf[last[last.length - 1]];
      if ((msOf[i] - prevMs) / DAY_MS < NON_AR_BREAK_DAYS
          && pseudoBlock(msOf[i]) === pseudoBlock(prevMs)) {
        last.push(i);
        continue;
      }
    }
    pseudo.push([i]);
  }
  return { clusters: [...arClusters, ...pseudo], arDays, arClusterCount: arClusters.length };
}

/** Deterministic digest of a cluster structure (first/last date per cluster). */
export function clusterDigest(dates, clusters) {
  return clusters.map((c) => `${dates[c[0]]}-${dates[c[c.length - 1]]}`).join(',');
}

/** Per-cluster (sum, finite count) of one value array; null/NaN are missing. */
export function clusterSums(values, clusters) {
  return clusters.map((idx) => {
    let sum = 0;
    let cnt = 0;
    for (const i of idx) {
      const v = values[i];
      if (v != null && Number.isFinite(v)) { sum += v; cnt += 1; }
    }
    return { sum, cnt };
  });
}

/**
 * Bootstrap interval for a ratio of date-weighted means Σnum / Σden.
 * Replicates with ``den <= 0`` are dropped; suppressed above 1% dropped.
 */
export function ratioBootstrap(numByDate, denByDate, clusters, draws) {
  if (!draws || clusters.length < 2) return { ci: null, dropped: 0, suppressed: false, reason: 'single cluster' };
  const nums = clusterSums(numByDate, clusters);
  const dens = clusterSums(denByDate, clusters);
  const ratios = [];
  for (const draw of draws) {
    let n = 0;
    let d = 0;
    for (const k of draw) { n += nums[k].sum; d += dens[k].sum; }
    if (d > 0) ratios.push(n / d);
  }
  const dropped = 1 - ratios.length / draws.length;
  if (!ratios.length || dropped > MAX_DROPPED_FRACTION) {
    return { ci: null, dropped, suppressed: true, reason: 'interval suppressed (unstable bootstrap)' };
  }
  return { ci: percentileInterval(ratios), dropped, suppressed: false, reason: '' };
}

/** Bootstrap interval for a date-weighted mean (NaN days missing). */
export function meanBootstrap(values, clusters, draws) {
  if (!draws || clusters.length < 2) return null;
  const sums = clusterSums(values, clusters);
  const means = [];
  for (const draw of draws) {
    let s = 0;
    let c = 0;
    for (const k of draw) { s += sums[k].sum; c += sums[k].cnt; }
    if (c > 0) means.push(s / c);
  }
  return percentileInterval(means);
}

/**
 * Replicate statistic over arbitrary per-cluster accumulators: ``reduce``
 * receives the list of drawn cluster ids and returns a number (NaN = drop).
 */
export function customBootstrap(clusters, draws, reduce) {
  if (!draws || clusters.length < 2) return { ci: null, dropped: 0, suppressed: false };
  const out = [];
  for (const draw of draws) {
    const v = reduce(draw);
    if (Number.isFinite(v)) out.push(v);
  }
  const dropped = 1 - out.length / draws.length;
  if (!out.length || dropped > MAX_DROPPED_FRACTION) {
    return { ci: null, dropped, suppressed: true };
  }
  return { ci: percentileInterval(out), dropped, suppressed: false };
}

export function finiteMean(values) {
  let s = 0;
  let c = 0;
  for (const v of values) if (v != null && Number.isFinite(v)) { s += v; c += 1; }
  return c > 0 ? { mean: s / c, count: c } : { mean: NaN, count: 0 };
}
