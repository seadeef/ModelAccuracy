/**
 * Derived lock-in quantities from the consistency-statistics curves.
 *
 * Definitions (see the README):
 * - Stabilization lead L_stab: the largest lead L at which jumpiness J drops
 *   below `fraction` × its own maximum over the curve AND stays below it for
 *   all smaller leads — the forecast has "locked in".
 * - Accuracy lead L_acc: the analogous lead on the convergence e_L curve.
 * - Lock-in gap = L_stab − L_acc. Positive → the forecast stopped changing
 *   before it became accurate (false confidence); ≈0 → stability was earned.
 *
 * Both curves here are period-pooled for the selected region (not per valid
 * date), so the gap reads as a climatological property of the slice.
 */

/**
 * Largest lead whose whole defined prefix sits below `fraction` × curve max.
 *
 * `series` is indexed from `leadMin` (series[i] = value at lead leadMin + i,
 * null where undefined). Undefined leads are skipped — they neither satisfy
 * nor violate the "stays below" condition. Returns null when the curve never
 * locks in (the first defined lead is already at/above the cut) or when there
 * is no defined data at all.
 */
export function lockInLead(series, leadMin, fraction) {
  let max = -Infinity;
  for (const v of series) {
    if (v !== null && v !== undefined && Number.isFinite(v) && v > max) max = v;
  }
  if (!Number.isFinite(max) || max <= 0) return null;
  const cut = fraction * max;
  let lead = null;
  for (let i = 0; i < series.length; i++) {
    const v = series[i];
    if (v === null || v === undefined || !Number.isFinite(v)) continue;
    if (v >= cut) break; // prefix property broken from here on
    lead = leadMin + i;
  }
  return lead;
}

/**
 * L_stab / L_acc / gap from the jumpiness and convergence series of one model.
 * Either lead is null when its curve never drops below the cut; the gap is
 * null unless both are defined.
 */
export function computeLockIn({ jSeries, eSeries, leadMin, fraction = 0.5 }) {
  const lStab = jSeries ? lockInLead(jSeries, leadMin, fraction) : null;
  const lAcc = eSeries ? lockInLead(eSeries, leadMin, fraction) : null;
  return {
    lStab,
    lAcc,
    gap: lStab !== null && lAcc !== null ? lStab - lAcc : null,
    fraction,
  };
}
