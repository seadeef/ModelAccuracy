/**
 * The client-side estimators must reproduce the pipeline's published numbers
 * on the same population: pooled-scope R from episodes.json (production
 * cluster bootstrap), the revision path, and the heavy-rain trigger sweep.
 * Run: `npm test` (needs the exported static_export/ar tree and
 * stats_output/ catalogs on disk; skips when absent).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  scopeIndices, scopeClusters, revisionEfficiency, revisionPath, triggerSkill, waterfall, pairedDeltaR,
} from '../src/lib/basinAnalysis.js';
import { episodeClusters } from '../src/lib/bootstrap.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const readJson = (p) => JSON.parse(fs.readFileSync(path.join(root, p), 'utf8'));
const exists = (p) => fs.existsSync(path.join(root, p));

const HUC = '180201';
const have = exists(`static_export/ar/basin_revisions/gfs/${HUC}.json`) && exists('stats_output/gfs/episodes.json');

test('pooled-scope R reproduces episodes.json (gfs, Lower Sacramento)', { skip: !have }, () => {
  const rev = readJson(`static_export/ar/basin_revisions/gfs/${HUC}.json`);
  const catalog = readJson('stats_output/gfs/episodes.json').basins[HUC];
  for (const scope of ['ar_all', 'non_ar', 'all_days']) {
    const ref = catalog.scopes[scope].revision_efficiency;
    const idx = scopeIndices(rev, { scope, episodes: catalog.episodes });
    const info = scopeClusters(rev, idx, catalog.episodes, scope);
    assert.equal(idx.length, ref.dates, `${scope}: date count`);
    assert.equal(info.clusters.length, ref.clusters, `${scope}: cluster count`);
    const mine = revisionEfficiency(rev, idx, info);
    assert.ok(Math.abs(mine.value - ref.value) < 2e-3, `${scope}: R ${mine.value} vs ${ref.value}`);
    if (ref.ci && mine.ci) {
      const w = ref.ci[1] - ref.ci[0];
      assert.ok(Math.abs(mine.ci[0] - ref.ci[0]) < 0.25 * w && Math.abs(mine.ci[1] - ref.ci[1]) < 0.25 * w, `${scope}: CI ${mine.ci} vs ${ref.ci}`);
    }
  }
});

test('revision path and trigger skill are well-formed', { skip: !have }, () => {
  const rev = readJson(`static_export/ar/basin_revisions/gfs/${HUC}.json`);
  const idx = scopeIndices(rev, { scope: 'all_days' });
  const p = revisionPath(rev, idx[0]);
  assert.equal(p.revisions.length, rev.lead_max - rev.lead_min);
  assert.ok(p.total >= 0);
  assert.equal(p.forecasts.length, rev.lead_max - rev.lead_min + 1);
  const t = triggerSkill(rev, idx, 10, 2);
  assert.ok(t.point.hits + t.point.misses === t.nObs);
  assert.ok(t.ap >= 0 && t.ap <= 1);
  const w = waterfall(rev, idx);
  assert.ok(Math.abs(w.revisionTotal / w.skillGain - w.r) < 1e-9);
});

test('trigger sweep matches the curated precision–recall operating point (gfs, 10 mm, lead 3)', { skip: !have || !exists('data/pr_curves.json') }, () => {
  const doc = readJson('data/pr_curves.json');
  const block = doc.models?.gfs?.['10']?.lead_3;
  if (!block || doc.huc !== HUC) return; // the artifact pools basins; only a same-basin extract is comparable
  const rev = readJson(`static_export/ar/basin_revisions/gfs/${HUC}.json`);
  const idx = scopeIndices(rev, { scope: 'all_days' });
  const t = triggerSkill(rev, idx, 10, 3 - rev.lead_min);
  assert.ok(Math.abs(t.ap - block.ap) < 0.05);
});

test('paired ΔR uses common random numbers (self-contrast is exactly zero)', { skip: !have }, () => {
  const rev = readJson(`static_export/ar/basin_revisions/gfs/${HUC}.json`);
  const d = pairedDeltaR(rev, rev, [], { scope: 'all_days' });
  assert.equal(d.delta, 0);
  assert.deepEqual(d.ci, [0, 0]);
});

test('pseudo-clusters split at 3-day gaps and 10-day epoch blocks', () => {
  const dates = ['20250101', '20250102', '20250103', '20250107', '20250108', '20250111', '20250112'];
  const { clusters, arDays, arClusterCount } = episodeClusters(dates, []);
  assert.equal(arClusterCount, 0);
  assert.equal(arDays.size, 0);
  // 01-01..01-03 (block 0), 01-07..01-08 (gap), 01-11..01-12 (block 1 starts 01-11)
  assert.deepEqual(clusters.map((c) => c.length), [3, 2, 2]);
});
