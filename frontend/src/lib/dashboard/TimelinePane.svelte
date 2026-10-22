<script>
  /** AR mode: the tARget footprint timeline (range drill, day cells, frame scrubber, episode markers). */
  import { ui } from '../state.svelte.js';
  import { dash, palette } from './dashData.svelte.js';
  import { ensureArManifest } from '../arManifest.svelte.js';
  import { perDateComponents } from '../basinAnalysis.js';
  import ArTimeline from '../components/ArTimeline.svelte';
  import { fmtRange } from './basinScope.svelte.js';

  $effect(() => { ensureArManifest(); });

  /** Best model per day on daily R (lower is better), for the timeline's winner pills. */
  const dayWinners = $derived.by(() => {
    const byModel = dash.basinSeries;
    if (!byModel) return null;
    const best = new Map();
    for (const [key, rev] of Object.entries(byModel)) {
      for (let d = 0; d < rev.dates.length; d++) {
        const fc = rev.forecasts[d];
        const y = rev.observed[d];
        if (y == null || !fc || fc.some((v) => v == null)) continue;
        const { revision, skill } = perDateComponents(fc, y);
        if (!(skill > 0)) continue;
        const v = revision / skill;
        const s = rev.dates[d];
        const iso = `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
        const cur = best.get(iso);
        if (!cur || v < cur.value) best.set(iso, { value: v, winner: key, tied: false });
        else if (v === cur.value) cur.tied = true;
      }
    }
    const map = new Map();
    for (const [iso, v] of best) if (!v.tied) map.set(iso, v.winner);
    return map;
  });
</script>

<div class="wrap">
  <div class="head">
    <div class="eyebrow" title="Drill the timeline (year → season → month → week) to set the range; at week or episode depth click a day to inspect it. Purple markers are catalogued AR episodes; the needle scrubs the map's footprints.">Atmospheric-river timeline · <em class="accent">{fmtRange() || '…'}</em></div>
  </div>
  <ArTimeline episodes={dash.episodes?.episodes ?? []} {dayWinners} winnerLabel="day revision efficiency — lower is better" />
</div>

<style>
  .wrap { display: flex; flex-direction: column; flex: 1; min-height: 0; overflow: auto; }
  .head { margin-bottom: 6px; }
  .accent { color: #b9a6ff; }
</style>
