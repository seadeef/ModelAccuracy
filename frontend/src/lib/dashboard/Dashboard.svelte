<script>
  /**
   * The analysis column of the one-screen dashboard, and the owner of every
   * data fetch (results land in ``dash`` for the header and tiles too).
   *
   * Fetch keys follow the Panel's generation-counter idiom: each effect reads
   * its reactive inputs, bails when the key is unchanged, and discards stale
   * responses. Period changes refetch accuracy/consistency; model changes do
   * not (every model is always loaded) except for the episode catalog.
   */
  import { untrack } from 'svelte';
  import { appConfig, ui, accuracyStatKeys, winnerStatKeys, isArPeriod } from '../state.svelte.js';
  import {
    fetchStatsAllLeadsAllModels, fetchLeadWinnersForRegion, fetchRevisionEfficiency,
    fetchBasinsIndex, fetchBasinRevisionsAllModels, fetchArEpisodes, fetchStormCatalog,
    fetchForecastAllVariables,
  } from '../api.js';
  import { STATIC_BASE } from '../constants.js';
  import { dash, resetDash, leadAxis, leadSink } from './dashData.svelte.js';
  import { datesInRange, wettestDay } from '../basinAnalysis.js';
  import { ymdToIso, isoToYmd } from '../bootstrap.js';
  import LeadBar from './LeadBar.svelte';
  import AccuracyPane from './AccuracyPane.svelte';
  import ConsistencyPane from './ConsistencyPane.svelte';
  import AnalysisTabs from './AnalysisTabs.svelte';

  let { onleadchange } = $props();
  // A fresh mount (region picked after the dashboard was closed) must not
  // show the previous region's numbers while the new fetches are in flight.
  resetDash();
  $effect(() => { leadSink.fn = onleadchange; return () => { leadSink.fn = null; }; });

  function regionKey(r) {
    if (!r) return '';
    if (r.type === 'point') return `pt:${r.coordinates.join(',')}`;
    if (r.type === 'rectangle') return `rect:${(r.bounds || r.coordinates || []).join(',')}`;
    if (r.type === 'polygon') return `poly:${JSON.stringify(r.coordinates)}`;
    if (r.type === 'admin') return `admin:${r.level}:${r.fips}`;
    if (r.type === 'huc') return `huc:${r.huc}`;
    if (r.type === 'military') return `mil:${r.site}`;
    return JSON.stringify(r);
  }

  /** Climatology period the gridded products are read at (AR mode keeps yearly). */
  function gridPeriod() {
    return isArPeriod(ui.period) ? 'yearly' : ui.period;
  }

  // ── Accuracy curves (all models, all verification stats) ──
  let accGen = 0;
  let accKey = '';
  $effect(() => {
    const region = ui.selectedRegion;
    const period = gridPeriod();
    const month = ui.month;
    const season = ui.season;
    const key = region ? `${regionKey(region)}|${period}|${month}|${season}` : '';
    if (key === accKey) return;
    accKey = key;
    const gen = ++accGen;
    if (!key) { resetDash(); return; }
    const stats = accuracyStatKeys().filter((k) => !appConfig.consistency.stats.includes(k));
    dash.accuracyLoading = true;
    queueMicrotask(async () => {
      const data = await fetchStatsAllLeadsAllModels({ region, period, month, season, statistics: stats });
      if (gen !== accGen) return;
      dash.accuracy = data;
      dash.accuracyLoading = false;
    });
  });

  // ── Consistency curves (J + e_L at the current threshold/scale; yearly/seasonal only) ──
  let conGen = 0;
  let conKey = '';
  $effect(() => {
    const region = ui.selectedRegion;
    const period = gridPeriod();
    const season = ui.season;
    const threshold = ui.consistencyThreshold;
    const scale = ui.consistencyScale;
    const supported = period === 'yearly' || period === 'seasonal';
    const key = region && supported ? `${regionKey(region)}|${period}|${season}|q${threshold}|s${scale}` : '';
    if (key === conKey) return;
    conKey = key;
    const gen = ++conGen;
    if (!key) { dash.consistency = null; return; }
    dash.consistencyLoading = true;
    queueMicrotask(async () => {
      const data = await fetchStatsAllLeadsAllModels({
        region, period, season, threshold, scale, statistics: appConfig.consistency.stats,
      });
      if (gen !== conGen) return;
      dash.consistency = data;
      dash.consistencyLoading = false;
    });
  });

  // ── Best model by lead for the selected statistic ──
  let winGen = 0;
  let winKey = '';
  $effect(() => {
    const region = ui.selectedRegion;
    const stat = ui.statistic;
    const period = gridPeriod();
    const month = ui.month;
    const season = ui.season;
    const ok = region && winnerStatKeys().includes(stat);
    const key = ok ? `${regionKey(region)}|${stat}|${period}|${month}|${season}` : '';
    if (key === winKey) return;
    winKey = key;
    const gen = ++winGen;
    if (!key) return;
    if (dash.winners[stat]?.__key === key) return;
    const axis = leadAxis();
    dash.winnersLoading = true;
    queueMicrotask(async () => {
      const data = await fetchLeadWinnersForRegion({
        region, statistic: stat, period, month, season, minLead: axis[0], maxLead: axis[axis.length - 1],
      });
      if (gen !== winGen) return;
      dash.winners = { ...dash.winners, [stat]: data ? { ...data, __key: key } : null };
      dash.winnersLoading = false;
    });
  });

  // ── Grid-pooled revision efficiency per model (non-basin fallback) ──
  let rgGen = 0;
  let rgKey = '';
  $effect(() => {
    const region = ui.selectedRegion;
    const period = gridPeriod();
    const season = ui.season;
    const supported = period === 'yearly' || period === 'seasonal';
    const key = region && supported ? `${regionKey(region)}|${period}|${season}` : '';
    if (key === rgKey) return;
    rgKey = key;
    const gen = ++rgGen;
    if (!key) { dash.revisionGrid = null; return; }
    queueMicrotask(async () => {
      const entries = await Promise.all(appConfig.models.map(async (m) => [
        m.key, await fetchRevisionEfficiency({ model: m.key, region, period, season }),
      ]));
      if (gen !== rgGen) return;
      const out = {};
      for (const [k, v] of entries) if (v) out[k] = v;
      dash.revisionGrid = out;
    });
  });

  // ── Basin resolution + series/catalog/storms ──
  let basinGen = 0;
  let basinKey = '';
  $effect(() => {
    const region = ui.selectedRegion;
    const huc = region?.type === 'huc' && String(region.huc).length >= 6 ? String(region.huc).slice(0, 6) : null;
    const key = huc || '';
    if (key === basinKey) return;
    basinKey = key;
    const gen = ++basinGen;
    dash.basinHuc = null;
    dash.basinInfo = null;
    dash.basinSeries = null;
    dash.episodes = null;
    dash.storms = null;
    if (!huc) return;
    dash.basinLoading = true;
    queueMicrotask(async () => {
      const index = await fetchBasinsIndex();
      const info = index?.basins?.[huc] ?? null;
      if (gen !== basinGen) return;
      if (!info) { dash.basinLoading = false; return; }
      const [series, storms] = await Promise.all([
        fetchBasinRevisionsAllModels({ huc }),
        fetchStormCatalog(info.huc2),
      ]);
      if (gen !== basinGen) return;
      dash.basinHuc = huc;
      dash.basinInfo = info;
      dash.basinSeries = series;
      dash.storms = storms;
      dash.basinLoading = false;
    });
  });

  // ── Episode catalog (per model: forecast_dates differ; membership does not) ──
  let epGen = 0;
  let epKey = '';
  $effect(() => {
    const huc = dash.basinHuc;
    const model = ui.model;
    const key = huc ? `${huc}|${model}` : '';
    if (key === epKey) return;
    epKey = key;
    const gen = ++epGen;
    if (!key) { dash.episodes = null; return; }
    queueMicrotask(async () => {
      let cat = null;
      try { cat = await fetchArEpisodes({ model, huc }); } catch { cat = null; }
      if (gen !== epGen) return;
      dash.episodes = cat;
    });
  });

  // ── Default inspected range + day once a basin's series is in ──
  $effect(() => {
    const series = dash.basinSeries;
    const storms = dash.storms;
    const huc = dash.basinHuc;
    if (!series || !huc) return;
    untrack(() => {
      const rev = series[ui.model] ?? Object.values(series)[0];
      if (!rev?.dates?.length) return;
      const first = rev.dates[0];
      const last = rev.dates[rev.dates.length - 1];
      const s = isoToYmd(ui.arRangeStart);
      const e = isoToYmd(ui.arRangeEnd);
      const rangeOk = s && e && s >= first && e <= last && s <= e;
      if (!rangeOk) {
        // Most recent analysed storm that touched this basin; else the last 30 days.
        const ev = (storms?.events || []).filter((x) => x.in_analysis && x.basin_hucs?.includes(huc)).at(-1);
        if (ev) {
          ui.arRangeStart = ev.start_date;
          ui.arRangeEnd = ev.end_date;
        } else {
          ui.arRangeStart = ymdToIso(rev.dates[Math.max(0, rev.dates.length - 30)]);
          ui.arRangeEnd = ymdToIso(last);
        }
        ui.arEpisodeId = null;
      }
      const range = [isoToYmd(ui.arRangeStart), isoToYmd(ui.arRangeEnd)];
      const target = isoToYmd(ui.arTargetDate);
      if (!target || target < range[0] || target > range[1] || rev.dates.indexOf(target) < 0) {
        const wet = wettestDay(rev, datesInRange(rev, range));
        if (wet != null) ui.arTargetDate = ymdToIso(rev.dates[wet]);
      }
    });
  });

  // ── Forecast (ensemble) for the Forecast tab ──
  let fcGen = 0;
  let fcKey = '';
  $effect(() => {
    const region = ui.selectedRegion;
    const key = region ? regionKey(region) : '';
    if (key === fcKey) return;
    fcKey = key;
    const gen = ++fcGen;
    if (!key) { dash.forecastByVar = null; return; }
    queueMicrotask(async () => {
      const data = await fetchForecastAllVariables({ region });
      if (gen !== fcGen) return;
      dash.forecastByVar = data;
    });
  });

  // ── Map overlay value range for the key ──
  $effect(() => {
    const model = ui.model;
    const stat = ui.statistic;
    const period = gridPeriod();
    const month = ui.month;
    const season = ui.season;
    const sub = period === 'monthly' ? `monthly/${month}` : period === 'seasonal' ? `seasonal/${season}` : 'yearly';
    queueMicrotask(async () => {
      try {
        const resp = await fetch(`${STATIC_BASE}/ranges/${model}/${stat}/${sub}.json`);
        dash.valueRange = resp.ok ? await resp.json() : null;
      } catch {
        dash.valueRange = null;
      }
    });
  });
</script>

<section class="results">
  <LeadBar />
  <div class="paired">
    <AccuracyPane />
    <ConsistencyPane />
  </div>
  <AnalysisTabs />
</section>

<style>
  .results {
    display: grid;
    grid-template-rows: 48px minmax(190px, 34%) minmax(0, 1fr);
    gap: 12px;
    min-width: 0;
    min-height: 0;
  }
  .paired {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
    min-width: 0;
    min-height: 0;
  }
  @media (max-width: 1100px) {
    .paired { grid-template-columns: 1fr; }
    .results { grid-template-rows: 48px auto minmax(0, 1fr); }
  }
</style>
