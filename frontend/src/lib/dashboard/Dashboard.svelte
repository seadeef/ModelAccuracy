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
    fetchStatsAllLeadsAllModels, fetchLeadWinnersForRegion,
    fetchBasinsIndex, fetchBasinRevisionsAllModels, fetchRegionSeriesAllModels, fetchRegionBasins, fetchArEpisodes, fetchStormCatalog,
    fetchForecastAllVariables,
  } from '../api.js';
  import { STATIC_BASE } from '../constants.js';
  import { dash, resetDash, leadAxis, leadSink, regionKind, regionTitle, periodLabel, regionKey, rangeOrigin } from './dashData.svelte.js';
  import { datesInRange, wettestDay, wettestSpells } from '../basinAnalysis.js';
  import { regionStormsFor } from './basinScope.svelte.js';
  import { ymdToIso, isoToYmd } from '../bootstrap.js';
  import { fitText } from '../fitText.js';
  import LeadBar from './LeadBar.svelte';
  import AccuracyPane from './AccuracyPane.svelte';
  import ConsistencyPane from './ConsistencyPane.svelte';
  import AnalysisTabs from './AnalysisTabs.svelte';

  let { onleadchange } = $props();
  // A fresh mount (region picked after the dashboard was closed) must not
  // show the previous region's numbers while the new fetches are in flight.
  resetDash();
  $effect(() => { leadSink.fn = onleadchange; return () => { leadSink.fn = null; }; });


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

  // ── Daily series behind the storm tabs ──
  // A click inside an exported HUC6 basin reads that basin's canonical history
  // (area-weighted, with its AR episode and storm catalogs). Every other
  // region — state, HUC2/4, county, installation, point or drawn shape — is
  // pooled from the per-cell cube on demand (``POST /api/series``), so the
  // storm tabs always have data; only the catalog-backed scopes stay basin-only.
  let basinGen = 0;
  let basinKey = '';
  $effect(() => {
    const region = ui.selectedRegion;
    const key = region ? regionKey(region) : '';
    if (key === basinKey) return;
    basinKey = key;
    const gen = ++basinGen;
    dash.basinHuc = null;
    dash.basinInfo = null;
    dash.basinSeries = null;
    dash.episodes = null;
    dash.storms = null;
    dash.regionStorms = null;
    if (!region) { dash.basinLoading = false; return; }
    const huc6 = region.type === 'huc' && String(region.huc).length >= 6 ? String(region.huc).slice(0, 6) : null;
    dash.basinLoading = true;
    queueMicrotask(async () => {
      const info = huc6 ? ((await fetchBasinsIndex())?.basins?.[huc6] ?? null) : null;
      if (gen !== basinGen) return;
      if (info) {
        const [series, storms] = await Promise.all([
          fetchBasinRevisionsAllModels({ huc: huc6 }),
          fetchStormCatalog(info.huc2),
        ]);
        if (gen !== basinGen) return;
        dash.basinHuc = huc6;
        dash.basinInfo = { ...info, source: 'basin' };
        dash.basinSeries = series;
        dash.storms = storms;
        dash.regionStorms = regionStormsFor([huc6], [storms]);
      } else {
        // The region's series and the catalogued basins it touches load
        // together; the storm catalogs of those basins' regions follow.
        const [series, hucs] = await Promise.all([
          fetchRegionSeriesAllModels({ region }),
          fetchRegionBasins({ region }),
        ]);
        if (gen !== basinGen) return;
        const huc2s = [...new Set((hucs ?? []).map((h) => h.slice(0, 2)))];
        const catalogs = await Promise.all(huc2s.map((h) => fetchStormCatalog(h)));
        if (gen !== basinGen) return;
        // Storms land with the series: the default range below reads both at once
        // (a series without its storms would open on a wet spell, not the latest storm).
        const first = series ? Object.values(series)[0] : null;
        dash.regionStorms = regionStormsFor(hucs ?? [], catalogs);
        dash.basinInfo = series
          ? { name: regionTitle(region), region: null, source: 'cube', cells: first?.cells ?? null, nearestCell: !!first?.nearest_cell }
          : null;
        dash.basinSeries = series;
      }
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

  // ── Default inspected range + day once a region's series is in ──
  $effect(() => {
    const series = dash.basinSeries;
    const storms = dash.regionStorms;
    if (!series) return;
    untrack(() => {
      const rev = series[ui.model] ?? Object.values(series)[0];
      if (!rev?.dates?.length) return;
      const first = rev.dates[0];
      const last = rev.dates[rev.dates.length - 1];
      const s = isoToYmd(ui.arRangeStart);
      const e = isoToYmd(ui.arRangeEnd);
      const rangeOk = s && e && s >= first && e <= last && s <= e;
      // A storm picked for another region is stale here unless that storm reached this region too.
      const key = regionKey(ui.selectedRegion);
      const reached = (storms || []).some((ev) => ev.start_date === ui.arRangeStart && ev.end_date === ui.arRangeEnd);
      const stale = !!rangeOrigin.regionKey && rangeOrigin.regionKey !== key && !reached;
      if (!rangeOk || stale) {
        // Most recent analysed storm that reached this region; else the region's
        // wettest five-day spell on record; else the last 30 days.
        const ev = (storms || []).filter((s) => s.notable).at(-1) ?? null;
        const spell = ev ? null : wettestSpells(rev, { span: 5, count: 1 })[0];
        if (ev) {
          ui.arRangeStart = ev.start_date;
          ui.arRangeEnd = ev.end_date;
        } else if (spell) {
          ui.arRangeStart = ymdToIso(rev.dates[spell.start]);
          ui.arRangeEnd = ymdToIso(rev.dates[spell.end]);
        } else {
          ui.arRangeStart = ymdToIso(rev.dates[Math.max(0, rev.dates.length - 30)]);
          ui.arRangeEnd = ymdToIso(last);
        }
        ui.arEpisodeId = null;
        rangeOrigin.regionKey = key;
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

</script>

<section class="results">
  <div class="top">
    <div class="eyebrow" use:fitText={{ min: 9 }} title="The map, the lead bar and both curves pool every scored day of this period over the drawn region. The storm panes below read the region's own daily forecast history instead: the exported HUC6 basin when the click lands in one, otherwise the per-cell archive pooled over the region.">Pooled over the {regionKind(ui.selectedRegion).toLowerCase()} · <em>{periodLabel()}</em></div>
    <LeadBar />
  </div>
  <div class="paired">
    <AccuracyPane />
    <ConsistencyPane />
  </div>
  <AnalysisTabs />
</section>

<style>
  .results {
    display: grid;
    grid-template-rows: auto minmax(200px, 35%) minmax(0, 1fr);
    min-width: 0;
    min-height: 0;
    background: var(--pane-bg);
    border: 1px solid var(--panel-border);
    border-radius: 12px;
    overflow: hidden;
  }
  .top { display: flex; flex-direction: column; gap: 9px; padding: 12px 16px 12px; border-bottom: 1px solid var(--rule); }
  .paired {
    display: grid;
    grid-template-columns: 1fr 1fr;
    min-width: 0;
    min-height: 0;
    border-bottom: 1px solid var(--rule);
  }
  .paired > :global(*:last-child) { border-left: 1px solid var(--rule); }
  @media (max-width: 1100px) {
    .paired { grid-template-columns: 1fr; }
    .paired > :global(*:last-child) { border-left: 0; border-top: 1px solid var(--rule); }
    .results { grid-template-rows: auto auto minmax(0, 1fr); }
  }
</style>
