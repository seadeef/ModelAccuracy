<script>
  import { ui } from '../state.svelte.js';
  import { MONTH_NAMES, SEASON_NAMES } from '../constants.js';

  let { onchange } = $props();

  let open = $state(false);

  function periodLabel() {
    if (ui.period === 'yearly') return 'Yearly';
    if (ui.period === 'monthly') return MONTH_NAMES[parseInt(ui.month, 10)] || ui.month;
    if (ui.period === 'seasonal') return SEASON_NAMES[ui.season] || ui.season;
    return 'Yearly';
  }

  function selectPeriod(period, month, season) {
    ui.period = period;
    if (month) ui.month = month;
    if (season) ui.season = season;
    open = false;
    onchange?.();
  }

  function handleClickOutside(e) {
    if (!e.target.closest('.period-menu')) open = false;
  }
</script>

<svelte:document onclick={handleClickOutside} />

<!-- svelte-ignore a11y_click_events_have_key_events -->
<!-- svelte-ignore a11y_no_static_element_interactions -->
<div class="period-menu" onclick={(e) => { e.stopPropagation(); open = !open; }}>
  <button class="period-trigger">
    {periodLabel()}
    <svg width="10" height="10" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 6l4 4 4-4"/></svg>
  </button>
  {#if open}
    <div class="period-dropdown">
      <!-- svelte-ignore a11y_click_events_have_key_events -->
      <!-- svelte-ignore a11y_no_static_element_interactions -->
      <div class="period-item" class:active={ui.period === 'yearly'} onclick={() => selectPeriod('yearly')}>Yearly</div>
      <div class="period-item has-sub" class:active={ui.period === 'monthly'}>
        Monthly
        <div class="period-sub">
          {#each Array.from({length: 12}, (_, i) => i + 1) as m}
            {@const mm = String(m).padStart(2, '0')}
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <!-- svelte-ignore a11y_no_static_element_interactions -->
            <div class="period-sub-item" class:active={ui.period === 'monthly' && ui.month === mm}
                 onclick={(e) => { e.stopPropagation(); selectPeriod('monthly', mm, null); }}>{MONTH_NAMES[m]}</div>
          {/each}
        </div>
      </div>
      <div class="period-item has-sub" class:active={ui.period === 'seasonal'}>
        Seasonal
        <div class="period-sub">
          {#each Object.entries(SEASON_NAMES) as [key, name]}
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <!-- svelte-ignore a11y_no_static_element_interactions -->
            <div class="period-sub-item" class:active={ui.period === 'seasonal' && ui.season === key}
                 onclick={(e) => { e.stopPropagation(); selectPeriod('seasonal', null, key); }}>{name}</div>
          {/each}
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  .period-menu {
    position: relative;
  }
  .period-trigger {
    display: flex;
    align-items: center;
    gap: 4px;
    background: var(--surface);
    color: var(--text-primary);
    border: 1px solid var(--panel-border);
    border-radius: 6px;
    padding: 4px 8px;
    font-size: 11px;
    font-family: inherit;
    cursor: pointer;
    transition: border-color 0.15s;
    white-space: nowrap;
  }
  .period-trigger:hover { border-color: rgba(255,255,255,0.15); }
  .period-trigger.disabled { opacity: 0.35; pointer-events: none; }
  .period-dropdown {
    position: absolute;
    bottom: 100%;
    left: 0;
    margin-bottom: 4px;
    background: var(--panel-solid, #0e1117);
    border: 1px solid var(--panel-border);
    border-radius: 8px;
    min-width: 130px;
    z-index: 1000;
    box-shadow: 0 8px 24px rgba(0,0,0,0.5);
    padding: 4px 0;
  }
  .period-item {
    padding: 6px 12px;
    font-size: 12px;
    color: var(--text-primary);
    cursor: pointer;
    position: relative;
  }
  .period-item:hover { background: var(--hover-bg); }
  .period-item.active { color: var(--accent); }
  .period-item.has-sub { padding-right: 22px; }
  .period-item.has-sub::after {
    content: '\25B8';
    position: absolute;
    right: 8px;
    top: 50%;
    transform: translateY(-50%);
    font-size: 9px;
    color: var(--text-secondary);
  }
  .period-sub {
    display: none;
    position: absolute;
    left: 100%;
    top: -4px;
    background: var(--panel-solid, #0e1117);
    border: 1px solid var(--panel-border);
    border-radius: 8px;
    min-width: 120px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.5);
    padding: 4px 0;
    z-index: 1001;
  }
  .period-item:hover > .period-sub { display: block; }
  .period-sub-item {
    padding: 5px 12px;
    font-size: 12px;
    color: var(--text-primary);
    cursor: pointer;
    white-space: nowrap;
  }
  .period-sub-item:hover { background: var(--hover-bg); }
  .period-sub-item.active { color: var(--accent); }
</style>
