<script>
  /**
   * The app's model-select idiom as a reusable dropdown: a trigger showing
   * the active item's name (+ optional sub line) and a floating list.
   * ``items``: [{ key, label, sub?, color?, group? }]. ``onselect(key)``.
   */
  let {
    items = [], value = null, placeholder = '—', ariaLabel = 'Select', title = '',
    onselect, compact = false, width = null, align = 'left', onhover = null,
  } = $props();
  let open = $state(false);
  let el = $state();
  const active = $derived(items.find((i) => i.key === value) ?? null);

  function pick(key) {
    open = false;
    onhover?.(null);
    onselect?.(key);
  }
  function onWindowClick(e) {
    if (open && el && !el.contains(e.target)) { open = false; onhover?.(null); }
  }
</script>

<svelte:window onclick={onWindowClick} onkeydown={(e) => { if (e.key === 'Escape') open = false; }} />

<div class="dd" class:compact bind:this={el} style:width={width}>
  <button
    type="button"
    class="dd-trigger"
    class:open
    aria-haspopup="listbox"
    aria-expanded={open}
    aria-label={ariaLabel}
    {title}
    onclick={(e) => { e.stopPropagation(); open = !open; }}
  >
    <span class="dd-name" style:color={active?.color || 'var(--text-primary)'}>{active?.label ?? placeholder}</span>
    {#if active?.sub && !compact}<span class="dd-sub">{active.sub}</span>{/if}
    <svg class="dd-caret" width="11" height="11" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 4.5L6 7.5L9 4.5"/></svg>
  </button>
  {#if open}
    <div class="dd-menu" class:right={align === 'right'} role="listbox" tabindex="-1" aria-label={ariaLabel} onmouseleave={() => onhover?.(null)}>
      {#each items as item (item.key)}
        {#if item.group}
          <div class="dd-group">{item.group}</div>
        {/if}
        <button
          type="button"
          class="dd-option"
          class:active={item.key === value}
          role="option"
          aria-selected={item.key === value}
          onmouseenter={() => onhover?.(item.key)}
          onclick={() => pick(item.key)}
        >
          <span class="dd-name" style:color={item.color || 'var(--text-primary)'}>{item.label}</span>
          {#if item.sub}<span class="dd-sub">{item.sub}</span>{/if}
        </button>
      {/each}
    </div>
  {/if}
</div>

<style>
  .dd { position: relative; min-width: 0; }
  .dd-trigger {
    width: 100%; display: flex; flex-direction: column; align-items: flex-start; gap: 1px;
    padding: 6px 28px 6px 11px; background: var(--surface); border: 1px solid var(--panel-border);
    border-radius: 8px; color: var(--text-primary); font: inherit; cursor: pointer; position: relative;
    text-align: left; transition: border-color .15s, background .15s;
  }
  .dd.compact .dd-trigger { padding: 5px 26px 5px 9px; }
  .dd-trigger:hover { background: var(--hover-bg); border-color: rgba(255,255,255,.18); }
  .dd-trigger.open { border-color: var(--accent); }
  .dd-name { font-size: 14px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 100%; }
  .dd.compact .dd-name { font-size: 13px; }
  .dd-sub { font-size: 10.5px; color: var(--text-secondary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 100%; }
  .dd-caret { position: absolute; right: 9px; top: 50%; transform: translateY(-50%); color: var(--text-secondary); transition: transform .15s; }
  .dd-trigger.open .dd-caret { transform: translateY(-50%) rotate(180deg); }
  .dd-menu {
    position: absolute; top: calc(100% + 4px); left: 0; min-width: 100%; max-height: 300px; overflow-y: auto;
    background: var(--panel-solid); border: 1px solid #454c58; border-radius: 8px; z-index: 90; padding: 4px;
    box-shadow: 0 8px 25px #0008;
  }
  .dd-menu.right { left: auto; right: 0; }
  .dd-group { font-size: 10px; text-transform: uppercase; letter-spacing: .05em; color: var(--text-secondary); padding: 6px 10px 2px; }
  .dd-option {
    display: flex; flex-direction: column; align-items: flex-start; gap: 1px; width: 100%; padding: 7px 10px;
    background: none; border: 0; border-radius: 6px; color: var(--text-primary); font: inherit; cursor: pointer; text-align: left;
    white-space: nowrap;
  }
  .dd-option:hover { background: var(--hover-bg); }
  .dd-option.active { background: var(--accent-glow); }
</style>
