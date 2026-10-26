<script>
  /**
   * The app's model-select idiom as a reusable dropdown: a trigger showing
   * the active item's name (+ optional sub line) and a floating list.
   * ``items``: [{ key, label, sub?, color?, group? }]. ``onselect(key)``.
   * ``placeholder`` is the trigger's text when no item matches ``value``.
   *
   * The list is lifted into <body> and positioned from the trigger's
   * viewport rectangle, so no ancestor's overflow or transform can clip it
   * (the floating AR panel has both), and it opens upward when the space
   * below is short.
   */
  import { fitText } from '../fitText.js';
  let {
    items = [], value = null, placeholder = '—', ariaLabel = 'Select', title = '',
    onselect, compact = false, width = null, align = 'left', onhover = null,
  } = $props();
  let open = $state(false);
  let el = $state();
  let menuEl = $state();
  /** Viewport placement of the open list: { up, left|right, top|bottom, minWidth, maxHeight }. */
  let pos = $state(null);
  const active = $derived(items.find((i) => i.key === value) ?? null);
  const MENU_MAX = 300;
  const GAP = 4;
  const EDGE = 8;

  function place() {
    if (!el) return;
    const r = el.getBoundingClientRect();
    const below = window.innerHeight - r.bottom - GAP - EDGE;
    const above = r.top - GAP - EDGE;
    const up = below < Math.min(MENU_MAX, 180) && above > below;
    pos = {
      up,
      left: align === 'right' ? null : r.left,
      right: align === 'right' ? window.innerWidth - r.right : null,
      top: up ? null : r.bottom + GAP,
      bottom: up ? window.innerHeight - r.top + GAP : null,
      minWidth: r.width,
      maxHeight: Math.max(96, Math.min(MENU_MAX, up ? above : below)),
    };
  }
  function close() { open = false; onhover?.(null); }
  function toggle(e) {
    e.stopPropagation();
    if (open) close();
    else { place(); open = true; }
  }
  function pick(key) {
    close();
    onselect?.(key);
  }
  function onWindowClick(e) {
    if (open && !el?.contains(e.target) && !menuEl?.contains(e.target)) close();
  }
  /** Any scroll outside the list moves the trigger away from the list: close rather than drift. */
  function onScroll(e) {
    if (open && !menuEl?.contains(e.target)) close();
  }
  /** Lift the list to <body>; its scoped classes and the :root variables still apply there. */
  function portal(node) {
    document.body.appendChild(node);
    return { destroy() { node.remove(); } };
  }
</script>

<svelte:window onclick={onWindowClick} onkeydown={(e) => { if (e.key === 'Escape') close(); }} onresize={() => { if (open) close(); }} onscrollcapture={onScroll} />

<div class="dd" class:compact bind:this={el} style:width={width}>
  <button
    type="button"
    class="dd-trigger"
    class:open
    aria-haspopup="listbox"
    aria-expanded={open}
    aria-label={ariaLabel}
    {title}
    onclick={toggle}
  >
    <span class="dd-name" style:color={active?.color || 'var(--text-primary)'} use:fitText={{ min: 10 }}>{active?.label ?? placeholder}</span>
    {#if active?.sub && !compact}<span class="dd-sub" use:fitText={{ min: 9 }}>{active.sub}</span>{/if}
    <svg class="dd-caret" width="11" height="11" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 4.5L6 7.5L9 4.5"/></svg>
  </button>
  {#if open && pos}
    <div
      class="dd-menu"
      class:up={pos.up}
      use:portal
      bind:this={menuEl}
      role="listbox"
      tabindex="-1"
      aria-label={ariaLabel}
      style:left={pos.left != null ? `${pos.left}px` : null}
      style:right={pos.right != null ? `${pos.right}px` : null}
      style:top={pos.top != null ? `${pos.top}px` : null}
      style:bottom={pos.bottom != null ? `${pos.bottom}px` : null}
      style:min-width="{pos.minWidth}px"
      style:max-height="{pos.maxHeight}px"
      onmouseleave={() => onhover?.(null)}
    >
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
      {#if !items.length}
        <div class="dd-empty">Nothing to pick</div>
      {/if}
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
    position: fixed; overflow-y: auto; max-width: min(420px, calc(100vw - 16px));
    background: var(--panel-solid); border: 1px solid #454c58; border-radius: 8px; z-index: 1000; padding: 4px;
    box-shadow: 0 8px 25px #0008; font-size: 13px;
  }
  .dd-menu.up { box-shadow: 0 -8px 25px #0008; }
  .dd-group { font-size: 10px; text-transform: uppercase; letter-spacing: .05em; color: var(--text-secondary); padding: 6px 10px 2px; }
  .dd-option + .dd-group { margin-top: 4px; padding-top: 8px; border-top: 1px solid var(--rule); }
  .dd-option {
    display: flex; flex-direction: column; align-items: flex-start; gap: 1px; width: 100%; padding: 7px 10px;
    background: none; border: 0; border-radius: 6px; color: var(--text-primary); font: inherit; cursor: pointer; text-align: left;
    white-space: nowrap; min-width: 0;
  }
  .dd-option .dd-name, .dd-option .dd-sub { max-width: 100%; }
  .dd-option:hover { background: var(--hover-bg); }
  .dd-option.active { background: var(--accent-glow); }
  .dd-empty { padding: 8px 10px; font-size: 12px; color: var(--text-secondary); }
</style>
