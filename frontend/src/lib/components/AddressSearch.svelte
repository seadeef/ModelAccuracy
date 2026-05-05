<script>
  import { suggestPlaces, retrievePlace, fetchZip, looksLikeZip } from '../api.js';

  let { onSelect, onStatus } = $props();

  let query = $state('');
  let suggestions = $state([]);
  let activeIndex = $state(-1);
  let open = $state(false);
  let loading = $state(false);
  let inputEl;

  let sessionToken = crypto.randomUUID();
  let debounceTimer;
  let inflight;

  function clearSuggestions() {
    suggestions = [];
    activeIndex = -1;
    open = false;
  }

  async function runSuggest(q) {
    if (inflight) inflight.abort();
    const ctrl = new AbortController();
    inflight = ctrl;
    loading = true;
    const results = await suggestPlaces(q, sessionToken, { signal: ctrl.signal });
    if (ctrl.signal.aborted) return;
    loading = false;
    suggestions = results;
    activeIndex = results.length ? 0 : -1;
    open = results.length > 0;
  }

  function onInput(e) {
    query = e.target.value;
    clearTimeout(debounceTimer);
    const q = query.trim();
    if (!q) { clearSuggestions(); loading = false; return; }
    // Pure 5-digit ZIP: skip the API — we already have a static lookup.
    if (looksLikeZip(q)) { clearSuggestions(); loading = false; return; }
    debounceTimer = setTimeout(() => runSuggest(q), 220);
  }

  async function selectSuggestion(s) {
    onStatus?.(`Locating ${s.name}…`);
    clearSuggestions();
    query = s.full_address || s.name;
    const data = await retrievePlace(s.mapbox_id, sessionToken);
    // Mapbox billing groups suggest+retrieve by session; rotate after a pick.
    sessionToken = crypto.randomUUID();
    if (!data.found) { onStatus?.('Location not found'); return; }
    onSelect?.(data);
    onStatus?.(`Centered on ${data.label || s.name}`);
    inputEl?.blur();
  }

  async function submitQuery() {
    const q = query.trim();
    if (!q) return;
    if (looksLikeZip(q)) {
      onStatus?.(`Looking up ${q}…`);
      const data = await fetchZip(q);
      if (!data.found) { onStatus?.(`ZIP not found: ${q}`); return; }
      onSelect?.(data);
      onStatus?.(`Centered on ${data.zip}`);
      clearSuggestions();
      inputEl?.blur();
      return;
    }
    if (activeIndex >= 0 && suggestions[activeIndex]) {
      selectSuggestion(suggestions[activeIndex]);
    } else if (suggestions[0]) {
      selectSuggestion(suggestions[0]);
    }
  }

  function onKeydown(e) {
    if (e.key === 'ArrowDown') {
      if (!suggestions.length) return;
      e.preventDefault();
      activeIndex = (activeIndex + 1) % suggestions.length;
      open = true;
    } else if (e.key === 'ArrowUp') {
      if (!suggestions.length) return;
      e.preventDefault();
      activeIndex = (activeIndex - 1 + suggestions.length) % suggestions.length;
      open = true;
    } else if (e.key === 'Enter') {
      e.preventDefault();
      submitQuery();
    } else if (e.key === 'Escape') {
      clearSuggestions();
      inputEl?.blur();
    }
  }

  function onBlur() {
    // Delay so a click on a list item can register before we close.
    setTimeout(() => { open = false; }, 120);
  }

  function onFocus() {
    if (suggestions.length) open = true;
  }

  function clearInput() {
    query = '';
    clearSuggestions();
    inputEl?.focus();
  }

  function splitLabel(s) {
    const full = s.full_address || s.name;
    const main = s.name || full;
    let secondary = '';
    if (s.full_address && s.full_address !== main) {
      secondary = s.full_address.startsWith(main + ',')
        ? s.full_address.slice(main.length + 1).trim()
        : s.full_address;
    } else if (s.place_formatted) {
      secondary = s.place_formatted;
    }
    return { main, secondary };
  }
</script>

<div class="search" role="combobox" aria-expanded={open} aria-haspopup="listbox" aria-owns="address-suggestions">
  <div class="search-box" class:search-box--open={open && suggestions.length > 0}>
    <svg class="search-icon" width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
      <circle cx="7" cy="7" r="4.5" />
      <path d="M10.5 10.5L14 14" stroke-linecap="round" />
    </svg>
    <input
      bind:this={inputEl}
      type="text"
      class="search-input"
      placeholder="Search address or ZIP"
      value={query}
      oninput={onInput}
      onkeydown={onKeydown}
      onblur={onBlur}
      onfocus={onFocus}
      autocomplete="off"
      spellcheck="false"
      aria-label="Search address or ZIP"
      aria-autocomplete="list"
      aria-controls="address-suggestions"
      aria-activedescendant={activeIndex >= 0 ? `sugg-${activeIndex}` : undefined}
    />
    {#if loading}
      <div class="spinner" aria-hidden="true"></div>
    {:else if query}
      <button type="button" class="clear-btn" onclick={clearInput} aria-label="Clear search">
        <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 4l8 8M12 4l-8 8"/></svg>
      </button>
    {/if}
  </div>

  {#if open && suggestions.length > 0}
    <ul
      id="address-suggestions"
      class="suggestions"
      role="listbox"
    >
      {#each suggestions as s, i (s.mapbox_id)}
        {@const parts = splitLabel(s)}
        <li
          id={`sugg-${i}`}
          role="option"
          aria-selected={i === activeIndex}
          class="sugg"
          class:sugg--active={i === activeIndex}
          onmousedown={(e) => { e.preventDefault(); selectSuggestion(s); }}
          onmouseenter={() => { activeIndex = i; }}
        >
          <svg class="sugg-icon" width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
            <path d="M8 14s5-4.5 5-8.5a5 5 0 10-10 0C3 9.5 8 14 8 14z" />
            <circle cx="8" cy="5.5" r="1.8" />
          </svg>
          <div class="sugg-text">
            <span class="sugg-main">{parts.main}</span>
            {#if parts.secondary}
              <span class="sugg-secondary">{parts.secondary}</span>
            {/if}
          </div>
        </li>
      {/each}
      <li class="attribution" aria-hidden="true">
        <span>Powered by Mapbox</span>
      </li>
    </ul>
  {/if}
</div>

<style>
  .search {
    position: relative;
    width: 280px;
    max-width: 100%;
  }
  .search-box {
    display: flex;
    align-items: center;
    gap: 8px;
    box-sizing: border-box;
    height: var(--toolbar-control-height, 32px);
    padding: 0 10px;
    background: var(--surface);
    border: 1px solid var(--panel-border);
    border-radius: 9px;
    transition: border-color 0.15s, box-shadow 0.15s, border-radius 0.15s;
  }
  .search-box:focus-within {
    border-color: var(--accent);
    box-shadow: 0 0 0 3px var(--accent-glow);
  }
  .search-box--open {
    border-bottom-left-radius: 0;
    border-bottom-right-radius: 0;
    border-bottom-color: transparent;
  }
  .search-icon {
    flex-shrink: 0;
    color: var(--text-secondary);
  }
  .search-input {
    flex: 1;
    min-width: 0;
    height: 100%;
    background: transparent;
    border: none;
    outline: none;
    color: var(--text-primary);
    font-family: inherit;
    font-size: 13px;
    line-height: 1.25;
  }
  .search-input::placeholder {
    color: var(--text-secondary);
    opacity: 0.8;
  }
  .clear-btn {
    flex-shrink: 0;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    background: transparent;
    border: none;
    border-radius: 50%;
    color: var(--text-secondary);
    cursor: pointer;
    transition: background 0.12s, color 0.12s;
  }
  .clear-btn:hover {
    background: var(--hover-bg);
    color: var(--text-primary);
  }
  .spinner {
    width: 13px;
    height: 13px;
    flex-shrink: 0;
    border: 1.5px solid var(--panel-border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
  }
  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  .suggestions {
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    z-index: 20;
    margin: 0;
    padding: 6px 0;
    list-style: none;
    background: var(--panel-solid);
    border: 1px solid var(--panel-border);
    border-top: 1px solid var(--panel-border);
    border-bottom-left-radius: 12px;
    border-bottom-right-radius: 12px;
    box-shadow: 0 12px 32px rgba(0, 0, 0, 0.5);
    max-height: 320px;
    overflow-y: auto;
    animation: dropdownIn 0.12s ease-out;
  }
  @keyframes dropdownIn {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
  }

  .sugg {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 9px 12px;
    cursor: pointer;
    transition: background 0.1s;
  }
  .sugg--active {
    background: var(--hover-bg);
  }
  .sugg-icon {
    flex-shrink: 0;
    margin-top: 1px;
    color: var(--text-secondary);
  }
  .sugg--active .sugg-icon {
    color: var(--accent);
  }
  .sugg-text {
    display: flex;
    flex-direction: column;
    min-width: 0;
    gap: 1px;
  }
  .sugg-main {
    font-size: 13px;
    font-weight: 500;
    color: var(--text-primary);
    line-height: 1.3;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .sugg-secondary {
    font-size: 11.5px;
    color: var(--text-secondary);
    line-height: 1.3;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .attribution {
    padding: 6px 12px 2px;
    font-size: 10px;
    letter-spacing: 0.35px;
    text-transform: uppercase;
    color: var(--text-secondary);
    text-align: right;
    opacity: 0.65;
    border-top: 1px solid var(--panel-border);
    margin-top: 4px;
  }

  @media (max-width: 640px) {
    .search {
      width: 100%;
      min-width: 180px;
    }
    .search-input {
      font-size: 16px; /* reduces iOS zoom-on-focus */
    }
    .suggestions {
      max-height: 60vh;
    }
  }
</style>
