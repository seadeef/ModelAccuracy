<script>
  import {
    authSession,
    beginCognitoSignIn,
    signOutCognito,
  } from '../authSession.svelte.js';
  import { ui } from '../state.svelte.js';
  import {
    glyphDownloadMap,
    glyphOpacityMoon,
    glyphZipChevron,
  } from '../appIcons.js';

  let {
    zipValue = $bindable(''),
    onOpacityInput,
    onExport,
    goToZip,
  } = $props();
</script>

<div class="map-toolbar" role="toolbar" aria-label="Map tools">
  <button type="button" class="btn-download" onclick={() => onExport?.()} aria-label="Download map image">
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">{@html glyphDownloadMap}</svg>
    <span class="btn-download-label">Download map</span>
  </button>

  <div class="toolbar-sep" aria-hidden="true"></div>

  <div class="opacity-block">
    <span class="ctrl-label">Map opacity</span>
    <div class="opacity-row">
      <svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" class="opacity-icon" aria-hidden="true">{@html glyphOpacityMoon}</svg>
      <input
        type="range"
        min="0"
        max="1"
        step="0.05"
        value={ui.weatherOpacity}
        oninput={onOpacityInput}
        aria-label="Map opacity"
      />
    </div>
  </div>

  <div class="toolbar-sep" aria-hidden="true"></div>

  <div class="zip-block">
    <div class="zip-field">
      <span class="ctrl-label">Fly to</span>
      <input
        type="text"
        placeholder="ZIP"
        size="5"
        bind:value={zipValue}
        onkeydown={(e) => { if (e.key === 'Enter') { e.preventDefault(); goToZip?.(); } }}
        aria-label="ZIP code"
      />
    </div>
    <button type="button" class="zip-go" onclick={() => goToZip?.()} aria-label="Go to ZIP">
      <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2">{@html glyphZipChevron}</svg>
    </button>
  </div>

  {#if authSession.ready && authSession.mode === 'cognito'}
    <div class="toolbar-sep auth-sep" aria-hidden="true"></div>
    <div class="auth-block">
      {#if authSession.hasSession}
        <span class="auth-label" title={authSession.userLabel ?? 'Signed in'}>
          {authSession.userLabel ?? 'Signed in'}
        </span>
        <button type="button" class="auth-btn" onclick={() => signOutCognito()}>Sign out</button>
      {:else}
        <button type="button" class="auth-btn auth-btn--primary" onclick={() => beginCognitoSignIn()}>
          Sign in
        </button>
      {/if}
    </div>
  {/if}
</div>

<style>
  .map-toolbar {
    --toolbar-control-height: 32px;
    position: absolute;
    top: max(14px, env(safe-area-inset-top, 0px));
    left: 50%;
    transform: translateX(-50%);
    z-index: 11;
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    justify-content: center;
    gap: 10px 12px;
    padding: 8px 14px 10px;
    max-width: calc(100vw - 120px - env(safe-area-inset-left, 0px) - env(safe-area-inset-right, 0px));
    background: var(--panel-bg);
    backdrop-filter: blur(20px) saturate(1.4);
    -webkit-backdrop-filter: blur(20px) saturate(1.4);
    border: 1px solid var(--panel-border);
    border-radius: 14px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
  }
  .btn-download {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 8px 14px;
    border: none;
    border-radius: 10px;
    background: linear-gradient(180deg, rgba(110, 181, 255, 0.22), rgba(110, 181, 255, 0.1));
    color: #b8d9ff;
    font-size: 13px;
    font-weight: 600;
    font-family: inherit;
    cursor: pointer;
    border: 1px solid rgba(110, 181, 255, 0.35);
    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.25);
    white-space: nowrap;
    transition: background 0.15s, color 0.15s, border-color 0.15s;
  }
  .btn-download:hover {
    background: linear-gradient(180deg, rgba(110, 181, 255, 0.32), rgba(110, 181, 255, 0.16));
    color: #e8f3ff;
    border-color: rgba(110, 181, 255, 0.5);
  }
  .auth-block {
    display: flex;
    align-items: center;
    gap: 8px;
    max-width: min(200px, 32vw);
  }
  .auth-label {
    font-size: 12px;
    font-weight: 500;
    color: var(--text-secondary);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .auth-btn {
    flex-shrink: 0;
    padding: 6px 12px;
    border-radius: 9px;
    border: 1px solid var(--panel-border);
    background: var(--surface);
    color: var(--text-primary);
    font-size: 12px;
    font-weight: 600;
    font-family: inherit;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s, color 0.15s;
  }
  .auth-btn:hover {
    background: var(--hover-bg);
    border-color: rgba(255, 255, 255, 0.14);
    color: var(--accent);
  }
  .auth-btn--primary {
    background: linear-gradient(180deg, rgba(110, 181, 255, 0.2), rgba(110, 181, 255, 0.08));
    border-color: rgba(110, 181, 255, 0.35);
    color: #b8d9ff;
  }
  .auth-btn--primary:hover {
    border-color: rgba(110, 181, 255, 0.5);
    color: #e8f3ff;
  }
  @media (max-width: 640px) {
    .map-toolbar {
      --toolbar-control-height: 40px;
      left: max(56px, calc(env(safe-area-inset-left, 0px) + 48px));
      right: max(8px, env(safe-area-inset-right, 0px));
      transform: none;
      max-width: none;
      justify-content: flex-start;
      row-gap: 8px;
    }
    .toolbar-sep:not(.auth-sep) {
      display: none;
    }
    .auth-sep {
      display: none;
    }
    .auth-block {
      max-width: min(160px, 40vw);
    }
    .auth-label {
      display: none;
    }
    .opacity-row input[type='range'] {
      width: min(120px, 28vw);
    }
    .btn-download-label {
      display: none;
    }
    .btn-download {
      min-width: 40px;
      min-height: 40px;
      padding: 10px;
      justify-content: center;
    }
    .zip-field input[type='text'] {
      font-size: 16px; /* reduces iOS zoom-on-focus */
    }
    .zip-go {
      width: 40px;
      height: 40px;
    }
  }
  .toolbar-sep {
    width: 1px;
    height: var(--toolbar-control-height);
    background: var(--panel-border);
    flex-shrink: 0;
  }
  .opacity-block {
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  .zip-block {
    display: flex;
    flex-direction: row;
    align-items: flex-end;
    gap: 3px;
  }
  .zip-field {
    display: flex;
    flex-direction: column;
    gap: 3px;
    align-items: stretch;
    width: fit-content;
  }
  .ctrl-label {
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.45px;
    color: var(--text-secondary);
    text-align: center;
  }
  .opacity-row {
    display: flex;
    align-items: center;
    gap: 7px;
    box-sizing: border-box;
    height: var(--toolbar-control-height);
    padding: 0 9px;
    background: var(--surface);
    border: 1px solid var(--panel-border);
    border-radius: 9px;
  }
  .opacity-icon {
    flex-shrink: 0;
    opacity: 0.55;
    color: var(--text-secondary);
  }
  .opacity-row input[type="range"] {
    width: 80px;
    height: 4px;
    accent-color: var(--accent);
  }
  .zip-field input[type="text"] {
    box-sizing: border-box;
    height: var(--toolbar-control-height);
    background: var(--surface);
    color: var(--text-primary);
    border: 1px solid var(--panel-border);
    border-radius: 9px;
    padding: 0 9px;
    font-size: 12px;
    line-height: 1.25;
    font-family: inherit;
    width: 58px;
    outline: none;
  }
  .zip-field input[type="text"]:focus {
    border-color: var(--accent);
  }
  .zip-go {
    box-sizing: border-box;
    flex-shrink: 0;
    height: var(--toolbar-control-height);
    width: var(--toolbar-control-height);
    background: transparent;
    border: none;
    color: var(--text-secondary);
    cursor: pointer;
    padding: 0;
    border-radius: 7px;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: color 0.15s, background 0.15s;
  }
  .zip-go:hover {
    color: var(--accent);
    background: var(--hover-bg);
  }
</style>
