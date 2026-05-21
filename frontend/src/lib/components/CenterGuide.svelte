<script>
  import { ui } from '../state.svelte.js';
  /**
   * Centered glass status overlay used to instruct the user mid-flow
   * (e.g. "Click a state to analyze"). Body is a snippet so callers can
   * compose rich text. Auto-repositions above the bottom dashboard panel
   * whenever a region is selected — callers don't need to wire that.
   *
   * Props:
   *   children — body snippet (the main instruction line)
   *   hint     — optional secondary line shown below the body
   */
  let { children, hint = null, pulse = false } = $props();
  const withPanel = $derived(!!ui.selectedRegion);
</script>

<div
  class="center-guide"
  class:center-guide--with-panel={withPanel}
  class:center-guide--pulsing={pulse}
  role="status"
  aria-live="polite"
>
  <div class="center-guide-text">{@render children?.()}</div>
  {#if hint}
    <div class="center-guide-hint">{hint}</div>
  {/if}
</div>

<style>
  .center-guide {
    position: fixed;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
    z-index: 25;
    pointer-events: none;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    padding: 12px 28px;
    text-align: center;
    max-width: min(560px, calc(100vw - 40px));
    white-space: nowrap;

    /* Smoked glass: deeper tint for legibility on light map areas,
       heavy blur + saturation to keep the refractive feel. */
    background: linear-gradient(
      135deg,
      rgba(8, 11, 18, 0.86) 0%,
      rgba(8, 11, 18, 0.78) 55%,
      rgba(8, 11, 18, 0.84) 100%
    );
    backdrop-filter: blur(28px) saturate(1.6);
    -webkit-backdrop-filter: blur(28px) saturate(1.6);
    border: 1px solid rgba(255, 255, 255, 0.16);
    border-radius: 18px;

    /* Outer drop + inner top-edge highlight for the lit-glass feel */
    box-shadow:
      0 20px 60px rgba(0, 0, 0, 0.55),
      0 1px 0 rgba(255, 255, 255, 0.22) inset,
      0 -1px 0 rgba(255, 255, 255, 0.04) inset;

    /* Match Panel's slideUp animation so the guide tracks the rising
       dashboard with constant clearance during the transition. */
    transition: top 0.4s cubic-bezier(0.16, 1, 0.3, 1);
  }
  .center-guide--pulsing {
    animation: center-guide-pulse 2.6s ease-in-out infinite;
  }
  .center-guide--with-panel {
    top: min(40vh, calc(100vh - 52vh - 56px));
  }
  .center-guide-text {
    font-size: 16px;
    font-weight: 600;
    letter-spacing: 0.01em;
    line-height: 1.4;
    color: #fff;
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.65);
  }
  .center-guide-hint {
    font-size: 13px;
    font-weight: 500;
    line-height: 1.35;
    color: rgba(255, 255, 255, 0.82);
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.55);
  }
  @keyframes center-guide-pulse {
    0%, 100% {
      opacity: 0.82;
      transform: translate(-50%, -50%) scale(1);
      box-shadow:
        0 20px 60px rgba(0, 0, 0, 0.5),
        0 1px 0 rgba(255, 255, 255, 0.18) inset,
        0 -1px 0 rgba(255, 255, 255, 0.04) inset;
    }
    50% {
      opacity: 1;
      transform: translate(-50%, -50%) scale(1.015);
      box-shadow:
        0 24px 70px rgba(0, 0, 0, 0.6),
        0 1px 0 rgba(255, 255, 255, 0.36) inset,
        0 -1px 0 rgba(255, 255, 255, 0.04) inset;
    }
  }
  @media (max-width: 640px) {
    .center-guide {
      max-width: calc(100vw - 20px);
      padding: 10px 18px;
      white-space: normal;
    }
    .center-guide--with-panel {
      top: min(28vh, calc(100dvh - 58vh - 48px));
    }
  }
</style>
