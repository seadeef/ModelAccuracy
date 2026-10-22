import { ui, isArPeriod } from './state.svelte.js';

/**
 * Switch between map presentation modes. Mode is presentation-only: it adds or
 * removes the AR footprint layer/timeline and swaps instruction text. It must
 * never clear the selected region, statistic, period, active tool, episode, or
 * watershed drill state — "mode changes presentation; period changes data;
 * neither destroys the other's state."
 *
 * Exception: each mode has its own period family. Entering Explore coerces any
 * AR period back to yearly and drops the range/episode selection. Entering AR
 * mode coerces any climatology period to ar-episode but leaves range/episode
 * selection to the AR timeline control (range picker + basin-selection flow).
 */
export function transitionAnalysisMode(mode) {
  if (ui.mode === mode) return false;
  ui.mode = mode;
  if (mode === 'accuracy' && isArPeriod(ui.period)) {
    ui.period = 'yearly';
    ui.arEpisodeId = null;
    ui.arHoverEpisodeId = null;
    ui.arEpisodeNote = '';
  } else if (mode === 'atmospheric-rivers' && !isArPeriod(ui.period)) {
    ui.period = 'ar-episode';
  }
  return true;
}
