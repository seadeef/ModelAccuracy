# Archived forecast UI

Forecast mode (map popup stats, lead slider + window chips in the top bar, date helpers) was removed from the active app. Restore by copying these files back under `src/` and re-wiring `App.svelte`, `MapView.svelte`, `state.svelte.js`, `api.js`, and `PeriodMenu.svelte` from git history if needed.

Files:

- `forecastDates.js` — calendar labels for forecast lead / status pill / popup
- `LeadControl.svelte` — forecast bar slider and multi-day window chips
- `TopBar.svelte` — alternate layout (not used by current `App.svelte`) that included forecast mode toggle
