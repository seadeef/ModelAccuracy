import App from './App.svelte';
import { mount } from 'svelte';

const target = document.getElementById('app');
if (target) {
  mount(App, { target });
}

// Dev-only hooks for capture/automation tooling (never bundled in production).
if (import.meta.env.DEV) {
  Promise.all([import('./lib/state.svelte.js'), import('./lib/hucSelection.js')]).then(([state, huc]) => {
    window.__rcUi = state.ui;
    window.__rcSelectHuc = async (code) => {
      const region = await huc.resolveHucRegion(code);
      state.ui.selectedRegion = region;
      state.ui.hasUsedAreaDrawTool = true;
      const b = region.bounds;
      if (window.__rcMap && Array.isArray(b) && b.every(Number.isFinite)) {
        window.__rcMap.fitBounds([[b[0], b[1]], [b[2], b[3]]], { padding: 40, maxZoom: 9, duration: 0 });
      }
      return region;
    };
  });
}
