<script>
  /**
   * Thin Chart.js host: pass a full config (type/data/options/plugins) and the
   * chart is created once and updated in place (no animation) whenever the
   * config object changes. Resizes with its container.
   */
  import { onMount, onDestroy } from 'svelte';
  import { Chart } from './chartSetup.js';

  let { config, onhover = null } = $props();
  let canvas;
  let chart = null;

  function build() {
    if (!canvas) return;
    chart?.destroy();
    chart = new Chart(canvas, {
      type: config.type,
      data: config.data,
      options: {
        ...config.options,
        onHover: onhover ? (evt, elements, ch) => onhover(evt, elements, ch) : config.options?.onHover,
      },
      plugins: config.plugins || [],
    });
  }

  onMount(build);
  onDestroy(() => { chart?.destroy(); chart = null; });

  $effect(() => {
    const c = config;
    if (!chart || !c) return;
    if (c.type !== chart.config.type || (c.plugins || []).length !== (chart.config.plugins || []).length) {
      build();
      return;
    }
    chart.data = c.data;
    chart.options = { ...c.options, onHover: chart.options.onHover };
    chart.update('none');
  });
</script>

<div class="chart-host"><canvas bind:this={canvas}></canvas></div>

<style>
  .chart-host { position: absolute; inset: 0; }
  canvas { width: 100% !important; height: 100% !important; }
</style>
