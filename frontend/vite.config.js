import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

export default defineConfig({
  plugins: [svelte()],
  server: {
    proxy: {
      '/api': 'http://localhost:8000',
      '/static': 'http://localhost:8000',
      '/data': 'http://localhost:8000',
    },
  },
  build: { outDir: 'dist' },
});
