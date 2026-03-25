import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const faviconSvgPath = path.join(__dirname, 'public', 'favicon.svg');

function faviconIcoFallback() {
  return {
    name: 'favicon-ico-fallback',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        if (req.url === '/favicon.ico' && fs.existsSync(faviconSvgPath)) {
          res.setHeader('Content-Type', 'image/svg+xml');
          res.end(fs.readFileSync(faviconSvgPath));
          return;
        }
        next();
      });
    },
    writeBundle(options) {
      const dir = options.dir;
      if (!dir || !fs.existsSync(faviconSvgPath)) return;
      try {
        fs.copyFileSync(faviconSvgPath, path.join(dir, 'favicon.ico'));
      } catch {
        /* ignore */
      }
    },
  };
}

export default defineConfig({
  plugins: [svelte(), faviconIcoFallback()],
  server: {
    proxy: {
      '/api': 'http://localhost:8000',
      '/static': 'http://localhost:8000',
      '/data': 'http://localhost:8000',
    },
  },
  build: { outDir: 'dist' },
});
