import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'node:path';

const API = process.env.DASHBOARD_URL ?? 'http://127.0.0.1:4300';

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { '@shared': resolve(import.meta.dirname, '../src/shared') },
  },
  server: {
    port: 4301,
    // Shared contract types live outside web/ — one source of truth for both sides.
    fs: { allow: ['..'] },
    proxy: {
      '/api': { target: API, changeOrigin: true },
      '/events': { target: API, changeOrigin: true, ws: false },
    },
  },
  build: { outDir: 'dist', emptyOutDir: true },
});
