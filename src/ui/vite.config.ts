import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

// https://vite.dev/config/
export default defineConfig({
  plugins: [svelte()],
  server: {
    port: 8015,
    host: 'localhost',
    allowedHosts: ['desktop.tail74e0bd.ts.net'],
    proxy: {
      '/api': {
        target: 'http://localhost:3015',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'build',
  },
});
