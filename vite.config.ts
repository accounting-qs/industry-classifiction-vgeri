import path from 'node:path';
import { fileURLToPath } from 'url';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default defineConfig({
  server: {
    port: 3000,
    host: '0.0.0.0',
    proxy: {
      '/api': 'http://localhost:3001'
    }
  },
  plugins: [react()],
  // Env vars that may reach the public browser bundle: none. The browser
  // talks only to this app's /api, never to Supabase, so it needs no URL or
  // key. Vite rejects an empty envPrefix, so this one matches no var we set.
  // Every var (API keys, SUPABASE_URL and the rest set on
  // Render) stays server-side, even if browser code reads import.meta.env
  // dynamically. Give a var this prefix only if it is public by design.
  envPrefix: 'PUBLIC_BROWSER_',
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './'),
    },
  },
});
