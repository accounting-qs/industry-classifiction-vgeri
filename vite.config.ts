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
  // Allowlist of env vars that may reach the public browser bundle. Both are
  // public by design. Every other var (API keys, including old VITE_* names
  // still set on Render) stays server-side, even if browser code reads
  // import.meta.env dynamically.
  envPrefix: ['VITE_SUPABASE_URL', 'VITE_SUPABASE_ANON_KEY'],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './'),
    },
  },
});
