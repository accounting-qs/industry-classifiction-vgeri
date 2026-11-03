/// <reference types="vite/client" />

// The only env vars browser code may read (see envPrefix in vite.config.ts).
// Read them with static property access so Vite inlines just that value.
interface ImportMetaEnv {
  readonly VITE_SUPABASE_URL?: string;
  readonly VITE_SUPABASE_ANON_KEY?: string;
}
