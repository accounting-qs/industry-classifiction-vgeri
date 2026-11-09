/// <reference types="vite/client" />

// Browser code reads no env vars (see envPrefix in vite.config.ts). If one
// is ever needed, it must be public by design: declare it here and read it
// with static property access so Vite inlines just that value.
interface ImportMetaEnv {}
