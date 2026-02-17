import dotenv from 'dotenv';

// Load .env.local first
dotenv.config({ path: '.env.local' });

// Fallback to .env (for Render)
dotenv.config();

console.log('✅ Environment variables initialized');
