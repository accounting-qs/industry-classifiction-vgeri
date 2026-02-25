import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '../.env.local') });

const supabaseUrl = process.env.VITE_SUPABASE_URL || '';
const supabaseKey = process.env.VITE_SUPABASE_ANON_KEY || '';
const supabase = createClient(supabaseUrl, supabaseKey);

async function unlock() {
  console.log('Unlocking DB...');
  const { error } = await supabase.from('pipeline_state').upsert({ id: 1, is_processing: false });
  if (error) console.error(error);
  else console.log('Successfully Unlocked DB!');
}
unlock();
