// Pulls real per-contact / per-run costs from the DB to ground the estimate.
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
dotenv.config({ path: path.join(path.dirname(__filename), '../../.env.local') });
const supabase = createClient(process.env.VITE_SUPABASE_URL || '', process.env.SUPABASE_SERVICE_ROLE_KEY || '');

// 1. Enrichment cost per contact
const { data: enr } = await supabase
    .from('enrichments')
    .select('cost')
    .eq('status', 'completed')
    .not('cost', 'is', null)
    .order('created_at', { ascending: false })
    .limit(5000);
const enrN = enr?.length || 0;
const enrTotal = (enr || []).reduce((s, r) => s + (Number(r.cost) || 0), 0);
const avgEnrich = enrN > 0 ? enrTotal / enrN : 0;
console.log(`Enrichment (Phase 0, gpt-4.1-mini):`);
console.log(`  sample: ${enrN} completed enrichments`);
console.log(`  avg cost per contact:  $${avgEnrich.toFixed(5)}`);

// 2. Bucketing runs — pair cost with contact count from bucket_assignments
const { data: runs } = await supabase
    .from('bucketing_runs')
    .select('id, cost_usd, list_names, created_at, status')
    .eq('status', 'completed')
    .not('cost_usd', 'is', null)
    .order('created_at', { ascending: false })
    .limit(10);

console.log('');
console.log('Bucketing (Phase 1a + 1b):');
const perContactBucketing = [];
for (const r of runs || []) {
    const { count } = await supabase
        .from('bucket_assignments')
        .select('contact_id', { count: 'exact', head: true })
        .eq('bucketing_run_id', r.id);
    const n = count || 0;
    const perContact = n > 0 ? Number(r.cost_usd) / n : 0;
    if (n > 0) perContactBucketing.push(perContact);
    console.log(`  $${Number(r.cost_usd).toFixed(2).padStart(7)}  ${String(n).padStart(7)} contacts  →  $${perContact.toFixed(5)}/contact  ${r.created_at?.slice(0, 10)}`);
}
const avgBucket = perContactBucketing.length > 0
    ? perContactBucketing.reduce((a, b) => a + b, 0) / perContactBucketing.length
    : 0;

console.log('');
console.log(`Average cost per contact (bucketing, gpt-4.1-mini era): $${avgBucket.toFixed(5)}`);

console.log('');
console.log('=== PROJECTED COSTS FOR 100,000 CONTACTS ===');
console.log('');
console.log(`Phase 0 — Enrichment (gpt-4.1-mini, unchanged):`);
console.log(`    $${(avgEnrich * 100_000).toFixed(2)}  (= $${avgEnrich.toFixed(5)}/contact × 100k)`);
console.log('');
console.log(`Phase 1a + 1b — Bucketing (pre-swap, gpt-4.1-mini):`);
console.log(`    $${(avgBucket * 100_000).toFixed(2)}  (from historical runs)`);
console.log('');
const newBucketEstimate = avgBucket * 100_000 * 3.0; // haiku ~3x mini per token
console.log(`Phase 1a + 1b — Bucketing (post-swap, claude-haiku-4-5, est. 3x):`);
console.log(`    $${newBucketEstimate.toFixed(2)}  (Phase 1b model swap from gpt-4.1-mini → claude-haiku-4-5)`);
console.log('');
console.log(`TOTAL pipeline (enrichment + bucketing, post-swap): ~$${(avgEnrich * 100_000 + newBucketEstimate).toFixed(2)}`);
console.log('');
console.log('Caveats:');
console.log(' • Excludes scraping fees (ZenRows / ScrapingBee / proxies).');
console.log(' • Bucketing cost scales with unique classifications, not contact count;');
console.log('   high-overlap lists (many duplicates) cost less.');
console.log(' • Claude system-prompt caching is in place but the Phase 1b bucket');
console.log('   reference is in the user prompt (uncached). Moving it to system');
console.log('   prompt would cut Phase 1b cost ~50%.');
