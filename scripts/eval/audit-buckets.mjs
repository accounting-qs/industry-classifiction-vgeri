// Audits bucket_library for cleanup: lists orphans (no primary_identity)
// and near-duplicate names. Read-only — emits a report; no writes.
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
dotenv.config({ path: path.join(path.dirname(__filename), '../../.env.local') });
const supabase = createClient(process.env.VITE_SUPABASE_URL || '', process.env.SUPABASE_SERVICE_ROLE_KEY || '');

const { data: rows, error } = await supabase
    .from('bucket_library')
    .select('id, bucket_name, primary_identity, description, is_canonical, created_by, times_used, last_used_at')
    .eq('archived', false)
    .order('bucket_name');
if (error) { console.error(error); process.exit(1); }
const buckets = rows || [];

console.log(`Total active buckets: ${buckets.length}\n`);

// ── 1. Orphans (no primary_identity) ────────────────────────────────
const RESERVED = new Set(['General', 'Disqualified']);
const orphans = buckets.filter(b =>
    (!b.primary_identity || b.primary_identity.trim() === '') &&
    !RESERVED.has(b.bucket_name)
);
console.log(`═══ ORPHAN BUCKETS (no primary_identity) — ${orphans.length} ═══`);
if (orphans.length === 0) console.log('  (none)');
for (const b of orphans) {
    console.log(`  ${b.bucket_name.padEnd(55)}  ${b.is_canonical ? 'CANON' : 'user '}  by=${b.created_by || '?'}  used=${b.times_used || 0}`);
    if (b.description) console.log(`    "${(b.description || '').slice(0, 90)}"`);
}
console.log();

// ── 2. Near-duplicate detection (token similarity ≥ 0.5) ────────────
function tokens(s) {
    return (s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().split(' ').filter(t => t.length >= 3);
}
function similarity(a, b) {
    const ta = tokens(a);
    const tb = tokens(b);
    if (!ta.length || !tb.length) return 0;
    const sb = new Set(tb);
    let inter = 0;
    for (const t of ta) if (sb.has(t)) inter++;
    return inter / Math.max(ta.length, tb.length);
}

const pairs = [];
for (let i = 0; i < buckets.length; i++) {
    for (let j = i + 1; j < buckets.length; j++) {
        const s = similarity(buckets[i].bucket_name, buckets[j].bucket_name);
        if (s >= 0.5) pairs.push({ a: buckets[i], b: buckets[j], sim: s });
    }
}
pairs.sort((x, y) => y.sim - x.sim);

console.log(`═══ NEAR-DUPLICATE BUCKET NAME PAIRS (sim ≥ 0.5) — ${pairs.length} ═══`);
if (pairs.length === 0) console.log('  (none)');
for (const p of pairs) {
    const sameId = (p.a.primary_identity || '') === (p.b.primary_identity || '');
    const idMark = sameId ? '★ same identity' : '  diff identity';
    console.log(`  sim=${p.sim.toFixed(2)}  ${idMark}`);
    console.log(`    A: "${p.a.bucket_name}" → ${p.a.primary_identity || 'NULL'}  (${p.a.is_canonical ? 'CANON' : 'user'}, used=${p.a.times_used || 0})`);
    console.log(`    B: "${p.b.bucket_name}" → ${p.b.primary_identity || 'NULL'}  (${p.b.is_canonical ? 'CANON' : 'user'}, used=${p.b.times_used || 0})`);
    console.log();
}

// ── 3. Mismatch flags (bucket primary_identity not in live taxonomy) ──
const { data: idRows } = await supabase
    .from('taxonomy_identities')
    .select('name')
    .eq('archived', false);
const validIdentities = new Set((idRows || []).map(r => r.name));
const mismatch = buckets.filter(b =>
    b.primary_identity && !validIdentities.has(b.primary_identity)
);
console.log(`═══ BUCKETS WITH primary_identity NOT IN LIVE TAXONOMY — ${mismatch.length} ═══`);
if (mismatch.length === 0) console.log('  (none)');
for (const b of mismatch) {
    console.log(`  ${b.bucket_name.padEnd(55)}  → ${b.primary_identity}`);
}
