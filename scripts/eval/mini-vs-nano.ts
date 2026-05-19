// One-off A/B eval: gpt-4.1-mini vs gpt-4.1-nano on the Phase 1a tagger.
//
// Pulls 30 distinct classifications from the user's selected lead lists
// (top by volume — those drive the most contacts), runs each through
// debugTagSingleIndustry on both models, and reports per-call latency,
// per-tag-layer agreement, and aggregate cost.
//
// Usage:  npx tsx scripts/eval/mini-vs-nano.ts
//
// Override the list set with EVAL_LISTS="list1,list2" or sample size with
// EVAL_N=30. Models are fixed: gpt-4.1-mini and gpt-4.1-nano.

import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import { debugTagSingleIndustry, type BucketingCtx } from '../../services/bucketingService.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '../../.env.local') });
dotenv.config({ path: path.join(__dirname, '../../.env') });

const SUPABASE_URL = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.VITE_SUPABASE_ANON_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const MODELS = ['gpt-4.1-mini', 'gpt-4.1-nano'] as const;
const N = Number(process.env.EVAL_N || 30);
const DEFAULT_LISTS = [
    'Ampleleads, Apr 1 2026, B2B SaaS, 0 - 10 employees, US',
    'Ampleleads, Apr 1 2026, B2B SaaS, 25 - 50 employees, US',
    'Ampleleads, Apr 1 2026, B2B SaaS, 50 - 100 employees, US',
    'Ampleleads, Apr 1 2026, B2B SaaS, 100 - 250 employees, US',
];
const LISTS = (process.env.EVAL_LISTS || '').trim()
    ? process.env.EVAL_LISTS!.split(',').map(s => s.trim())
    : DEFAULT_LISTS;

const ctx: BucketingCtx = {
    log: (msg: string) => console.log(`[ctx] ${msg}`),
    progress: () => {},
    checkCancel: async () => {},
    abortSignal: undefined,
};

interface Sample {
    classification: string;
    n: number;
    sample_companies: string[];
}

async function loadSampleIndustries(n: number): Promise<Sample[]> {
    const { data, error } = await supabase.rpc('get_classification_vocabulary', {
        p_list_names: LISTS,
        p_limit: n,
    });
    if (error) throw new Error(`vocab RPC failed: ${error.message}`);
    const rows = (Array.isArray(data) ? data : []) as Array<{
        classification: string; n: number; sample_companies: string[] | null;
    }>;
    // Filter out failed/scrape-error placeholders that aren't real industries.
    const real = rows.filter(r => {
        const lower = (r.classification || '').toLowerCase();
        return r.classification && !lower.startsWith('scrape error') && !lower.startsWith('site error') && lower !== 'unknown';
    });
    return real.slice(0, n).map(r => ({
        classification: r.classification,
        n: Number(r.n) || 0,
        sample_companies: r.sample_companies || [],
    }));
}

interface TagResult {
    model: string;
    industry: string;
    identity: string | null;
    sub_identity: string | null;
    sector: string | null;
    is_disqualified: boolean;
    confidence: number;
    latency_ms: number;
    cost_usd: number;
    error: string | null;
}

async function tagOne(sample: Sample, model: string): Promise<TagResult> {
    const t0 = Date.now();
    try {
        const res = await debugTagSingleIndustry(
            supabase,
            sample.classification,
            sample.sample_companies,
            model,
            ctx,
        );
        const p = res.parsed;
        return {
            model,
            industry: sample.classification,
            identity: p?.identity ?? null,
            sub_identity: p?.sub_identity ?? null,
            sector: p?.sector ?? null,
            is_disqualified: !!p?.is_disqualified,
            confidence: Number(p?.confidence ?? 0),
            latency_ms: Date.now() - t0,
            cost_usd: Number(res.cost_usd || 0),
            error: null,
        };
    } catch (e: any) {
        return {
            model,
            industry: sample.classification,
            identity: null,
            sub_identity: null,
            sector: null,
            is_disqualified: false,
            confidence: 0,
            latency_ms: Date.now() - t0,
            cost_usd: 0,
            error: e?.message || String(e),
        };
    }
}

function fmtVal(v: string | null): string {
    if (v === null || v === '') return '\x1b[2m—\x1b[0m';
    return v;
}

async function main() {
    console.log(`Loading ${N} sample industries from ${LISTS.length} list(s)…`);
    const samples = await loadSampleIndustries(N);
    console.log(`Got ${samples.length} samples.\n`);
    if (samples.length < 1) {
        console.error('No samples — aborting.');
        process.exit(1);
    }

    // Run sequentially per industry, both models in parallel for that
    // industry. Keeps total wall time low without overlapping models'
    // requests on the same input (cleaner per-input timing).
    const results: Record<string, TagResult[]> = {};
    for (const m of MODELS) results[m] = [];
    const startedAt = Date.now();
    for (let i = 0; i < samples.length; i++) {
        const s = samples[i];
        const [a, b] = await Promise.all(MODELS.map(m => tagOne(s, m)));
        results[MODELS[0]].push(a);
        results[MODELS[1]].push(b);
        const idMatch = a.identity === b.identity ? '✓' : '✗';
        const subMatch = a.sub_identity === b.sub_identity ? '✓' : '✗';
        const secMatch = a.sector === b.sector ? '✓' : '✗';
        process.stdout.write(
            `[${String(i + 1).padStart(2)}/${samples.length}] ` +
            `${a.latency_ms.toString().padStart(5)}ms / ${b.latency_ms.toString().padStart(5)}ms  ` +
            `id ${idMatch}  sub ${subMatch}  sec ${secMatch}  ` +
            `${s.classification.slice(0, 80)}\n`
        );
    }
    const elapsedMs = Date.now() - startedAt;
    console.log();

    // Aggregate
    const sum = (arr: number[]) => arr.reduce((a, b) => a + b, 0);
    const median = (arr: number[]) => {
        const s = [...arr].sort((a, b) => a - b);
        return s.length ? s[Math.floor(s.length / 2)] : 0;
    };
    const max = (arr: number[]) => arr.length ? Math.max(...arr) : 0;

    function modelStats(m: string) {
        const r = results[m].filter(x => !x.error);
        const errs = results[m].filter(x => x.error);
        return {
            n: results[m].length,
            errors: errs.length,
            sumLat: sum(r.map(x => x.latency_ms)),
            medLat: median(r.map(x => x.latency_ms)),
            maxLat: max(r.map(x => x.latency_ms)),
            sumCost: sum(r.map(x => x.cost_usd)),
            withIdentity: r.filter(x => !!x.identity).length,
            withSub: r.filter(x => !!x.sub_identity).length,
            withSector: r.filter(x => !!x.sector).length,
            withDQ: r.filter(x => x.is_disqualified).length,
        };
    }

    const mini = modelStats(MODELS[0]);
    const nano = modelStats(MODELS[1]);
    const n = samples.length;

    // Agreement
    let identityMatch = 0, subMatch = 0, sectorMatch = 0;
    let bothNullSub = 0, bothNullSec = 0;
    for (let i = 0; i < n; i++) {
        const a = results[MODELS[0]][i];
        const b = results[MODELS[1]][i];
        if (a.identity === b.identity) identityMatch++;
        if (a.sub_identity === b.sub_identity) subMatch++;
        if (a.sector === b.sector) sectorMatch++;
        if (a.sub_identity === null && b.sub_identity === null) bothNullSub++;
        if (a.sector === null && b.sector === null) bothNullSec++;
    }

    const pct = (x: number) => `${((x / n) * 100).toFixed(1)}%`;
    const lat = (ms: number) => `${(ms / 1000).toFixed(2)}s`;
    const dollar = (x: number) => `$${x.toFixed(4)}`;

    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`PHASE 1A TAGGER — ${MODELS[0]} vs ${MODELS[1]} — n=${n}`);
    console.log('═══════════════════════════════════════════════════════════════');
    console.log();
    console.log('LATENCY');
    console.log(`  ${MODELS[0]}: median ${lat(mini.medLat)} · max ${lat(mini.maxLat)} · total ${lat(mini.sumLat)}`);
    console.log(`  ${MODELS[1]}: median ${lat(nano.medLat)} · max ${lat(nano.maxLat)} · total ${lat(nano.sumLat)}`);
    const speedup = mini.medLat / Math.max(1, nano.medLat);
    console.log(`  → nano is ${speedup.toFixed(2)}× ${speedup > 1 ? 'faster' : 'slower'} (median)`);
    console.log();
    console.log('COST');
    console.log(`  ${MODELS[0]}: ${dollar(mini.sumCost)} total · ${dollar(mini.sumCost / n)}/industry`);
    console.log(`  ${MODELS[1]}: ${dollar(nano.sumCost)} total · ${dollar(nano.sumCost / n)}/industry`);
    if (mini.sumCost > 0) {
        console.log(`  → nano costs ${((nano.sumCost / mini.sumCost) * 100).toFixed(0)}% of mini`);
    }
    console.log();
    console.log('TAG COVERAGE (how often each layer was populated)');
    console.log(`               identity   sub-identity   sector`);
    console.log(`  ${MODELS[0]}:  ${pct(mini.withIdentity).padStart(7)}    ${pct(mini.withSub).padStart(7)}     ${pct(mini.withSector).padStart(7)}`);
    console.log(`  ${MODELS[1]}:  ${pct(nano.withIdentity).padStart(7)}    ${pct(nano.withSub).padStart(7)}     ${pct(nano.withSector).padStart(7)}`);
    console.log();
    console.log('AGREEMENT (same answer for the same industry)');
    console.log(`  identity      : ${pct(identityMatch)} (${identityMatch}/${n})`);
    console.log(`  sub_identity  : ${pct(subMatch)} (${subMatch}/${n})  [both-null: ${bothNullSub}]`);
    console.log(`  sector        : ${pct(sectorMatch)} (${sectorMatch}/${n})  [both-null: ${bothNullSec}]`);
    console.log();
    console.log('ERRORS');
    console.log(`  ${MODELS[0]}: ${mini.errors}`);
    console.log(`  ${MODELS[1]}: ${nano.errors}`);
    console.log();
    console.log('PER-INDUSTRY DISAGREEMENTS');
    let shown = 0;
    for (let i = 0; i < n; i++) {
        const a = results[MODELS[0]][i];
        const b = results[MODELS[1]][i];
        if (a.identity === b.identity && a.sub_identity === b.sub_identity && a.sector === b.sector) continue;
        shown++;
        if (shown > 15) {
            console.log(`  … and ${n - i - 1} more`);
            break;
        }
        console.log(`  ${(i + 1).toString().padStart(2)}. "${a.industry.slice(0, 70)}"`);
        console.log(`      mini: ${fmtVal(a.identity)} / ${fmtVal(a.sub_identity)} / ${fmtVal(a.sector)}`);
        console.log(`      nano: ${fmtVal(b.identity)} / ${fmtVal(b.sub_identity)} / ${fmtVal(b.sector)}`);
    }
    if (shown === 0) console.log(`  (none — full agreement)`);
    console.log();
    console.log(`Wall time: ${(elapsedMs / 1000).toFixed(1)}s`);
}

main().catch(e => { console.error(e); process.exit(1); });
