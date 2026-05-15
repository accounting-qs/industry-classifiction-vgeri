import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import { debugTagSingleIndustry, BucketingCancelledError, type BucketingCtx } from '../../services/bucketingService.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '../../.env.local') });
dotenv.config({ path: path.join(__dirname, '../../.env') });

// Default to gpt-4.1-mini to mirror Phase 1b's production model (and what
// you'd hit if you pick gpt-4.1-mini in the Bucketing UI). Override via
// EVAL_MODEL=claude-haiku-4-5 to A/B against the recommended Claude option.
const MODEL = process.env.EVAL_MODEL || 'gpt-4.1-mini';
const CONCURRENCY = Number(process.env.EVAL_CONCURRENCY || 6);
const GOLDENS_PATH = path.join(__dirname, 'goldens.json');
const LAST_RUN_PATH = path.join(__dirname, 'last-run.json');
const BASELINE_PATH = path.join(__dirname, 'baseline.json');

interface Expected {
    primary_identity: string | null;
    sub_identity: string | null;
    sector: string | null;
    is_disqualified: boolean;
}

interface GoldenCase {
    id: string;
    category: string;
    input: string;
    expected: Expected;
    notes?: string;
    allow_identity_only?: boolean;
}

interface CaseResult {
    id: string;
    category: string;
    input: string;
    expected: Expected;
    actual: {
        primary_identity: string | null;
        sub_identity: string | null;
        sector: string | null;
        is_disqualified: boolean;
        identity_confidence: number | null;
        sub_identity_confidence: number | null;
        sector_confidence: number | null;
    };
    field_pass: {
        primary_identity: boolean;
        sub_identity: boolean;
        sector: boolean;
        is_disqualified: boolean;
    };
    overall_pass: boolean;
    cost_usd: number;
    raw_response?: string;
    error?: string;
}

const supabaseUrl = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL || '';
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.VITE_SUPABASE_ANON_KEY || '';

if (!supabaseUrl || !supabaseKey) {
    console.error('Missing VITE_SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in .env.local');
    process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey, { auth: { persistSession: false } });

function makeCtx(): BucketingCtx {
    const ac = new AbortController();
    return {
        log: () => {},
        progress: () => {},
        checkCancel: async () => {},
        abortSignal: ac.signal,
    };
}

function normalize(value: string | null | undefined): string | null {
    if (value === null || value === undefined) return null;
    const trimmed = String(value).trim();
    return trimmed.length === 0 ? null : trimmed;
}

function fieldsMatch(
    expected: string | null,
    actual: string | null,
    confidence: number | null,
    allowIdentityOnly: boolean,
    isSubOrSector: boolean,
): boolean {
    const e = normalize(expected);
    const a = normalize(actual);
    if (e === a) return true;
    if (e === null && allowIdentityOnly && isSubOrSector) return true;
    // Treat low-confidence claims (<6/10) on null-expected fields as acceptable
    // — the LLM is being honest about uncertainty.
    if (e === null && a !== null && confidence !== null && confidence < 6) return true;
    return false;
}

async function runOne(c: GoldenCase): Promise<CaseResult> {
    try {
        const { parsed, cost_usd, raw_response } = await debugTagSingleIndustry(
            supabase,
            c.input,
            [],
            MODEL,
            makeCtx(),
        );
        const actual = {
            primary_identity: normalize(parsed?.identity ?? null),
            sub_identity: normalize(parsed?.sub_identity ?? null),
            sector: normalize(parsed?.sector ?? null),
            is_disqualified: Boolean(parsed?.is_disqualified),
            identity_confidence: parsed?.identity_confidence ?? null,
            sub_identity_confidence: parsed?.sub_identity_confidence ?? null,
            sector_confidence: parsed?.sector_confidence ?? null,
        };
        const allowIdentityOnly = Boolean(c.allow_identity_only);
        const field_pass = {
            primary_identity: fieldsMatch(c.expected.primary_identity, actual.primary_identity, actual.identity_confidence, allowIdentityOnly, false),
            sub_identity: fieldsMatch(c.expected.sub_identity, actual.sub_identity, actual.sub_identity_confidence, allowIdentityOnly, true),
            sector: fieldsMatch(c.expected.sector, actual.sector, actual.sector_confidence, allowIdentityOnly, true),
            is_disqualified: c.expected.is_disqualified === actual.is_disqualified,
        };
        const overall_pass = Object.values(field_pass).every(Boolean);
        return {
            id: c.id, category: c.category, input: c.input,
            expected: c.expected, actual, field_pass, overall_pass,
            cost_usd, raw_response,
        };
    } catch (err: any) {
        return {
            id: c.id, category: c.category, input: c.input,
            expected: c.expected,
            actual: {
                primary_identity: null, sub_identity: null, sector: null,
                is_disqualified: false, identity_confidence: null,
                sub_identity_confidence: null, sector_confidence: null,
            },
            field_pass: { primary_identity: false, sub_identity: false, sector: false, is_disqualified: false },
            overall_pass: false,
            cost_usd: 0,
            error: err?.message || String(err),
        };
    }
}

function pct(num: number, denom: number): string {
    if (denom === 0) return '0.0%';
    return `${((num / denom) * 100).toFixed(1)}%`;
}

function pad(s: string, n: number): string {
    return s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length);
}

async function main() {
    const raw = fs.readFileSync(GOLDENS_PATH, 'utf-8');
    const file = JSON.parse(raw) as { cases: GoldenCase[] };
    const cases = file.cases;
    console.log(`Loaded ${cases.length} goldens from ${GOLDENS_PATH}`);
    console.log(`Model: ${MODEL}  |  Concurrency: ${CONCURRENCY}`);
    console.log('');

    const limit = pLimit(CONCURRENCY);
    const t0 = Date.now();
    const results = await Promise.all(cases.map(c => limit(() => runOne(c))));
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);

    const totalCost = results.reduce((s, r) => s + r.cost_usd, 0);

    const fieldTotals = {
        primary_identity: results.filter(r => r.field_pass.primary_identity).length,
        sub_identity: results.filter(r => r.field_pass.sub_identity).length,
        sector: results.filter(r => r.field_pass.sector).length,
        is_disqualified: results.filter(r => r.field_pass.is_disqualified).length,
    };
    const overallPass = results.filter(r => r.overall_pass).length;

    console.log('=== ACCURACY ===');
    console.log(`primary_identity   ${pct(fieldTotals.primary_identity, results.length)}  (${fieldTotals.primary_identity}/${results.length})`);
    console.log(`sub_identity       ${pct(fieldTotals.sub_identity, results.length)}  (${fieldTotals.sub_identity}/${results.length})`);
    console.log(`sector             ${pct(fieldTotals.sector, results.length)}  (${fieldTotals.sector}/${results.length})`);
    console.log(`is_disqualified    ${pct(fieldTotals.is_disqualified, results.length)}  (${fieldTotals.is_disqualified}/${results.length})`);
    console.log(`overall            ${pct(overallPass, results.length)}  (${overallPass}/${results.length})`);

    const categories = Array.from(new Set(results.map(r => r.category)));
    console.log('');
    console.log('=== BY CATEGORY ===');
    for (const cat of categories) {
        const inCat = results.filter(r => r.category === cat);
        const pass = inCat.filter(r => r.overall_pass).length;
        console.log(`  ${pad(cat, 22)}  ${pct(pass, inCat.length)}  (${pass}/${inCat.length})`);
    }

    const failures = results.filter(r => !r.overall_pass);
    if (failures.length > 0) {
        console.log('');
        console.log('=== FAILURES ===');
        for (const r of failures) {
            console.log(`\n[${r.id}] ${r.category}  →  ${r.input}`);
            if (r.error) { console.log(`  ERROR: ${r.error}`); continue; }
            const wrong = (Object.keys(r.field_pass) as Array<keyof typeof r.field_pass>).filter(k => !r.field_pass[k]);
            for (const k of wrong) {
                const exp = (r.expected as any)[k];
                const got = (r.actual as any)[k];
                console.log(`  ${pad(String(k), 22)}  expected=${JSON.stringify(exp)}  got=${JSON.stringify(got)}`);
            }
        }
    }

    console.log('');
    console.log(`Cost: $${totalCost.toFixed(4)}  |  Time: ${elapsed}s`);

    const lastRun = {
        run_at: new Date().toISOString(),
        model: MODEL,
        totals: { ...fieldTotals, overall: overallPass, n: results.length },
        cost_usd: totalCost,
        elapsed_seconds: Number(elapsed),
        results,
    };
    fs.writeFileSync(LAST_RUN_PATH, JSON.stringify(lastRun, null, 2));
    console.log(`Wrote ${LAST_RUN_PATH}`);

    if (fs.existsSync(BASELINE_PATH)) {
        const baseline = JSON.parse(fs.readFileSync(BASELINE_PATH, 'utf-8'));
        const baselineMap = new Map<string, boolean>(
            (baseline.results || []).map((r: any) => [r.id, r.overall_pass])
        );
        const regressions = results.filter(r => baselineMap.get(r.id) === true && !r.overall_pass);
        const improvements = results.filter(r => baselineMap.get(r.id) === false && r.overall_pass);
        console.log('');
        console.log(`=== DELTA vs baseline.json ===`);
        console.log(`  improvements: ${improvements.length}  (${improvements.map(r => r.id).join(', ') || '-'})`);
        console.log(`  regressions:  ${regressions.length}  (${regressions.map(r => r.id).join(', ') || '-'})`);
    } else {
        console.log('');
        console.log(`No baseline.json yet. To set this run as baseline:  cp ${LAST_RUN_PATH} ${BASELINE_PATH}`);
    }

    process.exit(0);
}

main().catch(err => {
    if (err instanceof BucketingCancelledError) { console.error('Cancelled'); process.exit(2); }
    console.error(err);
    process.exit(1);
});
