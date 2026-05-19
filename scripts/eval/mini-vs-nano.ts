// One-off A/B eval: gpt-4.1-mini vs gpt-4.1-nano on the Phase 1a tagger,
// with Claude Sonnet 4.6 as the judge.
//
// 1. Pulls N distinct classifications from the user's selected lead lists
//    (top by volume).
// 2. Tags each with both gpt-4.1-mini and gpt-4.1-nano via the real
//    debugTagSingleIndustry path (same prompt + library snapshot as Phase 1a).
// 3. For each industry, sends both candidates to claude-sonnet-4-6 with the
//    library and asks the judge to mark each tag (identity, sub_identity,
//    sector) as correct / incorrect / partial-credit for each model.
// 4. Reports per-model accuracy per layer, weighted by user-stated priority
//    (identity > sub_identity > sector).
//
// Usage:  npx tsx scripts/eval/mini-vs-nano.ts
//
// EVAL_N overrides the sample count (default 50). EVAL_LISTS overrides the
// lead-list set (comma-separated).

import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';
import {
    debugTagSingleIndustry,
    loadTaxonomySnapshotForDebug,
    type BucketingCtx,
} from '../../services/bucketingService.js';

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

// Anthropic key: prefer the one stored in Supabase app_settings (same place
// the runtime tagger pulls from) so we don't need a duplicate env var.
async function loadAnthropicClient(): Promise<Anthropic> {
    const { data } = await supabase.from('app_settings').select('value').eq('key', 'ANTHROPIC_API_KEY').maybeSingle();
    const stored = (data?.value || '').trim();
    const key = stored || (process.env.ANTHROPIC_API_KEY || process.env.VITE_ANTHROPIC_API_KEY || '').trim();
    if (!key) {
        console.error('No Anthropic key found in app_settings or env. Set one via the Connectors page or export ANTHROPIC_API_KEY.');
        process.exit(1);
    }
    return new Anthropic({ apiKey: key });
}
let anthropic: Anthropic;

const CANDIDATE_MODELS = ['gpt-4.1-mini', 'gpt-4.1-nano'] as const;
type CandidateModel = typeof CANDIDATE_MODELS[number];
const JUDGE_MODEL = 'claude-sonnet-4-6';
const N = Number(process.env.EVAL_N || 50);
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
    log: () => {},
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
        p_limit: n * 3,    // grab extra so we can filter junk
    });
    if (error) throw new Error(`vocab RPC failed: ${error.message}`);
    const rows = (Array.isArray(data) ? data : []) as Array<{
        classification: string; n: number; sample_companies: string[] | null;
    }>;
    const real = rows.filter(r => {
        const lower = (r.classification || '').toLowerCase();
        if (!r.classification) return false;
        return !lower.startsWith('scrape error')
            && !lower.startsWith('site error')
            && lower !== 'unknown'
            && lower !== 'n/a'
            && lower !== 'error';
    });
    return real.slice(0, n).map(r => ({
        classification: r.classification,
        n: Number(r.n) || 0,
        sample_companies: r.sample_companies || [],
    }));
}

interface TagResult {
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
            identity: null, sub_identity: null, sector: null,
            is_disqualified: false, confidence: 0,
            latency_ms: Date.now() - t0, cost_usd: 0,
            error: e?.message || String(e),
        };
    }
}

interface JudgeResult {
    mini: { identity: 'correct' | 'wrong' | 'acceptable_null';
            sub_identity: 'correct' | 'wrong' | 'acceptable_null';
            sector: 'correct' | 'wrong' | 'acceptable_null'; };
    nano: { identity: 'correct' | 'wrong' | 'acceptable_null';
            sub_identity: 'correct' | 'wrong' | 'acceptable_null';
            sector: 'correct' | 'wrong' | 'acceptable_null'; };
    notes: string;
    raw: string;
}

function buildJudgePrompt(
    sample: Sample,
    mini: TagResult,
    nano: TagResult,
    libraryBlock: string,
): { system: string; user: string } {
    const system = `You are evaluating taxonomy taggings produced by two LLMs for the same industry text. Your job is to mark each (model × layer) cell as one of:

- "correct"          — the answer matches a valid library entry AND fits the industry.
- "wrong"            — the answer is in the library but doesn't fit the industry, OR the answer is null when a confident commit was warranted, OR the answer hallucinates (not in library), OR is internally inconsistent (sub-identity's parent doesn't match identity).
- "acceptable_null"  — null was the right call (ambiguous industry, no clear answer, prompt rule "if uncertain leave null and route to General").

LAYERS:
- identity (REQUIRED when applicable) — what kind of company is it at its core.
- sub_identity — the functional sub-type within identity.
- sector — the vertical the company SERVES (only if explicitly stated). Sector is independent of identity. "Marketing agency for healthcare" → identity=Agency, sector=Healthcare.

LIBRARY (the only valid VERBATIM values, per layer):
${libraryBlock}

RULES:
- Identity MUST be from VALID_IDENTITIES. Sub-identity MUST be from VALID_SUB_IDENTITIES and its parent MUST match the chosen identity. Sector MUST be from VALID_SECTORS or null.
- Sector is OPTIONAL. Most industries have no served vertical — "acceptable_null" is the right judgement when no vertical is mentioned. Only mark sector "wrong" if the industry CLEARLY serves a vertical and the model failed to set it (or set it incorrectly).
- If the industry text is a scrape error / unknown / placeholder, all layers should be null → "acceptable_null".
- Be strict on identity. A model committing the wrong identity is a meaningful error.

OUTPUT — strict JSON only, no prose:
{
  "mini": { "identity": "correct" | "wrong" | "acceptable_null",
            "sub_identity": "correct" | "wrong" | "acceptable_null",
            "sector": "correct" | "wrong" | "acceptable_null" },
  "nano": { "identity": "correct" | "wrong" | "acceptable_null",
            "sub_identity": "correct" | "wrong" | "acceptable_null",
            "sector": "correct" | "wrong" | "acceptable_null" },
  "notes": "<one short sentence justifying any 'wrong' verdicts>"
}`;

    const user = JSON.stringify({
        industry: sample.classification,
        sample_companies: sample.sample_companies.slice(0, 2),
        candidates: {
            'gpt-4.1-mini': {
                identity: mini.identity,
                sub_identity: mini.sub_identity,
                sector: mini.sector,
                is_disqualified: mini.is_disqualified,
            },
            'gpt-4.1-nano': {
                identity: nano.identity,
                sub_identity: nano.sub_identity,
                sector: nano.sector,
                is_disqualified: nano.is_disqualified,
            }
        }
    }, null, 2);

    return { system, user };
}

async function judge(sample: Sample, mini: TagResult, nano: TagResult, libraryBlock: string): Promise<JudgeResult> {
    const { system, user } = buildJudgePrompt(sample, mini, nano, libraryBlock);
    const resp = await anthropic.messages.create({
        model: JUDGE_MODEL,
        max_tokens: 1500,
        system,
        messages: [{ role: 'user', content: user }],
    });
    const text = (resp.content as any[]).filter(b => b.type === 'text').map(b => b.text).join('\n').trim();
    // Strip markdown fences if present
    const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
    const cleaned = (fence ? fence[1] : text).trim();
    let parsed: any;
    try { parsed = JSON.parse(cleaned); }
    catch (e: any) {
        return {
            mini: { identity: 'wrong', sub_identity: 'wrong', sector: 'wrong' },
            nano: { identity: 'wrong', sub_identity: 'wrong', sector: 'wrong' },
            notes: `JUDGE_PARSE_ERROR: ${text.slice(0, 200)}`,
            raw: text,
        };
    }
    return {
        mini: parsed.mini || { identity: 'wrong', sub_identity: 'wrong', sector: 'wrong' },
        nano: parsed.nano || { identity: 'wrong', sub_identity: 'wrong', sector: 'wrong' },
        notes: String(parsed.notes || ''),
        raw: text,
    };
}

function buildLibraryBlock(snap: { identities: any[]; sub_identities: any[]; sectors: any[] }): string {
    const identityNames = snap.identities.map(i => i.name);
    const sectorNames = snap.sectors.map(s => s.name);
    const subByParent: Record<string, string[]> = {};
    for (const s of snap.sub_identities) {
        const parent = s.parent_identity || '(unknown)';
        if (!subByParent[parent]) subByParent[parent] = [];
        subByParent[parent].push(s.name);
    }
    const subLines = Object.entries(subByParent)
        .map(([p, names]) => `  ${p}: [${names.map(n => `"${n}"`).join(', ')}]`)
        .join('\n');
    return `VALID_IDENTITIES = [${identityNames.map(n => `"${n}"`).join(', ')}]\n\nVALID_SECTORS = [${sectorNames.map(n => `"${n}"`).join(', ')}]\n\nVALID_SUB_IDENTITIES (grouped by parent identity):\n${subLines}`;
}

async function main() {
    anthropic = await loadAnthropicClient();
    console.log(`Loading ${N} sample industries from ${LISTS.length} list(s)…`);
    const samples = await loadSampleIndustries(N);
    console.log(`Got ${samples.length} samples.\n`);
    if (samples.length === 0) { console.error('No samples — aborting.'); process.exit(1); }

    console.log('Loading taxonomy snapshot for the judge prompt…');
    const snap = await loadTaxonomySnapshotForDebug(supabase);
    const libraryBlock = buildLibraryBlock(snap);
    console.log(`Library: ${snap.identities.length} identities, ${snap.sub_identities.length} sub-identities, ${snap.sectors.length} sectors.\n`);

    interface Row {
        sample: Sample;
        mini: TagResult;
        nano: TagResult;
        verdict: JudgeResult;
    }
    const rows: Row[] = [];

    const startedAt = Date.now();
    for (let i = 0; i < samples.length; i++) {
        const s = samples[i];
        const [mini, nano] = await Promise.all([
            tagOne(s, 'gpt-4.1-mini'),
            tagOne(s, 'gpt-4.1-nano'),
        ]);
        const verdict = await judge(s, mini, nano, libraryBlock);
        rows.push({ sample: s, mini, nano, verdict });

        // Inline progress
        const m = verdict.mini, na = verdict.nano;
        const tick = (v: string) => v === 'correct' || v === 'acceptable_null' ? '✓' : '✗';
        process.stdout.write(
            `[${String(i + 1).padStart(2)}/${samples.length}] ` +
            `mini: id ${tick(m.identity)} sub ${tick(m.sub_identity)} sec ${tick(m.sector)}   ` +
            `nano: id ${tick(na.identity)} sub ${tick(na.sub_identity)} sec ${tick(na.sector)}   ` +
            `${s.classification.slice(0, 60)}\n`
        );
    }
    const elapsedMs = Date.now() - startedAt;

    // Aggregate
    function countCorrect(model: CandidateModel, layer: 'identity' | 'sub_identity' | 'sector') {
        let correct = 0, wrong = 0, accNull = 0;
        for (const r of rows) {
            const v = model === 'gpt-4.1-mini' ? r.verdict.mini : r.verdict.nano;
            const cell = v[layer];
            if (cell === 'correct') correct++;
            else if (cell === 'wrong') wrong++;
            else if (cell === 'acceptable_null') accNull++;
        }
        return { correct, wrong, accNull, total: rows.length };
    }

    const layers: Array<'identity' | 'sub_identity' | 'sector'> = ['identity', 'sub_identity', 'sector'];
    const stats: Record<string, Record<string, ReturnType<typeof countCorrect>>> = {
        'gpt-4.1-mini': {} as any,
        'gpt-4.1-nano': {} as any,
    };
    for (const m of CANDIDATE_MODELS) for (const l of layers) stats[m][l] = countCorrect(m, l);

    const pct = (x: number, n: number) => n > 0 ? ((x / n) * 100).toFixed(1) + '%' : '—';
    const dollar = (x: number) => `$${x.toFixed(4)}`;
    const lat = (ms: number) => `${(ms / 1000).toFixed(2)}s`;

    // Latency / cost rollup
    const sum = (arr: number[]) => arr.reduce((a, b) => a + b, 0);
    const median = (arr: number[]) => {
        const s = [...arr].sort((a, b) => a - b);
        return s.length ? s[Math.floor(s.length / 2)] : 0;
    };
    const miniLat = rows.map(r => r.mini.latency_ms).filter(x => x > 0);
    const nanoLat = rows.map(r => r.nano.latency_ms).filter(x => x > 0);
    const miniCost = sum(rows.map(r => r.mini.cost_usd));
    const nanoCost = sum(rows.map(r => r.nano.cost_usd));

    console.log();
    console.log('═══════════════════════════════════════════════════════════════════════════');
    console.log(`PHASE 1A TAGGER — gpt-4.1-mini vs gpt-4.1-nano — judged by ${JUDGE_MODEL}`);
    console.log(`N=${rows.length} industries · judge prompt uses live taxonomy library`);
    console.log('═══════════════════════════════════════════════════════════════════════════');
    console.log();
    console.log('ACCURACY PER LAYER (correct OR acceptable-null counted as pass)');
    console.log();
    console.log('                          gpt-4.1-mini              gpt-4.1-nano');
    console.log('                    correct  wrong  acc-null   correct  wrong  acc-null');
    for (const l of layers) {
        const a = stats['gpt-4.1-mini'][l];
        const b = stats['gpt-4.1-nano'][l];
        const aPass = a.correct + a.accNull;
        const bPass = b.correct + b.accNull;
        console.log(
            `  ${l.padEnd(16)}` +
            `  ${a.correct.toString().padStart(2)}    ${a.wrong.toString().padStart(3)}    ${a.accNull.toString().padStart(3)}      ` +
            `   ${b.correct.toString().padStart(2)}    ${b.wrong.toString().padStart(3)}    ${b.accNull.toString().padStart(3)}`
        );
        console.log(
            `  ${' '.padEnd(16)}` +
            `  pass: ${aPass}/${a.total} (${pct(aPass, a.total)})            ` +
            `pass: ${bPass}/${b.total} (${pct(bPass, b.total)})`
        );
    }
    console.log();
    console.log('STRICT ACCURACY ("correct" only — acceptable-null NOT counted)');
    for (const l of layers) {
        const a = stats['gpt-4.1-mini'][l];
        const b = stats['gpt-4.1-nano'][l];
        console.log(
            `  ${l.padEnd(16)} mini=${a.correct}/${a.total} (${pct(a.correct, a.total)})      ` +
            `nano=${b.correct}/${b.total} (${pct(b.correct, b.total)})`
        );
    }
    console.log();
    console.log('WEIGHTED QUALITY SCORE (your priority: identity=3, sub_identity=2, sector=1)');
    const w = { identity: 3, sub_identity: 2, sector: 1 };
    function weightedScore(m: CandidateModel) {
        let numer = 0, denom = 0;
        for (const l of layers) {
            const s = stats[m][l];
            const pass = s.correct + s.accNull;
            numer += w[l] * pass;
            denom += w[l] * s.total;
        }
        return denom > 0 ? (numer / denom) * 100 : 0;
    }
    console.log(`  gpt-4.1-mini: ${weightedScore('gpt-4.1-mini').toFixed(1)}/100`);
    console.log(`  gpt-4.1-nano: ${weightedScore('gpt-4.1-nano').toFixed(1)}/100`);
    console.log();
    console.log('SPEED + COST');
    console.log(`  mini:  median ${lat(median(miniLat))} · total ${lat(sum(miniLat))} · cost ${dollar(miniCost)} (${dollar(miniCost/rows.length)}/industry)`);
    console.log(`  nano:  median ${lat(median(nanoLat))} · total ${lat(sum(nanoLat))} · cost ${dollar(nanoCost)} (${dollar(nanoCost/rows.length)}/industry)`);
    console.log();
    console.log(`Wall time: ${(elapsedMs / 1000).toFixed(1)}s`);
    console.log();

    console.log('SAMPLE WRONG VERDICTS (first 10)');
    let shown = 0;
    for (const r of rows) {
        const wrongs: string[] = [];
        for (const l of layers) {
            if (r.verdict.mini[l] === 'wrong') wrongs.push(`mini.${l}`);
            if (r.verdict.nano[l] === 'wrong') wrongs.push(`nano.${l}`);
        }
        if (wrongs.length === 0) continue;
        shown++;
        if (shown > 10) break;
        console.log(`  • "${r.sample.classification.slice(0, 70)}"`);
        console.log(`    mini: ${r.mini.identity ?? '—'} / ${r.mini.sub_identity ?? '—'} / ${r.mini.sector ?? '—'}`);
        console.log(`    nano: ${r.nano.identity ?? '—'} / ${r.nano.sub_identity ?? '—'} / ${r.nano.sector ?? '—'}`);
        console.log(`    wrong: ${wrongs.join(', ')} — ${r.verdict.notes.slice(0, 180)}`);
    }
}

main().catch(e => { console.error(e); process.exit(1); });
