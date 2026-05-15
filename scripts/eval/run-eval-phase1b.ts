// Smoke / accuracy test for Phase 1b (per-contact routing). Feeds the same
// goldens through classifyContactBatch using the live taxonomy as leaves.
// Validates: (1) Anthropic call path works, (2) shared prompt rules fire,
// (3) snap-to-library catches paraphrases, (4) JSON parsing handles the
// Claude response shape.
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import Anthropic from '@anthropic-ai/sdk';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '../../.env.local') });

import { evalClassifyContact } from '../../services/bucketingService.js';

const supabaseUrl = process.env.VITE_SUPABASE_URL || '';
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const supabase = createClient(supabaseUrl, supabaseKey, { auth: { persistSession: false } });

interface Golden {
    id: string;
    category: string;
    input: string;
    expected: { primary_identity: string | null; sub_identity: string | null; sector: string | null; is_disqualified: boolean };
    allow_identity_only?: boolean;
}

const GOLDENS_PATH = path.join(__dirname, 'goldens.json');
const raw = fs.readFileSync(GOLDENS_PATH, 'utf-8');
const file = JSON.parse(raw) as { cases: Golden[] };
const cases = file.cases;

const MODEL = process.env.EVAL_MATCH_MODEL || 'gpt-4.1-mini';
console.log(`Phase 1b smoke test — ${cases.length} cases via classifyContactBatch (${MODEL})`);

function normalize(v: string | null | undefined): string | null {
    if (v === null || v === undefined) return null;
    const t = String(v).trim();
    return t.length === 0 ? null : t;
}

function fieldsMatch(expected: string | null, actual: string | null, score: number | null, allowIdentityOnly: boolean, isSubOrSector: boolean): boolean {
    const e = normalize(expected);
    const a = normalize(actual);
    if (e === a) return true;
    if (e === null && allowIdentityOnly && isSubOrSector) return true;
    if (e === null && a !== null && score !== null && score < 0.4) return true;
    return false;
}

async function main() {
    const limit = pLimit(4);
    const t0 = Date.now();
    const results = await Promise.all(cases.map(c => limit(async () => {
        try {
            const r = await evalClassifyContact(supabase, c.input);
            const actual = {
                primary_identity: normalize(r.primary_identity),
                sub_identity: normalize(r.sub_identity),
                sector: normalize(r.sector),
                is_disqualified: r.is_disqualified,
            };
            const allow = !!c.allow_identity_only;
            const idPass = fieldsMatch(c.expected.primary_identity, actual.primary_identity, r.identity_score, allow, false);
            const subPass = fieldsMatch(c.expected.sub_identity, actual.sub_identity, r.sub_identity_score, allow, true);
            const secPass = fieldsMatch(c.expected.sector, actual.sector, null, allow, true);
            const dqPass = c.expected.is_disqualified === actual.is_disqualified;
            const overall = idPass && subPass && secPass && dqPass;
            return { id: c.id, category: c.category, input: c.input, expected: c.expected, actual, idPass, subPass, secPass, dqPass, overall, cost: r.cost_usd, raw: r.raw_response };
        } catch (err: any) {
            return { id: c.id, category: c.category, input: c.input, error: err?.message || String(err), overall: false, idPass: false, subPass: false, secPass: false, dqPass: false, cost: 0 };
        }
    })));
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);

    const id = results.filter((r: any) => r.idPass).length;
    const sub = results.filter((r: any) => r.subPass).length;
    const sec = results.filter((r: any) => r.secPass).length;
    const dq = results.filter((r: any) => r.dqPass).length;
    const overall = results.filter((r: any) => r.overall).length;
    const cost = results.reduce((s: number, r: any) => s + (r.cost || 0), 0);

    const pct = (n: number) => `${((n / results.length) * 100).toFixed(1)}%`;
    console.log('');
    console.log('=== ACCURACY (Phase 1b path) ===');
    console.log(`primary_identity   ${pct(id)}  (${id}/${results.length})`);
    console.log(`sub_identity       ${pct(sub)}  (${sub}/${results.length})`);
    console.log(`sector             ${pct(sec)}  (${sec}/${results.length})`);
    console.log(`is_disqualified    ${pct(dq)}  (${dq}/${results.length})`);
    console.log(`overall            ${pct(overall)}  (${overall}/${results.length})`);

    const failures = results.filter((r: any) => !r.overall);
    if (failures.length > 0) {
        console.log('');
        console.log('=== FAILURES ===');
        for (const r of failures as any[]) {
            console.log(`\n[${r.id}] ${r.category}  →  ${r.input}`);
            if (r.error) { console.log(`  ERROR: ${r.error}`); continue; }
            const fields: Array<'primary_identity' | 'sub_identity' | 'sector' | 'is_disqualified'> = ['primary_identity','sub_identity','sector','is_disqualified'];
            const flags = { primary_identity: r.idPass, sub_identity: r.subPass, sector: r.secPass, is_disqualified: r.dqPass };
            for (const k of fields) {
                if (!flags[k]) {
                    const e = (r.expected as any)[k];
                    const a = (r.actual as any)[k];
                    console.log(`  ${k.padEnd(20)}  expected=${JSON.stringify(e)}  got=${JSON.stringify(a)}`);
                }
            }
        }
    }
    console.log('');
    console.log(`Cost: $${cost.toFixed(4)}  |  Time: ${elapsed}s`);
    process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
