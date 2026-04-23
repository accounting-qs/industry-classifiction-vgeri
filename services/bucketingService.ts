/**
 * Bucketing Service — Phase 1 (taxonomy proposal) + Phase 2 (assignment).
 *
 * Core insight: the unit of work is the *distinct industry string*, not the
 * contact. 100k contacts share only ~500–2000 unique industries. Both phases
 * reason at vocabulary scale; per-contact fan-out is a single SQL JOIN.
 *
 * Phase 1 (one LLM call):
 *   1. Pull vocabulary via get_industry_vocabulary RPC
 *   2. Send to gpt-4.1 with strict JSON schema → propose 6–15 buckets
 *   3. Persist proposal + Phase 1 industry→bucket map rows
 *
 * Phase 2 (~70s end-to-end for 100k):
 *   1. Deterministic SQL JOIN: contacts.industry = map.industry_string → 90–95% covered
 *   2. Embed residual industries + bucket definitions (text-embedding-3-small)
 *   3. Auto-assign labels with cosine top1 ≥ 0.72 AND margin ≥ 0.08
 *   4. Batched LLM call (gpt-4.1-mini, 50 labels per request) for the rest
 *   5. Catch-all "Other" sweep — every contact in the lists ends up assigned
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';

const TAXONOMY_MODEL = process.env.BUCKETING_TAXONOMY_MODEL || 'gpt-4.1';
const RESIDUAL_MODEL = process.env.BUCKETING_RESIDUAL_MODEL || 'gpt-4.1-mini';
const EMBEDDING_MODEL = process.env.BUCKETING_EMBEDDING_MODEL || 'text-embedding-3-small';
const RESIDUAL_BATCH_SIZE = parseInt(process.env.BUCKETING_BATCH_SIZE || '50', 10);
const EMBED_AUTO_THRESHOLD = parseFloat(process.env.BUCKETING_EMBED_AUTO_THRESHOLD || '0.72');
const EMBED_MARGIN = parseFloat(process.env.BUCKETING_EMBED_MARGIN || '0.08');
const CONCURRENCY_AI = parseInt(process.env.CONCURRENCY_AI || '15', 10);
const OPENAI_TIMEOUT_MS = 90_000;
const TAXONOMY_TIMEOUT_MS = 120_000;

// gpt-4.1 family pricing per 1M tokens
const PRICING: Record<string, { input: number; output: number }> = {
    'gpt-4.1':       { input: 2.00,  output: 8.00 },
    'gpt-4.1-mini':  { input: 0.40,  output: 1.60 },
    'gpt-4.1-nano':  { input: 0.10,  output: 0.40 },
};
const EMBEDDING_PRICE_PER_1M = 0.02; // text-embedding-3-small

function getOpenAIKey(): string {
    const key = process.env.VITE_OPENAI_API_KEY || process.env.OPENAI_API_KEY;
    if (!key) throw new Error('VITE_OPENAI_API_KEY missing');
    return key.trim();
}

interface VocabRow {
    industry: string;
    n: number;
    avg_conf: number;
    sample_companies: string[] | null;
    sample_reasoning: string[] | null;
}

interface ProposedBucket {
    name: string;
    definition: string;
    personalization_angle?: string;
    example_industries: string[];
    estimated_count?: number;
}

interface TaxonomyResponse {
    buckets: ProposedBucket[];
    residual_note?: string;
}

const RESERVED_OTHER = 'Other';

// ────────────────────────────────────────────────────────────────────────
// PHASE 1 — Bucket Determination
// ────────────────────────────────────────────────────────────────────────

export async function runTaxonomyProposal(
    supabase: SupabaseClient,
    runId: string,
    log: (msg: string, level?: 'info' | 'warn' | 'error') => void
): Promise<void> {
    log(`[Bucketing ${runId}] Phase 1: starting taxonomy proposal`);

    const { data: run, error: runErr } = await supabase
        .from('bucketing_runs').select('*').eq('id', runId).single();
    if (runErr || !run) throw new Error(`Run not found: ${runErr?.message}`);

    // 1) Vocabulary extraction
    const { data: vocab, error: vErr } = await supabase
        .rpc('get_industry_vocabulary', { p_list_names: run.list_names });
    if (vErr) throw new Error(`get_industry_vocabulary failed: ${vErr.message}`);

    const vocabRows = (vocab || []) as VocabRow[];
    const totalContacts = vocabRows.reduce((s, r) => s + Number(r.n || 0), 0);
    log(`[Bucketing ${runId}] vocabulary: ${vocabRows.length} distinct industries, ${totalContacts} contacts`);

    if (vocabRows.length === 0) {
        await supabase.from('bucketing_runs').update({
            status: 'failed',
            error_message: 'No enriched contacts found for the selected lists.',
            total_contacts: 0
        }).eq('id', runId);
        throw new Error('Empty vocabulary — none of the selected lists have completed enrichments.');
    }

    // 2) LLM taxonomy call
    const t0 = Date.now();
    const { proposal, costUsd } = await callTaxonomyLLM(vocabRows);
    log(`[Bucketing ${runId}] taxonomy LLM call: ${(Date.now() - t0) / 1000}s, $${costUsd.toFixed(4)}, ${proposal.buckets.length} buckets`);

    // Filter out any "Other"-named bucket the model returned (reserved name)
    const cleanBuckets = proposal.buckets
        .filter(b => b.name && b.name.trim().toLowerCase() !== RESERVED_OTHER.toLowerCase())
        .map(b => ({
            ...b,
            name: b.name.trim(),
            definition: (b.definition || '').trim(),
            example_industries: Array.from(
                new Set((b.example_industries || []).map(s => (s || '').trim()).filter(Boolean))
            )
        }));

    // 3) Persist proposal
    await supabase.from('bucketing_runs').update({
        taxonomy_proposal: { buckets: cleanBuckets, residual_note: proposal.residual_note },
        taxonomy_model: TAXONOMY_MODEL,
        total_contacts: totalContacts,
        cost_usd: costUsd,
        status: 'taxonomy_ready',
        taxonomy_completed_at: new Date().toISOString()
    }).eq('id', runId);

    // Insert Phase 1 map rows. The vocabulary set bounds what's valid — drop
    // any LLM-hallucinated industry strings the model invented that aren't in
    // the actual data.
    const validIndustries = new Set(vocabRows.map(r => r.industry));
    const mapRows: { bucketing_run_id: string; industry_string: string; bucket_name: string; source: string; confidence: number }[] = [];
    const seen = new Set<string>();
    for (const b of cleanBuckets) {
        for (const ind of b.example_industries) {
            if (!validIndustries.has(ind)) continue;
            if (seen.has(ind)) continue; // first bucket wins on conflict — model rarely conflicts
            seen.add(ind);
            mapRows.push({
                bucketing_run_id: runId,
                industry_string: ind,
                bucket_name: b.name,
                source: 'llm_phase1',
                confidence: 1.0
            });
        }
    }

    if (mapRows.length > 0) {
        // chunked upsert; 1000-row batches stay under PostgREST payload limits
        for (let i = 0; i < mapRows.length; i += 1000) {
            const chunk = mapRows.slice(i, i + 1000);
            const { error } = await supabase.from('bucket_industry_map')
                .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
            if (error) throw new Error(`bucket_industry_map insert failed: ${error.message}`);
        }
    }
    log(`[Bucketing ${runId}] Phase 1 done: ${mapRows.length} initial industry mappings`);
}

async function callTaxonomyLLM(vocabRows: VocabRow[]): Promise<{ proposal: TaxonomyResponse; costUsd: number }> {
    // Top 800 covers >99% of contact mass for a typical 100k-contact list.
    // Tail summary keeps the rest visible without blowing the prompt out.
    const HEAD_LIMIT = 800;
    const head = vocabRows.slice(0, HEAD_LIMIT);
    const tail = vocabRows.slice(HEAD_LIMIT);
    const tailContacts = tail.reduce((s, r) => s + Number(r.n || 0), 0);
    const tailExamples = tail.slice(0, 20).map(r => r.industry);

    const vocabularyTable = head.map(r => {
        const samples = (r.sample_companies || []).slice(0, 3).filter(Boolean).join(' | ');
        const reason = (r.sample_reasoning || [])[0] || '';
        const trimmedReason = reason.length > 160 ? reason.slice(0, 160) + '…' : reason;
        return `${r.industry} | n=${r.n} | companies=[${samples}] | reasoning="${trimmedReason}"`;
    }).join('\n');

    const tailSection = tail.length > 0
        ? `\n\nTail (${tail.length} more labels covering ${tailContacts} contacts). Examples: ${tailExamples.join(', ')}.`
        : '';

    const exampleBucketStyle = [
        'FinTech & Financial Services SaaS',
        'IT Consulting & Managed Services',
        'Accounting Firms USA',
        'Architecture, Engineering & Construction',
        'Accounting, Audit & Tax Services',
        'Venture Capital Firm',
        'Data, Analytics & AI SaaS',
        'Insurance Services & Brokerage',
        'Branding, Creative & PR Agency',
        'Digital Marketing & SEO Agency'
    ];

    const systemPrompt = `You are designing outreach segments for cold email campaigns.

Your job: given a vocabulary of narrow industry labels (each label was assigned to companies by a prior classifier), propose 6–15 wider-but-still-specific BUCKETS. Each bucket must:
- Be specific enough that a single personalized opener works for every company in it.
- Avoid generic names ("Professional Services", "Technology", "Software"). Use specific-plus-modifier ("IT Consulting & Managed Services", not "Consulting").
- Map cleanly to a clear set of the input industry labels.
- NEVER use the name "Other" — that name is reserved for the catch-all bucket.

Each bucket needs:
- name: short, usable as a UI chip, in the same style as these examples: ${exampleBucketStyle.join(', ')}
- definition: 1–2 sentences describing what kind of company belongs there.
- personalization_angle: one sentence explaining why grouping these together = same outreach hook.
- example_industries: an array of 5–20 EXACT strings copied verbatim from the vocabulary you were given. These are what the assignment engine will use as the deterministic mapping.
- estimated_count: rough total contacts based on the counts in the vocabulary table.

Output strict JSON matching the schema. No prose, no markdown.`;

    const userPrompt = `Vocabulary table (industry | n=count | companies | reasoning), sorted by count descending:

${vocabularyTable}${tailSection}

Propose buckets now. Remember: the example_industries array MUST contain only strings that appear in the vocabulary table above, copied verbatim.`;

    const schema = {
        type: 'object',
        additionalProperties: false,
        required: ['buckets'],
        properties: {
            buckets: {
                type: 'array',
                items: {
                    type: 'object',
                    additionalProperties: false,
                    required: ['name', 'definition', 'example_industries'],
                    properties: {
                        name: { type: 'string' },
                        definition: { type: 'string' },
                        personalization_angle: { type: 'string' },
                        example_industries: { type: 'array', items: { type: 'string' } },
                        estimated_count: { type: 'integer' }
                    }
                }
            },
            residual_note: { type: 'string' }
        }
    };

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), TAXONOMY_TIMEOUT_MS);
    try {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${getOpenAIKey()}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                model: TAXONOMY_MODEL,
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt }
                ],
                response_format: {
                    type: 'json_schema',
                    json_schema: { name: 'bucket_taxonomy', strict: true, schema }
                },
                temperature: 0.2
            }),
            signal: controller.signal
        });

        if (!response.ok) {
            const errBody = await response.text().catch(() => '');
            throw new Error(`Taxonomy LLM error ${response.status}: ${errBody.slice(0, 500)}`);
        }
        const data = await response.json();
        const text = data.choices?.[0]?.message?.content;
        if (!text) throw new Error('Empty taxonomy response');
        const proposal = JSON.parse(text) as TaxonomyResponse;

        const usage = data.usage || {};
        const costUsd = computeCost(TAXONOMY_MODEL, usage.prompt_tokens || 0, usage.completion_tokens || 0);
        return { proposal, costUsd };
    } finally {
        clearTimeout(timeoutId);
    }
}

// ────────────────────────────────────────────────────────────────────────
// USER EDIT APPLICATION (between Phase 1 and Phase 2)
// ────────────────────────────────────────────────────────────────────────

interface TaxonomyEdits {
    keep?: string[];                          // bucket names to keep
    rename?: Record<string, string>;          // {old: new}
    add?: { name: string; definition: string }[];
    min_volume?: number;
}

export async function applyTaxonomyEdits(
    supabase: SupabaseClient,
    runId: string,
    edits: TaxonomyEdits,
    log: (msg: string, level?: 'info' | 'warn' | 'error') => void
): Promise<void> {
    const { data: run, error } = await supabase.from('bucketing_runs').select('*').eq('id', runId).single();
    if (error || !run) throw new Error(`Run not found: ${error?.message}`);
    const proposal: TaxonomyResponse | null = run.taxonomy_proposal;
    if (!proposal) throw new Error('Taxonomy proposal missing — run Phase 1 first');

    let buckets = [...proposal.buckets];

    // 1) Apply renames
    if (edits.rename) {
        for (const [oldName, newName] of Object.entries(edits.rename)) {
            const target = newName.trim();
            if (!target || target.toLowerCase() === RESERVED_OTHER.toLowerCase()) continue;
            buckets = buckets.map(b => b.name === oldName ? { ...b, name: target } : b);
            const { error: upErr } = await supabase.from('bucket_industry_map')
                .update({ bucket_name: target })
                .eq('bucketing_run_id', runId)
                .eq('bucket_name', oldName);
            if (upErr) throw new Error(`Rename failed: ${upErr.message}`);
        }
    }

    // 2) Drop unkept buckets
    if (edits.keep) {
        const keepSet = new Set(edits.keep.map(s => s.trim()));
        const dropped = buckets.filter(b => !keepSet.has(b.name)).map(b => b.name);
        buckets = buckets.filter(b => keepSet.has(b.name));
        if (dropped.length > 0) {
            const { error: delErr } = await supabase.from('bucket_industry_map')
                .delete()
                .eq('bucketing_run_id', runId)
                .in('bucket_name', dropped);
            if (delErr) throw new Error(`Drop failed: ${delErr.message}`);
        }
    }

    // 3) Add manual buckets (no map rows yet — Phase 2 embedding/LLM populates)
    if (edits.add) {
        for (const a of edits.add) {
            const name = (a.name || '').trim();
            if (!name || name.toLowerCase() === RESERVED_OTHER.toLowerCase()) continue;
            if (buckets.some(b => b.name === name)) continue;
            buckets.push({
                name,
                definition: (a.definition || '').trim(),
                example_industries: []
            });
        }
    }

    const update: any = { taxonomy_final: { buckets } };
    if (typeof edits.min_volume === 'number' && edits.min_volume >= 0) {
        update.min_volume = edits.min_volume;
    }
    const { error: upRunErr } = await supabase.from('bucketing_runs').update(update).eq('id', runId);
    if (upRunErr) throw new Error(`Run update failed: ${upRunErr.message}`);
    log(`[Bucketing ${runId}] taxonomy edits applied: ${buckets.length} buckets`);
}

// ────────────────────────────────────────────────────────────────────────
// PHASE 2 — Bucket Assignment
// ────────────────────────────────────────────────────────────────────────

export async function runAssignment(
    supabase: SupabaseClient,
    runId: string,
    log: (msg: string, level?: 'info' | 'warn' | 'error') => void
): Promise<void> {
    const { data: run, error } = await supabase.from('bucketing_runs').select('*').eq('id', runId).single();
    if (error || !run) throw new Error(`Run not found: ${error?.message}`);

    const final = run.taxonomy_final || run.taxonomy_proposal;
    if (!final?.buckets || final.buckets.length === 0) {
        throw new Error('No buckets defined for assignment');
    }
    const buckets = final.buckets as ProposedBucket[];
    const allowedNames = new Set(buckets.map(b => b.name));

    await supabase.from('bucketing_runs').update({ status: 'assigning' }).eq('id', runId);

    // Apply min-volume threshold against the deterministic map. This drops
    // map rows whose bucket would fall under the user's threshold; the
    // affected contacts then funnel into "Other" via the catch-all sweep.
    if (run.min_volume && run.min_volume > 0) {
        const { data: counts } = await supabase
            .rpc('get_bucket_map_counts', { p_run_id: runId });
        const undersized = (counts || [])
            .filter((c: any) => Number(c.contact_count) < run.min_volume)
            .map((c: any) => c.bucket_name);
        if (undersized.length > 0) {
            await supabase.from('bucket_industry_map').delete()
                .eq('bucketing_run_id', runId).in('bucket_name', undersized);
            log(`[Bucketing ${runId}] dropped ${undersized.length} buckets below min_volume=${run.min_volume}`);
        }
    }

    let totalCost = Number(run.cost_usd || 0);

    // Step 1 — deterministic SQL fan-out (chunked client-side since PostgREST
    // doesn't expose INSERT … SELECT). We pull industry → bucket map then walk
    // contacts in pages, joining client-side. For ~100k this is ~10–20s.
    log(`[Bucketing ${runId}] step 1/4: deterministic fan-out`);
    const determCount = await deterministicFanOut(supabase, runId, run.list_names);
    log(`[Bucketing ${runId}] deterministic assigned: ${determCount}`);

    // Step 2 — embedding fallback
    log(`[Bucketing ${runId}] step 2/4: embedding fallback`);
    const embedCost = await embeddingFallback(supabase, runId, run.list_names, buckets);
    totalCost += embedCost;

    // Step 3 — residual LLM batch
    log(`[Bucketing ${runId}] step 3/4: residual LLM batch classification`);
    const llmCost = await residualLLMClassification(supabase, runId, run.list_names, buckets, allowedNames);
    totalCost += llmCost;

    // Re-run fan-out to pick up newly added map rows from steps 2 and 3
    await deterministicFanOut(supabase, runId, run.list_names);

    // Step 4 — catch-all "Other" sweep. Every remaining contact in the
    // selected lists lands here — scrape failures, no enrichment, low
    // confidence, no fit, dropped-bucket orphans. ON CONFLICT keeps existing
    // assignments untouched.
    log(`[Bucketing ${runId}] step 4/4: catch-all Other sweep`);
    const otherCount = await catchAllOther(supabase, runId, run.list_names);
    log(`[Bucketing ${runId}] Other bucket received ${otherCount} contacts`);

    // Final stats
    const { count: assignedCount } = await supabase
        .from('bucket_assignments')
        .select('contact_id', { count: 'exact', head: true })
        .eq('bucketing_run_id', runId);

    await supabase.from('bucketing_runs').update({
        status: 'completed',
        assigned_contacts: assignedCount || 0,
        cost_usd: totalCost,
        assignment_completed_at: new Date().toISOString()
    }).eq('id', runId);

    log(`[Bucketing ${runId}] DONE — ${assignedCount} contacts assigned, total cost $${totalCost.toFixed(4)}`);
}

// ───── Step 1: deterministic fan-out ─────
async function deterministicFanOut(
    supabase: SupabaseClient,
    runId: string,
    listNames: string[]
): Promise<number> {
    // Pull the full map for this run into memory (it's bounded by vocabulary
    // size — typically a few thousand rows max).
    const { data: mapRows, error: mErr } = await supabase
        .from('bucket_industry_map')
        .select('industry_string,bucket_name,source,confidence')
        .eq('bucketing_run_id', runId);
    if (mErr) throw new Error(`map fetch failed: ${mErr.message}`);

    const lookup = new Map<string, { bucket_name: string; source: string; confidence: number | null }>();
    for (const r of (mapRows || [])) {
        lookup.set(r.industry_string, { bucket_name: r.bucket_name, source: r.source, confidence: r.confidence });
    }
    if (lookup.size === 0) return 0;

    // Walk contacts in pages, write assignments in chunks.
    const PAGE_SIZE = 5000;
    const UPSERT_CHUNK = 1000;
    let lastId: string | null = null;
    let inserted = 0;

    while (true) {
        let q: any = supabase.from('contacts')
            .select('contact_id,industry')
            .in('lead_list_name', listNames)
            .order('contact_id', { ascending: true })
            .limit(PAGE_SIZE);
        if (lastId) q = q.gt('contact_id', lastId);

        const { data: contactPage, error: cErr } = await q;
        if (cErr) throw new Error(`contacts page failed: ${cErr.message}`);
        const page = (contactPage || []) as { contact_id: string; industry: string | null }[];
        if (page.length === 0) break;

        const rows: any[] = [];
        for (const c of page) {
            if (!c.industry) continue;
            const hit = lookup.get(c.industry);
            if (!hit) continue;
            rows.push({
                bucketing_run_id: runId,
                contact_id: c.contact_id,
                bucket_name: hit.bucket_name,
                source: hit.source === 'llm_phase1' || hit.source === 'manual' ? 'deterministic' : hit.source,
                confidence: hit.confidence ?? 1.0
            });
        }

        for (let i = 0; i < rows.length; i += UPSERT_CHUNK) {
            const chunk = rows.slice(i, i + UPSERT_CHUNK);
            const { error } = await supabase.from('bucket_assignments')
                .upsert(chunk, { onConflict: 'bucketing_run_id,contact_id', ignoreDuplicates: true });
            if (error) throw new Error(`bucket_assignments insert failed: ${error.message}`);
        }
        inserted += rows.length;

        lastId = page[page.length - 1].contact_id;
        if (page.length < PAGE_SIZE) break;
    }

    return inserted;
}

// ───── Step 2: embedding fallback ─────
async function embeddingFallback(
    supabase: SupabaseClient,
    runId: string,
    listNames: string[],
    buckets: ProposedBucket[]
): Promise<number> {
    // Find residual industries: distinct industry strings in the lists that
    // don't yet have a map row for this run.
    const residuals = await getResidualIndustries(supabase, runId, listNames);
    if (residuals.length === 0) return 0;

    // Embed bucket definitions + all residual industries in one batch.
    // OpenAI embeddings API accepts an array of inputs (limit 2048 per call).
    const bucketTexts = buckets.map(b => `${b.name}: ${b.definition || b.name}`);
    const allInputs = [...bucketTexts, ...residuals];
    const { embeddings, costUsd } = await embedBatch(allInputs);
    if (embeddings.length !== allInputs.length) {
        throw new Error(`embedding count mismatch: got ${embeddings.length}, expected ${allInputs.length}`);
    }
    const bucketVecs = embeddings.slice(0, buckets.length);
    const residualVecs = embeddings.slice(buckets.length);

    const inserts: any[] = [];
    for (let i = 0; i < residuals.length; i++) {
        const ind = residuals[i];
        const sims = bucketVecs.map(bv => cosine(residualVecs[i], bv));
        const sorted = sims.map((s, j) => ({ s, j })).sort((a, b) => b.s - a.s);
        const top = sorted[0];
        const second = sorted[1] || { s: 0 };
        if (top.s >= EMBED_AUTO_THRESHOLD && (top.s - second.s) >= EMBED_MARGIN) {
            inserts.push({
                bucketing_run_id: runId,
                industry_string: ind,
                bucket_name: buckets[top.j].name,
                source: 'embedding',
                confidence: Number(top.s.toFixed(2))
            });
        }
    }

    if (inserts.length > 0) {
        for (let i = 0; i < inserts.length; i += 1000) {
            const chunk = inserts.slice(i, i + 1000);
            const { error } = await supabase.from('bucket_industry_map')
                .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
            if (error) throw new Error(`embedding map insert failed: ${error.message}`);
        }
    }
    return costUsd;
}

async function embedBatch(inputs: string[]): Promise<{ embeddings: number[][]; costUsd: number }> {
    const BATCH = 1024;
    const all: number[][] = [];
    let totalTokens = 0;
    for (let i = 0; i < inputs.length; i += BATCH) {
        const slice = inputs.slice(i, i + BATCH);
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), OPENAI_TIMEOUT_MS);
        try {
            const res = await fetch('https://api.openai.com/v1/embeddings', {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${getOpenAIKey()}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ model: EMBEDDING_MODEL, input: slice }),
                signal: controller.signal
            });
            if (!res.ok) {
                const body = await res.text().catch(() => '');
                throw new Error(`embeddings ${res.status}: ${body.slice(0, 300)}`);
            }
            const data = await res.json();
            for (const d of data.data) all.push(d.embedding as number[]);
            totalTokens += data.usage?.total_tokens || 0;
        } finally {
            clearTimeout(t);
        }
    }
    const costUsd = (totalTokens / 1_000_000) * EMBEDDING_PRICE_PER_1M;
    return { embeddings: all, costUsd };
}

function cosine(a: number[], b: number[]): number {
    let dot = 0, na = 0, nb = 0;
    for (let i = 0; i < a.length; i++) { dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i]; }
    if (na === 0 || nb === 0) return 0;
    return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

// ───── Step 3: residual LLM batch ─────
async function residualLLMClassification(
    supabase: SupabaseClient,
    runId: string,
    listNames: string[],
    buckets: ProposedBucket[],
    allowedNames: Set<string>
): Promise<number> {
    // Residuals after embedding pass: distinct industries with no map row yet.
    const residuals = await getResidualIndustries(supabase, runId, listNames);
    if (residuals.length === 0) return 0;

    // Pull sample context for each residual industry — counts + sample
    // companies/reasoning to give the LLM enough signal to bucket well.
    const samplesByIndustry = await getSamplesForIndustries(supabase, residuals, listNames);

    const bucketsForPrompt = buckets.map(b => ({ name: b.name, definition: b.definition || b.name }));
    const limit = pLimit(CONCURRENCY_AI);

    const batches: string[][] = [];
    for (let i = 0; i < residuals.length; i += RESIDUAL_BATCH_SIZE) {
        batches.push(residuals.slice(i, i + RESIDUAL_BATCH_SIZE));
    }

    let totalCost = 0;
    const allInserts: any[] = [];

    await Promise.all(batches.map(batch => limit(async () => {
        const labelPayload = batch.map(ind => {
            const s = samplesByIndustry.get(ind);
            return {
                industry: ind,
                count: s?.n ?? 0,
                sample_companies: s?.sample_companies?.slice(0, 3) || [],
                sample_reasoning: (s?.sample_reasoning?.[0] || '').slice(0, 200)
            };
        });

        const { results, costUsd } = await classifyResidualBatch(labelPayload, bucketsForPrompt, allowedNames);
        totalCost += costUsd;

        for (const r of results) {
            // Validate: if confidence < 6 or unknown bucket, route to "Other"
            // by inserting an industry→Other mapping. The deterministic
            // fan-out re-run picks it up.
            const isValid = allowedNames.has(r.bucket_name) && (r.confidence ?? 0) >= 6;
            allInserts.push({
                bucketing_run_id: runId,
                industry_string: r.industry,
                bucket_name: isValid ? r.bucket_name : RESERVED_OTHER,
                source: 'llm_phase2',
                confidence: Number(((r.confidence ?? 0) / 10).toFixed(2))
            });
        }
    })));

    if (allInserts.length > 0) {
        for (let i = 0; i < allInserts.length; i += 1000) {
            const chunk = allInserts.slice(i, i + 1000);
            const { error } = await supabase.from('bucket_industry_map')
                .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
            if (error) throw new Error(`llm phase2 map insert failed: ${error.message}`);
        }
    }
    return totalCost;
}

interface BatchClassifyResult {
    industry: string;
    bucket_name: string;
    confidence: number;
    rationale?: string;
}

async function classifyResidualBatch(
    labels: { industry: string; count: number; sample_companies: string[]; sample_reasoning: string }[],
    buckets: { name: string; definition: string }[],
    allowedNames: Set<string>
): Promise<{ results: BatchClassifyResult[]; costUsd: number }> {
    const systemPrompt = `Assign each industry label to exactly one bucket from the provided list. Use confidence 1–10. If no bucket fits with confidence >= 6, use "Other". Do not invent bucket names. Return strict JSON.`;

    const userPrompt = `Allowed buckets:\n${buckets.map(b => `- ${b.name}: ${b.definition}`).join('\n')}\n\nIndustries to assign:\n${JSON.stringify(labels, null, 2)}\n\nReturn an "assignments" array with one object per industry: {industry, bucket_name, confidence, rationale}. The bucket_name MUST be one of the allowed bucket names listed above (or "Other").`;

    const schema = {
        type: 'object',
        additionalProperties: false,
        required: ['assignments'],
        properties: {
            assignments: {
                type: 'array',
                items: {
                    type: 'object',
                    additionalProperties: false,
                    required: ['industry', 'bucket_name', 'confidence'],
                    properties: {
                        industry: { type: 'string' },
                        bucket_name: { type: 'string' },
                        confidence: { type: 'number' },
                        rationale: { type: 'string' }
                    }
                }
            }
        }
    };

    const callOnce = async () => {
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), OPENAI_TIMEOUT_MS);
        try {
            const res = await fetch('https://api.openai.com/v1/chat/completions', {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${getOpenAIKey()}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    model: RESIDUAL_MODEL,
                    messages: [
                        { role: 'system', content: systemPrompt },
                        { role: 'user', content: userPrompt }
                    ],
                    response_format: {
                        type: 'json_schema',
                        json_schema: { name: 'residual_assignments', strict: true, schema }
                    },
                    temperature: 0.1
                }),
                signal: controller.signal
            });
            if (!res.ok) {
                const body = await res.text().catch(() => '');
                throw new Error(`residual classify ${res.status}: ${body.slice(0, 300)}`);
            }
            const data = await res.json();
            const text = data.choices?.[0]?.message?.content;
            const parsed = JSON.parse(text);
            const usage = data.usage || {};
            const costUsd = computeCost(RESIDUAL_MODEL, usage.prompt_tokens || 0, usage.completion_tokens || 0);
            return { assignments: parsed.assignments as BatchClassifyResult[], costUsd };
        } finally {
            clearTimeout(t);
        }
    };

    let { assignments, costUsd } = await callOnce();

    // Drift guard: if any returned bucket isn't in the allowed set, retry once
    // with the same prompt — usually corrects on retry. Else those rows fall
    // through to the "<6 confidence" check downstream and route to Other.
    const bad = assignments.filter(a => a.bucket_name !== RESERVED_OTHER && !allowedNames.has(a.bucket_name));
    if (bad.length > 0) {
        const retried = await callOnce();
        assignments = retried.assignments;
        costUsd += retried.costUsd;
    }

    return { results: assignments, costUsd };
}

// ───── Step 4: catch-all Other sweep ─────
async function catchAllOther(
    supabase: SupabaseClient,
    runId: string,
    listNames: string[]
): Promise<number> {
    // Walk contacts in pages, insert any without an existing assignment as
    // "Other". The unique constraint makes the upsert idempotent.
    const PAGE_SIZE = 5000;
    let lastId: string | null = null;
    let inserted = 0;

    while (true) {
        let q: any = supabase.from('contacts')
            .select('contact_id')
            .in('lead_list_name', listNames)
            .order('contact_id', { ascending: true })
            .limit(PAGE_SIZE);
        if (lastId) q = q.gt('contact_id', lastId);

        const { data: page, error } = await q;
        if (error) throw new Error(`contacts sweep page failed: ${error.message}`);
        const rows = (page || []) as { contact_id: string }[];
        if (rows.length === 0) break;

        const inserts = rows.map(r => ({
            bucketing_run_id: runId,
            contact_id: r.contact_id,
            bucket_name: RESERVED_OTHER,
            source: 'other',
            confidence: 0
        }));

        for (let i = 0; i < inserts.length; i += 1000) {
            const chunk = inserts.slice(i, i + 1000);
            const { error: upErr } = await supabase.from('bucket_assignments')
                .upsert(chunk, { onConflict: 'bucketing_run_id,contact_id', ignoreDuplicates: true });
            if (upErr) throw new Error(`Other sweep insert failed: ${upErr.message}`);
        }
        inserted += inserts.length;

        lastId = rows[rows.length - 1].contact_id;
        if (rows.length < PAGE_SIZE) break;
    }
    return inserted;
}

// ───── helpers ─────

async function getResidualIndustries(
    supabase: SupabaseClient,
    runId: string,
    listNames: string[]
): Promise<string[]> {
    // Distinct industries in the selected lists that don't yet have a map row.
    // We can't express NOT EXISTS in PostgREST cleanly, so we fetch both sets
    // and diff client-side. The vocabulary RPC already gives us the distinct
    // industries — we filter against the existing map.
    const { data: vocab, error: vErr } = await supabase
        .rpc('get_industry_vocabulary', { p_list_names: listNames });
    if (vErr) throw new Error(`vocab fetch failed: ${vErr.message}`);

    const { data: mapRows, error: mErr } = await supabase
        .from('bucket_industry_map').select('industry_string')
        .eq('bucketing_run_id', runId);
    if (mErr) throw new Error(`map read failed: ${mErr.message}`);

    const mapped = new Set((mapRows || []).map((r: any) => r.industry_string));
    return ((vocab || []) as VocabRow[])
        .map(r => r.industry)
        .filter(ind => !mapped.has(ind));
}

async function getSamplesForIndustries(
    supabase: SupabaseClient,
    industries: string[],
    listNames: string[]
): Promise<Map<string, VocabRow>> {
    const { data: vocab } = await supabase
        .rpc('get_industry_vocabulary', { p_list_names: listNames });
    const map = new Map<string, VocabRow>();
    const wanted = new Set(industries);
    for (const r of (vocab || []) as VocabRow[]) {
        if (wanted.has(r.industry)) map.set(r.industry, r);
    }
    return map;
}

function computeCost(model: string, promptTokens: number, completionTokens: number): number {
    const p = PRICING[model] || PRICING['gpt-4.1-mini'];
    return (promptTokens / 1_000_000) * p.input + (completionTokens / 1_000_000) * p.output;
}
