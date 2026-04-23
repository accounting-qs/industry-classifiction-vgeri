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
    // Empty string = top-level parent. Otherwise references another bucket's
    // `name` in the same taxonomy — used to roll children up when they fall
    // below the min_volume threshold instead of dumping them to Other.
    parent_bucket?: string;
}

interface TaxonomyResponse {
    buckets: ProposedBucket[];
    residual_note?: string;
}

const RESERVED_OTHER = 'Other';

// PostgREST caps RPC responses at 1000 rows by default. For lists with long
// industry tails (common — classifier produces near-unique labels per company)
// that means `totalContacts` reflects only the top 1000 industries, and the
// LLM proposes buckets against a biased sample. Paginate explicitly to fetch
// the full vocabulary up to a generous safety cap.
const VOCAB_PAGE_SIZE = 1000;
const VOCAB_MAX_ROWS = 100_000;

async function fetchFullVocabulary(
    supabase: SupabaseClient,
    listNames: string[]
): Promise<VocabRow[]> {
    const all: VocabRow[] = [];
    let offset = 0;
    while (offset < VOCAB_MAX_ROWS) {
        const { data, error } = await supabase
            .rpc('get_industry_vocabulary', { p_list_names: listNames })
            .range(offset, offset + VOCAB_PAGE_SIZE - 1);
        if (error) throw new Error(`vocabulary fetch failed: ${error.message}`);
        const page = (data || []) as VocabRow[];
        if (page.length === 0) break;
        all.push(...page);
        if (page.length < VOCAB_PAGE_SIZE) break;
        offset += VOCAB_PAGE_SIZE;
    }
    return all;
}

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

    // 1) Vocabulary extraction (paginated — bypasses PostgREST 1000-row cap)
    const vocabRows = await fetchFullVocabulary(supabase, run.list_names);
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

    // Filter out any "Other"-named bucket the model returned (reserved name),
    // normalize names, and validate parent references point at a real bucket.
    const rawBuckets = proposal.buckets
        .filter(b => b.name && b.name.trim().toLowerCase() !== RESERVED_OTHER.toLowerCase())
        .map(b => ({
            ...b,
            name: b.name.trim(),
            definition: (b.definition || '').trim(),
            parent_bucket: (b.parent_bucket || '').trim(),
            example_industries: Array.from(
                new Set((b.example_industries || []).map(s => (s || '').trim()).filter(Boolean))
            )
        }));

    const bucketNames = new Set(rawBuckets.map(b => b.name));
    const cleanBuckets = rawBuckets.map(b => ({
        ...b,
        // If parent reference is broken (model drift), treat as top-level.
        parent_bucket: b.parent_bucket && bucketNames.has(b.parent_bucket) ? b.parent_bucket : ''
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
    // Classifier tends to produce hyper-specific labels, so lists can have a
    // very long tail. Send a larger head to the model (~2500 rows ≈ 50k input
    // tokens, well within gpt-4.1) so the proposed taxonomy reflects the
    // actual distribution, not just the 800 most-common labels.
    const HEAD_LIMIT = 2500;
    const head = vocabRows.slice(0, HEAD_LIMIT);
    const tail = vocabRows.slice(HEAD_LIMIT);
    const tailContacts = tail.reduce((s, r) => s + Number(r.n || 0), 0);
    const headContacts = head.reduce((s, r) => s + Number(r.n || 0), 0);
    const tailExamples = tail.slice(0, 40).map(r => r.industry);

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

Your job: given a vocabulary of narrow industry labels (assigned by a prior classifier), propose a TWO-LEVEL bucket taxonomy:

- 3–6 PARENT buckets: broader categories (e.g. "IT & Cybersecurity Services", "Financial Services", "Legal Services"). These catch children that fall below the user's minimum volume threshold.
- 8–20 CHILD buckets: more specific segments inside a parent (e.g. under "IT & Cybersecurity Services" → "Managed IT Services for SMBs", "Cybersecurity Consulting & PenTesting", "IT Consulting for Regulated Industries"). A child always references its parent by name.

Every bucket (parent or child) must:
- Be specific enough that a personalized opener works for every company in it.
- Avoid generic names ("Professional Services", "Technology", "Software"). Use specific-plus-modifier ("IT Consulting & Managed Services", not "Consulting").
- Map cleanly to a clear set of the input industry labels.
- NEVER use the name "Other" — that name is reserved for the catch-all bucket.

Each bucket needs:
- name: short, usable as a UI chip, in the same style as these examples: ${exampleBucketStyle.join(', ')}
- definition: 1–2 sentences describing what kind of company belongs there.
- personalization_angle: one sentence explaining why grouping these together = same outreach hook.
- example_industries: an array of 5–25 EXACT strings copied verbatim from the vocabulary you were given. These are what the assignment engine will use as the deterministic mapping. Parent buckets may have few or zero example_industries if they're purely a rollup category — their children carry the examples.
- estimated_count: rough total contacts based on the counts in the vocabulary table (for a parent, sum of its children).
- parent_bucket: the name of this bucket's PARENT bucket, copied verbatim from another bucket's \`name\` field. Use empty string "" if this bucket IS a parent (top-level).

IMPORTANT: children stay narrow so a tight opener works; parents are the fallback target when a child is too small to survive the user's volume threshold. So children should together cover most of the vocabulary, and each child MUST name a parent that also appears in your \`buckets\` array.

Output strict JSON matching the schema. No prose, no markdown.`;

    const userPrompt = `Vocabulary table (industry | n=count | companies | reasoning), sorted by count descending. The head below covers ${headContacts} contacts across ${head.length} distinct labels:

${vocabularyTable}${tailSection}

Total contacts across all labels: ${headContacts + tailContacts}.

Propose the two-level taxonomy now. Remember: example_industries must be strings copied verbatim from the vocabulary table. Every child's parent_bucket must match another bucket's name in your output.`;

    // OpenAI strict JSON schema requires every property in `properties` to
    // appear in `required` — there is no "optional" field when strict=true.
    // The model can still return empty strings / 0 for fields it has nothing
    // useful to say about.
    const schema = {
        type: 'object',
        additionalProperties: false,
        required: ['buckets', 'residual_note'],
        properties: {
            buckets: {
                type: 'array',
                items: {
                    type: 'object',
                    additionalProperties: false,
                    required: ['name', 'definition', 'personalization_angle', 'example_industries', 'estimated_count', 'parent_bucket'],
                    properties: {
                        name: { type: 'string' },
                        definition: { type: 'string' },
                        personalization_angle: { type: 'string' },
                        example_industries: { type: 'array', items: { type: 'string' } },
                        estimated_count: { type: 'integer' },
                        parent_bucket: { type: 'string' }
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

    // 1) Apply renames — also retarget any child whose parent was renamed.
    if (edits.rename) {
        for (const [oldName, newName] of Object.entries(edits.rename)) {
            const target = newName.trim();
            if (!target || target.toLowerCase() === RESERVED_OTHER.toLowerCase()) continue;
            buckets = buckets.map(b => {
                const renamed = b.name === oldName ? { ...b, name: target } : b;
                return renamed.parent_bucket === oldName ? { ...renamed, parent_bucket: target } : renamed;
            });
            const { error: upErr } = await supabase.from('bucket_industry_map')
                .update({ bucket_name: target })
                .eq('bucketing_run_id', runId)
                .eq('bucket_name', oldName);
            if (upErr) throw new Error(`Rename failed: ${upErr.message}`);
        }
    }

    // 2) Drop unkept buckets. Children whose parent is dropped become
    //    top-level (parent_bucket="") so they still survive on their own.
    if (edits.keep) {
        const keepSet = new Set(edits.keep.map(s => s.trim()));
        const dropped = buckets.filter(b => !keepSet.has(b.name)).map(b => b.name);
        const droppedSet = new Set(dropped);
        buckets = buckets
            .filter(b => keepSet.has(b.name))
            .map(b => droppedSet.has(b.parent_bucket || '') ? { ...b, parent_bucket: '' } : b);
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

    // Apply min-volume threshold with parent rollup. A child below threshold
    // is rewritten to its parent (keeping its contacts together at a coarser
    // segment) instead of dumped to Other. Only buckets that are still below
    // threshold after rollup — or have no surviving parent — are deleted;
    // those contacts then funnel into "Other" via the catch-all sweep.
    if (run.min_volume && run.min_volume > 0) {
        const parentOf = new Map<string, string>();
        for (const b of buckets) {
            if (b.parent_bucket && allowedNames.has(b.parent_bucket)) {
                parentOf.set(b.name, b.parent_bucket);
            }
        }

        const { data: counts } = await supabase
            .rpc('get_bucket_map_counts', { p_run_id: runId });
        const countMap = new Map<string, number>(
            (counts || []).map((c: any) => [c.bucket_name as string, Number(c.contact_count) || 0])
        );

        // Rollup order: process by current count ascending so the smallest
        // children roll up first, giving their parents a chance to cross the
        // threshold. Walk parent chains up to 4 levels (far more than the
        // model is asked to produce) to cover nested rollups without looping.
        const rollupPlan: Array<{ from: string; to: string }> = [];
        const sortedNames = [...countMap.entries()]
            .sort((a, b) => a[1] - b[1])
            .map(([name]) => name);

        for (const name of sortedNames) {
            if ((countMap.get(name) || 0) >= run.min_volume) continue;
            let current = name;
            let target = parentOf.get(current);
            let hops = 0;
            while (target && hops < 4 && (countMap.get(target) || 0) < run.min_volume) {
                current = target;
                target = parentOf.get(current);
                hops++;
            }
            if (target) {
                rollupPlan.push({ from: name, to: target });
                countMap.set(target, (countMap.get(target) || 0) + (countMap.get(name) || 0));
                countMap.set(name, 0);
            }
        }

        for (const r of rollupPlan) {
            const { error: upErr } = await supabase.from('bucket_industry_map')
                .update({ bucket_name: r.to })
                .eq('bucketing_run_id', runId).eq('bucket_name', r.from);
            if (upErr) throw new Error(`rollup failed: ${upErr.message}`);
        }
        if (rollupPlan.length > 0) {
            log(`[Bucketing ${runId}] rolled up ${rollupPlan.length} sub-bucket(s) into parents below min_volume=${run.min_volume}`);
        }

        // Anything still below threshold (no parent or parent also undersized) → delete, lands in Other.
        const { data: freshCounts } = await supabase
            .rpc('get_bucket_map_counts', { p_run_id: runId });
        const undersized = (freshCounts || [])
            .filter((c: any) => Number(c.contact_count) < run.min_volume)
            .map((c: any) => c.bucket_name);
        if (undersized.length > 0) {
            await supabase.from('bucket_industry_map').delete()
                .eq('bucketing_run_id', runId).in('bucket_name', undersized);
            log(`[Bucketing ${runId}] dropped ${undersized.length} buckets below min_volume=${run.min_volume} (→ Other)`);
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
                    // strict=true: every property must be in required
                    required: ['industry', 'bucket_name', 'confidence', 'rationale'],
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
    // and diff client-side. Use the paginated vocabulary fetcher so we don't
    // silently cap at 1000 residuals — on long-tail lists that truncation
    // would leave tens of thousands of contacts stranded at Phase 2.
    const vocab = await fetchFullVocabulary(supabase, listNames);

    const { data: mapRows, error: mErr } = await supabase
        .from('bucket_industry_map').select('industry_string')
        .eq('bucketing_run_id', runId);
    if (mErr) throw new Error(`map read failed: ${mErr.message}`);

    const mapped = new Set((mapRows || []).map((r: any) => r.industry_string));
    return vocab.map(r => r.industry).filter(ind => !mapped.has(ind));
}

async function getSamplesForIndustries(
    supabase: SupabaseClient,
    industries: string[],
    listNames: string[]
): Promise<Map<string, VocabRow>> {
    const vocab = await fetchFullVocabulary(supabase, listNames);
    const map = new Map<string, VocabRow>();
    const wanted = new Set(industries);
    for (const r of vocab) {
        if (wanted.has(r.industry)) map.set(r.industry, r);
    }
    return map;
}

function computeCost(model: string, promptTokens: number, completionTokens: number): number {
    const p = PRICING[model] || PRICING['gpt-4.1-mini'];
    return (promptTokens / 1_000_000) * p.input + (completionTokens / 1_000_000) * p.output;
}
