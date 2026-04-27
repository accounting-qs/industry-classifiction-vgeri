/**
 * Bucketing Service v2 — discovery + matching with 3-level chain.
 *
 * Phase 1a (DISCOVERY, ONE call):
 *   Pulls full vocabulary, sends to Sonnet (or gpt-4.1 fallback) with the
 *   Project Context + the user's discovery prompt. Model returns:
 *     - 30–60 leaf buckets, each with direct_ancestor + root_category
 *     - 6–15 ancestors, 3–6 roots
 *     - observed_patterns (sanity-check section)
 *   Optionally seeded with PREFERRED_BUCKETS from the bucket_library so the
 *   model reuses them at high alignment instead of inventing duplicates.
 *
 * Phase 1b (MATCHING, per distinct industry):
 *   Per-string call to gpt-4.1-mini that returns the full chain
 *   {leaf, ancestor, root, score, reason} + generic/disqualified flags.
 *   Optimizations:
 *     - Embedding pre-filter (text-embedding-3-small) auto-assigns obvious
 *       matches with cosine ≥ 0.85 + ≥ 0.10 margin → no LLM call.
 *     - Remaining strings batched 8 per call.
 *     - Concurrency 40 (configurable).
 *     - Prompt caching: bucket_reference is the prefix, queries are the tail.
 *
 * Volume rollup (no LLM): a SQL RPC walks each row of bucket_industry_map
 * and writes the EFFECTIVE bucket_name based on min_volume thresholds —
 * leaf, then ancestor, then root, then "Generic". Disqualified rows go to
 * "Disqualified" unconditionally.
 *
 * Final fan-out: existing bucketing_deterministic_fanout RPC writes one
 * bucket_assignments row per contact, including the full chain.
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import Anthropic from '@anthropic-ai/sdk';

// ─── model + concurrency config ──────────────────────────────────────
const TAXONOMY_PROVIDER = (process.env.BUCKETING_TAXONOMY_PROVIDER || 'anthropic').toLowerCase(); // 'anthropic' | 'openai'
const TAXONOMY_MODEL_ANTHROPIC = process.env.BUCKETING_TAXONOMY_MODEL_ANTHROPIC || 'claude-sonnet-4-6';
const TAXONOMY_MODEL_OPENAI = process.env.BUCKETING_TAXONOMY_MODEL_OPENAI || 'gpt-4.1';
const MATCH_MODEL = process.env.BUCKETING_MATCH_MODEL || 'gpt-4.1-mini';
const EMBEDDING_MODEL = process.env.BUCKETING_EMBEDDING_MODEL || 'text-embedding-3-small';

const MATCH_BATCH_SIZE = parseInt(process.env.BUCKETING_BATCH_SIZE || '8', 10);
const MATCH_CONCURRENCY = parseInt(process.env.BUCKETING_CONCURRENCY_AI || '40', 10);
const EMBED_PREFILTER_ENABLED = (process.env.BUCKETING_EMBED_PREFILTER || 'true').toLowerCase() !== 'false';
const EMBED_AUTO_THRESHOLD = parseFloat(process.env.BUCKETING_EMBED_AUTO_THRESHOLD || '0.85');
const EMBED_MARGIN = parseFloat(process.env.BUCKETING_EMBED_MARGIN || '0.10');

const OPENAI_TIMEOUT_MS = 90_000;
const TAXONOMY_TIMEOUT_MS = 180_000;

// ─── pricing ─────────────────────────────────────────────────────────
const OPENAI_PRICING: Record<string, { input: number; output: number; cached_input?: number }> = {
    'gpt-4.1':       { input: 2.00,  output: 8.00,  cached_input: 0.50 },
    'gpt-4.1-mini':  { input: 0.40,  output: 1.60,  cached_input: 0.10 },
    'gpt-4.1-nano':  { input: 0.10,  output: 0.40,  cached_input: 0.025 },
};
const ANTHROPIC_PRICING: Record<string, { input: number; output: number }> = {
    'claude-sonnet-4-6': { input: 3.00, output: 15.00 },
    'claude-opus-4-7':   { input: 15.00, output: 75.00 },
    'claude-haiku-4-5':  { input: 1.00, output: 5.00 },
};
const EMBEDDING_PRICE_PER_1M = 0.02;

const RESERVED_GENERIC = 'Generic';
const RESERVED_DISQUALIFIED = 'Disqualified';
const RESERVED = new Set([RESERVED_GENERIC.toLowerCase(), RESERVED_DISQUALIFIED.toLowerCase(), 'other']);

// ─── env helpers ─────────────────────────────────────────────────────
function getOpenAIKey(): string {
    const key = process.env.VITE_OPENAI_API_KEY || process.env.OPENAI_API_KEY;
    if (!key) throw new Error('VITE_OPENAI_API_KEY missing');
    return key.trim();
}
function getAnthropicKey(): string | null {
    const key = process.env.ANTHROPIC_API_KEY || process.env.VITE_ANTHROPIC_API_KEY;
    return key ? key.trim() : null;
}

let _anthropicClient: Anthropic | null = null;
function getAnthropic(): Anthropic | null {
    if (_anthropicClient) return _anthropicClient;
    const key = getAnthropicKey();
    if (!key) return null;
    _anthropicClient = new Anthropic({ apiKey: key });
    return _anthropicClient;
}

// ─── shared types ────────────────────────────────────────────────────
interface VocabRow {
    industry: string;
    n: number;
    avg_conf: number;
    sample_companies: string[] | null;
    sample_reasoning: string[] | null;
}

interface DiscoveredBucket {
    bucket_name: string;
    description: string;
    direct_ancestor: string;
    root_category: string;
    include?: string[];
    exclude?: string[];
    example_strings?: string[];
    estimated_usage_label?: string;
    rough_volume_estimate?: string;
    library_match_id?: string | null;
}

interface DiscoveryOutput {
    observed_patterns: string[];
    buckets: DiscoveredBucket[];
}

interface MatchChain {
    bucket_1: { name: string; score: number; reason: string };
    bucket_2: { name: string; score: number; reason: string };
    bucket_3: { name: string; score: number; reason: string };
    generic: boolean;
    disqualified: boolean;
}

// ─── paginated vocabulary fetch ──────────────────────────────────────
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

// ─── Project Context (system role for both phases) ──────────────────
const PROJECT_CONTEXT = `<<<SYSTEM ROLE AND CONTEXT

You are operating inside a revenue-critical B2B growth system.
Your outputs directly influence who is invited to high-volume webinars,
how those webinars are positioned, and ultimately revenue outcomes.

This is NOT an academic taxonomy exercise.
This is NOT a generic classification task.
This is NOT about elegance, novelty, or theoretical completeness.

This system exists to solve a very specific operational problem at scale.

========================================
BACKGROUND
========================================

We run live B2B webinars at very large scale.
Invitations are sent via Google Calendar to tens or hundreds of thousands of founders.

Current state:
- Invitations are largely generic.
- Industry personalization is shallow or inaccurate.
- Lead lists (e.g., from Apollo) have noisy, unreliable industry tags.
- Attendance rates are lower than they should be.

We cannot personalize per individual.
However, we CAN personalize per industry bucket,
where each bucket supports thousands of invitees.

========================================
CORE GOAL
========================================

Increase webinar ATTENDANCE RATE (not opens, not clicks)
by making the invitation feel highly relevant to the recipient's industry context.

Relevance is driven by:
- Industry specificity
- Accurate sub-vertical framing
- Avoiding obvious mismatches

Success is measured downstream by:
- Attendance rate
- Show-up quality
- Call bookings
- Conversion efficiency

NOT by:
- Number of buckets
- Clever naming
- Taxonomic purity

========================================
IDEAL CLIENT PROFILE (ICP)
========================================

The system is designed for B2B, high-ticket businesses.

Strong ICP:
- Agencies (marketing, performance, CRO, dev, etc.)
- Consulting / advisory firms
- Professional services
- B2B or enterprise SaaS (including sub-verticals)
- High-ticket info-product businesses (when plausibly scalable)
- Financial services and investment firms (advisors, M&A, private equity, funds)

Explicitly NON-ICP:
- Ecommerce / DTC physical products
- Local services tied to geography
- Brick-and-mortar retail
- Low-ticket consumer businesses

Disqualification must be CONSERVATIVE.
False negatives are worse than false positives.
If ambiguous, do NOT disqualify.

========================================
CRITICAL CONSTRAINTS
========================================

1. Accuracy > Coverage. Do not force-fit businesses into buckets.
2. Specificity > Breadth. Buckets must feel "this is for me".
3. Reusability > Novelty. Buckets must work across multiple lists and campaigns.
4. Structure > Guessing. Ancestor relationships must be explicit and logical.
5. Determinism > Creativity. Predictable behavior at scale.`;

// ────────────────────────────────────────────────────────────────────
// PHASE 1A — DISCOVERY
// ────────────────────────────────────────────────────────────────────

export async function runTaxonomyProposal(
    supabase: SupabaseClient,
    runId: string,
    log: (msg: string, level?: 'info' | 'warn' | 'error') => void
): Promise<void> {
    log(`[Bucketing ${runId}] Phase 1a: starting discovery`);

    const { data: run, error: runErr } = await supabase
        .from('bucketing_runs').select('*').eq('id', runId).single();
    if (runErr || !run) throw new Error(`Run not found: ${runErr?.message}`);

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

    // Optional preferred buckets from library (set on the run row at creation time)
    const preferredIds: string[] = Array.isArray(run.preferred_library_ids) ? run.preferred_library_ids : [];
    let preferred: any[] = [];
    if (preferredIds.length > 0) {
        const { data: libRows } = await supabase
            .from('bucket_library').select('*').in('id', preferredIds);
        preferred = libRows || [];
    }

    const t0 = Date.now();
    const { discovery, costUsd, modelUsed } = await callDiscoveryLLM(vocabRows, preferred);
    log(`[Bucketing ${runId}] discovery LLM (${modelUsed}): ${(Date.now() - t0) / 1000}s, $${costUsd.toFixed(4)}, ${discovery.buckets.length} leaves`);

    // Validate: every leaf has a non-empty ancestor; reject reserved names.
    const leaves = discovery.buckets.filter(b => {
        const n = (b.bucket_name || '').trim();
        return n && !RESERVED.has(n.toLowerCase());
    }).map(b => ({
        ...b,
        bucket_name: b.bucket_name.trim(),
        description: (b.description || '').trim(),
        direct_ancestor: (b.direct_ancestor || '').trim(),
        root_category: (b.root_category || '').trim(),
        include: Array.from(new Set((b.include || []).map(s => (s || '').trim()).filter(Boolean))),
        exclude: Array.from(new Set((b.exclude || []).map(s => (s || '').trim()).filter(Boolean))),
        example_strings: Array.from(new Set((b.example_strings || []).map(s => (s || '').trim()).filter(Boolean)))
    }));

    // Persist discovery output
    await supabase.from('bucketing_runs').update({
        taxonomy_proposal: { observed_patterns: discovery.observed_patterns || [], buckets: leaves },
        taxonomy_model: modelUsed,
        total_contacts: totalContacts,
        cost_usd: costUsd,
        status: 'taxonomy_ready',
        taxonomy_completed_at: new Date().toISOString()
    }).eq('id', runId);

    // Library link rows (which library buckets were reused)
    if (preferred.length > 0) {
        const reusedNames = new Set(leaves.filter(l => l.library_match_id).map(l => l.library_match_id!));
        if (reusedNames.size > 0) {
            const links = leaves.filter(l => l.library_match_id).map(l => ({
                bucketing_run_id: runId,
                library_bucket_id: l.library_match_id!,
                bucket_name_in_run: l.bucket_name
            }));
            await supabase.from('bucket_library_run_links').upsert(links, { onConflict: 'bucketing_run_id,library_bucket_id' });
            // Bump usage stats
            for (const id of reusedNames) {
                await supabase.from('bucket_library')
                    .update({ times_used: (preferred.find((p: any) => p.id === id)?.times_used || 0) + 1, last_used_at: new Date().toISOString() })
                    .eq('id', id);
            }
        }
    }

    log(`[Bucketing ${runId}] Phase 1a done — ${leaves.length} leaves persisted`);
}

async function callDiscoveryLLM(
    vocabRows: VocabRow[],
    preferred: any[]
): Promise<{ discovery: DiscoveryOutput; costUsd: number; modelUsed: string }> {
    const HEAD_LIMIT = 2500;
    const head = vocabRows.slice(0, HEAD_LIMIT);
    const tail = vocabRows.slice(HEAD_LIMIT);
    const tailContacts = tail.reduce((s, r) => s + Number(r.n || 0), 0);
    const headContacts = head.reduce((s, r) => s + Number(r.n || 0), 0);
    const tailExamples = tail.slice(0, 50).map(r => r.industry);

    const vocabularyTable = head.map(r => {
        const samples = (r.sample_companies || []).slice(0, 2).filter(Boolean).join(' | ');
        const reason = (r.sample_reasoning || [])[0] || '';
        const trimmedReason = reason.length > 120 ? reason.slice(0, 120) + '…' : reason;
        return `${r.industry} | n=${r.n} | companies=[${samples}] | reasoning="${trimmedReason}"`;
    }).join('\n');

    const tailSection = tail.length > 0
        ? `\n\nTail (${tail.length} more labels covering ${tailContacts} contacts). Examples: ${tailExamples.join(', ')}.`
        : '';

    const preferredSection = preferred.length > 0 ? `

========================================
PREFERRED BUCKETS (from library)
========================================

The buckets below were defined in prior runs and have proven useful.
If a discovered pattern aligns with one of these at score >= 0.7,
REUSE that exact bucket: copy its bucket_name, description,
direct_ancestor, and root_category VERBATIM, and set library_match_id
to the id provided. Do NOT create a near-duplicate with different
wording. Otherwise, propose new buckets that follow the rules above.
Treat preferred buckets as anchors.

${preferred.map(p => `id=${p.id} | name="${p.bucket_name}" | ancestor="${p.direct_ancestor}" | root="${p.root_category}" | desc="${p.description || ''}"`).join('\n')}` : '';

    const userPrompt = `${PROJECT_CONTEXT}

========================================
PHASE 1A — BUCKET DISCOVERY
========================================

Your job is to DISCOVER a set of reusable, meaningful LEAF buckets from the
dataset of AI-enriched industry classification strings below.

NO-SHORTCUTS RULES:
1) Base buckets ONLY on patterns that appear in the provided data.
2) Do NOT infer industries that are not evidenced in the input strings.
3) Do NOT use "common sense" priors unless the data shows it.
4) Do NOT compress the problem by ignoring the dataset.
5) If a classification is ambiguous, treat it as ambiguous — do not guess.

ICP BOUNDARIES:
- Strong ICP: agencies, consulting/advisory, professional services (non-local),
  B2B/enterprise SaaS, financial services & investment firms, plausibly scalable
  high-ticket info products.
- Non-ICP (route to Generic, NOT to ICP buckets): ecommerce/DTC physical products,
  local services, brick-and-mortar retail, low-ticket consumer.

BUCKET DESIGN:
- Output between 30 and 60 LEAF buckets.
- Specific enough for "this is for me" effect in webinar titles. Not so micro
  that they apply to a handful of records.
- Do NOT create near-duplicates that differ only in word order or synonyms.
- Each leaf MUST have a direct_ancestor (broader, shared roll-up) and a
  root_category (3–6 family-level categories).
- Target: 6–15 ancestors total, 3–6 roots total. Many leaves share an ancestor.
- An ancestor with only 1 leaf under it is invalid.

❌ TOO BROAD (forbidden as leaves; allowed as ancestors/roots only):
"SaaS", "B2B SaaS", "Marketing Agency", "Consulting Firm", "Financial Services",
"Professional Services", "Technology Company".

❌ TOO NARROW (forbidden):
"AI-powered Stripe reconciliation SaaS for EU neobanks",
"TikTok ads agency for DTC candle brands",
"RevOps consulting for Series B HR SaaS companies only",
"Family office for German real estate developers".

✅ GOLDILOCKS (target leaves):
"Payments Infrastructure SaaS", "FinTech SaaS", "Performance Marketing Agency",
"Conversion Rate Optimization (CRO) Agency", "Revenue Operations Consulting",
"Fractional CFO Services", "Private Equity Firm", "Venture Capital Fund",
"M&A Advisory Services", "MarTech SaaS", "Vertical SaaS",
"B2B Demand Generation Agency", "Branding & Creative Agency".
${preferredSection}

REQUIRED PROCESS — DO NOT SKIP:
A) Identify 10–15 high-frequency patterns observed in the dataset.
   Each must reference recurring concepts/phrases you actually see in the data.
B) Use those patterns to justify why your top leaves are ranked highest.

OUTPUT (strict JSON only, no prose, no markdown fences):

{
  "observed_patterns": [<10–15 strings>],
  "buckets": [
    {
      "bucket_name": "<leaf>",
      "description": "<1 sentence>",
      "direct_ancestor": "<ancestor>",
      "root_category": "<root>",
      "include": [<string keywords>],
      "exclude": [<string keywords>],
      "example_strings": [<6–10 verbatim from vocab below>],
      "estimated_usage_label": "<dominant|very_common|common|moderate|niche_but_meaningful|rare>",
      "rough_volume_estimate": "<e.g. ~8–12% of rows>",
      "library_match_id": "<id from PREFERRED_BUCKETS, or empty string>"
    }
  ]
}

Rules for output:
- buckets ordered MOST common → LEAST common.
- example_strings MUST be verbatim from the vocabulary below.
- 30–60 leaves total. Every leaf has direct_ancestor and root_category set.

========================================
VOCABULARY
========================================

The head below covers ${headContacts} contacts across ${head.length} distinct labels.
Format: industry | n=count | companies=[2 samples] | reasoning="…"

${vocabularyTable}${tailSection}

Total contacts across all labels: ${headContacts + tailContacts}.

Now produce the JSON.`;

    // Try Anthropic Sonnet first if configured + key present.
    if (TAXONOMY_PROVIDER === 'anthropic') {
        const client = getAnthropic();
        if (client) {
            try {
                const t0 = Date.now();
                const resp = await client.messages.create({
                    model: TAXONOMY_MODEL_ANTHROPIC,
                    max_tokens: 16_000,
                    temperature: 0.2,
                    system: 'You output strict JSON only. No markdown, no prose, no fences. Just the JSON object.',
                    messages: [{ role: 'user', content: userPrompt }]
                }, { timeout: TAXONOMY_TIMEOUT_MS });
                const text = resp.content
                    .filter((c: any) => c.type === 'text')
                    .map((c: any) => c.text).join('');
                const discovery = parseDiscoveryJson(text);
                const usage = (resp as any).usage || {};
                const inTok = (usage.input_tokens || 0) + (usage.cache_creation_input_tokens || 0);
                const outTok = usage.output_tokens || 0;
                const costUsd = computeAnthropicCost(TAXONOMY_MODEL_ANTHROPIC, inTok, outTok);
                console.log(`[Bucketing] Sonnet discovery: ${(Date.now() - t0) / 1000}s, in=${inTok} out=${outTok}`);
                return { discovery, costUsd, modelUsed: TAXONOMY_MODEL_ANTHROPIC };
            } catch (err: any) {
                console.warn(`[Bucketing] Anthropic discovery failed, falling back to OpenAI: ${err.message}`);
                // fall through to OpenAI
            }
        }
    }

    // OpenAI fallback (or default if provider=openai)
    const schema = buildDiscoverySchema();
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
                model: TAXONOMY_MODEL_OPENAI,
                messages: [
                    { role: 'system', content: 'You output strict JSON only. No markdown, no prose, no fences.' },
                    { role: 'user', content: userPrompt }
                ],
                response_format: {
                    type: 'json_schema',
                    json_schema: { name: 'bucket_discovery', strict: true, schema }
                },
                temperature: 0.2
            }),
            signal: controller.signal
        });
        if (!response.ok) {
            const errBody = await response.text().catch(() => '');
            throw new Error(`Discovery LLM error ${response.status}: ${errBody.slice(0, 500)}`);
        }
        const data = await response.json();
        const text = data.choices?.[0]?.message?.content;
        if (!text) throw new Error('Empty discovery response');
        const discovery = parseDiscoveryJson(text);
        const usage = data.usage || {};
        const cached = usage.prompt_tokens_details?.cached_tokens || 0;
        const uncached = (usage.prompt_tokens || 0) - cached;
        const costUsd = computeOpenAICost(TAXONOMY_MODEL_OPENAI, uncached, cached, usage.completion_tokens || 0);
        return { discovery, costUsd, modelUsed: TAXONOMY_MODEL_OPENAI };
    } finally {
        clearTimeout(timeoutId);
    }
}

function parseDiscoveryJson(text: string): DiscoveryOutput {
    const trimmed = text.trim().replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/, '');
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed.buckets)) throw new Error('Discovery output missing `buckets` array');
    return parsed;
}

function buildDiscoverySchema() {
    return {
        type: 'object',
        additionalProperties: false,
        required: ['observed_patterns', 'buckets'],
        properties: {
            observed_patterns: { type: 'array', items: { type: 'string' } },
            buckets: {
                type: 'array',
                items: {
                    type: 'object',
                    additionalProperties: false,
                    required: ['bucket_name', 'description', 'direct_ancestor', 'root_category',
                               'include', 'exclude', 'example_strings', 'estimated_usage_label',
                               'rough_volume_estimate', 'library_match_id'],
                    properties: {
                        bucket_name: { type: 'string' },
                        description: { type: 'string' },
                        direct_ancestor: { type: 'string' },
                        root_category: { type: 'string' },
                        include: { type: 'array', items: { type: 'string' } },
                        exclude: { type: 'array', items: { type: 'string' } },
                        example_strings: { type: 'array', items: { type: 'string' } },
                        estimated_usage_label: { type: 'string' },
                        rough_volume_estimate: { type: 'string' },
                        library_match_id: { type: 'string' }
                    }
                }
            }
        }
    };
}

// ────────────────────────────────────────────────────────────────────
// EDIT APPLICATION
// ────────────────────────────────────────────────────────────────────

interface TaxonomyEdits {
    keep?: string[];
    rename?: Record<string, string>;
    add?: { bucket_name: string; description: string; direct_ancestor: string; root_category: string }[];
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
    const proposal = run.taxonomy_proposal as DiscoveryOutput | null;
    if (!proposal?.buckets) throw new Error('Taxonomy proposal missing — run Phase 1a first');

    let leaves = proposal.buckets.map(b => ({ ...b }));

    if (edits.rename) {
        for (const [oldName, newName] of Object.entries(edits.rename)) {
            const target = newName.trim();
            if (!target || RESERVED.has(target.toLowerCase())) continue;
            leaves = leaves.map(b => b.bucket_name === oldName ? { ...b, bucket_name: target } : b);
        }
    }

    if (edits.keep) {
        const keepSet = new Set(edits.keep.map(s => s.trim()));
        leaves = leaves.filter(b => keepSet.has(b.bucket_name));
    }

    if (edits.add) {
        for (const a of edits.add) {
            const name = (a.bucket_name || '').trim();
            if (!name || RESERVED.has(name.toLowerCase())) continue;
            if (leaves.some(l => l.bucket_name === name)) continue;
            leaves.push({
                bucket_name: name,
                description: (a.description || '').trim(),
                direct_ancestor: (a.direct_ancestor || '').trim(),
                root_category: (a.root_category || '').trim(),
                include: [],
                exclude: [],
                example_strings: []
            });
        }
    }

    const update: any = { taxonomy_final: { observed_patterns: proposal.observed_patterns || [], buckets: leaves } };
    if (typeof edits.min_volume === 'number' && edits.min_volume >= 0) {
        update.min_volume = edits.min_volume;
    }
    await supabase.from('bucketing_runs').update(update).eq('id', runId);
    log(`[Bucketing ${runId}] taxonomy edits applied: ${leaves.length} leaves`);
}

// ────────────────────────────────────────────────────────────────────
// PHASE 1B — MATCHING + VOLUME ROLLUP + FAN-OUT
// ────────────────────────────────────────────────────────────────────

export async function runAssignment(
    supabase: SupabaseClient,
    runId: string,
    log: (msg: string, level?: 'info' | 'warn' | 'error') => void
): Promise<void> {
    const { data: run, error } = await supabase.from('bucketing_runs').select('*').eq('id', runId).single();
    if (error || !run) throw new Error(`Run not found: ${error?.message}`);

    const final = (run.taxonomy_final || run.taxonomy_proposal) as DiscoveryOutput | null;
    if (!final?.buckets || final.buckets.length === 0) {
        throw new Error('No buckets defined for assignment');
    }
    const leaves = final.buckets;
    const leafByName = new Map(leaves.map(b => [b.bucket_name, b]));

    await supabase.from('bucketing_runs').update({ status: 'assigning' }).eq('id', runId);

    let totalCost = Number(run.cost_usd || 0);

    // Step 1: get distinct industries that need a chain assignment.
    log(`[Bucketing ${runId}] step 1/5: load vocabulary`);
    const vocab = await fetchFullVocabulary(supabase, run.list_names);
    log(`[Bucketing ${runId}] ${vocab.length} distinct industries to match`);

    // Step 2: clear any prior chain rows for this run so re-runs are clean.
    await supabase.from('bucket_industry_map').delete().eq('bucketing_run_id', runId);

    // Step 3: embedding pre-filter — auto-assign obvious matches with cosine.
    let assignedRows: any[] = [];
    let pendingIndustries: VocabRow[] = vocab;

    if (EMBED_PREFILTER_ENABLED) {
        log(`[Bucketing ${runId}] step 3/5: embedding pre-filter`);
        const embedRes = await runEmbeddingPrefilter(vocab, leaves, runId);
        totalCost += embedRes.costUsd;
        assignedRows = embedRes.autoAssigned;
        pendingIndustries = embedRes.pending;
        log(`[Bucketing ${runId}] embedding auto-assigned ${assignedRows.length}/${vocab.length}, ${pendingIndustries.length} pending`);
    }

    // Step 4: residual LLM matching (batched, concurrent).
    log(`[Bucketing ${runId}] step 4/5: residual LLM matching`);
    const llmRes = await runMatchingLLM(pendingIndustries, leaves, runId);
    totalCost += llmRes.costUsd;
    assignedRows = assignedRows.concat(llmRes.rows);
    log(`[Bucketing ${runId}] LLM matched ${llmRes.rows.length}, total chain rows ${assignedRows.length}, cost so far $${totalCost.toFixed(4)}`);

    // Insert chain rows into bucket_industry_map (in chunks).
    if (assignedRows.length > 0) {
        for (let i = 0; i < assignedRows.length; i += 1000) {
            const chunk = assignedRows.slice(i, i + 1000);
            const { error: upErr } = await supabase.from('bucket_industry_map')
                .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
            if (upErr) throw new Error(`map insert failed: ${upErr.message}`);
        }
    }

    // Step 5: volume rollup (server-side SQL) → writes effective bucket_name per row.
    log(`[Bucketing ${runId}] step 5/5: volume rollup + fan-out`);
    const { error: rollupErr } = await supabase.rpc('bucketing_apply_volume_rollup', { p_run_id: runId });
    if (rollupErr) throw new Error(`volume rollup failed: ${rollupErr.message}`);

    // Clear prior assignments (idempotent re-run support) then fan out.
    await supabase.from('bucket_assignments').delete().eq('bucketing_run_id', runId);
    const { data: fanoutCount, error: fanErr } = await supabase
        .rpc('bucketing_deterministic_fanout', { p_run_id: runId });
    if (fanErr) throw new Error(`fanout failed: ${fanErr.message}`);
    log(`[Bucketing ${runId}] fan-out wrote ${Number(fanoutCount || 0)} assignments`);

    // Catch-all: contacts with industries we never saw (e.g. NULL industry) → Generic
    const { error: catchErr } = await supabase.rpc('bucketing_catchall_other', { p_run_id: runId });
    if (catchErr) throw new Error(`catch-all failed: ${catchErr.message}`);

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

// ─── embedding pre-filter ──────────────────────────────────────────
async function runEmbeddingPrefilter(
    vocab: VocabRow[],
    leaves: DiscoveredBucket[],
    runId: string
): Promise<{ autoAssigned: any[]; pending: VocabRow[]; costUsd: number }> {
    if (vocab.length === 0 || leaves.length === 0) return { autoAssigned: [], pending: vocab, costUsd: 0 };

    const leafTexts = leaves.map(l => `${l.bucket_name}: ${l.description || l.bucket_name}. Includes: ${(l.include || []).join(', ')}.`);
    const vocabTexts = vocab.map(v => v.industry);
    const allInputs = [...leafTexts, ...vocabTexts];

    const { embeddings, costUsd } = await embedBatch(allInputs);
    const leafVecs = embeddings.slice(0, leaves.length);
    const vocabVecs = embeddings.slice(leaves.length);

    const autoAssigned: any[] = [];
    const pending: VocabRow[] = [];

    for (let i = 0; i < vocab.length; i++) {
        const sims = leafVecs.map(lv => cosine(vocabVecs[i], lv));
        const sorted = sims.map((s, j) => ({ s, j })).sort((a, b) => b.s - a.s);
        const top = sorted[0];
        const second = sorted[1] || { s: 0 };
        if (top.s >= EMBED_AUTO_THRESHOLD && (top.s - second.s) >= EMBED_MARGIN) {
            const leaf = leaves[top.j];
            autoAssigned.push({
                bucketing_run_id: runId,
                industry_string: vocab[i].industry,
                bucket_name: leaf.bucket_name,
                source: 'embedding',
                confidence: Number(top.s.toFixed(2)),
                bucket_leaf: leaf.bucket_name,
                bucket_ancestor: leaf.direct_ancestor || '',
                bucket_root: leaf.root_category || '',
                leaf_score: Number(top.s.toFixed(2)),
                ancestor_score: Number(top.s.toFixed(2)),
                root_score: Number(top.s.toFixed(2)),
                is_generic: false,
                is_disqualified: false,
                reasons: { auto: 'embedding pre-filter', cosine: top.s }
            });
        } else {
            pending.push(vocab[i]);
        }
    }
    return { autoAssigned, pending, costUsd };
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
                headers: { Authorization: `Bearer ${getOpenAIKey()}`, 'Content-Type': 'application/json' },
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

// ─── residual LLM matching ─────────────────────────────────────────
async function runMatchingLLM(
    pending: VocabRow[],
    leaves: DiscoveredBucket[],
    runId: string
): Promise<{ rows: any[]; costUsd: number }> {
    if (pending.length === 0) return { rows: [], costUsd: 0 };

    const validLeafNames = new Set(leaves.map(l => l.bucket_name));
    const ancestorByLeaf = new Map(leaves.map(l => [l.bucket_name, l.direct_ancestor || '']));
    const rootByLeaf = new Map(leaves.map(l => [l.bucket_name, l.root_category || '']));

    // Bucket reference (the cacheable prefix)
    const bucketReferenceJson = JSON.stringify(leaves.map(l => ({
        bucket_name: l.bucket_name,
        description: l.description,
        direct_ancestor: l.direct_ancestor,
        root_category: l.root_category,
        include: l.include || [],
        exclude: l.exclude || [],
        example_strings: (l.example_strings || []).slice(0, 6)
    })));

    const limit = pLimit(MATCH_CONCURRENCY);
    const batches: VocabRow[][] = [];
    for (let i = 0; i < pending.length; i += MATCH_BATCH_SIZE) {
        batches.push(pending.slice(i, i + MATCH_BATCH_SIZE));
    }

    let totalCost = 0;
    const rows: any[] = [];
    const rowsLock: any[] = []; // not actually needed since pushes are JS-atomic, but kept for clarity

    await Promise.all(batches.map(batch => limit(async () => {
        const { results, costUsd } = await classifyBatch(batch, bucketReferenceJson, validLeafNames);
        totalCost += costUsd;
        for (let i = 0; i < batch.length; i++) {
            const ind = batch[i].industry;
            const r = results[i] || makeFallbackChain();
            const leafOk = r.bucket_1.name && validLeafNames.has(r.bucket_1.name) && r.bucket_1.score >= 0.55;
            const leafName = leafOk ? r.bucket_1.name : '';
            const ancestor = leafOk ? (ancestorByLeaf.get(leafName) || r.bucket_2.name) : (r.bucket_2.name || '');
            const root = leafOk ? (rootByLeaf.get(leafName) || r.bucket_3.name) : (r.bucket_3.name || '');
            rows.push({
                bucketing_run_id: runId,
                industry_string: ind,
                bucket_name: leafName || (r.disqualified ? RESERVED_DISQUALIFIED : RESERVED_GENERIC), // pre-rollup placeholder
                source: 'llm_phase1b',
                confidence: Number((r.bucket_1.score || 0).toFixed(2)),
                bucket_leaf: leafName,
                bucket_ancestor: ancestor,
                bucket_root: root,
                leaf_score: r.bucket_1.score,
                ancestor_score: r.bucket_2.score,
                root_score: r.bucket_3.score,
                is_generic: !!r.generic && !leafOk,
                is_disqualified: !!r.disqualified,
                reasons: {
                    leaf: r.bucket_1.reason,
                    ancestor: r.bucket_2.reason,
                    root: r.bucket_3.reason
                }
            });
        }
    })));

    return { rows, costUsd: totalCost };
}

function makeFallbackChain(): MatchChain {
    return {
        bucket_1: { name: '', score: 0, reason: 'fallback' },
        bucket_2: { name: '', score: 0, reason: 'fallback' },
        bucket_3: { name: '', score: 0, reason: 'fallback' },
        generic: true,
        disqualified: false
    };
}

async function classifyBatch(
    batch: VocabRow[],
    bucketReferenceJson: string,
    validLeafNames: Set<string>
): Promise<{ results: MatchChain[]; costUsd: number }> {
    const systemPrompt = `${PROJECT_CONTEXT}

========================================
PHASE 1B — BUCKET MATCHING
========================================

You are matching company classification strings to an existing industry taxonomy.
This output measures bucket volumes and enables roll-ups via ancestors.
NOT a creative writing task.

RULES:
- Base each decision ONLY on the classification string and the bucket definitions.
- Do not guess what the company does beyond the text.
- bucket_1 MUST be a LEAF bucket from the BUCKET_REFERENCE.
- bucket_2 MUST be the direct_ancestor of bucket_1.
- bucket_3 MUST be the root_category of bucket_1 (if defined), else "".
- bucket_2_score <= bucket_1_score.
- bucket_3_score <= bucket_2_score.
- If no leaf aligns at >= 0.55, leave bucket_1.name empty and set generic=true.
- Disqualify ONLY for clear ecommerce/DTC physical products, local services tied to
  geography, brick-and-mortar retail, or low-ticket consumer. Ambiguous → Generic.
- Reasons: max 18 words each, must cite a phrase from the classification.

Return strict JSON, no prose, no markdown fences.`;

    const userPrompt = `BUCKET_REFERENCE:
${bucketReferenceJson}

COMPANIES_TO_CLASSIFY (array of classification strings, in order):
${JSON.stringify(batch.map(b => b.industry))}

Return JSON: { "assignments": [<one object per company in the same order>] }
Each assignment object:
{
  "bucket_1": {"name": "<leaf or empty>", "score": 0.00, "reason": ""},
  "bucket_2": {"name": "<ancestor or empty>", "score": 0.00, "reason": ""},
  "bucket_3": {"name": "<root or empty>", "score": 0.00, "reason": ""},
  "generic": false,
  "disqualified": false
}`;

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
                    required: ['bucket_1', 'bucket_2', 'bucket_3', 'generic', 'disqualified'],
                    properties: {
                        bucket_1: chainItemSchema(),
                        bucket_2: chainItemSchema(),
                        bucket_3: chainItemSchema(),
                        generic: { type: 'boolean' },
                        disqualified: { type: 'boolean' }
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
                headers: { Authorization: `Bearer ${getOpenAIKey()}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    model: MATCH_MODEL,
                    messages: [
                        { role: 'system', content: systemPrompt },
                        { role: 'user', content: userPrompt }
                    ],
                    response_format: {
                        type: 'json_schema',
                        json_schema: { name: 'phase1b_match', strict: true, schema }
                    },
                    temperature: 0.1
                }),
                signal: controller.signal
            });
            if (!res.ok) {
                const body = await res.text().catch(() => '');
                throw new Error(`match ${res.status}: ${body.slice(0, 300)}`);
            }
            const data = await res.json();
            const text = data.choices?.[0]?.message?.content;
            const parsed = JSON.parse(text);
            const usage = data.usage || {};
            const cached = usage.prompt_tokens_details?.cached_tokens || 0;
            const uncached = (usage.prompt_tokens || 0) - cached;
            const costUsd = computeOpenAICost(MATCH_MODEL, uncached, cached, usage.completion_tokens || 0);
            return { assignments: parsed.assignments as MatchChain[], costUsd };
        } finally {
            clearTimeout(t);
        }
    };

    let result;
    try {
        result = await callOnce();
    } catch (err) {
        // Retry once on transient failure
        result = await callOnce();
    }

    let { assignments, costUsd } = result;
    // Drift guard: bucket_1.name must be in validLeafNames OR empty
    const drift = assignments.some(a => a.bucket_1.name && !validLeafNames.has(a.bucket_1.name));
    if (drift) {
        try {
            const retried = await callOnce();
            assignments = retried.assignments;
            costUsd += retried.costUsd;
        } catch { /* keep original */ }
    }

    // Pad/truncate to batch size
    const padded: MatchChain[] = [];
    for (let i = 0; i < batch.length; i++) padded.push(assignments[i] || makeFallbackChain());
    return { results: padded, costUsd };
}

function chainItemSchema() {
    return {
        type: 'object',
        additionalProperties: false,
        required: ['name', 'score', 'reason'],
        properties: {
            name: { type: 'string' },
            score: { type: 'number' },
            reason: { type: 'string' }
        }
    };
}

// ─── pricing helpers ───────────────────────────────────────────────
function computeOpenAICost(model: string, uncachedInput: number, cachedInput: number, output: number): number {
    const p = OPENAI_PRICING[model] || OPENAI_PRICING['gpt-4.1-mini'];
    const cachedCost = (cachedInput / 1_000_000) * (p.cached_input ?? p.input);
    const uncachedCost = (uncachedInput / 1_000_000) * p.input;
    const outCost = (output / 1_000_000) * p.output;
    return cachedCost + uncachedCost + outCost;
}

function computeAnthropicCost(model: string, input: number, output: number): number {
    const p = ANTHROPIC_PRICING[model] || ANTHROPIC_PRICING['claude-sonnet-4-6'];
    return (input / 1_000_000) * p.input + (output / 1_000_000) * p.output;
}
