/**
 * Bucketing Service v2.1 — identity-first taxonomy with sector-focus metadata.
 *
 * Universal routing rule (the foundation of everything below):
 *   Classify by core business identity FIRST. Sector served SECOND. Never
 *   reverse the order. A private equity firm focused on healthcare is a PE
 *   firm — not a healthcare operator. Only operators in a sector belong in
 *   that sector's bucket.
 *
 * Phase 1a (DISCOVERY, ONE call): Sonnet (or gpt-4.1 fallback) reads the
 * vocabulary and produces:
 *   - Identity-first leaf taxonomy (30–60 leaves), each tagged with
 *     identity_type (operator | service_provider | agency | software_vendor |
 *     investor | advisor | staffing | distributor | media | other), plus
 *     priority_rank, operator_required, strong_identity_signals,
 *     weak_sector_signals, disqualifying_signals.
 *   - A separate sector-focus vocabulary (controlled list of sectors that may
 *     describe whom a company serves, NEVER what a company is).
 *
 * Phase 1b (MATCHING, per contact): gpt-4.1-mini batched 8/call,
 * concurrency 40. Uses company-specific context (name, website, enriched
 * classification, confidence, reasoning) and writes contact-level pre-rollup
 * decisions before computing final campaign buckets.
 *
 * Volume rollup: sector+specialization → specialization → identity → General.
 * Disqualified / invalid rows land in General with audit reasons.
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import Anthropic from '@anthropic-ai/sdk';
import { getSetting } from './appSettings';

// ─── HARD-CODED MODEL + CONCURRENCY CONFIG ─────────────────────────
// Per user request: no env reliance for non-secret config. Only the
// Anthropic API key is runtime-configurable (via Connectors UI / app_settings).
const TAXONOMY_MODEL_ANTHROPIC = 'claude-sonnet-4-6';
const TAXONOMY_MODEL_OPENAI = 'gpt-4.1';
const MATCH_MODEL = 'gpt-4.1-mini';
const EMBEDDING_MODEL = 'text-embedding-3-small';
const MATCH_BATCH_SIZE = 8;
const MATCH_CONCURRENCY = 40;
const EMBED_PREFILTER_ENABLED = true;
// Lowered from 0.85/0.10 — at the old thresholds, the live data showed
// "embedding auto-assigned 0/1000" and "preview embedding wrote 0 map
// rows" because narrow industry strings rarely scored above 0.85 against
// the broader spec definitions. 0.78/0.07 is still tight enough to avoid
// gross misroutes (Phase 1b LLM still gets the residuals, and the user
// reviews specs before assignment).
const EMBED_AUTO_THRESHOLD = 0.78;
const EMBED_MARGIN = 0.07;
const CONTACT_EMBED_AUTO_THRESHOLD = 0.90;
const CONTACT_EMBED_MARGIN = 0.12;
const OPENAI_TIMEOUT_MS = 90_000;
const TAXONOMY_TIMEOUT_MS = 180_000;

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

// Two reserved buckets:
//   General      — low-confidence routing, sub-threshold rollups, no good
//                  taxonomy fit. Still considered usable for outreach review.
//   Disqualified — failed enrichments, missing industries, identities the
//                  user has flagged as DQ in the taxonomy library. Excluded
//                  from outreach by default.
const RESERVED_GENERAL = 'General';
const RESERVED_DISQUALIFIED = 'Disqualified';

// Live progress + log surface used by the Bucketing UI. Server.ts builds
// one of these per run and passes it through the entry points; every step
// in Phase 1a / Phase 1b reports to it. log() persists to bucketing_run_logs;
// progress() debounces writes to bucketing_runs.progress so we don't hammer
// the DB on tight inner loops. checkCancel() reads the cancel_requested
// flag and throws BucketingCancelledError if the user clicked Stop —
// called at every phase boundary so the run shuts down cleanly.
export interface BucketingCtx {
    log(msg: string, level?: 'info' | 'warn' | 'error' | 'phase'): void;
    progress(p: ProgressUpdate): void;
    checkCancel(): Promise<void>;
}

export class BucketingCancelledError extends Error {
    constructor() { super('Cancelled by user'); this.name = 'BucketingCancelledError'; }
}

export interface ProgressUpdate {
    phase: 'phase1a' | 'phase1b' | 'edit_preview';
    step: string;                 // human-readable, e.g. "library_match"
    current?: number;             // optional progress counter
    total?: number;               // optional total for the counter
    note?: string;                // optional one-line caption
}
const RESERVED = new Set([
    'general', 'generic', 'disqualified', 'other'
]);

// Confidence floor — Sonnet returns 1-10, anything below this routes the
// industry to General with needs_qa=true so the user can review post-run.
const PHASE1A_QA_FLOOR = 6;
const PHASE1A_BATCH_SIZE = 8;
const PHASE1A_CONCURRENCY = 40;

const ANTHROPIC_KEY_NAME = 'ANTHROPIC_API_KEY';

// ─── secret access ─────────────────────────────────────────────────
function getOpenAIKey(): string {
    const key = process.env.VITE_OPENAI_API_KEY || process.env.OPENAI_API_KEY;
    if (!key) throw new Error('VITE_OPENAI_API_KEY missing');
    return key.trim();
}

async function getAnthropicKey(supabase: SupabaseClient): Promise<string | null> {
    // Prefer DB-stored key (Connectors UI). Fall back to env if set.
    const stored = await getSetting(supabase, ANTHROPIC_KEY_NAME);
    if (stored) return stored.trim();
    const envKey = process.env.ANTHROPIC_API_KEY || process.env.VITE_ANTHROPIC_API_KEY;
    return envKey ? envKey.trim() : null;
}

async function getAnthropic(supabase: SupabaseClient): Promise<Anthropic | null> {
    const key = await getAnthropicKey(supabase);
    if (!key) return null;
    return new Anthropic({ apiKey: key });
}

// ─── shared types ──────────────────────────────────────────────────
interface VocabRow {
    industry: string;
    n: number;
    enrichment_status?: string | null;   // completed | scrape_error | unenriched | failed | pending
    avg_conf: number;
    sample_companies: string[] | null;
    sample_reasoning: string[] | null;
}

interface TaxonomyEntry {
    id: string;
    name: string;
    description?: string | null;
    parent_identity?: string | null;     // characteristics only
    is_disqualified?: boolean;           // identities only
    synonyms?: string | null;            // sectors only
    created_by?: string;
    archived: boolean;
    sort_order?: number | null;
}

interface TaxonomySnapshot {
    identities: TaxonomyEntry[];
    characteristics: TaxonomyEntry[];
    sectors: TaxonomyEntry[];
}

interface IndustryTagging {
    industry: string;
    identity: string | null;
    is_new_identity: boolean;
    characteristic: string | null;
    is_new_characteristic: boolean;
    sector: string | null;
    is_new_sector: boolean;
    is_disqualified: boolean;
    confidence: number;          // 1-10
    reason: string;
}

interface ContactRouteInput {
    contact_id: string;
    company_name: string | null;
    company_website: string | null;
    industry: string | null;
    lead_list_name: string | null;
    enrichment_status: string | null;
    classification: string | null;
    confidence: number | null;
    reasoning: string | null;
    error_message: string | null;
    embedding_candidates?: EmbeddingCandidate[];
}

interface EmbeddingCandidate {
    functional_specialization: string;
    primary_identity: string;
    score: number;
}

// A primary_identity is a Layer-1 high-level business type (Agency,
// Consulting & Advisory, Software & SaaS, …). Layer-2 functional
// specializations are nested under it.
interface PrimaryIdentity {
    name: string;
    description: string;
    identity_type: string;       // operator | service_provider | agency | software_vendor | investor | advisor | staffing | distributor | media | other
    operator_required: boolean;
}

// A functional_specialization is the Layer-2 subtype within an identity.
// This is the "leaf" the LLM matches industry strings to — but the campaign
// bucket actually used downstream is decided by the rollup (combo > spec >
// identity > Generic).
interface DiscoveredBucket {
    functional_specialization: string;
    primary_identity: string;        // foreign key to a PrimaryIdentity.name
    description: string;
    identity_type: string;
    operator_required: boolean;
    priority_rank: number;
    include?: string[];
    exclude?: string[];
    example_strings?: string[];
    strong_identity_signals?: string[];
    weak_sector_signals?: string[];
    disqualifying_signals?: string[];
    estimated_usage_label?: string;
    rough_volume_estimate?: string;
    library_match_id?: string | null;
}

interface DiscoveryOutput {
    observed_patterns: string[];
    sector_focus_vocabulary: string[];
    primary_identities: PrimaryIdentity[];
    buckets: DiscoveredBucket[];   // Layer-2 specializations
}

// Phase 1b returns Layer-1 + Layer-2 + Layer-3 per industry string. Layer-4
// (campaign bucket) is computed by SQL afterwards.
interface MatchChain {
    primary_identity: { name: string; score: number; reason: string };
    functional_specialization: { name: string; score: number; reason: string };
    sector_focus: string;
    identity_type: string;
    generic: boolean;
    disqualified: boolean;
}

interface ContactMapRow {
    bucketing_run_id: string;
    contact_id: string;
    industry_string: string;
    primary_identity: string;
    functional_specialization: string;
    sector_focus: string;
    pre_rollup_bucket_name: string;
    bucket_name: string;
    rollup_level: 'combo' | 'specialization' | 'identity' | 'general';
    source: string;
    confidence: number;
    leaf_score: number;
    ancestor_score: number;
    root_score: number;
    is_generic: boolean;
    is_disqualified: boolean;
    general_reason: string | null;
    reasons: Record<string, any>;
}

// ─── heartbeat wrapper ─────────────────────────────────────────────
// Any operation that can run silent for more than ~30s should be wrapped
// in withHeartbeat — it pings ctx.log every HEARTBEAT_MS so the user sees
// forward motion in the live log stream instead of a frozen UI. Default
// 30s, max 60s gap is the user's stated tolerance.
const HEARTBEAT_MS = 30_000;

async function withHeartbeat<T>(
    label: string,
    op: () => Promise<T>,
    ctx?: BucketingCtx
): Promise<T> {
    const start = Date.now();
    const beat = ctx ? setInterval(() => {
        const elapsed = Math.round((Date.now() - start) / 1000);
        ctx.log(`[Bucketing] ${label} still running, elapsed ${elapsed}s…`);
    }, HEARTBEAT_MS) : null;
    try {
        return await op();
    } finally {
        if (beat) clearInterval(beat);
    }
}

// ─── single-shot vocabulary fetch ──────────────────────────────────
// Pagination over .rpc().range() re-executed the full JOIN+GROUP+aggregate
// on every page (PostgREST function-call semantics), causing multi-minute
// hangs on 100k+ long-tail lists. The v2.6 RPC accepts an optional p_limit
// and runs with statement_timeout=300s, so we fetch everything in ONE call.
//
// Cap at 10k distinct labels — covers any realistic list (the LLM only
// looks at the top ~2500 anyway, and the long tail beyond 10k contributes
// effectively nothing to discovery quality).
const VOCAB_HARD_LIMIT = 10_000;

async function fetchFullVocabulary(
    supabase: SupabaseClient,
    listNames: string[],
    ctx?: BucketingCtx
): Promise<VocabRow[]> {
    return withHeartbeat('vocabulary fetch', async () => {
        // .range() is required to bypass PostgREST's default db-max-rows
        // (1000) on RPC responses — without it the function-level LIMIT
        // doesn't matter, the response gets truncated at 1000 rows.
        const { data, error } = await supabase
            .rpc('get_industry_vocabulary', {
                p_list_names: listNames,
                p_limit: VOCAB_HARD_LIMIT
            })
            .range(0, VOCAB_HARD_LIMIT - 1);
        if (error) throw new Error(`vocabulary fetch failed: ${error.message}`);
        return (data || []) as VocabRow[];
    }, ctx);
}

const CONTACT_PAGE_SIZE = 1000;

async function countSelectedContacts(
    supabase: SupabaseClient,
    listNames: string[]
): Promise<number> {
    const { count, error } = await supabase
        .from('contacts')
        .select('contact_id', { count: 'exact', head: true })
        .in('lead_list_name', listNames);
    if (error) throw new Error(`selected contact count failed: ${error.message}`);
    return count || 0;
}

async function fetchContactsForRouting(
    supabase: SupabaseClient,
    listNames: string[],
    ctx?: BucketingCtx
): Promise<ContactRouteInput[]> {
    return withHeartbeat('contact fetch', async () => {
        const rows: ContactRouteInput[] = [];
        let offset = 0;
        while (true) {
            await ctx?.checkCancel();
            const { data, error } = await supabase
                .from('contacts')
                .select('contact_id,company_name,company_website,industry,lead_list_name,enrichments(status,classification,confidence,reasoning,error_message)')
                .in('lead_list_name', listNames)
                .order('contact_id', { ascending: true })
                .range(offset, offset + CONTACT_PAGE_SIZE - 1);
            if (error) throw new Error(`contact fetch failed: ${error.message}`);
            const page = data || [];
            for (const r of page as any[]) {
                const enr = Array.isArray(r.enrichments) ? r.enrichments[0] : r.enrichments;
                rows.push({
                    contact_id: r.contact_id,
                    company_name: r.company_name || null,
                    company_website: r.company_website || null,
                    industry: r.industry || null,
                    lead_list_name: r.lead_list_name || null,
                    enrichment_status: enr?.status || null,
                    classification: enr?.classification || null,
                    confidence: typeof enr?.confidence === 'number' ? enr.confidence : null,
                    reasoning: enr?.reasoning || null,
                    error_message: enr?.error_message || null
                });
            }
            if (page.length < CONTACT_PAGE_SIZE) break;
            offset += CONTACT_PAGE_SIZE;
            ctx?.progress({
                phase: 'phase1b',
                step: 'load_contacts',
                current: rows.length,
                note: `Loaded ${rows.length.toLocaleString()} contacts…`
            });
        }
        return rows;
    }, ctx);
}

// ─── PROJECT CONTEXT (4-layer model, identity-first) ──────────────
const PROJECT_CONTEXT = `<<<SYSTEM ROLE AND CONTEXT

You are operating inside a revenue-critical B2B growth system.
Your outputs directly influence who is invited to high-volume webinars,
how those webinars are positioned, and ultimately revenue outcomes.

This is NOT an academic taxonomy exercise.
This is NOT a generic classification task.

========================================
THE 4-LAYER CLASSIFICATION MODEL
========================================

Every company is described along three independent axes. The campaign
bucket is decided AFTERWARDS by combining counts across those axes.

Layer 1 — PRIMARY IDENTITY (high-level business type, ~6-12 total)
   What kind of company is this AT ITS CORE?
   Examples: "Agency", "Consulting & Advisory", "Software & SaaS",
   "IT Services", "Financial Services", "Real Estate Operator",
   "Healthcare Operator", "Education Operator", "Staffing & Recruiting",
   "Legal Services", "Accounting & Tax".

Layer 2 — FUNCTIONAL SPECIALIZATION (subtype within identity)
   What kind of {identity} is it?
   Examples (always coupled to a primary identity):
     Agency → "SEO Agency", "Branding Agency", "Performance Marketing
              Agency", "B2B Demand Generation Agency"
     Consulting & Advisory → "IT Consulting", "Management Consulting",
              "Revenue Operations Consulting", "M&A Advisory"
     Financial Services → "Private Equity Firm", "Venture Capital Fund",
              "Growth Equity Firm", "Family Office", "Investment Bank"
     Software & SaaS → "MarTech SaaS", "FinTech SaaS", "PropTech SaaS",
              "Vertical SaaS", "HR SaaS", "Data & Analytics SaaS"
     IT Services → "Managed IT Services", "Cybersecurity Services",
              "Cloud Migration Services"

Layer 3 — SECTOR FOCUS (optional vertical served, ~10-20 total)
   Who do they MAINLY serve, if explicitly stated?
   Examples: "Healthcare", "Real Estate", "Government", "Education",
   "Manufacturing", "Financial Services", "Hospitality", "Energy",
   "Non-profit", "Multi-industry", or "" (none).

Layer 4 — CAMPAIGN BUCKET (decided downstream, NOT by you)
   The actual outreach bucket is computed from the data — not predicted.
   The routing engine combines volume across the three axes:
     - Use "{sector_focus} {specialization}" if that combo has enough
       leads (e.g. "Real Estate SEO Agency").
     - Else fall back to "{specialization}" (e.g. "SEO Agency").
     - Else fall back to "{primary_identity}" (e.g. "Agency").
     - Else "General" (single catch-all bucket — disqualified rows go here too).
   You DO NOT predict campaign buckets. You produce accurate Layer 1-3
   classifications. The system computes Layer 4 from your output + counts.

========================================
UNIVERSAL ROUTING PRINCIPLE
========================================

Classify each company by its CORE BUSINESS IDENTITY first.
Classify SECTOR SERVED second.
Never reverse that order.

- A private equity firm focused on healthcare is a Private Equity Firm
  in Financial Services with sector_focus = Healthcare. NOT a healthcare
  company.
- An IT consultancy serving hospitals is an IT Consulting firm in
  Consulting & Advisory with sector_focus = Healthcare.
- A marketing agency for life sciences is an Agency.
- "Software for schools" is Software & SaaS, not Education.

Distinguish: WHAT the company is vs WHO it serves. Identity decides
primary_identity + functional_specialization. Sector served decides
sector_focus.

========================================
OPERATOR vs ENABLER
========================================

Operators directly operate in a vertical: clinics, hospitals, schools,
universities, banks, city governments, churches, property managers,
manufacturing plants, retailers, restaurants. Their primary_identity
NAMES the vertical (e.g. "Healthcare Operator", "Education Operator").

Enablers serve verticals from outside: agencies, consultants, software
firms, investors, staffing firms, IT providers, advisors. Their
primary_identity is the enabler category (Agency, Consulting & Advisory,
Software & SaaS, …) — the vertical they serve goes in sector_focus.

Operator identities (Healthcare Operator, Education Operator, Government,
Real Estate Operator, Religious Organization) require explicit operator
evidence: "clinic", "hospital", "school district", "university", "city
government", "church", "property management company". Generic mentions
of the sector ("healthcare technology", "marketing for hospitals") are
NOT operator evidence — those companies are enablers.

========================================
BACKGROUND
========================================

We run live B2B webinars at very large scale. Invitations are sent via
Google Calendar to tens or hundreds of thousands of founders. We can't
personalize per individual but we CAN personalize per bucket — each
bucket supports thousands of invitees.

Goal: increase ATTENDANCE RATE by making invitations feel highly relevant
to the recipient's actual business identity. Pressure test: if a recipient
read outreach written for their bucket, would they say "yes, that sounds
like my company" — or "no, that sounds like one of my clients"? If it
sounds like their CLIENTS, the routing is wrong.

========================================
IDEAL CLIENT PROFILE (ICP)
========================================

Strong ICP (always inviteable): agencies, consulting/advisory, professional
services (non-local), B2B/enterprise SaaS, financial services & investment
firms, plausibly scalable high-ticket info products.

Explicit NON-ICP (route to "General", still inviteable): ecommerce/DTC
physical products, local services tied to geography, brick-and-mortar
retail, low-ticket consumer.

Set the disqualified flag (audit only) for clear non-ICP operators with
no plausible upsell path. Disqualified rows still land in the General
bucket — there is no separate Disqualified bucket. Disqualification is
conservative; if ambiguous, prefer the generic flag (also routes to General).

========================================
CRITICAL CONSTRAINTS
========================================

1. Identity > Sector. Bucket the company by what it IS.
2. Operator evidence required for operator identities.
3. Accuracy > Coverage. Do not force-fit.
4. Reusability > Novelty. Identities + specializations must be reusable.
5. Determinism > Creativity. Predictable behavior at scale.`;

// ────────────────────────────────────────────────────────────────────
// PHASE 1A — DISCOVERY
// ────────────────────────────────────────────────────────────────────

export async function runTaxonomyProposal(
    supabase: SupabaseClient,
    runId: string,
    ctx: BucketingCtx
): Promise<void> {
    ctx.log(`[Bucketing ${runId}] Phase 1a: tag-based classification`, 'phase');
    ctx.progress({ phase: 'phase1a', step: 'load_vocabulary', note: 'Loading vocabulary from selected lists…' });

    const { data: run, error: runErr } = await supabase
        .from('bucketing_runs').select('*').eq('id', runId).single();
    if (runErr || !run) throw new Error(`Run not found: ${runErr?.message}`);

    const vocabRows = await fetchFullVocabulary(supabase, run.list_names, ctx);
    const totalContacts = await countSelectedContacts(supabase, run.list_names);
    ctx.log(`[Bucketing ${runId}] vocabulary: ${vocabRows.length} distinct (industry, status) rows over ${totalContacts.toLocaleString()} selected contacts`);

    if (vocabRows.length === 0) {
        await supabase.from('bucketing_runs').update({
            status: 'failed',
            error_message: 'No contacts found for the selected lists.',
            total_contacts: 0
        }).eq('id', runId);
        throw new Error('Empty vocabulary — selected lists have no contacts.');
    }

    // Wipe any prior preview/proposal map rows (idempotent on re-run).
    await supabase.from('bucket_industry_map').delete().eq('bucketing_run_id', runId);

    // ── Partition vocabulary by enrichment status ────────────────────
    // Only `completed` rows go to the LLM. Everything else (scrape_error,
    // unenriched, failed, pending) is a known-bad row and routes straight
    // to the Disqualified bucket without spending a token.
    const completedVocab = vocabRows.filter(r => (r.enrichment_status || 'completed') === 'completed');
    const dqVocab = vocabRows.filter(r => (r.enrichment_status || 'completed') !== 'completed');
    ctx.log(`[Bucketing ${runId}] partition: ${completedVocab.length} taggable, ${dqVocab.length} → Disqualified (failed/missing/scrape_error)`);

    let totalCost = 0;
    const preMapRows: any[] = [];

    // ── Disqualified passthrough ─────────────────────────────────────
    for (const v of dqVocab) {
        const reason = v.enrichment_status || 'unknown';
        preMapRows.push({
            bucketing_run_id: runId,
            industry_string: v.industry,
            raw_industry: v.industry,
            bucket_name: RESERVED_DISQUALIFIED,
            source: 'disqualified_passthrough',
            confidence: 0,
            identity: null,
            characteristic: null,
            sector: null,
            is_new_identity: false,
            is_new_characteristic: false,
            is_new_sector: false,
            is_disqualified: true,
            is_generic: false,
            needs_qa: false,
            llm_reason: `enrichment_status=${reason}`,
            // Mirror trigger fills primary_identity/etc; leave nulls so they
            // survive as nulls in the legacy columns too.
            example_industries: [v.industry]
        });
    }

    // ── Load active taxonomy library (3 SELECTs) ─────────────────────
    const snapshot = await loadTaxonomySnapshot(supabase);
    ctx.log(`[Bucketing ${runId}] taxonomy library: ${snapshot.identities.length} identities, ${snapshot.characteristics.length} characteristics, ${snapshot.sectors.length} sectors`);

    if (completedVocab.length > 0) {
        // ── Tag completed industries via Sonnet (batched) ────────────
        await ctx.checkCancel();
        ctx.progress({
            phase: 'phase1a',
            step: 'sonnet_tagging',
            current: 0,
            total: completedVocab.length,
            note: `Tagging ${completedVocab.length} industries with Sonnet…`
        });
        const tagResult = await withHeartbeat(
            `Sonnet tagging (${completedVocab.length} industries)`,
            () => tagIndustriesWithSonnet(supabase, completedVocab, snapshot, runId, ctx),
            ctx
        );
        totalCost += tagResult.costUsd;
        ctx.log(`[Bucketing ${runId}] Sonnet tagging: ${tagResult.taggings.length} results, $${tagResult.costUsd.toFixed(4)}, model=${tagResult.modelUsed}`);

        // ── Build map rows from taggings ─────────────────────────────
        const identitySet = new Set(snapshot.identities.map(i => i.name));
        const charSet = new Set(snapshot.characteristics.map(c => c.name));
        const sectorSet = new Set(snapshot.sectors.map(s => s.name));
        const dqIdentities = new Set(snapshot.identities.filter(i => i.is_disqualified).map(i => i.name));

        for (const t of tagResult.taggings) {
            const v = completedVocab.find(c => c.industry === t.industry);
            const conf01 = Math.max(0, Math.min(1, (t.confidence || 0) / 10));
            const lowConf = (t.confidence || 0) < PHASE1A_QA_FLOOR;
            const identityIsDq = !!(t.identity && dqIdentities.has(t.identity));
            const isDisqualified = t.is_disqualified || identityIsDq;

            // Pre-rollup name: characteristic preferred → identity → fallback.
            // The actual final bucket_name is rewritten by the volume rollup
            // in Phase 1b. Disqualified is terminal — never rolled up.
            let preBucket: string;
            if (isDisqualified) {
                preBucket = RESERVED_DISQUALIFIED;
            } else if (lowConf) {
                preBucket = RESERVED_GENERAL;
            } else if (t.characteristic) {
                preBucket = t.characteristic;
            } else if (t.identity) {
                preBucket = t.identity;
            } else {
                preBucket = RESERVED_GENERAL;
            }

            preMapRows.push({
                bucketing_run_id: runId,
                industry_string: t.industry,
                raw_industry: t.industry,
                bucket_name: preBucket,
                source: 'llm_phase1a',
                confidence: Number(conf01.toFixed(2)),
                identity: t.identity,
                characteristic: t.characteristic,
                sector: t.sector,
                is_new_identity: t.is_new_identity && !identitySet.has(t.identity || ''),
                is_new_characteristic: t.is_new_characteristic && !charSet.has(t.characteristic || ''),
                is_new_sector: t.is_new_sector && !sectorSet.has(t.sector || ''),
                is_disqualified: isDisqualified,
                is_generic: false,
                needs_qa: lowConf,
                llm_reason: t.reason,
                example_industries: [t.industry]
            });

            // If this is the first time we've seen vocab.n on a tagged row,
            // include n in reasons for downstream debug.
        }
        ctx.progress({
            phase: 'phase1a',
            step: 'sonnet_tagging',
            current: tagResult.taggings.length,
            total: completedVocab.length,
            note: `Tagged ${tagResult.taggings.length} industries`
        });
    }

    // ── Bulk insert all map rows ─────────────────────────────────────
    for (let i = 0; i < preMapRows.length; i += 1000) {
        const chunk = preMapRows.slice(i, i + 1000);
        const { error: upErr } = await supabase.from('bucket_industry_map')
            .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
        if (upErr) throw new Error(`bucket_industry_map insert failed: ${upErr.message}`);
    }
    ctx.log(`[Bucketing ${runId}] wrote ${preMapRows.length} map rows`);

    // ── Build a synthetic taxonomy_proposal for the existing UI ──────
    // The Review screen reads taxonomy_proposal.{primary_identities, buckets}
    // — we synthesize them from the unique tags actually used so the user
    // sees a familiar tree (Identity → Characteristic) backed by the new
    // tag-based classification.
    const usedIdentitySet = new Set<string>();
    const usedCharByKey = new Map<string, { spec: string; identity: string; description: string }>();
    const usedSectors = new Set<string>();
    for (const r of preMapRows) {
        if (r.identity) usedIdentitySet.add(r.identity);
        if (r.characteristic && r.identity) {
            const key = `${r.identity}::${r.characteristic}`;
            if (!usedCharByKey.has(key)) {
                const charDesc = snapshot.characteristics.find(c => c.name === r.characteristic)?.description || '';
                usedCharByKey.set(key, { spec: r.characteristic, identity: r.identity, description: charDesc });
            }
        }
        if (r.sector) usedSectors.add(r.sector);
    }

    const primaryIdentities = Array.from(usedIdentitySet).map(name => {
        const ent = snapshot.identities.find(i => i.name === name);
        return {
            name,
            description: ent?.description || '',
            identity_type: 'other',
            operator_required: false
        };
    });

    const buckets: DiscoveredBucket[] = Array.from(usedCharByKey.values()).map(c => ({
        functional_specialization: c.spec,
        primary_identity: c.identity,
        description: c.description,
        identity_type: 'other',
        operator_required: false,
        priority_rank: 5,
        include: [],
        exclude: [],
        example_strings: [],
        strong_identity_signals: [],
        weak_sector_signals: [],
        disqualifying_signals: []
    }));

    await supabase.from('bucketing_runs').update({
        taxonomy_proposal: {
            observed_patterns: [],
            sector_focus_vocabulary: Array.from(usedSectors),
            primary_identities: primaryIdentities,
            buckets: buckets
        },
        taxonomy_snapshot: {
            identities: snapshot.identities,
            characteristics: snapshot.characteristics,
            sectors: snapshot.sectors
        },
        taxonomy_model: TAXONOMY_MODEL_ANTHROPIC,
        cost_usd: totalCost,
        total_contacts: totalContacts,
        status: 'taxonomy_ready',
        taxonomy_completed_at: new Date().toISOString()
    }).eq('id', runId);

    ctx.log(`[Bucketing ${runId}] Phase 1a done — tagged ${preMapRows.length} industries (${dqVocab.length} DQ, ${completedVocab.length} via LLM), $${totalCost.toFixed(4)}`, 'phase');
    ctx.progress({
        phase: 'phase1a', step: 'done', current: 1, total: 1,
        note: `Tagging complete — ${primaryIdentities.length} identities, ${buckets.length} characteristics, ${usedSectors.size} sectors used`
    });
}

// ────────────────────────────────────────────────────────────────────
// PHASE 1A HELPERS — taxonomy snapshot + Sonnet tagging
// ────────────────────────────────────────────────────────────────────

async function loadTaxonomySnapshot(supabase: SupabaseClient): Promise<TaxonomySnapshot> {
    const [idRes, chRes, secRes] = await Promise.all([
        supabase.from('taxonomy_identities').select('*').eq('archived', false).order('sort_order'),
        supabase.from('taxonomy_characteristics').select('*').eq('archived', false).order('sort_order'),
        supabase.from('taxonomy_sectors').select('*').eq('archived', false).order('sort_order')
    ]);
    if (idRes.error) throw new Error(`taxonomy_identities load failed: ${idRes.error.message}`);
    if (chRes.error) throw new Error(`taxonomy_characteristics load failed: ${chRes.error.message}`);
    if (secRes.error) throw new Error(`taxonomy_sectors load failed: ${secRes.error.message}`);
    return {
        identities: (idRes.data || []) as TaxonomyEntry[],
        characteristics: (chRes.data || []) as TaxonomyEntry[],
        sectors: (secRes.data || []) as TaxonomyEntry[]
    };
}

async function tagIndustriesWithSonnet(
    supabase: SupabaseClient,
    vocab: VocabRow[],
    snapshot: TaxonomySnapshot,
    runId: string,
    ctx: BucketingCtx
): Promise<{ taggings: IndustryTagging[]; costUsd: number; modelUsed: string }> {
    const anthropic = await getAnthropic(supabase);
    if (!anthropic) {
        throw new Error('Anthropic API key not configured. Add it on the Connectors page (saved as ANTHROPIC_API_KEY).');
    }

    const systemPrompt = buildTaggingSystemPrompt(snapshot);
    const limit = pLimit(PHASE1A_CONCURRENCY);
    const batches: VocabRow[][] = [];
    for (let i = 0; i < vocab.length; i += PHASE1A_BATCH_SIZE) {
        batches.push(vocab.slice(i, i + PHASE1A_BATCH_SIZE));
    }

    let totalIn = 0;
    let totalOut = 0;
    let totalCachedIn = 0;
    let done = 0;
    const taggings: IndustryTagging[] = [];

    await Promise.all(batches.map((batch) => limit(async () => {
        await ctx.checkCancel();
        const userPrompt = JSON.stringify({
            industries: batch.map((v, i) => ({
                id: i,
                industry: v.industry,
                sample_companies: v.sample_companies?.slice(0, 2) || []
            }))
        });
        try {
            const resp = await anthropic.messages.create({
                model: TAXONOMY_MODEL_ANTHROPIC,
                max_tokens: 2000,
                system: [{ type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }],
                messages: [{ role: 'user', content: userPrompt }]
            }, { timeout: TAXONOMY_TIMEOUT_MS });

            // Track usage with cache awareness.
            const usage: any = (resp as any).usage || {};
            totalIn += usage.input_tokens || 0;
            totalOut += usage.output_tokens || 0;
            totalCachedIn += usage.cache_read_input_tokens || 0;

            // Find the JSON block in the response.
            const text = (resp.content as any[])
                .filter(b => b.type === 'text').map(b => b.text).join('\n');
            const parsed = parseTaggingJson(text, batch);
            for (const t of parsed) taggings.push(t);
        } catch (err: any) {
            ctx.log(`[Bucketing ${runId}] tagging batch error (${batch.length} industries): ${err.message}`, 'error');
            // Fail-open: emit needs_qa rows so we don't lose contacts.
            for (const v of batch) {
                taggings.push({
                    industry: v.industry,
                    identity: null,
                    is_new_identity: false,
                    characteristic: null,
                    is_new_characteristic: false,
                    sector: null,
                    is_new_sector: false,
                    is_disqualified: false,
                    confidence: 0,
                    reason: `tagging_error: ${err.message?.slice(0, 200)}`
                });
            }
        }
        done += batch.length;
        if (done % (PHASE1A_BATCH_SIZE * 10) === 0 || done === vocab.length) {
            ctx.progress({
                phase: 'phase1a',
                step: 'sonnet_tagging',
                current: done,
                total: vocab.length,
                note: `Tagged ${done}/${vocab.length} industries…`
            });
        }
    })));

    const costUsd = computeAnthropicCost(TAXONOMY_MODEL_ANTHROPIC, totalIn - totalCachedIn, totalOut)
        + (totalCachedIn / 1_000_000) * (ANTHROPIC_PRICING[TAXONOMY_MODEL_ANTHROPIC]?.input || 3) * 0.1; // cached at ~10%
    return { taggings, costUsd, modelUsed: TAXONOMY_MODEL_ANTHROPIC };
}

function buildTaggingSystemPrompt(s: TaxonomySnapshot): string {
    const idLines = s.identities.map(i =>
        `  - ${i.name}${i.is_disqualified ? ' [DQ]' : ''}: ${i.description || ''}`
    ).join('\n');
    const chLines = s.characteristics.map(c =>
        `  - ${c.name} (under ${c.parent_identity}): ${c.description || ''}`
    ).join('\n');
    const secLines = s.sectors.map(sec =>
        `  - ${sec.name}: ${sec.synonyms || sec.description || ''}`
    ).join('\n');

    return `You are tagging B2B contact industries for outreach segmentation.

For each industry text the user provides, return three independent tags:

1. **Identity** (REQUIRED) — what kind of company is it? Pick from the IDENTITY library below. If nothing fits well (you have less than ~6/10 confidence), propose a new identity name and set is_new_identity=true. Identities marked [DQ] mean the company is disqualified for outreach.

2. **Characteristic** (optional) — the more specific subtype within the identity. Pick from the CHARACTERISTICS library below; the parent_identity must match the identity you chose. If nothing fits well, propose a new one and set is_new_characteristic=true. If no characteristic applies, return null.

3. **Sector** (optional) — what *vertical* does the company serve? Pick from the SECTOR library below. Sector is independent of identity (a "Marketing Agency serving Healthcare" has identity=Agency, characteristic=Specialty Marketing Agency, sector=Healthcare / Medical). If the company doesn't have a clear served sector (it's a generic service / cross-vertical), return null. If nothing fits, propose new and set is_new_sector=true.

CLASSIFICATION RULES (critical):
- Classify by core business identity FIRST, sector served SECOND. A PE firm focused on healthcare is identity=Financial Services (PE), NOT Healthcare.
- Operators in a vertical (a hospital, a SaaS company) belong to that identity. Service providers TO that vertical (an agency that markets healthcare, a consultant to insurers) belong to the service identity, with the vertical going into sector.
- If the identity is marked [DQ] in the library OR the company's core business is consumer-facing retail/hospitality/DTC, set is_disqualified=true.
- confidence is 1-10. <6 means you weren't able to find a confident match.

OUTPUT FORMAT — return ONLY valid JSON, no prose:
{
  "results": [
    {
      "id": <integer matching input id>,
      "identity": "<library name or new>" | null,
      "is_new_identity": <bool>,
      "characteristic": "<library name or new>" | null,
      "is_new_characteristic": <bool>,
      "sector": "<library name or new>" | null,
      "is_new_sector": <bool>,
      "is_disqualified": <bool>,
      "confidence": <1-10 integer>,
      "reason": "<brief justification, <= 30 words>"
    }, ...
  ]
}

== IDENTITY LIBRARY ==
${idLines}

== CHARACTERISTICS LIBRARY ==
${chLines}

== SECTOR LIBRARY ==
${secLines}`;
}

function parseTaggingJson(raw: string, batch: VocabRow[]): IndustryTagging[] {
    // Accept the JSON either as the entire response or wrapped in ``` blocks.
    let txt = (raw || '').trim();
    const fence = txt.match(/```(?:json)?\s*([\s\S]*?)```/);
    if (fence) txt = fence[1].trim();
    let parsed: any;
    try { parsed = JSON.parse(txt); }
    catch { throw new Error(`tagging response is not valid JSON: ${txt.slice(0, 200)}`); }

    const out: IndustryTagging[] = [];
    const arr = Array.isArray(parsed) ? parsed : (parsed.results || []);
    for (const item of arr) {
        const idx = typeof item.id === 'number' ? item.id : -1;
        if (idx < 0 || idx >= batch.length) continue;
        const v = batch[idx];
        out.push({
            industry: v.industry,
            identity: nz(item.identity),
            is_new_identity: !!item.is_new_identity,
            characteristic: nz(item.characteristic),
            is_new_characteristic: !!item.is_new_characteristic,
            sector: nz(item.sector),
            is_new_sector: !!item.is_new_sector,
            is_disqualified: !!item.is_disqualified,
            confidence: typeof item.confidence === 'number' ? item.confidence : 0,
            reason: typeof item.reason === 'string' ? item.reason.slice(0, 500) : ''
        });
    }
    return out;
}

function nz(v: any): string | null {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    return s.length > 0 ? s : null;
}

function dedupe(arr: string[] | undefined): string[] {
    return Array.from(new Set((arr || []).map(s => (s || '').trim()).filter(Boolean)));
}

async function callDiscoveryLLM(
    supabase: SupabaseClient,
    vocabRows: VocabRow[],
    preferred: any[]
): Promise<{ discovery: DiscoveryOutput; costUsd: number; modelUsed: string }> {
    // Lighter prompt: smaller head, drop reasoning excerpts (each was ~120
    // chars × N rows = many thousands of tokens that contributed only
    // marginal signal). Sonnet now sees a tight `industry | n | 2 samples`
    // table that's ~3× smaller and resolves in ~30–60s instead of 90–180s.
    const HEAD_LIMIT = 1500;
    const head = vocabRows.slice(0, HEAD_LIMIT);
    const tail = vocabRows.slice(HEAD_LIMIT);
    const tailContacts = tail.reduce((s, r) => s + Number(r.n || 0), 0);
    const headContacts = head.reduce((s, r) => s + Number(r.n || 0), 0);
    const tailExamples = tail.slice(0, 40).map(r => r.industry);

    const vocabularyTable = head.map(r => {
        const samples = (r.sample_companies || []).slice(0, 2).filter(Boolean).join(' | ');
        return `${r.industry} | n=${r.n} | companies=[${samples}]`;
    }).join('\n');

    const tailSection = tail.length > 0
        ? `\n\nTail (${tail.length} more labels covering ${tailContacts} contacts). Examples: ${tailExamples.join(', ')}.`
        : '';

    const preferredSection = preferred.length > 0 ? `

========================================
PREFERRED SPECIALIZATIONS (from library)
========================================

These specializations were defined in prior runs and have proven useful. If
a discovered pattern aligns with one of these at score >= 0.7, REUSE it:
copy functional_specialization, primary_identity, and description VERBATIM
and set library_match_id. Do NOT invent a near-duplicate with different
wording.

${preferred.map(p => `id=${p.id} | spec="${p.bucket_name}" | identity="${p.direct_ancestor || p.root_category}" | desc="${p.description || ''}"`).join('\n')}` : '';

    const userPrompt = `${PROJECT_CONTEXT}

========================================
PHASE 1A — DISCOVER PRIMARY IDENTITIES + FUNCTIONAL SPECIALIZATIONS
========================================

You are NOT predicting campaign buckets. You are producing the catalog
the routing engine uses. Discover:

A) PRIMARY IDENTITIES (Layer 1): 6–12 high-level identities present in the
   vocabulary. Examples: "Agency", "Consulting & Advisory", "Software & SaaS",
   "IT Services", "Financial Services", "Real Estate Operator",
   "Healthcare Operator", "Education Operator", "Staffing & Recruiting",
   "Legal Services", "Accounting & Tax", "Media & Publishing".
   Each identity must be evidenced by multiple companies in the vocabulary.

B) FUNCTIONAL SPECIALIZATIONS (Layer 2): 30–60 specializations across the
   identities. Each specialization belongs to exactly ONE primary_identity.
   Examples (always coupled): Agency → "SEO Agency", "Branding Agency",
   "Performance Marketing Agency", "B2B Demand Generation Agency". Consulting
   & Advisory → "IT Consulting", "Management Consulting", "Revenue Operations
   Consulting", "M&A Advisory". Financial Services → "Private Equity Firm",
   "Venture Capital Fund", "Family Office". Software & SaaS → "MarTech SaaS",
   "FinTech SaaS", "Vertical SaaS", "PropTech SaaS".

C) SECTOR FOCUS VOCABULARY (Layer 3): a controlled list of sector terms
   that appear in the data as "served vertical" signals. These NEVER become
   primary identities or specializations — they are sector_focus values used
   in Phase 1b. Examples: "Healthcare", "Real Estate", "Government",
   "Education", "Manufacturing", "Financial Services", "Hospitality",
   "Energy", "Non-profit", "Legal", "Multi-industry".

NO-SHORTCUTS RULES:
1) Base everything ONLY on patterns evidenced in the vocabulary.
2) Sector words must NOT become a primary_identity unless the vocabulary
   clearly contains OPERATORS in that sector (clinics, schools, city
   governments, etc.) AND that identity carries operator_required=true.
3) No near-duplicates. Specializations differing only by word order or
   synonym must be merged.
4) An identity with no specializations is invalid. Every identity must
   have ≥1 specialization underneath it.
5) Coverage requirement: proposed specializations + identities should
   collectively cover at least 80% of usable contact volume represented
   in the vocabulary. General is for bad/no-data, clear non-ICP, and true
   no-fit cases — do NOT create a niche-only taxonomy that dumps normal
   B2B companies into General.

❌ TOO BROAD as a SPECIALIZATION (must be a primary_identity instead):
"SaaS", "B2B SaaS", "Marketing Agency", "Consulting Firm", "Software".

❌ TOO NARROW (forbidden):
"TikTok ads agency for DTC candle brands", "Family office for German
real estate developers", "RevOps consulting for Series B HR SaaS".

✅ GOLDILOCKS specializations:
"SEO Agency", "Performance Marketing Agency", "B2B Demand Generation Agency",
"IT Consulting", "Revenue Operations Consulting", "M&A Advisory",
"Private Equity Firm", "Venture Capital Fund", "MarTech SaaS",
"PropTech SaaS", "Managed IT Services", "Cybersecurity Services",
"Healthcare Clinic / Hospital" [operator_required=true],
"K-12 School District" [operator_required=true].

PER-SPECIALIZATION ROUTING METADATA — REQUIRED FIELDS:
- identity_type ∈ {operator, service_provider, agency, software_vendor,
  investor, advisor, staffing, distributor, media, other}
- operator_required: true ONLY for specializations whose identity is an
  operator identity (Healthcare Operator → "Medical Clinic / Hospital",
  Education Operator → "K-12 School District").
- priority_rank: 1–10. 1 = strongest identity nouns (PE Firm, Law Firm,
  Marketing Agency, MSP). 10 = weakest (operator specializations that
  lose to enabler signals).
- include / exclude / example_strings: keyword hints that drive Phase 1b
  routing. include = phrases that PROVE this spec. exclude = phrases that
  should route AWAY. example_strings = verbatim industry strings from the
  vocabulary.
${preferredSection}

REQUIRED PROCESS — DO NOT SKIP:
A) List 10–15 high-frequency patterns observed in the vocabulary.
B) Use those patterns to justify the top primary_identities and which
   specializations belong under each.

OUTPUT (strict JSON only, no prose, no markdown fences):

{
  "observed_patterns": [<10–15 strings>],
  "sector_focus_vocabulary": [<sector terms; NEVER appear as identities or specs>],
  "primary_identities": [
    {
      "name": "<identity, Layer 1>",
      "description": "<1 sentence>",
      "identity_type": "<operator|service_provider|agency|software_vendor|investor|advisor|staffing|distributor|media|other>",
      "operator_required": <true|false>
    }
  ],
  "buckets": [
    {
      "functional_specialization": "<spec, Layer 2>",
      "primary_identity": "<MUST exactly match a name in primary_identities above>",
      "description": "<1 sentence — what the company IS>",
      "identity_type": "<...>",
      "operator_required": <true|false>,
      "priority_rank": <1..10>,
      "include": [<keywords>],
      "exclude": [<keywords>],
      "example_strings": [<6–10 verbatim from vocab>],
      "estimated_usage_label": "<dominant|very_common|common|moderate|niche_but_meaningful|rare>",
      "rough_volume_estimate": "<e.g. ~8–12% of rows>",
      "library_match_id": "<id from PREFERRED, or empty string>"
    }
  ]
}

Rules:
- Specializations ordered MOST common → LEAST common.
- example_strings MUST be verbatim from the vocabulary.
- 6–12 primary_identities, 30–60 specializations total.
- Every spec's primary_identity field exactly matches a name in the
  primary_identities array.

========================================
VOCABULARY
========================================

Head covers ${headContacts} contacts across ${head.length} distinct labels.
Format: industry | n=count | companies=[2 samples] | reasoning="…"

${vocabularyTable}${tailSection}

Total contacts across all labels: ${headContacts + tailContacts}.

Now produce the JSON.`;

    // Try Anthropic Sonnet first if a key is configured.
    const anthropic = await getAnthropic(supabase);
    if (anthropic) {
        try {
            const t0 = Date.now();
            const resp = await anthropic.messages.create({
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
        }
    }

    // OpenAI fallback
    const schema = buildDiscoverySchema();
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), TAXONOMY_TIMEOUT_MS);
    try {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: { Authorization: `Bearer ${getOpenAIKey()}`, 'Content-Type': 'application/json' },
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
    if (!Array.isArray(parsed.primary_identities)) parsed.primary_identities = [];
    if (!Array.isArray(parsed.sector_focus_vocabulary)) parsed.sector_focus_vocabulary = [];
    if (!Array.isArray(parsed.observed_patterns)) parsed.observed_patterns = [];
    return parsed;
}

function buildDiscoverySchema() {
    return {
        type: 'object',
        additionalProperties: false,
        required: ['observed_patterns', 'sector_focus_vocabulary', 'primary_identities', 'buckets'],
        properties: {
            observed_patterns: { type: 'array', items: { type: 'string' } },
            sector_focus_vocabulary: { type: 'array', items: { type: 'string' } },
            primary_identities: {
                type: 'array',
                items: {
                    type: 'object',
                    additionalProperties: false,
                    required: ['name', 'description', 'identity_type', 'operator_required'],
                    properties: {
                        name: { type: 'string' },
                        description: { type: 'string' },
                        identity_type: { type: 'string' },
                        operator_required: { type: 'boolean' }
                    }
                }
            },
            buckets: {
                type: 'array',
                items: {
                    type: 'object',
                    additionalProperties: false,
                    // Trimmed required list. The three signals arrays
                    // (strong_identity / weak_sector / disqualifying) used
                    // to be required and forced ~5x the output tokens per
                    // spec; we now derive equivalent hints from include /
                    // exclude / example_strings instead.
                    required: ['functional_specialization', 'primary_identity', 'description',
                               'identity_type', 'operator_required', 'priority_rank',
                               'include', 'exclude', 'example_strings',
                               'estimated_usage_label', 'rough_volume_estimate', 'library_match_id'],
                    properties: {
                        functional_specialization: { type: 'string' },
                        primary_identity: { type: 'string' },
                        description: { type: 'string' },
                        identity_type: { type: 'string' },
                        operator_required: { type: 'boolean' },
                        priority_rank: { type: 'integer' },
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
    keep?: string[];                              // functional_specialization names
    rename?: Record<string, string>;              // {old spec name: new spec name}
    add?: {
        functional_specialization: string;
        primary_identity: string;
        description: string;
        identity_type?: string;
        operator_required?: boolean;
    }[];
    min_volume?: number;
    bucket_budget?: number;
}

export async function applyTaxonomyEdits(
    supabase: SupabaseClient,
    runId: string,
    edits: TaxonomyEdits,
    ctx: BucketingCtx
): Promise<void> {
    const { data: run, error } = await supabase.from('bucketing_runs').select('*').eq('id', runId).single();
    if (error || !run) throw new Error(`Run not found: ${error?.message}`);
    const sourceTaxonomy = (run.taxonomy_final || run.taxonomy_proposal) as DiscoveryOutput | null;
    if (!sourceTaxonomy?.buckets) throw new Error('Taxonomy proposal missing — run Phase 1a first');

    let leaves = sourceTaxonomy.buckets.map(b => ({ ...b }));

    if (edits.rename) {
        for (const [oldName, newName] of Object.entries(edits.rename)) {
            const target = newName.trim();
            if (!target || RESERVED.has(target.toLowerCase())) continue;
            leaves = leaves.map(b => b.functional_specialization === oldName
                ? { ...b, functional_specialization: target }
                : b);
        }
    }

    if (edits.keep) {
        const keepSet = new Set(edits.keep.map(s => s.trim()));
        leaves = leaves.filter(b => keepSet.has(b.functional_specialization));
    }

    if (edits.add) {
        for (const a of edits.add) {
            const spec = (a.functional_specialization || '').trim();
            const ident = (a.primary_identity || '').trim();
            if (!spec || RESERVED.has(spec.toLowerCase()) || !ident) continue;
            if (leaves.some(l => l.functional_specialization === spec)) continue;
            leaves.push({
                functional_specialization: spec,
                primary_identity: ident,
                description: (a.description || '').trim(),
                identity_type: a.identity_type || 'other',
                operator_required: !!a.operator_required,
                priority_rank: 5,
                include: [], exclude: [], example_strings: [],
                strong_identity_signals: [], weak_sector_signals: [], disqualifying_signals: []
            });
        }
    }

    const update: any = {
        taxonomy_final: {
            observed_patterns: sourceTaxonomy.observed_patterns || [],
            sector_focus_vocabulary: sourceTaxonomy.sector_focus_vocabulary || [],
            primary_identities: (sourceTaxonomy as any).primary_identities || [],
            buckets: leaves
        }
    };
    if (typeof edits.min_volume === 'number' && edits.min_volume >= 0) {
        update.min_volume = edits.min_volume;
    }
    if (typeof edits.bucket_budget === 'number' && edits.bucket_budget > 0) {
        update.bucket_budget = Math.floor(edits.bucket_budget);
    }
    await supabase.from('bucketing_runs').update(update).eq('id', runId);
    ctx.log(`[Bucketing ${runId}] taxonomy edits applied: ${leaves.length} specializations`);

    // Rebuild the preview map so Review counts reflect the edited taxonomy
    // (renames / drops / new specs all change which industries map where).
    // Cheap: one batched embedding call against the run's vocab.
    try {
        await rebuildPreviewMap(supabase, runId, leaves, sourceTaxonomy.sector_focus_vocabulary || [], run.list_names || [], ctx);
    } catch (e: any) {
        ctx.log(`[Bucketing ${runId}] preview rebuild failed (non-fatal): ${e.message}`, 'warn');
    }
}

async function rebuildPreviewMap(
    supabase: SupabaseClient,
    runId: string,
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    listNames: string[],
    ctx: BucketingCtx
): Promise<void> {
    if (!Array.isArray(listNames) || listNames.length === 0) return;
    const vocab = await fetchFullVocabulary(supabase, listNames, ctx);
    if (vocab.length === 0) return;

    // Drop only the preview-source rows; keep any library_match rows the
    // user might want to retain even after editing other specs.
    await supabase.from('bucket_industry_map')
        .delete()
        .eq('bucketing_run_id', runId)
        .in('source', ['preview_embedding', 'llm_phase1', 'embedding']);

    // Industries still claimed by surviving non-preview rows (e.g. library
    // matches) shouldn't be re-assigned by the preview pass.
    const { data: keptRows } = await supabase
        .from('bucket_industry_map')
        .select('industry_string')
        .eq('bucketing_run_id', runId);
    const keptSet = new Set((keptRows || []).map((r: any) => r.industry_string));
    const previewVocab = vocab.filter(v => !keptSet.has(v.industry));

    const { rows, costUsd } = await runPreviewEmbedding(previewVocab, leaves, sectorVocab, runId);
    if (rows.length > 0) {
        for (let i = 0; i < rows.length; i += 1000) {
            const chunk = rows.slice(i, i + 1000);
            const { error } = await supabase.from('bucket_industry_map')
                .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
            if (error) throw new Error(error.message);
        }
    }
    ctx.log(`[Bucketing ${runId}] preview rebuilt: ${rows.length} rows, $${costUsd.toFixed(4)}`);
}

// ────────────────────────────────────────────────────────────────────
// PHASE 1B — IDENTITY-FIRST MATCHING
// ────────────────────────────────────────────────────────────────────

export async function runAssignment(
    supabase: SupabaseClient,
    runId: string,
    ctx: BucketingCtx
): Promise<void> {
    const { data: run, error } = await supabase.from('bucketing_runs').select('*').eq('id', runId).single();
    if (error || !run) throw new Error(`Run not found: ${error?.message}`);

    const final = (run.taxonomy_final || run.taxonomy_proposal) as DiscoveryOutput | null;
    if (!final?.buckets || final.buckets.length === 0) {
        throw new Error('No buckets defined for assignment');
    }
    const leaves = final.buckets;
    const sectorVocab: string[] = (final as any).sector_focus_vocabulary || [];
    const finalSpecNames = new Set(leaves.map(l => l.functional_specialization));

    // Library buckets the user opted into for this run. Only keep library
    // specs that survived Review; a dropped library spec must not reappear
    // during assignment.
    const preferredIds: string[] = Array.isArray(run.preferred_library_ids) ? run.preferred_library_ids : [];
    let libraryBuckets: any[] = [];
    if (preferredIds.length > 0) {
        const { data } = await supabase
            .from('bucket_library').select('*').in('id', preferredIds);
        libraryBuckets = (data || []).filter((b: any) =>
            (b.functional_specialization || b.bucket_name) && (b.primary_identity || b.direct_ancestor)
            && finalSpecNames.has(b.functional_specialization || b.bucket_name)
        );
    }

    await supabase.from('bucketing_runs').update({ status: 'assigning' }).eq('id', runId);

    let totalCost = Number(run.cost_usd || 0);

    await ctx.checkCancel();
    ctx.log(`[Bucketing ${runId}] step 1/5: load contacts`, 'phase');
    ctx.progress({ phase: 'phase1b', step: 'load_contacts', note: 'Loading selected contacts…' });
    const contacts = await fetchContactsForRouting(supabase, run.list_names, ctx);
    ctx.log(`[Bucketing ${runId}] ${contacts.length} contacts to route`);
    ctx.progress({ phase: 'phase1b', step: 'contacts_loaded', current: contacts.length, total: contacts.length, note: `${contacts.length.toLocaleString()} contacts loaded` });

    const { error: clearContactMapError } = await supabase
        .from('bucket_contact_map')
        .delete()
        .eq('bucketing_run_id', runId);
    if (clearContactMapError) throw new Error(`contact map cleanup failed: ${clearContactMapError.message}`);
    const { error: clearAssignmentsError } = await supabase
        .from('bucket_assignments')
        .delete()
        .eq('bucketing_run_id', runId);
    if (clearAssignmentsError) throw new Error(`assignment cleanup failed: ${clearAssignmentsError.message}`);

    const assignedRows: ContactMapRow[] = [];
    const usableContacts: ContactRouteInput[] = [];
    for (const contact of contacts) {
        const reason = getUnclassifiableReason(contact);
        if (reason) assignedRows.push(makeGeneralContactRow(runId, contact, reason));
        else usableContacts.push(contact);
    }
    let pendingContacts: ContactRouteInput[] = usableContacts;
    ctx.log(`[Bucketing ${runId}] usable contacts: ${usableContacts.length}; General before routing: ${assignedRows.length}`);

    // Step 2: selected-library high-confidence match, per contact.
    if (libraryBuckets.length > 0 && pendingContacts.length > 0) {
        await ctx.checkCancel();
        ctx.log(`[Bucketing ${runId}] step 2/5: library-first match against ${libraryBuckets.length} preferred buckets`, 'phase');
        ctx.progress({ phase: 'phase1b', step: 'library_match', note: `Matching against ${libraryBuckets.length} library buckets…` });
        const libRes = await withHeartbeat('library-first match (Phase 1b)',
            () => runContactLibraryFirstMatch(pendingContacts, libraryBuckets, sectorVocab, runId), ctx);
        totalCost += libRes.costUsd;
        assignedRows.push(...libRes.autoAssigned);
        pendingContacts = libRes.pending;
        ctx.log(`[Bucketing ${runId}] library matched ${libRes.autoAssigned.length}/${usableContacts.length}, ${pendingContacts.length} pending`);
        ctx.progress({ phase: 'phase1b', step: 'library_match_done', current: assignedRows.length, total: contacts.length, note: `${libRes.autoAssigned.length} matched via library, ${pendingContacts.length} pending` });
    }

    // Step 3: strict contact-level embedding auto-match. This is intentionally
    // stricter than the review preview threshold; otherwise the LLM handles it.
    if (EMBED_PREFILTER_ENABLED && pendingContacts.length > 0) {
        await ctx.checkCancel();
        ctx.log(`[Bucketing ${runId}] step 3/5: strict embedding pre-filter`, 'phase');
        ctx.progress({ phase: 'phase1b', step: 'embedding_prefilter', note: `Embedding pre-filter on ${pendingContacts.length.toLocaleString()} contacts…` });
        const embedRes = await withHeartbeat(
            `contact embedding pre-filter (${pendingContacts.length} contacts)`,
            () => runContactEmbeddingPrefilter(pendingContacts, leaves, sectorVocab, runId),
            ctx
        );
        totalCost += embedRes.costUsd;
        assignedRows.push(...embedRes.autoAssigned);
        pendingContacts = embedRes.pending;
        ctx.log(`[Bucketing ${runId}] embedding auto-assigned ${embedRes.autoAssigned.length}/${usableContacts.length}, ${pendingContacts.length} still pending`);
        ctx.progress({ phase: 'phase1b', step: 'embedding_prefilter_done', current: contacts.length - pendingContacts.length, total: contacts.length, note: `${embedRes.autoAssigned.length} matched via strict embedding, ${pendingContacts.length} still pending LLM` });
    }

    await ctx.checkCancel();
    ctx.log(`[Bucketing ${runId}] step 4/5: routing LLM matching`, 'phase');
    ctx.progress({ phase: 'phase1b', step: 'llm_routing', current: 0, total: pendingContacts.length, note: `LLM routing on ${pendingContacts.length.toLocaleString()} contacts…` });
    const llmRes = await runContactMatchingLLM(pendingContacts, leaves, sectorVocab, runId, ctx);
    totalCost += llmRes.costUsd;
    assignedRows.push(...llmRes.rows);
    ctx.log(`[Bucketing ${runId}] LLM routed ${llmRes.rows.length}, total contact rows ${assignedRows.length}, cost so far $${totalCost.toFixed(4)}`);
    ctx.progress({ phase: 'phase1b', step: 'llm_routing_done', current: contacts.length, total: contacts.length, note: `LLM routed ${llmRes.rows.length.toLocaleString()} contacts, $${totalCost.toFixed(4)} spent` });

    await ctx.checkCancel();
    ctx.log(`[Bucketing ${runId}] step 5/5: contact-level volume rollup + write`, 'phase');
    ctx.progress({ phase: 'phase1b', step: 'rollup_write', note: 'Computing contact-level campaign buckets…' });
    const rolledRows = computeContactRollup(assignedRows, Number(run.min_volume || 0), Number(run.bucket_budget || 30));
    await writeContactMapAndAssignments(supabase, runId, rolledRows);

    const assignedCount = rolledRows.length;
    const coverageSummary = buildCoverageSummary(contacts, rolledRows);
    const qualityWarnings = buildQualityWarnings(coverageSummary, rolledRows);

    await supabase.from('bucketing_runs').update({
        status: 'completed',
        assigned_contacts: assignedCount,
        cost_usd: totalCost,
        coverage_summary: coverageSummary,
        quality_warnings: qualityWarnings,
        assignment_completed_at: new Date().toISOString()
    }).eq('id', runId);

    for (const warning of qualityWarnings) ctx.log(`[Bucketing ${runId}] warning: ${warning}`, 'warn');
    ctx.log(`[Bucketing ${runId}] DONE — ${assignedCount.toLocaleString()} contacts assigned, total cost $${totalCost.toFixed(4)}`, 'phase');
    ctx.progress({ phase: 'phase1b', step: 'done', current: assignedCount, total: contacts.length, note: `Assigned ${assignedCount.toLocaleString()} contacts — total cost $${totalCost.toFixed(4)}` });
}

function getUnclassifiableReason(contact: ContactRouteInput): string | null {
    const label = (contact.classification || contact.industry || '').trim().toLowerCase();
    if (contact.enrichment_status !== 'completed') return 'failed_enrichment';
    if (!label) return 'missing_industry';
    if (['site error', 'scrape error', 'unknown', 'error', 'n/a', 'na', 'none'].includes(label)) {
        return 'scrape_site_unknown';
    }
    return null;
}

function contactRoutingText(contact: ContactRouteInput): string {
    return [
        contact.company_name ? `Company: ${contact.company_name}` : '',
        contact.company_website ? `Website: ${contact.company_website}` : '',
        contact.industry ? `Industry: ${contact.industry}` : '',
        contact.classification && contact.classification !== contact.industry ? `Classification: ${contact.classification}` : '',
        typeof contact.confidence === 'number' ? `Confidence: ${contact.confidence}` : '',
        contact.reasoning ? `Reasoning: ${contact.reasoning}` : ''
    ].filter(Boolean).join('\n');
}

function cleanScore(n: number | undefined | null): number {
    if (typeof n !== 'number' || !Number.isFinite(n)) return 0;
    return Number(Math.max(0, Math.min(1, n)).toFixed(2));
}

function preRollupName(row: Pick<ContactMapRow, 'sector_focus' | 'functional_specialization' | 'primary_identity' | 'is_generic' | 'is_disqualified'>): string {
    if (row.is_disqualified) return RESERVED_DISQUALIFIED;
    if (row.is_generic) return RESERVED_GENERAL;
    if (row.functional_specialization && row.sector_focus && row.sector_focus !== 'Multi-industry') {
        return `${row.sector_focus} ${row.functional_specialization}`;
    }
    if (row.functional_specialization) return row.functional_specialization;
    if (row.primary_identity) return row.primary_identity;
    return RESERVED_GENERAL;
}

function makeGeneralContactRow(
    runId: string,
    contact: ContactRouteInput,
    reason: string
): ContactMapRow {
    // Unclassifiable rows (failed enrichment, missing industry, scrape errors)
    // route to the dedicated Disqualified bucket — separate from General so
    // outreach exports can exclude them by default while still being able to
    // see why each contact landed there.
    return {
        bucketing_run_id: runId,
        contact_id: contact.contact_id,
        industry_string: (contact.classification || contact.industry || '').trim(),
        primary_identity: '',
        functional_specialization: '',
        sector_focus: '',
        pre_rollup_bucket_name: RESERVED_DISQUALIFIED,
        bucket_name: RESERVED_DISQUALIFIED,
        rollup_level: 'general',
        source: 'unclassifiable',
        confidence: 0,
        leaf_score: 0,
        ancestor_score: 0,
        root_score: 0,
        is_generic: false,
        is_disqualified: true,
        general_reason: reason,
        reasons: {
            general_reason: reason,
            enrichment_status: contact.enrichment_status,
            error_message: contact.error_message
        }
    };
}

function makeMatchedContactRow(
    runId: string,
    contact: ContactRouteInput,
    params: {
        primary_identity: string;
        functional_specialization: string;
        sector_focus: string;
        source: string;
        confidence: number;
        leaf_score: number;
        ancestor_score: number;
        root_score: number;
        is_generic?: boolean;
        is_disqualified?: boolean;
        general_reason?: string | null;
        reasons?: Record<string, any>;
    }
): ContactMapRow {
    const row: ContactMapRow = {
        bucketing_run_id: runId,
        contact_id: contact.contact_id,
        industry_string: (contact.classification || contact.industry || '').trim(),
        primary_identity: params.primary_identity,
        functional_specialization: params.functional_specialization,
        sector_focus: params.sector_focus,
        pre_rollup_bucket_name: RESERVED_GENERAL,
        bucket_name: RESERVED_GENERAL,
        rollup_level: 'general',
        source: params.source,
        confidence: cleanScore(params.confidence),
        leaf_score: cleanScore(params.leaf_score),
        ancestor_score: cleanScore(params.ancestor_score),
        root_score: cleanScore(params.root_score),
        is_generic: !!params.is_generic,
        is_disqualified: !!params.is_disqualified,
        general_reason: params.general_reason || null,
        reasons: params.reasons || {}
    };
    row.pre_rollup_bucket_name = preRollupName(row);
    row.bucket_name = row.pre_rollup_bucket_name;
    row.rollup_level = row.bucket_name === RESERVED_GENERAL
        ? 'general'
        : row.functional_specialization && row.sector_focus && row.sector_focus !== 'Multi-industry'
            ? 'combo'
            : row.functional_specialization
                ? 'specialization'
                : 'identity';
    return row;
}

function computeContactRollup(rows: ContactMapRow[], minVolume: number, bucketBudget: number): ContactMapRow[] {
    const comboCounts = new Map<string, number>();
    const specCounts = new Map<string, number>();
    const identityCounts = new Map<string, number>();

    for (const row of rows) {
        if (row.is_generic || row.is_disqualified) continue;
        if (row.functional_specialization) {
            specCounts.set(row.functional_specialization, (specCounts.get(row.functional_specialization) || 0) + 1);
            if (row.sector_focus && row.sector_focus !== 'Multi-industry') {
                const combo = `${row.sector_focus} ${row.functional_specialization}`;
                comboCounts.set(combo, (comboCounts.get(combo) || 0) + 1);
            }
        }
        if (row.primary_identity) {
            identityCounts.set(row.primary_identity, (identityCounts.get(row.primary_identity) || 0) + 1);
        }
    }

    const rolled = rows.map(row => {
        const next = { ...row };
        next.pre_rollup_bucket_name = preRollupName(next);
        if (next.is_disqualified) {
            next.bucket_name = RESERVED_DISQUALIFIED;
            next.rollup_level = 'general';
            next.general_reason = next.general_reason || 'disqualified';
        } else if (next.is_generic && !next.primary_identity) {
            next.bucket_name = RESERVED_GENERAL;
            next.rollup_level = 'general';
            next.general_reason = next.general_reason || 'generic_low_confidence';
        } else if (next.functional_specialization && next.sector_focus && next.sector_focus !== 'Multi-industry'
            && (comboCounts.get(`${next.sector_focus} ${next.functional_specialization}`) || 0) >= minVolume) {
            next.bucket_name = `${next.sector_focus} ${next.functional_specialization}`;
            next.rollup_level = 'combo';
            next.general_reason = null;
        } else if (next.functional_specialization && (specCounts.get(next.functional_specialization) || 0) >= minVolume) {
            next.bucket_name = next.functional_specialization;
            next.rollup_level = 'specialization';
            next.general_reason = null;
        } else if (next.primary_identity && (identityCounts.get(next.primary_identity) || 0) >= minVolume) {
            next.bucket_name = next.primary_identity;
            next.rollup_level = 'identity';
            next.general_reason = null;
        } else {
            next.bucket_name = RESERVED_GENERAL;
            next.rollup_level = 'general';
            next.general_reason = next.general_reason || 'rolled_up_to_general';
        }
        return next;
    });

    const budget = Math.max(1, Math.floor(bucketBudget || 30));
    let safety = 0;
    while (safety++ < 300) {
        const counts = new Map<string, number>();
        for (const row of rolled) {
            if (row.bucket_name === RESERVED_GENERAL) continue;
            if (row.bucket_name === RESERVED_DISQUALIFIED) continue;
            counts.set(row.bucket_name, (counts.get(row.bucket_name) || 0) + 1);
        }
        if (counts.size <= budget) break;
        const smallest = Array.from(counts.entries()).sort((a, b) => a[1] - b[1])[0]?.[0];
        if (!smallest) break;
        for (const row of rolled) {
            if (row.bucket_name !== smallest) continue;
            if (row.rollup_level === 'combo' && row.functional_specialization) {
                row.bucket_name = row.functional_specialization;
                row.rollup_level = 'specialization';
            } else if (row.rollup_level === 'specialization' && row.primary_identity) {
                row.bucket_name = row.primary_identity;
                row.rollup_level = 'identity';
            } else {
                row.bucket_name = RESERVED_GENERAL;
                row.rollup_level = 'general';
                row.general_reason = row.general_reason || 'bucket_budget_rollup';
            }
        }
    }
    return rolled;
}

async function writeContactMapAndAssignments(
    supabase: SupabaseClient,
    runId: string,
    rows: ContactMapRow[]
): Promise<void> {
    const mapRows = rows.map(row => ({
        bucketing_run_id: row.bucketing_run_id,
        contact_id: row.contact_id,
        industry_string: row.industry_string,
        primary_identity: row.primary_identity || null,
        functional_specialization: row.functional_specialization || null,
        sector_focus: row.sector_focus || null,
        pre_rollup_bucket_name: row.pre_rollup_bucket_name,
        bucket_name: row.bucket_name,
        rollup_level: row.rollup_level,
        source: row.source,
        confidence: row.confidence,
        leaf_score: row.leaf_score,
        ancestor_score: row.ancestor_score,
        root_score: row.root_score,
        is_generic: row.is_generic,
        is_disqualified: row.is_disqualified,
        general_reason: row.general_reason,
        reasons: row.reasons
    }));
    for (let i = 0; i < mapRows.length; i += 1000) {
        const chunk = mapRows.slice(i, i + 1000);
        const { error } = await supabase.from('bucket_contact_map')
            .upsert(chunk, { onConflict: 'bucketing_run_id,contact_id' });
        if (error) throw new Error(`contact map insert failed: ${error.message}`);
    }

    const assignmentRows = rows.map(row => ({
        bucketing_run_id: runId,
        contact_id: row.contact_id,
        bucket_name: row.bucket_name,
        source: row.source,
        confidence: row.confidence,
        bucket_leaf: row.functional_specialization || null,
        bucket_ancestor: row.primary_identity || null,
        bucket_root: row.primary_identity || null,
        primary_identity: row.primary_identity || null,
        functional_specialization: row.functional_specialization || null,
        sector_focus: row.sector_focus || null,
        pre_rollup_bucket_name: row.pre_rollup_bucket_name,
        rollup_level: row.rollup_level,
        general_reason: row.general_reason,
        reasons: row.reasons,
        is_generic: row.is_generic,
        is_disqualified: row.is_disqualified
    }));
    for (let i = 0; i < assignmentRows.length; i += 1000) {
        const chunk = assignmentRows.slice(i, i + 1000);
        const { error } = await supabase.from('bucket_assignments')
            .upsert(chunk, { onConflict: 'bucketing_run_id,contact_id' });
        if (error) throw new Error(`assignment insert failed: ${error.message}`);
    }
}

function buildCoverageSummary(contacts: ContactRouteInput[], rows: ContactMapRow[]) {
    const generalRows = rows.filter(r => r.bucket_name === RESERVED_GENERAL);
    const sourceCounts: Record<string, number> = {};
    const generalReasons: Record<string, number> = {};
    for (const row of rows) sourceCounts[row.source] = (sourceCounts[row.source] || 0) + 1;
    for (const row of generalRows) {
        const reason = row.general_reason || 'unspecified';
        generalReasons[reason] = (generalReasons[reason] || 0) + 1;
    }
    return {
        selected_contacts: contacts.length,
        assigned_contacts: rows.length,
        usable_contacts: contacts.filter(c => !getUnclassifiableReason(c)).length,
        unclassifiable_contacts: contacts.filter(c => !!getUnclassifiableReason(c)).length,
        general_contacts: generalRows.length,
        general_pct: rows.length > 0 ? Number(((generalRows.length / rows.length) * 100).toFixed(2)) : 0,
        source_counts: sourceCounts,
        general_reasons: generalReasons,
        unexpected_unassigned_contacts: Math.max(0, contacts.length - rows.length)
    };
}

function buildQualityWarnings(summary: any, rows: ContactMapRow[]): string[] {
    const warnings: string[] = [];
    if (summary.assigned_contacts !== summary.selected_contacts) {
        warnings.push(`Assigned ${summary.assigned_contacts} of ${summary.selected_contacts} selected contacts.`);
    }
    if (summary.general_pct > 50) {
        warnings.push(`General is ${summary.general_pct}% of assigned contacts; review General breakdown before using this run.`);
    }
    const catchall = rows.filter(r => r.source === 'catchall').length;
    const unprocessed = Math.max(0, summary.selected_contacts - summary.assigned_contacts);
    const denominator = Math.max(1, summary.selected_contacts);
    if ((catchall + unprocessed) / denominator > 0.05) {
        warnings.push(`Catchall/unprocessed contacts exceed 5% (${catchall + unprocessed} contacts).`);
    }
    return warnings;
}

// ─── sector_focus extraction (deterministic) ───────────────────────
//
// Used by the embedding prefilter and library-first match to populate
// sector_focus from the raw industry string when no LLM has run on it.
// Without this, embedding-matched rows would all have sector_focus=''
// and combo buckets ("Real Estate SEO Agency") could never form.
//
// Strategy: case-insensitive substring match against sector_focus_vocabulary,
// preferring longer matches (so "Real Estate" wins over "Real").
function extractSectorFocus(industryString: string, sectorVocab: string[]): string {
    if (!industryString || sectorVocab.length === 0) return '';
    const lower = industryString.toLowerCase();
    const hits: { sector: string; len: number }[] = [];
    for (const sec of sectorVocab) {
        const s = (sec || '').trim();
        if (!s) continue;
        // word-ish match — sector term must appear bordered by start/end or non-letter.
        const re = new RegExp(`(^|[^a-z0-9])${escapeRegExp(s.toLowerCase())}([^a-z0-9]|$)`);
        if (re.test(lower)) hits.push({ sector: s, len: s.length });
    }
    if (hits.length === 0) return '';
    // multiple hits → "Multi-industry" (cold-email copy can't pin one sector)
    if (hits.length > 1) {
        // unless all hits are the same after normalization (synonym safety)
        const set = new Set(hits.map(h => h.sector.toLowerCase()));
        if (set.size > 1) return 'Multi-industry';
    }
    // pick the longest match
    return hits.sort((a, b) => b.len - a.len)[0].sector;
}

function escapeRegExp(s: string): string {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// ─── library-first deterministic match ─────────────────────────────
//
// Embeds the user's selected library buckets and matches each vocabulary
// industry against them. High-confidence hits short-circuit the LLM and
// produce a map row using the library's primary_identity +
// functional_specialization, with sector_focus extracted deterministically.
// ─── preview embedding pass (post-discovery, pre-review) ───────────
//
// Replaces the old "seed from example_strings" behavior. After Phase 1a
// proposes a taxonomy, we embed every proposed spec and every unmatched
// industry, then cosine-match. High-confidence pairs become preview map
// rows so the Review screen shows real per-spec counts before assignment.
//
// Returns rows for the caller to upsert + the OpenAI embedding cost.
// Phase 1b clears and rewrites these rows with the full LLM chain.
async function runPreviewEmbedding(
    vocab: VocabRow[],
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    runId: string
): Promise<{ rows: any[]; costUsd: number }> {
    if (vocab.length === 0 || leaves.length === 0) return { rows: [], costUsd: 0 };

    const leafTexts = leaves.map(l => {
        const sig = (l.strong_identity_signals || []).slice(0, 6).join(', ');
        const inc = (l.include || []).slice(0, 6).join(', ');
        const examples = (l.example_strings || []).slice(0, 3).join(' | ');
        return `${l.functional_specialization} (under ${l.primary_identity}): ${l.description || ''}. Identity: ${l.identity_type}. Strong signals: ${sig}. Include: ${inc}. Examples: ${examples}.`;
    });
    const vocabTexts = vocab.map(v => v.industry);

    const { embeddings, costUsd } = await embedBatch([...leafTexts, ...vocabTexts]);
    const leafVecs = embeddings.slice(0, leaves.length);
    const vocabVecs = embeddings.slice(leaves.length);

    const rows: any[] = [];
    for (let i = 0; i < vocab.length; i++) {
        const sims = leafVecs.map(lv => cosine(vocabVecs[i], lv));
        const sorted = sims.map((s, j) => ({ s, j })).sort((a, b) => b.s - a.s);
        const top = sorted[0];
        const second = sorted[1] || { s: 0 };
        if (top.s >= EMBED_AUTO_THRESHOLD && (top.s - second.s) >= EMBED_MARGIN) {
            const leaf = leaves[top.j];
            // Skip operator_required specs — operator evidence is an LLM
            // call, not a similarity check.
            if (leaf.operator_required) continue;
            const sector = extractSectorFocus(vocab[i].industry, sectorVocab);
            rows.push({
                bucketing_run_id: runId,
                industry_string: vocab[i].industry,
                bucket_name: leaf.functional_specialization,
                source: 'preview_embedding',
                confidence: Number(top.s.toFixed(2)),
                bucket_leaf: leaf.functional_specialization,
                bucket_ancestor: leaf.primary_identity,
                bucket_root: leaf.primary_identity,
                primary_identity: leaf.primary_identity,
                functional_specialization: leaf.functional_specialization,
                sector_focus: sector,
                leaf_score: Number(top.s.toFixed(2)),
                ancestor_score: Number(top.s.toFixed(2)),
                root_score: Number(top.s.toFixed(2)),
                is_generic: false,
                is_disqualified: false
            });
        }
    }
    return { rows, costUsd };
}

async function runLibraryFirstMatch(
    vocab: VocabRow[],
    libraryBuckets: any[],
    sectorVocab: string[],
    runId: string
): Promise<{ autoAssigned: any[]; pending: VocabRow[]; costUsd: number }> {
    if (vocab.length === 0 || libraryBuckets.length === 0) {
        return { autoAssigned: [], pending: vocab, costUsd: 0 };
    }

    // Library bucket vector representation. Identity signals + include terms
    // dominate so cosine matches business identity, not sector.
    const libTexts = libraryBuckets.map(b => {
        const spec = b.functional_specialization || b.bucket_name || '';
        const ident = b.primary_identity || b.direct_ancestor || '';
        const inc = (b.include_terms || []).slice(0, 8).join(', ');
        const examples = (b.example_strings || []).slice(0, 4).join(' | ');
        return `${spec} (under ${ident}): ${b.description || ''}. Includes: ${inc}. Examples: ${examples}.`;
    });
    const vocabTexts = vocab.map(v => v.industry);
    const { embeddings, costUsd } = await embedBatch([...libTexts, ...vocabTexts]);
    const libVecs = embeddings.slice(0, libraryBuckets.length);
    const vocabVecs = embeddings.slice(libraryBuckets.length);

    const autoAssigned: any[] = [];
    const pending: VocabRow[] = [];

    for (let i = 0; i < vocab.length; i++) {
        const sims = libVecs.map(lv => cosine(vocabVecs[i], lv));
        const sorted = sims.map((s, j) => ({ s, j })).sort((a, b) => b.s - a.s);
        const top = sorted[0];
        const second = sorted[1] || { s: 0 };
        if (top.s >= EMBED_AUTO_THRESHOLD && (top.s - second.s) >= EMBED_MARGIN) {
            const lib = libraryBuckets[top.j];
            const spec = lib.functional_specialization || lib.bucket_name || '';
            const ident = lib.primary_identity || lib.direct_ancestor || '';
            const sector = extractSectorFocus(vocab[i].industry, sectorVocab);
            autoAssigned.push({
                bucketing_run_id: runId,
                industry_string: vocab[i].industry,
                bucket_name: spec, // pre-rollup placeholder
                source: 'library_match',
                confidence: Number(top.s.toFixed(2)),
                bucket_leaf: spec,
                bucket_ancestor: ident,
                bucket_root: ident,
                primary_identity: ident,
                functional_specialization: spec,
                sector_focus: sector,
                leaf_score: Number(top.s.toFixed(2)),
                ancestor_score: Number(top.s.toFixed(2)),
                root_score: Number(top.s.toFixed(2)),
                is_generic: false,
                is_disqualified: false,
                reasons: { auto: 'library_match', library_bucket_id: lib.id, cosine: top.s }
            });
        } else {
            pending.push(vocab[i]);
        }
    }
    return { autoAssigned, pending, costUsd };
}

// ─── embedding pre-filter (sector-aware) ───────────────────────────
async function runEmbeddingPrefilter(
    vocab: VocabRow[],
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    runId: string
): Promise<{ autoAssigned: any[]; pending: VocabRow[]; costUsd: number }> {
    if (vocab.length === 0 || leaves.length === 0) return { autoAssigned: [], pending: vocab, costUsd: 0 };

    // Embedding text leans on identity signals so cosine matches the
    // specialization, not the vertical the company serves.
    const leafTexts = leaves.map(l => {
        const sig = (l.strong_identity_signals || []).slice(0, 6).join(', ');
        const inc = (l.include || []).slice(0, 6).join(', ');
        return `${l.functional_specialization} (under ${l.primary_identity}): ${l.description || ''}. Identity: ${l.identity_type}. Strong signals: ${sig}. Include: ${inc}.`;
    });
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
            // Don't auto-assign to operator_required specs via embedding —
            // operator evidence has to be an explicit LLM check.
            if (leaf.operator_required) {
                pending.push(vocab[i]);
                continue;
            }
            // Extract sector deterministically from the industry string so
            // combo campaign buckets ("Real Estate SEO Agency") still form
            // on embedding-matched rows.
            const sector = extractSectorFocus(vocab[i].industry, sectorVocab);
            autoAssigned.push({
                bucketing_run_id: runId,
                industry_string: vocab[i].industry,
                bucket_name: leaf.functional_specialization, // pre-rollup placeholder
                source: 'embedding',
                confidence: Number(top.s.toFixed(2)),
                bucket_leaf: leaf.functional_specialization,
                bucket_ancestor: leaf.primary_identity,
                bucket_root: leaf.primary_identity,
                primary_identity: leaf.primary_identity,
                functional_specialization: leaf.functional_specialization,
                sector_focus: sector,
                leaf_score: Number(top.s.toFixed(2)),
                ancestor_score: Number(top.s.toFixed(2)),
                root_score: Number(top.s.toFixed(2)),
                is_generic: false,
                is_disqualified: false,
                reasons: { auto: 'embedding pre-filter', cosine: top.s, identity_type: leaf.identity_type }
            });
        } else {
            pending.push(vocab[i]);
        }
    }
    return { autoAssigned, pending, costUsd };
}

async function runContactLibraryFirstMatch(
    contacts: ContactRouteInput[],
    libraryBuckets: any[],
    sectorVocab: string[],
    runId: string
): Promise<{ autoAssigned: ContactMapRow[]; pending: ContactRouteInput[]; costUsd: number }> {
    if (contacts.length === 0 || libraryBuckets.length === 0) {
        return { autoAssigned: [], pending: contacts, costUsd: 0 };
    }
    const libTexts = libraryBuckets.map(b => {
        const spec = b.functional_specialization || b.bucket_name || '';
        const ident = b.primary_identity || b.direct_ancestor || '';
        const inc = (b.include_terms || []).slice(0, 8).join(', ');
        const examples = (b.example_strings || []).slice(0, 4).join(' | ');
        return `${spec} (under ${ident}): ${b.description || ''}. Includes: ${inc}. Examples: ${examples}.`;
    });
    const contactTexts = contacts.map(contactRoutingText);
    const { embeddings, costUsd } = await embedBatch([...libTexts, ...contactTexts]);
    const libVecs = embeddings.slice(0, libraryBuckets.length);
    const contactVecs = embeddings.slice(libraryBuckets.length);

    const autoAssigned: ContactMapRow[] = [];
    const pending: ContactRouteInput[] = [];
    for (let i = 0; i < contacts.length; i++) {
        const sims = libVecs.map(lv => cosine(contactVecs[i], lv));
        const sorted = sims.map((s, j) => ({ s, j })).sort((a, b) => b.s - a.s);
        const top = sorted[0];
        const second = sorted[1] || { s: 0 };
        if (top.s >= CONTACT_EMBED_AUTO_THRESHOLD && (top.s - second.s) >= CONTACT_EMBED_MARGIN) {
            const lib = libraryBuckets[top.j];
            const spec = lib.functional_specialization || lib.bucket_name || '';
            const ident = lib.primary_identity || lib.direct_ancestor || '';
            autoAssigned.push(makeMatchedContactRow(runId, contacts[i], {
                primary_identity: ident,
                functional_specialization: spec,
                sector_focus: extractSectorFocus(contactRoutingText(contacts[i]), sectorVocab),
                source: 'library_match',
                confidence: top.s,
                leaf_score: top.s,
                ancestor_score: top.s,
                root_score: top.s,
                reasons: { auto: 'library_match', library_bucket_id: lib.id, cosine: top.s }
            }));
        } else {
            pending.push(contacts[i]);
        }
    }
    return { autoAssigned, pending, costUsd };
}

async function runContactEmbeddingPrefilter(
    contacts: ContactRouteInput[],
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    runId: string
): Promise<{ autoAssigned: ContactMapRow[]; pending: ContactRouteInput[]; costUsd: number }> {
    if (contacts.length === 0 || leaves.length === 0) {
        return { autoAssigned: [], pending: contacts, costUsd: 0 };
    }
    const leafTexts = leaves.map(l => {
        const sig = (l.strong_identity_signals || []).slice(0, 6).join(', ');
        const inc = (l.include || []).slice(0, 6).join(', ');
        const examples = (l.example_strings || []).slice(0, 4).join(' | ');
        return `${l.functional_specialization} (under ${l.primary_identity}): ${l.description || ''}. Identity: ${l.identity_type}. Strong signals: ${sig}. Include: ${inc}. Examples: ${examples}.`;
    });
    const contactTexts = contacts.map(contactRoutingText);
    const { embeddings, costUsd } = await embedBatch([...leafTexts, ...contactTexts]);
    const leafVecs = embeddings.slice(0, leaves.length);
    const contactVecs = embeddings.slice(leaves.length);

    const autoAssigned: ContactMapRow[] = [];
    const pending: ContactRouteInput[] = [];
    for (let i = 0; i < contacts.length; i++) {
        const sims = leafVecs.map(lv => cosine(contactVecs[i], lv));
        const sorted = sims.map((s, j) => ({ s, j })).sort((a, b) => b.s - a.s);
        const top = sorted[0];
        const second = sorted[1] || { s: 0 };
        const leaf = leaves[top.j];
        if (!leaf.operator_required && top.s >= CONTACT_EMBED_AUTO_THRESHOLD && (top.s - second.s) >= CONTACT_EMBED_MARGIN) {
            autoAssigned.push(makeMatchedContactRow(runId, contacts[i], {
                primary_identity: leaf.primary_identity,
                functional_specialization: leaf.functional_specialization,
                sector_focus: extractSectorFocus(contactRoutingText(contacts[i]), sectorVocab),
                source: 'embedding_high_confidence',
                confidence: top.s,
                leaf_score: top.s,
                ancestor_score: top.s,
                root_score: top.s,
                reasons: { auto: 'contact embedding pre-filter', cosine: top.s, identity_type: leaf.identity_type }
            }));
        } else {
            const embedding_candidates = sorted.slice(0, 8).map(({ s, j }) => ({
                functional_specialization: leaves[j].functional_specialization,
                primary_identity: leaves[j].primary_identity,
                score: Number(s.toFixed(3))
            }));
            pending.push({ ...contacts[i], embedding_candidates });
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

// ─── routing LLM (Phase 1b) ────────────────────────────────────────
async function runContactMatchingLLM(
    pending: ContactRouteInput[],
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    runId: string,
    ctx?: BucketingCtx
): Promise<{ rows: ContactMapRow[]; costUsd: number }> {
    if (pending.length === 0) return { rows: [], costUsd: 0 };

    const validSpecNames = new Set(leaves.map(l => l.functional_specialization));
    const validIdentityNames = new Set(leaves.map(l => l.primary_identity));
    const identityBySpec = new Map(leaves.map(l => [l.functional_specialization, l.primary_identity]));
    const refByIdentity: Record<string, any[]> = {};
    for (const l of leaves) {
        const ident = l.primary_identity;
        if (!refByIdentity[ident]) refByIdentity[ident] = [];
        refByIdentity[ident].push({
            functional_specialization: l.functional_specialization,
            description: l.description,
            identity_type: l.identity_type,
            operator_required: l.operator_required,
            priority_rank: l.priority_rank,
            include: (l.include || []).slice(0, 8),
            exclude: (l.exclude || []).slice(0, 8),
            example_strings: (l.example_strings || []).slice(0, 8)
        });
    }
    const bucketReferenceJson = JSON.stringify(refByIdentity);
    const limit = pLimit(MATCH_CONCURRENCY);
    const batches: ContactRouteInput[][] = [];
    for (let i = 0; i < pending.length; i += MATCH_BATCH_SIZE) batches.push(pending.slice(i, i + MATCH_BATCH_SIZE));

    let totalCost = 0;
    let completedBatches = 0;
    const rows: ContactMapRow[] = [];
    const totalBatches = batches.length;

    await Promise.all(batches.map(batch => limit(async () => {
        if (ctx) { try { await ctx.checkCancel(); } catch { return; } }
        const { results, costUsd } = await classifyContactBatch(
            batch, bucketReferenceJson, sectorVocab, validSpecNames, validIdentityNames
        );
        totalCost += costUsd;
        completedBatches++;
        if (ctx) {
            const completedItems = Math.min(pending.length, completedBatches * MATCH_BATCH_SIZE);
            ctx.progress({
                phase: 'phase1b',
                step: 'llm_routing',
                current: completedItems,
                total: pending.length,
                note: `LLM batch ${completedBatches}/${totalBatches} done (${completedItems.toLocaleString()}/${pending.length.toLocaleString()} contacts)`
            });
        }
        for (let i = 0; i < batch.length; i++) {
            const contact = batch[i];
            const r = results[i] || makeFallbackChain();
            const specOk = !!r.functional_specialization.name
                && validSpecNames.has(r.functional_specialization.name)
                && r.functional_specialization.score >= 0.40;
            const specName = specOk ? r.functional_specialization.name : '';
            const identityCandidate = specOk
                ? (identityBySpec.get(specName) || r.primary_identity.name)
                : r.primary_identity.name;
            const identityOk = !!identityCandidate
                && validIdentityNames.has(identityCandidate)
                && r.primary_identity.score >= 0.40;
            const identName = identityOk ? identityCandidate : '';
            const isDisqualified = !!r.disqualified;
            const isGeneric = isDisqualified ? false : (!specName && !identName);
            rows.push(makeMatchedContactRow(runId, contact, {
                primary_identity: isDisqualified ? '' : identName,
                functional_specialization: isDisqualified ? '' : specName,
                sector_focus: isDisqualified ? '' : (r.sector_focus || '').trim(),
                source: 'llm_phase1b',
                confidence: specName ? r.functional_specialization.score : r.primary_identity.score,
                leaf_score: r.functional_specialization.score,
                ancestor_score: r.primary_identity.score,
                root_score: r.primary_identity.score,
                is_generic: isGeneric,
                is_disqualified: isDisqualified,
                general_reason: isDisqualified ? 'disqualified' : (isGeneric ? 'generic_low_confidence' : null),
                reasons: {
                    spec: r.functional_specialization.reason,
                    identity: r.primary_identity.reason,
                    identity_type: r.identity_type,
                    model_generic: r.generic
                }
            }));
        }
    })));

    return { rows, costUsd: totalCost };
}

async function classifyContactBatch(
    batch: ContactRouteInput[],
    bucketReferenceJson: string,
    sectorVocab: string[],
    validSpecNames: Set<string>,
    validIdentityNames: Set<string>
): Promise<{ results: MatchChain[]; costUsd: number }> {
    const systemPrompt = `${PROJECT_CONTEXT}

========================================
PHASE 1B — ROUTE EACH CONTACT TO IDENTITY + SPECIALIZATION + SECTOR
========================================

You classify individual company contacts, not abstract industry labels.
Use the company name, website, enriched classification, model confidence,
and reasoning. Do not invent facts beyond those fields.

Return three separate fields:
- primary_identity: Layer 1, must be one of BUCKET_REFERENCE keys
- functional_specialization: Layer 2, must be listed under that identity
- sector_focus: Layer 3, must be from SECTOR_VOCABULARY, "Multi-industry", or ""

General is a last resort. If a primary_identity is a reasonable fit, return it
even when no specialization is precise. Only leave both names blank when the
business is truly unclear, bad data, clear non-ICP, or confidence is below 0.40.

Each contact may include embedding_candidate_buckets. Treat these as a shortlist,
not a decision. Prefer them when the text supports them, but choose another
reference bucket if the contact evidence is clearly better.

Rules:
- Strong business-model nouns beat sector nouns.
- "Serving X" does not mean "is X"; X belongs in sector_focus.
- Operator specializations require explicit operator evidence.
- Disqualify only clear ecommerce/DTC physical, local geo-tied services,
  brick-and-mortar retail, or low-ticket consumer.
- Scores are alignment scores, not probabilities. Use >=0.40 for a reasonable
  identity/specialization fit, >=0.70 for strong fit.
- Reasons: max 18 words each and cite the contact fields.

Return strict JSON only.`;

    const contactPayload = batch.map(c => ({
        company_name: c.company_name || '',
        website: c.company_website || '',
        industry: c.industry || '',
        enriched_classification: c.classification || '',
        enrichment_confidence: c.confidence,
        enrichment_reasoning: c.reasoning || '',
        embedding_candidate_buckets: c.embedding_candidates || []
    }));

    const userPrompt = `BUCKET_REFERENCE (grouped by primary_identity):
${bucketReferenceJson}

SECTOR_VOCABULARY: ${JSON.stringify(sectorVocab)}

CONTACTS_TO_CLASSIFY (same order as output):
${JSON.stringify(contactPayload)}

Return JSON: { "assignments": [<one object per contact in the same order>] }
Each assignment object:
{
  "primary_identity": {"name": "<identity key from BUCKET_REFERENCE or empty>", "score": 0.00, "reason": ""},
  "functional_specialization": {"name": "<spec under that identity, or empty>", "score": 0.00, "reason": ""},
  "sector_focus": "<from SECTOR_VOCABULARY, 'Multi-industry', or ''>",
  "identity_type": "<operator|service_provider|agency|software_vendor|investor|advisor|staffing|distributor|media|other>",
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
                    required: ['primary_identity', 'functional_specialization',
                               'sector_focus', 'identity_type', 'generic', 'disqualified'],
                    properties: {
                        primary_identity: chainItemSchema(),
                        functional_specialization: chainItemSchema(),
                        sector_focus: { type: 'string' },
                        identity_type: { type: 'string' },
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
                        json_schema: { name: 'phase1b_contact_match', strict: true, schema }
                    },
                    temperature: 0.1
                }),
                signal: controller.signal
            });
            if (!res.ok) {
                const body = await res.text().catch(() => '');
                throw new Error(`contact match ${res.status}: ${body.slice(0, 300)}`);
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
    try { result = await callOnce(); }
    catch { result = await callOnce(); }

    let { assignments, costUsd } = result;
    const drift = assignments.some(a =>
        (a.functional_specialization.name && !validSpecNames.has(a.functional_specialization.name)) ||
        (a.primary_identity.name && !validIdentityNames.has(a.primary_identity.name))
    );
    if (drift) {
        try {
            const retried = await callOnce();
            assignments = retried.assignments;
            costUsd += retried.costUsd;
        } catch { /* keep original */ }
    }

    const padded: MatchChain[] = [];
    for (let i = 0; i < batch.length; i++) padded.push(assignments[i] || makeFallbackChain());
    return { results: padded, costUsd };
}

async function runMatchingLLM(
    pending: VocabRow[],
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    runId: string,
    ctx?: BucketingCtx
): Promise<{ rows: any[]; costUsd: number }> {
    if (pending.length === 0) return { rows: [], costUsd: 0 };

    // Valid Layer-1 + Layer-2 names (the LLM may only return these).
    const validSpecNames = new Set(leaves.map(l => l.functional_specialization));
    const validIdentityNames = new Set(leaves.map(l => l.primary_identity));
    const identityBySpec = new Map(leaves.map(l => [l.functional_specialization, l.primary_identity]));

    // Bucket reference is the cacheable prompt prefix. Group by primary_identity
    // so the model sees the hierarchy clearly.
    const refByIdentity: Record<string, any[]> = {};
    for (const l of leaves) {
        const ident = l.primary_identity;
        if (!refByIdentity[ident]) refByIdentity[ident] = [];
        refByIdentity[ident].push({
            functional_specialization: l.functional_specialization,
            description: l.description,
            identity_type: l.identity_type,
            operator_required: l.operator_required,
            priority_rank: l.priority_rank,
            strong_identity_signals: (l.strong_identity_signals || []).slice(0, 8),
            weak_sector_signals: (l.weak_sector_signals || []).slice(0, 6),
            disqualifying_signals: (l.disqualifying_signals || []).slice(0, 6),
            include: (l.include || []).slice(0, 6),
            exclude: (l.exclude || []).slice(0, 6),
            example_strings: (l.example_strings || []).slice(0, 6)
        });
    }
    const bucketReferenceJson = JSON.stringify(refByIdentity);

    const limit = pLimit(MATCH_CONCURRENCY);
    const batches: VocabRow[][] = [];
    for (let i = 0; i < pending.length; i += MATCH_BATCH_SIZE) {
        batches.push(pending.slice(i, i + MATCH_BATCH_SIZE));
    }

    let totalCost = 0;
    let completedBatches = 0;
    const rows: any[] = [];
    const totalBatches = batches.length;

    await Promise.all(batches.map(batch => limit(async () => {
        // Skip remaining queued batches if a cancel is requested. We don't
        // throw mid-batch — that races with already-in-flight HTTP calls —
        // but we DO bail out of starting any more, so cancel takes effect
        // within ~1 batch (5–10s) instead of waiting for the whole loop.
        if (ctx) { try { await ctx.checkCancel(); } catch (e) { return; } }
        const { results, costUsd } = await classifyBatch(
            batch, bucketReferenceJson, sectorVocab, validSpecNames, validIdentityNames
        );
        totalCost += costUsd;
        completedBatches++;
        if (ctx) {
            const completedItems = Math.min(pending.length, completedBatches * MATCH_BATCH_SIZE);
            ctx.progress({
                phase: 'phase1b',
                step: 'llm_routing',
                current: completedItems,
                total: pending.length,
                note: `LLM batch ${completedBatches}/${totalBatches} done (${completedItems}/${pending.length} industries)`
            });
        }
        for (let i = 0; i < batch.length; i++) {
            const ind = batch[i].industry;
            const r = results[i] || makeFallbackChain();
            const specOk = r.functional_specialization.name
                && validSpecNames.has(r.functional_specialization.name)
                && r.functional_specialization.score >= 0.55;
            const specName = specOk ? r.functional_specialization.name : '';
            // Trust the spec→identity mapping from Phase 1a, not whatever the
            // LLM returned for primary_identity (drift guard).
            const identName = specOk
                ? (identityBySpec.get(specName) || r.primary_identity.name)
                : (r.primary_identity.name && validIdentityNames.has(r.primary_identity.name)
                    ? r.primary_identity.name
                    : '');
            rows.push({
                bucketing_run_id: runId,
                industry_string: ind,
                // Pre-rollup placeholder. The SQL rollup will overwrite this
                // with the campaign bucket (combo / spec / identity / Generic).
                bucket_name: specName || RESERVED_GENERAL,
                source: 'llm_phase1b',
                confidence: Number((r.functional_specialization.score || 0).toFixed(2)),
                bucket_leaf: specName,
                bucket_ancestor: identName,
                bucket_root: identName,
                primary_identity: identName,
                functional_specialization: specName,
                sector_focus: (r.sector_focus || '').trim(),
                leaf_score: r.functional_specialization.score,
                ancestor_score: r.primary_identity.score,
                root_score: r.primary_identity.score,
                is_generic: !!r.generic && !specOk,
                is_disqualified: !!r.disqualified,
                reasons: {
                    spec: r.functional_specialization.reason,
                    identity: r.primary_identity.reason,
                    identity_type: r.identity_type
                }
            });
        }
    })));

    return { rows, costUsd: totalCost };
}

function makeFallbackChain(): MatchChain {
    return {
        primary_identity: { name: '', score: 0, reason: 'fallback' },
        functional_specialization: { name: '', score: 0, reason: 'fallback' },
        sector_focus: '',
        identity_type: 'other',
        generic: true,
        disqualified: false
    };
}

async function classifyBatch(
    batch: VocabRow[],
    bucketReferenceJson: string,
    sectorVocab: string[],
    validSpecNames: Set<string>,
    validIdentityNames: Set<string>
): Promise<{ results: MatchChain[]; costUsd: number }> {
    const systemPrompt = `${PROJECT_CONTEXT}

========================================
PHASE 1B — ROUTE EACH COMPANY TO IDENTITY + SPECIALIZATION + SECTOR
========================================

You produce three separate classifications per company:
  - primary_identity      (Layer 1, MUST be one of the identity keys in BUCKET_REFERENCE)
  - functional_specialization (Layer 2, MUST be one of the specialization
                          names listed UNDER that identity in BUCKET_REFERENCE)
  - sector_focus          (Layer 3, optional — from SECTOR_VOCABULARY only)

You DO NOT produce a campaign bucket. The system computes that from these
three values + counts.

DECISION SEQUENCE (apply in this order):

1) Determine PRIMARY IDENTITY (the company's core business model).
   Investor? Software vendor? Agency? Consulting firm? Staffing firm?
   MSP? Operator (clinic, school, government entity)?

2) Inside that identity, pick the FUNCTIONAL SPECIALIZATION that best fits.
   Examples: under "Agency" → "SEO Agency"; under "Financial Services" →
   "Private Equity Firm"; under "Consulting & Advisory" → "IT Consulting".

3) Determine SECTOR FOCUS — the vertical the company SERVES if explicitly
   stated. If multiple, use "Multi-industry". If unspecified, "".

4) If neither identity nor specialization fits at >= 0.55 confidence,
   set generic = true and leave the name fields empty. Never use a sector
   word as a shortcut bucket.

UNIVERSAL ROUTING RULES:

Rule 1 — Strong business-model nouns BEAT sector nouns.
  ("private equity", "venture capital", "law firm", "accounting",
   "staffing", "agency", "consulting", "managed services", "software
   platform", "brokerage") OUTRANK ("healthcare", "education",
   "government", "legal", "financial", "real estate", "manufacturing")
  unless the text explicitly says the company OPERATES in that sector.

Rule 2 — "Serving X" does NOT mean "is X".
  • software for schools ≠ school
  • recruiting for hospitals ≠ healthcare provider
  • private equity focused on HVAC ≠ HVAC company
  • marketing for law firms ≠ law firm
  • IT services for government agencies ≠ government entity

Rule 3 — Operator specializations (operator_required=true) require
  EXPLICIT operator evidence: "clinic", "hospital", "school district",
  "university", "city government", "church", "property management
  company", "factory". Generic sector mentions are NOT evidence.

Rule 4 — When BOTH identity and sector appear, fill BOTH fields.
  Example: "Healthcare private equity firm" →
    primary_identity = "Financial Services",
    functional_specialization = "Private Equity Firm",
    sector_focus = "Healthcare".

EXPLICIT EXAMPLES — CORRECT:
  • "Healthcare private equity investment firm" →
      primary_identity = Financial Services
      functional_specialization = Private Equity Firm
      sector_focus = Healthcare
  • "Government IT consulting firm" →
      primary_identity = Consulting & Advisory  (or IT Services depending
        on which identity contains "IT Consulting" in BUCKET_REFERENCE)
      functional_specialization = IT Consulting
      sector_focus = Government
  • "Marketing agency for dental practices" →
      primary_identity = Agency
      functional_specialization = Performance Marketing Agency  (or
        Branding Agency, etc — pick the closest spec from BUCKET_REFERENCE)
      sector_focus = Healthcare
  • "Real estate software for hospitals" →
      primary_identity = Software & SaaS
      functional_specialization = PropTech SaaS  (or Vertical SaaS)
      sector_focus = Real Estate     ← yes, real estate is the OWN model
  • "Medical clinic" →
      primary_identity = Healthcare Operator
      functional_specialization = Medical Clinic / Hospital
      sector_focus = ""

EXPLICIT EXAMPLES — INCORRECT:
  • "Healthcare private equity investment firm" → primary_identity = Healthcare ❌
  • "Government IT consulting firm" → primary_identity = Government ❌
  • "Marketing agency for dental practices" → primary_identity = Healthcare ❌
  • "Real estate software for hospitals" → primary_identity = Healthcare ❌
  • "Software for schools" → primary_identity = Education ❌

PRESSURE TEST: "If outreach were written for this primary_identity +
specialization, would the recipient say 'yes that's me' or 'no that's
my client'?" If 'my client' → routing is wrong.

OUTPUT CONSTRAINTS:
- primary_identity.name MUST be one of the identity keys in BUCKET_REFERENCE,
  or "" if generic / disqualified.
- functional_specialization.name MUST be a spec listed under that exact
  identity, or "" if generic / disqualified.
- functional_specialization.score must be <= primary_identity.score.
- sector_focus MUST be from SECTOR_VOCABULARY, "Multi-industry", or "".
  NEVER put an identity noun in sector_focus.
- identity_type ∈ operator | service_provider | agency | software_vendor |
  investor | advisor | staffing | distributor | media | other.
- Disqualify ONLY clear ecommerce/DTC physical, local geo-tied services,
  brick-and-mortar retail, low-ticket consumer.
- Reasons: max 18 words each, must cite a phrase from the classification.

Return strict JSON, no prose, no markdown fences.`;

    const userPrompt = `BUCKET_REFERENCE (grouped by primary_identity):
${bucketReferenceJson}

SECTOR_VOCABULARY: ${JSON.stringify(sectorVocab)}

COMPANIES_TO_CLASSIFY (in order):
${JSON.stringify(batch.map(b => b.industry))}

Return JSON: { "assignments": [<one object per company in the same order>] }
Each assignment object:
{
  "primary_identity": {"name": "<identity key from BUCKET_REFERENCE or empty>", "score": 0.00, "reason": ""},
  "functional_specialization": {"name": "<spec under that identity, or empty>", "score": 0.00, "reason": ""},
  "sector_focus": "<from SECTOR_VOCABULARY, 'Multi-industry', or ''>",
  "identity_type": "<operator|service_provider|agency|software_vendor|investor|advisor|staffing|distributor|media|other>",
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
                    required: ['primary_identity', 'functional_specialization',
                               'sector_focus', 'identity_type', 'generic', 'disqualified'],
                    properties: {
                        primary_identity: chainItemSchema(),
                        functional_specialization: chainItemSchema(),
                        sector_focus: { type: 'string' },
                        identity_type: { type: 'string' },
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
    try { result = await callOnce(); }
    catch { result = await callOnce(); }

    let { assignments, costUsd } = result;
    const drift = assignments.some(a =>
        (a.functional_specialization.name && !validSpecNames.has(a.functional_specialization.name)) ||
        (a.primary_identity.name && !validIdentityNames.has(a.primary_identity.name))
    );
    if (drift) {
        try {
            const retried = await callOnce();
            assignments = retried.assignments;
            costUsd += retried.costUsd;
        } catch { /* keep original */ }
    }

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
