/**
 * Bucketing Service v2.1 — identity-first taxonomy with sector-focus metadata.
 *
 * Universal routing rule (the foundation of everything below):
 *   Classify by core business identity FIRST. Sector served SECOND. Never
 *   reverse the order. A private equity firm focused on healthcare is a PE
 *   firm — not a healthcare operator. Only operators in a sector belong in
 *   that sector's bucket.
 *
 * Phase 1a (TAGGING, batched): the user-selected tagger (gpt-4.1-mini default,
 * Claude Haiku 4.5 optional) reads each distinct enrichments.classification
 * string and assigns identity / characteristic / sector + per-tag confidence.
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
 * Volume rollup: sector+characteristic → characteristic → identity → General.
 * Disqualified / invalid rows land in General with audit reasons.
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import Anthropic from '@anthropic-ai/sdk';
import { getSetting } from './appSettings';
import { checkBucketingSchema } from './bucketingSchemaCheck';

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

// Canonical reason codes for routing decisions. Persist these strings on
// bucket_assignments / bucket_contact_map.general_reason so a campaign
// operator can filter "show me everyone who landed in General because the
// volume rollup couldn't find a layer over the threshold" without
// regex-matching free text. The bucket_reason column still gets a
// human-readable sentence; general_reason gets the code.
export const REASON = {
    // Routed into a real campaign bucket — three rollup levels (v5+):
    COMBO_THRESHOLD_MET: 'combo_threshold_met',
    SPECIALIZATION_THRESHOLD_MET: 'characteristic_threshold_met',
    IDENTITY_THRESHOLD_MET: 'identity_threshold_met',
    // Generic Audit moved row out of General into a live bucket.
    GENERIC_AUDIT_RECLAIMED: 'generic_audit_reclaimed',
    // Routed to General:
    LOW_VOLUME: 'no_layer_cleared_threshold',
    BUDGET_ROLLUP: 'bucket_budget_rollup',
    LOW_IDENTITY_CONFIDENCE: 'low_identity_confidence',
    LOW_OVERALL_CONFIDENCE: 'low_overall_confidence',
    NO_BUCKETS_DEFINED: 'no_buckets_defined',
    UNCLASSIFIABLE_INVITEABLE: 'unclassifiable_inviteable',
    // Routed to Disqualified:
    DISQUALIFIED_BY_LLM: 'disqualified_by_llm',
    DISQUALIFIED_BY_IDENTITY_CASCADE: 'disqualified_by_identity_cascade',
    FAILED_ENRICHMENT: 'failed_enrichment',
    MISSING_INDUSTRY: 'missing_industry',
    SCRAPE_SITE_UNKNOWN: 'scrape_site_unknown',
} as const;
export type ReasonCode = typeof REASON[keyof typeof REASON];

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

// Confidence floor — taggers return 1-10, anything below this routes the
// industry to General with needs_qa=true so the user can review post-run.
const PHASE1A_QA_FLOOR = 6;
// Disqualification requires HIGH identity confidence (>= 7/10). Soft DQ
// rule: when in doubt, route to General — never auto-DQ on uncertain
// identity. A "Consumer & Retail" tag at confidence 5 is too risky to
// silently exclude from outreach.
const PHASE1A_DQ_FLOOR = 7;
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
    sample_companies: string[] | null;
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
    // Per-tag 1-10 scores. Drives smart fallbacks in Phase 1b: a row
    // with identity=8, characteristic=4 should route at the identity
    // level rather than tumble all the way to General. confidence is
    // the legacy single score (= min of the three) kept for existing
    // QA panel + reporting code.
    identity_confidence: number;
    characteristic_confidence: number;
    sector_confidence: number;
    confidence: number;
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
    characteristic: string;
    primary_identity: string;
    score: number;
}

// A primary_identity is a Layer-1 high-level business type (Agency,
// Consulting & Advisory, Software & SaaS, …). Layer-2 functional
// characteristics are nested under it.
interface PrimaryIdentity {
    name: string;
    description: string;
    identity_type: string;       // operator | service_provider | agency | software_vendor | investor | advisor | staffing | distributor | media | other
    operator_required: boolean;
}

// A characteristic is the Layer-2 subtype within an identity.
// This is the "leaf" the LLM matches industry strings to — but the campaign
// bucket actually used downstream is decided by the rollup (combo > spec >
// identity > Generic).
interface DiscoveredBucket {
    characteristic: string;
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
    sector_vocabulary: string[];
    primary_identities: PrimaryIdentity[];
    buckets: DiscoveredBucket[];   // Layer-2 characteristics
}

// Phase 1b returns Layer-1 + Layer-2 + Layer-3 per industry string. Layer-4
// (campaign bucket) is computed by SQL afterwards.
interface MatchChain {
    primary_identity: { name: string; score: number; reason: string };
    characteristic: { name: string; score: number; reason: string };
    sector: string;
    identity_type: string;
    generic: boolean;
    disqualified: boolean;
}

interface ContactMapRow {
    bucketing_run_id: string;
    contact_id: string;
    industry_string: string;
    primary_identity: string;
    characteristic: string;
    sector: string;
    canonical_classification: string;
    bucket_reason: string;
    pre_rollup_bucket_name: string;
    bucket_name: string;
    // Per-tag confidence (0-1). Used by computeContactRollup to gate which
    // layer the row may enter at: a row with sector_confidence < 0.6 is
    // ineligible for the combo (sector + characteristic) layer.
    identity_confidence: number;
    characteristic_confidence: number;
    sector_confidence: number;
    rollup_level: 'combo' | 'characteristic' | 'identity' | 'general';
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

const CONTACT_PAGE_SIZE = 2000;
// 200 UUIDs × ~37 chars ≈ 7,400-char URL — under PostgREST's 8KB request-line
// cap. Same constraint that broke list deletion at 1,000 IDs. Don't raise
// this without moving the IN list into a server-side RPC.
const ENRICHMENT_BATCH_SIZE = 200;

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

// Loads ONE keyset-paginated chunk of contacts and hydrates its
// enrichments. Returns the rows + the cursor id for the next chunk.
// Used by runAssignment to stream Phase 1b in chunks instead of
// loading every contact into memory at once. Each chunk's
// ContactRouteInput[] is short-lived — the caller routes it, appends
// to the persistent ContactMapRow[] output, and lets GC reclaim the
// chunk before the next iteration.
async function fetchContactsChunk(
    supabase: SupabaseClient,
    listNames: string[],
    lastId: string | null,
    pageSize: number,
    ctx?: BucketingCtx
): Promise<{ rows: ContactRouteInput[]; nextLastId: string | null; hasMore: boolean; pageMs: number; enrichmentMs: number }> {
    const pageStart = Date.now();
    // .range() (not .limit()) is required to bypass PostgREST's default
    // db-max-rows cap of 1000 — without this, page.length stayed at 1000
    // even for pageSize=2000, which made hasMore evaluate false on chunk
    // 1 and the entire run silently stopped after 1k contacts. Same fix
    // already lives on the vocab RPC at line 375.
    let q = supabase
        .from('contacts')
        .select('contact_id,company_name,company_website,industry,lead_list_name')
        .in('lead_list_name', listNames)
        .order('contact_id', { ascending: true })
        .range(0, pageSize - 1);
    if (lastId !== null) q = q.gt('contact_id', lastId);
    const { data, error } = await q;
    const pageMs = Date.now() - pageStart;
    if (error) {
        ctx?.log(`[Bucketing] contact page failed after ${pageMs}ms (lastId=${lastId?.slice(0, 8) || 'null'}): ${error.message}`, 'error');
        throw new Error(`contact fetch failed: ${error.message}`);
    }
    const page = (data || []) as any[];
    if (page.length === 0) {
        return { rows: [], nextLastId: lastId, hasMore: false, pageMs, enrichmentMs: 0 };
    }

    const rows: ContactRouteInput[] = page.map((r: any) => ({
        contact_id: r.contact_id,
        company_name: r.company_name || null,
        company_website: r.company_website || null,
        industry: r.industry || null,
        lead_list_name: r.lead_list_name || null,
        enrichment_status: null,
        classification: null,
        confidence: null,
        reasoning: null,
        error_message: null,
    }));

    // Hydrate enrichments for this chunk only (200 IDs/batch — see
    // ENRICHMENT_BATCH_SIZE comment for URL-length rationale).
    const enrStart = Date.now();
    const idToRow = new Map(rows.map(r => [r.contact_id, r]));
    const ids = rows.map(r => r.contact_id);
    for (let i = 0; i < ids.length; i += ENRICHMENT_BATCH_SIZE) {
        const slice = ids.slice(i, i + ENRICHMENT_BATCH_SIZE);
        const { data: edata, error: eErr } = await supabase
            .from('enrichments')
            .select('contact_id,status,classification,confidence,reasoning,error_message')
            .in('contact_id', slice);
        if (eErr) {
            ctx?.log(`[Bucketing] enrichment batch failed: ${eErr.message}`, 'error');
            throw new Error(`enrichment fetch failed: ${eErr.message}`);
        }
        for (const e of (edata || []) as any[]) {
            const r = idToRow.get(e.contact_id);
            if (!r) continue;
            r.enrichment_status = e.status || null;
            r.classification = e.classification || null;
            r.confidence = typeof e.confidence === 'number' ? e.confidence : null;
            r.reasoning = e.reasoning || null;
            r.error_message = e.error_message || null;
        }
    }
    const enrichmentMs = Date.now() - enrStart;

    return {
        rows,
        nextLastId: page[page.length - 1].contact_id,
        hasMore: page.length === pageSize,
        pageMs,
        enrichmentMs,
    };
}

// Backwards-compat: full-load fetch. Kept because Phase 1a's preview
// rebuild call still expects a single array. Phase 1b uses the chunked
// loader directly so it never holds the full contacts[] array.
async function fetchContactsForRouting(
    supabase: SupabaseClient,
    listNames: string[],
    ctx?: BucketingCtx
): Promise<ContactRouteInput[]> {
    return withHeartbeat('contact fetch', async () => {
        const overallStart = Date.now();
        const rows: ContactRouteInput[] = [];
        let lastId: string | null = null;
        let pageNum = 0;

        // Step 1: keyset-paginate contacts. Each page is independent and
        // O(log N) thanks to the (lead_list_name, contact_id) composite
        // index. We log per-page timing so future slow-page bugs are
        // visible at the page level instead of "the whole thing died".
        while (true) {
            await ctx?.checkCancel();
            pageNum++;
            const pageStart = Date.now();
            // .range() (not .limit()) — PostgREST default db-max-rows is
            // 1000, so .limit(2000) silently returned 1000 rows. See the
            // matching fix in fetchContactsChunk above.
            let q = supabase
                .from('contacts')
                .select('contact_id,company_name,company_website,industry,lead_list_name')
                .in('lead_list_name', listNames)
                .order('contact_id', { ascending: true })
                .range(0, CONTACT_PAGE_SIZE - 1);
            if (lastId !== null) q = q.gt('contact_id', lastId);
            const { data, error } = await q;
            const pageMs = Date.now() - pageStart;
            if (error) {
                ctx?.log(`[Bucketing] contact page ${pageNum} failed after ${pageMs}ms (lastId=${lastId?.slice(0, 8) || 'null'}): ${error.message}`, 'error');
                throw new Error(`contact fetch failed: ${error.message}`);
            }
            const page = (data || []) as any[];
            if (page.length === 0) break;
            for (const r of page) {
                rows.push({
                    contact_id: r.contact_id,
                    company_name: r.company_name || null,
                    company_website: r.company_website || null,
                    industry: r.industry || null,
                    lead_list_name: r.lead_list_name || null,
                    // Enrichments fields populated below.
                    enrichment_status: null,
                    classification: null,
                    confidence: null,
                    reasoning: null,
                    error_message: null,
                });
            }
            lastId = page[page.length - 1].contact_id;
            if (pageMs > 5000 || pageNum % 5 === 0) {
                ctx?.log(`[Bucketing] contacts page ${pageNum} loaded in ${pageMs}ms — total ${rows.length.toLocaleString()}`);
            }
            ctx?.progress({
                phase: 'phase1b',
                step: 'load_contacts',
                current: rows.length,
                note: `Loaded ${rows.length.toLocaleString()} contacts (page ${pageNum}, ${pageMs}ms)…`
            });
            if (page.length < CONTACT_PAGE_SIZE) break;
        }

        const contactsLoadedMs = Date.now() - overallStart;
        ctx?.log(`[Bucketing] contacts loaded: ${rows.length.toLocaleString()} rows in ${pageNum} pages, ${(contactsLoadedMs / 1000).toFixed(1)}s`);

        // Step 2: hydrate enrichments in batches keyed by contact_id.
        // PostgREST .in() over very long ID lists blows past the URL
        // size cap, so we chunk to ENRICHMENT_BATCH_SIZE per round trip.
        // Most contacts have at most one enrichment row, so the result
        // count tracks the input count closely.
        const idToRow = new Map<string, ContactRouteInput>(rows.map(r => [r.contact_id, r]));
        const ids = rows.map(r => r.contact_id);
        const enrStart = Date.now();
        let hydrated = 0;
        for (let i = 0; i < ids.length; i += ENRICHMENT_BATCH_SIZE) {
            await ctx?.checkCancel();
            const slice = ids.slice(i, i + ENRICHMENT_BATCH_SIZE);
            const batchStart = Date.now();
            const { data, error } = await supabase
                .from('enrichments')
                .select('contact_id,status,classification,confidence,reasoning,error_message')
                .in('contact_id', slice);
            const batchMs = Date.now() - batchStart;
            if (error) {
                ctx?.log(`[Bucketing] enrichment batch ${i}/${ids.length} failed after ${batchMs}ms: ${error.message}`, 'error');
                throw new Error(`enrichment fetch failed: ${error.message}`);
            }
            for (const e of (data || []) as any[]) {
                const r = idToRow.get(e.contact_id);
                if (!r) continue;
                r.enrichment_status = e.status || null;
                r.classification = e.classification || null;
                r.confidence = typeof e.confidence === 'number' ? e.confidence : null;
                r.reasoning = e.reasoning || null;
                r.error_message = e.error_message || null;
                hydrated++;
            }
            if (batchMs > 5000 || (i / ENRICHMENT_BATCH_SIZE) % 10 === 0) {
                ctx?.log(`[Bucketing] enrichment batch ${i + slice.length}/${ids.length} hydrated in ${batchMs}ms`);
            }
        }

        const totalMs = Date.now() - overallStart;
        ctx?.log(`[Bucketing] contacts+enrichments fetch: ${rows.length.toLocaleString()} contacts (${hydrated.toLocaleString()} with enrichment), total ${(totalMs / 1000).toFixed(1)}s (contacts ${(contactsLoadedMs / 1000).toFixed(1)}s, enrichments ${((Date.now() - enrStart) / 1000).toFixed(1)}s)`, 'phase');
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
THE 3-LAYER CLASSIFICATION MODEL
========================================

Every company is described along three independent axes. The campaign
bucket is decided AFTERWARDS by combining counts across those axes.

Layer 1 — PRIMARY IDENTITY (high-level business type, ~6-12 total)
   What kind of company is this AT ITS CORE?
   Examples: "Agency", "Consulting & Advisory", "Software & SaaS",
   "IT Services", "Financial Services", "Real Estate Operator",
   "Healthcare Operator", "Education Operator", "Staffing & Recruiting",
   "Legal Services", "Accounting & Tax".

Layer 2 — CHARACTERISTIC (subtype within identity)
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

Layer 3 — SECTOR (optional vertical served, ~10-20 total)
   Who do they MAINLY serve, if explicitly stated?
   Examples: "Healthcare", "Real Estate", "Government", "Education",
   "Manufacturing", "Financial Services", "Hospitality", "Energy",
   "Non-profit", "Multi-industry", or "" (none).

CAMPAIGN BUCKET (decided downstream, NOT by you)
   The actual outreach bucket is computed by the rollup engine — not
   predicted. It combines volume across the three axes:
     - "{sector} {characteristic}" if that combo has enough leads
       (e.g. "Real Estate SEO Agency").
     - Else "{characteristic}" (e.g. "SEO Agency").
     - Else "{primary_identity}" (e.g. "Agency").
     - Else "General" (single catch-all — disqualified rows go here too).
   You produce accurate Layer 1-3 classifications. The system computes
   the bucket name from your output + per-bucket contact counts.

========================================
UNIVERSAL ROUTING PRINCIPLE
========================================

Classify each company by its CORE BUSINESS IDENTITY first.
Classify SECTOR SERVED second.
Never reverse that order.

- A private equity firm focused on healthcare is a Private Equity Firm
  in Financial Services with sector = Healthcare. NOT a healthcare
  company.
- An IT consultancy serving hospitals is an IT Consulting firm in
  Consulting & Advisory with sector = Healthcare.
- A marketing agency for life sciences is an Agency.
- "Software for schools" is Software & SaaS, not Education.

Distinguish: WHAT the company is vs WHO it serves. Identity decides
primary_identity + characteristic. Sector served decides
sector.

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
Software & SaaS, …) — the vertical they serve goes in sector.

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
4. Reusability > Novelty. Identities + characteristics must be reusable.
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

    // Pre-flight schema check — fails fast with a precise list of
    // missing tables/columns/RPCs instead of letting the run get half-
    // way through tagger calls and crash on a column write.
    const schemaRes = await checkBucketingSchema(supabase);
    if (!schemaRes.ok) {
        ctx.log(`[Bucketing ${runId}] schema check failed:\n${schemaRes.summary}`, 'error');
        throw new Error(schemaRes.summary);
    }

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
    //
    // The vocab RPC groups by (industry, enrichment_status), so the same
    // industry text can appear in two rows if some contacts enriched and
    // others didn't (e.g. "Wealth Management Services" with 100 completed
    // + 3 scrape_errors). bucket_industry_map's PK is (run_id, industry),
    // so we MUST collapse to one row per industry before inserting —
    // otherwise the upsert hits "ON CONFLICT DO UPDATE command cannot
    // affect row a second time". A 'completed' row beats anything else
    // because the LLM tagger has the best signal there; everything else
    // collapses into a single Disqualified passthrough row.
    const byIndustry = new Map<string, VocabRow>();
    for (const v of vocabRows) {
        const cur = byIndustry.get(v.industry);
        if (!cur) { byIndustry.set(v.industry, v); continue; }
        const curIsCompleted = (cur.enrichment_status || 'completed') === 'completed';
        const newIsCompleted = (v.enrichment_status || 'completed') === 'completed';
        if (newIsCompleted && !curIsCompleted) {
            byIndustry.set(v.industry, { ...v, n: Number(cur.n || 0) + Number(v.n || 0) });
        } else {
            // Same precedence — accumulate counts so totals stay correct.
            cur.n = Number(cur.n || 0) + Number(v.n || 0);
        }
    }
    const dedupedVocab = Array.from(byIndustry.values());
    const completedVocab = dedupedVocab.filter(r => (r.enrichment_status || 'completed') === 'completed');
    const dqVocab = dedupedVocab.filter(r => (r.enrichment_status || 'completed') !== 'completed');
    ctx.log(`[Bucketing ${runId}] partition: ${completedVocab.length} taggable, ${dqVocab.length} → Disqualified (failed/missing/scrape_error)${vocabRows.length !== dedupedVocab.length ? ` — ${vocabRows.length - dedupedVocab.length} duplicate (industry, status) rows collapsed` : ''}`);

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
            canonical_classification: 'Disqualified',
            llm_reason: `enrichment_status=${reason}`,
        });
    }

    // ── Load active taxonomy library (3 SELECTs) ─────────────────────
    const snapshot = await loadTaxonomySnapshot(supabase);
    ctx.log(`[Bucketing ${runId}] taxonomy library: ${snapshot.identities.length} identities, ${snapshot.characteristics.length} characteristics, ${snapshot.sectors.length} sectors`);

    if (completedVocab.length > 0) {
        // ── Tag completed industries (batched, model selected at Setup) ────
        await ctx.checkCancel();
        // Per-run model choice from the Setup screen, written to
        // bucketing_runs.taxonomy_model in /determine. Fall back to the
        // historical Anthropic default if a run pre-dates the picker.
        const phase1aModel: string = (run as any).taxonomy_model || TAXONOMY_MODEL_ANTHROPIC;
        ctx.progress({
            phase: 'phase1a',
            step: 'tagging',
            current: 0,
            total: completedVocab.length,
            note: `Tagging ${completedVocab.length} industries with ${phase1aModel}…`
        });
        const tagResult = await withHeartbeat(
            `${phase1aModel} tagging (${completedVocab.length} industries)`,
            () => tagIndustries(supabase, phase1aModel, completedVocab, snapshot, runId, ctx),
            ctx
        );
        totalCost += tagResult.costUsd;
        ctx.log(`[Bucketing ${runId}] tagging: ${tagResult.taggings.length} results, $${tagResult.costUsd.toFixed(4)}, model=${tagResult.modelUsed}`);

        // ── Build map rows from taggings ─────────────────────────────
        const identitySet = new Set(snapshot.identities.map(i => i.name));
        const charSet = new Set(snapshot.characteristics.map(c => c.name));
        const sectorSet = new Set(snapshot.sectors.map(s => s.name));
        const dqIdentities = new Set(snapshot.identities.filter(i => i.is_disqualified).map(i => i.name));
        // Identity-DQ cascade is OFF by default — we trust the tagger's per-row
        // is_disqualified judgment. Set apply_identity_dq_cascade=true on
        // the run to restore the old auto-DQ behavior for [DQ]-flagged
        // identities (e.g. force every "Consumer & Retail" tag to DQ).
        const cascadeDq = !!(run as any).apply_identity_dq_cascade;

        for (const t of tagResult.taggings) {
            const conf01 = Math.max(0, Math.min(1, (t.confidence || 0) / 10));
            const lowConf = (t.confidence || 0) < PHASE1A_QA_FLOOR;
            const identityIsDq = !!(t.identity && dqIdentities.has(t.identity));
            // DQ confidence floor — only honor a DQ verdict when the tagger is
            // ≥ 7/10 sure about the identity. Below that, route to General
            // (with needs_qa=true so the user can spot-check) instead of
            // silently excluding the contact from outreach.
            const idConf = t.identity_confidence || 0;
            const dqByLLM = !!t.is_disqualified && idConf >= PHASE1A_DQ_FLOOR;
            const dqByCascade = cascadeDq && identityIsDq && idConf >= PHASE1A_DQ_FLOOR;
            const isDisqualified = dqByLLM || dqByCascade;
            // Track when LLM said DQ but we overrode due to low confidence —
            // flag for QA. Easy filter on bucket_industry_map: needs_qa=true
            // AND is_disqualified=false AND llm_reason ILIKE '%dq%' would
            // surface these soft-DQ borderlines.
            const dqDowngraded = !!t.is_disqualified && idConf < PHASE1A_DQ_FLOOR;

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

            // Canonical classification: concise truth statement combining the
            // narrowest available fields. Used for the CSV export.
            const canonical = isDisqualified
                ? 'Disqualified'
                : (t.sector && t.characteristic ? `${t.sector} ${t.characteristic}`
                    : (t.characteristic || t.identity || 'Generic'));

            preMapRows.push({
                bucketing_run_id: runId,
                industry_string: t.industry,
                raw_industry: t.industry,
                bucket_name: preBucket,
                source: 'llm_phase1a',
                confidence: Number(conf01.toFixed(2)),
                identity_confidence: Number(((t.identity_confidence || 0) / 10).toFixed(2)),
                characteristic_confidence: Number(((t.characteristic_confidence || 0) / 10).toFixed(2)),
                sector_confidence: Number(((t.sector_confidence || 0) / 10).toFixed(2)),
                identity: t.identity,
                characteristic: t.characteristic,
                sector: t.sector,
                is_new_identity: t.is_new_identity && !identitySet.has(t.identity || ''),
                is_new_characteristic: t.is_new_characteristic && !charSet.has(t.characteristic || ''),
                is_new_sector: t.is_new_sector && !sectorSet.has(t.sector || ''),
                is_disqualified: isDisqualified,
                is_generic: false,
                needs_qa: lowConf || dqDowngraded,
                canonical_classification: canonical,
                llm_reason: dqDowngraded
                    ? `DQ-downgraded (identity_confidence=${idConf}/10 below DQ floor of ${PHASE1A_DQ_FLOOR}). Original reason: ${t.reason}`
                    : t.reason
            });

            // If this is the first time we've seen vocab.n on a tagged row,
            // include n in reasons for downstream debug.
        }
        ctx.progress({
            phase: 'phase1a',
            step: 'tagging',
            current: tagResult.taggings.length,
            total: completedVocab.length,
            note: `Tagged ${tagResult.taggings.length} industries`
        });
    }

    // ── Bulk insert all map rows ─────────────────────────────────────
    // Final dedupe pass — the (run_id, industry_string) PK rejects any
    // accidental duplicate. Keep the last write (LLM tagging beats
    // disqualified passthrough since the tagger appends after the DQ
    // passthrough loop).
    const dedupedMap = new Map<string, any>();
    for (const r of preMapRows) dedupedMap.set(r.industry_string, r);
    const finalRows = Array.from(dedupedMap.values());
    if (finalRows.length !== preMapRows.length) {
        ctx.log(`[Bucketing ${runId}] dedupe: ${preMapRows.length} → ${finalRows.length} rows (collapsed ${preMapRows.length - finalRows.length} dup industry strings)`, 'warn');
    }
    for (let i = 0; i < finalRows.length; i += 1000) {
        const chunk = finalRows.slice(i, i + 1000);
        const { error: upErr } = await supabase.from('bucket_industry_map')
            .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
        if (upErr) throw new Error(`bucket_industry_map insert failed: ${upErr.message}`);
    }
    ctx.log(`[Bucketing ${runId}] wrote ${finalRows.length} map rows`);

    // ── Build a synthetic taxonomy_proposal for the existing UI ──────
    // The Review screen reads taxonomy_proposal.{primary_identities, buckets}
    // — we synthesize them from the unique tags actually used so the user
    // sees a familiar tree (Identity → Characteristic) backed by the new
    // tag-based classification.
    const usedIdentitySet = new Set<string>();
    const usedCharByKey = new Map<string, { spec: string; identity: string; description: string }>();
    const usedSectors = new Set<string>();
    for (const r of finalRows) {
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

    // Only emit a bucket per (identity, characteristic) pair where the tagger
    // committed to BOTH layers. Identity-only rows (the tagger was sure about
    // identity but not characteristic) don't get a fake "identity as spec"
    // bucket — instead Phase 1b's rollup decides their fate based on
    // identity volume vs the threshold (clears → identity bucket, doesn't →
    // General with reason=low_volume). This keeps the user's rule "if you
    // can't decide, send to General" honest.
    const buckets: DiscoveredBucket[] = [];
    for (const c of usedCharByKey.values()) {
        buckets.push({
            characteristic: c.spec,
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
        });
    }

    await supabase.from('bucketing_runs').update({
        taxonomy_proposal: {
            observed_patterns: [],
            sector_vocabulary: Array.from(usedSectors),
            primary_identities: primaryIdentities,
            buckets: buckets
        },
        taxonomy_snapshot: {
            identities: snapshot.identities,
            characteristics: snapshot.characteristics,
            sectors: snapshot.sectors
        },
        // taxonomy_model is now set by /determine from the user's Setup
        // screen choice — don't overwrite it here.
        cost_usd: totalCost,
        total_contacts: totalContacts,
        status: 'taxonomy_ready',
        taxonomy_completed_at: new Date().toISOString()
    }).eq('id', runId);

    ctx.log(`[Bucketing ${runId}] Phase 1a done — tagged ${finalRows.length} industries (${dqVocab.length} DQ, ${completedVocab.length} via LLM), $${totalCost.toFixed(4)}`, 'phase');
    ctx.progress({
        phase: 'phase1a', step: 'done', current: 1, total: 1,
        note: `Tagging complete — ${primaryIdentities.length} identities, ${buckets.length} characteristics, ${usedSectors.size} sectors used`
    });
}

// ────────────────────────────────────────────────────────────────────
// PHASE 1A HELPERS — taxonomy snapshot + per-string tagging
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

// Unified Phase 1a tagger. Dispatches by model: Anthropic for `claude-*`,
// OpenAI for `gpt-*`. Both paths consume the same batched system+user
// prompts and produce the same IndustryTagging[] shape, so the rest of
// runTaxonomyProposal is model-agnostic. Cost is computed via the
// matching pricing table.
async function tagIndustries(
    supabase: SupabaseClient,
    model: string,
    vocab: VocabRow[],
    snapshot: TaxonomySnapshot,
    runId: string,
    ctx: BucketingCtx
): Promise<{ taggings: IndustryTagging[]; costUsd: number; modelUsed: string }> {
    const isAnthropic = model.startsWith('claude-');
    const isOpenAI = model.startsWith('gpt-');
    if (!isAnthropic && !isOpenAI) {
        throw new Error(`Unsupported Phase 1a model: ${model}`);
    }

    const anthropic = isAnthropic ? await getAnthropic(supabase) : null;
    if (isAnthropic && !anthropic) {
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
    let batchesAttempted = 0;
    let batchesFailed = 0;
    let firstError: string | null = null;
    const taggings: IndustryTagging[] = [];

    await Promise.all(batches.map((batch) => limit(async () => {
        await ctx.checkCancel();
        batchesAttempted++;
        const userPrompt = JSON.stringify({
            industries: batch.map((v, i) => ({
                id: i,
                industry: v.industry,
                sample_companies: v.sample_companies?.slice(0, 2) || []
            }))
        });
        try {
            let text = '';
            if (isAnthropic) {
                const resp = await anthropic!.messages.create({
                    model,
                    max_tokens: 2000,
                    system: [{ type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }],
                    messages: [{ role: 'user', content: userPrompt }]
                }, { timeout: TAXONOMY_TIMEOUT_MS });
                const usage: any = (resp as any).usage || {};
                totalIn += usage.input_tokens || 0;
                totalOut += usage.output_tokens || 0;
                totalCachedIn += usage.cache_read_input_tokens || 0;
                text = (resp.content as any[])
                    .filter(b => b.type === 'text').map(b => b.text).join('\n');
            } else {
                // OpenAI chat completions with json_object response_format
                // forces valid JSON without us hand-rolling a schema.
                const resp = await fetch('https://api.openai.com/v1/chat/completions', {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${getOpenAIKey()}`, 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        model,
                        max_tokens: 2000,
                        response_format: { type: 'json_object' },
                        messages: [
                            { role: 'system', content: systemPrompt },
                            { role: 'user', content: userPrompt }
                        ]
                    }),
                    signal: AbortSignal.timeout(TAXONOMY_TIMEOUT_MS)
                });
                if (!resp.ok) {
                    const body = await resp.text();
                    throw new Error(`OpenAI ${resp.status}: ${body.slice(0, 300)}`);
                }
                const json: any = await resp.json();
                const usage = json.usage || {};
                const cached = usage.prompt_tokens_details?.cached_tokens || 0;
                totalIn += (usage.prompt_tokens || 0);
                totalCachedIn += cached;
                totalOut += usage.completion_tokens || 0;
                text = json.choices?.[0]?.message?.content || '';
            }
            const parsed = parseTaggingJson(text, batch);
            for (const t of parsed) taggings.push(t);
        } catch (err: any) {
            batchesFailed++;
            if (!firstError) firstError = err.message || String(err);
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
                    identity_confidence: 0,
                    characteristic_confidence: 0,
                    sector_confidence: 0,
                    confidence: 0,
                    reason: `tagging_error: ${err.message?.slice(0, 200)}`
                });
            }
        }
        done += batch.length;
        if (done % (PHASE1A_BATCH_SIZE * 10) === 0 || done === vocab.length) {
            ctx.progress({
                phase: 'phase1a',
                step: 'tagging',
                current: done,
                total: vocab.length,
                note: `Tagged ${done}/${vocab.length} industries…`
            });
        }
    })));

    // Catastrophic failure detection: if more than half of all batches
    // failed, the run is producing garbage (almost certainly an API auth /
    // rate-limit / network problem). Better to fail loud than ship a
    // 100%-needs-qa "successful" run.
    if (batchesAttempted > 0 && batchesFailed / batchesAttempted > 0.5) {
        const pct = Math.round((batchesFailed / batchesAttempted) * 100);
        throw new Error(
            `${model} tagging failed for ${pct}% of batches (${batchesFailed}/${batchesAttempted}). ` +
            `First error: ${firstError || 'unknown'}. Most likely causes: (1) API key invalid or out of credit, ` +
            `(2) rate limit hit, (3) network outage. Check the Connectors page and retry.`
        );
    }

    const costUsd = isAnthropic
        ? computeAnthropicCost(model, totalIn - totalCachedIn, totalOut)
            + (totalCachedIn / 1_000_000) * (ANTHROPIC_PRICING[model]?.input || 3) * 0.1
        : computeOpenAICost(model, totalIn - totalCachedIn, totalCachedIn, totalOut);
    return { taggings, costUsd, modelUsed: model };
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

For each industry text the user provides, return three independent tags. The three tags form a layered truth model: identity is broadest, sector is narrowest.

1. **Identity** (REQUIRED) — what kind of company is it at its core? Pick from the IDENTITY library below. If nothing fits well, propose a new identity name and set is_new_identity=true. Identities marked [DQ] are *historically* disqualified for B2B outreach; treat the marker as a strong hint, not a hard rule (see DISQUALIFICATION RULES).

2. **Characteristic** (optional) — the specific subtype inside the identity. Pick from the CHARACTERISTICS library below; the parent_identity must match the identity you chose. If nothing fits well, propose a new one and set is_new_characteristic=true. If no characteristic applies, return null.

3. **Sector** (optional) — the specific vertical the company serves. Pick from the SECTOR library below. Sector is independent of identity (a "Marketing Agency serving Healthcare" has identity=Agency, sector=Healthcare / Medical). If the company doesn't have a clear served sector, return null. If nothing fits, propose new and set is_new_sector=true.

CLASSIFICATION RULES (critical):
- Classify by core business identity FIRST, sector served SECOND. A PE firm focused on healthcare is identity=Financial Services, NOT Healthcare.
- Operators in a vertical (a hospital, a SaaS company) belong to that identity. Service providers TO that vertical (an agency that markets healthcare, a consultant to insurers) belong to the service identity, with the vertical going into sector.
- **SERVICES vs PRODUCTS — do not collapse them**. Companies that *rent*, *maintain*, *repair*, or *operate-as-a-service* should NEVER be tagged "B2B Product Manufacturer" / "Industrial Equipment / Machinery" just because they're industrial-adjacent. If no services characteristic fits, prefer a Services / Field Services / Equipment Rental / Maintenance Services characteristic if one exists in the library — otherwise propose one (is_new_characteristic=true) rather than picking a wrong manufacturer label. Reserve Manufacturer characteristics for companies whose primary act is *making* a physical product.
- **Talent / Artist Management is an Agency, not Consulting.** Don't tag artist management firms, modeling agencies, or sports management firms as Management Consulting.
- **Game / Software studios are Software & SaaS (or a new Software identity), not Hospitality / Travel / Consumer Retail** — even if the games are sold to consumers. Use is_disqualified=true only if the company is unambiguously pure-consumer with no B2B angle.

DISQUALIFICATION RULES (be CONSERVATIVE — false negatives are worse than false positives):
- Set is_disqualified=true ONLY when the text gives clear, explicit evidence the company is a pure-consumer / hyper-local / low-ticket business with no plausible B2B angle. Examples:
    * "Family-owned restaurant in Austin"
    * "Independent dog grooming salon"
    * "Local plumbing business, residential only"
    * "Direct-to-consumer candle brand"
- Do NOT auto-DQ just because the identity is marked [DQ] or the text mentions retail/hospitality/consumer. Many such companies have a B2B / wholesale / SaaS / advisory arm that we'd want to invite. Examples that should stay inviteable (is_disqualified=false):
    * "Hospitality SaaS for boutique hotels" → Software & SaaS, not DQ
    * "Wholesale distribution platform for restaurants" → Software & SaaS or Consulting, not DQ
    * "Multi-location dental group with 40+ practices" → Healthcare & Medical operator, not DQ
    * "DTC brand with B2B wholesale channel" → keep inviteable
- When uncertain, set is_disqualified=false and let the user decide downstream. The reason field should explain why if you DO disqualify.

CONFIDENCE SCORING (per tag, 1-10):
- Return THREE independent confidence scores: identity_confidence, characteristic_confidence, sector_confidence.
- Each scores how sure you are about THAT tag specifically, not the whole row.
- A "PE firm investing in healthcare" might be identity_confidence=10, characteristic_confidence=9, sector_confidence=8 (sector clear).
- A "consulting firm" with no vertical mentioned might be identity_confidence=8, characteristic_confidence=5 (which kind of consulting?), sector_confidence=2 (no sector mentioned — return null sector and score 1-3).
- <6 means: this specific tag is a guess; downstream code may ignore it.
- If you return null for a tag (e.g. no sector applicable), set its confidence to 1.

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
      "identity_confidence": <1-10 integer>,
      "characteristic_confidence": <1-10 integer>,
      "sector_confidence": <1-10 integer>,
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
        // Per-tag confidences with back-compat: if the tagger returns the
        // legacy single `confidence` (e.g. older cached prompts), use it
        // for all three. The min becomes the overall confidence.
        const legacyConf = typeof item.confidence === 'number' ? item.confidence : 0;
        const idConf = typeof item.identity_confidence === 'number' ? item.identity_confidence : legacyConf;
        const chConf = typeof item.characteristic_confidence === 'number' ? item.characteristic_confidence : legacyConf;
        const secConf = typeof item.sector_confidence === 'number' ? item.sector_confidence : legacyConf;
        out.push({
            industry: v.industry,
            identity: nz(item.identity),
            is_new_identity: !!item.is_new_identity,
            characteristic: nz(item.characteristic),
            is_new_characteristic: !!item.is_new_characteristic,
            sector: nz(item.sector),
            is_new_sector: !!item.is_new_sector,
            is_disqualified: !!item.is_disqualified,
            identity_confidence: idConf,
            characteristic_confidence: chConf,
            sector_confidence: secConf,
            confidence: Math.min(idConf || 10, chConf || 10, secConf || 10),
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
    // marginal signal). The discovery LLM now sees a tight `industry | n | 2 samples`
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

These characteristics were defined in prior runs and have proven useful. If
a discovered pattern aligns with one of these at score >= 0.7, REUSE it:
copy characteristic, primary_identity, and description VERBATIM
and set library_match_id. Do NOT invent a near-duplicate with different
wording.

${preferred.map(p => `id=${p.id} | characteristic="${p.bucket_name}" | identity="${p.direct_ancestor || p.root_category}" | desc="${p.description || ''}"`).join('\n')}` : '';

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

B) CHARACTERISTICS (Layer 2): 30–60 characteristics across the identities.
   Each characteristic belongs to exactly ONE primary_identity.
   Examples (always coupled): Agency → "SEO Agency", "Branding Agency",
   "Performance Marketing Agency", "B2B Demand Generation Agency". Consulting
   & Advisory → "IT Consulting", "Management Consulting", "Revenue Operations
   Consulting", "M&A Advisory". Financial Services → "Private Equity Firm",
   "Venture Capital Fund", "Family Office". Software & SaaS → "MarTech SaaS",
   "FinTech SaaS", "Vertical SaaS", "PropTech SaaS".

C) SECTOR VOCABULARY (Layer 3): a controlled list of sector terms that
   appear in the data as "served vertical" signals. These NEVER become
   primary identities or characteristics — they are sector values used in
   Phase 1b. Examples: "Healthcare", "Real Estate", "Government",
   "Education", "Manufacturing", "Financial Services", "Hospitality",
   "Energy", "Non-profit", "Legal", "Multi-industry".

NO-SHORTCUTS RULES:
1) Base everything ONLY on patterns evidenced in the vocabulary.
2) Sector words must NOT become a primary_identity unless the vocabulary
   clearly contains OPERATORS in that sector (clinics, schools, city
   governments, etc.) AND that identity carries operator_required=true.
3) No near-duplicates. Specializations differing only by word order or
   synonym must be merged.
4) An identity with no characteristics is invalid. Every identity must
   have ≥1 characteristic underneath it.
5) Coverage requirement: proposed characteristics + identities should
   collectively cover at least 80% of usable contact volume represented
   in the vocabulary. General is for bad/no-data, clear non-ICP, and true
   no-fit cases — do NOT create a niche-only taxonomy that dumps normal
   B2B companies into General.

❌ TOO BROAD as a CHARACTERISTIC (must be a primary_identity instead):
"SaaS", "B2B SaaS", "Marketing Agency", "Consulting Firm", "Software".

❌ TOO NARROW (forbidden):
"TikTok ads agency for DTC candle brands", "Family office for German
real estate developers", "RevOps consulting for Series B HR SaaS".

✅ GOLDILOCKS characteristics:
"SEO Agency", "Performance Marketing Agency", "B2B Demand Generation Agency",
"IT Consulting", "Revenue Operations Consulting", "M&A Advisory",
"Private Equity Firm", "Venture Capital Fund", "MarTech SaaS",
"PropTech SaaS", "Managed IT Services", "Cybersecurity Services",
"Healthcare Clinic / Hospital" [operator_required=true],
"K-12 School District" [operator_required=true].

PER-CHARACTERISTIC ROUTING METADATA — REQUIRED FIELDS:
- identity_type ∈ {operator, service_provider, agency, software_vendor,
  investor, advisor, staffing, distributor, media, other}
- operator_required: true ONLY for characteristics whose identity is an
  operator identity (Healthcare Operator → "Medical Clinic / Hospital",
  Education Operator → "K-12 School District").
- priority_rank: 1–10. 1 = strongest identity nouns (PE Firm, Law Firm,
  Marketing Agency, MSP). 10 = weakest (operator characteristics that
  lose to enabler signals).
- include / exclude / example_strings: keyword hints that drive Phase 1b
  routing. include = phrases that PROVE this characteristic. exclude =
  phrases that should route AWAY. example_strings = verbatim industry
  strings from the vocabulary.
${preferredSection}

REQUIRED PROCESS — DO NOT SKIP:
A) List 10–15 high-frequency patterns observed in the vocabulary.
B) Use those patterns to justify the top primary_identities and which
   characteristics belong under each.

OUTPUT (strict JSON only, no prose, no markdown fences):

{
  "observed_patterns": [<10–15 strings>],
  "sector_vocabulary": [<sector terms; NEVER appear as identities or characteristics>],
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
      "characteristic": "<characteristic, Layer 2>",
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
- 6–12 primary_identities, 30–60 characteristics total.
- Every characteristic's primary_identity field exactly matches a name in the
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
    if (!Array.isArray(parsed.sector_vocabulary)) parsed.sector_vocabulary = [];
    if (!Array.isArray(parsed.observed_patterns)) parsed.observed_patterns = [];
    return parsed;
}

function buildDiscoverySchema() {
    return {
        type: 'object',
        additionalProperties: false,
        required: ['observed_patterns', 'sector_vocabulary', 'primary_identities', 'buckets'],
        properties: {
            observed_patterns: { type: 'array', items: { type: 'string' } },
            sector_vocabulary: { type: 'array', items: { type: 'string' } },
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
                    required: ['characteristic', 'primary_identity', 'description',
                               'identity_type', 'operator_required', 'priority_rank',
                               'include', 'exclude', 'example_strings',
                               'estimated_usage_label', 'rough_volume_estimate', 'library_match_id'],
                    properties: {
                        characteristic: { type: 'string' },
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
    keep?: string[];                              // characteristic names
    rename?: Record<string, string>;              // {old spec name: new spec name}
    add?: {
        characteristic: string;
        primary_identity: string;
        description: string;
        identity_type?: string;
        operator_required?: boolean;
    }[];
    min_volume?: number;
    bucket_budget?: number;
    preferred_library_ids?: string[];             // moved from Setup screen — see review screen
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
            leaves = leaves.map(b => b.characteristic === oldName
                ? { ...b, characteristic: target }
                : b);
        }
    }

    if (edits.keep) {
        const keepSet = new Set(edits.keep.map(s => s.trim()));
        leaves = leaves.filter(b => keepSet.has(b.characteristic));
    }

    if (edits.add) {
        for (const a of edits.add) {
            const spec = (a.characteristic || '').trim();
            const ident = (a.primary_identity || '').trim();
            if (!spec || RESERVED.has(spec.toLowerCase()) || !ident) continue;
            if (leaves.some(l => l.characteristic === spec)) continue;
            leaves.push({
                characteristic: spec,
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
            sector_vocabulary: sourceTaxonomy.sector_vocabulary || [],
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
    if (Array.isArray(edits.preferred_library_ids)) {
        update.preferred_library_ids = edits.preferred_library_ids;
    }
    await supabase.from('bucketing_runs').update(update).eq('id', runId);
    ctx.log(`[Bucketing ${runId}] taxonomy edits applied: ${leaves.length} characteristics`);

    // Rebuild the preview map so Review counts reflect the edited taxonomy
    // (renames / drops / new specs all change which industries map where).
    // Cheap: one batched embedding call against the run's vocab.
    try {
        await rebuildPreviewMap(supabase, runId, leaves, sourceTaxonomy.sector_vocabulary || [], run.list_names || [], ctx);
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
    // Same pre-flight as Phase 1a — covers cases where the user retries
    // assignment after a schema migration ran mid-cycle. Cheap probe.
    const schemaRes = await checkBucketingSchema(supabase);
    if (!schemaRes.ok) {
        ctx.log(`[Bucketing ${runId}] schema check failed:\n${schemaRes.summary}`, 'error');
        throw new Error(schemaRes.summary);
    }

    const { data: run, error } = await supabase.from('bucketing_runs').select('*').eq('id', runId).single();
    if (error || !run) throw new Error(`Run not found: ${error?.message}`);

    const final = (run.taxonomy_final || run.taxonomy_proposal) as DiscoveryOutput | null;
    // Empty buckets[] is a legitimate state when the entire selected
    // list is unenriched / scrape-errored — every contact will route to
    // Disqualified via the catchall in the rollup. Don't refuse the run
    // just because there are no campaign buckets to fan out to.
    const leaves = final?.buckets || [];
    if (leaves.length === 0) {
        ctx.log(`[Bucketing ${runId}] no proposal buckets — every contact will route to General/Disqualified per fallback rules`, 'warn');
    }
    const sectorVocab: string[] = (final as any)?.sector_vocabulary || [];
    const finalSpecNames = new Set(leaves.map(l => l.characteristic));

    // Library buckets the user opted into for this run. Only keep library
    // specs that survived Review; a dropped library spec must not reappear
    // during assignment.
    const preferredIds: string[] = Array.isArray(run.preferred_library_ids) ? run.preferred_library_ids : [];
    let libraryBuckets: any[] = [];
    if (preferredIds.length > 0) {
        const { data } = await supabase
            .from('bucket_library').select('*').in('id', preferredIds);
        libraryBuckets = (data || []).filter((b: any) =>
            (b.characteristic || b.bucket_name) && (b.primary_identity || b.direct_ancestor)
            && finalSpecNames.has(b.characteristic || b.bucket_name)
        );
    }

    await supabase.from('bucketing_runs').update({ status: 'assigning' }).eq('id', runId);

    let totalCost = Number(run.cost_usd || 0);
    // Step-level timing telemetry. Each step pushes its elapsed ms into
    // `timings` so the run-end summary tells us exactly where the seconds
    // went, and any future timeout pinpoints the slow step at a glance.
    const runStart = Date.now();
    const timings: { step: string; ms: number }[] = [];
    const timeStep = async <T>(label: string, fn: () => Promise<T>): Promise<T> => {
        const t0 = Date.now();
        try {
            const out = await fn();
            const ms = Date.now() - t0;
            timings.push({ step: label, ms });
            ctx.log(`[Bucketing ${runId}] ⏱  ${label}: ${(ms / 1000).toFixed(2)}s`);
            return out;
        } catch (err: any) {
            const ms = Date.now() - t0;
            timings.push({ step: `${label} (FAILED)`, ms });
            ctx.log(`[Bucketing ${runId}] ⏱  ${label} FAILED after ${(ms / 1000).toFixed(2)}s: ${err.message}`, 'error');
            throw err;
        }
    };

    await timeStep('clear_previous_assignments', async () => {
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
    });

    // Streaming Phase 1b: load contacts in keyset chunks, route each
    // chunk through library / embedding / LLM matchers, append to the
    // persistent assignedRows[] array, then drop the chunk from memory.
    // Without this, peak memory holds both the contacts[] array (~100 MB
    // on 500k contacts) AND the assignedRows[] (~150 MB) at the same
    // time. Chunked: only the current chunk's contacts (~5 MB) plus
    // assignedRows is alive.
    await ctx.checkCancel();
    ctx.log(`[Bucketing ${runId}] step 1-4/5: streaming routing in chunks of ${CONTACT_PAGE_SIZE.toLocaleString()}`, 'phase');
    ctx.progress({ phase: 'phase1b', step: 'streaming_route', note: 'Loading + routing contacts in chunks…' });

    const assignedRows: ContactMapRow[] = [];
    let lastContactId: string | null = null;
    let chunkNum = 0;
    let totalLoaded = 0;
    let totalUsable = 0;
    let totalDqEarly = 0;
    let phase1aJoined = 0;
    let libraryMatched = 0;
    let embeddingMatched = 0;
    let llmRouted = 0;

    // Load Phase 1a's per-industry-string taxonomy once and reuse it for
    // every chunk. The JOIN-first step below short-circuits the LLM for any
    // contact whose enriched classification text is already in the map —
    // typically 90%+ of usable contacts, since the vocab IS the deduped
    // set of classification strings. Cuts Phase 1b LLM cost by ~40x.
    const taxonomyMap = await fetchPhase1aTaxonomyMap(supabase, runId, ctx);
    ctx.log(`[Bucketing ${runId}] Phase 1a taxonomy: ${taxonomyMap.size.toLocaleString()} mapped industry strings ready for JOIN-first lookup`);

    await timeStep('streaming_route_all_chunks', async () => {
        while (true) {
            await ctx.checkCancel();
            chunkNum++;

            const chunk = await fetchContactsChunk(supabase, run.list_names, lastContactId, CONTACT_PAGE_SIZE, ctx);
            if (chunk.rows.length === 0) break;
            totalLoaded += chunk.rows.length;
            lastContactId = chunk.nextLastId;

            // Filter unclassifiable rows from the chunk straight to
            // Disqualified — never sent to LLM, just pushed to output.
            const usableInChunk: ContactRouteInput[] = [];
            for (const c of chunk.rows) {
                const reason = getUnclassifiableReason(c);
                if (reason) {
                    assignedRows.push(makeGeneralContactRow(runId, c, reason));
                    totalDqEarly++;
                } else {
                    usableInChunk.push(c);
                }
            }
            totalUsable += usableInChunk.length;

            let pending: ContactRouteInput[] = usableInChunk;

            // JOIN-first: short-circuit using Phase 1a's per-industry-string
            // tag. If the contact's enriched classification text is already
            // in taxonomyMap, copy that tag straight onto the contact and
            // skip every downstream matcher (library / embedding / LLM).
            // Empty-tag rows (very rare — taxonomy edits dropped the spec
            // since 1a wrote the map) fall through to the cascade.
            if (pending.length > 0 && taxonomyMap.size > 0) {
                const stillPending: ContactRouteInput[] = [];
                for (const c of pending) {
                    const industryKey = (c.classification || c.industry || '').trim();
                    const tax = industryKey ? taxonomyMap.get(industryKey) : null;
                    const usable = tax && (
                        tax.is_disqualified || tax.primary_identity || tax.characteristic
                    );
                    if (usable) {
                        assignedRows.push(makeJoinedContactRow(runId, c, tax));
                        phase1aJoined++;
                    } else {
                        stillPending.push(c);
                    }
                }
                pending = stillPending;
            }

            // Library match (per chunk).
            if (libraryBuckets.length > 0 && pending.length > 0) {
                await ctx.checkCancel();
                const libRes = await runContactLibraryFirstMatch(pending, libraryBuckets, sectorVocab, runId);
                totalCost += libRes.costUsd;
                assignedRows.push(...libRes.autoAssigned);
                libraryMatched += libRes.autoAssigned.length;
                pending = libRes.pending;
            }

            // Embedding pre-filter (per chunk). Re-embeds leaves each
            // chunk — small constant cost vs the savings from streaming.
            if (EMBED_PREFILTER_ENABLED && pending.length > 0 && leaves.length > 0) {
                await ctx.checkCancel();
                const embedRes = await runContactEmbeddingPrefilter(pending, leaves, sectorVocab, runId);
                totalCost += embedRes.costUsd;
                assignedRows.push(...embedRes.autoAssigned);
                embeddingMatched += embedRes.autoAssigned.length;
                pending = embedRes.pending;
            }

            // LLM routing (per chunk). Skipped when leaves=[] because
            // the LLM has no candidate set to choose from — those
            // contacts fall through to the rollup which routes them to
            // General with reason=LOW_VOLUME.
            if (pending.length > 0 && leaves.length > 0) {
                await ctx.checkCancel();
                const llmRes = await runContactMatchingLLM(pending, leaves, sectorVocab, runId, ctx);
                totalCost += llmRes.costUsd;
                assignedRows.push(...llmRes.rows);
                llmRouted += llmRes.rows.length;
            } else if (pending.length > 0) {
                // No buckets defined — synthesize General rows per the
                // user's "if you can't decide, send to General" rule.
                for (const c of pending) {
                    const row = makeGeneralContactRow(runId, c, REASON.NO_BUCKETS_DEFINED);
                    row.bucket_reason = 'No buckets defined for this run — defaulted to General';
                    assignedRows.push(row);
                }
            }

            ctx.log(
                `[Bucketing ${runId}] chunk ${chunkNum}: ` +
                `loaded ${chunk.rows.length} (${chunk.pageMs}ms+${chunk.enrichmentMs}ms enrich), ` +
                `total assignedRows=${assignedRows.length.toLocaleString()}`
            );
            ctx.progress({
                phase: 'phase1b',
                step: 'streaming_route',
                current: totalLoaded,
                note: `Routed ${assignedRows.length.toLocaleString()} contacts (chunk ${chunkNum}, phase1a=${phase1aJoined}, library=${libraryMatched}, embed=${embeddingMatched}, llm=${llmRouted}, dq=${totalDqEarly})`
            });

            if (!chunk.hasMore) break;
        }
    });

    ctx.log(
        `[Bucketing ${runId}] streaming complete — ${totalLoaded.toLocaleString()} loaded, ` +
        `${totalUsable.toLocaleString()} usable, ${totalDqEarly.toLocaleString()} early-DQ; ` +
        `phase1a=${phase1aJoined}, library=${libraryMatched}, embed=${embeddingMatched}, llm=${llmRouted}, $${totalCost.toFixed(4)}`,
        'phase'
    );
    ctx.progress({
        phase: 'phase1b',
        step: 'streaming_route_done',
        current: totalLoaded,
        total: totalLoaded,
        note: `Streaming complete — ${assignedRows.length.toLocaleString()} contacts in ${chunkNum} chunks, $${totalCost.toFixed(4)}`,
    });

    // Build a `contacts` reference for downstream coverage analytics.
    // We don't need every field anymore — just the ids — but the
    // existing buildCoverageSummary signature wants ContactRouteInput[].
    // Reconstructing skeleton rows from assignedRows keeps memory tight.
    const contacts: ContactRouteInput[] = assignedRows.map(r => ({
        contact_id: r.contact_id,
        company_name: null,
        company_website: null,
        industry: r.industry_string || null,
        lead_list_name: null,
        enrichment_status: r.is_disqualified ? 'failed' : 'completed',
        classification: r.industry_string || null,
        confidence: null,
        reasoning: null,
        error_message: null,
    }));

    await ctx.checkCancel();
    ctx.log(`[Bucketing ${runId}] step 5/5: contact-level volume rollup + write`, 'phase');
    ctx.progress({ phase: 'phase1b', step: 'rollup_write', note: 'Computing contact-level campaign buckets…' });

    // Pull per-industry per-tag confidences from Phase 1a's bucket_industry_map
    // so the rollup can gate which layer each row may enter at. Phase 1b
    // matchers (library, embedding, LLM) currently only return one overall
    // score; here we backfill the per-tag scores from the source-of-truth.
    const industryStrings = Array.from(new Set(assignedRows.map(r => r.industry_string).filter(Boolean)));
    const confByIndustry = new Map<string, { id?: number; ch?: number; sec?: number }>();
    if (industryStrings.length > 0) {
        // Chunked fetch — `.in()` blows up on >1000 long strings.
        for (let i = 0; i < industryStrings.length; i += 200) {
            const slice = industryStrings.slice(i, i + 200);
            const { data: confRows } = await supabase
                .from('bucket_industry_map')
                .select('industry_string,identity_confidence,characteristic_confidence,sector_confidence')
                .eq('bucketing_run_id', runId)
                .in('industry_string', slice);
            for (const cr of (confRows || []) as any[]) {
                confByIndustry.set(cr.industry_string, {
                    id: typeof cr.identity_confidence === 'number' ? cr.identity_confidence : undefined,
                    ch: typeof cr.characteristic_confidence === 'number' ? cr.characteristic_confidence : undefined,
                    sec: typeof cr.sector_confidence === 'number' ? cr.sector_confidence : undefined
                });
            }
        }
    }
    for (const r of assignedRows) {
        const c = confByIndustry.get(r.industry_string || '');
        if (c) {
            if (typeof c.id === 'number' && (r.identity_confidence == null || r.identity_confidence === 0)) r.identity_confidence = c.id;
            if (typeof c.ch === 'number' && (r.characteristic_confidence == null || r.characteristic_confidence === 0)) r.characteristic_confidence = c.ch;
            if (typeof c.sec === 'number' && (r.sector_confidence == null || r.sector_confidence === 0)) r.sector_confidence = c.sec;
        }
    }

    let rolledRows = await timeStep('rollup', async () =>
        computeContactRollup(assignedRows, Number(run.min_volume || 0), Number(run.bucket_budget || 30))
    );

    // Generic Audit — always runs after rollup. Pattern-recovers rows
    // from General by re-routing groups of ≥ min_volume/4 rows up the
    // chain to characteristic or identity buckets that already exist.
    const auditRes = await timeStep('generic_audit', async () =>
        runGenericAudit(rolledRows, Number(run.min_volume || 0))
    );
    rolledRows = auditRes.rows;
    ctx.log(`[Bucketing ${runId}] Generic Audit: reclaimed ${auditRes.reclaimed} rows from General into ${auditRes.targets.length} target bucket(s)${auditRes.targets.length > 0 ? ` (${auditRes.targets.slice(0, 3).map(t => `${t.bucket}+${t.count}`).join(', ')}${auditRes.targets.length > 3 ? '…' : ''})` : ''}`, 'phase');

    await timeStep('write_contact_map_and_assignments', () =>
        writeContactMapAndAssignments(supabase, runId, rolledRows)
    );

    const assignedCount = rolledRows.length;
    const coverageSummary = buildCoverageSummary(contacts, rolledRows);
    const qualityWarnings = buildQualityWarnings(coverageSummary, rolledRows);

    await supabase.from('bucketing_runs').update({
        status: 'completed',
        assigned_contacts: assignedCount,
        cost_usd: totalCost,
        coverage_summary: coverageSummary,
        quality_warnings: qualityWarnings,
        generic_audit: {
            reclaimed: auditRes.reclaimed,
            targets: auditRes.targets,
            ran_at: new Date().toISOString()
        },
        assignment_completed_at: new Date().toISOString()
    }).eq('id', runId);

    for (const warning of qualityWarnings) ctx.log(`[Bucketing ${runId}] warning: ${warning}`, 'warn');

    // Final summary: every step's elapsed seconds in one line so a future
    // slow run is debuggable from the log alone.
    const totalRunMs = Date.now() - runStart;
    const sortedTimings = [...timings].sort((a, b) => b.ms - a.ms);
    ctx.log(
        `[Bucketing ${runId}] step timings (slowest first): ` +
        sortedTimings.map(t => `${t.step}=${(t.ms / 1000).toFixed(1)}s`).join(', '),
        'phase'
    );
    ctx.log(`[Bucketing ${runId}] DONE — ${assignedCount.toLocaleString()} contacts assigned in ${(totalRunMs / 1000).toFixed(1)}s, total cost $${totalCost.toFixed(4)}`, 'phase');
    ctx.progress({ phase: 'phase1b', step: 'done', current: assignedCount, total: contacts.length, note: `Assigned ${assignedCount.toLocaleString()} contacts in ${(totalRunMs / 1000).toFixed(1)}s — total cost $${totalCost.toFixed(4)}` });
}

function getUnclassifiableReason(contact: ContactRouteInput): ReasonCode | null {
    const label = (contact.classification || contact.industry || '').trim().toLowerCase();
    if (contact.enrichment_status !== 'completed') return REASON.FAILED_ENRICHMENT;
    if (!label) return REASON.MISSING_INDUSTRY;
    if (['site error', 'scrape error', 'unknown', 'error', 'n/a', 'na', 'none'].includes(label)) {
        return REASON.SCRAPE_SITE_UNKNOWN;
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

function preRollupName(row: Pick<ContactMapRow, 'sector' | 'characteristic' | 'primary_identity' | 'is_generic' | 'is_disqualified'>): string {
    if (row.is_disqualified) return RESERVED_DISQUALIFIED;
    if (row.is_generic) return RESERVED_GENERAL;
    if (row.characteristic && row.sector && row.sector !== 'Multi-industry') {
        return `${row.sector} ${row.characteristic}`;
    }
    if (row.characteristic) return row.characteristic;
    if (row.primary_identity) return row.primary_identity;
    return RESERVED_GENERAL;
}

// Pre-loads Phase 1a's per-industry-string taxonomy into memory. Used
// by Phase 1b's JOIN-first step so the LLM is skipped for any contact
// whose enriched classification text was already analysed in 1a — the
// expected fast path, since Phase 1a's vocab IS the deduped set of
// classification strings appearing in the selected lists.
async function fetchPhase1aTaxonomyMap(
    supabase: SupabaseClient,
    runId: string,
    ctx: BucketingCtx
): Promise<Map<string, any>> {
    const map = new Map<string, any>();
    const { data, error } = await supabase
        .from('bucket_industry_map')
        .select('industry_string,primary_identity,characteristic,sector,confidence,identity_confidence,characteristic_confidence,sector_confidence,is_generic,is_disqualified,llm_reason,source')
        .eq('bucketing_run_id', runId)
        .range(0, 49999);
    if (error) {
        ctx.log(`[Bucketing ${runId}] failed to load Phase 1a taxonomy map: ${error.message}`, 'error');
        return map;
    }
    for (const row of (data || []) as any[]) {
        if (row.industry_string) map.set(row.industry_string, row);
    }
    return map;
}

// Mirror of makeMatchedContactRow, but copies Phase 1a's per-string tag
// straight onto the contact with source='phase1a_taxonomy'. Disqualified
// rows in 1a become DQ contacts here without re-querying the LLM.
function makeJoinedContactRow(
    runId: string,
    contact: ContactRouteInput,
    tax: any
): ContactMapRow {
    return makeMatchedContactRow(runId, contact, {
        primary_identity: tax.primary_identity || '',
        characteristic: tax.characteristic || '',
        sector: tax.sector || '',
        source: 'phase1a_taxonomy',
        confidence: Number(tax.confidence) || 0,
        identity_confidence: Number(tax.identity_confidence) || 0,
        characteristic_confidence: Number(tax.characteristic_confidence) || 0,
        sector_confidence: Number(tax.sector_confidence) || 0,
        leaf_score: Number(tax.confidence) || 0,
        ancestor_score: Number(tax.identity_confidence) || 0,
        root_score: Number(tax.identity_confidence) || 0,
        is_generic: !!tax.is_generic,
        is_disqualified: !!tax.is_disqualified,
        general_reason: tax.is_disqualified ? REASON.DISQUALIFIED_BY_LLM : null,
        reasons: { phase1a_source: tax.source, llm_reason: tax.llm_reason || null }
    });
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
        characteristic: '',
        sector: '',
        canonical_classification: 'Disqualified',
        bucket_reason: `Unclassifiable: ${reason}`,
        pre_rollup_bucket_name: RESERVED_DISQUALIFIED,
        bucket_name: RESERVED_DISQUALIFIED,
        identity_confidence: 0,
        characteristic_confidence: 0,
        sector_confidence: 0,
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
        characteristic: string;
        sector: string;
        source: string;
        confidence: number;
        identity_confidence?: number;
        characteristic_confidence?: number;
        sector_confidence?: number;
        leaf_score: number;
        ancestor_score: number;
        root_score: number;
        is_generic?: boolean;
        is_disqualified?: boolean;
        general_reason?: string | null;
        reasons?: Record<string, any>;
    }
): ContactMapRow {
    const canonical = params.is_disqualified
        ? 'Disqualified'
        : (params.sector && params.characteristic
            ? `${params.sector} ${params.characteristic}`
            : (params.characteristic || params.primary_identity || 'Generic'));

    const row: ContactMapRow = {
        bucketing_run_id: runId,
        contact_id: contact.contact_id,
        industry_string: (contact.classification || contact.industry || '').trim(),
        primary_identity: params.primary_identity,
        characteristic: params.characteristic,
        sector: params.sector,
        canonical_classification: canonical,
        bucket_reason: '',
        pre_rollup_bucket_name: RESERVED_GENERAL,
        bucket_name: RESERVED_GENERAL,
        identity_confidence: cleanScore(params.identity_confidence ?? params.confidence),
        characteristic_confidence: cleanScore(params.characteristic_confidence ?? params.confidence),
        sector_confidence: cleanScore(params.sector_confidence ?? params.confidence),
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
        : row.characteristic && row.sector && row.sector !== 'Multi-industry'
            ? 'combo'
            : row.characteristic
                ? 'characteristic'
                : 'identity';
    return row;
}

// Per-tag confidence floor below which we don't trust a tag to drive
// routing. The tagger returns 1-10, persisted as 0-1 — so 0.4 = "<4/10".
// Lowered from 0.6 → 0.4 because the tagger's 5/10 confidence is "this is the
// closest available option in the taxonomy", not "I'm wrong" — so we use
// the tag and let the cascade roll up to the right level.
const TAG_CONF_FLOOR = 0.4;

// Build the "effective" routing view for a row by masking tags whose
// per-tag confidence is below the floor. The original row keeps its
// data for export / display; only the effective copy is used for
// rollup counting and decisions.
//
// Cascade rule: when identity_confidence is low, we KEEP the identity but
// mask spec / sector — the rollup will then try the identity-only bucket.
// Only when primary_identity is genuinely empty does the row fall through
// to General with REASON.LOW_VOLUME.
function effectiveTags(row: ContactMapRow): {
    primary_identity: string;
    characteristic: string;
    sector: string;
    masked: string[];   // names of layers we suppressed, for bucket_reason
} {
    const idLow = row.identity_confidence != null && row.identity_confidence < TAG_CONF_FLOOR;
    const chLow = row.characteristic_confidence != null && row.characteristic_confidence < TAG_CONF_FLOOR;
    const secLow = row.sector_confidence != null && row.sector_confidence < TAG_CONF_FLOOR;
    const masked: string[] = [];
    let ch = row.characteristic || '';
    let sec = row.sector || '';
    if (idLow) {
        // Identity itself is shaky — drop everything below it so the
        // cascade only attempts the identity-only bucket.
        if (ch) masked.push('characteristic');
        if (sec) masked.push('sector');
        masked.push('identity_low');
        ch = '';
        sec = '';
    } else {
        if (chLow) {
            if (ch) masked.push('characteristic');
            ch = '';
        }
        if (secLow) {
            if (sec) masked.push('sector');
            sec = '';
        }
    }
    return {
        primary_identity: row.primary_identity || '',
        characteristic: ch,
        sector: sec,
        masked
    };
}

function computeContactRollup(rows: ContactMapRow[], minVolume: number, bucketBudget: number): ContactMapRow[] {
    // Per-row effective view, computed once.
    const effective = rows.map(r => effectiveTags(r));

    // 3-level rollup cascade: combo (sector + characteristic) → characteristic
    // → identity → General. The two intermediate "core" layers (functional_
    // core, sector_core) were dropped in v5 — they were rollup-only fallback
    // buckets that added complexity without sharpening segmentation.
    const comboCounts = new Map<string, number>();
    const characteristicCounts = new Map<string, number>();
    const identityCounts = new Map<string, number>();

    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        if (row.is_generic || row.is_disqualified) continue;
        const eff = effective[i];
        if (eff.characteristic) {
            characteristicCounts.set(eff.characteristic, (characteristicCounts.get(eff.characteristic) || 0) + 1);
            if (eff.sector && eff.sector !== 'Multi-industry') {
                const combo = `${eff.sector} ${eff.characteristic}`;
                comboCounts.set(combo, (comboCounts.get(combo) || 0) + 1);
            }
        }
        if (eff.primary_identity) {
            identityCounts.set(eff.primary_identity, (identityCounts.get(eff.primary_identity) || 0) + 1);
        }
    }

    const rolled = rows.map((row, i) => {
        const next = { ...row };
        next.pre_rollup_bucket_name = preRollupName(next);
        const eff = effective[i];
        const maskedSuffix = eff.masked.length > 0
            ? ` (low-confidence ${eff.masked.join(' + ')} masked)` : '';

        if (next.is_disqualified) {
            next.bucket_name = RESERVED_DISQUALIFIED;
            next.rollup_level = 'general';
            next.general_reason = next.general_reason || REASON.DISQUALIFIED_BY_LLM;
            next.bucket_reason = next.bucket_reason || 'Disqualified — flagged with high identity confidence';
        } else if (next.is_generic && !eff.primary_identity) {
            next.bucket_name = RESERVED_GENERAL;
            next.rollup_level = 'general';
            next.general_reason = next.general_reason || REASON.LOW_OVERALL_CONFIDENCE;
            next.bucket_reason = next.bucket_reason || 'Inviteable but insufficient classification evidence';
        } else if (eff.characteristic && eff.sector && eff.sector !== 'Multi-industry'
            && (comboCounts.get(`${eff.sector} ${eff.characteristic}`) || 0) >= minVolume) {
            next.bucket_name = `${eff.sector} ${eff.characteristic}`;
            next.rollup_level = 'combo';
            next.general_reason = REASON.COMBO_THRESHOLD_MET;
            next.bucket_reason = `Sector + characteristic combo cleared ${minVolume}-lead threshold${maskedSuffix}`;
        } else if (eff.characteristic && (characteristicCounts.get(eff.characteristic) || 0) >= minVolume) {
            next.bucket_name = eff.characteristic;
            next.rollup_level = 'characteristic';
            next.general_reason = REASON.SPECIALIZATION_THRESHOLD_MET;
            next.bucket_reason = `Characteristic cleared threshold${maskedSuffix}`;
        } else if (eff.primary_identity && (identityCounts.get(eff.primary_identity) || 0) >= minVolume) {
            next.bucket_name = eff.primary_identity;
            next.rollup_level = 'identity';
            next.general_reason = REASON.IDENTITY_THRESHOLD_MET;
            next.bucket_reason = `Rolled up to primary_identity; characteristic too small${maskedSuffix}`;
        } else {
            next.bucket_name = RESERVED_GENERAL;
            next.rollup_level = 'general';
            next.general_reason = REASON.LOW_VOLUME;
            next.bucket_reason = `No layer cleared the ${minVolume}-lead threshold${maskedSuffix}`;
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
            // 3-level demotion: combo → characteristic → identity → General
            if (row.rollup_level === 'combo' && row.characteristic) {
                row.bucket_name = row.characteristic;
                row.rollup_level = 'characteristic';
            } else if (row.rollup_level === 'characteristic' && row.primary_identity) {
                row.bucket_name = row.primary_identity;
                row.rollup_level = 'identity';
            } else {
                row.bucket_name = RESERVED_GENERAL;
                row.rollup_level = 'general';
                row.general_reason = REASON.BUDGET_ROLLUP;
                row.bucket_reason = row.bucket_reason || 'Bucket budget exceeded — rolled up to General';
            }
        }
    }
    return rolled;
}

// Generic Audit — runs after computeContactRollup. Looks at rows currently
// in the General bucket, groups them by (combo OR characteristic OR
// primary_identity), and re-routes groups of ≥ floor(minVolume/4) rows to a
// matching live bucket if one exists in the run. Recovers volume that the
// rollup left in General because the narrowest (combo) layer was too small
// but a broader layer (characteristic, identity) already had a bucket.
function runGenericAudit(rows: ContactMapRow[], minVolume: number): {
    rows: ContactMapRow[];
    reclaimed: number;
    targets: { bucket: string; count: number; from: string }[];
} {
    const liveBuckets = new Set<string>();
    for (const r of rows) {
        if (r.bucket_name && r.bucket_name !== RESERVED_GENERAL && r.bucket_name !== RESERVED_DISQUALIFIED) {
            liveBuckets.add(r.bucket_name);
        }
    }

    // Threshold for a recovery group — quarter of min_volume but at least 50.
    const groupFloor = Math.max(50, Math.floor((minVolume || 0) / 4));

    // Group General rows by target bucket candidate. Try three candidates
    // in priority order (combo → characteristic → identity); first match
    // against a live bucket wins. This mirrors the simplified 3-level
    // cascade in computeContactRollup.
    const groups = new Map<string, { rows: ContactMapRow[]; from: string }>();
    for (const r of rows) {
        if (r.bucket_name !== RESERVED_GENERAL) continue;
        if (r.is_disqualified) continue;
        const candidates: { name: string; from: string }[] = [];
        if (r.sector && r.characteristic && r.sector !== 'Multi-industry') {
            candidates.push({ name: `${r.sector} ${r.characteristic}`, from: 'combo' });
        }
        if (r.characteristic) {
            candidates.push({ name: r.characteristic, from: 'characteristic' });
        }
        if (r.primary_identity) {
            candidates.push({ name: r.primary_identity, from: 'identity' });
        }
        const target = candidates.find(c => liveBuckets.has(c.name));
        if (!target) continue;
        const g = groups.get(target.name) || { rows: [], from: target.from };
        g.rows.push(r);
        groups.set(target.name, g);
    }

    let reclaimed = 0;
    const targets: { bucket: string; count: number; from: string }[] = [];
    for (const [bucketName, g] of groups.entries()) {
        if (g.rows.length < groupFloor) continue;
        for (const r of g.rows) {
            r.bucket_name = bucketName;
            r.rollup_level = g.from === 'identity' ? 'identity'
                : g.from === 'combo' ? 'combo'
                : 'characteristic';
            r.general_reason = REASON.GENERIC_AUDIT_RECLAIMED;
            r.bucket_reason = `Generic Audit: re-routed from General to ${bucketName} (matched on ${g.from})`;
        }
        reclaimed += g.rows.length;
        targets.push({ bucket: bucketName, count: g.rows.length, from: g.from });
    }

    targets.sort((a, b) => b.count - a.count);
    return { rows, reclaimed, targets };
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
        characteristic: row.characteristic || null,
        sector: row.sector || null,
        canonical_classification: row.canonical_classification || null,
        bucket_reason: row.bucket_reason || null,
        pre_rollup_bucket_name: row.pre_rollup_bucket_name,
        bucket_name: row.bucket_name,
        rollup_level: row.rollup_level,
        source: row.source,
        confidence: row.confidence,
        identity_confidence: row.identity_confidence ?? null,
        characteristic_confidence: row.characteristic_confidence ?? null,
        sector_confidence: row.sector_confidence ?? null,
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
        identity_confidence: row.identity_confidence ?? null,
        characteristic_confidence: row.characteristic_confidence ?? null,
        sector_confidence: row.sector_confidence ?? null,
        bucket_leaf: row.characteristic || null,
        bucket_ancestor: row.primary_identity || null,
        bucket_root: row.primary_identity || null,
        primary_identity: row.primary_identity || null,
        characteristic: row.characteristic || null,
        sector: row.sector || null,
        canonical_classification: row.canonical_classification || null,
        bucket_reason: row.bucket_reason || null,
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

// ─── sector extraction (deterministic) ───────────────────────
//
// Used by the embedding prefilter and library-first match to populate
// sector from the raw industry string when no LLM has run on it.
// Without this, embedding-matched rows would all have sector=''
// and combo buckets ("Real Estate SEO Agency") could never form.
//
// Strategy: case-insensitive substring match against sector_vocabulary,
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
// characteristic, with sector extracted deterministically.
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
        return `${l.characteristic} (under ${l.primary_identity}): ${l.description || ''}. Identity: ${l.identity_type}. Strong signals: ${sig}. Include: ${inc}. Examples: ${examples}.`;
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
                bucket_name: leaf.characteristic,
                source: 'preview_embedding',
                confidence: Number(top.s.toFixed(2)),
                bucket_leaf: leaf.characteristic,
                bucket_ancestor: leaf.primary_identity,
                bucket_root: leaf.primary_identity,
                primary_identity: leaf.primary_identity,
                characteristic: leaf.characteristic,
                sector: sector,
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
        const spec = b.characteristic || b.bucket_name || '';
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
            const spec = lib.characteristic || lib.bucket_name || '';
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
                characteristic: spec,
                sector: sector,
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
    // characteristic, not the vertical the company serves.
    const leafTexts = leaves.map(l => {
        const sig = (l.strong_identity_signals || []).slice(0, 6).join(', ');
        const inc = (l.include || []).slice(0, 6).join(', ');
        return `${l.characteristic} (under ${l.primary_identity}): ${l.description || ''}. Identity: ${l.identity_type}. Strong signals: ${sig}. Include: ${inc}.`;
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
                bucket_name: leaf.characteristic, // pre-rollup placeholder
                source: 'embedding',
                confidence: Number(top.s.toFixed(2)),
                bucket_leaf: leaf.characteristic,
                bucket_ancestor: leaf.primary_identity,
                bucket_root: leaf.primary_identity,
                primary_identity: leaf.primary_identity,
                characteristic: leaf.characteristic,
                sector: sector,
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
        const spec = b.characteristic || b.bucket_name || '';
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
            const spec = lib.characteristic || lib.bucket_name || '';
            const ident = lib.primary_identity || lib.direct_ancestor || '';
            autoAssigned.push(makeMatchedContactRow(runId, contacts[i], {
                primary_identity: ident,
                characteristic: spec,
                sector: extractSectorFocus(contactRoutingText(contacts[i]), sectorVocab),
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
        return `${l.characteristic} (under ${l.primary_identity}): ${l.description || ''}. Identity: ${l.identity_type}. Strong signals: ${sig}. Include: ${inc}. Examples: ${examples}.`;
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
                characteristic: leaf.characteristic,
                sector: extractSectorFocus(contactRoutingText(contacts[i]), sectorVocab),
                source: 'embedding_high_confidence',
                confidence: top.s,
                leaf_score: top.s,
                ancestor_score: top.s,
                root_score: top.s,
                reasons: { auto: 'contact embedding pre-filter', cosine: top.s, identity_type: leaf.identity_type }
            }));
        } else {
            const embedding_candidates = sorted.slice(0, 8).map(({ s, j }) => ({
                characteristic: leaves[j].characteristic,
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

    const validSpecNames = new Set(leaves.map(l => l.characteristic));
    const validIdentityNames = new Set(leaves.map(l => l.primary_identity));
    const identityBySpec = new Map(leaves.map(l => [l.characteristic, l.primary_identity]));
    const refByIdentity: Record<string, any[]> = {};
    for (const l of leaves) {
        const ident = l.primary_identity;
        if (!refByIdentity[ident]) refByIdentity[ident] = [];
        refByIdentity[ident].push({
            characteristic: l.characteristic,
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
            const specOk = !!r.characteristic.name
                && validSpecNames.has(r.characteristic.name)
                && r.characteristic.score >= 0.40;
            const specName = specOk ? r.characteristic.name : '';
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
                characteristic: isDisqualified ? '' : specName,
                sector: isDisqualified ? '' : (r.sector || '').trim(),
                source: 'llm_phase1b',
                confidence: specName ? r.characteristic.score : r.primary_identity.score,
                leaf_score: r.characteristic.score,
                ancestor_score: r.primary_identity.score,
                root_score: r.primary_identity.score,
                is_generic: isGeneric,
                is_disqualified: isDisqualified,
                general_reason: isDisqualified ? 'disqualified' : (isGeneric ? 'generic_low_confidence' : null),
                reasons: {
                    spec: r.characteristic.reason,
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
- characteristic: Layer 2, must be listed under that identity
- sector: Layer 3, must be from SECTOR_VOCABULARY, "Multi-industry", or ""

General is a last resort. If a primary_identity is a reasonable fit, return it
even when no characteristic is precise. Only leave both names blank when the
business is truly unclear, bad data, clear non-ICP, or confidence is below 0.40.

Each contact may include embedding_candidate_buckets. Treat these as a shortlist,
not a decision. Prefer them when the text supports them, but choose another
reference bucket if the contact evidence is clearly better.

Rules:
- Strong business-model nouns beat sector nouns.
- "Serving X" does not mean "is X"; X belongs in sector.
- Operator characteristics require explicit operator evidence.
- Disqualify only clear ecommerce/DTC physical, local geo-tied services,
  brick-and-mortar retail, or low-ticket consumer.
- SERVICES vs PRODUCTS — never tag a company that rents/maintains/repairs/
  operates-as-a-service as "B2B Product Manufacturer" or any product
  manufacturer characteristic. If no services-side characteristic fits,
  prefer the identity-only fallback (return primary_identity, leave
  characteristic "") rather than picking a wrong product label.
- Talent / artist / sports management firms are Agency, not Consulting.
- Game / software studios are Software & SaaS, not Hospitality / Travel /
  Consumer Retail — only DQ if unambiguously pure-consumer, no B2B angle.
- Scores are alignment scores, not probabilities. Use >=0.40 for a reasonable
  identity/characteristic fit, >=0.70 for strong fit.
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
  "characteristic": {"name": "<spec under that identity, or empty>", "score": 0.00, "reason": ""},
  "sector": "<from SECTOR_VOCABULARY, 'Multi-industry', or ''>",
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
                    required: ['primary_identity', 'characteristic',
                               'sector', 'identity_type', 'generic', 'disqualified'],
                    properties: {
                        primary_identity: chainItemSchema(),
                        characteristic: chainItemSchema(),
                        sector: { type: 'string' },
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
        (a.characteristic.name && !validSpecNames.has(a.characteristic.name)) ||
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
    const validSpecNames = new Set(leaves.map(l => l.characteristic));
    const validIdentityNames = new Set(leaves.map(l => l.primary_identity));
    const identityBySpec = new Map(leaves.map(l => [l.characteristic, l.primary_identity]));

    // Bucket reference is the cacheable prompt prefix. Group by primary_identity
    // so the model sees the hierarchy clearly.
    const refByIdentity: Record<string, any[]> = {};
    for (const l of leaves) {
        const ident = l.primary_identity;
        if (!refByIdentity[ident]) refByIdentity[ident] = [];
        refByIdentity[ident].push({
            characteristic: l.characteristic,
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
            const specOk = r.characteristic.name
                && validSpecNames.has(r.characteristic.name)
                && r.characteristic.score >= 0.55;
            const specName = specOk ? r.characteristic.name : '';
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
                confidence: Number((r.characteristic.score || 0).toFixed(2)),
                bucket_leaf: specName,
                bucket_ancestor: identName,
                bucket_root: identName,
                primary_identity: identName,
                characteristic: specName,
                sector: (r.sector || '').trim(),
                leaf_score: r.characteristic.score,
                ancestor_score: r.primary_identity.score,
                root_score: r.primary_identity.score,
                is_generic: !!r.generic && !specOk,
                is_disqualified: !!r.disqualified,
                reasons: {
                    spec: r.characteristic.reason,
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
        characteristic: { name: '', score: 0, reason: 'fallback' },
        sector: '',
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
  - characteristic (Layer 2, MUST be one of the characteristic
                          names listed UNDER that identity in BUCKET_REFERENCE)
  - sector          (Layer 3, optional — from SECTOR_VOCABULARY only)

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

4) If neither identity nor characteristic fits at >= 0.55 confidence,
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

Rule 3 — Operator characteristics (operator_required=true) require
  EXPLICIT operator evidence: "clinic", "hospital", "school district",
  "university", "city government", "church", "property management
  company", "factory". Generic sector mentions are NOT evidence.

Rule 4 — When BOTH identity and sector appear, fill BOTH fields.
  Example: "Healthcare private equity firm" →
    primary_identity = "Financial Services",
    characteristic = "Private Equity Firm",
    sector = "Healthcare".

EXPLICIT EXAMPLES — CORRECT:
  • "Healthcare private equity investment firm" →
      primary_identity = Financial Services
      characteristic = Private Equity Firm
      sector = Healthcare
  • "Government IT consulting firm" →
      primary_identity = Consulting & Advisory  (or IT Services depending
        on which identity contains "IT Consulting" in BUCKET_REFERENCE)
      characteristic = IT Consulting
      sector = Government
  • "Marketing agency for dental practices" →
      primary_identity = Agency
      characteristic = Performance Marketing Agency  (or
        Branding Agency, etc — pick the closest spec from BUCKET_REFERENCE)
      sector = Healthcare
  • "Real estate software for hospitals" →
      primary_identity = Software & SaaS
      characteristic = PropTech SaaS  (or Vertical SaaS)
      sector = Real Estate     ← yes, real estate is the OWN model
  • "Medical clinic" →
      primary_identity = Healthcare Operator
      characteristic = Medical Clinic / Hospital
      sector = ""

EXPLICIT EXAMPLES — INCORRECT:
  • "Healthcare private equity investment firm" → primary_identity = Healthcare ❌
  • "Government IT consulting firm" → primary_identity = Government ❌
  • "Marketing agency for dental practices" → primary_identity = Healthcare ❌
  • "Real estate software for hospitals" → primary_identity = Healthcare ❌
  • "Software for schools" → primary_identity = Education ❌

PRESSURE TEST: "If outreach were written for this primary_identity +
characteristic, would the recipient say 'yes that's me' or 'no that's
my client'?" If 'my client' → routing is wrong.

OUTPUT CONSTRAINTS:
- primary_identity.name MUST be one of the identity keys in BUCKET_REFERENCE,
  or "" if generic / disqualified.
- characteristic.name MUST be a spec listed under that exact
  identity, or "" if generic / disqualified.
- characteristic.score must be <= primary_identity.score.
- sector MUST be from SECTOR_VOCABULARY, "Multi-industry", or "".
  NEVER put an identity noun in sector.
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
  "characteristic": {"name": "<spec under that identity, or empty>", "score": 0.00, "reason": ""},
  "sector": "<from SECTOR_VOCABULARY, 'Multi-industry', or ''>",
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
                    required: ['primary_identity', 'characteristic',
                               'sector', 'identity_type', 'generic', 'disqualified'],
                    properties: {
                        primary_identity: chainItemSchema(),
                        characteristic: chainItemSchema(),
                        sector: { type: 'string' },
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
        (a.characteristic.name && !validSpecNames.has(a.characteristic.name)) ||
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
