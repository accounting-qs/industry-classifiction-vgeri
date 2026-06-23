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
 * string and assigns identity / sub-identity / sector + per-tag confidence.
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
 * Volume rollup: sector+sub-identity → sub-identity → identity → General.
 * Disqualified / invalid rows land in General with audit reasons.
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import Anthropic from '@anthropic-ai/sdk';
import { getSetting } from './appSettings';
import { checkBucketingSchema } from './bucketingSchemaCheck';
import {
    HARD_KEYWORD_ROUTING,
    CORE_PRINCIPLES,
    DISQUALIFICATION_RULES,
    EXACT_SPELLING_RULE,
    renderLibraryMenu,
    renderLibraryReference
} from './bucketingPromptParts';

// ─── HARD-CODED MODEL + CONCURRENCY CONFIG ─────────────────────────
// Per user request: no env reliance for non-secret config. Only the
// Anthropic API key is runtime-configurable (via Connectors UI / app_settings).
const TAXONOMY_MODEL_ANTHROPIC = 'claude-sonnet-4-6';
const TAXONOMY_MODEL_OPENAI = 'gpt-4.1';
// Phase 1b matching model. `gpt-4.1-mini` is ~3× cheaper than
// `claude-haiku-4-5` and accuracy is equivalent on our 30-case golden set,
// so we default to mini. classifyContactBatch dispatches on the model prefix
// (`gpt-*` → OpenAI fetch with strict json_schema; `claude-*` → Anthropic SDK
// with lenient parser + drift-retry) so the constant can be flipped without
// touching the call path. Override at eval time via the EVAL_MATCH_MODEL env.
const MATCH_MODEL = process.env.EVAL_MATCH_MODEL || 'gpt-4.1-mini';
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

// Error sentinel strings the scraper sometimes stores as the literal
// classification when its enrichment "succeeded" with no usable signal
// (e.g. "Site Error", "404", "no content"). When enrichment_status='completed'
// the partition would otherwise pass these to the LLM — wasting tokens on
// rows that carry zero classification signal. ERROR_SENTINEL_REGEX matches
// the whole string (after stripping wrapping quotes/whitespace), so a real
// classification that merely mentions "error" (e.g. "Insurance Brokerage
// Specializing in Errors & Omissions") is NOT filtered. Case-insensitive.
//
// "Unknown" is intentionally left OFF — sometimes it's a legit upstream
// "we don't know" placeholder rather than an error, and the LLM returning
// nulls is acceptable. Add it here if a future run shows it's high-volume
// pure-noise.
const ERROR_SENTINEL_REGEX = /^(?:(?:site|scrape|html|fetch|page|enrichment|crawler?|http)\s*error|error|failed|failure|timeout|blocked|403|404|500|502|503|504|no\s*data|no\s*content|empty|n\/?a|null|none|undefined)$/i;

// Canonical reason codes for routing decisions. Persist these strings on
// bucket_assignments / bucket_contact_map.general_reason so a campaign
// operator can filter "show me everyone who landed in General because the
// volume rollup couldn't find a layer over the threshold" without
// regex-matching free text. The bucket_reason column still gets a
// human-readable sentence; general_reason gets the code.
export const REASON = {
    // Routed into a real campaign bucket — three rollup levels (v5+):
    COMBO_THRESHOLD_MET: 'combo_threshold_met',
    SPECIALIZATION_THRESHOLD_MET: 'sub_identity_threshold_met',
    IDENTITY_THRESHOLD_MET: 'identity_threshold_met',
    // Generic Audit moved row out of General into a live bucket.
    GENERIC_AUDIT_RECLAIMED: 'generic_audit_reclaimed',
    // Routed to General:
    LOW_VOLUME: 'no_layer_cleared_threshold',
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
    // AbortSignal that fires the moment the user clicks Stop (the
    // /cancel endpoint calls .abort() on the per-run AbortController).
    // LLM call sites pass this to fetch() so in-flight requests die in
    // milliseconds instead of waiting up to 90 s for the timeout. The
    // signal is also wired into checkCancel() — if the signal is already
    // aborted, checkCancel throws without a DB round-trip.
    abortSignal: AbortSignal;
}

// Combine a fresh timeout signal with the run's cancel signal so a single
// AbortSignal can be passed to fetch / Anthropic SDK and trigger on
// EITHER condition. AbortSignal.any (Node 20.3+) is preferred; fallback
// hand-combines via a controller for older runtimes.
function combinedAbortSignal(timeoutMs: number, runSignal?: AbortSignal): AbortSignal {
    const timeoutSignal = AbortSignal.timeout(timeoutMs);
    if (!runSignal) return timeoutSignal;
    if (typeof (AbortSignal as any).any === 'function') {
        return (AbortSignal as any).any([timeoutSignal, runSignal]);
    }
    const ac = new AbortController();
    const propagate = (sig: AbortSignal) => {
        if (sig.aborted) { ac.abort((sig as any).reason); return; }
        sig.addEventListener('abort', () => ac.abort((sig as any).reason), { once: true });
    };
    propagate(timeoutSignal);
    propagate(runSignal);
    return ac.signal;
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
const PHASE1A_BATCH_SIZE = 20;
const PHASE1A_CONCURRENCY = 300;

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
    parent_identity?: string | null;     // sub-identities only
    is_disqualified?: boolean;           // identities only
    synonyms?: string | null;            // sectors only
    created_by?: string;
    archived: boolean;
    sort_order?: number | null;
}

interface TaxonomySnapshot {
    identities: TaxonomyEntry[];
    sub_identities: TaxonomyEntry[];
    sectors: TaxonomyEntry[];
}

// ─── AI proposal-routing suggestions ───────────────────────────────
// Populated by suggestProposalRoutings(); persisted on
// bucketing_runs.ai_proposal_suggestions; surfaced through
// /api/bucketing/runs/:id/proposed-tags so the UI can pre-fill
// the "Route to…" dropdown / tick the "accept as new" checkbox.
//
// Sub-identity keying uses "|" (illegal in identity names) to avoid
// collisions with the "::" the UI uses inside routeDraft values.
export interface ProposalSuggestion {
    route_to?: string;            // existing library entry name (verbatim) — the model's top pick
    route_to_parent?: string;     // parent identity of route_to (set for sub-identity targets,
                                  // including cross-layer identity → sub routes)
    // Cross-layer routing target — meaningful only on identity-layer
    // suggestions today, set to 'sub_identities' when the model judged
    // the proposal really belongs to a sub-identity (e.g. "Holding Company"
    // → "Venture Capital" under "Financial Services"). The UI uses this
    // to rewrite BOTH columns atomically. Omitted for same-layer routes.
    route_to_layer?: 'identities' | 'sub_identities';
    accept_as_new?: boolean;      // exactly one of route_to / accept_as_new / wrong_layer is set
    wrong_layer?: boolean;        // proposal's NAME is from a different layer (identity-as-sub,
                                  // sector-as-sub, sub-as-sector). Surfaced to the human; route/
                                  // accept-as-new are both wrong here.
    confidence: number;           // 1-10
    reason: string;               // <= 30 words, single sentence
    // Up to 2 alternative ranked route candidates (excludes the primary
    // route_to above; descending confidence). Populated only when decision
    // = route AND the model genuinely sees multiple plausible library
    // targets. UI renders each as a clickable chip so the user picks one
    // instead of being committed to the top pick.
    alt_routes?: Array<{
        route_to: string;
        route_to_parent?: string;
        route_to_layer?: 'identities' | 'sub_identities';
        confidence: number;
        reason: string;
    }>;
}

export interface ProposalSuggestionsBlob {
    identities?:     Record<string, ProposalSuggestion>;
    sub_identities?: Record<string, ProposalSuggestion>;   // key = `${name}|${parent}`
    sectors?:        Record<string, ProposalSuggestion>;
    _meta?: {
        model: string;
        cost_usd: number;
        generated_at: string;
        counts: { identities: number; sub_identities: number; sectors: number };
    };
}

interface IndustryTagging {
    industry: string;
    identity: string | null;
    is_new_identity: boolean;
    sub_identity: string | null;
    is_new_sub_identity: boolean;
    sector: string | null;
    is_new_sector: boolean;
    is_disqualified: boolean;
    // Per-tag 1-10 scores. Drives smart fallbacks in Phase 1b: a row
    // with identity=8, sub-identity=4 should route at the identity
    // level rather than tumble all the way to General. confidence is
    // the legacy single score (= min of the three) kept for existing
    // QA panel + reporting code.
    identity_confidence: number;
    sub_identity_confidence: number;
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
    sub_identity: string;
    primary_identity: string;
    score: number;
}

// A primary_identity is a Layer-1 high-level business type (Agency,
// Consulting & Advisory, Software & SaaS, …). Layer-2 functional
// sub-identities are nested under it.
interface PrimaryIdentity {
    name: string;
    description: string;
    identity_type: string;       // operator | service_provider | agency | software_vendor | investor | advisor | staffing | distributor | media | other
    operator_required: boolean;
}

// A sub-identity is the Layer-2 subtype within an identity.
// This is the "leaf" the LLM matches industry strings to — but the campaign
// bucket actually used downstream is decided by the rollup (combo > spec >
// identity > Generic).
interface DiscoveredBucket {
    sub_identity: string;
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
    buckets: DiscoveredBucket[];   // Layer-2 sub-identities
}

// Phase 1b returns Layer-1 + Layer-2 + Layer-3 per industry string. Layer-4
// (campaign bucket) is computed by SQL afterwards.
interface MatchChain {
    primary_identity: { name: string; score: number; reason: string };
    sub_identity: { name: string; score: number; reason: string };
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
    sub_identity: string;
    sector: string;
    // Bucket Assignment output (Phase 1a sub-step). When the user has run
    // bucket assignment, this is the LLM's pick from bucket_library (or a
    // proposed new bucket). Phase 1b's rollup prefers these over the
    // synthesized combo→sub-identity→identity name when present.
    assigned_bucket_name: string | null;
    assigned_bucket_primary_identity: string | null;
    canonical_classification: string;
    bucket_reason: string;
    pre_rollup_bucket_name: string;
    bucket_name: string;
    // Per-tag confidence (0-1). Used by computeContactRollup to gate which
    // layer the row may enter at: a row with sector_confidence < 0.6 is
    // ineligible for the combo (sector + sub-identity) layer.
    identity_confidence: number;
    sub_identity_confidence: number;
    sector_confidence: number;
    rollup_level: 'combo' | 'sub_identity' | 'identity' | 'general';
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

// ─── full Phase 1a vocabulary fetch ─────────────────────────────────
// Phase 1a is the source of truth for the exportable taxonomy columns, so
// "tagging complete" must mean every selected contact's classification key
// has a bucket_industry_map row. The older SQL RPC path returned only the
// top 10k distinct labels; on long-tail enriched lists that made the run
// look complete while most contacts had no Phase 1a taxonomy to export.
//
// Build the vocabulary by streaming contacts + enrichments instead. This is
// more round-trips than one aggregate RPC, but it is exact and cannot be
// silently truncated by PostgREST's 1000-row function response cap.
// Ceiling raised 250k→500k (2026-06-23, per user directive). A 473,672-contact /
// 8-list run produced 307,380 distinct (classification, status) rows, tripping the
// original 250k limit — ~42% past the largest run that had ever completed (216k
// distinct, 290k contacts). This is a tripwire against silently marking a *partial*
// taxonomy complete, not a capacity wall: the RPC already transferred 250k full
// vocab objects in ~2s before throwing, and tagging cost/time scale linearly.
// Override per-run via the env var; 500k leaves ~60% headroom over the current
// largest known vocabulary.
const VOCAB_HARD_LIMIT = Number(process.env.BUCKETING_VOCAB_HARD_LIMIT || 500_000);

function vocabularyStatus(contact: ContactRouteInput): { industry: string; status: string } {
    const classification = (contact.classification || '').trim();
    const lower = classification.toLowerCase();
    const industry = classification || 'Scrape Error';
    if (!classification) return { industry, status: 'unenriched' };
    if (lower.startsWith('scrape error') || lower.startsWith('site error')) {
        return { industry, status: 'scrape_error' };
    }
    if (contact.enrichment_status === 'failed') return { industry, status: 'failed' };
    if (contact.enrichment_status === 'completed') return { industry, status: 'completed' };
    return { industry, status: 'pending' };
}

function completedBeats(status: string): boolean {
    return (status || 'completed') === 'completed';
}

async function fetchFullVocabulary(
    supabase: SupabaseClient,
    listNames: string[],
    ctx?: BucketingCtx,
    totalContacts?: number
): Promise<VocabRow[]> {
    return withHeartbeat('vocabulary fetch', async () => {
        await ctx?.checkCancel();
        ctx?.progress({
            phase: 'phase1a',
            step: 'load_vocabulary',
            current: 0,
            total: totalContacts,
            note: `Loading vocabulary from ${listNames.length} list${listNames.length === 1 ? '' : 's'}…`
        });

        // Server-side aggregation via the get_classification_vocabulary RPC.
        // The RPC returns a single JSONB array (not SETOF rows), so the full
        // vocab — typically 100k–250k entries on a real-size run — comes
        // back in one response, bypassing PostgREST's db-max-rows cap that
        // previously silently truncated us to the top 1000 industries.
        // Keys on COALESCE(NULLIF(TRIM(e.classification),''), c.industry),
        // matching what Phase 1b's JOIN looks up downstream.
        const t0 = Date.now();
        const { data, error } = await supabase.rpc('get_classification_vocabulary', {
            p_list_names: listNames,
            // +1 so we can detect overflow without truncating silently.
            p_limit: VOCAB_HARD_LIMIT + 1
        });
        const ms = Date.now() - t0;
        if (error) {
            ctx?.log(`[Bucketing] vocabulary RPC failed after ${ms}ms: ${error.message}`, 'error');
            throw new Error(`vocabulary fetch failed: ${error.message}`);
        }
        const rpcRows = (Array.isArray(data) ? data : []) as Array<{
            classification: string;
            n: number | string;
            enrichment_status: string;
            sample_companies: string[] | null;
        }>;
        if (rpcRows.length > VOCAB_HARD_LIMIT) {
            throw new Error(
                `Phase 1a vocabulary exceeds BUCKETING_VOCAB_HARD_LIMIT=${VOCAB_HARD_LIMIT.toLocaleString()} distinct (classification, status) rows. ` +
                `Increase the limit or split the run; refusing to mark a partial taxonomy as complete.`
            );
        }
        ctx?.log(
            `[Bucketing] vocabulary RPC returned ${rpcRows.length.toLocaleString()} (classification, status) rows in ${ms}ms`
        );

        // The RPC groups by (classification, enrichment_status) so the same
        // string can appear under multiple statuses (e.g. some contacts
        // completed, some failed). Collapse to one VocabRow per
        // classification: sum counts, keep the highest-precedence status,
        // merge up to 3 sample companies.
        const byIndustry = new Map<string, VocabRow>();
        for (const r of rpcRows) {
            const key = r.classification;
            const existing = byIndustry.get(key);
            if (!existing) {
                byIndustry.set(key, {
                    industry: key,
                    n: Number(r.n) || 0,
                    enrichment_status: r.enrichment_status,
                    sample_companies: (r.sample_companies || []).slice(0, 3)
                });
                continue;
            }
            existing.n = Number(existing.n || 0) + (Number(r.n) || 0);
            if (!completedBeats(existing.enrichment_status) && completedBeats(r.enrichment_status)) {
                existing.enrichment_status = r.enrichment_status;
            }
            if ((existing.sample_companies || []).length < 3 && r.sample_companies?.length) {
                const merged = Array.from(new Set([...(existing.sample_companies || []), ...r.sample_companies]));
                existing.sample_companies = merged.slice(0, 3);
            }
        }
        const collapsed = Array.from(byIndustry.values())
            .sort((a, b) => Number(b.n || 0) - Number(a.n || 0));

        ctx?.progress({
            phase: 'phase1a',
            step: 'load_vocabulary',
            current: totalContacts || collapsed.reduce((s, v) => s + Number(v.n || 0), 0),
            total: totalContacts,
            note: `Loaded ${collapsed.length.toLocaleString()} distinct classifications.`
        });
        return collapsed;
    }, ctx);
}

// 1000 (not 2000) so the page size never exceeds Supabase's default
// PostgREST db.maxRows cap of 1000. With pageSize=2000, .range(0, 1999)
// silently returned 1000 rows and hasMore (page.length === pageSize)
// evaluated false → loop exited after the first chunk, capping every run
// at 1k contacts no matter how many were selected.
const CONTACT_PAGE_SIZE = 1000;
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
    // Server-side RPC with statement_timeout=300s. The direct PostgREST
    // path used to die at ~3s on 293K-row runs because service_role's
    // HTTP-side timeout is tighter than the DB statement_timeout we need.
    // See supabase/migrations/20260519_get_contacts_chunk_rpc.sql.
    const { data, error } = await supabase.rpc('get_contacts_chunk', {
        p_list_names: listNames,
        p_last_id: lastId,
        p_limit: pageSize
    });
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
            // Server-side RPC with statement_timeout=300s — same reason as
            // fetchContactsChunk: service_role's HTTP-side timeout is
            // tighter than the DB statement_timeout we need on 200K+ runs.
            const { data, error } = await supabase.rpc('get_contacts_chunk', {
                p_list_names: listNames,
                p_last_id: lastId,
                p_limit: CONTACT_PAGE_SIZE
            });
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
   "IT Services", "Financial Services", "Real Estate",
   "Healthcare Provider", "Education & Training", "Staffing & Recruiting",
   "Legal Services", "Accounting & Tax".

Layer 2 — SUB-IDENTITY (subtype within identity)
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
     IT Services → "Cybersecurity Services", "Custom Software Development",
              "Data Migration Services" (MSPs are the default — leave sub null)

Layer 3 — SECTOR (optional vertical served, ~10-20 total)
   Who do they MAINLY serve, if explicitly stated?
   Examples: "Healthcare", "Real Estate", "Government", "Education",
   "Manufacturing", "Financial Services", "Hospitality", "Energy",
   "Non-profit", "Multi-industry", or "" (none).

CAMPAIGN BUCKET (decided downstream, NOT by you)
   The actual outreach bucket is computed by the rollup engine — not
   predicted. It combines volume across the three axes:
     - "{sector} {sub-identity}" if that combo has enough leads
       (e.g. "Real Estate SEO Agency").
     - Else "{sub-identity}" (e.g. "SEO Agency").
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
primary_identity + sub-identity. Sector served decides
sector.

========================================
OPERATOR vs ENABLER
========================================

Operators directly operate in a vertical: clinics, hospitals, schools,
universities, banks, city governments, churches, property managers,
manufacturing plants, retailers, restaurants. Their primary_identity
NAMES the vertical (e.g. "Healthcare Provider", "Education & Training").

Enablers serve verticals from outside: agencies, consultants, software
firms, investors, staffing firms, IT providers, advisors. Their
primary_identity is the enabler category (Agency, Consulting & Advisory,
Software & SaaS, …) — the vertical they serve goes in sector.

Operator identities (Healthcare Provider, Education & Training, Government
Contractor, Real Estate, Non-Profit & Association) require explicit operator
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
4. Reusability > Novelty. Identities + sub-identities must be reusable.
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

    const totalContacts = await countSelectedContacts(supabase, run.list_names);
    const vocabRows = await fetchFullVocabulary(supabase, run.list_names, ctx, totalContacts);
    ctx.log(`[Bucketing ${runId}] vocabulary: ${vocabRows.length} distinct classification rows over ${totalContacts.toLocaleString()} selected contacts`);

    if (vocabRows.length === 0) {
        await supabase.from('bucketing_runs').update({
            status: 'failed',
            error_message: 'No contacts found for the selected lists.',
            total_contacts: 0
        }).eq('id', runId);
        throw new Error('Empty vocabulary — selected lists have no contacts.');
    }

    // Cross-run tag inheritance DISABLED (per user directive, 2026-06-02).
    //
    // Earlier design called inherit_phase1a_tags here to copy byte-exact
    // industry_string tags from prior successful runs, sparing the LLM
    // cost on already-seen strings. The trade-off was that stale tags
    // (from before recent prompt or library changes) leaked into new runs,
    // and any bad pattern in an early run — meta-text sub names, mistaken
    // identities, pre-cleanup taxonomy values — propagated forever. The
    // explicit policy is now: each run tags from scratch against the current
    // prompt + library; no cross-run state is reused.
    //
    // In-run proposal sharing (within tagIndustries) is unchanged — batches
    // still see proposals coined by earlier batches in the same run via the
    // PROPOSAL APPENDIX. That keeps naming consistent inside a single run
    // without dragging stale tags across runs. The inherit_phase1a_tags
    // RPC stays defined in the DB but is no longer called.
    ctx.log(`[Bucketing ${runId}] cross-run tag inheritance disabled — tagging from scratch against current library + prompt`);

    // Resume support: don't wipe. Read existing rows (including just-inherited
    // ones from the prior step) so we know which industries are already tagged
    // and skip them in the tagger loop. A server restart / OOM mid-tag would
    // otherwise force a full re-tag at ~$20-30 per run. The (run_id,
    // industry_string) PK keeps the upsert idempotent for overlaps.
    const { data: existingRowsData, error: existingErr } = await supabase
        .from('bucket_industry_map')
        .select('industry_string,source')
        .eq('bucketing_run_id', runId);
    if (existingErr) throw new Error(`existing bucket_industry_map read failed: ${existingErr.message}`);
    const existingByIndustry = new Set((existingRowsData || []).map((r: any) => r.industry_string as string));
    const existingLLMTagged = new Set((existingRowsData || [])
        .filter((r: any) => r.source === 'llm_phase1a' || r.source === 'inherited_phase1a')
        .map((r: any) => r.industry_string as string));
    if (existingByIndustry.size > 0) {
        ctx.log(`[Bucketing ${runId}] resume — ${existingByIndustry.size.toLocaleString()} existing rows in map (${existingLLMTagged.size.toLocaleString()} already tagged — LLM or inherited). Skipping those to avoid re-spending.`);
    }

    // ── Partition vocabulary by enrichment status ────────────────────
    // Only `completed` rows go to the LLM. Everything else (scrape_error,
    // unenriched, failed, pending) is a known-bad row and routes straight
    // to the Disqualified bucket without spending a token.
    //
    // Keep the defensive collapse to one row per industry before inserting.
    // The current streaming vocabulary builder already emits one row per
    // classification key, but older/rebuilt callers may still hand us
    // duplicate status rows. A completed row beats anything else because
    // the LLM tagger has the best signal there.
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
    const phase1aCoveredContacts = dedupedVocab.reduce((sum, row) => sum + Number(row.n || 0), 0);

    // Error sentinel strings the scraper sometimes stores as the literal
    // classification (e.g. "Site Error", "404", "no content"). When
    // enrichment_status='completed' these would otherwise be sent to the LLM
    // even though they carry zero signal. Auto-route them to General with the
    // same passthrough path the failed/pending rows use.
    const sentinelMatch = (industry: string | null | undefined): string | null => {
        if (!industry) return 'empty industry';
        const norm = industry.replace(/["'`]+/g, '').trim();
        if (!norm) return 'empty industry';
        return ERROR_SENTINEL_REGEX.test(norm) ? `error sentinel: "${industry}"` : null;
    };

    const completedVocab: VocabRow[] = [];
    const dqVocab: VocabRow[] = [];
    const sentinelReasonByIndustry = new Map<string, string>();
    let sentinelContacts = 0;
    for (const v of dedupedVocab) {
        const status = v.enrichment_status || 'completed';
        if (status !== 'completed') { dqVocab.push(v); continue; }
        const sentinel = sentinelMatch(v.industry);
        if (sentinel) {
            dqVocab.push(v);
            sentinelReasonByIndustry.set(v.industry, sentinel);
            sentinelContacts += Number(v.n || 0);
        } else {
            completedVocab.push(v);
        }
    }
    ctx.log(`[Bucketing ${runId}] partition: ${completedVocab.length} taggable, ${dqVocab.length} → General (data quality: failed/missing/scrape_error${sentinelReasonByIndustry.size > 0 ? ` + ${sentinelReasonByIndustry.size} error sentinels covering ${sentinelContacts.toLocaleString()} contacts` : ''})${vocabRows.length !== dedupedVocab.length ? ` — ${vocabRows.length - dedupedVocab.length} duplicate (industry, status) rows collapsed` : ''}`);
    if (phase1aCoveredContacts !== totalContacts) {
        ctx.log(
            `[Bucketing ${runId}] Phase 1a coverage warning: vocabulary accounts for ${phase1aCoveredContacts.toLocaleString()}/${totalContacts.toLocaleString()} selected contacts`,
            'warn'
        );
    }

    let totalCost = 0;
    const preMapRows: any[] = [];

    // ── General passthrough (failed enrichment / scrape error / no data) ─
    // Disqualified is reserved for LLM-confident "not our ICP" verdicts —
    // contacts we successfully enriched + classified, where the taxonomy
    // tells us they're out of scope. Failed enrichment / scrape errors
    // are a data-quality problem, not a fit problem; they belong in
    // General so the user sees them as "couldn't classify" rather than
    // "we deliberately excluded them".
    for (const v of dqVocab) {
        // Resume: skip DQ-passthrough rows already inserted.
        if (existingByIndustry.has(v.industry)) continue;
        const sentinel = sentinelReasonByIndustry.get(v.industry);
        const reason = sentinel
            ? sentinel
            : `enrichment_status=${v.enrichment_status || 'unknown'}`;
        preMapRows.push({
            bucketing_run_id: runId,
            industry_string: v.industry,
            raw_industry: v.industry,
            bucket_name: RESERVED_GENERAL,
            source: 'general_passthrough',
            confidence: 0,
            primary_identity: null,
            sub_identity: null,
            sector: null,
            is_new_identity: false,
            is_new_sub_identity: false,
            is_new_sector: false,
            is_disqualified: false,
            is_generic: true,
            needs_qa: false,
            canonical_classification: 'General',
            llm_reason: reason,
        });
    }

    // ── Load active taxonomy library (3 SELECTs) ─────────────────────
    const snapshot = await loadTaxonomySnapshot(supabase);
    ctx.log(`[Bucketing ${runId}] taxonomy library: ${snapshot.identities.length} identities, ${snapshot.sub_identities.length} sub-identities, ${snapshot.sectors.length} sectors`);

    // Resume filter: skip industries that already have an llm_phase1a row.
    const completedVocabToTag = completedVocab.filter(v => !existingLLMTagged.has(v.industry));
    const skippedTaggedCount = completedVocab.length - completedVocabToTag.length;
    if (skippedTaggedCount > 0) {
        ctx.log(`[Bucketing ${runId}] resume — ${skippedTaggedCount.toLocaleString()} industries already tagged, ${completedVocabToTag.length.toLocaleString()} remaining`);
    }

    if (completedVocabToTag.length > 0) {
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
            total: completedVocabToTag.length,
            note: `Tagging ${completedVocabToTag.length} industries with ${phase1aModel}…`
        });
        const tagResult = await withHeartbeat(
            `${phase1aModel} tagging (${completedVocabToTag.length} industries)`,
            () => tagIndustries(supabase, phase1aModel, completedVocabToTag, snapshot, runId, ctx),
            ctx
        );
        totalCost += tagResult.costUsd;
        ctx.log(`[Bucketing ${runId}] tagging: ${tagResult.taggings.length} results, $${tagResult.costUsd.toFixed(4)}, model=${tagResult.modelUsed}`);

        // Post-batch consolidation: collapse near-duplicate tags across the
        // 40-concurrent batches that don't see each other's output. Critical
        // when the library is small/empty — without this, a single concept
        // shows up under 4-5 spelling variants in the AI-PROPOSED panel.
        const merges = consolidateTaggings(tagResult.taggings, snapshot);
        if (merges.identityMerges + merges.subIdentityMerges + merges.sectorMerges > 0) {
            ctx.log(`[Bucketing ${runId}] post-batch dedup: collapsed ${merges.identityMerges} identity / ${merges.subIdentityMerges} sub-identity / ${merges.sectorMerges} sector duplicate variants`);
        }

        // LLM semantic consolidation of proposed-new sub-identities. Catches
        // semantic duplicates the normalizer misses — "Wealth Management" +
        // "Investment Advisory" + "Wealth & Investment Advisory" → one name.
        // No-op when proposed-new count < 60 (small runs don't need it).
        //
        // Multi-pass: each pass merges what it can; subsequent passes catch
        // chains the first missed (e.g. "Investment Research" → "Investment
        // Advisory" → "Investment Management"). Stops early when a pass
        // finds < 5 merges (diminishing returns) or after 3 passes max.
        let totalCharMerges = 0;
        let totalCharCost = 0;
        for (let pass = 1; pass <= 3; pass++) {
            const r = await consolidateSubIdentitiesViaLLM(
                supabase, phase1aModel, tagResult.taggings, snapshot, runId, ctx
            );
            totalCharMerges += r.merges;
            totalCharCost += r.costUsd;
            if (r.merges > 0) {
                ctx.log(`[Bucketing ${runId}] LLM sub-identity consolidation pass ${pass}: merged ${r.merges} variant names, $${r.costUsd.toFixed(4)}`);
            }
            if (r.merges < 5) break;  // diminishing returns
        }
        const charConsol = { merges: totalCharMerges, costUsd: totalCharCost };
        if (charConsol.merges > 0) {
            totalCost += charConsol.costUsd;
            ctx.log(`[Bucketing ${runId}] LLM sub-identity consolidation TOTAL: ${charConsol.merges} merges, $${charConsol.costUsd.toFixed(4)}`);
        }

        // LLM semantic consolidation of proposed-new sectors. Same pattern,
        // sector-tailored prompt — catches Sports / Sports & Athletics / Esports
        // → "Media & Entertainment" and drops identity-bleed entries like
        // "Marketing / Advertising" / "Insurance" / "IT Services" to BLANK.
        // No-op when proposed-new count < 15.
        const secConsol = await consolidateSectorsViaLLM(
            supabase, phase1aModel, tagResult.taggings, snapshot, runId, ctx
        );
        if (secConsol.merges > 0) {
            totalCost += secConsol.costUsd;
            ctx.log(`[Bucketing ${runId}] LLM sector consolidation: merged ${secConsol.merges} variant names, $${secConsol.costUsd.toFixed(4)}`);
        }

        // LLM semantic consolidation of proposed-new identities. Smaller
        // input than chars/sectors (typically 5-25 entries) so a single call
        // suffices. Catches semantic duplicates the normalizer misses ("Real
        // Estate Services" → "Real Estate") and merges proposals INTO existing
        // library identities. No-op when proposed-new count < 6.
        const idConsol = await consolidateIdentitiesViaLLM(
            supabase, phase1aModel, tagResult.taggings, snapshot, runId, ctx
        );
        if (idConsol.merges > 0) {
            totalCost += idConsol.costUsd;
            ctx.log(`[Bucketing ${runId}] LLM identity consolidation: merged ${idConsol.merges} variant names, $${idConsol.costUsd.toFixed(4)}`);
        }

        // Drop singleton new-identity proposals (count < 3) — these would
        // never clear min_volume anyway and bloat the library across runs.
        // Runs AFTER LLM identity consolidation so any near-duplicate that
        // could've merged into a higher-count peer has already been merged.
        const singletonDrop = dropSingletonNewIdentities(
            tagResult.taggings, snapshot, runId, ctx
        );

        // Re-run the cheap normalizer-based dedup so any new identical names
        // produced by the LLM merges collapse into one canonical.
        if (charConsol.merges + secConsol.merges + idConsol.merges + singletonDrop.dropped > 0) {
            const reMerges = consolidateTaggings(tagResult.taggings, snapshot);
            if (reMerges.identityMerges + reMerges.subIdentityMerges + reMerges.sectorMerges > 0) {
                ctx.log(`[Bucketing ${runId}] post-LLM normalizer pass: collapsed ${reMerges.identityMerges} more identity + ${reMerges.subIdentityMerges} more sub-identity + ${reMerges.sectorMerges} more sector variants`);
            }
        }

        // ── Build map rows from taggings ─────────────────────────────
        const identitySet = new Set(snapshot.identities.map(i => i.name));
        const charSet = new Set(snapshot.sub_identities.map(c => c.name));
        const sectorSet = new Set(snapshot.sectors.map(s => s.name));
        const dqIdentities = new Set(snapshot.identities.filter(i => i.is_disqualified).map(i => i.name));
        // Fuzzy dedup: normalize each existing canonical name once so we can
        // catch near-duplicates the LLM proposes ("Telecommunications" vs
        // "Telecommunications Services", "agency" vs "Agency", etc.). When a
        // proposed-new tag normalizes to an existing entry, we rewrite the
        // tag to the canonical name AND flip is_new_*=false so the library
        // doesn't grow with bloat.
        const identityCanonical = new Map<string, string>();
        for (const i of snapshot.identities) identityCanonical.set(normalizeTaxonomyName(i.name), i.name);
        const charCanonical = new Map<string, string>();
        for (const c of snapshot.sub_identities) charCanonical.set(normalizeTaxonomyName(c.name), c.name);
        const sectorCanonical = new Map<string, string>();
        for (const s of snapshot.sectors) sectorCanonical.set(normalizeTaxonomyName(s.name), s.name);
        // Identity-DQ cascade is OFF by default — we trust the tagger's per-row
        // is_disqualified judgment. Set apply_identity_dq_cascade=true on
        // the run to restore the old auto-DQ behavior for [DQ]-flagged
        // identities (e.g. force every "Consumer & Retail" tag to DQ).
        const cascadeDq = !!(run as any).apply_identity_dq_cascade;

        for (const t of tagResult.taggings) {
            const conf01 = Math.max(0, Math.min(1, (t.confidence || 0) / 10));
            // Confidence gate — use identity_confidence ONLY, not the
            // row-min (which is dragged down to 1 whenever the LLM correctly
            // nulls sub_identity or sector per the new per-tag confidence
            // rule in CORE_PRINCIPLES). The previous row-min gate was
            // dumping rows with high identity confidence + null sub into
            // the pre-rollup General bucket, making the taxonomy_ready
            // panel under-report identity counts.
            const lowConf = (t.identity_confidence || 0) < PHASE1A_QA_FLOOR;
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

            // Fuzzy-dedup: if the LLM's tag normalizes to an existing canonical
            // entry, rewrite the tag to that canonical name and clear is_new_*.
            const idCanonical = t.identity ? (identityCanonical.get(normalizeTaxonomyName(t.identity)) || null) : null;
            const chCanonical = t.sub_identity ? (charCanonical.get(normalizeTaxonomyName(t.sub_identity)) || null) : null;
            const secCanonical = t.sector ? (sectorCanonical.get(normalizeTaxonomyName(t.sector)) || null) : null;
            const tIdentity = idCanonical || t.identity;
            const tSubIdentity = chCanonical || t.sub_identity;
            const tSector = secCanonical || t.sector;
            const tIsNewIdentity = !!t.is_new_identity && !idCanonical && !identitySet.has(tIdentity || '');
            const tIsNewSubIdentity = !!t.is_new_sub_identity && !chCanonical && !charSet.has(tSubIdentity || '');
            const tIsNewSector = !!t.is_new_sector && !secCanonical && !sectorSet.has(tSector || '');

            // Pre-rollup name: sub-identity preferred → identity → fallback.
            // The actual final bucket_name is rewritten by the volume rollup
            // in Phase 1b. Disqualified is terminal — never rolled up.
            let preBucket: string;
            if (isDisqualified) {
                preBucket = RESERVED_DISQUALIFIED;
            } else if (lowConf) {
                preBucket = RESERVED_GENERAL;
            } else if (tSubIdentity) {
                preBucket = tSubIdentity;
            } else if (tIdentity) {
                preBucket = tIdentity;
            } else {
                preBucket = RESERVED_GENERAL;
            }

            // Canonical classification: concise truth statement combining the
            // narrowest available fields. Used for the CSV export. Old format
            // glued sector + sub_identity with a space ("Construction &
            // Infrastructure Custom Home Builder") which read as a single
            // garbled name; explicit " — " separator + parens for sector
            // makes "Custom Home Builder (Construction & Infrastructure)"
            // read as one tag with vertical context.
            const canonical = isDisqualified
                ? 'Disqualified'
                : (tSubIdentity && tSector ? `${tSubIdentity} (${tSector})`
                    : (tSubIdentity || tIdentity || 'Generic'));

            preMapRows.push({
                bucketing_run_id: runId,
                industry_string: t.industry,
                raw_industry: t.industry,
                bucket_name: preBucket,
                source: 'llm_phase1a',
                confidence: Number(conf01.toFixed(2)),
                identity_confidence: Number(((t.identity_confidence || 0) / 10).toFixed(2)),
                sub_identity_confidence: Number(((t.sub_identity_confidence || 0) / 10).toFixed(2)),
                sector_confidence: Number(((t.sector_confidence || 0) / 10).toFixed(2)),
                primary_identity: tIdentity,
                sub_identity: tSubIdentity,
                sector: tSector,
                is_new_identity: tIsNewIdentity,
                is_new_sub_identity: tIsNewSubIdentity,
                is_new_sector: tIsNewSector,
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
    // — we synthesize them from the unique tags actually used.
    //
    // Resume-aware: read the full bucket_industry_map for this run from DB
    // instead of using the in-memory preMapRows. This way a resumed run that
    // only re-tagged a few thousand new industries still sees the full
    // historical set in the proposal.
    const proposalRows: any[] = [];
    {
        const PAGE = 1000;
        for (let off = 0; off < 500_000; off += PAGE) {
            const { data, error } = await supabase
                .from('bucket_industry_map')
                .select('industry_string,primary_identity,sub_identity,sector')
                .eq('bucketing_run_id', runId)
                .range(off, off + PAGE - 1);
            if (error) throw new Error(`taxonomy_proposal rebuild fetch failed at offset ${off}: ${error.message}`);
            const rows = (data || []);
            proposalRows.push(...rows);
            if (rows.length < PAGE) break;
        }
    }
    const usedIdentitySet = new Set<string>();
    const usedCharByKey = new Map<string, { spec: string; identity: string; description: string }>();
    const usedSectors = new Set<string>();
    for (const r of proposalRows) {
        if (r.primary_identity) usedIdentitySet.add(r.primary_identity);
        if (r.sub_identity && r.primary_identity) {
            const key = `${r.primary_identity}::${r.sub_identity}`;
            if (!usedCharByKey.has(key)) {
                const charDesc = snapshot.sub_identities.find(c => c.name === r.sub_identity)?.description || '';
                usedCharByKey.set(key, { spec: r.sub_identity, identity: r.primary_identity, description: charDesc });
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

    // Only emit a bucket per (identity, sub-identity) pair where the tagger
    // committed to BOTH layers. Identity-only rows (the tagger was sure about
    // identity but not sub-identity) don't get a fake "identity as spec"
    // bucket — instead Phase 1b's rollup decides their fate based on
    // identity volume vs the threshold (clears → identity bucket, doesn't →
    // General with reason=low_volume). This keeps the user's rule "if you
    // can't decide, send to General" honest.
    const buckets: DiscoveredBucket[] = [];
    for (const c of usedCharByKey.values()) {
        buckets.push({
            sub_identity: c.spec,
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
            sub_identities: snapshot.sub_identities,
            sectors: snapshot.sectors
        },
        // taxonomy_model is now set by /determine from the user's Setup
        // screen choice — don't overwrite it here.
        cost_usd: totalCost,
        total_contacts: totalContacts,
        coverage_summary: {
            phase1a_contacts: phase1aCoveredContacts,
            selected_contacts: totalContacts,
            phase1a_coverage_pct: totalContacts > 0
                ? Number(((phase1aCoveredContacts / totalContacts) * 100).toFixed(2))
                : 0,
            distinct_classifications: proposalRows.length,
            taggable_classifications: completedVocab.length,
            passthrough_classifications: dqVocab.length
        },
        quality_warnings: phase1aCoveredContacts === totalContacts ? [] : [
            `Phase 1a taxonomy covers ${phase1aCoveredContacts.toLocaleString()} of ${totalContacts.toLocaleString()} selected contacts.`
        ],
        status: 'taxonomy_ready',
        taxonomy_completed_at: new Date().toISOString()
    }).eq('id', runId);

    ctx.log(`[Bucketing ${runId}] Phase 1a done — tagged ${finalRows.length} industries (${dqVocab.length} DQ, ${completedVocab.length} via LLM), $${totalCost.toFixed(4)}`, 'phase');
    ctx.progress({
        phase: 'phase1a', step: 'done', current: 1, total: 1,
        note: `Tagging complete — ${primaryIdentities.length} identities, ${buckets.length} sub-identities, ${usedSectors.size} sectors used`
    });
}

// ────────────────────────────────────────────────────────────────────
// RECALC AGAINST UPDATED LIBRARY
//
// After the Review screen accepts AI-proposed identities/sub-identities/
// sectors, those entries land in the taxonomy_* library tables. The
// existing bucket_industry_map rows still carry the original tag strings
// (which is correct — accepting doesn't change names) but their
// is_new_* flags are stale (now that the names ARE in the library) AND
// the LLM consolidation passes haven't seen the new library entries as
// merge targets. This helper:
//
//   1. Reloads the (now larger) library snapshot.
//   2. Hydrates IndustryTagging[] from existing bucket_industry_map rows.
//   3. Refreshes is_new_* flags by checking each tag against the live
//      library (cheap, no LLM).
//   4. Re-runs the cheap normalizer dedup + every LLM consolidation pass
//      (sub-identities / sectors / identities) so any STILL-proposed-new
//      entries can merge INTO the now-canonical library names.
//   5. Drops new-identity singletons (count < 3) — same rule as Phase 1a.
//   6. Writes the updated tags back to bucket_industry_map and
//      re-synthesizes taxonomy_proposal so the Review screen's
//      Discovered Sub-Identities panel reflects the new shape.
//
// Intentionally does NOT re-tag any contact via LLM — the per-row tags
// produced by Phase 1a are kept intact. Only consolidation/flag refresh.
// ────────────────────────────────────────────────────────────────────

export async function recalculateTaxonomyWithLibrary(
    supabase: SupabaseClient,
    runId: string,
    ctx: BucketingCtx
): Promise<{
    updatedRows: number;
    flagsRefreshed: number;
    identityMerges: number;
    subIdentityMerges: number;
    sectorMerges: number;
    singletonsDropped: number;
    costUsd: number;
}> {
    const { data: run, error: runErr } = await supabase
        .from('bucketing_runs').select('*').eq('id', runId).single();
    if (runErr || !run) throw new Error(`Run not found: ${runErr?.message}`);
    if (run.status !== 'taxonomy_ready' && run.status !== 'completed') {
        throw new Error(`Cannot recalculate from status "${run.status}" — run must be in taxonomy_ready or completed`);
    }

    const phase1aModel = (run.taxonomy_model as string) || 'gpt-4.1-mini';

    const snapshot = await loadTaxonomySnapshot(supabase);
    ctx.log(`[Recalc ${runId}] library snapshot: ${snapshot.identities.length} identities, ${snapshot.sub_identities.length} sub-identities, ${snapshot.sectors.length} sectors`);

    // Pull every row in this run's bucket_industry_map. Paginate to dodge
    // PostgREST's 1000-row hard cap (same trick the streaming CSV uses).
    const allRows: any[] = [];
    {
        const PAGE = 1000;
        for (let off = 0; off < 200_000; off += PAGE) {
            const { data, error } = await supabase
                .from('bucket_industry_map')
                .select('*')
                .eq('bucketing_run_id', runId)
                .range(off, off + PAGE - 1);
            if (error) throw new Error(`bucket_industry_map fetch failed at offset ${off}: ${error.message}`);
            const rows = (data || []) as any[];
            allRows.push(...rows);
            if (rows.length < PAGE) break;
        }
    }
    ctx.log(`[Recalc ${runId}] loaded ${allRows.length} bucket_industry_map rows`);

    // Disqualified-passthrough rows (source != 'llm_phase1a') don't carry
    // identity/sub-identity/sector data — leave them untouched.
    const llmRows = allRows.filter(r => r.source === 'llm_phase1a');

    // Hydrate IndustryTagging[] from rows. Stored confidences are 0-1
    // (Phase 1a rounded them); IndustryTagging uses 0-10 internally.
    const taggings: IndustryTagging[] = llmRows.map(r => ({
        industry: r.industry_string,
        identity: r.primary_identity || null,
        is_new_identity: !!r.is_new_identity,
        sub_identity: r.sub_identity || null,
        is_new_sub_identity: !!r.is_new_sub_identity,
        sector: r.sector || null,
        is_new_sector: !!r.is_new_sector,
        is_disqualified: !!r.is_disqualified,
        identity_confidence: Math.round((Number(r.identity_confidence) || 0) * 10),
        sub_identity_confidence: Math.round((Number(r.sub_identity_confidence) || 0) * 10),
        sector_confidence: Math.round((Number(r.sector_confidence) || 0) * 10),
        confidence: Math.round((Number(r.confidence) || 0) * 10),
        reason: r.llm_reason || ''
    }));

    const libIdentities = new Set(snapshot.identities.map(i => i.name));
    const libChars = new Set(snapshot.sub_identities.map(c => c.name));
    const libSectors = new Set(snapshot.sectors.map(s => s.name));
    // Parent-aware sub lookup: a sub is only "established" relative to its
    // declared parent. "Non-Profit Organization" exists in the library, but
    // pairing it with identity = Consulting & Advisory is still wrong.
    const libSubParent = new Map<string, string>();
    for (const c of snapshot.sub_identities) libSubParent.set(c.name, c.parent_identity || '');
    const subEstablished = (subName: string | null, identityName: string | null): boolean => {
        if (!subName) return false;
        const parent = libSubParent.get(subName);
        if (!parent) return false;
        return !identityName || parent === identityName;
    };

    // Drop sub_identities whose library parent doesn't match the chosen
    // identity. The Review screen only surfaces new-proposal rows, so these
    // cross-parent pairs would otherwise survive Finalize. Better to lose
    // the sub than to write a structurally inconsistent pair.
    let crossParentDrops = 0;
    for (const t of taggings) {
        if (t.sub_identity && t.identity && !t.is_new_identity) {
            const subParent = libSubParent.get(t.sub_identity);
            if (subParent && subParent !== t.identity) {
                t.sub_identity = null;
                t.is_new_sub_identity = false;
                t.sub_identity_confidence = 1;
                crossParentDrops++;
            }
        }
    }
    if (crossParentDrops > 0) {
        ctx.log(`[Bucketing ${runId}] recalc: dropped ${crossParentDrops} sub_identity values whose library parent differed from the chosen identity`);
    }

    // Snapshot the original is_new flags so we can count how many flipped.
    const originalFlags = taggings.map(t => ({
        ni: !!t.is_new_identity, nc: !!t.is_new_sub_identity, ns: !!t.is_new_sector
    }));

    // Step 1 — refresh is_new_* against the now-current library.
    // sub_identity is "established" only when its library parent matches the
    // chosen identity; otherwise the LLM mis-paired and we keep is_new=true
    // so the Review screen surfaces it.
    for (const t of taggings) {
        t.is_new_identity = !!t.is_new_identity && !libIdentities.has(t.identity || '');
        t.is_new_sub_identity = !!t.is_new_sub_identity && !subEstablished(t.sub_identity, t.identity);
        t.is_new_sector = !!t.is_new_sector && !libSectors.has(t.sector || '');
    }

    // Step 2 — cheap normalizer dedup. Catches plurals / casing the
    // newly-accepted library entries can now collapse into.
    consolidateTaggings(taggings, snapshot);

    // Step 3 — LLM consolidation passes. Each is a no-op when the
    // proposed-new pool is below its threshold, so subsequent recalcs
    // (after the user has whittled the panel down) cost nothing.
    let costUsd = 0;
    let totalCharMerges = 0;
    for (let pass = 1; pass <= 2; pass++) {
        const r = await consolidateSubIdentitiesViaLLM(supabase, phase1aModel, taggings, snapshot, runId, ctx);
        totalCharMerges += r.merges;
        costUsd += r.costUsd;
        if (r.merges < 5) break;
    }
    const secConsol = await consolidateSectorsViaLLM(supabase, phase1aModel, taggings, snapshot, runId, ctx);
    costUsd += secConsol.costUsd;
    const idConsol = await consolidateIdentitiesViaLLM(supabase, phase1aModel, taggings, snapshot, runId, ctx);
    costUsd += idConsol.costUsd;

    // Step 4 — drop new-identity singletons (count < 3).
    const singletonDrop = dropSingletonNewIdentities(taggings, snapshot, runId, ctx);

    // Step 5 — re-run normalizer once more to catch any new collisions
    // produced by the LLM consolidation merges.
    consolidateTaggings(taggings, snapshot);

    // Step 6 — final flag refresh after consolidation may have rewritten
    // names INTO library canonicals. Same parent-aware sub check as Step 1.
    let flagsRefreshed = 0;
    for (let i = 0; i < taggings.length; i++) {
        const t = taggings[i];
        const newNi = !!t.is_new_identity && !libIdentities.has(t.identity || '');
        const newNc = !!t.is_new_sub_identity && !subEstablished(t.sub_identity, t.identity);
        const newNs = !!t.is_new_sector && !libSectors.has(t.sector || '');
        const orig = originalFlags[i];
        if (newNi !== orig.ni || newNc !== orig.nc || newNs !== orig.ns) flagsRefreshed++;
        t.is_new_identity = newNi;
        t.is_new_sub_identity = newNc;
        t.is_new_sector = newNs;
    }

    // Step 7 — write updated rows back. Re-derive bucket_name +
    // canonical_classification the same way runTaxonomyProposal did so
    // the preview counts and downstream cascade stay in sync.
    const updates: any[] = [];
    for (const t of taggings) {
        // Identity-confidence gate (same fix as in the Phase 1a tagger hot
        // path — see the lengthier comment there). Row-min was dragging
        // confident-identity rows into pre-rollup General whenever sub or
        // sector were correctly null.
        const lowConf = (t.identity_confidence || 0) < PHASE1A_QA_FLOOR;
        const idConf = t.identity_confidence || 0;
        const dqByLLM = !!t.is_disqualified && idConf >= PHASE1A_DQ_FLOOR;
        const isDisqualified = dqByLLM;

        let preBucket: string;
        if (isDisqualified) preBucket = RESERVED_DISQUALIFIED;
        else if (lowConf) preBucket = RESERVED_GENERAL;
        else if (t.sub_identity) preBucket = t.sub_identity;
        else if (t.identity) preBucket = t.identity;
        else preBucket = RESERVED_GENERAL;

        const canonical = isDisqualified
            ? 'Disqualified'
            : (t.sub_identity && t.sector ? `${t.sub_identity} (${t.sector})`
                : (t.sub_identity || t.identity || 'Generic'));

        updates.push({
            bucketing_run_id: runId,
            industry_string: t.industry,
            // NOT NULL columns must be present even on update-via-upsert: PG
            // validates the INSERT path before the ON CONFLICT redirect, so
            // omitting source / raw_industry / bucket_name throws even when
            // the row already exists.
            source: 'llm_phase1a',
            raw_industry: t.industry,
            primary_identity: t.identity,
            sub_identity: t.sub_identity,
            sector: t.sector,
            is_new_identity: t.is_new_identity,
            is_new_sub_identity: t.is_new_sub_identity,
            is_new_sector: t.is_new_sector,
            is_disqualified: isDisqualified,
            bucket_name: preBucket,
            canonical_classification: canonical
        });
    }

    for (let i = 0; i < updates.length; i += 1000) {
        const chunk = updates.slice(i, i + 1000);
        const { error } = await supabase.from('bucket_industry_map')
            .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
        if (error) throw new Error(`recalc upsert failed: ${error.message}`);
    }

    // Step 8 — re-synthesize taxonomy_proposal from the freshly-written
    // rows. Mirrors the synthesis at the end of runTaxonomyProposal.
    const usedIdentitySet = new Set<string>();
    const usedCharByKey = new Map<string, { spec: string; identity: string; description: string }>();
    const usedSectors = new Set<string>();
    for (const r of updates) {
        if (r.primary_identity) usedIdentitySet.add(r.primary_identity);
        if (r.sub_identity && r.primary_identity) {
            const key = `${r.primary_identity}::${r.sub_identity}`;
            if (!usedCharByKey.has(key)) {
                const charDesc = snapshot.sub_identities.find(c => c.name === r.sub_identity)?.description || '';
                usedCharByKey.set(key, { spec: r.sub_identity, identity: r.primary_identity, description: charDesc });
            }
        }
        if (r.sector) usedSectors.add(r.sector);
    }
    const primaryIdentities = Array.from(usedIdentitySet).map(name => {
        const ent = snapshot.identities.find(i => i.name === name);
        return { name, description: ent?.description || '', identity_type: 'other', operator_required: false };
    });
    const buckets: DiscoveredBucket[] = [];
    for (const c of usedCharByKey.values()) {
        buckets.push({
            sub_identity: c.spec, primary_identity: c.identity, description: c.description,
            identity_type: 'other', operator_required: false, priority_rank: 5,
            include: [], exclude: [], example_strings: [],
            strong_identity_signals: [], weak_sector_signals: [], disqualifying_signals: []
        });
    }
    await supabase.from('bucketing_runs').update({
        taxonomy_proposal: {
            observed_patterns: [],
            sector_vocabulary: Array.from(usedSectors),
            primary_identities: primaryIdentities,
            buckets
        },
        taxonomy_snapshot: {
            identities: snapshot.identities,
            sub_identities: snapshot.sub_identities,
            sectors: snapshot.sectors
        },
        cost_usd: (Number(run.cost_usd) || 0) + costUsd
    }).eq('id', runId);

    ctx.log(`[Recalc ${runId}] done — ${updates.length} rows updated, ${flagsRefreshed} is_new flags flipped, char merges=${totalCharMerges}, sector merges=${secConsol.merges}, identity merges=${idConsol.merges}, singletons dropped=${singletonDrop.dropped}, cost $${costUsd.toFixed(4)}`, 'phase');

    return {
        updatedRows: updates.length,
        flagsRefreshed,
        identityMerges: idConsol.merges,
        subIdentityMerges: totalCharMerges,
        sectorMerges: secConsol.merges,
        singletonsDropped: singletonDrop.dropped,
        costUsd
    };
}

// ────────────────────────────────────────────────────────────────────
// FINALIZE — re-tag remaining proposals against library only
//
// Once the user is done reviewing AI-proposed taxonomy additions, the
// rows still flagged is_new_*=true are entries the user did NOT
// accept (rejected, or simply ignored). Phase 1a v2 finalize is now
// deterministic: any is_new_* layer whose name was accepted into the
// library this session survives; any layer that was NOT accepted gets
// nulled, and rows whose primary_identity is nulled route to General.
// Zero LLM calls, predictable cost.
// ────────────────────────────────────────────────────────────────────

export async function finalizeTaxonomyAgainstLibrary(
    supabase: SupabaseClient,
    runId: string,
    ctx: BucketingCtx
): Promise<{
    candidates: number;
    rerouted: number;
    nullified: number;
    failed: number;
    costUsd: number;
    per_contact_total?: number;
    per_contact_with_identity?: number;
    per_contact_with_sub?: number;
    per_contact_with_sector?: number;
}> {
    const { data: run, error: runErr } = await supabase
        .from('bucketing_runs').select('*').eq('id', runId).single();
    if (runErr || !run) throw new Error(`Run not found: ${runErr?.message}`);
    if (run.status !== 'taxonomy_ready' && run.status !== 'completed') {
        throw new Error(`Cannot finalize from status "${run.status}" — run must be in taxonomy_ready or completed`);
    }

    const phase1aModel = (run.taxonomy_model as string) || 'gpt-4.1-mini';
    const isAnthropic = phase1aModel.startsWith('claude-');
    const anthropic = isAnthropic ? await getAnthropic(supabase) : null;
    if (isAnthropic && !anthropic) {
        throw new Error('Anthropic API key not configured. Add it on the Connectors page (saved as ANTHROPIC_API_KEY).');
    }

    const snapshot = await loadTaxonomySnapshot(supabase);
    if (snapshot.identities.length === 0 && snapshot.sub_identities.length === 0 && snapshot.sectors.length === 0) {
        throw new Error('Library is empty — accept at least some AI-proposed entries before finalizing.');
    }

    // Phase 1a v2: Finalize is now deterministic. Original behavior asked the
    // LLM to re-tag every is_new_* orphan against a library-only prompt —
    // expensive AND it could re-coin variants the user explicitly rejected.
    // New behavior matches the user's spec: any is_new_* layer that the user
    // accepted into the library survives (is_new flag cleared); any is_new_*
    // that wasn't accepted gets nulled, and rows whose primary_identity is
    // nulled route to General. Zero LLM calls, predictable cost.
    //
    // Each accepted-this-session entry is already present in the live
    // library snapshot (the panel's Accept button writes to
    // taxonomy_{identities|sub_identities|sectors}). So "did the user accept
    // this proposal?" reduces to "is this name in the library snapshot now?"
    const libIdentities = new Set(snapshot.identities.map(i => i.name));
    const libSubs = new Set(snapshot.sub_identities.map(c => c.name));
    const libSectors = new Set(snapshot.sectors.map(s => s.name));
    const dqIdentities = new Set(snapshot.identities.filter(i => i.is_disqualified).map(i => i.name));

    // Single keyset scan of the run's Phase 1a map. OFFSET pagination
    // (.range) degrades to O(n²) — each page re-scans the whole 300k-row
    // partition with random heap I/O — and on a large run it blew past the
    // REST statement_timeout ("canceling statement due to statement timeout"
    // at offset 10000). Keyset by industry_string (the PK's second column)
    // walks the partition once via the PK index, so every page is 1000
    // sequential rows regardless of run size.
    //
    // We read ALL rows once and partition in JS instead of filtering on the
    // unindexed is_new_* booleans:
    //   • candidates = source='llm_phase1a' rows with any is_new_* flag —
    //                  these get the keep/null pass below.
    //   • everything else keeps its taxonomy through finalize, so we fold it
    //     straight into the used-taxonomy accumulators that re-synthesize
    //     taxonomy_proposal — replacing the old second full-table re-fetch.
    const usedIdentitySet = new Set<string>();
    const usedCharByKey = new Map<string, { spec: string; identity: string; description: string }>();
    const usedSectors = new Set<string>();
    const addUsed = (identity: string | null, sub: string | null, sector: string | null) => {
        if (identity) usedIdentitySet.add(identity);
        if (sub && identity) {
            const key = `${identity}::${sub}`;
            if (!usedCharByKey.has(key)) {
                const charDesc = snapshot.sub_identities.find(c => c.name === sub)?.description || '';
                usedCharByKey.set(key, { spec: sub, identity, description: charDesc });
            }
        }
        if (sector) usedSectors.add(sector);
    };

    const candidates: any[] = [];
    {
        const PAGE = 1000;
        let lastKey: string | null = null;
        for (;;) {
            let q = supabase
                .from('bucket_industry_map')
                .select('industry_string,primary_identity,sub_identity,sector,is_new_identity,is_new_sub_identity,is_new_sector,is_disqualified,source,raw_industry')
                .eq('bucketing_run_id', runId);
            if (lastKey !== null) q = q.gt('industry_string', lastKey);
            const { data, error } = await q.order('industry_string', { ascending: true }).limit(PAGE);
            if (error) throw new Error(`bucket_industry_map scan failed after ${candidates.length} candidates: ${error.message}`);
            const rows = (data || []) as any[];
            for (const r of rows) {
                if (r.source === 'llm_phase1a' && (r.is_new_identity || r.is_new_sub_identity || r.is_new_sector)) {
                    candidates.push(r);
                } else {
                    // Untouched by finalize → its current taxonomy is final.
                    addUsed(r.primary_identity, r.sub_identity, r.sector);
                }
            }
            if (rows.length < PAGE) break;
            lastKey = rows[rows.length - 1].industry_string;
        }
    }
    ctx.log(`[Finalize ${runId}] ${candidates.length} orphan row(s) — keeping accepted proposals, routing rejected ones to General (library: ${snapshot.identities.length} identities / ${snapshot.sub_identities.length} sub-identities / ${snapshot.sectors.length} sectors)`);

    if (candidates.length === 0) {
        return { candidates: 0, rerouted: 0, nullified: 0, failed: 0, costUsd: 0 };
    }

    let rerouted = 0;   // at least one layer survived (matched library)
    let nullified = 0;  // all proposed layers got nulled → General
    const failed = 0;   // no LLM calls means no batch failures
    const costUsd = 0;
    const updates: any[] = [];

    for (const row of candidates) {
        // Per layer: keep the tag iff EITHER it wasn't flagged is_new_*, or
        // its name is now in the library (the user accepted it this session).
        // Otherwise drop the layer to null.
        const idAccepted = !row.is_new_identity || (row.primary_identity && libIdentities.has(row.primary_identity));
        const subAccepted = !row.is_new_sub_identity || (row.sub_identity && libSubs.has(row.sub_identity));
        const secAccepted = !row.is_new_sector || (row.sector && libSectors.has(row.sector));

        const newIdentity = idAccepted ? row.primary_identity : null;
        const newSub = subAccepted ? row.sub_identity : null;
        const newSector = secAccepted ? row.sector : null;

        const identityIsDq = !!(newIdentity && dqIdentities.has(newIdentity));
        const isDisqualified = !!row.is_disqualified || identityIsDq;

        // Bucket routing: if all layers got nulled OR identity got nulled,
        // route to General. Otherwise preserve whatever bucket the row had
        // — taxonomy → bucket re-map happens in /assign-buckets later.
        let preBucket: string;
        if (isDisqualified) preBucket = RESERVED_DISQUALIFIED;
        else if (!newIdentity) preBucket = RESERVED_GENERAL;
        else if (newSub) preBucket = newSub;
        else preBucket = newIdentity;

        const canonical = isDisqualified
            ? 'Disqualified'
            : (newSub && newSector ? `${newSub} (${newSector})`
                : (newSub || newIdentity || 'Generic'));

        // Post-finalize values feed the taxonomy_proposal re-synthesis below
        // (this replaces re-reading the whole table after the update).
        addUsed(newIdentity, newSub, newSector);

        if (!newIdentity && !newSub && !newSector) nullified++;
        else rerouted++;

        updates.push({
            bucketing_run_id: runId,
            industry_string: row.industry_string,
            // NOT NULL columns must be present even on update-via-upsert.
            source: row.source || 'llm_phase1a',
            raw_industry: row.raw_industry || row.industry_string,
            primary_identity: newIdentity,
            sub_identity: newSub,
            sector: newSector,
            // Clear ALL is_new_* flags — finalize is the terminal step.
            // Any layer the user didn't accept is now null; nothing left
            // to flag as "new and pending".
            is_new_identity: false,
            is_new_sub_identity: false,
            is_new_sector: false,
            is_disqualified: isDisqualified,
            bucket_name: preBucket,
            canonical_classification: canonical
        });
    }

    for (let i = 0; i < updates.length; i += 1000) {
        const chunk = updates.slice(i, i + 1000);
        const { error } = await supabase.from('bucket_industry_map')
            .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
        if (error) throw new Error(`finalize upsert failed: ${error.message}`);
    }

    // Re-synthesize taxonomy_proposal so the Review screen reflects the new
    // shape (orphan rows now point at library entries or null). The used-tag
    // accumulators were filled during the single scan above (non-candidate
    // rows) and the keep/null pass (candidates' post-finalize values), so no
    // second full-table read is needed.
    const primaryIdentities = Array.from(usedIdentitySet).map(name => {
        const ent = snapshot.identities.find(i => i.name === name);
        return { name, description: ent?.description || '', identity_type: 'other', operator_required: false };
    });
    const buckets: DiscoveredBucket[] = [];
    for (const c of usedCharByKey.values()) {
        buckets.push({
            sub_identity: c.spec, primary_identity: c.identity, description: c.description,
            identity_type: 'other', operator_required: false, priority_rank: 5,
            include: [], exclude: [], example_strings: [],
            strong_identity_signals: [], weak_sector_signals: [], disqualifying_signals: []
        });
    }
    await supabase.from('bucketing_runs').update({
        taxonomy_proposal: {
            observed_patterns: [],
            sector_vocabulary: Array.from(usedSectors),
            primary_identities: primaryIdentities,
            buckets
        },
        taxonomy_snapshot: {
            identities: snapshot.identities,
            sub_identities: snapshot.sub_identities,
            sectors: snapshot.sectors
        },
        cost_usd: (Number(run.cost_usd) || 0) + costUsd,
        // Persist finalize state so the UI can show "already finalized"
        // after a page reload instead of relying on in-memory React state.
        finalize_completed_at: new Date().toISOString(),
        finalize_rerouted_count: rerouted,
        finalize_nullified_count: nullified,
        finalize_failed_count: failed
    }).eq('id', runId);

    ctx.log(`[Finalize ${runId}] done — ${candidates.length} orphans processed: ${rerouted} kept (proposals accepted into library), ${nullified} → General (proposals rejected). No LLM calls.`, 'phase');

    // Phase 1a's terminal step: explode the per-industry taxonomy out to
    // every contact in the run's lists. Writes bucket_assignments +
    // bucket_contact_map with bucket_name='Pending' so the user can see
    // per-contact taxonomy BEFORE clicking Save & Assign. Phase 1b's
    // apply_rollup_bucket_assignments still DELETE+INSERTs both tables —
    // so this preview gets overwritten cleanly when the rollup runs.
    const totalContacts = Number(run.total_contacts || 0);
    ctx.progress({
        phase: 'phase1a',
        step: 'finalize_per_contact',
        current: 0,
        total: totalContacts,
        note: `Writing per-contact taxonomy for ${totalContacts.toLocaleString()} contacts…`
    });
    let perContactStats: {
        total_contacts?: number;
        with_primary_identity?: number;
        with_sub_identity?: number;
        with_sector?: number;
    } = {};
    try {
        const { data: pcRes, error: pcErr } = await supabase.rpc('finalize_per_contact_taxonomy', { p_run_id: runId });
        if (pcErr) throw new Error(pcErr.message);
        perContactStats = (pcRes || {}) as typeof perContactStats;
        const wrote = Number(perContactStats.total_contacts || 0);
        ctx.progress({
            phase: 'phase1a',
            step: 'finalize_per_contact_done',
            current: wrote,
            total: totalContacts || wrote,
            note: `Per-contact taxonomy written — ${wrote.toLocaleString()} contacts, ${(perContactStats.with_primary_identity || 0).toLocaleString()} with identity, ${(perContactStats.with_sub_identity || 0).toLocaleString()} with sub-identity`
        });
        ctx.log(`[Finalize ${runId}] per-contact explosion: ${wrote.toLocaleString()} contacts written — ${(perContactStats.with_primary_identity || 0).toLocaleString()} with identity, ${(perContactStats.with_sub_identity || 0).toLocaleString()} with sub-identity, ${(perContactStats.with_sector || 0).toLocaleString()} with sector`, 'phase');
    } catch (e: any) {
        // Non-fatal: the per-contact explosion is a preview convenience.
        // Phase 1b's rollup will populate per-contact rows regardless.
        ctx.log(`[Finalize ${runId}] per-contact explosion failed (non-fatal — Phase 1b will populate): ${e.message}`, 'warn');
    }

    return {
        candidates: candidates.length,
        rerouted,
        nullified,
        failed,
        costUsd,
        per_contact_total:         perContactStats.total_contacts,
        per_contact_with_identity: perContactStats.with_primary_identity,
        per_contact_with_sub:      perContactStats.with_sub_identity,
        per_contact_with_sector:   perContactStats.with_sector,
    };
}

// ────────────────────────────────────────────────────────────────────
// BUCKET ASSIGNMENT — map every taxonomy-tagged industry to a bucket
//
// User's mental model: taxonomy (identity / sub-identity / sector)
// classifies WHAT the contact is, with no size limits. Buckets are the
// FINAL campaign segments — fewer in number, with min_volume gating
// at this step. They live in bucket_library and grow over time as the
// LLM proposes new ones for industries that don't fit.
//
// This pass takes every Phase 1a-tagged row and asks an LLM:
//   "Given this industry text + (identity, sub-identity, sector),
//    pick the closest bucket from bucket_library, or propose a new
//    bucket (with primary_identity) if nothing fits."
//
// Output: bucket_industry_map gets assigned_bucket_name +
// assigned_bucket_primary_identity + is_new_bucket. The accept/recalc/
// finalize cycle is intentionally identical to the taxonomy flow:
//   • Accept a proposed bucket → create the bucket_library entry
//   • Recalc → re-run, library grows, more orphans get matched
//   • Finalize → re-tag remaining orphans library-only or null
// ────────────────────────────────────────────────────────────────────

interface BucketLibraryEntry {
    id: string;
    bucket_name: string;
    primary_identity: string | null;
    description: string | null;
    include_terms: string[];
    example_strings: string[];
    archived: boolean;
}

async function loadBucketLibrarySnapshot(supabase: SupabaseClient): Promise<BucketLibraryEntry[]> {
    const { data, error } = await supabase
        .from('bucket_library')
        .select('id,bucket_name,primary_identity,description,include_terms,example_strings,archived')
        .eq('archived', false)
        .order('bucket_name');
    if (error) throw new Error(`bucket_library load failed: ${error.message}`);
    return (data || []).map((r: any) => ({
        ...r,
        include_terms: Array.isArray(r.include_terms) ? r.include_terms : [],
        example_strings: Array.isArray(r.example_strings) ? r.example_strings : [],
    })) as BucketLibraryEntry[];
}

interface BucketDecision {
    industry: string;
    bucket_name: string | null;          // null = no library fit AND no new proposal worth making
    primary_identity: string | null;     // required when bucket_name != null
    is_new: boolean;
    confidence: number;                  // 0-10
    reason: string;
}

function buildBucketAssignmentPrompt(
    bucketLibrary: BucketLibraryEntry[],
    snapshot: TaxonomySnapshot
): string {
    // Group library buckets by their primary_identity for the prompt — gives
    // the LLM a clean "buckets under each identity" structure that lines up
    // with how the taxonomy is shaped. Unassigned library entries are listed
    // last under (no parent identity).
    const byIdentity = new Map<string, BucketLibraryEntry[]>();
    for (const b of bucketLibrary) {
        const key = b.primary_identity || '(unassigned)';
        if (!byIdentity.has(key)) byIdentity.set(key, []);
        byIdentity.get(key)!.push(b);
    }
    const libBlock = Array.from(byIdentity.entries()).map(([ident, entries]) => {
        const lines = entries.map(e => {
            const head = `    - "${e.bucket_name}"${e.description ? ` — ${e.description}` : ''}`;
            const inc = e.include_terms && e.include_terms.length
                ? `\n        includes: ${e.include_terms.slice(0, 8).map(t => `"${t}"`).join(', ')}`
                : '';
            const ex = e.example_strings && e.example_strings.length
                ? `\n        examples: ${e.example_strings.slice(0, 3).map(t => `"${t}"`).join(', ')}`
                : '';
            return `${head}${inc}${ex}`;
        }).join('\n');
        return `  Identity: ${ident}\n${lines}`;
    }).join('\n\n');

    const idNames = snapshot.identities.map(i => `"${i.name}"`).join(', ');
    // Identities that already have ≥1 library bucket — for these, the LLM
    // must not propose the identity name itself as a bucket (lazy fallback).
    // Identities with no library buckets yet are exempt: there, the identity
    // name is a reasonable starting bucket.
    const identitiesWithBuckets = Array.from(byIdentity.keys())
        .filter(k => k !== '(unassigned)')
        .map(k => `"${k}"`)
        .join(', ');

    return `You assign B2B contacts to CAMPAIGN BUCKETS. Buckets are coarser than the per-contact taxonomy — they're the actual outreach segments a marketer would run.

For each industry the user provides, you receive the taxonomy tags from the prior pass: identity, sub-identity, sector. Use them to pick the closest bucket from the BUCKET LIBRARY below — or propose a new bucket if nothing in the library fits.

BUCKET LIBRARY (curated by the user — prefer these whenever applicable):

${libBlock || '  (library is empty — every assignment will be a new proposal)'}

VALID PRIMARY IDENTITIES (use one of these for any new bucket proposal):
${idNames || '(none — taxonomy library is empty)'}

RULES:
1. PREFER LIBRARY: if any library bucket is even loosely applicable, pick it (verbatim). The library is the source of truth — only propose new buckets when nothing reasonable fits. Use the includes / examples lines above to judge fit, not just the name.
2. NEW PROPOSALS: when proposing a new bucket, set is_new=true. The bucket_name should be marketer-friendly (e.g. "FinTech / Financial Software", "Renewable Energy Services" — not too narrow, not generic). Required fields: bucket_name AND primary_identity (must be one from the VALID PRIMARY IDENTITIES list).
3. NO IDENTITY-NAME PROPOSALS: the following identities already have library buckets listed above — for these, bucket_name MUST NOT equal the identity name verbatim. Pick the closest library bucket or propose a more specific name (e.g. under "Agency", propose "B2B Lead Gen Agencies", not just "Agency"). Identities not in this list have no library buckets yet, so the identity name is acceptable as a starting bucket.
   Identities requiring sub-specific names: ${identitiesWithBuckets || '(none)'}
4. NULL: if the contact's taxonomy is too vague to bucket meaningfully (e.g. identity=null, or generic catch-all), set bucket_name=null. The contact will route to General.
5. CONSISTENCY: when two industries share the same identity + sub-identity, they should usually get the same bucket. Don't fork unless the difference is meaningful for outreach.
6. Confidence 1-10: 10 = obvious fit, 7 = good fit, 5 = forced match, 3 = stretch. Use the score honestly.

OUTPUT (strict JSON, key MUST be "results"):
{
  "results": [
    {
      "id": <integer matching the input>,
      "bucket_name": "<library entry verbatim, OR new bucket name, OR null>",
      "primary_identity": "<identity from VALID list>",
      "is_new": <true | false>,
      "confidence": <1-10>,
      "reason": "<one short sentence — why this bucket>"
    },
    ...
  ]
}

Do not include any other fields. One result per input.`;
}

const BUCKET_BATCH_SIZE = 10;
const BUCKET_CONCURRENCY = 30;

export async function runBucketAssignment(
    supabase: SupabaseClient,
    runId: string,
    ctx: BucketingCtx
): Promise<{
    candidates: number;
    matched: number;
    proposed: number;
    nullified: number;
    failed: number;
    costUsd: number;
}> {
    // Phase 1b v2: there is no separate "Bucket Assignment" LLM step anymore —
    // bucket name is derived deterministically from taxonomy + volume rollup
    // by apply_rollup_bucket_assignments. This endpoint stays as an alias for
    // backwards-compat with the existing UI button; it just runs the rollup
    // and returns counts shaped to match the v1 response contract.

    const { data: run, error: runErr } = await supabase
        .from('bucketing_runs').select('*').eq('id', runId).single();
    if (runErr || !run) throw new Error(`Run not found: ${runErr?.message}`);
    if (run.status !== 'taxonomy_ready' && run.status !== 'completed' && run.status !== 'assigning') {
        throw new Error(`Cannot assign buckets from status "${run.status}" — run must be in taxonomy_ready, assigning, or completed`);
    }

    // Two independent thresholds. min_volume is the sub-identity floor
    // (when (identity, sub) drops below this, roll up to identity-only).
    // identity_min_volume is the identity floor (when an identity drops
    // below this, fold it into General). Default identity_min_volume=1
    // means every non-empty identity gets its own bucket — the original
    // single-threshold behavior at sub_min=1.
    const subMin = Number(run.min_volume) > 0 ? Number(run.min_volume) : 1;
    const idMin  = Number(run.identity_min_volume) > 0 ? Number(run.identity_min_volume) : 1;

    ctx.log(`[BucketAssign ${runId}] v2 deterministic rollup, sub_min=${subMin}, identity_min=${idMin}`, 'phase');
    const t0 = Date.now();
    const { data: result, error: rpcErr } = await supabase.rpc('apply_rollup_bucket_assignments', {
        p_run_id: runId,
        p_sub_min_volume: subMin,
        p_identity_min_volume: idMin
    });
    const ms = Date.now() - t0;
    if (rpcErr) {
        ctx.log(`[BucketAssign ${runId}] rollup RPC failed after ${ms}ms: ${rpcErr.message}`, 'error');
        throw new Error(`rollup failed: ${rpcErr.message}`);
    }

    const r = (result || {}) as {
        total_contacts?: number;
        at_sub_identity?: number;
        rolled_up_to_identity?: number;
        general?: number;
        disqualified?: number;
    };
    const total = r.total_contacts || 0;
    const general = r.general || 0;
    const disqualified = r.disqualified || 0;
    const matchedAtSubOrIdentity = (r.at_sub_identity || 0) + (r.rolled_up_to_identity || 0);

    ctx.log(
        `[BucketAssign ${runId}] done in ${(ms / 1000).toFixed(2)}s — ` +
        `${total.toLocaleString()} contacts: ` +
        `${(r.at_sub_identity || 0).toLocaleString()} sub-identity buckets, ` +
        `${(r.rolled_up_to_identity || 0).toLocaleString()} rolled up to identity, ` +
        `${general.toLocaleString()} → General, ` +
        `${disqualified.toLocaleString()} → Disqualified`,
        'phase'
    );

    return {
        candidates: total,
        matched: matchedAtSubOrIdentity,
        proposed: 0,                 // v2 never proposes new buckets — they ARE taxonomy
        nullified: general,          // legacy field name; conceptually "routed to General"
        failed: 0,
        costUsd: 0,
    };
}

// ────────────────────────────────────────────────────────────────────
// PHASE 1A HELPERS — taxonomy snapshot + per-string tagging
// ────────────────────────────────────────────────────────────────────

export async function loadTaxonomySnapshotForDebug(supabase: SupabaseClient): Promise<TaxonomySnapshot> {
    return loadTaxonomySnapshot(supabase);
}

// Single-industry pressure test. Routes through the same prompt + parser
// + dedup pass as production tagIndustries, then returns the raw LLM
// output alongside the parsed result so a regression in either layer
// shows up immediately. Not used by the bucketing pipeline itself.
export async function debugTagSingleIndustry(
    supabase: SupabaseClient,
    industry: string,
    sampleCompanies: string[],
    model: string,
    ctx: BucketingCtx
): Promise<{
    raw_response: string;
    parsed: IndustryTagging | null;
    snapshot: { identities: number; sub_identities: number; sectors: number };
    prompt_chars: number;
    model_used: string;
    cost_usd: number;
}> {
    const snapshot = await loadTaxonomySnapshot(supabase);
    const isAnthropic = model.startsWith('claude-');
    const isOpenAI = model.startsWith('gpt-');
    if (!isAnthropic && !isOpenAI) throw new Error(`Unsupported model: ${model}`);

    const anthropic = isAnthropic ? await getAnthropic(supabase) : null;
    if (isAnthropic && !anthropic) throw new Error('Anthropic API key not configured.');

    const systemPrompt = buildTaggingSystemPrompt(snapshot);
    const vocab: VocabRow[] = [{
        industry,
        n: 1,
        enrichment_status: 'completed',
        sample_companies: sampleCompanies.slice(0, 2)
    }];
    const userPrompt = JSON.stringify({
        industries: vocab.map((v, i) => ({ id: i, industry: v.industry, sample_companies: v.sample_companies || [] }))
    });

    let raw = '';
    let cost = 0;
    if (isAnthropic) {
        const resp = await anthropic!.messages.create({
            model,
            max_tokens: 2000,
            system: [{ type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }],
            messages: [{ role: 'user', content: userPrompt }]
        }, { signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal) });
        raw = (resp.content as any[]).filter(b => b.type === 'text').map(b => b.text).join('\n');
        const usage: any = (resp as any).usage || {};
        cost = computeAnthropicCost(model, usage.input_tokens || 0, usage.output_tokens || 0);
    } else {
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
            signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal)
        });
        if (!resp.ok) throw new Error(`OpenAI ${resp.status}: ${(await resp.text()).slice(0, 300)}`);
        const json: any = await resp.json();
        raw = json.choices?.[0]?.message?.content || '';
        const usage = json.usage || {};
        const cached = usage.prompt_tokens_details?.cached_tokens || 0;
        cost = computeOpenAICost(model, (usage.prompt_tokens || 0) - cached, cached, usage.completion_tokens || 0);
    }

    const parsed = snapTaggingsToLibrary(parseTaggingJson(raw, vocab), snapshot);
    return {
        raw_response: raw,
        parsed: parsed[0] || null,
        snapshot: {
            identities: snapshot.identities.length,
            sub_identities: snapshot.sub_identities.length,
            sectors: snapshot.sectors.length
        },
        prompt_chars: systemPrompt.length + userPrompt.length,
        model_used: model,
        cost_usd: cost
    };
}

// Eval helper — runs a single classification through the real Phase 1b path
// (classifyContactBatch + snap) using the live taxonomy as leaves. Used by
// scripts/eval/run-eval-phase1b.ts. Not used in production.
export async function evalClassifyContact(
    supabase: SupabaseClient,
    classification: string
): Promise<{
    primary_identity: string | null;
    sub_identity: string | null;
    sector: string | null;
    is_disqualified: boolean;
    identity_score: number;
    sub_identity_score: number;
    cost_usd: number;
    raw_response: string;
}> {
    const snapshot = await loadTaxonomySnapshot(supabase);
    // Match production dispatch: only resolve Anthropic when MATCH_MODEL is claude-*.
    const useAnthropic = MATCH_MODEL.startsWith('claude-');
    const anthropic = useAnthropic ? await getAnthropic(supabase) : null;
    if (useAnthropic && !anthropic) throw new Error('Anthropic API key not configured.');

    // Build leaves from the live taxonomy — one per sub-identity. Phase 1b
    // production builds leaves from per-run Phase 1a results, but for eval we
    // surface the full library so the LLM has the same choices as Phase 1a.
    const leaves: DiscoveredBucket[] = snapshot.sub_identities.map(s => ({
        sub_identity: s.name,
        primary_identity: s.parent_identity || '',
        description: s.description || '',
        identity_type: 'other',
        operator_required: false,
        priority_rank: 5,
    }));
    const validSpecNames = new Set(leaves.map(l => l.sub_identity));
    const validIdentityNames = new Set(snapshot.identities.map(i => i.name));
    const sectorVocab = snapshot.sectors.map(s => s.name);

    const refByIdentity: Record<string, any[]> = {};
    for (const l of leaves) {
        if (!refByIdentity[l.primary_identity]) refByIdentity[l.primary_identity] = [];
        refByIdentity[l.primary_identity].push({
            sub_identity: l.sub_identity,
            description: l.description,
            identity_type: l.identity_type,
            operator_required: l.operator_required,
            priority_rank: l.priority_rank,
        });
    }
    const bucketReferenceJson = JSON.stringify(refByIdentity);

    const fakeBatch: ContactRouteInput[] = [{
        contact_id: 'eval',
        company_name: null,
        company_website: null,
        industry: null,
        lead_list_name: null,
        enrichment_status: 'completed',
        classification,
        confidence: 0.9,
        reasoning: null,
        error_message: null,
    }];

    const { results, costUsd } = await classifyContactBatch(
        fakeBatch, bucketReferenceJson, sectorVocab, validSpecNames, validIdentityNames,
        anthropic, undefined
    );
    const r = results[0] || makeFallbackChain();
    return {
        primary_identity: r.primary_identity.name || null,
        sub_identity: r.sub_identity.name || null,
        sector: (r.sector || '').trim() || null,
        is_disqualified: !!r.disqualified,
        identity_score: r.primary_identity.score || 0,
        sub_identity_score: r.sub_identity.score || 0,
        cost_usd: costUsd,
        raw_response: JSON.stringify(r),
    };
}

async function loadTaxonomySnapshot(supabase: SupabaseClient): Promise<TaxonomySnapshot> {
    const [idRes, chRes, secRes] = await Promise.all([
        supabase.from('taxonomy_identities').select('*').eq('archived', false).order('sort_order'),
        supabase.from('taxonomy_sub_identities').select('*').eq('archived', false).order('sort_order'),
        supabase.from('taxonomy_sectors').select('*').eq('archived', false).order('sort_order')
    ]);
    if (idRes.error) throw new Error(`taxonomy_identities load failed: ${idRes.error.message}`);
    if (chRes.error) throw new Error(`taxonomy_sub_identities load failed: ${chRes.error.message}`);
    if (secRes.error) throw new Error(`taxonomy_sectors load failed: ${secRes.error.message}`);
    return {
        identities: (idRes.data || []) as TaxonomyEntry[],
        sub_identities: (chRes.data || []) as TaxonomyEntry[],
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

    // Telemetry: track 429s and per-batch latency so we can detect when we
    // hit OpenAI/Anthropic rate-limits. Logged every ~60s while the run is
    // active so the user can see if concurrency=300 is over-saturating.
    let rateLimitHits = 0;
    let lastRateLimitAt = 0;
    let batchLatenciesMs: number[] = [];   // ring buffer kept ≤ 200 entries
    const RATE_LIMIT_LOG_INTERVAL_MS = 60_000;
    let lastTelemetryLogAt = Date.now();
    const recordLatency = (ms: number) => {
        batchLatenciesMs.push(ms);
        if (batchLatenciesMs.length > 200) batchLatenciesMs.shift();
    };
    const isRateLimitError = (err: any): boolean => {
        if (!err) return false;
        const msg = (err.message || String(err)).toLowerCase();
        if (err.status === 429 || err.statusCode === 429) return true;
        return msg.includes('rate limit') || msg.includes('too many requests') ||
               msg.includes('rate_limit_exceeded') || msg.includes('429');
    };
    const maybeLogTelemetry = () => {
        const now = Date.now();
        if (now - lastTelemetryLogAt < RATE_LIMIT_LOG_INTERVAL_MS) return;
        lastTelemetryLogAt = now;
        if (batchLatenciesMs.length === 0) return;
        const sorted = [...batchLatenciesMs].sort((a, b) => a - b);
        const p50 = sorted[Math.floor(sorted.length * 0.5)];
        const p95 = sorted[Math.floor(sorted.length * 0.95)];
        const p99 = sorted[Math.floor(sorted.length * 0.99)];
        const avg = Math.round(sorted.reduce((s, n) => s + n, 0) / sorted.length);
        const rateLimited = rateLimitHits > 0 ? ` · 429 hits=${rateLimitHits}` : '';
        ctx.log(
            `[Bucketing ${runId}] tagger telemetry — batches=${batchesAttempted} done=${done}/${vocab.length}  ` +
            `latency p50=${p50}ms p95=${p95}ms p99=${p99}ms avg=${avg}ms${rateLimited}`
        );
        if (rateLimitHits > 0 && (now - lastRateLimitAt) < RATE_LIMIT_LOG_INTERVAL_MS) {
            ctx.log(
                `[Bucketing ${runId}] ⚠ provider rate-limited ${rateLimitHits}× in the last minute. ` +
                `Consider lowering PHASE1A_CONCURRENCY (currently ${PHASE1A_CONCURRENCY}) if hits keep accumulating.`,
                'warn'
            );
        }
    };

    // In-run proposal reuse. Earlier we used wave boundaries (run 40 in
    // parallel, wait for ALL, gather proposals, start next 40) to give the
    // next wave's prompt a clean snapshot of prior coinings. That fence cost
    // 5-6× throughput because each wave waited for the slowest of 40 LLM
    // calls — a single slow OpenAI response stalled the whole wave.
    //
    // Instead: keep a continuous pool of PHASE1A_CONCURRENCY batches in flight
    // via pLimit. Each batch, the moment it starts, snapshots the running
    // proposal maps as they are *now* — so every batch sees every coining
    // that completed before its own LLM call started. Some overlap will still
    // produce two near-identical names independently coined at the same time;
    // the existing post-pass consolidation (consolidateTaggings +
    // consolidateSubIdentitiesViaLLM + consolidateSectorsViaLLM +
    // consolidateIdentitiesViaLLM) collapses those.
    //
    // Cap per-layer proposals shown in the prompt so token usage doesn't
    // blow up on 100k+ vocabs: ranked by frequency so most-reused names
    // persist in the prompt.
    const inRunIdentities = new Map<string, TaxonomyEntry & { _usage: number }>();
    const inRunSubIdentities = new Map<string, TaxonomyEntry & { _usage: number }>();
    const inRunSectors = new Map<string, TaxonomyEntry & { _usage: number }>();
    const originalIdNames = new Set(snapshot.identities.map(i => i.name));
    const originalSubNames = new Set(snapshot.sub_identities.map(i => i.name));
    const originalSecNames = new Set(snapshot.sectors.map(i => i.name));
    // Parent-aware sub lookup. A sub is "originally established" only when
    // it is paired with its library parent; cross-parent pairs are treated
    // as new proposals so the Review screen surfaces them.
    const originalSubParent = new Map<string, string>();
    for (const s of snapshot.sub_identities) originalSubParent.set(s.name, s.parent_identity || '');
    const PROPOSALS_PER_LAYER_CAP = 80;

    const topByUsage = <T extends { _usage: number }>(m: Map<string, T>): T[] =>
        Array.from(m.values()).sort((a, b) => b._usage - a._usage).slice(0, PROPOSALS_PER_LAYER_CAP);

    // Per-batch snapshot — captures whatever proposals were published by
    // batches that completed before this one started. Still used by
    // snapTaggingsToLibrary so in-run proposal names snap to "library hit"
    // (is_new=false) for the second batch onward.
    const buildEffectiveSnapshot = (): TaxonomySnapshot => ({
        identities: [...snapshot.identities, ...topByUsage(inRunIdentities)],
        sub_identities: [...snapshot.sub_identities, ...topByUsage(inRunSubIdentities)],
        sectors: [...snapshot.sectors, ...topByUsage(inRunSectors)],
    });

    // CACHE OPTIMIZATION. The system prompt is split into two parts:
    //   1. stableSystemPrompt — built ONCE per run from the library snapshot
    //      captured at run start. Identical across every batch in this run,
    //      so it serves as the cache prefix (Anthropic prompt cache via
    //      cache_control / OpenAI automatic prefix cache).
    //   2. proposalAppendix — built PER BATCH from the in-run proposal pool.
    //      Differs as proposals accumulate, so it's NOT cached. Sits after
    //      the cache breakpoint so it doesn't invalidate the prefix.
    // On a 3000-batch run, this should hit the cache on every batch after
    // the first (Anthropic) and effectively all batches (OpenAI prefix cache
    // matches as long as the static prefix is identical).
    const stableSystemPrompt = buildTaggingSystemPrompt(snapshot);

    let batchesSinceGc = 0;

    await Promise.all(batches.map((batch) => limit(async () => {
        await ctx.checkCancel();
        batchesAttempted++;
        // Snapshot proposals at batch-start time. Mutations to the in-run
        // maps by other batches between now and when this batch's LLM call
        // returns are intentional — the harvest step at the end of this
        // batch publishes our own coinings for the next batch to see.
        const effective = buildEffectiveSnapshot();
        const proposalAppendix = buildInRunProposalAppendix({
            identities:     topByUsage(inRunIdentities),
            sub_identities: topByUsage(inRunSubIdentities),
            sectors:        topByUsage(inRunSectors),
        });
        const userPrompt = JSON.stringify({
            industries: batch.map((v, i) => ({
                id: i,
                industry: v.industry,
                sample_companies: v.sample_companies?.slice(0, 2) || []
            }))
        });
        const callStart = Date.now();
        try {
            let text = '';
            if (isAnthropic) {
                // Two system blocks: stable prefix is cached, appendix isn't.
                // When the appendix is empty (first batch) the stable block is
                // the entire system; the cache write still happens on this call
                // and every subsequent batch can read from it.
                const systemBlocks: any[] = [
                    { type: 'text', text: stableSystemPrompt, cache_control: { type: 'ephemeral' } }
                ];
                if (proposalAppendix) {
                    systemBlocks.push({ type: 'text', text: proposalAppendix });
                }
                const resp = await anthropic!.messages.create({
                    model,
                    max_tokens: 4000,
                    system: systemBlocks,
                    messages: [{ role: 'user', content: userPrompt }]
                }, { signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal) });
                const usage: any = (resp as any).usage || {};
                totalIn += usage.input_tokens || 0;
                totalOut += usage.output_tokens || 0;
                totalCachedIn += usage.cache_read_input_tokens || 0;
                text = (resp.content as any[])
                    .filter(b => b.type === 'text').map(b => b.text).join('\n');
            } else {
                // OpenAI chat completions with json_object response_format
                // forces valid JSON without us hand-rolling a schema.
                // OpenAI's automatic prefix cache matches on the longest
                // shared prefix across requests, so concatenating stable +
                // appendix (in that order) keeps every batch's prefix bytes
                // identical to the previous one's.
                const systemPrompt = proposalAppendix
                    ? `${stableSystemPrompt}${proposalAppendix}`
                    : stableSystemPrompt;
                const resp = await fetch('https://api.openai.com/v1/chat/completions', {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${getOpenAIKey()}`, 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        model,
                        max_tokens: 4000,
                        response_format: { type: 'json_object' },
                        messages: [
                            { role: 'system', content: systemPrompt },
                            { role: 'user', content: userPrompt }
                        ]
                    }),
                    signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal)
                });
                if (!resp.ok) {
                    const body = await resp.text();
                    if (resp.status === 429) {
                        rateLimitHits++;
                        lastRateLimitAt = Date.now();
                    }
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
            recordLatency(Date.now() - callStart);
            maybeLogTelemetry();
            const parsedRaw = parseTaggingJson(text, batch);
            // Snap against the effective snapshot (library + in-run proposals
            // visible at batch start). Library matches stay is_new=false.
            // Matches to in-run proposals are still proposals — re-flag
            // is_new=true so the AI-Proposed panel surfaces them.
            const parsed = snapTaggingsToLibrary(parsedRaw, effective).map(t => {
                if (t.identity && !originalIdNames.has(t.identity)) t.is_new_identity = true;
                // sub is "originally library" only when its library parent
                // matches the chosen identity. Cross-parent pairs surface as
                // proposals so the user can reroute on the Review screen.
                if (t.sub_identity) {
                    const libParent = originalSubParent.get(t.sub_identity);
                    const libMatch = !!libParent && (!t.identity || libParent === t.identity);
                    if (!libMatch) t.is_new_sub_identity = true;
                }
                if (t.sector && !originalSecNames.has(t.sector)) t.is_new_sector = true;
                return t;
            });
            if (parsed.length === 0) {
                batchesFailed++;
                if (!firstError) firstError = `LLM returned a valid JSON shape but no parseable taggings. Sample: ${text.slice(0, 200)}`;
                ctx.log(`[Bucketing ${runId}] tagging batch parsed to 0 results (${batch.length} industries) — output: ${text.slice(0, 200)}`, 'warn');
                for (const v of batch) {
                    taggings.push({
                        industry: v.industry,
                        identity: null,
                        is_new_identity: false,
                        sub_identity: null,
                        is_new_sub_identity: false,
                        sector: null,
                        is_new_sector: false,
                        is_disqualified: false,
                        identity_confidence: 0,
                        sub_identity_confidence: 0,
                        sector_confidence: 0,
                        confidence: 0,
                        reason: `parse_empty: ${text.slice(0, 100)}`
                    });
                }
            } else {
                for (const t of parsed) taggings.push(t);
                // Publish this batch's new coinings into the running maps
                // immediately so the NEXT batch to start sees them.
                for (const t of parsed) {
                    if (t.is_new_identity && t.identity) {
                        const cur = inRunIdentities.get(t.identity);
                        if (cur) cur._usage++;
                        else inRunIdentities.set(t.identity, {
                            id: '', name: t.identity, archived: false, _usage: 1
                        });
                    }
                    if (t.is_new_sub_identity && t.sub_identity && t.identity) {
                        const cur = inRunSubIdentities.get(t.sub_identity);
                        if (cur) cur._usage++;
                        else inRunSubIdentities.set(t.sub_identity, {
                            id: '', name: t.sub_identity, parent_identity: t.identity, archived: false, _usage: 1
                        });
                    }
                    if (t.is_new_sector && t.sector) {
                        const cur = inRunSectors.get(t.sector);
                        if (cur) cur._usage++;
                        else inRunSectors.set(t.sector, {
                            id: '', name: t.sector, archived: false, _usage: 1
                        });
                    }
                }
                // Checkpoint to bucket_industry_map immediately. If the
                // process dies (cancel, OOM, server restart), Resume reads
                // these rows back and skips them — paid LLM work is never
                // lost. The end-of-tagging upsert later overwrites these
                // with consolidated names (same PK), so this is just a
                // crash-safety draft.
                //
                // FIRE-AND-FORGET: don't await — at concurrency=300, awaiting
                // would queue ~300 simultaneous upserts on Supabase's pooler
                // and block the concurrency slot. The .then() handler logs
                // errors so we notice if writes start failing. Risk: a crash
                // in the ~50-200ms window between LLM-returns and checkpoint-
                // lands loses at most ~PHASE1A_CONCURRENCY × PHASE1A_BATCH_SIZE
                // (~6000) industries' worth of work, which the resume filter
                // would re-tag on the next run start.
                {
                    const checkpoint = parsed.map(t => ({
                        bucketing_run_id: runId,
                        industry_string: t.industry,
                        raw_industry: t.industry,
                        // Pre-rollup placeholder. Apply_rollup_bucket_assignments
                        // sets the final bucket_name; this just satisfies
                        // the NOT NULL constraint until the post-pass
                        // upsert writes the proper pre-rollup value.
                        bucket_name: t.is_disqualified ? RESERVED_DISQUALIFIED : RESERVED_GENERAL,
                        source: 'llm_phase1a',
                        primary_identity: t.identity,
                        sub_identity: t.sub_identity,
                        sector: t.sector,
                        is_new_identity: !!t.is_new_identity,
                        is_new_sub_identity: !!t.is_new_sub_identity,
                        is_new_sector: !!t.is_new_sector,
                        is_disqualified: !!t.is_disqualified,
                        is_generic: false,
                        needs_qa: false,
                        identity_confidence: t.identity_confidence ? Number(((t.identity_confidence) / 10).toFixed(2)) : null,
                        sub_identity_confidence: t.sub_identity_confidence ? Number(((t.sub_identity_confidence) / 10).toFixed(2)) : null,
                        sector_confidence: t.sector_confidence ? Number(((t.sector_confidence) / 10).toFixed(2)) : null,
                        confidence: t.confidence ? Number(((t.confidence) / 10).toFixed(2)) : null,
                        llm_reason: (t.reason || '').slice(0, 500) || null,
                        canonical_classification: t.sub_identity || t.identity || 'Generic',
                    }));
                    supabase.from('bucket_industry_map')
                        .upsert(checkpoint, { onConflict: 'bucketing_run_id,industry_string' })
                        .then(({ error: ckErr }) => {
                            if (ckErr) {
                                // Non-fatal: the end-of-tagging upsert will catch
                                // up if the process survives. Log so we notice
                                // if writes start failing silently.
                                ctx.log(`[Bucketing ${runId}] checkpoint write failed (${parsed.length} rows): ${ckErr.message}`, 'warn');
                            }
                        }, (ckErr: any) => {
                            ctx.log(`[Bucketing ${runId}] checkpoint exception: ${ckErr.message}`, 'warn');
                        });
                }
            }
        } catch (err: any) {
            batchesFailed++;
            recordLatency(Date.now() - callStart);
            if (isRateLimitError(err)) {
                rateLimitHits++;
                lastRateLimitAt = Date.now();
            }
            maybeLogTelemetry();
            if (!firstError) firstError = err.message || String(err);
            ctx.log(`[Bucketing ${runId}] tagging batch error (${batch.length} industries): ${err.message}`, 'error');
            // Fail-open: emit needs_qa rows so we don't lose contacts.
            for (const v of batch) {
                taggings.push({
                    industry: v.industry,
                    identity: null,
                    is_new_identity: false,
                    sub_identity: null,
                    is_new_sub_identity: false,
                    sector: null,
                    is_new_sector: false,
                    is_disqualified: false,
                    identity_confidence: 0,
                    sub_identity_confidence: 0,
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
        // Periodic GC hint — drop prompt + response buffers V8 hasn't
        // compacted yet. Every PHASE1A_CONCURRENCY batches ≈ once per
        // "wave-equivalent". --expose-gc gates this; no-op if unavailable.
        if (++batchesSinceGc >= PHASE1A_CONCURRENCY) {
            batchesSinceGc = 0;
            if (typeof global.gc === 'function') global.gc();
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

    // Anthropic's input_tokens is uncached only (per SDK type docs). Bill the
    // three input components at their respective rates instead of subtracting
    // cached from input — that subtraction over-discounts and can go negative
    // when cache reads dominate.
    const costUsd = isAnthropic
        ? computeAnthropicCost(model, totalIn, totalOut)
            + (totalCachedIn / 1_000_000) * (ANTHROPIC_PRICING[model]?.input || 3) * 0.1
        : computeOpenAICost(model, totalIn - totalCachedIn, totalCachedIn, totalOut);
    return { taggings, costUsd, modelUsed: model };
}

// Render a compact appendix listing taxonomy values that were proposed
// earlier in this same run (not yet in the library, but already coined by
// previous batches). Lives at the END of the system prompt so the immutable
// library-prefix portion can be served from the provider prompt cache
// (Anthropic cache_control / OpenAI automatic prefix cache). Returns an
// empty string when no in-run proposals exist, in which case the appendix
// is omitted entirely.
function buildInRunProposalAppendix(proposals: {
    identities: { name: string }[];
    sub_identities: { name: string; parent_identity?: string }[];
    sectors: { name: string }[];
}): string {
    const haveAny = proposals.identities.length + proposals.sub_identities.length + proposals.sectors.length > 0;
    if (!haveAny) return '';

    const idLine = proposals.identities.length > 0
        ? `Additional valid identities (in addition to VALID_IDENTITIES above): ${
            JSON.stringify(proposals.identities.map(i => i.name))
          }`
        : '';

    const subByParent: Record<string, string[]> = {};
    for (const s of proposals.sub_identities) {
        const p = s.parent_identity || '(unknown)';
        if (!subByParent[p]) subByParent[p] = [];
        subByParent[p].push(s.name);
    }
    const subLines = Object.keys(subByParent).length > 0
        ? `Additional valid sub-identities (extend the parent's children list):\n${
            Object.entries(subByParent)
                .map(([p, names]) => `  ${JSON.stringify(p)}: ${JSON.stringify(names)}`)
                .join(',\n')
          }`
        : '';

    const secLine = proposals.sectors.length > 0
        ? `Additional valid sectors: ${JSON.stringify(proposals.sectors.map(s => s.name))}`
        : '';

    const body = [idLine, subLines, secLine].filter(Boolean).join('\n\n');

    return `

═══════════════════════════════════════════════════════════════════════════
IN-RUN PROPOSALS (extend the menus above for this run only)
═══════════════════════════════════════════════════════════════════════════

The following names were proposed earlier in this same run. Treat them as
valid values that extend the VALID_* menus above. If your tag matches one of
these names EXACTLY, set the corresponding is_new_* flag to FALSE — the
proposal has already been coined this run, so it's not novel anymore.

${body}`;
}

function buildTaggingSystemPrompt(s: TaxonomySnapshot): string {
    return `You tag B2B contact industries for outreach segmentation.

For each input industry, return three independent tags plus per-tag confidence:
- identity (REQUIRED)  — what kind of company is it at its core?
- sub_identity         — the functional sub-type within that identity (optional)
- sector               — the vertical the company SERVES, only when explicit (optional)

${EXACT_SPELLING_RULE}

${renderLibraryMenu(s)}

${HARD_KEYWORD_ROUTING}

${CORE_PRINCIPLES}

${DISQUALIFICATION_RULES}

═══════════════════════════════════════════════════════════════════════════
CONFIDENCE SCORING (per tag, integer 1-10) — gates the value
═══════════════════════════════════════════════════════════════════════════

Return THREE independent scores: identity_confidence, sub_identity_confidence,
sector_confidence. Score each tag on its OWN — high identity confidence does
not depend on sub or sector being set.

  9-10  explicit, unambiguous (rule triggers, exact name in the input)
  7-8   strong inference from clear context
  5-6   reasonable inference — you would defend the pick under review
   ≤ 4  below the threshold — NULL this tag and set its confidence to 1

The 5/4 line is the gate. Confidence ≥ 5 → keep the value. Confidence ≤ 4 →
null the value. This is PER-TAG:
  - identity_confidence=8, sub_identity_confidence=3 → identity set, sub=null.
    Identity-without-sub is the normal outcome when no library sub fits.
  - identity_confidence=3, sub_identity_confidence anything → both null. Sector
    is still independently set if its own confidence ≥ 5.
  - All three confidences ≥ 5 → all three set.

If you set a tag to null, its confidence MUST be 1. Always include a one-line
"reason" explaining the identity choice (or why identity is null).

═══════════════════════════════════════════════════════════════════════════
NEW-VALUE PROPOSALS
═══════════════════════════════════════════════════════════════════════════

Set is_new_*=true ONLY when no library entry is even loosely applicable AND the
proposed value would apply to multiple companies (not a one-off). When proposing
new values, avoid:
  - one-off niche names
  - rewordings of existing entries (e.g. "Wealth Management Firm" when "Wealth
    Management" already exists in the library)
  - names that mix in served verticals (those belong in sector)
  - plural/singular variants of existing names

═══════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT — return ONLY valid JSON, no prose, no markdown
═══════════════════════════════════════════════════════════════════════════

The top-level object MUST use the key "results". Each item's "id" MUST match
the input "id" (integer). Return one item per input industry, same order:

{
  "results": [
    {
      "id": <integer matching input id>,
      "identity": "<exact library name or proposed new>" | null,
      "is_new_identity": <bool>,
      "sub_identity": "<exact library name or proposed new>" | null,
      "is_new_sub_identity": <bool>,
      "sector": "<exact library name or proposed new>" | null,
      "is_new_sector": <bool>,
      "is_disqualified": <bool>,
      "identity_confidence": <1-10 integer>,
      "sub_identity_confidence": <1-10 integer>,
      "sector_confidence": <1-10 integer>,
      "reason": "<brief justification, <= 30 words>"
    }
  ]
}

═══════════════════════════════════════════════════════════════════════════
LIBRARIES (richer reference — VALID_* above is the source of truth for names)
═══════════════════════════════════════════════════════════════════════════

${renderLibraryReference(s)}

═══════════════════════════════════════════════════════════════════════════
WORKED EXAMPLES (illustrative — always pick from the VALID_* lists above)
═══════════════════════════════════════════════════════════════════════════

─── Basic identity recognition ────────────────────────────────────────

Input:  Digital marketing agency
Output: identity=Agency, sub_identity=Performance Marketing Agency, sector=null

Input:  SEO agency for healthcare clinics
Output: identity=Agency, sub_identity=Performance Marketing Agency, sector=Healthcare
Note:   "X for Y" — Y is the served vertical, becomes the sector.

Input:  Private equity firm focused on lower-middle market buyouts
Output: identity=Financial Services, sub_identity=Private Equity, sector=null

Input:  Investment bank focused on healthcare M&A
Output: identity=Financial Services, sub_identity=Investment Banking, sector=Healthcare

Input:  Community bank serving small businesses
Output: identity=Financial Services, sub_identity=Banking & Lending, sector=null
Note:   banks / credit unions / lenders / mortgage brokers all collapse into
        the single combined sub "Banking & Lending".

Input:  Custom software development shop building bespoke apps
Output: identity=IT Services, sub_identity=Custom Software Development, sector=null
Note:   service-delivered, NOT a SaaS product. Parent of Custom Software
        Development is IT Services, not Software & SaaS.

Input:  Software platform for K-12 schools
Output: identity=Software & SaaS, sub_identity=null, sector=Education
Note:   no specific SaaS sub (CRM/Sales/Cybersecurity/FinTech/HR/MarTech)
        fits → sub stays null. Identity-without-sub is fine.

Input:  Venture-backed fintech startup automating B2B payments
Output: identity=Software & SaaS, sub_identity=FinTech SaaS, sector=null
Note:   "fintech startup" → Software & SaaS (X-tech rule 1b). "Venture-
        backed" is a financing status, NOT Financial Services.

─── Identity-without-sub: the common case the model used to over-null ───

Input:  Managed IT Services provider for small and mid-sized businesses
Output: identity=IT Services, sub_identity=null, sector=null
Note:   MSPs are the DEFAULT flavor of IT Services — no dedicated sub. Set
        identity, leave sub null. Only set a sub when the input clearly says
        Cybersecurity Services, Custom Software Development, Data Migration
        Services, or IT Asset Disposition.

Input:  Managed IT Services and Office Technology Solutions Provider for SMBs
Output: identity=IT Services, sub_identity=null, sector=null
Note:   "Managed IT" triggers rule 12. The "Office Technology" detail does
        NOT downgrade your confidence — keep identity=IT Services with
        identity_confidence ≥ 7. Identity-confidence is independent of
        sub-confidence.

Input:  Managed IT Services for Automotive Dealerships including DMS Hosting and Network Solutions
Output: identity=IT Services, sub_identity=null, sector=Retail
Note:   "Managed IT" fixes identity. Auto dealerships are the served
        vertical → sector=Retail. Identity, sub, and sector are scored
        independently.

Input:  Domain registration and web hosting services
Output: identity=IT Services, sub_identity=null, sector=null
Note:   hosting / domain / cloud / colocation → IT Services (rule 12). No
        sub fits → sub=null. Do NOT return identity=null.

Input:  Web Design, Web Development, and IT Support Services for SMBs
Output: identity=IT Services, sub_identity=null, sector=null
Note:   web design + web dev + IT support are all IT Services work. No
        single sub captures all three → sub=null.

Input:  Boutique IT consulting and managed IT support for SMBs
Output: identity=Consulting & Advisory, sub_identity=IT Consulting, sector=null
Note:   first-mentioned-wins (rule 11d): "IT consulting" appears first →
        Consulting & Advisory > IT Consulting.

Input:  IT consulting and SAP solutions provider with application development and test automation services
Output: identity=Consulting & Advisory, sub_identity=IT Consulting, sector=null
Note:   "IT consulting" first → Consulting & Advisory > IT Consulting. The
        dev/test work is secondary.

Input:  B2B software development and consulting for ecommerce businesses
Output: identity=IT Services, sub_identity=Custom Software Development, sector=Retail
Note:   first-mentioned-wins: "software development" first → IT Services.
        Ecommerce is the served vertical → sector=Retail.

Input:  Strategy consulting and custom development for SaaS founders
Output: identity=Consulting & Advisory, sub_identity=Management Consulting, sector=null
Note:   first-mentioned-wins: "strategy consulting" first → Consulting &
        Advisory. "SaaS founders" is an audience, not a served vertical.

Input:  Consumer Insights and Market Research Services for Product Development
Output: identity=Consulting & Advisory, sub_identity=Management Consulting, sector=null
Note:   research-and-recommend work is Consulting & Advisory even without the
        word "consulting" (rule 11b expanded). Market research collapses into
        Management Consulting.

─── IT Services vs Consulting & Advisory: BUILD/OPERATE/DELIVER vs ADVISE ───
The same vocabulary (cloud, cybersecurity, SAP, Microsoft, IT) shows up in
both identities. The deciding question: is the company DOING the work for
the client (IT Services) or RECOMMENDING what should be done (Consulting &
Advisory)? "Consulting", "advisory", "strategy", "assessment", "readiness",
"roadmap" → Consulting & Advisory. "Managed", "as-a-service", "monitoring",
"operations", "implementation", "support", "migration", "hosting",
"administration" → IT Services.

Input:  Managed cybersecurity monitoring and SOC-as-a-service for SMBs
Output: identity=IT Services, sub_identity=Cybersecurity Services, sector=null
Note:   "Managed monitoring" + "SOC-as-a-service" = THEY OPERATE the
        cybersecurity for the client. IT Services > Cybersecurity Services.

Input:  Cybersecurity strategy, risk assessment, and SOC 2 readiness advisory
Output: identity=Consulting & Advisory, sub_identity=IT Consulting, sector=null
Note:   "strategy", "risk assessment", "advisory" = ADVICE, not delivery.
        Consulting & Advisory > IT Consulting.

Input:  Cloud security and compliance consulting for healthcare providers
Output: identity=Consulting & Advisory, sub_identity=IT Consulting, sector=Healthcare
Note:   "consulting" is the work itself — they advise. Consulting & Advisory
        > IT Consulting. Healthcare is the served vertical → sector.

Input:  24/7 managed IT helpdesk and endpoint management for law firms
Output: identity=IT Services, sub_identity=null, sector=Legal
Note:   "Managed" + "helpdesk" + "endpoint management" = THEY OPERATE the
        IT for the client. IT Services, sub=null (MSP is the default flavor),
        Legal is the served vertical.

Input:  Cloud infrastructure migration and ongoing managed cloud for enterprise
Output: identity=IT Services, sub_identity=Data Migration Services, sector=null
Note:   "migration" + "managed cloud" = THEY DELIVER and OPERATE. IT Services.
        Sub = Data Migration Services since migration is the explicit work.

Input:  Microsoft Dynamics 365 implementation and ongoing administration services
Output: identity=IT Services, sub_identity=Custom Software Development, sector=null
Note:   "implementation" + "administration" = THEY BUILD and OPERATE. Even
        for a packaged-software product like Dynamics 365, the work is
        custom-built integration, not advice.

Input:  Microsoft Cloud and Dynamics 365 consulting and roadmap services
Output: identity=Consulting & Advisory, sub_identity=IT Consulting, sector=null
Note:   "consulting" + "roadmap" = ADVICE on what to build, not the build
        itself. Consulting & Advisory > IT Consulting.

─── "Advisory" attached to investment verbs — NOT consulting ──────────

Input:  Venture Capital Advisory for Secondary Market Transactions and Pre-IPO Financing
Output: identity=Financial Services, sub_identity=Venture Capital, sector=null
Note:   "VC advisory / secondary market / pre-IPO" → Financial Services (rule
        4b), NOT Consulting & Advisory.

─── Healthcare split, layer separation, parent-sub correctness ─────────

Input:  Multi-location dental group
Output: identity=Healthcare Provider, sub_identity=null, sector=null
Note:   treats patients → Healthcare Provider. Dental has no dedicated sub.

Input:  Biotech developing CAR-T cell therapies
Output: identity=Life Sciences & MedTech, sub_identity=Biotechnology, sector=null
Note:   develops therapies → Life Sciences & MedTech, NOT Healthcare Provider.

Input:  Medical device manufacturer specializing in orthopedic implants
Output: identity=Life Sciences & MedTech, sub_identity=Medical Device Manufacturer, sector=null
Note:   medical-device manufacturing → Life Sciences & MedTech. NOT Manufacturing.

Input:  Healthtech company building remote patient monitoring tools
Output: identity=Software & SaaS, sub_identity=null, sector=Healthcare
Note:   "healthtech company" → Software & SaaS (X-tech rule 1b). Healthcare
        is the served vertical → sector.

Input:  Home health agency providing in-home senior care
Output: identity=Healthcare Provider, sub_identity=Healthcare Support Services, sector=null
Note:   Healthcare Support Services' parent is Healthcare Provider — keep
        the pair consistent.

Input:  Digital consulting and technology strategy services for mission-driven organizations
Output: identity=Consulting & Advisory, sub_identity=Management Consulting, sector=Non-Profit & Social Impact
Note:   non-profit is the SERVED vertical → sector. Never put "Non-Profit
        Organization" in sub_identity when identity = Consulting & Advisory.

─── Edge cases ────────────────────────────────────────────────────────

Input:  Industrial distributor selling fasteners to OEMs
Output: identity=Distribution & Wholesale, sub_identity=null, sector=null
Note:   B2B middleman → Distribution & Wholesale, NOT Retail, NOT Logistics.

Input:  Independent insurance agency for commercial lines
Output: identity=Insurance Services, sub_identity=Insurance Brokerage, sector=null
Note:   Insurance Services is its own identity, NOT Financial Services.

Input:  Family-owned restaurant in Austin
Output: identity=Hospitality & Travel, sub_identity=Hotel & Hospitality Operator, sector=null, is_disqualified=true

Input:  Boutique law firm specializing in employment litigation
Output: identity=Legal Services, sub_identity=null, sector=null
Note:   Legal Services has NO sub-identities — sub_identity always null
        regardless of practice area.

Input:  Boutique strategy consulting firm
Output: identity=Consulting & Advisory, sub_identity=Management Consulting, sector=null

Input:  Regional CPA firm offering audit and tax services
Output: identity=Accounting & Tax, sub_identity=Tax & Audit Advisory, sector=null
Note:   CPAs → Accounting & Tax, NOT Consulting & Advisory.

Input:  Charter / private jet operator managing a fleet of light jets
Output: identity=Hospitality & Travel, sub_identity=Business Aviation Operator, sector=null

Input:  Boutique film production company shooting commercials and brand content
Output: identity=Media & Entertainment, sub_identity=Production Company, sector=null
Note:   production companies → Media & Entertainment, NOT Agency.

Input:  Educational consulting firm advising K-12 school districts on curriculum
Output: identity=Consulting & Advisory, sub_identity=Educational Consulting, sector=Education

Input:  AI-driven digital product agency for custom software design and development
Output: identity=IT Services, sub_identity=Custom Software Development, sector=null
Note:   "digital product agency" reads like marketing but the work is custom
        software development — IT Services wins.

─── When identity really IS too vague (the rare case) ─────────────────

Input:  Professional services firm  (no further description)
Output: identity=null, sub_identity=null, sector=null  (all confidences ≤ 4)
Note:   the input names no business model, no service type, no vertical.
        Genuinely impossible to identify — return nulls with reason.

Input:  Technology company  (no further description)
Output: identity=null, sub_identity=null, sector=null  (all confidences ≤ 4)
Note:   could be Software & SaaS, IT Services, Telecommunications, or
        Manufacturing — the input gives no signal. Null is correct here.`;
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
    // Find the array of results — be lenient about the wrapping key. gpt-4.1-mini
    // in json_object mode often echoes back the user prompt's key (`industries`)
    // instead of the prompt-specified `results`. We accept either by scanning
    // the parsed object's top-level values for the first array of items that
    // look like taggings (have `id`, `identity`, or `sub-identity`).
    let arr: any[] = [];
    if (Array.isArray(parsed)) {
        arr = parsed;
    } else if (parsed && typeof parsed === 'object') {
        // Preferred keys first; then any array value found at top level.
        const preferred = ['results', 'industries', 'taggings', 'output', 'data'];
        for (const k of preferred) {
            if (Array.isArray(parsed[k])) { arr = parsed[k]; break; }
        }
        if (arr.length === 0) {
            for (const k of Object.keys(parsed)) {
                if (Array.isArray(parsed[k]) && parsed[k].length > 0) { arr = parsed[k]; break; }
            }
        }
    }
    for (let i = 0; i < arr.length; i++) {
        const item = arr[i];
        // Accept item.id as number, numeric string, or fall back to position.
        // gpt-4.1-mini sometimes returns ids as strings ("0", "1") or omits
        // them entirely; positional fallback recovers what would otherwise
        // be silently dropped.
        let idx = -1;
        if (typeof item.id === 'number') idx = item.id;
        else if (typeof item.id === 'string' && /^\d+$/.test(item.id)) idx = parseInt(item.id, 10);
        else idx = i;
        if (idx < 0 || idx >= batch.length) continue;
        const v = batch[idx];
        // Per-tag confidences with back-compat: if the tagger returns the
        // legacy single `confidence` (e.g. older cached prompts), use it
        // for all three. The min becomes the overall confidence.
        const legacyConf = typeof item.confidence === 'number' ? item.confidence : 0;
        const idConf = typeof item.identity_confidence === 'number' ? item.identity_confidence : legacyConf;
        const chConf = typeof item.sub_identity_confidence === 'number' ? item.sub_identity_confidence : legacyConf;
        const secConf = typeof item.sector_confidence === 'number' ? item.sector_confidence : legacyConf;
        // Validate each tag slot: drop values that look like sentence
        // text rather than a taxonomy name. Stops the "primary_identity =
        // 'The homepage explicitly states…'" failure mode where the LLM
        // dumps reason text into the wrong field.
        const rawIdentity = nz(item.identity);
        const rawSubIdentity = nz(item.sub_identity);
        const rawSector = nz(item.sector);
        const cleanIdentity = validTag(rawIdentity);
        const cleanSubIdentity = validTag(rawSubIdentity);
        const cleanSector = validTag(rawSector);
        const hadTextBleed = (rawIdentity && !cleanIdentity)
            || (rawSubIdentity && !cleanSubIdentity)
            || (rawSector && !cleanSector);

        out.push({
            industry: v.industry,
            identity: cleanIdentity,
            is_new_identity: !!item.is_new_identity && !!cleanIdentity,
            sub_identity: cleanSubIdentity,
            is_new_sub_identity: !!item.is_new_sub_identity && !!cleanSubIdentity,
            sector: cleanSector,
            is_new_sector: !!item.is_new_sector && !!cleanSector,
            is_disqualified: !!item.is_disqualified,
            identity_confidence: hadTextBleed ? Math.min(idConf, 4) : idConf,
            sub_identity_confidence: hadTextBleed ? Math.min(chConf, 4) : chConf,
            sector_confidence: hadTextBleed ? Math.min(secConf, 4) : secConf,
            confidence: hadTextBleed
                ? Math.min(4, idConf || 10, chConf || 10, secConf || 10)
                : Math.min(idConf || 10, chConf || 10, secConf || 10),
            reason: typeof item.reason === 'string' ? item.reason.slice(0, 500) : ''
        });
    }
    return out;
}

// Lenient JSON parser for Phase 1b Anthropic output. Anthropic doesn't support
// OpenAI's `response_format: { type: 'json_schema', strict: true }`, so the
// model occasionally wraps the JSON in markdown fences or omits the
// "assignments" wrapper. We extract whatever array-of-objects we can find that
// looks like Phase 1b assignments.
function parsePhase1bAssignments(raw: string): any[] {
    let txt = (raw || '').trim();
    const fence = txt.match(/```(?:json)?\s*([\s\S]*?)```/);
    if (fence) txt = fence[1].trim();
    let parsed: any;
    try { parsed = JSON.parse(txt); }
    catch { throw new Error(`Phase 1b response is not valid JSON: ${txt.slice(0, 200)}`); }
    if (Array.isArray(parsed?.assignments)) return parsed.assignments;
    if (Array.isArray(parsed)) return parsed;
    if (parsed && typeof parsed === 'object') {
        for (const k of Object.keys(parsed)) {
            if (Array.isArray(parsed[k]) && parsed[k].length > 0) return parsed[k];
        }
    }
    return [];
}

function nz(v: any): string | null {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    if (s.length === 0) return null;
    // Placeholder strings the LLM sometimes returns as a string instead of
    // emitting JSON null. Without this guard "null" / "None" / "N/A" leak
    // through as real values and end up on the proposal Review screen as
    // "AI-proposed sub-identity = null" — a confusing ghost row that owns
    // a few hundred contacts.
    if (PLACEHOLDER_TAG_VALUES.has(s.toLowerCase())) return null;
    return s;
}

const PLACEHOLDER_TAG_VALUES = new Set([
    'null', 'none', 'undefined', 'n/a', 'n.a.', 'na', 'nan',
    'tbd', 'tba', 'unknown', 'unspecified', 'not specified',
    'not applicable', 'not available', '-', '--', 'na/a'
]);

// Reject obvious text-bleed values where the LLM dumped reasoning prose
// into a tag slot (e.g. primary_identity="The homepage explicitly states
// that Sunflower Development Center specializes in..."). Length + sentence
// punctuation are sufficient — anything shorter that the LLM emits gets
// normalised by snapTaggingsToLibrary downstream. (An earlier sentence-
// start regex matched "It " case-insensitively and was nuking legitimate
// "IT Services" identities — removed.)
const TAG_MAX_LEN = 60;
const TAG_SENTENCE_PUNCT = /[.!?]\s+[A-Z]/;
function validTag(v: string | null): string | null {
    if (!v) return null;
    if (v.length > TAG_MAX_LEN) return null;
    if (TAG_SENTENCE_PUNCT.test(v)) return null;
    return v;
}

// ─── snap-to-library ────────────────────────────────────────────────
// Claude Haiku and gpt-4.1-mini both consistently shorten or paraphrase
// taxonomy names ("Private Equity" instead of "Private Equity Firm";
// "Cybersecurity Software" instead of "Cybersecurity SaaS"). The prompt
// asks for exact spelling, but compliance is unreliable. snapTaggingsToLibrary
// runs after parseTaggingJson and deterministically maps fuzzy outputs back
// to canonical library entries. If no canonical entry covers the LLM's value
// well enough, we leave it alone with is_new_*=true so the user can promote
// it during review.

function snapTokens(s: string): string[] {
    return s.toLowerCase().replace(/[^a-z0-9]+/g, ' ').split(' ').filter(t => t.length >= 3);
}

function snapTokenMatches(a: string, b: string): boolean {
    if (a === b) return true;
    // Stem-ish prefix match — handles bank/banking, advisor/advisory.
    if (a.length >= 4 && b.length >= 4) {
        return a.startsWith(b) || b.startsWith(a);
    }
    return false;
}

function snapTokenIntersectionSize(va: string[], vb: string[]): number {
    let n = 0;
    const usedB = new Array(vb.length).fill(false);
    for (const ta of va) {
        for (let i = 0; i < vb.length; i++) {
            if (usedB[i]) continue;
            if (snapTokenMatches(ta, vb[i])) { n++; usedB[i] = true; break; }
        }
    }
    return n;
}

function snapToLibraryName(value: string, validNames: string[]): string | null {
    const v = value.trim();
    if (!v) return null;
    if (validNames.includes(v)) return v;
    const vTokens = snapTokens(v);
    if (vTokens.length === 0) return null;
    let best: { name: string; score: number; nLen: number } | null = null;
    for (const name of validNames) {
        const nTokens = snapTokens(name);
        if (nTokens.length === 0) continue;
        const inter = snapTokenIntersectionSize(vTokens, nTokens);
        if (inter === 0) continue;
        const sV = inter / vTokens.length;
        const sN = inter / nTokens.length;
        // Require both directions to be well-covered so we don't snap
        // "Cybersecurity" → "Cybersecurity Services" purely on one shared word.
        if (sV < 0.5 || sN < 0.5) continue;
        const score = Math.min(sV, sN);
        const nLen = nTokens.length;
        if (!best || score > best.score
            || (score === best.score && Math.abs(nLen - vTokens.length) < Math.abs(best.nLen - vTokens.length))) {
            best = { name, score, nLen };
        }
    }
    return best ? best.name : null;
}

function applySnap(value: string | null, validNames: string[]): { value: string | null; is_new: boolean } {
    if (!value) return { value: null, is_new: false };
    const snapped = snapToLibraryName(value, validNames);
    if (snapped) return { value: snapped, is_new: false };
    return { value: value.trim() || null, is_new: true };
}

// Snap Phase 1b LLM outputs (MatchChain shape) to canonical library names.
// Same idea as snapTaggingsToLibrary but operating on the {name, score, reason}
// chain-item structure. validSpecNames is the union of sub-identities across
// the per-run leaves (so paraphrases of names not in this set still get dropped
// by the existing strict-match validation downstream).
function snapMatchChain(
    chain: MatchChain,
    validIdentityNames: Set<string>,
    validSpecNames: Set<string>,
    sectorVocab: string[]
): MatchChain {
    const idSnap = chain.primary_identity.name
        ? snapToLibraryName(chain.primary_identity.name, Array.from(validIdentityNames))
        : null;
    const subSnap = chain.sub_identity.name
        ? snapToLibraryName(chain.sub_identity.name, Array.from(validSpecNames))
        : null;
    const secSnap = chain.sector
        ? snapToLibraryName(chain.sector, sectorVocab)
        : null;
    return {
        ...chain,
        primary_identity: {
            ...chain.primary_identity,
            name: idSnap || chain.primary_identity.name
        },
        sub_identity: {
            ...chain.sub_identity,
            name: subSnap || chain.sub_identity.name
        },
        sector: secSnap || chain.sector
    };
}

function snapTaggingsToLibrary(taggings: IndustryTagging[], snapshot: TaxonomySnapshot): IndustryTagging[] {
    const identityNames = snapshot.identities.map(i => i.name);
    const identityNameSet = new Set(identityNames);
    const sectorNames = snapshot.sectors.map(sec => sec.name);
    const sectorNameSet = new Set(sectorNames);
    const subByParent = new Map<string, string[]>();
    const subParent = new Map<string, string>();
    for (const sub of snapshot.sub_identities) {
        const parent = sub.parent_identity || '';
        const list = subByParent.get(parent) || [];
        list.push(sub.name);
        subByParent.set(parent, list);
        subParent.set(sub.name, parent);
    }
    return taggings.map(t => {
        const idSnap = applySnap(t.identity, identityNames);
        // Sub-identity must be under the chosen identity; if identity didn't
        // resolve to a library entry, fall back to global sub-identity match.
        const subAllowed = idSnap.value && !idSnap.is_new
            ? (subByParent.get(idSnap.value) || [])
            : snapshot.sub_identities.map(s => s.name);
        let subSnap = applySnap(t.sub_identity, subAllowed);
        // Parent-consistency guard. If the sub_identity name exists in the
        // GLOBAL library under a parent different from the chosen identity,
        // the LLM mis-paired — drop the sub (null) rather than persist a wrong
        // pair. Phase 1b will route to identity-level.
        //
        // Crucially we check subSnap.is_new=true cases too: applySnap above
        // only sees the chosen identity's children (a tight allowlist), so a
        // sub borrowed from another parent gets marked is_new=true even
        // though the name is already in the library. The earlier version of
        // this guard required !subSnap.is_new and silently let those through —
        // 437 fresh mismatches survived in one observed run as a result.
        if (
            !idSnap.is_new && idSnap.value && subSnap.value &&
            subParent.has(subSnap.value) &&
            subParent.get(subSnap.value) !== idSnap.value
        ) {
            subSnap = { value: null, is_new: false };
        }
        // Identity-required: if sub_identity is set but identity is null,
        // promote the sub's library parent to identity. Stops Phase 1b from
        // dropping the row to "General" just because the LLM hesitated on
        // identity. Only safe when the sub is a library hit.
        if (
            !subSnap.is_new && subSnap.value &&
            (!idSnap.value || idSnap.is_new) &&
            subParent.has(subSnap.value)
        ) {
            idSnap.value = subParent.get(subSnap.value) || idSnap.value;
            idSnap.is_new = false;
        }
        const secSnap = applySnap(t.sector, sectorNames);
        // Sub-identity must not duplicate the sector value verbatim — that
        // pattern always means the LLM put the vertical in the wrong field.
        if (subSnap.value && secSnap.value && subSnap.value === secSnap.value) {
            subSnap = { value: null, is_new: false };
        }
        // Wrong-layer guard: if the proposed sub_identity name matches an
        // existing IDENTITY name ("Real Estate", "Distribution & Wholesale",
        // "Software & SaaS"), the LLM is using a Layer-1 name as a Layer-2
        // value — drop it. Same for sub == sector name. Without this the
        // Review screen surfaces ghost proposals like "AI-proposed sub
        // = Real Estate (under Education & Training)" that no one wants.
        if (subSnap.value && (identityNameSet.has(subSnap.value) || sectorNameSet.has(subSnap.value))) {
            subSnap = { value: null, is_new: false };
        }
        return {
            ...t,
            identity: idSnap.value,
            is_new_identity: idSnap.is_new,
            sub_identity: subSnap.value,
            is_new_sub_identity: subSnap.is_new,
            sector: secSnap.value,
            is_new_sector: secSnap.is_new,
        };
    });
}

// Post-batch dedup: collapse near-duplicate identity / sub-identity /
// sector tags WITHIN a single run's taggings array. Phase 1a runs in
// 40-concurrent batches that don't see each other's output, so the same
// concept can show up under 4-5 spelling variants ("Field Services /
// Maintenance" + "B2B Field Services / Maintenance" + "Field Services &
// Maintenance"). The per-row library-dedup already handles the case where
// one variant exists in the snapshot library — this handles the case where
// the LIBRARY IS EMPTY (or the variants only appear in the LLM output).
//
// For each layer, group all proposed tags by normalized name. For each
// normalized group, pick a canonical name: existing library entry > most
// frequent variant > shortest. Then rewrite every tagging in-place.
function consolidateTaggings(taggings: IndustryTagging[], snapshot: TaxonomySnapshot): {
    identityMerges: number;
    subIdentityMerges: number;
    sectorMerges: number;
} {
    const buildCanonical = (
        existing: string[],
        getter: (t: IndustryTagging) => string | null
    ): { canonical: Map<string, string>; merges: number } => {
        const counts = new Map<string, Map<string, number>>(); // normalized → name → count
        // Existing library entries are always canonical for their normalized key.
        for (const e of existing) {
            const norm = normalizeTaxonomyName(e);
            if (!norm) continue;
            if (!counts.has(norm)) counts.set(norm, new Map());
            counts.get(norm)!.set(e, Number.MAX_SAFE_INTEGER);
        }
        for (const t of taggings) {
            const v = getter(t);
            if (!v) continue;
            const norm = normalizeTaxonomyName(v);
            if (!norm) continue;
            if (!counts.has(norm)) counts.set(norm, new Map());
            const m = counts.get(norm)!;
            m.set(v, (m.get(v) || 0) + 1);
        }
        const canonical = new Map<string, string>();
        let merges = 0;
        for (const [norm, names] of counts) {
            const sorted = Array.from(names.entries()).sort((a, b) => {
                if (a[1] !== b[1]) return b[1] - a[1]; // by count desc
                return a[0].length - b[0].length;       // shorter wins ties
            });
            canonical.set(norm, sorted[0][0]);
            if (names.size > 1) merges += names.size - 1;
        }
        return { canonical, merges };
    };

    const id = buildCanonical(snapshot.identities.map(i => i.name), t => t.identity);
    const ch = buildCanonical(snapshot.sub_identities.map(c => c.name), t => t.sub_identity);
    const sec = buildCanonical(snapshot.sectors.map(s => s.name), t => t.sector);

    for (const t of taggings) {
        if (t.identity) {
            const norm = normalizeTaxonomyName(t.identity);
            const can = id.canonical.get(norm);
            if (can) t.identity = can;
        }
        if (t.sub_identity) {
            const norm = normalizeTaxonomyName(t.sub_identity);
            const can = ch.canonical.get(norm);
            if (can) t.sub_identity = can;
        }
        if (t.sector) {
            const norm = normalizeTaxonomyName(t.sector);
            const can = sec.canonical.get(norm);
            if (can) t.sector = can;
        }
    }
    return {
        identityMerges: id.merges,
        subIdentityMerges: ch.merges,
        sectorMerges: sec.merges
    };
}

// Normalizer for taxonomy entry names — used in fuzzy dedup so the LLM's
// "Telecommunications Services" / "telecommunications" / "TELECOM SERVICES"
// all collapse to the same key as the library's "Telecommunications". We
// lowercase, collapse whitespace, strip a small set of generic trailing
// nouns + corporate suffixes that the LLM tends to add or omit. This is a
// pragmatic dedup, NOT a semantic merge — entries with genuinely different
// meanings (e.g. "Marketing Agency" vs "Marketing Services") will normalize
// to different strings and stay separate.
function normalizeTaxonomyName(name: string): string {
    if (!name) return '';
    let s = String(name).toLowerCase().trim();
    // Strip parenthesized notes like "(under Tech)" the LLM occasionally adds.
    s = s.replace(/\s*\([^)]*\)\s*$/g, '').trim();
    // Strip leading "B2B " — the LLM sprinkles this for emphasis and it's
    // never semantically meaningful at the identity / sub-identity level.
    s = s.replace(/^b2b\s+/i, '').trim();
    // Collapse delimiter + connector variants so "Field Services / Maintenance"
    // and "Field Services & Maintenance" and "Field Services and Maintenance"
    // all normalize to the same string.
    s = s.replace(/\s*\/\s*/g, ' and ').replace(/\s*&\s*/g, ' and ').replace(/\s+/g, ' ');
    // Strip trailing generic / corporate suffixes (only at the end).
    const trailingSuffixes = [
        ' services', ' solutions', ' group', ' inc', ' inc.', ' llc', ' ltd',
        ' co.', ' company', ' corp', ' corp.', ' corporation', ' firm'
    ];
    let changed = true;
    while (changed) {
        changed = false;
        for (const suf of trailingSuffixes) {
            if (s.endsWith(suf)) {
                s = s.slice(0, -suf.length).trim();
                changed = true;
                break;
            }
        }
    }
    // Strip trailing plural 's' so "Associations" / "Investments" / "Resources"
    // collapse with their singular forms in the dedup key. Skip 'ss' (SaaS,
    // Business) and 'ics' (Analytics, Diagnostics — singular nouns that look
    // plural). Length floor of 5 protects 3-4 letter acronyms / short names.
    if (s.length >= 5 && s.endsWith('s') && !s.endsWith('ss') && !s.endsWith('ics')) {
        s = s.slice(0, -1).trim();
    }
    return s;
}

// LLM-based semantic consolidation of proposed-new sub-identities. Runs
// AFTER consolidateTaggings (which only catches normalized-name matches).
// The LLM merges semantically similar entries the normalizer can't catch
// ("Wealth Management" + "Investment Advisory" + "Wealth & Investment
// Advisory" → all collapse to "Wealth Management"). Skipped when there
// are < 60 proposed-new sub-identities — for small runs it's not worth
// the extra LLM call.
//
// Input: the taggings array + the library snapshot.
// Output: how many sub-identity names were merged. Mutates `taggings`
// in-place to use the canonical name from each merge group.
async function consolidateSubIdentitiesViaLLM(
    supabase: SupabaseClient,
    model: string,
    taggings: IndustryTagging[],
    snapshot: TaxonomySnapshot,
    runId: string,
    ctx: BucketingCtx
): Promise<{ merges: number; costUsd: number }> {
    const libraryNames = new Set(snapshot.sub_identities.map(c => c.name));

    // Count distinct proposed-new sub-identity occurrences, grouped with
    // their parent identity (so the LLM only merges WITHIN the same identity
    // — "FinTech SaaS" under Software & SaaS shouldn't merge with "FinTech
    // Investment" under Financial Services).
    const counts = new Map<string, { count: number; identity: string }>();
    for (const t of taggings) {
        const c = t.sub_identity;
        if (!c) continue;
        if (libraryNames.has(c)) continue;            // skip library-canonical
        const key = `${t.identity || ''}::${c}`;
        const cur = counts.get(key);
        if (cur) cur.count++;
        else counts.set(key, { count: 1, identity: t.identity || '' });
    }
    if (counts.size < 60) {
        ctx.log(`[Bucketing ${runId}] sub-identity consolidation skipped (${counts.size} proposed-new < 60 threshold)`);
        return { merges: 0, costUsd: 0 };
    }

    // Sort by count descending — we want to give the LLM stable, frequent
    // names first since those are more likely to "win" as canonical.
    const entries = Array.from(counts.entries())
        .map(([key, v]) => {
            const [identity, subIdentity] = key.split('::');
            return { identity, sub_identity: subIdentity, count: v.count };
        })
        .sort((a, b) => b.count - a.count);

    // Group by identity. Each identity becomes its own LLM call run in
    // parallel — splitting 350+ inputs across 15-20 small calls instead of
    // one massive one. This fixes two failure modes the previous one-shot
    // call hit:
    //   1. Output token cap (4k tokens couldn't fit 200+ merge entries →
    //      response truncated mid-JSON → parse failed → 0 merges).
    //   2. Single point of failure — one bad batch killed all consolidation.
    // Per-identity batches are 5-50 entries each, fitting comfortably in
    // 8k output tokens. pLimit(5) caps concurrency to stay under rate limits.
    const byIdent = new Map<string, { name: string; count: number }[]>();
    for (const e of entries) {
        if (!byIdent.has(e.identity)) byIdent.set(e.identity, []);
        byIdent.get(e.identity)!.push({ name: e.sub_identity, count: e.count });
    }

    // Library sub-identities indexed by parent_identity. Each per-identity
    // chunk surfaces its slice as "EXISTING LIBRARY SUB-IDENTITIES UNDER
    // THIS IDENTITY" so the LLM merges proposals INTO library entries first
    // (the user's curated taxonomy is the source of truth and must never be
    // removed or renamed by the consolidator).
    const libraryByIdentity = new Map<string, string[]>();
    for (const c of snapshot.sub_identities) {
        const parent = c.parent_identity || '';
        if (!libraryByIdentity.has(parent)) libraryByIdentity.set(parent, []);
        libraryByIdentity.get(parent)!.push(c.name);
    }

    const systemPrompt = `You consolidate a list of B2B sub-identity proposals into a small, reusable canonical set. Each sub-identity is a sub-type within a primary identity.

GOAL: collapse near-duplicate / overly narrow entries within the same identity. TARGET: 3–6 sub-identities per identity, TOTAL 50–80 across the whole list. If the input has more than 80 distinct sub-identities, you MUST merge aggressively. Be RUTHLESS — leaving variants separate is the failure mode, merging too much is rare.

LIBRARY IS THE SOURCE OF TRUTH:
The user prompt for each identity includes a section "EXISTING LIBRARY SUB-IDENTITIES UNDER THIS IDENTITY". Those names are the ONLY canonical merge targets — they are the user's curated taxonomy. Don't coin alternatives. Always prefer merging proposals INTO an existing library entry over coining a new name; only emit a new canonical name when no library entry under the same identity is even loosely applicable. NEVER remove or rename a library entry.

GENERIC MERGE GUIDANCE:
- Strip trailing "Firm" / "Services" / "Solutions" / "Group" / "Inc" / "LLC" / "Corp" unless removing the suffix changes meaning.
- Plural → singular ("Insurance Brokerages" → Insurance Brokerage).
- "B2B " prefix → strip when it doesn't change meaning.
- Vertical-flavored names ("Healthcare CRM SaaS", "Restaurant POS Software") merge into the closest non-vertical sibling under the same identity, or drop to null and let the contact's sector field carry the vertical.

LOW-COUNT MERGE RULE (count ≤ 2):
Any sub-identity appearing only 1–2 times in the input list MUST be either:
  (a) merged into the closest broader sibling sub-identity under the same identity (preferred), OR
  (b) dropped to null if no reasonable sibling exists (output {"from": "<name>", "to": ""})
Low-count entries fragment the bucket count without earning their own viable bucket — a 1× sub-identity will roll up to identity-only at min_volume anyway.

IDENTITY-BLEED RULE (CRITICAL):
If the sub-identity name IS an identity name (any name that appears in VALID_IDENTITIES — e.g. "Healthcare Provider", "Field Services & Maintenance", "Consulting & Advisory", "Legal Services", "Non-Profit & Association", "Hospitality & Travel", "Software & SaaS", "IT Services"), drop it to null with {"from": "<name>", "to": ""}. The contact will roll up to identity-only via the cascade. Identity names must never appear as sub-identities.

QUALITY CHECK before keeping a name as-is. Ask:
  1. Is this sub-identity reusable across multiple companies (≥3 ideally)?
  2. Is it meaningfully distinct from existing options under the same identity?
  3. Would a marketer or sales operator actually use this distinction in a campaign?
  4. Could this be merged into a broader label without losing important value?
  5. Is this actually a sector (vertical served) instead of a sub-identity?
Only keep the name if the answers support it. When in doubt, MERGE.

HARD RULES:
- Merge ONLY within the same identity. Never cross-identity.
- Genuinely different sub-types stay separate (e.g. an advocacy-focused proposal and a religious-focused proposal under Non-Profit & Association are not the same sub).

OUTPUT (strict JSON, key MUST be "merges"):
{
  "merges": [
    { "from": "<original sub-identity name verbatim>", "to": "<canonical name OR empty string \\"\\" to drop>" },
    ...
  ]
}
Only include entries where from != to (or where to="" to drop). Be ruthless — if the input has 200+ sub-identities, expect to output 150+ merges.`;

    // Per-identity LLM call. Each call gets a small input (5-50 chars under
    // ONE identity) and produces small output (3-30 merges). Failures in
    // one identity don't kill the whole consolidation.
    const consolidateOneIdentity = async (
        identity: string,
        list: { name: string; count: number }[]
    ): Promise<{ map: Map<string, string>; costUsd: number; error?: string }> => {
        const inputBody = list.map(l => `  - "${l.name}" (${l.count}×)`).join('\n');
        const libUnderId = (libraryByIdentity.get(identity) || []).slice().sort();
        const libBlock = libUnderId.length > 0
            ? `\n\nEXISTING LIBRARY SUB-IDENTITIES UNDER THIS IDENTITY (the only canonical merge targets — NEVER remove or rename):\n${libUnderId.map(n => `  - "${n}"`).join('\n')}`
            : '\n\n(No library sub-identities exist yet under this identity — emit a new canonical only when the proposal is reusable and identity-appropriate; otherwise drop to null.)';
        const userPrompt = `Identity to consolidate: ${identity || '(none)'}${libBlock}\n\nProposed-new sub-identities under this identity (apply LIBRARY rule, then LOW-COUNT MERGE and IDENTITY-BLEED rules):\n\n${inputBody}`;

        const isAnthropic = model.startsWith('claude-');
        let text = '';
        let cost = 0;
        try {
            if (isAnthropic) {
                const anthropic = await getAnthropic(supabase);
                if (!anthropic) throw new Error('Anthropic API key not configured');
                const resp = await anthropic.messages.create({
                    model,
                    max_tokens: 8000,
                    system: systemPrompt,
                    messages: [{ role: 'user', content: userPrompt }]
                }, { signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal) });
                const usage: any = (resp as any).usage || {};
                cost = computeAnthropicCost(model, usage.input_tokens || 0, usage.output_tokens || 0);
                text = (resp.content as any[]).filter(b => b.type === 'text').map(b => b.text).join('\n');
            } else {
                const resp = await fetch('https://api.openai.com/v1/chat/completions', {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${getOpenAIKey()}`, 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        model,
                        max_tokens: 8000,
                        response_format: { type: 'json_object' },
                        messages: [
                            { role: 'system', content: systemPrompt },
                            { role: 'user', content: userPrompt }
                        ]
                    }),
                    signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal)
                });
                if (!resp.ok) throw new Error(`OpenAI ${resp.status}: ${(await resp.text()).slice(0, 300)}`);
                const json: any = await resp.json();
                const usage = json.usage || {};
                cost = computeOpenAICost(model, usage.prompt_tokens || 0, 0, usage.completion_tokens || 0);
                text = json.choices?.[0]?.message?.content || '';
            }

            // Parse — try strict JSON first, fall back to regex repair on
            // truncation. Truncation produces output like `{"merges":[{...},{...},{"f` →
            // regex pulls out the well-formed entries before the cutoff.
            const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
            const cleaned = (fence ? fence[1] : text).trim();
            const out = new Map<string, string>();
            try {
                const parsed = JSON.parse(cleaned);
                const merges = Array.isArray(parsed.merges) ? parsed.merges : [];
                for (const m of merges) {
                    if (typeof m?.from === 'string' && typeof m?.to === 'string' && m.from !== m.to) {
                        out.set(m.from, m.to);
                    }
                }
            } catch {
                // JSON repair: extract complete {"from": "...", "to": "..."} objects.
                const re = /\{\s*"from"\s*:\s*"((?:[^"\\]|\\.)*)"\s*,\s*"to"\s*:\s*"((?:[^"\\]|\\.)*)"\s*\}/g;
                let m: RegExpExecArray | null;
                let recovered = 0;
                while ((m = re.exec(cleaned)) !== null) {
                    const from = m[1].replace(/\\(.)/g, '$1');
                    const to = m[2].replace(/\\(.)/g, '$1');
                    if (from !== to) { out.set(from, to); recovered++; }
                }
                if (recovered === 0) throw new Error(`unrecoverable parse failure (${cleaned.length} chars)`);
                ctx.log(`[Bucketing ${runId}] [${identity}] consolidation JSON truncated — recovered ${recovered} merges via regex`, 'warn');
            }
            return { map: out, costUsd: cost };
        } catch (err: any) {
            return { map: new Map(), costUsd: cost, error: err.message };
        }
    };

    // Run identities with >= 5 entries in parallel. Identities with < 5 don't
    // need consolidation (already small enough).
    const limit = pLimit(5);
    const candidates = Array.from(byIdent.entries()).filter(([_, list]) => list.length >= 5);
    if (candidates.length === 0) {
        ctx.log(`[Bucketing ${runId}] sub-identity consolidation: no identities with >= 5 entries to consolidate`);
        return { merges: 0, costUsd: 0 };
    }

    let totalCost = 0;
    let totalErrors = 0;
    const mergeMap = new Map<string, string>();

    await Promise.all(candidates.map(([identity, list]) => limit(async () => {
        const r = await consolidateOneIdentity(identity, list);
        totalCost += r.costUsd;
        if (r.error) {
            totalErrors++;
            ctx.log(`[Bucketing ${runId}] [${identity}] consolidation failed: ${r.error}`, 'warn');
            return;
        }
        for (const [from, to] of r.map) mergeMap.set(from, to);
    })));

    if (totalErrors > 0) {
        ctx.log(`[Bucketing ${runId}] sub-identity consolidation: ${totalErrors}/${candidates.length} identities failed (non-fatal — others applied)`);
    }

    // Apply the mapping in-place. An empty string target means "drop to null"
    // — used by LOW-COUNT MERGE and IDENTITY-BLEED rules. The contact will
    // then roll up to identity-only via the cascade.
    for (const t of taggings) {
        if (t.sub_identity && mergeMap.has(t.sub_identity)) {
            const target = mergeMap.get(t.sub_identity) || '';
            t.sub_identity = target.trim() ? target : null;
        }
    }
    return { merges: mergeMap.size, costUsd: totalCost };
}

// Sector-specific consolidation. Same shape as the sub-identity version
// but with sector-tailored merge rules and a more aggressive "drop or merge
// to BLANK" instruction. Sectors fragment heavily across batches because
// the LLM invents niche verticals (Sports / Sports & Athletics / Esports)
// that should all collapse to broader buckets, AND sometimes mis-classifies
// identities as sectors ("Marketing / Advertising", "Insurance", "IT
// Services"). The merge rules drop those entirely (set to "" / blank).
async function consolidateSectorsViaLLM(
    supabase: SupabaseClient,
    model: string,
    taggings: IndustryTagging[],
    snapshot: TaxonomySnapshot,
    runId: string,
    ctx: BucketingCtx
): Promise<{ merges: number; costUsd: number }> {
    const libraryNames = new Set(snapshot.sectors.map(s => s.name));

    const counts = new Map<string, number>();
    for (const t of taggings) {
        const s = t.sector;
        if (!s) continue;
        if (libraryNames.has(s)) continue;
        counts.set(s, (counts.get(s) || 0) + 1);
    }
    if (counts.size < 15) {
        ctx.log(`[Bucketing ${runId}] sector consolidation skipped (${counts.size} proposed-new < 15 threshold)`);
        return { merges: 0, costUsd: 0 };
    }

    const entries = Array.from(counts.entries())
        .map(([sector, count]) => ({ sector, count }))
        .sort((a, b) => b.count - a.count);
    const promptBody = entries.map(e => `  - "${e.sector}" (${e.count}×)`).join('\n');
    const libraryList = Array.from(libraryNames).sort().map(n => `  - "${n}"`).join('\n');

    const systemPrompt = `You consolidate a list of B2B SECTOR proposals (Layer 3 — vertical the company SERVES) into a smaller canonical set. Sectors should be broad served verticals — not the company's own identity, not narrow niches.

GOAL: collapse near-duplicates and identity-bleed. TARGET: 10–20 sectors total.

LIBRARY IS THE SOURCE OF TRUTH:
The list below is the user's curated taxonomy. These are the ONLY canonical merge targets — don't coin alternatives. Always prefer merging proposals INTO an existing library sector over coining a new name; only emit a new canonical when no library sector is even loosely applicable. NEVER remove or rename a library entry.

EXISTING LIBRARY SECTORS:
${libraryList || '  (library is empty — emit a new canonical only when the proposal names a clear, broadly-served vertical; otherwise drop to "".)'}

GENERIC MERGE GUIDANCE:
- Plural / singular variants collapse to the library entry's form ("Healthcare Services" → "Healthcare").
- Vertical-flavored variants (e.g. "Solar Energy", "Pharma", "Affordable Housing") merge into the broader library sector when one fits ("Energy & Utilities", "Life Sciences & Biotech", "Real Estate").

MAP TO BLANK (these inputs are NOT sectors — set to ""):
  Marketing · Advertising · Marketing / Advertising · Marketing & Sales ·
  IT Services · Technology Services · Professional Services · Consulting ·
  Corporate · B2B · Small Business · Subscription · Holding Company ·
  Multi-industry (only when there is no specific vertical — leave blank)
These are identity / generic labels, not verticals served. The contact's identity already captures them.

RULES:
- Use the canonical name verbatim (no paraphrasing).
- When the input could fit multiple canonicals, pick the broadest reasonable match.
- Keep the proposed-new set as small as possible — when in doubt, merge or drop to blank.

OUTPUT (strict JSON, key MUST be "merges"):
{
  "merges": [
    { "from": "<original sector name verbatim>", "to": "<canonical name OR empty string \\"\\" to drop>" },
    ...
  ]
}
Only include entries where from != to (or where to is "" to drop). Be aggressive — 50+ proposed sectors should reduce to ~10–20 canonical.`;

    const userPrompt = `Consolidate these proposed-new sectors:\n\n${promptBody}`;

    let costUsd = 0;
    let mergeCount = 0;
    const mergeMap = new Map<string, string>();

    try {
        const isAnthropic = model.startsWith('claude-');
        let text = '';
        if (isAnthropic) {
            const anthropic = await getAnthropic(supabase);
            if (!anthropic) throw new Error('Anthropic API key not configured');
            const resp = await anthropic.messages.create({
                model,
                max_tokens: 3000,
                system: systemPrompt,
                messages: [{ role: 'user', content: userPrompt }]
            }, { signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal) });
            const usage: any = (resp as any).usage || {};
            costUsd = computeAnthropicCost(model, usage.input_tokens || 0, usage.output_tokens || 0);
            text = (resp.content as any[]).filter(b => b.type === 'text').map(b => b.text).join('\n');
        } else {
            const resp = await fetch('https://api.openai.com/v1/chat/completions', {
                method: 'POST',
                headers: { Authorization: `Bearer ${getOpenAIKey()}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    model,
                    max_tokens: 3000,
                    response_format: { type: 'json_object' },
                    messages: [
                        { role: 'system', content: systemPrompt },
                        { role: 'user', content: userPrompt }
                    ]
                }),
                signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal)
            });
            if (!resp.ok) throw new Error(`OpenAI ${resp.status}: ${(await resp.text()).slice(0, 300)}`);
            const json: any = await resp.json();
            const usage = json.usage || {};
            costUsd = computeOpenAICost(model, usage.prompt_tokens || 0, 0, usage.completion_tokens || 0);
            text = json.choices?.[0]?.message?.content || '';
        }

        const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
        const cleaned = (fence ? fence[1] : text).trim();
        const parsed = JSON.parse(cleaned);
        const merges = Array.isArray(parsed.merges) ? parsed.merges : [];
        for (const m of merges) {
            if (typeof m?.from === 'string' && typeof m?.to === 'string' && m.from !== m.to) {
                // Allow merge target of "" — means "drop this sector to blank"
                mergeMap.set(m.from, m.to);
            }
        }
        mergeCount = mergeMap.size;
    } catch (err: any) {
        ctx.log(`[Bucketing ${runId}] sector consolidation LLM call failed (non-fatal): ${err.message}`, 'warn');
        return { merges: 0, costUsd };
    }

    // Apply the mapping in-place. An empty string target means "drop to null".
    for (const t of taggings) {
        if (t.sector && mergeMap.has(t.sector)) {
            const target = mergeMap.get(t.sector) || '';
            t.sector = target.trim() ? target : null;
        }
    }
    return { merges: mergeCount, costUsd };
}

// LLM semantic consolidation of proposed-new IDENTITIES (Layer 1 — top-level
// business model). Smaller input than sub-identities (typically 5–25
// proposals), so a single LLM call is sufficient. The library list is
// included in the prompt so the LLM can merge proposals INTO existing
// canonical names rather than fork variants. Identity is the cascade root,
// so this is conservative: high-count proposals stay separate unless an
// obvious canonical equivalent exists; low-count proposals (< 5) are
// aggressively merged or dropped.
async function consolidateIdentitiesViaLLM(
    supabase: SupabaseClient,
    model: string,
    taggings: IndustryTagging[],
    snapshot: TaxonomySnapshot,
    runId: string,
    ctx: BucketingCtx
): Promise<{ merges: number; costUsd: number }> {
    const libraryNames = new Set(snapshot.identities.map(i => i.name));

    const counts = new Map<string, number>();
    for (const t of taggings) {
        const i = t.identity;
        if (!i) continue;
        if (libraryNames.has(i)) continue;
        counts.set(i, (counts.get(i) || 0) + 1);
    }
    if (counts.size < 6) {
        ctx.log(`[Bucketing ${runId}] identity consolidation skipped (${counts.size} proposed-new < 6 threshold)`);
        return { merges: 0, costUsd: 0 };
    }

    const entries = Array.from(counts.entries())
        .map(([identity, count]) => ({ identity, count }))
        .sort((a, b) => b.count - a.count);
    const promptBody = entries.map(e => `  - "${e.identity}" (${e.count}×)`).join('\n');
    const libraryList = Array.from(libraryNames).sort().map(n => `  - "${n}"`).join('\n');

    const systemPrompt = `You consolidate B2B IDENTITY proposals (Layer 1 — the company's top-level business model) into the canonical set.

GOAL: collapse near-duplicates and merge proposals INTO existing library identities wherever possible. TARGET: 10–15 identities total across the library + final proposals.

LIBRARY IS THE SOURCE OF TRUTH:
The list below is the user's curated taxonomy. These are the ONLY canonical merge targets — don't coin alternatives. Always prefer merging proposals INTO an existing library identity over coining a new name; only emit a new canonical when no library identity is even loosely applicable. NEVER remove or rename a library entry.

EXISTING LIBRARY IDENTITIES:
${libraryList || '  (library is empty — emit a new canonical only when the proposal names a clear, reusable business model; otherwise drop to "".)'}

GENERIC MERGE GUIDANCE:
- Plural / singular variants collapse to the library form ("Non-Profit & Associations" → "Non-Profit & Association").
- Vertical-flavored variants merge into the broader library identity when one fits ("Insurance" / "Insurance Carrier" → an existing insurance-related library identity; "Marketplace Platform" → an existing software identity; etc.).

MAP TO BLANK (these inputs are NOT valid identities — set to ""):
  Specialty Services · Professional Services · B2B · Subscription Service · Other ·
  Multi-industry · General Services · Service Provider · Conglomerate ·
  Holding Company (when pure passive holding with no clear business)

RULES:
- Use the canonical / library name verbatim (no paraphrasing).
- When the input could fit multiple canonicals, pick the closest existing LIBRARY name first; fall back to the canonical list otherwise.
- Identity is the top of the cascade — be CONSERVATIVE with high-count proposals (≥10×). Only merge them if there is an obvious canonical / library equivalent. Real new business models do exist.
- Low-count proposals (< 5×) should aggressively merge or drop to "" — they will never earn their own bucket.

OUTPUT (strict JSON, key MUST be "merges"):
{
  "merges": [
    { "from": "<original identity name verbatim>", "to": "<canonical / library name OR empty string \\"\\" to drop>" },
    ...
  ]
}
Only include entries where from != to (or where to is "" to drop).`;

    const userPrompt = `Consolidate these proposed-new identities (existing library names listed in system prompt — prefer merging INTO those):\n\n${promptBody}`;

    let costUsd = 0;
    let mergeCount = 0;
    const mergeMap = new Map<string, string>();

    try {
        const isAnthropic = model.startsWith('claude-');
        let text = '';
        if (isAnthropic) {
            const anthropic = await getAnthropic(supabase);
            if (!anthropic) throw new Error('Anthropic API key not configured');
            const resp = await anthropic.messages.create({
                model,
                max_tokens: 3000,
                system: systemPrompt,
                messages: [{ role: 'user', content: userPrompt }]
            }, { signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal) });
            const usage: any = (resp as any).usage || {};
            costUsd = computeAnthropicCost(model, usage.input_tokens || 0, usage.output_tokens || 0);
            text = (resp.content as any[]).filter(b => b.type === 'text').map(b => b.text).join('\n');
        } else {
            const resp = await fetch('https://api.openai.com/v1/chat/completions', {
                method: 'POST',
                headers: { Authorization: `Bearer ${getOpenAIKey()}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    model,
                    max_tokens: 3000,
                    response_format: { type: 'json_object' },
                    messages: [
                        { role: 'system', content: systemPrompt },
                        { role: 'user', content: userPrompt }
                    ]
                }),
                signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, ctx.abortSignal)
            });
            if (!resp.ok) throw new Error(`OpenAI ${resp.status}: ${(await resp.text()).slice(0, 300)}`);
            const json: any = await resp.json();
            const usage = json.usage || {};
            costUsd = computeOpenAICost(model, usage.prompt_tokens || 0, 0, usage.completion_tokens || 0);
            text = json.choices?.[0]?.message?.content || '';
        }

        const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
        const cleaned = (fence ? fence[1] : text).trim();
        const parsed = JSON.parse(cleaned);
        const merges = Array.isArray(parsed.merges) ? parsed.merges : [];
        for (const m of merges) {
            if (typeof m?.from === 'string' && typeof m?.to === 'string' && m.from !== m.to) {
                mergeMap.set(m.from, m.to);
            }
        }
        mergeCount = mergeMap.size;
    } catch (err: any) {
        ctx.log(`[Bucketing ${runId}] identity consolidation LLM call failed (non-fatal): ${err.message}`, 'warn');
        return { merges: 0, costUsd };
    }

    // Apply: empty target = drop identity (and its child sub-identity, since
    // sub-identity is parented to identity — orphaned sub-identities from
    // a dropped identity rarely make sense). Sector is independent and stays.
    for (const t of taggings) {
        if (t.identity && mergeMap.has(t.identity)) {
            const target = mergeMap.get(t.identity) || '';
            if (target.trim()) {
                t.identity = target;
            } else {
                t.identity = null;
                t.sub_identity = null;
            }
        }
    }
    return { merges: mergeCount, costUsd };
}

// ────────────────────────────────────────────────────────────────────
// AI-PROPOSED-ROUTING SUGGESTIONS  (one-shot per-run, manual trigger)
// ────────────────────────────────────────────────────────────────────
//
// The "AI-proposed taxonomy additions" panel shows the tagger's
// is_new_* coinings. Today the reviewer decides one row at a time
// whether to accept-as-new or route-to-existing-library-entry. This
// function asks Claude Opus 4.7 once per layer to pre-decide every
// row: route OR accept_as_new, with 1-10 confidence and a one-line
// reason. Results persist on bucketing_runs.ai_proposal_suggestions
// and are read back inline by GET /proposed-tags so the UI can
// pre-fill the dropdown / pre-tick the row checkbox.
//
// Conservative model choice: Opus 4.7. Worst-case cost on a ~250-
// proposal run is well under $1 (3 LLM calls, ~5k input + ~7.5k
// output tokens total). Cancellation: the per-run AbortController
// piped via ctx.abortSignal kills the in-flight Anthropic call.
const SUGGEST_ROUTINGS_MODEL = 'claude-opus-4-7';
const SUGGEST_ROUTINGS_MAX_TOKENS = 8000;

type SuggestLayer = 'identities' | 'sub_identities' | 'sectors';

interface LoadedProposalBase {
    name: string;
    samples: string[];
    count: number;
}
interface LoadedIdentityProposal extends LoadedProposalBase {
    // Top-2 sectors / sub-identities most commonly co-occurring with this
    // proposed identity in this run. Helps the router disambiguate when
    // the proposal name alone is ambiguous.
    topSectors: Array<{ name: string; count: number }>;
    topSubs: Array<{ name: string; count: number }>;
}
interface LoadedSubProposal extends LoadedProposalBase {
    parent: string;
    topSectors: Array<{ name: string; count: number }>;
}
interface LoadedSectorProposal extends LoadedProposalBase {
    topIdentities: Array<{ name: string; count: number }>;
    topSubs: Array<{ name: string; parent: string; count: number }>;
}

interface LoadedProposals {
    identities: LoadedIdentityProposal[];
    sub_identities: LoadedSubProposal[];
    sectors: LoadedSectorProposal[];
}

function topN<K>(counter: Map<K, number>, n: number): Array<{ key: K; count: number }> {
    return Array.from(counter.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, n)
        .map(([key, count]) => ({ key, count }));
}

// Lifted from server.ts GET /proposed-tags so the suggestion service
// can pull the same shape without duplicating the aggregation. The
// route handler now delegates here. Also computes cross-layer
// co-occurrence (top-2 sectors / identities / subs) per proposal so
// the router can use it as extra signal — see buildSuggestPrompt.
export async function loadRunProposals(
    supabase: SupabaseClient,
    runId: string
): Promise<LoadedProposals> {
    const { data, error } = await supabase
        .from('bucket_industry_map')
        .select('primary_identity,is_new_identity,sub_identity,is_new_sub_identity,sector,is_new_sector,industry_string')
        .eq('bucketing_run_id', runId)
        .or('is_new_identity.eq.true,is_new_sub_identity.eq.true,is_new_sector.eq.true');
    if (error) throw new Error(error.message);

    type IdEntry = LoadedIdentityProposal & { _sectors: Map<string, number>; _subs: Map<string, number> };
    type SubEntry = LoadedSubProposal & { _sectors: Map<string, number> };
    type SecEntry = LoadedSectorProposal & { _identities: Map<string, number>; _subs: Map<string, number> };

    const ids = new Map<string, IdEntry>();
    const chars = new Map<string, SubEntry>();
    const secs = new Map<string, SecEntry>();
    for (const r of (data || []) as any[]) {
        if (r.is_new_identity && r.primary_identity) {
            let e = ids.get(r.primary_identity);
            if (!e) {
                e = { name: r.primary_identity, samples: [], count: 0,
                      topSectors: [], topSubs: [],
                      _sectors: new Map(), _subs: new Map() };
                ids.set(r.primary_identity, e);
            }
            e.count++;
            if (e.samples.length < 5) e.samples.push(r.industry_string);
            if (r.sector) e._sectors.set(r.sector, (e._sectors.get(r.sector) || 0) + 1);
            if (r.sub_identity) e._subs.set(r.sub_identity, (e._subs.get(r.sub_identity) || 0) + 1);
        }
        if (r.is_new_sub_identity && r.sub_identity) {
            let e = chars.get(r.sub_identity);
            if (!e) {
                e = { name: r.sub_identity, parent: r.primary_identity || '',
                      samples: [], count: 0, topSectors: [], _sectors: new Map() };
                chars.set(r.sub_identity, e);
            }
            e.count++;
            if (e.samples.length < 5) e.samples.push(r.industry_string);
            if (r.sector) e._sectors.set(r.sector, (e._sectors.get(r.sector) || 0) + 1);
        }
        if (r.is_new_sector && r.sector) {
            let e = secs.get(r.sector);
            if (!e) {
                e = { name: r.sector, samples: [], count: 0,
                      topIdentities: [], topSubs: [],
                      _identities: new Map(), _subs: new Map() };
                secs.set(r.sector, e);
            }
            e.count++;
            if (e.samples.length < 5) e.samples.push(r.industry_string);
            if (r.primary_identity) e._identities.set(r.primary_identity, (e._identities.get(r.primary_identity) || 0) + 1);
            if (r.sub_identity) {
                const key = `${r.sub_identity}|${r.primary_identity || ''}`;
                e._subs.set(key, (e._subs.get(key) || 0) + 1);
            }
        }
    }

    const idsOut: LoadedIdentityProposal[] = Array.from(ids.values()).map(e => ({
        name: e.name, samples: e.samples, count: e.count,
        topSectors: topN(e._sectors, 2).map(t => ({ name: t.key, count: t.count })),
        topSubs:    topN(e._subs,    2).map(t => ({ name: t.key, count: t.count })),
    }));
    const subsOut: LoadedSubProposal[] = Array.from(chars.values()).map(e => ({
        name: e.name, parent: e.parent, samples: e.samples, count: e.count,
        topSectors: topN(e._sectors, 2).map(t => ({ name: t.key, count: t.count })),
    }));
    const secsOut: LoadedSectorProposal[] = Array.from(secs.values()).map(e => ({
        name: e.name, samples: e.samples, count: e.count,
        topIdentities: topN(e._identities, 2).map(t => ({ name: t.key, count: t.count })),
        topSubs:       topN(e._subs, 2).map(t => {
            const [name, parent] = t.key.split('|');
            return { name, parent, count: t.count };
        }),
    }));

    return {
        identities: idsOut.sort((a, b) => b.count - a.count),
        sub_identities: subsOut.sort((a, b) => b.count - a.count),
        sectors: secsOut.sort((a, b) => b.count - a.count),
    };
}

async function loadProposedContactCounts(
    supabase: SupabaseClient,
    runId: string
): Promise<Record<string, number>> {
    const { data, error } = await supabase.rpc('get_proposed_tag_contact_counts', { p_run_id: runId });
    if (error) return {};
    const map: Record<string, number> = {};
    for (const r of (data || []) as any[]) {
        if (r?.layer && r?.name) map[`${r.layer}:${r.name}`] = Number(r.contact_count || 0);
    }
    return map;
}

// Up to 3 example industry strings per library entry (identities by
// name; sub-identities keyed by `${name}|${parent}`; sectors by name).
// Sampled from past confirmed mappings in bucket_industry_map (where
// is_new_* = false). Used by buildSuggestPrompt so the router sees
// concrete usage of each library entry, not just the (often thin)
// description text. Keeps overhead bounded by sampling the most
// recent 30k confirmed rows rather than the full table.
interface LibraryExamples {
    identities:     Record<string, string[]>;   // key: name
    sub_identities: Record<string, string[]>;   // key: `${name}|${parent}`
    sectors:        Record<string, string[]>;   // key: name
}
async function loadLibraryExamples(supabase: SupabaseClient): Promise<LibraryExamples> {
    const out: LibraryExamples = { identities: {}, sub_identities: {}, sectors: {} };
    const { data, error } = await supabase
        .from('bucket_industry_map')
        .select('primary_identity,is_new_identity,sub_identity,is_new_sub_identity,sector,is_new_sector,industry_string')
        .order('id', { ascending: false })
        .limit(30000);
    if (error || !data) return out;
    const push = (bucket: Record<string, string[]>, key: string, sample: string) => {
        if (!bucket[key]) bucket[key] = [];
        if (bucket[key].length < 3 && sample && !bucket[key].includes(sample)) bucket[key].push(sample);
    };
    for (const r of data as any[]) {
        if (!r.is_new_identity && r.primary_identity) push(out.identities, r.primary_identity, r.industry_string);
        if (!r.is_new_sub_identity && r.sub_identity && r.primary_identity) {
            push(out.sub_identities, `${r.sub_identity}|${r.primary_identity}`, r.industry_string);
        }
        if (!r.is_new_sector && r.sector) push(out.sectors, r.sector, r.industry_string);
    }
    return out;
}

interface RawSuggestion {
    name: string;
    parent?: string;
    decision: 'route' | 'new' | 'wrong_layer';
    route_to?: string;
    route_to_parent?: string;
    // Cross-layer routing target. Only meaningful for IDENTITY proposals
    // today — set to 'sub_identities' when the LLM judges the proposal
    // really belongs to a sub-identity (e.g. "Holding Company" → "Venture
    // Capital" under "Financial Services"). Omitted for same-layer routes.
    route_to_layer?: 'identities' | 'sub_identities';
    confidence: number;
    reason: string;
    alt_routes?: Array<{
        route_to: string;
        route_to_parent?: string;
        route_to_layer?: 'identities' | 'sub_identities';
        confidence: number;
        reason: string;
    }>;
}

function layerGuidance(layer: SuggestLayer): string {
    if (layer === 'identities') {
        return `IDENTITY proposals (Layer 1 — the company's top-level business model). Identities are stable, curated, and adding a new one is cheap. Route ONLY when the proposal is an OBVIOUS paraphrase of an existing identity ("Insurance" → "Insurance Services"; "PE Firm" → "Financial Services"). DO NOT route to a "least-wrong" neighbour — return decision=new instead. NEVER route at confidence ≥ 5 unless it's a true synonym.

CROSS-LAYER (identity → sub-identity): when the proposal is really a SUB-IDENTITY rather than a top-level identity (e.g. "Holding Company" is too narrow to be its own identity but fits cleanly as "Venture Capital" under "Financial Services"), route to that SUB-IDENTITY by setting route_to to the sub-identity name, route_to_parent to its parent identity, AND route_to_layer="sub_identities". The library block lists sub-identity entries with the "(sub under <Parent>)" suffix so you can pick them. Confidence rules unchanged — only set confidence ≥ 7 when the sub is a clear semantic fit; ≤ 4 when it's the least-wrong of several. This atomically rewrites BOTH the proposal's primary_identity AND sub_identity on every matching contact row — use it instead of "new" whenever a sub-identity is the right answer.

Inputs like "Professional Services" / "B2B" / "Subscription" are not identities — return decision=new at confidence ≤ 4 and the human will reject. "Holding Company" specifically: prefer the cross-layer sub-identity route above over decision=new.`;
    }
    if (layer === 'sub_identities') {
        return `SUB-IDENTITY proposals (Layer 2 — functional subtype under an identity). Each proposal lists a parent identity. Sub-identities fragment heavily; prefer route. Same-parent route is the default. Cross-parent routes (route_to_parent ≠ proposal's parent) are allowed but MUST cap confidence ≤ 4 — they silently re-parent contacts and need human review. If the proposal's NAME is itself an identity name ("Manufacturing & Industrial" used as a sub-identity) or a sector name ("Life Sciences & Biotech" used as a sub-identity), return decision=wrong_layer with the reason naming the correct layer — DO NOT route, DO NOT accept_as_new.`;
    }
    return `SECTOR proposals (Layer 3 — the vertical the company SERVES, not the company's identity). Sector library is curated; prefer route for genuine verticals. NULL or "general" proposals: read the samples — if they're a clear vertical (e.g. CPG → Food & Beverage), route at confidence ≥ 6. Inputs that are NOT sectors (Marketing, Advertising, IT Services, Consulting, Professional Services, B2B, Corporate, Subscription, Holding Company): route to the LEAST WRONG existing sector at confidence ≤ 3 — NEVER decision=new for those. If the proposal's NAME is an identity name ("Software & SaaS" used as a sector) or a sub-identity name used as a sector, return decision=wrong_layer. For genuine verticals not in the library (e.g. a real "Public Safety" or "Mining" with strong vertical samples), decision=new at confidence ≥ 7 is fine.`;
}

function buildSuggestPrompt(
    layer: SuggestLayer,
    proposals: LoadedProposals[SuggestLayer],
    libEntries: TaxonomyEntry[],
    contactCounts: Record<string, number>,
    libraryExamples: Record<string, string[]>,
    // Extra library entries from a DIFFERENT layer that are valid cross-
    // layer route targets. Today: only populated for the identity layer
    // (passed the sub-identity library). The LLM sees them with a
    // "(sub under <Parent>)" suffix in the LIBRARY block.
    crossLayerEntries?: TaxonomyEntry[],
): { system: string; user: string } {
    const layerCountKey = layer === 'identities' ? 'identity' : layer === 'sub_identities' ? 'sub_identity' : 'sector';

    const system = `You route AI-proposed B2B taxonomy entries to an existing curated library. For each PROPOSAL choose EXACTLY ONE of:
  - "route" — the proposal is a paraphrase, spelling variant, or narrower vertical case of an existing LIBRARY entry. Set route_to (and route_to_parent for sub-identities) to the EXACT library name, character-for-character.
  - "new" — the proposal is a genuine concept missing from the library. The reviewer will accept it as a new library entry.
  - "wrong_layer" — the proposal's NAME belongs to a different taxonomy layer (e.g. an identity name being used as a sub-identity, a sector name being used as a sub-identity). Do NOT route, do NOT accept_as_new. The reviewer will move it to the right layer manually.

WHEN TO ROUTE vs. NEW:
  - Route only when the proposal is one of: exact synonym, spelling/suffix variant, narrower vertical case, or the same concept under a different name. "Closest existing entry" is NOT a valid reason to route — if no entry is a paraphrase or narrower case, return "new".
  - A new library entry is cheap. A wrong route silently mis-tags every contact behind that proposal — that's far worse than asking the human to accept a new entry.

LAYER GUIDANCE — read carefully, this overrides general intuition:
${layerGuidance(layer)}

CROSS-PARENT MOVES (sub-identities only):
  - Default: route_to_parent equals the proposal's listed parent.
  - If the library has the right entry under a DIFFERENT parent, you may route there, but cap confidence at 4 — cross-parent routes re-parent contacts and need a human pass.

CONFIDENCE (1-10) — use the full range:
  9-10 — exact synonym or clearly novel concept with no plausible library mapping.
  6-8  — strong match; difference is suffix / pluralization / vertical narrowing.
  3-5  — defensible but disputable. Cross-parent route, or route to a near-but-not-quite entry.
  1-2  — uncertain; both route and new feel wrong. Last-resort least-wrong route.

REASONING:
  - ≤ 30 words. ONE sentence. Cite the library entry's name when routing. Name the correct layer when wrong_layer.

ALTERNATIVE ROUTES (decision=route only):
  - In addition to your top pick (route_to / confidence), you MAY include up to 2 alternative library targets in alt_routes when more than one library entry is plausibly a paraphrase or narrower case of the proposal. The reviewer will see all of them as clickable picks and choose.
  - Order alt_routes by descending confidence; each alt's confidence MUST be ≤ the primary's confidence.
  - Skip alt_routes (omit the field, or empty array) when there is no genuine second-best match — don't pad with "least-wrong" entries. Better to give 1 high-confidence pick than 3 mediocre ones.
  - Each alt obeys the same validation as the primary: the route_to MUST exist in the LIBRARY block; for sub-identities the route_to_parent MUST be that entry's listed parent (or cap confidence ≤ 4 if cross-parent, same rule as the primary).
  - Never repeat the primary route_to in alt_routes. Each alt must be a distinct library entry.

OUTPUT — strict JSON, single object with key "suggestions":
{
  "suggestions": [
    { "name": "<proposal name verbatim>",
      "parent": "<parent identity name verbatim>",        // sub_identities only
      "decision": "route" | "new" | "wrong_layer",
      "route_to": "<library entry name verbatim>",        // when decision = route (top pick)
      "route_to_parent": "<library parent identity>",     // when target is a sub-identity (same-layer sub OR cross-layer from identity)
      "route_to_layer": "sub_identities",                 // OPTIONAL, identity proposals only, when routing to a SUB-identity in the library
      "confidence": 1-10,
      "reason": "...",
      "alt_routes": [                                     // OPTIONAL, decision=route only, max 2 entries
        { "route_to": "<library entry name verbatim>",
          "route_to_parent": "<library parent identity>", // when alt's target is a sub-identity
          "route_to_layer": "sub_identities",             // OPTIONAL, same semantics as primary
          "confidence": 1-10,
          "reason": "..." }
      ]
    }
  ]
}

HARD RULES:
  - One object per PROPOSAL in the user message. Do not drop any.
  - When decision=route, route_to MUST appear in the LIBRARY block. Copy character-for-character. Do NOT invent names.
  - For sub-identities + route, route_to_parent MUST match the library entry's listed parent (after the cross-parent confidence cap above).
  - For IDENTITY proposals routed cross-layer to a sub-identity, set route_to_layer="sub_identities" and route_to_parent to the sub's parent identity. route_to must appear in the library block under the "(sub under <Parent>)" suffix.
  - When decision=new or wrong_layer, omit route_to / route_to_parent / route_to_layer / alt_routes.
  - Output ONLY the JSON object. No prose, no markdown fences.`;

    // Library block — each entry gets its description AND up to 3 concrete
    // example industries from past confirmed mappings, so the router sees
    // what the entry actually contains rather than just a one-line gloss.
    const sameLayerBlock = libEntries.map(e => {
        const key = layer === 'sub_identities' ? `${e.name}|${e.parent_identity || ''}` : e.name;
        const exs = libraryExamples[key] || [];
        const exsStr = exs.length ? ` · ex: ${exs.map(s => `"${s}"`).join(', ')}` : '';
        const desc = e.description ? `: ${e.description}` : '';
        if (layer === 'sub_identities') {
            return `  - "${e.name}" (under "${e.parent_identity || ''}")${desc}${exsStr}`;
        }
        return `  - "${e.name}"${desc}${exsStr}`;
    }).join('\n');

    // Cross-layer block. For identity proposals we expose the sub-identity
    // library so the LLM can route a too-narrow identity proposal (e.g.
    // "Holding Company") to a sub-identity ("Venture Capital" under
    // "Financial Services") in one step. Description only — examples are
    // looked up by layer-scoped `libraryExamples` and aren't passed for the
    // cross layer (keeps the surface area small; the description carries
    // enough signal for the route decision).
    const crossBlock = (layer === 'identities' && crossLayerEntries && crossLayerEntries.length > 0)
        ? '\n\n  CROSS-LAYER TARGETS (sub-identities — for the cross-layer route described in LAYER GUIDANCE; set route_to_layer="sub_identities" + route_to_parent when picking one):\n'
            + crossLayerEntries.map(e => {
                const desc = e.description ? `: ${e.description}` : '';
                return `  - "${e.name}" (sub under "${e.parent_identity || ''}")${desc}`;
            }).join('\n')
        : '';
    const libBlock = sameLayerBlock + crossBlock;

    // Proposal block — 5 samples (not 3) plus cross-layer co-occurrence so
    // the router can disambiguate identity-vs-sector confusion. E.g. a sub
    // proposal "Manufacturing & Industrial" whose top sector is also
    // "Manufacturing" is almost certainly a Vertical SaaS routing case.
    const propBlock = (proposals as any[]).map((p: any) => {
        const cc = contactCounts[`${layerCountKey}:${p.name}`];
        const ccStr = cc !== undefined ? `${cc.toLocaleString()} contacts · ` : '';
        const samples = (p.samples || []).slice(0, 5).map((s: string) => `"${s}"`).join(', ');
        const co: string[] = [];
        if (layer === 'identities') {
            if (p.topSectors?.length) co.push(`top sectors: ${p.topSectors.map((x: any) => `${x.name}(${x.count})`).join(', ')}`);
            if (p.topSubs?.length)    co.push(`top subs: ${p.topSubs.map((x: any) => `${x.name}(${x.count})`).join(', ')}`);
        } else if (layer === 'sub_identities') {
            if (p.topSectors?.length) co.push(`top sectors: ${p.topSectors.map((x: any) => `${x.name}(${x.count})`).join(', ')}`);
        } else {
            if (p.topIdentities?.length) co.push(`top identities: ${p.topIdentities.map((x: any) => `${x.name}(${x.count})`).join(', ')}`);
            if (p.topSubs?.length)       co.push(`top subs: ${p.topSubs.map((x: any) => `${x.name}(${x.count})`).join(', ')}`);
        }
        const coStr = co.length ? ` · ${co.join(' · ')}` : '';
        if (layer === 'sub_identities') {
            return `  - "${p.name}" (proposed under "${p.parent || ''}") — ${ccStr}${p.count}× industries · samples: ${samples}${coStr}`;
        }
        return `  - "${p.name}" — ${ccStr}${p.count}× industries · samples: ${samples}${coStr}`;
    }).join('\n');

    const user = `LIBRARY (${layer}, archived entries excluded — entries marked "ex:" show real industries already mapped there):
${libBlock}

PROPOSALS to route (${(proposals as any[]).length} total). Each row shows: name, contact count, distinct-industry count, sample industry strings, and top co-occurring values from other layers:
${propBlock}

Produce one suggestion object per proposal, in the exact order listed.`;

    return { system, user };
}

async function callSuggestLLM(
    supabase: SupabaseClient,
    layer: SuggestLayer,
    proposals: LoadedProposals[SuggestLayer],
    libEntries: TaxonomyEntry[],
    contactCounts: Record<string, number>,
    libraryExamples: Record<string, string[]>,
    signal: AbortSignal | undefined,
    log: (m: string, level?: 'info' | 'warn') => void,
    crossLayerEntries?: TaxonomyEntry[],
): Promise<{ suggestions: RawSuggestion[]; cost: number }> {
    const anthropic = await getAnthropic(supabase);
    if (!anthropic) throw new Error('Anthropic API key not configured. Add it on the Connectors page (saved as ANTHROPIC_API_KEY).');

    const { system, user } = buildSuggestPrompt(layer, proposals, libEntries, contactCounts, libraryExamples, crossLayerEntries);
    const resp = await anthropic.messages.create({
        model: SUGGEST_ROUTINGS_MODEL,
        max_tokens: SUGGEST_ROUTINGS_MAX_TOKENS,
        system,
        messages: [{ role: 'user', content: user }],
    }, { signal: combinedAbortSignal(TAXONOMY_TIMEOUT_MS, signal) });

    const usage: any = (resp as any).usage || {};
    const cost = computeAnthropicCost(SUGGEST_ROUTINGS_MODEL, usage.input_tokens || 0, usage.output_tokens || 0);
    const text = (resp.content as any[]).filter(b => b.type === 'text').map(b => b.text).join('\n');

    // Strict parse first; regex recovery on truncation — same pattern as
    // consolidateSubIdentitiesViaLLM above. Pulls complete suggestion
    // objects out of the prefix even when the trailing array is cut.
    const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
    const cleaned = (fence ? fence[1] : text).trim();
    let suggestions: RawSuggestion[] = [];
    try {
        const parsed = JSON.parse(cleaned);
        if (Array.isArray(parsed.suggestions)) suggestions = parsed.suggestions as RawSuggestion[];
    } catch {
        // Match balanced { ... } chunks at the top level of the array.
        // Depth-aware so route_to_parent etc. don't trip a naive regex.
        const start = cleaned.indexOf('[');
        if (start >= 0) {
            let depth = 0;
            let objStart = -1;
            const chunks: string[] = [];
            for (let i = start; i < cleaned.length; i++) {
                const ch = cleaned[i];
                if (ch === '{') {
                    if (depth === 0) objStart = i;
                    depth++;
                } else if (ch === '}') {
                    depth--;
                    if (depth === 0 && objStart >= 0) {
                        chunks.push(cleaned.slice(objStart, i + 1));
                        objStart = -1;
                    }
                }
            }
            for (const c of chunks) {
                try {
                    const obj = JSON.parse(c);
                    if (obj && typeof obj.name === 'string') suggestions.push(obj as RawSuggestion);
                } catch { /* skip malformed */ }
            }
        }
        if (suggestions.length === 0) {
            throw new Error(`unrecoverable parse failure (${cleaned.length} chars)`);
        }
        log(`[${layer}] JSON truncated — recovered ${suggestions.length} via regex`, 'warn');
    }
    return { suggestions, cost };
}

function clampConfidence(n: any): number {
    const v = Number(n);
    if (!Number.isFinite(v)) return 5;
    if (v < 1) return 1;
    if (v > 10) return 10;
    return Math.round(v);
}

export async function suggestProposalRoutings(
    supabase: SupabaseClient,
    runId: string,
    ctx?: { log?: (m: string, level?: 'info' | 'warn') => void; abortSignal?: AbortSignal }
): Promise<{
    counts: { identities: number; sub_identities: number; sectors: number };
    costUsd: number;
    model: string;
    blob: ProposalSuggestionsBlob;
}> {
    const log = ctx?.log ?? ((m: string) => console.log(`[suggest-routings ${runId}] ${m}`));
    const signal = ctx?.abortSignal;

    const snapshot = await loadTaxonomySnapshot(supabase);
    const proposals = await loadRunProposals(supabase, runId);
    const contactCounts = await loadProposedContactCounts(supabase, runId);
    const libraryExamples = await loadLibraryExamples(supabase);

    const totalProposals = proposals.identities.length + proposals.sub_identities.length + proposals.sectors.length;
    if (totalProposals === 0) {
        log('no proposals — writing empty blob');
        const empty: ProposalSuggestionsBlob = {
            identities: {}, sub_identities: {}, sectors: {},
            _meta: {
                model: SUGGEST_ROUTINGS_MODEL,
                cost_usd: 0,
                generated_at: new Date().toISOString(),
                counts: { identities: 0, sub_identities: 0, sectors: 0 },
            },
        };
        await persistSuggestionsBlob(supabase, runId, empty);
        return { counts: empty._meta!.counts, costUsd: 0, model: SUGGEST_ROUTINGS_MODEL, blob: empty };
    }

    const layers: SuggestLayer[] = ['identities', 'sub_identities', 'sectors'];
    const results = await Promise.all(layers.map(async (layer) => {
        const libEntries = snapshot[layer];
        const layerProposals = proposals[layer] as any[];
        if (layerProposals.length === 0) {
            return { layer, suggestions: [] as RawSuggestion[], cost: 0 };
        }
        if (libEntries.length === 0) {
            // No library to route into — force accept_as_new for every proposal.
            log(`[${layer}] library empty — defaulting all ${layerProposals.length} proposals to accept_as_new`);
            return {
                layer,
                cost: 0,
                suggestions: layerProposals.map((p: any) => ({
                    name: p.name,
                    parent: p.parent,
                    decision: 'new' as const,
                    confidence: 10,
                    reason: 'No library entries exist for this layer yet; accepting as new.',
                })),
            };
        }
        try {
            const layerExamples = libraryExamples[layer];
            // Identity proposals get the sub-identity library as cross-layer
            // route targets — see CROSS-LAYER guidance in the prompt.
            const crossLayer = layer === 'identities' ? snapshot.sub_identities : undefined;
            const r = await callSuggestLLM(supabase, layer, layerProposals as any, libEntries, contactCounts, layerExamples, signal, log, crossLayer);
            log(`[${layer}] ${r.suggestions.length} suggestions for ${layerProposals.length} proposals · $${r.cost.toFixed(4)}`);
            return { layer, suggestions: r.suggestions, cost: r.cost };
        } catch (err: any) {
            log(`[${layer}] LLM call failed: ${err.message}`, 'warn');
            throw err;
        }
    }));

    const blob: ProposalSuggestionsBlob = { identities: {}, sub_identities: {}, sectors: {} };
    let totalCost = 0;
    // Cross-layer route targets, layer-keyed. Today only the identity
    // layer is allowed to route cross-layer (to sub-identities). Lookup
    // by name + parent.
    const crossLayerLibs: Record<SuggestLayer, TaxonomyEntry[]> = {
        identities: snapshot.sub_identities,
        sub_identities: [],
        sectors: [],
    };
    for (const r of results) {
        totalCost += r.cost;
        const layerLib = snapshot[r.layer];
        const crossLib = crossLayerLibs[r.layer];
        for (const raw of r.suggestions) {
            if (!raw || typeof raw.name !== 'string') continue;
            const out: ProposalSuggestion = {
                confidence: clampConfidence(raw.confidence),
                reason: String(raw.reason || '').slice(0, 240),
            };
            if (raw.decision === 'wrong_layer') {
                // Surface "the proposal name belongs to a different layer"
                // to the human. No route, no accept_as_new — the UI shows a
                // distinct badge and leaves the row untouched for manual
                // handling.
                out.wrong_layer = true;
            } else {
                const wantRoute = raw.decision === 'route' && typeof raw.route_to === 'string' && raw.route_to.trim();
                // Cross-layer route: identity proposal pointing at a
                // sub-identity target. Requires route_to_layer="sub_identities"
                // AND a route_to_parent that exists alongside route_to in the
                // sub-identity library. Same-layer routes leave route_to_layer
                // unset.
                const wantCrossLayer = wantRoute && r.layer === 'identities' && raw.route_to_layer === 'sub_identities';
                if (wantRoute) {
                    const exists = wantCrossLayer
                        ? crossLib.some(e => e.name === raw.route_to && e.parent_identity === raw.route_to_parent)
                        : layerLib.some(e =>
                            e.name === raw.route_to &&
                            (r.layer !== 'sub_identities' || e.parent_identity === raw.route_to_parent)
                          );
                    if (exists) {
                        out.route_to = raw.route_to;
                        if (wantCrossLayer) {
                            out.route_to_layer = 'sub_identities';
                            out.route_to_parent = raw.route_to_parent;
                        } else if (r.layer === 'sub_identities') {
                            out.route_to_parent = raw.route_to_parent;
                            // Cross-parent routes silently re-parent contacts.
                            // Cap confidence at 4 so the human reviews even if
                            // the model returned higher.
                            if (raw.route_to_parent && raw.parent && raw.route_to_parent !== raw.parent) {
                                if (out.confidence > 4) {
                                    out.confidence = 4;
                                    out.reason = `(cross-parent: ${raw.parent} → ${raw.route_to_parent}) ${out.reason}`.slice(0, 240);
                                }
                            }
                        }
                        // Validate alt_routes — same checks as the primary.
                        // Drop any alt whose target doesn't exist; dedupe
                        // against the primary; apply the cross-parent cap.
                        // Cap to 2 alts even if the model returns more.
                        if (Array.isArray(raw.alt_routes) && raw.alt_routes.length > 0) {
                            // The dedupe key folds layer + name + parent so a
                            // same-name sub-identity under a different parent
                            // counts as a distinct alt.
                            const primaryKey = (out.route_to_layer === 'sub_identities' || r.layer === 'sub_identities')
                                ? `${out.route_to_layer || r.layer}:${out.route_to}|${out.route_to_parent || ''}`
                                : `${r.layer}:${out.route_to}`;
                            const seenKeys = new Set<string>([primaryKey]);
                            const alts: NonNullable<ProposalSuggestion['alt_routes']> = [];
                            for (const a of raw.alt_routes) {
                                if (alts.length >= 2) break;
                                if (!a || typeof a.route_to !== 'string' || !a.route_to.trim()) continue;
                                const altCrossLayer = r.layer === 'identities' && a.route_to_layer === 'sub_identities';
                                const effLayer: SuggestLayer = altCrossLayer ? 'sub_identities' : r.layer;
                                const altKey = (effLayer === 'sub_identities')
                                    ? `${effLayer}:${a.route_to}|${a.route_to_parent || ''}`
                                    : `${effLayer}:${a.route_to}`;
                                if (seenKeys.has(altKey)) continue;
                                const altExists = altCrossLayer
                                    ? crossLib.some(e => e.name === a.route_to && e.parent_identity === a.route_to_parent)
                                    : layerLib.some(e =>
                                        e.name === a.route_to &&
                                        (r.layer !== 'sub_identities' || e.parent_identity === a.route_to_parent)
                                      );
                                if (!altExists) continue;
                                let altConf = clampConfidence(a.confidence);
                                let altReason = String(a.reason || '').slice(0, 240);
                                if (r.layer === 'sub_identities'
                                    && a.route_to_parent && raw.parent
                                    && a.route_to_parent !== raw.parent
                                    && altConf > 4) {
                                    altConf = 4;
                                    altReason = `(cross-parent: ${raw.parent} → ${a.route_to_parent}) ${altReason}`.slice(0, 240);
                                }
                                // Alt confidence must not exceed the primary.
                                altConf = Math.min(altConf, out.confidence);
                                alts.push({
                                    route_to: a.route_to,
                                    route_to_parent: effLayer === 'sub_identities' ? a.route_to_parent : undefined,
                                    route_to_layer: altCrossLayer ? 'sub_identities' : undefined,
                                    confidence: altConf,
                                    reason: altReason,
                                });
                                seenKeys.add(altKey);
                            }
                            if (alts.length > 0) out.alt_routes = alts;
                        }
                    } else {
                        // Hallucinated primary target — downgrade to accept_as_new at low confidence.
                        out.accept_as_new = true;
                        out.confidence = Math.min(out.confidence, 4);
                        out.reason = `(invalid route target "${raw.route_to}") ${out.reason}`.slice(0, 240);
                    }
                } else {
                    out.accept_as_new = true;
                }
            }
            const key = r.layer === 'sub_identities'
                ? `${raw.name}|${raw.parent || ''}`
                : raw.name;
            (blob[r.layer] as Record<string, ProposalSuggestion>)[key] = out;
        }
    }

    blob._meta = {
        model: SUGGEST_ROUTINGS_MODEL,
        cost_usd: totalCost,
        generated_at: new Date().toISOString(),
        counts: {
            identities:     Object.keys(blob.identities || {}).length,
            sub_identities: Object.keys(blob.sub_identities || {}).length,
            sectors:        Object.keys(blob.sectors || {}).length,
        },
    };
    await persistSuggestionsBlob(supabase, runId, blob);
    const n = blob._meta.counts.identities + blob._meta.counts.sub_identities + blob._meta.counts.sectors;
    log(`done — ${n} suggestions · $${totalCost.toFixed(4)}`);
    return { counts: blob._meta.counts, costUsd: totalCost, model: SUGGEST_ROUTINGS_MODEL, blob };
}

async function persistSuggestionsBlob(
    supabase: SupabaseClient,
    runId: string,
    blob: ProposalSuggestionsBlob,
): Promise<void> {
    const { error } = await supabase
        .from('bucketing_runs')
        .update({ ai_proposal_suggestions: blob, ai_proposal_suggestions_status: null })
        .eq('id', runId);
    if (error) throw new Error(`persist suggestions failed: ${error.message}`);
}

// Deterministic post-pass: any proposed-new identity with count < 3 gets
// dropped (the row's identity + sub-identity become null and the contact
// rolls up to General via the cascade). Singletons fragment the library
// without earning a viable bucket — at min_volume, a 1× identity has no
// chance of clearing the floor. Runs AFTER the LLM consolidation pass so
// any near-duplicate that could've been merged has already been merged.
function dropSingletonNewIdentities(
    taggings: IndustryTagging[],
    snapshot: TaxonomySnapshot,
    runId: string,
    ctx: BucketingCtx
): { dropped: number; rerouted: number } {
    const libraryNames = new Set(snapshot.identities.map(i => i.name));
    const counts = new Map<string, number>();
    for (const t of taggings) {
        const i = t.identity;
        if (!i) continue;
        if (libraryNames.has(i)) continue;
        counts.set(i, (counts.get(i) || 0) + 1);
    }
    const singletons = new Set<string>();
    for (const [name, count] of counts) {
        if (count < 3) singletons.add(name);
    }
    if (singletons.size === 0) return { dropped: 0, rerouted: 0 };

    let rerouted = 0;
    for (const t of taggings) {
        if (t.identity && singletons.has(t.identity)) {
            t.identity = null;
            t.sub_identity = null;
            rerouted++;
        }
    }
    ctx.log(`[Bucketing ${runId}] dropped ${singletons.size} singleton new-identity proposals (count < 3) → re-routed ${rerouted} rows to General`);
    return { dropped: singletons.size, rerouted };
}

// ────────────────────────────────────────────────────────────────────
// EDIT APPLICATION
// ────────────────────────────────────────────────────────────────────

interface TaxonomyEdits {
    keep?: string[];                              // sub-identity names
    rename?: Record<string, string>;              // {old spec name: new spec name}
    add?: {
        sub_identity: string;
        primary_identity: string;
        description: string;
        identity_type?: string;
        operator_required?: boolean;
    }[];
    min_volume?: number;                          // sub-identity floor (legacy column name)
    identity_min_volume?: number;                 // identity floor — fold tiny identities into General
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
            leaves = leaves.map(b => b.sub_identity === oldName
                ? { ...b, sub_identity: target }
                : b);
        }
    }

    if (edits.keep) {
        const keepSet = new Set(edits.keep.map(s => s.trim()));
        leaves = leaves.filter(b => keepSet.has(b.sub_identity));
    }

    if (edits.add) {
        for (const a of edits.add) {
            const spec = (a.sub_identity || '').trim();
            const ident = (a.primary_identity || '').trim();
            if (!spec || RESERVED.has(spec.toLowerCase()) || !ident) continue;
            if (leaves.some(l => l.sub_identity === spec)) continue;
            leaves.push({
                sub_identity: spec,
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
    if (typeof edits.identity_min_volume === 'number' && edits.identity_min_volume >= 0) {
        update.identity_min_volume = edits.identity_min_volume;
    }
    if (Array.isArray(edits.preferred_library_ids)) {
        update.preferred_library_ids = edits.preferred_library_ids;
    }
    await supabase.from('bucketing_runs').update(update).eq('id', runId);
    ctx.log(`[Bucketing ${runId}] taxonomy edits applied: ${leaves.length} sub-identities`);

    // NOTE: a previous "preview rebuild" pass used to fire here. It DELETEd
    // all llm_phase1a rows for the run and re-routed via embedding
    // similarity. With the AI-proposed-additions panel (accept / route-to /
    // rename / re-parent), every per-industry tag rewrite already happens
    // atomically when the user takes the action — so the rebuild was
    // duplicative AND destructive (the embedding pass routinely returned
    // zero confident matches, leaving bucket_industry_map gutted down to
    // the general_passthrough rows). Removed deliberately; do not bring
    // it back. The user-defined Finalize step is what propagates accepted
    // edits to per-contact taxonomy.
}

// ────────────────────────────────────────────────────────────────────
// PHASE 1B — IDENTITY-FIRST MATCHING
// ────────────────────────────────────────────────────────────────────

export async function runAssignment(
    supabase: SupabaseClient,
    runId: string,
    ctx: BucketingCtx,
    _opts?: { resume?: boolean }   // resume is a no-op now; single-transaction rollup
): Promise<void> {
    // Phase 1b v2: deterministic, SQL-only.
    //
    // Old v1 ran a 4-stage cascade (JOIN → library → embedding → LLM) over
    // every contact + an in-memory assignedRows[] accumulator. That OOMed the
    // 2GB Render plan on 293k-contact runs and racked up unnecessary LLM cost.
    //
    // v2 reads taxonomy from Phase 1a (bucket_industry_map), applies volume
    // rollup (sub-identity if it has ≥ min_volume, else identity, else
    // General; is_disqualified → Disqualified), and writes bucket_contact_map
    // + bucket_assignments + bucket_industry_map.assigned_bucket_name in a
    // single transaction. Runs in seconds. Zero LLM calls.

    const schemaRes = await checkBucketingSchema(supabase);
    if (!schemaRes.ok) {
        ctx.log(`[Bucketing ${runId}] schema check failed:\n${schemaRes.summary}`, 'error');
        throw new Error(schemaRes.summary);
    }

    const { data: run, error } = await supabase
        .from('bucketing_runs').select('*').eq('id', runId).single();
    if (error || !run) throw new Error(`Run not found: ${error?.message}`);

    // Two independent thresholds from the run row.
    //   min_volume          (sub-identity floor) — UI defaults to 1000.
    //   identity_min_volume (identity floor)     — UI defaults to 100.
    // Either left null/0 falls back to 1 — at sub_min=1 every distinct
    // (identity, sub-identity) gets its own bucket; at identity_min=1
    // every identity with ≥1 contact gets its own bucket.
    const subMin = Number(run.min_volume) > 0 ? Number(run.min_volume) : 1;
    const idMin  = Number(run.identity_min_volume) > 0 ? Number(run.identity_min_volume) : 1;

    await supabase.from('bucketing_runs').update({
        status: 'assigning',
        cancel_requested: false,
        error_message: null,
        progress: {
            phase: 'phase1b',
            step: 'rollup',
            note: `Computing deterministic rollup at sub_min=${subMin}, identity_min=${idMin}…`,
            current: 0,
            total: Number(run.total_contacts || 0),
            elapsed_seconds: 0,
            updated_at: new Date().toISOString()
        }
    }).eq('id', runId);

    ctx.log(`[Bucketing ${runId}] Phase 1b v2 — deterministic rollup, sub_min=${subMin}, identity_min=${idMin}`, 'phase');
    const t0 = Date.now();
    const { data: result, error: rpcErr } = await supabase.rpc('apply_rollup_bucket_assignments', {
        p_run_id: runId,
        p_sub_min_volume: subMin,
        p_identity_min_volume: idMin
    });
    const ms = Date.now() - t0;
    if (rpcErr) {
        ctx.log(`[Bucketing ${runId}] rollup RPC failed after ${ms}ms: ${rpcErr.message}`, 'error');
        throw new Error(`rollup failed: ${rpcErr.message}`);
    }

    const r = (result || {}) as {
        total_contacts?: number;
        at_sub_identity?: number;
        rolled_up_to_identity?: number;
        general?: number;
        disqualified?: number;
        min_volume?: number;
    };
    ctx.log(
        `[Bucketing ${runId}] rollup complete in ${(ms / 1000).toFixed(2)}s — ` +
        `${(r.total_contacts || 0).toLocaleString()} contacts: ` +
        `${(r.at_sub_identity || 0).toLocaleString()} at sub-identity, ` +
        `${(r.rolled_up_to_identity || 0).toLocaleString()} rolled up to identity, ` +
        `${(r.general || 0).toLocaleString()} → General, ` +
        `${(r.disqualified || 0).toLocaleString()} → Disqualified`,
        'phase'
    );

    // The RPC already set status='completed' and assignment_completed_at, plus
    // a 100%-progress payload. Nothing else to write here.
    ctx.progress({
        phase: 'phase1b',
        step: 'done',
        current: r.total_contacts || 0,
        total: r.total_contacts || 0,
        note: `Rollup done — ${(r.total_contacts || 0).toLocaleString()} contacts assigned in ${(ms / 1000).toFixed(1)}s (no LLM cost)`
    });
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

function preRollupName(row: Pick<ContactMapRow, 'sector' | 'sub_identity' | 'primary_identity' | 'is_generic' | 'is_disqualified'>): string {
    if (row.is_disqualified) return RESERVED_DISQUALIFIED;
    if (row.is_generic) return RESERVED_GENERAL;
    if (row.sub_identity && row.sector && row.sector !== 'Multi-industry') {
        return `${row.sector} ${row.sub_identity}`;
    }
    if (row.sub_identity) return row.sub_identity;
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
    // Paginate at PostgREST's 1000-row hard cap. A single .range(0, 49999)
    // would silently return only 1000 rows, so runs with >1k distinct
    // industry strings would have most contacts miss the JOIN-first lookup
    // and waste LLM cycles in Phase 1b.
    const PAGE = 1000;
    let offset = 0;
    while (true) {
        const { data, error } = await supabase
            .from('bucket_industry_map')
            .select('industry_string,primary_identity,sub_identity,sector,confidence,identity_confidence,sub_identity_confidence,sector_confidence,is_generic,is_disqualified,llm_reason,source,assigned_bucket_name,assigned_bucket_primary_identity')
            .eq('bucketing_run_id', runId)
            .range(offset, offset + PAGE - 1);
        if (error) {
            ctx.log(`[Bucketing ${runId}] failed to load Phase 1a taxonomy map (offset=${offset}): ${error.message}`, 'error');
            return map;
        }
        const rows = (data || []) as any[];
        for (const row of rows) {
            if (row.industry_string) map.set(row.industry_string, row);
        }
        if (rows.length < PAGE) break;
        offset += PAGE;
        if (offset > 200_000) break; // hard ceiling — bucket_industry_map should never have this many rows for one run
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
        sub_identity: tax.sub_identity || '',
        sector: tax.sector || '',
        source: 'phase1a_taxonomy',
        confidence: Number(tax.confidence) || 0,
        identity_confidence: Number(tax.identity_confidence) || 0,
        sub_identity_confidence: Number(tax.sub_identity_confidence) || 0,
        sector_confidence: Number(tax.sector_confidence) || 0,
        leaf_score: Number(tax.confidence) || 0,
        ancestor_score: Number(tax.identity_confidence) || 0,
        root_score: Number(tax.identity_confidence) || 0,
        is_generic: !!tax.is_generic,
        is_disqualified: !!tax.is_disqualified,
        general_reason: tax.is_disqualified ? REASON.DISQUALIFIED_BY_LLM : null,
        reasons: { phase1a_source: tax.source, llm_reason: tax.llm_reason || null },
        // Carry the bucket-assignment output through to the contact row so
        // computeContactRollup can prefer it over the synthesized cascade.
        assigned_bucket_name: tax.assigned_bucket_name || null,
        assigned_bucket_primary_identity: tax.assigned_bucket_primary_identity || null
    });
}

function makeGeneralContactRow(
    runId: string,
    contact: ContactRouteInput,
    reason: string
): ContactMapRow {
    // Unclassifiable rows (failed enrichment, missing industry, scrape
    // errors) route to General — NOT Disqualified. Disqualified is
    // reserved for LLM-confident "not our ICP" verdicts where we did
    // successfully enrich + classify the contact and the taxonomy says
    // they're out of scope. Failed enrichment is a data-quality issue,
    // not a fit issue; the contact still belongs in the universe with
    // a "couldn't classify" reason so outreach can still pick them up
    // with a generic message if desired. is_generic=true marks them
    // for the General Breakdown panel.
    return {
        bucketing_run_id: runId,
        contact_id: contact.contact_id,
        industry_string: (contact.classification || contact.industry || '').trim(),
        primary_identity: '',
        sub_identity: '',
        sector: '',
        assigned_bucket_name: null,
        assigned_bucket_primary_identity: null,
        canonical_classification: 'General',
        bucket_reason: `Unclassifiable: ${reason}`,
        pre_rollup_bucket_name: RESERVED_GENERAL,
        bucket_name: RESERVED_GENERAL,
        identity_confidence: 0,
        sub_identity_confidence: 0,
        sector_confidence: 0,
        rollup_level: 'general',
        source: 'unclassifiable',
        confidence: 0,
        leaf_score: 0,
        ancestor_score: 0,
        root_score: 0,
        is_generic: true,
        is_disqualified: false,
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
        sub_identity: string;
        sector: string;
        source: string;
        confidence: number;
        identity_confidence?: number;
        sub_identity_confidence?: number;
        sector_confidence?: number;
        leaf_score: number;
        ancestor_score: number;
        root_score: number;
        is_generic?: boolean;
        is_disqualified?: boolean;
        general_reason?: string | null;
        reasons?: Record<string, any>;
        // Bucket-assignment output (when present, computeContactRollup
        // uses these instead of the synthesized combo→identity cascade).
        assigned_bucket_name?: string | null;
        assigned_bucket_primary_identity?: string | null;
    }
): ContactMapRow {
    const canonical = params.is_disqualified
        ? 'Disqualified'
        : (params.sector && params.sub_identity
            ? `${params.sector} ${params.sub_identity}`
            : (params.sub_identity || params.primary_identity || 'Generic'));

    const row: ContactMapRow = {
        bucketing_run_id: runId,
        contact_id: contact.contact_id,
        industry_string: (contact.classification || contact.industry || '').trim(),
        primary_identity: params.primary_identity,
        sub_identity: params.sub_identity,
        sector: params.sector,
        assigned_bucket_name: params.assigned_bucket_name ?? null,
        assigned_bucket_primary_identity: params.assigned_bucket_primary_identity ?? null,
        canonical_classification: canonical,
        bucket_reason: '',
        pre_rollup_bucket_name: RESERVED_GENERAL,
        bucket_name: RESERVED_GENERAL,
        identity_confidence: cleanScore(params.identity_confidence ?? params.confidence),
        sub_identity_confidence: cleanScore(params.sub_identity_confidence ?? params.confidence),
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
        : row.sub_identity && row.sector && row.sector !== 'Multi-industry'
            ? 'combo'
            : row.sub_identity
                ? 'sub_identity'
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
    sub_identity: string;
    sector: string;
    masked: string[];   // names of layers we suppressed, for bucket_reason
} {
    const idLow = row.identity_confidence != null && row.identity_confidence < TAG_CONF_FLOOR;
    const chLow = row.sub_identity_confidence != null && row.sub_identity_confidence < TAG_CONF_FLOOR;
    const secLow = row.sector_confidence != null && row.sector_confidence < TAG_CONF_FLOOR;
    const masked: string[] = [];
    let ch = row.sub_identity || '';
    let sec = row.sector || '';
    if (idLow) {
        // Identity itself is shaky — drop everything below it so the
        // cascade only attempts the identity-only bucket.
        if (ch) masked.push('sub_identity');
        if (sec) masked.push('sector');
        masked.push('identity_low');
        ch = '';
        sec = '';
    } else {
        if (chLow) {
            if (ch) masked.push('sub_identity');
            ch = '';
        }
        if (secLow) {
            if (sec) masked.push('sector');
            sec = '';
        }
    }
    return {
        primary_identity: row.primary_identity || '',
        sub_identity: ch,
        sector: sec,
        masked
    };
}

// NOTE: a 125-line in-process `computeContactRollup` lived here. It applied a
// 4-level cascade (combo → sub-identity → identity → General) + a
// bucket_budget post-pass that demoted the smallest buckets until the total
// count fit a user-supplied cap (default 30). It hasn't been called by any
// live code path since the deterministic-rollup RPC took over Phase 1b. The
// bucket_budget cap was the only consumer of the legacy `bucket_budget`
// column; the new rollup uses two thresholds (`min_volume`,
// `identity_min_volume`) and doesn't cap bucket count. The function +
// REASON.BUDGET_ROLLUP code were removed together with the column.

// Generic Audit — runs after computeContactRollup. Looks at rows currently
// in the General bucket, groups them by (combo OR sub-identity OR
// primary_identity), and re-routes groups of ≥ floor(minVolume/4) rows to a
// matching live bucket if one exists in the run. Recovers volume that the
// rollup left in General because the narrowest (combo) layer was too small
// but a broader layer (sub-identity, identity) already had a bucket.
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
    // in priority order (combo → sub-identity → identity); first match
    // against a live bucket wins. This mirrors the simplified 3-level
    // cascade in computeContactRollup.
    const groups = new Map<string, { rows: ContactMapRow[]; from: string }>();
    for (const r of rows) {
        if (r.bucket_name !== RESERVED_GENERAL) continue;
        if (r.is_disqualified) continue;
        const candidates: { name: string; from: string }[] = [];
        if (r.sector && r.sub_identity && r.sector !== 'Multi-industry') {
            candidates.push({ name: `${r.sector} ${r.sub_identity}`, from: 'combo' });
        }
        if (r.sub_identity) {
            candidates.push({ name: r.sub_identity, from: 'sub_identity' });
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
                : 'sub_identity';
            r.general_reason = REASON.GENERIC_AUDIT_RECLAIMED;
            r.bucket_reason = `Generic Audit: re-routed from General to ${bucketName} (matched on ${g.from})`;
        }
        reclaimed += g.rows.length;
        targets.push({ bucket: bucketName, count: g.rows.length, from: g.from });
    }

    targets.sort((a, b) => b.count - a.count);
    return { rows, reclaimed, targets };
}

// Persist a slice of ContactMapRow into bucket_contact_map. Used in
// two places: the per-chunk checkpoint inside the streaming route (so
// cancellations preserve work) and the final post-rollup upsert (which
// updates bucket_name + rollup_level on the already-checkpointed rows).
async function writeContactMapRows(
    supabase: SupabaseClient,
    rows: ContactMapRow[]
): Promise<void> {
    if (rows.length === 0) return;
    const mapRows = rows.map(row => ({
        bucketing_run_id: row.bucketing_run_id,
        contact_id: row.contact_id,
        industry_string: row.industry_string,
        primary_identity: row.primary_identity || null,
        sub_identity: row.sub_identity || null,
        sector: row.sector || null,
        canonical_classification: row.canonical_classification || null,
        bucket_reason: row.bucket_reason || null,
        pre_rollup_bucket_name: row.pre_rollup_bucket_name,
        bucket_name: row.bucket_name,
        rollup_level: row.rollup_level,
        source: row.source,
        confidence: row.confidence,
        identity_confidence: row.identity_confidence ?? null,
        sub_identity_confidence: row.sub_identity_confidence ?? null,
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
}

// Hydrate ContactMapRow[] from bucket_contact_map for a given run. Used
// on /resume to restore the in-memory assignedRows array so the rollup
// at the end of streaming sees the FULL set (hydrated + new) and the
// volume thresholds remain correct.
async function fetchPreRollupContactMap(
    supabase: SupabaseClient,
    runId: string
): Promise<ContactMapRow[]> {
    const out: ContactMapRow[] = [];
    const PAGE = 1000;
    let cursor: string | null = null;
    while (true) {
        let q: any = supabase.from('bucket_contact_map')
            .select('*')
            .eq('bucketing_run_id', runId)
            .order('contact_id', { ascending: true })
            .limit(PAGE);
        if (cursor) q = q.gt('contact_id', cursor);
        const { data, error } = await q;
        if (error) throw new Error(`resume hydrate failed: ${error.message}`);
        const rows = (data || []) as any[];
        if (rows.length === 0) break;
        for (const r of rows) {
            out.push({
                bucketing_run_id: r.bucketing_run_id,
                contact_id: r.contact_id,
                industry_string: r.industry_string || '',
                primary_identity: r.primary_identity || '',
                sub_identity: r.sub_identity || '',
                sector: r.sector || '',
                assigned_bucket_name: r.assigned_bucket_name || null,
                assigned_bucket_primary_identity: r.assigned_bucket_primary_identity || null,
                canonical_classification: r.canonical_classification || '',
                bucket_reason: r.bucket_reason || '',
                pre_rollup_bucket_name: r.pre_rollup_bucket_name || r.bucket_name || '',
                bucket_name: r.bucket_name || '',
                identity_confidence: Number(r.identity_confidence ?? 0),
                sub_identity_confidence: Number(r.sub_identity_confidence ?? 0),
                sector_confidence: Number(r.sector_confidence ?? 0),
                rollup_level: (r.rollup_level || 'general') as ContactMapRow['rollup_level'],
                source: r.source || '',
                confidence: Number(r.confidence ?? 0),
                leaf_score: Number(r.leaf_score ?? 0),
                ancestor_score: Number(r.ancestor_score ?? 0),
                root_score: Number(r.root_score ?? 0),
                is_generic: !!r.is_generic,
                is_disqualified: !!r.is_disqualified,
                general_reason: r.general_reason || null,
                reasons: r.reasons || {}
            });
        }
        if (rows.length < PAGE) break;
        cursor = rows[rows.length - 1].contact_id;
    }
    return out;
}

async function writeContactMapAndAssignments(
    supabase: SupabaseClient,
    runId: string,
    rows: ContactMapRow[]
): Promise<void> {
    // Re-uses the per-chunk writer for the contact_map upsert (so the
    // final post-rollup write updates bucket_name on the already-
    // checkpointed rows). bucket_assignments stays a one-shot write.
    await writeContactMapRows(supabase, rows);

    const assignmentRows = rows.map(row => ({
        bucketing_run_id: runId,
        contact_id: row.contact_id,
        bucket_name: row.bucket_name,
        source: row.source,
        confidence: row.confidence,
        identity_confidence: row.identity_confidence ?? null,
        sub_identity_confidence: row.sub_identity_confidence ?? null,
        sector_confidence: row.sector_confidence ?? null,
        bucket_leaf: row.sub_identity || null,
        bucket_ancestor: row.primary_identity || null,
        bucket_root: row.primary_identity || null,
        primary_identity: row.primary_identity || null,
        sub_identity: row.sub_identity || null,
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
// sub-identity, with sector extracted deterministically.
// ─── preview embedding pass (post-discovery, pre-review) ───────────
//
// Replaces the old "seed from example_strings" behavior. After Phase 1a
// proposes a taxonomy, we embed every proposed spec and every unmatched
// industry, then cosine-match. High-confidence pairs become preview map
// rows so the Review screen shows real per-spec counts before assignment.
//
// Returns rows for the caller to upsert + the OpenAI embedding cost.
// Phase 1b clears and rewrites these rows with the full LLM chain.
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
        const spec = b.sub_identity || b.bucket_name || '';
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
            const spec = lib.sub_identity || lib.bucket_name || '';
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
                sub_identity: spec,
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
    // sub-identity, not the vertical the company serves.
    const leafTexts = leaves.map(l => {
        const sig = (l.strong_identity_signals || []).slice(0, 6).join(', ');
        const inc = (l.include || []).slice(0, 6).join(', ');
        return `${l.sub_identity} (under ${l.primary_identity}): ${l.description || ''}. Identity: ${l.identity_type}. Strong signals: ${sig}. Include: ${inc}.`;
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
                bucket_name: leaf.sub_identity, // pre-rollup placeholder
                source: 'embedding',
                confidence: Number(top.s.toFixed(2)),
                bucket_leaf: leaf.sub_identity,
                bucket_ancestor: leaf.primary_identity,
                bucket_root: leaf.primary_identity,
                primary_identity: leaf.primary_identity,
                sub_identity: leaf.sub_identity,
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
        const spec = b.sub_identity || b.bucket_name || '';
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
            const spec = lib.sub_identity || lib.bucket_name || '';
            const ident = lib.primary_identity || lib.direct_ancestor || '';
            autoAssigned.push(makeMatchedContactRow(runId, contacts[i], {
                primary_identity: ident,
                sub_identity: spec,
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
        return `${l.sub_identity} (under ${l.primary_identity}): ${l.description || ''}. Identity: ${l.identity_type}. Strong signals: ${sig}. Include: ${inc}. Examples: ${examples}.`;
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
                sub_identity: leaf.sub_identity,
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
                sub_identity: leaves[j].sub_identity,
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
    supabase: SupabaseClient,
    pending: ContactRouteInput[],
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    runId: string,
    ctx?: BucketingCtx
): Promise<{ rows: ContactMapRow[]; costUsd: number }> {
    if (pending.length === 0) return { rows: [], costUsd: 0 };

    // Resolve the Anthropic client once per batch run (only if we're using a
    // claude-* MATCH_MODEL). Avoids N concurrent key-fetches against app_settings
    // when classifyContactBatch runs under pLimit. For gpt-* models, anthropic
    // stays null and we use OpenAI fetch instead.
    const anthropic = MATCH_MODEL.startsWith('claude-')
        ? await getAnthropic(supabase)
        : null;
    if (MATCH_MODEL.startsWith('claude-') && !anthropic) {
        throw new Error(
            `Anthropic API key not configured (Phase 1b is set to ${MATCH_MODEL}). Add the key on the Connectors page.`
        );
    }

    const validSpecNames = new Set(leaves.map(l => l.sub_identity));
    const validIdentityNames = new Set(leaves.map(l => l.primary_identity));
    const identityBySpec = new Map(leaves.map(l => [l.sub_identity, l.primary_identity]));
    const refByIdentity: Record<string, any[]> = {};
    for (const l of leaves) {
        const ident = l.primary_identity;
        if (!refByIdentity[ident]) refByIdentity[ident] = [];
        refByIdentity[ident].push({
            sub_identity: l.sub_identity,
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
        // Bail out cheaply if the run was already aborted while we were
        // waiting in the pLimit queue — saves an LLM round-trip per
        // queued batch when the user clicks Stop.
        if (ctx?.abortSignal?.aborted) return;
        let results: MatchChain[]; let costUsd: number;
        try {
            ({ results, costUsd } = await classifyContactBatch(
                batch, bucketReferenceJson, sectorVocab, validSpecNames, validIdentityNames,
                anthropic, ctx?.abortSignal
            ));
        } catch (err: any) {
            // AbortError when the run is cancelled — propagate as
            // BucketingCancelledError so the run lands at status='cancelled',
            // not status='failed'. Any other error is a real failure.
            if (ctx?.abortSignal?.aborted || err?.name === 'AbortError') {
                throw new BucketingCancelledError();
            }
            throw err;
        }
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
            const specOk = !!r.sub_identity.name
                && validSpecNames.has(r.sub_identity.name)
                && r.sub_identity.score >= 0.40;
            const specName = specOk ? r.sub_identity.name : '';
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
                sub_identity: isDisqualified ? '' : specName,
                sector: isDisqualified ? '' : (r.sector || '').trim(),
                source: 'llm_phase1b',
                confidence: specName ? r.sub_identity.score : r.primary_identity.score,
                leaf_score: r.sub_identity.score,
                ancestor_score: r.primary_identity.score,
                root_score: r.primary_identity.score,
                is_generic: isGeneric,
                is_disqualified: isDisqualified,
                general_reason: isDisqualified ? 'disqualified' : (isGeneric ? 'generic_low_confidence' : null),
                reasons: {
                    spec: r.sub_identity.reason,
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
    validIdentityNames: Set<string>,
    anthropic: Anthropic | null,
    runAbortSignal?: AbortSignal
): Promise<{ results: MatchChain[]; costUsd: number }> {
    // System prompt is the stable, cacheable prefix: contains the
    // PROJECT_CONTEXT, shared rules, and the per-run BUCKET_REFERENCE +
    // SECTOR_VOCABULARY. OpenAI auto-caches prefixes >=1024 tokens; Anthropic
    // caches the marked `ephemeral` block. Only the user prompt (per-batch
    // contact list) varies between calls.
    const systemPrompt = `${PROJECT_CONTEXT}

========================================
PHASE 1B — ROUTE EACH CONTACT TO IDENTITY + SUB-IDENTITY + SECTOR
========================================

You classify individual company contacts, not abstract industry labels.
Use the company name, website, enriched classification, model confidence,
and reasoning. Do not invent facts beyond those fields.

Return three separate fields:
- primary_identity: Layer 1, must be one of BUCKET_REFERENCE keys
- sub_identity: Layer 2, must be listed under that identity
- sector: Layer 3, must be from SECTOR_VOCABULARY, "Multi-industry", or ""

General is a last resort. If a primary_identity is a reasonable fit, return it
even when no sub-identity is precise. Only leave both names blank when the
business is truly unclear, bad data, clear non-ICP, or confidence is below 0.40.

Each contact may include embedding_candidate_buckets. Treat these as a shortlist,
not a decision. Prefer them when the text supports them, but choose another
reference bucket if the contact evidence is clearly better.

Scores: alignment scores 0.0–1.0 (NOT 1-10). Use >=0.70 for strong fit,
>=0.40 for reasonable fit, <0.40 means the tag shouldn't be used.
Reasons: max 18 words each and cite the contact fields.

${HARD_KEYWORD_ROUTING}

${CORE_PRINCIPLES}

${DISQUALIFICATION_RULES}

BUCKET_REFERENCE (grouped by primary_identity):
${bucketReferenceJson}

SECTOR_VOCABULARY: ${JSON.stringify(sectorVocab)}

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

    const userPrompt = `CONTACTS_TO_CLASSIFY (same order as output):
${JSON.stringify(contactPayload)}

Return JSON: { "assignments": [<one object per contact in the same order>] }
Each assignment object:
{
  "primary_identity": {"name": "<identity key from BUCKET_REFERENCE or empty>", "score": 0.00, "reason": ""},
  "sub_identity": {"name": "<spec under that identity, or empty>", "score": 0.00, "reason": ""},
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
                    required: ['primary_identity', 'sub_identity',
                               'sector', 'identity_type', 'generic', 'disqualified'],
                    properties: {
                        primary_identity: chainItemSchema(),
                        sub_identity: chainItemSchema(),
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
        // Combined abort: timeout OR the run's cancel signal OR an explicit
        // abort. The run-cancel listener is added with { once: true } so it
        // doesn't leak across batches on long runs.
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), OPENAI_TIMEOUT_MS);
        const onRunAbort = () => controller.abort();
        if (runAbortSignal) {
            if (runAbortSignal.aborted) controller.abort();
            else runAbortSignal.addEventListener('abort', onRunAbort, { once: true });
        }
        try {
            if (anthropic) {
                // Anthropic path (claude-*): lenient JSON parsing + cache_control.
                const resp = await anthropic.messages.create({
                    model: MATCH_MODEL,
                    max_tokens: 4000,
                    system: [{ type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }],
                    messages: [{ role: 'user', content: userPrompt }]
                }, { signal: controller.signal });
                const text = (resp.content as any[])
                    .filter(b => b.type === 'text').map(b => b.text).join('\n');
                const parsed = parsePhase1bAssignments(text);
                const usage: any = (resp as any).usage || {};
                const input = usage.input_tokens || 0;
                const output = usage.output_tokens || 0;
                // Anthropic's usage.input_tokens is uncached only (cache reads/
                // creations are reported separately). Sum the three components at
                // their respective billing rates: full / 0.1× / 1.25×.
                const cachedIn = usage.cache_read_input_tokens || 0;
                const cacheCreate = usage.cache_creation_input_tokens || 0;
                const inputRate = ANTHROPIC_PRICING[MATCH_MODEL]?.input || 1;
                const costUsd = computeAnthropicCost(MATCH_MODEL, input, output)
                    + (cachedIn / 1_000_000) * inputRate * 0.1
                    + (cacheCreate / 1_000_000) * inputRate * 1.25;
                return { assignments: parsed as MatchChain[], costUsd };
            }
            // OpenAI path (gpt-*): strict json_schema. OpenAI auto-caches
            // stable prefixes >=1024 tokens — system prompt qualifies.
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
            if (runAbortSignal) runAbortSignal.removeEventListener('abort', onRunAbort);
        }
    };

    let result;
    try { result = await callOnce(); }
    catch (e: any) {
        // Don't retry if the user clicked Stop — surface the cancel
        // immediately so the run unwinds. Without this, the bare retry
        // would queue another LLM call after the user explicitly stopped.
        if (runAbortSignal?.aborted || e?.name === 'AbortError') throw e;
        result = await callOnce();
    }

    let { assignments, costUsd } = result;
    // Snap fuzzy LLM outputs to canonical library names BEFORE drift detection,
    // so paraphrases ("Cybersecurity Software" → "Cybersecurity SaaS") survive
    // instead of being dropped by the strict-match validation downstream.
    assignments = assignments.map(a => snapMatchChain(a, validIdentityNames, validSpecNames, sectorVocab));
    const drift = assignments.some(a =>
        (a.sub_identity.name && !validSpecNames.has(a.sub_identity.name)) ||
        (a.primary_identity.name && !validIdentityNames.has(a.primary_identity.name))
    );
    if (drift) {
        try {
            const retried = await callOnce();
            assignments = retried.assignments.map(a => snapMatchChain(a, validIdentityNames, validSpecNames, sectorVocab));
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
    const validSpecNames = new Set(leaves.map(l => l.sub_identity));
    const validIdentityNames = new Set(leaves.map(l => l.primary_identity));
    const identityBySpec = new Map(leaves.map(l => [l.sub_identity, l.primary_identity]));

    // Bucket reference is the cacheable prompt prefix. Group by primary_identity
    // so the model sees the hierarchy clearly.
    const refByIdentity: Record<string, any[]> = {};
    for (const l of leaves) {
        const ident = l.primary_identity;
        if (!refByIdentity[ident]) refByIdentity[ident] = [];
        refByIdentity[ident].push({
            sub_identity: l.sub_identity,
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
            const specOk = r.sub_identity.name
                && validSpecNames.has(r.sub_identity.name)
                && r.sub_identity.score >= 0.55;
            const specName = specOk ? r.sub_identity.name : '';
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
                confidence: Number((r.sub_identity.score || 0).toFixed(2)),
                bucket_leaf: specName,
                bucket_ancestor: identName,
                bucket_root: identName,
                primary_identity: identName,
                sub_identity: specName,
                sector: (r.sector || '').trim(),
                leaf_score: r.sub_identity.score,
                ancestor_score: r.primary_identity.score,
                root_score: r.primary_identity.score,
                is_generic: !!r.generic && !specOk,
                is_disqualified: !!r.disqualified,
                reasons: {
                    spec: r.sub_identity.reason,
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
        sub_identity: { name: '', score: 0, reason: 'fallback' },
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
PHASE 1B — ROUTE EACH COMPANY TO IDENTITY + SUB-IDENTITY + SECTOR
========================================

You produce three separate classifications per company:
  - primary_identity      (Layer 1, MUST be one of the identity keys in BUCKET_REFERENCE)
  - sub-identity (Layer 2, MUST be one of the sub-identity
                          names listed UNDER that identity in BUCKET_REFERENCE)
  - sector          (Layer 3, optional — from SECTOR_VOCABULARY only)

You DO NOT produce a campaign bucket. The system computes that from these
three values + counts.

DECISION SEQUENCE (apply in this order):

1) Determine PRIMARY IDENTITY (the company's core business model).
   Investor? Software vendor? Agency? Consulting firm? Staffing firm?
   MSP? Operator (clinic, school, government entity)?

2) Inside that identity, pick the SUB-IDENTITY that best fits.
   Examples: under "Agency" → "SEO Agency"; under "Financial Services" →
   "Private Equity Firm"; under "Consulting & Advisory" → "IT Consulting".

3) Determine SECTOR — the vertical the company SERVES if explicitly
   stated. If multiple, use "Multi-industry". If unspecified, "".

4) If neither identity nor sub-identity fits at >= 0.55 confidence,
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

Rule 3 — Operator sub-identities (operator_required=true) require
  EXPLICIT operator evidence: "clinic", "hospital", "school district",
  "university", "city government", "church", "property management
  company", "factory". Generic sector mentions are NOT evidence.

Rule 4 — When BOTH identity and sector appear, fill BOTH fields.
  Example: "Healthcare private equity firm" →
    primary_identity = "Financial Services",
    sub-identity = "Private Equity Firm",
    sector = "Healthcare".

EXPLICIT EXAMPLES — CORRECT:
  • "Healthcare private equity investment firm" →
      primary_identity = Financial Services
      sub-identity = Private Equity Firm
      sector = Healthcare
  • "Government IT consulting firm" →
      primary_identity = Consulting & Advisory  (or IT Services depending
        on which identity contains "IT Consulting" in BUCKET_REFERENCE)
      sub-identity = IT Consulting
      sector = Government
  • "Marketing agency for dental practices" →
      primary_identity = Agency
      sub-identity = Performance Marketing Agency  (or
        Branding Agency, etc — pick the closest spec from BUCKET_REFERENCE)
      sector = Healthcare
  • "Real estate software for hospitals" →
      primary_identity = Software & SaaS
      sub-identity = PropTech SaaS  (or Vertical SaaS)
      sector = Real Estate     ← yes, real estate is the OWN model
  • "Medical clinic" →
      primary_identity = Healthcare Provider
      sub-identity = null  (medical clinics use identity-only — no matching sub)
      sector = ""

EXPLICIT EXAMPLES — INCORRECT:
  • "Healthcare private equity investment firm" → primary_identity = Healthcare ❌
  • "Government IT consulting firm" → primary_identity = Government ❌
  • "Marketing agency for dental practices" → primary_identity = Healthcare ❌
  • "Real estate software for hospitals" → primary_identity = Healthcare ❌
  • "Software for schools" → primary_identity = Education ❌

PRESSURE TEST: "If outreach were written for this primary_identity +
sub-identity, would the recipient say 'yes that's me' or 'no that's
my client'?" If 'my client' → routing is wrong.

OUTPUT CONSTRAINTS:
- primary_identity.name MUST be one of the identity keys in BUCKET_REFERENCE,
  or "" if generic / disqualified.
- sub-identity.name MUST be a spec listed under that exact
  identity, or "" if generic / disqualified.
- sub-identity.score must be <= primary_identity.score.
- sector MUST be from SECTOR_VOCABULARY, "Multi-industry", or "".
  NEVER put an identity noun in sector.
- identity_type ∈ operator | service_provider | agency | software_vendor |
  investor | advisor | staffing | distributor | media | other.
- Disqualify ONLY clear ecommerce/DTC physical, local geo-tied services,
  brick-and-mortar retail, low-ticket consumer.
- Reasons: max 18 words each, must cite a phrase from the classification.

${HARD_KEYWORD_ROUTING}

${CORE_PRINCIPLES}

${DISQUALIFICATION_RULES}

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
  "sub_identity": {"name": "<spec under that identity, or empty>", "score": 0.00, "reason": ""},
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
                    required: ['primary_identity', 'sub_identity',
                               'sector', 'identity_type', 'generic', 'disqualified'],
                    properties: {
                        primary_identity: chainItemSchema(),
                        sub_identity: chainItemSchema(),
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
    // Snap fuzzy LLM names to canonical library names before strict drift check.
    assignments = assignments.map(a => snapMatchChain(a, validIdentityNames, validSpecNames, sectorVocab));
    const drift = assignments.some(a =>
        (a.sub_identity.name && !validSpecNames.has(a.sub_identity.name)) ||
        (a.primary_identity.name && !validIdentityNames.has(a.primary_identity.name))
    );
    if (drift) {
        try {
            const retried = await callOnce();
            assignments = retried.assignments.map(a => snapMatchChain(a, validIdentityNames, validSpecNames, sectorVocab));
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
