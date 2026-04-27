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
 * Phase 1b (MATCHING, per distinct industry): gpt-4.1-mini batched 8/call,
 * concurrency 40. Returns leaf+ancestor+root chain PLUS sector_focus,
 * generic/disqualified flags, identity_type. Strict routing rules in the
 * prompt — strong identity nouns (PE firm, agency, MSP, …) outrank sector
 * nouns (healthcare, government, …) unless operator evidence is present.
 *
 * Volume rollup is unchanged: leaf → ancestor → root → Generic. Disqualified
 * stays disqualified.
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
const EMBED_AUTO_THRESHOLD = 0.85;
const EMBED_MARGIN = 0.10;
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

// Single catch-all bucket for everything that doesn't earn its own segment:
// generic, disqualified, sub-threshold rollups, scrape failures.
// is_disqualified flag is still tracked per row for audit but no longer
// drives a separate bucket.
const RESERVED_GENERAL = 'General';
const RESERVED = new Set([
    'general', 'generic', 'disqualified', 'other'
]);

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
    avg_conf: number;
    sample_companies: string[] | null;
    sample_reasoning: string[] | null;
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

// ─── paginated vocabulary fetch ────────────────────────────────────
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

    const preferredIds: string[] = Array.isArray(run.preferred_library_ids) ? run.preferred_library_ids : [];
    let preferred: any[] = [];
    if (preferredIds.length > 0) {
        const { data: libRows } = await supabase
            .from('bucket_library').select('*').in('id', preferredIds);
        preferred = (libRows || []).filter((b: any) =>
            (b.functional_specialization || b.bucket_name) && (b.primary_identity || b.direct_ancestor)
        );
    }

    // Wipe any prior preview map rows for this run (idempotent on re-discover).
    await supabase.from('bucket_industry_map').delete().eq('bucketing_run_id', runId);

    let totalCost = 0;

    // ── Step A: library-first match BEFORE discovery ──────────────────
    // High-confidence matches against selected library buckets are written
    // immediately, and those industries are removed from the vocabulary the
    // discovery LLM sees. Saves prompt tokens AND prevents the model from
    // re-inventing buckets we've already saved.
    const sectorVocabPreview: string[] = []; // populated post-discovery
    let discoveryVocab: VocabRow[] = vocabRows;
    let libraryMatchedIndustries = new Set<string>();
    let libraryUsageByBucketId = new Map<string, number>();

    if (preferred.length > 0) {
        log(`[Bucketing ${runId}] Phase 1a step 1/3: library-first match against ${preferred.length} preferred buckets`);
        const libRes = await runLibraryFirstMatch(vocabRows, preferred, [], runId);
        totalCost += libRes.costUsd;

        if (libRes.autoAssigned.length > 0) {
            for (let i = 0; i < libRes.autoAssigned.length; i += 1000) {
                const chunk = libRes.autoAssigned.slice(i, i + 1000);
                const { error: upErr } = await supabase.from('bucket_industry_map')
                    .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
                if (upErr) throw new Error(`library map insert failed: ${upErr.message}`);
            }
        }
        for (const r of libRes.autoAssigned) {
            libraryMatchedIndustries.add(r.industry_string);
            const libId: string | undefined = r.reasons?.library_bucket_id;
            if (libId) libraryUsageByBucketId.set(libId, (libraryUsageByBucketId.get(libId) || 0) + 1);
        }
        discoveryVocab = libRes.pending;
        log(`[Bucketing ${runId}] library-matched ${libRes.autoAssigned.length}/${vocabRows.length}, ${discoveryVocab.length} sent to discovery`);
    }

    // ── Step B: discovery LLM on residual vocab ───────────────────────
    log(`[Bucketing ${runId}] Phase 1a step 2/3: discovery LLM on ${discoveryVocab.length} residual industries`);
    const t0 = Date.now();
    const { discovery, costUsd, modelUsed } = await callDiscoveryLLM(supabase, discoveryVocab, preferred);
    totalCost += costUsd;
    log(`[Bucketing ${runId}] discovery LLM (${modelUsed}): ${(Date.now() - t0) / 1000}s, $${costUsd.toFixed(4)}, ${discovery.buckets.length} discovered specs`);

    // Normalize primary_identities; drop any reserved names; dedupe.
    const seenIdent = new Set<string>();
    let primaryIdentities = (discovery.primary_identities || []).filter(p => {
        const n = (p.name || '').trim();
        if (!n || RESERVED.has(n.toLowerCase())) return false;
        if (seenIdent.has(n)) return false;
        seenIdent.add(n);
        return true;
    }).map(p => ({
        name: p.name.trim(),
        description: (p.description || '').trim(),
        identity_type: (p.identity_type || 'other').trim(),
        operator_required: !!p.operator_required
    }));

    // Merge in identities from selected library buckets so they appear in
    // the Review tree even if the discovery model didn't list them.
    for (const lib of preferred) {
        const ident = (lib.primary_identity || lib.direct_ancestor || '').trim();
        if (!ident || seenIdent.has(ident)) continue;
        seenIdent.add(ident);
        primaryIdentities.push({
            name: ident,
            description: lib.description || '',
            identity_type: 'service_provider',
            operator_required: false
        });
    }

    // Validate specialization → identity references; drop dangling ones.
    const identitySet = new Set(primaryIdentities.map(p => p.name));
    const discoveredLeaves = discovery.buckets.filter(b => {
        const spec = (b.functional_specialization || '').trim();
        const ident = (b.primary_identity || '').trim();
        return spec
            && !RESERVED.has(spec.toLowerCase())
            && ident
            && identitySet.has(ident);
    }).map(b => ({
        ...b,
        functional_specialization: b.functional_specialization.trim(),
        primary_identity: b.primary_identity.trim(),
        description: (b.description || '').trim(),
        identity_type: (b.identity_type || 'other').trim(),
        operator_required: !!b.operator_required,
        priority_rank: typeof b.priority_rank === 'number' ? b.priority_rank : 5,
        include: dedupe(b.include),
        exclude: dedupe(b.exclude),
        example_strings: dedupe(b.example_strings),
        strong_identity_signals: dedupe(b.strong_identity_signals),
        weak_sector_signals: dedupe(b.weak_sector_signals),
        disqualifying_signals: dedupe(b.disqualifying_signals)
    }));

    // Inject library buckets that had at least one match into the proposal
    // so the user can review/keep/drop them alongside discovered specs.
    const lockedFromLibrary: DiscoveredBucket[] = [];
    for (const lib of preferred) {
        const spec = (lib.functional_specialization || lib.bucket_name || '').trim();
        const ident = (lib.primary_identity || lib.direct_ancestor || '').trim();
        if (!spec || !ident) continue;
        if (discoveredLeaves.some(l => l.functional_specialization === spec)) continue;
        lockedFromLibrary.push({
            functional_specialization: spec,
            primary_identity: ident,
            description: lib.description || '',
            identity_type: 'service_provider',
            operator_required: false,
            priority_rank: 3,
            include: lib.include_terms || [],
            exclude: lib.exclude_terms || [],
            example_strings: lib.example_strings || [],
            strong_identity_signals: [],
            weak_sector_signals: [],
            disqualifying_signals: [],
            library_match_id: lib.id
        });
    }
    const leaves = [...lockedFromLibrary, ...discoveredLeaves];
    const sectorVocab = discovery.sector_focus_vocabulary || [];

    await supabase.from('bucketing_runs').update({
        taxonomy_proposal: {
            observed_patterns: discovery.observed_patterns || [],
            sector_focus_vocabulary: sectorVocab,
            primary_identities: primaryIdentities,
            buckets: leaves
        },
        taxonomy_model: modelUsed,
        cost_usd: totalCost,
        total_contacts: totalContacts
    }).eq('id', runId);

    // ── Step C: embedding preview against ALL proposed specs ──────────
    // Replaces the old example_strings seed. Embed every still-unmatched
    // industry against every proposed spec; cosine ≥ EMBED_AUTO_THRESHOLD
    // and margin ≥ EMBED_MARGIN write a preview map row.
    const stillPending = vocabRows.filter(r => !libraryMatchedIndustries.has(r.industry));
    log(`[Bucketing ${runId}] Phase 1a step 3/3: embedding preview against ${leaves.length} specs over ${stillPending.length} pending industries`);
    const previewRes = await runPreviewEmbedding(stillPending, leaves, sectorVocab, runId);
    totalCost += previewRes.costUsd;
    if (previewRes.rows.length > 0) {
        for (let i = 0; i < previewRes.rows.length; i += 1000) {
            const chunk = previewRes.rows.slice(i, i + 1000);
            const { error: upErr } = await supabase.from('bucket_industry_map')
                .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
            if (upErr) throw new Error(`preview embedding insert failed: ${upErr.message}`);
        }
    }
    log(`[Bucketing ${runId}] preview embedding wrote ${previewRes.rows.length} map rows`);

    // Final cost + state
    await supabase.from('bucketing_runs').update({
        cost_usd: totalCost,
        status: 'taxonomy_ready',
        taxonomy_completed_at: new Date().toISOString()
    }).eq('id', runId);

    // Library link rows + usage counters
    if (preferred.length > 0) {
        const links: any[] = [];
        for (const lib of preferred) {
            const spec = (lib.functional_specialization || lib.bucket_name || '').trim();
            if (!spec) continue;
            links.push({
                bucketing_run_id: runId,
                library_bucket_id: lib.id,
                bucket_name_in_run: spec
            });
        }
        if (links.length > 0) {
            await supabase.from('bucket_library_run_links').upsert(links, { onConflict: 'bucketing_run_id,library_bucket_id' });
        }
        for (const lib of preferred) {
            const matched = libraryUsageByBucketId.get(lib.id) || 0;
            if (matched > 0) {
                await supabase.from('bucket_library')
                    .update({
                        times_used: (lib.times_used || 0) + 1,
                        last_used_at: new Date().toISOString()
                    })
                    .eq('id', lib.id);
            }
        }
    }

    log(`[Bucketing ${runId}] Phase 1a done — ${leaves.length} specs (${lockedFromLibrary.length} from library, ${discoveredLeaves.length} discovered), $${totalCost.toFixed(4)}`);
}

function dedupe(arr: string[] | undefined): string[] {
    return Array.from(new Set((arr || []).map(s => (s || '').trim()).filter(Boolean)));
}

async function callDiscoveryLLM(
    supabase: SupabaseClient,
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
- strong_identity_signals, weak_sector_signals, disqualifying_signals:
  same as before — phrases that prove / hint at / route AWAY from this
  specialization.
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
      "strong_identity_signals": [<phrases>],
      "weak_sector_signals": [<sector phrases that often appear but don't determine this spec>],
      "disqualifying_signals": [<phrases that should route AWAY>],
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
                    required: ['functional_specialization', 'primary_identity', 'description',
                               'identity_type', 'operator_required', 'priority_rank',
                               'include', 'exclude', 'example_strings',
                               'strong_identity_signals', 'weak_sector_signals', 'disqualifying_signals',
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
                        strong_identity_signals: { type: 'array', items: { type: 'string' } },
                        weak_sector_signals: { type: 'array', items: { type: 'string' } },
                        disqualifying_signals: { type: 'array', items: { type: 'string' } },
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
            observed_patterns: proposal.observed_patterns || [],
            sector_focus_vocabulary: proposal.sector_focus_vocabulary || [],
            primary_identities: (proposal as any).primary_identities || [],
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
    log(`[Bucketing ${runId}] taxonomy edits applied: ${leaves.length} specializations`);

    // Rebuild the preview map so Review counts reflect the edited taxonomy
    // (renames / drops / new specs all change which industries map where).
    // Cheap: one batched embedding call against the run's vocab.
    try {
        await rebuildPreviewMap(supabase, runId, leaves, proposal.sector_focus_vocabulary || [], run.list_names || [], log);
    } catch (e: any) {
        log(`[Bucketing ${runId}] preview rebuild failed (non-fatal): ${e.message}`, 'warn');
    }
}

async function rebuildPreviewMap(
    supabase: SupabaseClient,
    runId: string,
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    listNames: string[],
    log: (msg: string, level?: 'info' | 'warn' | 'error') => void
): Promise<void> {
    if (!Array.isArray(listNames) || listNames.length === 0) return;
    const vocab = await fetchFullVocabulary(supabase, listNames);
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
    log(`[Bucketing ${runId}] preview rebuilt: ${rows.length} rows, $${costUsd.toFixed(4)}`);
}

// ────────────────────────────────────────────────────────────────────
// PHASE 1B — IDENTITY-FIRST MATCHING
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
    const sectorVocab: string[] = (final as any).sector_focus_vocabulary || [];

    // Library buckets the user opted into for this run. We match them first,
    // deterministically, before LLM matching. They've earned their place
    // across past runs — no point burning LLM tokens on industries they
    // already cover.
    const preferredIds: string[] = Array.isArray(run.preferred_library_ids) ? run.preferred_library_ids : [];
    let libraryBuckets: any[] = [];
    if (preferredIds.length > 0) {
        const { data } = await supabase
            .from('bucket_library').select('*').in('id', preferredIds);
        libraryBuckets = (data || []).filter((b: any) =>
            (b.functional_specialization || b.bucket_name) && (b.primary_identity || b.direct_ancestor)
        );
    }

    await supabase.from('bucketing_runs').update({ status: 'assigning' }).eq('id', runId);

    let totalCost = Number(run.cost_usd || 0);

    log(`[Bucketing ${runId}] step 1/5: load vocabulary`);
    const vocab = await fetchFullVocabulary(supabase, run.list_names);
    log(`[Bucketing ${runId}] ${vocab.length} distinct industries to match`);

    await supabase.from('bucket_industry_map').delete().eq('bucketing_run_id', runId);

    let assignedRows: any[] = [];
    let pendingIndustries: VocabRow[] = vocab;

    // Step 2 (NEW): library-first deterministic match. Embedding cosine
    // against library bucket definitions; high-confidence hits are written
    // immediately as map rows with source='library_match'. This puts saved
    // institutional knowledge in front of fresh LLM discovery.
    if (libraryBuckets.length > 0) {
        log(`[Bucketing ${runId}] step 2/5: library-first match against ${libraryBuckets.length} preferred buckets`);
        const libRes = await runLibraryFirstMatch(vocab, libraryBuckets, sectorVocab, runId);
        totalCost += libRes.costUsd;
        assignedRows = libRes.autoAssigned;
        pendingIndustries = libRes.pending;
        log(`[Bucketing ${runId}] library matched ${assignedRows.length}/${vocab.length}, ${pendingIndustries.length} pending`);
    }

    // Step 3: embedding pre-filter against THIS run's discovered specs.
    // Now sector-aware: we deterministically scan the industry string for
    // any sector_focus_vocabulary term and tag the resulting row with it,
    // so combo buckets like "Real Estate SEO Agency" can form even on
    // embedding-matched rows.
    if (EMBED_PREFILTER_ENABLED && pendingIndustries.length > 0) {
        log(`[Bucketing ${runId}] step 3/5: embedding pre-filter (sector-aware)`);
        const embedRes = await runEmbeddingPrefilter(pendingIndustries, leaves, sectorVocab, runId);
        totalCost += embedRes.costUsd;
        assignedRows = assignedRows.concat(embedRes.autoAssigned);
        pendingIndustries = embedRes.pending;
        log(`[Bucketing ${runId}] embedding auto-assigned ${embedRes.autoAssigned.length}/${vocab.length}, ${pendingIndustries.length} still pending`);
    }

    log(`[Bucketing ${runId}] step 4/5: routing LLM matching`);
    const llmRes = await runMatchingLLM(pendingIndustries, leaves, sectorVocab, runId);
    totalCost += llmRes.costUsd;
    assignedRows = assignedRows.concat(llmRes.rows);
    log(`[Bucketing ${runId}] LLM matched ${llmRes.rows.length}, total chain rows ${assignedRows.length}, cost so far $${totalCost.toFixed(4)}`);

    if (assignedRows.length > 0) {
        for (let i = 0; i < assignedRows.length; i += 1000) {
            const chunk = assignedRows.slice(i, i + 1000);
            const { error: upErr } = await supabase.from('bucket_industry_map')
                .upsert(chunk, { onConflict: 'bucketing_run_id,industry_string' });
            if (upErr) throw new Error(`map insert failed: ${upErr.message}`);
        }
    }

    log(`[Bucketing ${runId}] step 5/5: volume rollup + fan-out`);
    const { error: rollupErr } = await supabase.rpc('bucketing_apply_volume_rollup', { p_run_id: runId });
    if (rollupErr) throw new Error(`volume rollup failed: ${rollupErr.message}`);

    await supabase.from('bucket_assignments').delete().eq('bucketing_run_id', runId);
    const { data: fanoutCount, error: fanErr } = await supabase
        .rpc('bucketing_deterministic_fanout', { p_run_id: runId });
    if (fanErr) throw new Error(`fanout failed: ${fanErr.message}`);
    log(`[Bucketing ${runId}] fan-out wrote ${Number(fanoutCount || 0)} assignments`);

    const { error: catchErr } = await supabase.rpc('bucketing_catchall_other', { p_run_id: runId });
    if (catchErr) throw new Error(`catch-all failed: ${catchErr.message}`);

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
async function runMatchingLLM(
    pending: VocabRow[],
    leaves: DiscoveredBucket[],
    sectorVocab: string[],
    runId: string
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
    const rows: any[] = [];

    await Promise.all(batches.map(batch => limit(async () => {
        const { results, costUsd } = await classifyBatch(
            batch, bucketReferenceJson, sectorVocab, validSpecNames, validIdentityNames
        );
        totalCost += costUsd;
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
