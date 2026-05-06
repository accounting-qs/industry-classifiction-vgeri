-- =====================================================================
-- Single-round-trip CSV export RPC
--
-- The async CSV worker (server.ts → runCsvExportJob) used to do 11
-- round-trips per 1000-contact page: 1 contacts + 5 enrichment-batches
-- + 5 bucket_assignment-batches. Even after parallelizing the 10
-- hydration calls, each page still pays a min ~1 s of network +
-- PostgREST overhead. For 370k contacts that's ~6-8 min.
--
-- This RPC collapses those 11 round-trips into ONE: a SQL JOIN that
-- returns one row per contact with everything the CSV needs already
-- attached. Keyset-paginated via p_after_id so the worker can keep
-- streaming pages until the result is empty.
--
-- Expected speedup: ~3× over the parallel-hydration version (network
-- overhead dominates; one round-trip per 1000 rows beats ten).
--
-- The bucket_industry_map JOIN uses the same COALESCE precedence as
-- Phase 1b's lookup (enrichments.classification first, contacts.industry
-- as fallback) so phase1a_llm_reason populates correctly even when the
-- raw imported industry string differs from the AI-classified one.
--
-- Idempotent: CREATE OR REPLACE.
-- =====================================================================

CREATE OR REPLACE FUNCTION public.get_contact_export_page(
    p_run_id   UUID,
    p_after_id TEXT    DEFAULT NULL,
    p_limit    INTEGER DEFAULT 1000
)
RETURNS TABLE (
    contact_id                 TEXT,
    email                      TEXT,
    first_name                 TEXT,
    last_name                  TEXT,
    company_name               TEXT,
    company_website            TEXT,
    lead_list_name             TEXT,
    industry                   TEXT,
    enrichment_status          TEXT,
    enrichment_classification  TEXT,
    enrichment_confidence      NUMERIC,
    enrichment_reasoning       TEXT,
    primary_identity           TEXT,
    characteristic             TEXT,
    sector                     TEXT,
    canonical_classification   TEXT,
    bucket_name                TEXT,
    pre_rollup_bucket_name     TEXT,
    rollup_level               TEXT,
    assignment_source          TEXT,
    identity_confidence        NUMERIC,
    characteristic_confidence  NUMERIC,
    sector_confidence          NUMERIC,
    assignment_confidence      NUMERIC,
    is_disqualified            BOOLEAN,
    is_generic                 BOOLEAN,
    phase1a_llm_reason         TEXT,
    bucket_reason              TEXT,
    general_reason             TEXT,
    reasons                    JSONB
)
LANGUAGE sql
STABLE
SET statement_timeout TO '300s'
AS $$
    -- Pull list_names once so we don't re-query bucketing_runs in the join.
    --
    -- Type-mismatch dance: contacts.contact_id is UUID, but
    -- bucket_assignments.contact_id was declared TEXT in the original v1
    -- bucketing migration. Cast the UUID side to TEXT for the join and
    -- to TEXT for the returned column (worker treats it as a string).
    -- p_after_id comes in as TEXT from the worker (PostgREST sends UUIDs
    -- as strings), so we cast it to UUID to compare against the indexed
    -- UUID column rather than slowing the comparison with both-side casts.
    WITH run AS (
        SELECT list_names FROM bucketing_runs WHERE id = p_run_id
    )
    SELECT
        c.contact_id::TEXT AS contact_id,
        c.email,
        c.first_name,
        c.last_name,
        c.company_name,
        c.company_website,
        c.lead_list_name,
        c.industry,
        e.status         AS enrichment_status,
        e.classification AS enrichment_classification,
        e.confidence     AS enrichment_confidence,
        e.reasoning      AS enrichment_reasoning,
        a.primary_identity,
        a.characteristic,
        a.sector,
        a.canonical_classification,
        a.bucket_name,
        a.pre_rollup_bucket_name,
        a.rollup_level,
        a.source         AS assignment_source,
        a.identity_confidence,
        a.characteristic_confidence,
        a.sector_confidence,
        a.confidence     AS assignment_confidence,
        a.is_disqualified,
        a.is_generic,
        m.llm_reason     AS phase1a_llm_reason,
        a.bucket_reason,
        a.general_reason,
        a.reasons
    FROM contacts c
    LEFT JOIN enrichments e
           ON e.contact_id = c.contact_id
    LEFT JOIN bucket_assignments a
           ON a.contact_id       = c.contact_id::TEXT
          AND a.bucketing_run_id = p_run_id
    -- Phase 1a tag lookup: same join key Phase 1b uses, so the
    -- llm_reason hits even when contacts.industry diverges from the
    -- enrichment classification.
    LEFT JOIN bucket_industry_map m
           ON m.bucketing_run_id = p_run_id
          AND m.industry_string  = COALESCE(NULLIF(TRIM(e.classification), ''), c.industry)
    WHERE c.lead_list_name = ANY((SELECT list_names FROM run))
      AND (p_after_id IS NULL OR c.contact_id > p_after_id::UUID)
    ORDER BY c.contact_id
    LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION public.get_contact_export_page(UUID, TEXT, INTEGER)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
