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
-- as fallback) so Phase 1a taxonomy is available even when the run has
-- not produced bucket_assignments yet.
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
    -- Type-mismatch dance: contacts.contact_id is UUID, but
    -- bucket_assignments.contact_id was declared TEXT in the original v1
    -- bucketing migration. Cast the UUID side to TEXT for the join.
    -- p_after_id comes in as TEXT from the worker (PostgREST sends UUIDs
    -- as strings), so we cast it to UUID to compare against the indexed
    -- UUID column.
    --
    -- Page first, JOIN second:
    -- The previous shape (one big SELECT with all 4 LEFT JOINs and a
    -- LIMIT at the bottom) gave Postgres room to pick a hash-join plan
    -- that built hash tables on the full bucket_assignments and
    -- bucket_industry_map tables — work proportional to the WHOLE
    -- dataset per page, not 1000 rows. ~15 min on a 370k run.
    --
    -- The CTE here forces the LIMIT to apply BEFORE the heavy joins:
    -- contact_page is exactly 1000 rows of (contact basics + enrichment
    -- + pre-computed industry_key), then the outer SELECT does just
    -- 1000 PK probes against bucket_assignments and bucket_industry_map.
    -- Should drop per-page time from ~2-3 s to <0.5 s.
    WITH contact_page AS (
        SELECT
            c.contact_id,
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
            -- Pre-compute the industry key once per row so the
            -- bucket_industry_map join can use the (run_id, string) PK
            -- instead of recomputing COALESCE on every probe.
            COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_key
        FROM contacts c
        LEFT JOIN enrichments e
               ON e.contact_id = c.contact_id
        WHERE c.lead_list_name = ANY(
                  SELECT UNNEST(list_names) FROM bucketing_runs WHERE id = p_run_id
              )
          AND (p_after_id IS NULL OR c.contact_id > p_after_id::UUID)
        ORDER BY c.contact_id
        LIMIT p_limit
    )
    SELECT
        cp.contact_id::TEXT AS contact_id,
        cp.email,
        cp.first_name,
        cp.last_name,
        cp.company_name,
        cp.company_website,
        cp.lead_list_name,
        cp.industry,
        cp.enrichment_status,
        cp.enrichment_classification,
        cp.enrichment_confidence,
        cp.enrichment_reasoning,
        COALESCE(NULLIF(a.primary_identity, ''), m.primary_identity) AS primary_identity,
        COALESCE(NULLIF(a.characteristic, ''), m.characteristic) AS characteristic,
        COALESCE(NULLIF(a.sector, ''), m.sector) AS sector,
        COALESCE(NULLIF(a.canonical_classification, ''), m.canonical_classification) AS canonical_classification,
        a.bucket_name,
        COALESCE(NULLIF(a.pre_rollup_bucket_name, ''), m.bucket_name) AS pre_rollup_bucket_name,
        a.rollup_level,
        COALESCE(NULLIF(a.source, ''), m.source) AS assignment_source,
        COALESCE(a.identity_confidence, m.identity_confidence) AS identity_confidence,
        COALESCE(a.characteristic_confidence, m.characteristic_confidence) AS characteristic_confidence,
        COALESCE(a.sector_confidence, m.sector_confidence) AS sector_confidence,
        COALESCE(a.confidence, m.confidence) AS assignment_confidence,
        COALESCE(a.is_disqualified, m.is_disqualified) AS is_disqualified,
        COALESCE(a.is_generic, m.is_generic) AS is_generic,
        m.llm_reason     AS phase1a_llm_reason,
        a.bucket_reason,
        a.general_reason,
        COALESCE(
            a.reasons,
            CASE
                WHEN m.industry_string IS NULL THEN NULL
                ELSE jsonb_build_object(
                    'phase1a_source', m.source,
                    'llm_reason', m.llm_reason
                )
            END
        ) AS reasons
    FROM contact_page cp
    LEFT JOIN bucket_assignments a
           ON a.contact_id       = cp.contact_id::TEXT
          AND a.bucketing_run_id = p_run_id
    LEFT JOIN bucket_industry_map m
           ON m.bucketing_run_id = p_run_id
          AND m.industry_string  = cp.industry_key
    ORDER BY cp.contact_id;
$$;

GRANT EXECUTE ON FUNCTION public.get_contact_export_page(UUID, TEXT, INTEGER)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
