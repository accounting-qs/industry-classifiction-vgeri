-- =====================================================================
-- Single-query CSV export source
--
-- Companion to get_contact_export_page (20260514). Same projection +
-- same COALESCE precedence, but no keyset / no LIMIT — returns every
-- contact for the run in one streaming result. Consumed by server.ts
-- runCsvExportJob via `COPY (SELECT * FROM bucketing_run_export_rows(:id))
-- TO STDOUT WITH CSV HEADER`, piped through pg-copy-streams → gzip →
-- file. Collapses the previous 148 × keyset RPC round-trips into one
-- Postgres backend cursor, dropping a 147k-contact export from ~5 min
-- to ~30 s.
--
-- get_contact_export_page stays in place for any caller that still
-- needs paged access; this function is purely the full-export source.
--
-- Idempotent: CREATE OR REPLACE.
-- =====================================================================

CREATE OR REPLACE FUNCTION public.bucketing_run_export_rows(
    p_run_id UUID
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
    sub_identity               TEXT,
    sector                     TEXT,
    canonical_classification   TEXT,
    bucket_name                TEXT,
    pre_rollup_bucket_name     TEXT,
    rollup_level               TEXT,
    assigned_bucket_name       TEXT,
    assigned_bucket_primary_identity TEXT,
    assignment_source          TEXT,
    identity_confidence        NUMERIC,
    sub_identity_confidence    NUMERIC,
    sector_confidence          NUMERIC,
    confidence                 NUMERIC,
    is_disqualified            BOOLEAN,
    is_generic                 BOOLEAN,
    phase1a_llm_reason         TEXT,
    bucket_reason              TEXT,
    general_reason             TEXT,
    reasons                    JSONB
)
LANGUAGE sql
STABLE
SET statement_timeout TO '600s'
AS $$
    -- One CTE per join target, joined at the end. Postgres builds hash
    -- tables on the run-filtered bucket_assignments + bucket_industry_map
    -- slices (typically ~100k–150k rows each, both indexed by run_id),
    -- then streams the contacts × enrichments join through them. For a
    -- full-run export this is faster than the keyset CTE-paginate
    -- pattern get_contact_export_page uses, because we want every row
    -- anyway — no per-page replanning, no LIMIT-induced nested loops.
    WITH base AS (
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
            COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_key
        FROM contacts c
        LEFT JOIN enrichments e
               ON e.contact_id = c.contact_id
        WHERE c.lead_list_name = ANY(
                  SELECT UNNEST(list_names) FROM bucketing_runs WHERE id = p_run_id
              )
    )
    SELECT
        b.contact_id::TEXT AS contact_id,
        b.email,
        b.first_name,
        b.last_name,
        b.company_name,
        b.company_website,
        b.lead_list_name,
        b.industry,
        b.enrichment_status,
        b.enrichment_classification,
        b.enrichment_confidence,
        b.enrichment_reasoning,
        COALESCE(NULLIF(a.primary_identity, ''), m.primary_identity) AS primary_identity,
        COALESCE(NULLIF(a.sub_identity, ''), m.sub_identity) AS sub_identity,
        COALESCE(NULLIF(a.sector, ''), m.sector) AS sector,
        COALESCE(NULLIF(a.canonical_classification, ''), m.canonical_classification) AS canonical_classification,
        a.bucket_name,
        COALESCE(NULLIF(a.pre_rollup_bucket_name, ''), m.bucket_name) AS pre_rollup_bucket_name,
        a.rollup_level,
        m.assigned_bucket_name,
        m.assigned_bucket_primary_identity,
        COALESCE(NULLIF(a.source, ''), m.source) AS assignment_source,
        COALESCE(a.identity_confidence, m.identity_confidence) AS identity_confidence,
        COALESCE(a.sub_identity_confidence, m.sub_identity_confidence) AS sub_identity_confidence,
        COALESCE(a.sector_confidence, m.sector_confidence) AS sector_confidence,
        COALESCE(a.confidence, m.confidence) AS confidence,
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
    FROM base b
    LEFT JOIN bucket_assignments a
           ON a.contact_id       = b.contact_id::TEXT
          AND a.bucketing_run_id = p_run_id
    LEFT JOIN bucket_industry_map m
           ON m.bucketing_run_id = p_run_id
          AND m.industry_string  = b.industry_key
    ORDER BY b.contact_id;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_run_export_rows(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
