-- =====================================================================
-- Extra contact fields from Apollo CSV exports
--
-- Adds seven passthrough columns to contacts so the import wizard can
-- store Apollo's country / firmographic fields, and threads them into
-- the bucketing CSV export (bucketing_run_export_rows).
--
--   contact_country          ← CSV "Country"
--   employees_count          ← CSV "Employees Count"
--   seniority                ← CSV "Seniority"
--   company_founded_year     ← CSV "Company Founded Year"  (funded year)
--   company_total_funding    ← CSV "Company Total Funding" (raw numeric)
--   company_annual_revenue   ← CSV "Company Annual Revenue" (raw numeric)
--   company_country          ← CSV "Company Country"
--
-- All TEXT: import is a verbatim passthrough (empty cells stay NULL),
-- so we don't want a stray non-numeric cell to fail the whole insert.
-- `title` (job title) and `industry` (apollo industry) already exist —
-- this migration only surfaces them in the export projection.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE OR REPLACE.
-- =====================================================================

ALTER TABLE public.contacts ADD COLUMN IF NOT EXISTS contact_country        TEXT;
ALTER TABLE public.contacts ADD COLUMN IF NOT EXISTS employees_count        TEXT;
ALTER TABLE public.contacts ADD COLUMN IF NOT EXISTS seniority              TEXT;
ALTER TABLE public.contacts ADD COLUMN IF NOT EXISTS company_founded_year   TEXT;
ALTER TABLE public.contacts ADD COLUMN IF NOT EXISTS company_total_funding  TEXT;
ALTER TABLE public.contacts ADD COLUMN IF NOT EXISTS company_annual_revenue TEXT;
ALTER TABLE public.contacts ADD COLUMN IF NOT EXISTS company_country        TEXT;

-- ---------------------------------------------------------------------
-- Re-create the full-run CSV export source with the new columns +
-- title/industry surfaced. Same body as 20260520_csv_export_view.sql,
-- only the projection grew — every join / COALESCE precedence is
-- unchanged. New firmographic columns land right after `industry` so
-- the existing bucketing columns keep their positions.
--
-- DROP first: the RETURNS TABLE signature changed (new OUT columns), and
-- Postgres refuses to CREATE OR REPLACE across a return-type change.
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.bucketing_run_export_rows(UUID);

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
    title                      TEXT,
    seniority                  TEXT,
    contact_country            TEXT,
    employees_count            TEXT,
    company_founded_year       TEXT,
    company_total_funding      TEXT,
    company_annual_revenue     TEXT,
    company_country            TEXT,
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
            c.title,
            c.seniority,
            c.contact_country,
            c.employees_count,
            c.company_founded_year,
            c.company_total_funding,
            c.company_annual_revenue,
            c.company_country,
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
        b.title,
        b.seniority,
        b.contact_country,
        b.employees_count,
        b.company_founded_year,
        b.company_total_funding,
        b.company_annual_revenue,
        b.company_country,
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
