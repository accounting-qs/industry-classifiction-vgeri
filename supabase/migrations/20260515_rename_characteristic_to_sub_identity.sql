-- =====================================================================
-- Rename: "characteristic" → "sub_identity"
--
-- The Layer-2 taxonomy field has been called "characteristic" since v3
-- but the term doesn't match how the user thinks about it — they treat
-- these as sub-identities (a more specific identity nested under the
-- primary one). Renaming everywhere: 1 table + 7 columns across 3
-- tables. ALTER TABLE RENAME preserves data, indexes, and FK
-- references — this is purely a schema-name change, no data movement.
--
-- After this migration, all RPCs that reference these columns are
-- redefined with the new names. The TypeScript layer is updated in
-- the same release; older deploys will start throwing
-- "column 'characteristic' does not exist" errors against the new
-- schema, so do NOT roll the server forward without rolling the DB
-- forward at the same time.
--
-- Idempotent: every RENAME and CREATE OR REPLACE is wrapped in a
-- check so re-running the migration is a no-op.
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- 1. Table rename: taxonomy_characteristics → taxonomy_sub_identities
-- ─────────────────────────────────────────────────────────────────────

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'taxonomy_characteristics')
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'taxonomy_sub_identities')
    THEN
        ALTER TABLE public.taxonomy_characteristics RENAME TO taxonomy_sub_identities;
    END IF;
END$$;

-- ─────────────────────────────────────────────────────────────────────
-- 2. Column renames (3 tables × up to 3 columns)
--
-- Wrapped in DO blocks so the RENAME only fires if the OLD column
-- exists and the NEW one doesn't — idempotent re-run.
-- ─────────────────────────────────────────────────────────────────────

DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT * FROM (VALUES
            ('bucket_industry_map',  'characteristic',            'sub_identity'),
            ('bucket_industry_map',  'is_new_characteristic',     'is_new_sub_identity'),
            ('bucket_industry_map',  'characteristic_confidence', 'sub_identity_confidence'),
            ('bucket_assignments',   'characteristic',            'sub_identity'),
            ('bucket_assignments',   'characteristic_confidence', 'sub_identity_confidence'),
            ('bucket_contact_map',   'characteristic',            'sub_identity'),
            ('bucket_contact_map',   'characteristic_confidence', 'sub_identity_confidence')
        ) AS t(tbl, old_col, new_col)
    LOOP
        IF EXISTS (SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=rec.tbl AND column_name=rec.old_col)
           AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                            WHERE table_schema='public' AND table_name=rec.tbl AND column_name=rec.new_col)
        THEN
            EXECUTE format('ALTER TABLE public.%I RENAME COLUMN %I TO %I', rec.tbl, rec.old_col, rec.new_col);
        END IF;
    END LOOP;
END$$;

-- ─────────────────────────────────────────────────────────────────────
-- 3. RPC redefinitions
--
-- Every function that referenced the old columns or the renamed table
-- is redefined here with the new names. CREATE OR REPLACE so the
-- migration replaces in place — no DROP needed.
-- ─────────────────────────────────────────────────────────────────────

-- 3a. bucketing_schema_gaps — required-columns list updated for v6.
CREATE OR REPLACE FUNCTION public.bucketing_schema_gaps()
RETURNS TABLE (table_name TEXT, column_name TEXT)
LANGUAGE sql
STABLE
AS $$
    WITH required(t, c) AS (VALUES
        -- bucket_industry_map (Phase 1a per-industry tags)
        ('bucket_industry_map', 'bucketing_run_id'),
        ('bucket_industry_map', 'industry_string'),
        ('bucket_industry_map', 'bucket_name'),
        ('bucket_industry_map', 'source'),
        ('bucket_industry_map', 'confidence'),
        ('bucket_industry_map', 'primary_identity'),
        ('bucket_industry_map', 'sub_identity'),
        ('bucket_industry_map', 'sector'),
        ('bucket_industry_map', 'is_new_identity'),
        ('bucket_industry_map', 'is_new_sub_identity'),
        ('bucket_industry_map', 'is_new_sector'),
        ('bucket_industry_map', 'is_disqualified'),
        ('bucket_industry_map', 'is_generic'),
        ('bucket_industry_map', 'needs_qa'),
        ('bucket_industry_map', 'raw_industry'),
        ('bucket_industry_map', 'llm_reason'),
        ('bucket_industry_map', 'canonical_classification'),
        ('bucket_industry_map', 'identity_confidence'),
        ('bucket_industry_map', 'sub_identity_confidence'),
        ('bucket_industry_map', 'sector_confidence'),
        ('bucket_industry_map', 'assigned_bucket_name'),
        ('bucket_industry_map', 'assigned_bucket_primary_identity'),
        ('bucket_industry_map', 'is_new_bucket'),
        ('bucket_industry_map', 'bucket_assignment_reason'),
        ('bucket_industry_map', 'bucket_assignment_confidence'),

        -- bucket_assignments (Phase 1b per-contact final routing)
        ('bucket_assignments', 'bucketing_run_id'),
        ('bucket_assignments', 'contact_id'),
        ('bucket_assignments', 'bucket_name'),
        ('bucket_assignments', 'source'),
        ('bucket_assignments', 'confidence'),
        ('bucket_assignments', 'bucket_leaf'),
        ('bucket_assignments', 'bucket_ancestor'),
        ('bucket_assignments', 'bucket_root'),
        ('bucket_assignments', 'primary_identity'),
        ('bucket_assignments', 'sub_identity'),
        ('bucket_assignments', 'sector'),
        ('bucket_assignments', 'pre_rollup_bucket_name'),
        ('bucket_assignments', 'rollup_level'),
        ('bucket_assignments', 'general_reason'),
        ('bucket_assignments', 'reasons'),
        ('bucket_assignments', 'is_generic'),
        ('bucket_assignments', 'is_disqualified'),
        ('bucket_assignments', 'canonical_classification'),
        ('bucket_assignments', 'bucket_reason'),
        ('bucket_assignments', 'identity_confidence'),
        ('bucket_assignments', 'sub_identity_confidence'),
        ('bucket_assignments', 'sector_confidence'),
        ('bucket_assignments', 'assigned_at'),

        -- bucket_contact_map (Phase 1b pre-rollup decisions)
        ('bucket_contact_map', 'bucketing_run_id'),
        ('bucket_contact_map', 'contact_id'),
        ('bucket_contact_map', 'industry_string'),
        ('bucket_contact_map', 'primary_identity'),
        ('bucket_contact_map', 'sub_identity'),
        ('bucket_contact_map', 'sector'),
        ('bucket_contact_map', 'pre_rollup_bucket_name'),
        ('bucket_contact_map', 'bucket_name'),
        ('bucket_contact_map', 'rollup_level'),
        ('bucket_contact_map', 'source'),
        ('bucket_contact_map', 'confidence'),
        ('bucket_contact_map', 'leaf_score'),
        ('bucket_contact_map', 'ancestor_score'),
        ('bucket_contact_map', 'root_score'),
        ('bucket_contact_map', 'is_generic'),
        ('bucket_contact_map', 'is_disqualified'),
        ('bucket_contact_map', 'general_reason'),
        ('bucket_contact_map', 'reasons'),
        ('bucket_contact_map', 'canonical_classification'),
        ('bucket_contact_map', 'bucket_reason'),
        ('bucket_contact_map', 'identity_confidence'),
        ('bucket_contact_map', 'sub_identity_confidence'),
        ('bucket_contact_map', 'sector_confidence'),
        ('bucket_contact_map', 'assigned_at'),

        -- bucketing_runs (run lifecycle)
        ('bucketing_runs', 'id'),
        ('bucketing_runs', 'name'),
        ('bucketing_runs', 'list_names'),
        ('bucketing_runs', 'min_volume'),
        ('bucketing_runs', 'bucket_budget'),
        ('bucketing_runs', 'status'),
        ('bucketing_runs', 'taxonomy_proposal'),
        ('bucketing_runs', 'taxonomy_final'),
        ('bucketing_runs', 'taxonomy_model'),
        ('bucketing_runs', 'preferred_library_ids'),
        ('bucketing_runs', 'total_contacts'),
        ('bucketing_runs', 'assigned_contacts'),
        ('bucketing_runs', 'cost_usd'),
        ('bucketing_runs', 'created_at'),
        ('bucketing_runs', 'taxonomy_completed_at'),
        ('bucketing_runs', 'assignment_completed_at'),
        ('bucketing_runs', 'progress'),
        ('bucketing_runs', 'cancel_requested'),
        ('bucketing_runs', 'error_message'),
        ('bucketing_runs', 'taxonomy_snapshot'),
        ('bucketing_runs', 'taxonomy_version'),
        ('bucketing_runs', 'quality_warnings'),
        ('bucketing_runs', 'coverage_summary'),
        ('bucketing_runs', 'generic_audit'),
        ('bucketing_runs', 'apply_identity_dq_cascade'),

        -- Renamed library tables
        ('taxonomy_identities', 'id'),
        ('taxonomy_identities', 'name'),
        ('taxonomy_identities', 'description'),
        ('taxonomy_identities', 'is_disqualified'),
        ('taxonomy_identities', 'created_by'),
        ('taxonomy_identities', 'archived'),

        ('taxonomy_sub_identities', 'id'),
        ('taxonomy_sub_identities', 'name'),
        ('taxonomy_sub_identities', 'parent_identity'),
        ('taxonomy_sub_identities', 'description'),
        ('taxonomy_sub_identities', 'created_by'),
        ('taxonomy_sub_identities', 'archived'),

        ('taxonomy_sectors', 'id'),
        ('taxonomy_sectors', 'name'),
        ('taxonomy_sectors', 'synonyms'),
        ('taxonomy_sectors', 'description'),
        ('taxonomy_sectors', 'created_by'),
        ('taxonomy_sectors', 'archived'),

        ('bucketing_run_logs', 'id'),
        ('bucketing_run_logs', 'bucketing_run_id'),
        ('bucketing_run_logs', 'timestamp'),
        ('bucketing_run_logs', 'level'),
        ('bucketing_run_logs', 'message')
    )
    SELECT r.t, r.c
    FROM required r
    LEFT JOIN information_schema.columns c
           ON c.table_schema = 'public'
          AND c.table_name = r.t
          AND c.column_name = r.c
    WHERE c.column_name IS NULL;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_schema_gaps()
    TO anon, authenticated, service_role;

-- 3b. get_bucket_map_counts — unchanged JOIN logic, no renamed columns
--     in this RPC's body. Kept here as a no-op CREATE OR REPLACE so the
--     full set of RPCs stays grouped in one migration.

CREATE OR REPLACE FUNCTION public.get_bucket_map_counts(p_run_id UUID)
RETURNS TABLE (
    bucket_name     TEXT,
    contact_count   BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '120s'
AS $$
    WITH contact_industry AS (
        SELECT
            c.contact_id,
            COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_str
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE c.lead_list_name = ANY(
            SELECT UNNEST(list_names) FROM bucketing_runs WHERE id = p_run_id
        )
    )
    SELECT
        m.bucket_name,
        COUNT(*)::BIGINT AS contact_count
    FROM contact_industry ci
    JOIN bucket_industry_map m ON m.industry_string = ci.industry_str
    WHERE m.bucketing_run_id = p_run_id
    GROUP BY m.bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_map_counts(UUID)
    TO anon, authenticated, service_role;

-- 3c. get_bucket_assignment_counts — same shape, no renamed cols here.
CREATE OR REPLACE FUNCTION public.get_bucket_assignment_counts(p_run_id UUID)
RETURNS TABLE (
    bucket_name     TEXT,
    contact_count   BIGINT,
    other_sources   JSONB
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT
        bucket_name,
        COUNT(*)::BIGINT AS contact_count,
        jsonb_object_agg(source, source_count) AS other_sources
    FROM (
        SELECT bucket_name, source, COUNT(*) AS source_count
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY bucket_name, source
    ) sub
    GROUP BY bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_assignment_counts(UUID)
    TO anon, authenticated, service_role;

-- 3d. get_assigned_bucket_counts — unchanged, no renamed cols in body.
CREATE OR REPLACE FUNCTION public.get_assigned_bucket_counts(p_run_id UUID)
RETURNS TABLE (
    assigned_bucket_name             TEXT,
    assigned_bucket_primary_identity TEXT,
    is_new_bucket                    BOOLEAN,
    contact_count                    BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '120s'
AS $$
    WITH contact_industry AS (
        SELECT
            c.contact_id,
            COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_str
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE c.lead_list_name = ANY(
            SELECT UNNEST(list_names) FROM bucketing_runs WHERE id = p_run_id
        )
    )
    SELECT
        m.assigned_bucket_name,
        m.assigned_bucket_primary_identity,
        m.is_new_bucket,
        COUNT(*)::BIGINT AS contact_count
    FROM contact_industry ci
    JOIN bucket_industry_map m ON m.industry_string = ci.industry_str
    WHERE m.bucketing_run_id = p_run_id
      AND m.assigned_bucket_name IS NOT NULL
    GROUP BY m.assigned_bucket_name, m.assigned_bucket_primary_identity, m.is_new_bucket;
$$;

GRANT EXECUTE ON FUNCTION public.get_assigned_bucket_counts(UUID)
    TO anon, authenticated, service_role;

-- 3e. get_contact_export_page — full re-definition with the renamed
--     columns. Returned column "sub_identity" replaces "characteristic"
--     and "sub_identity_confidence" replaces "characteristic_confidence".
--     Other behavior identical to the version in 20260514.

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
    sub_identity               TEXT,
    sector                     TEXT,
    canonical_classification   TEXT,
    bucket_name                TEXT,
    pre_rollup_bucket_name     TEXT,
    rollup_level               TEXT,
    assignment_source          TEXT,
    identity_confidence        NUMERIC,
    sub_identity_confidence    NUMERIC,
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
        COALESCE(NULLIF(a.sub_identity, ''), m.sub_identity) AS sub_identity,
        COALESCE(NULLIF(a.sector, ''), m.sector) AS sector,
        COALESCE(NULLIF(a.canonical_classification, ''), m.canonical_classification) AS canonical_classification,
        a.bucket_name,
        COALESCE(NULLIF(a.pre_rollup_bucket_name, ''), m.bucket_name) AS pre_rollup_bucket_name,
        a.rollup_level,
        COALESCE(NULLIF(a.source, ''), m.source) AS assignment_source,
        COALESCE(a.identity_confidence, m.identity_confidence) AS identity_confidence,
        COALESCE(a.sub_identity_confidence, m.sub_identity_confidence) AS sub_identity_confidence,
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
