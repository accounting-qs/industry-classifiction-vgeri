-- Phase 1b v2.1: two-threshold rollup + fix NULL pre_rollup_bucket_name.
--
-- Two bugs / features:
--
-- 1. NULL pre_rollup_bucket_name (run b8469cde-… 2026-05-20)
--    _ct LEFT JOINs bucket_industry_map. When a contact's enrichment text
--    doesn't match any Phase 1a row (no enrichment row at all, classification
--    is NULL/blank, or it just wasn't tagged), m.bucket_name is NULL. The
--    column on bucket_contact_map is NOT NULL → rollup fails for the entire
--    run with 0/13,036 progress. Fix: COALESCE m.bucket_name to 'General'
--    in _ct (matches what the rollup decision would assign anyway when
--    primary_identity is NULL).
--
-- 2. Single min_volume conflates two questions that should be tunable
--    independently:
--       - "When should a sub-identity be its own bucket vs. roll up to its
--         parent identity?"  → sub_identity_min_volume (still stored on the
--         existing min_volume column to avoid breaking older runs).
--       - "When should a tiny identity get its own bucket vs. fall to General?"
--         → identity_min_volume (new column, default 1 = never fold identities
--         into General unless the user explicitly raises it).
--    The old RPC always used min_volume for BOTH gates, which forced users
--    to choose between "break out more sub-buckets" (low value) and "fold
--    tiny identities into General" (high value).
--
-- The RPC's input signature changes (INTEGER → two INTEGERs), so we DROP +
-- CREATE rather than CREATE OR REPLACE.

-- ─────────────────────────────────────────────────────────────────────
-- 1. New column on bucketing_runs.
-- ─────────────────────────────────────────────────────────────────────
ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS identity_min_volume INTEGER DEFAULT 1;

-- ─────────────────────────────────────────────────────────────────────
-- 2. Replace apply_rollup_bucket_assignments with two-threshold version.
-- ─────────────────────────────────────────────────────────────────────
DROP FUNCTION IF EXISTS public.apply_rollup_bucket_assignments(UUID, INTEGER);

CREATE OR REPLACE FUNCTION public.apply_rollup_bucket_assignments(
    p_run_id             UUID,
    p_sub_min_volume     INTEGER,
    p_identity_min_volume INTEGER
)
RETURNS JSONB
LANGUAGE plpgsql
VOLATILE
SET statement_timeout TO '600s'
AS $$
DECLARE
    v_list_names        TEXT[];
    v_sub_min           INTEGER := GREATEST(COALESCE(p_sub_min_volume, 1), 1);
    v_id_min            INTEGER := GREATEST(COALESCE(p_identity_min_volume, 1), 1);
    v_total             BIGINT;
    v_sub_level         BIGINT;
    v_id_level          BIGINT;
    v_general           BIGINT;
    v_disq              BIGINT;
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'apply_rollup_bucket_assignments: run % not found', p_run_id;
    END IF;

    -- Per-contact pre-rollup taxonomy. COALESCE m.bucket_name to 'General'
    -- so contacts with no Phase 1a row (NULL enrichment text, scrape error,
    -- or industry_string the tagger never saw) don't blow up the NOT NULL
    -- constraint on bucket_contact_map.pre_rollup_bucket_name.
    CREATE TEMP TABLE _ct ON COMMIT DROP AS
    SELECT
        c.contact_id,
        c.lead_list_name,
        m.industry_string,
        m.primary_identity,
        m.sub_identity,
        m.sector,
        COALESCE(m.is_disqualified, false)           AS is_disqualified,
        COALESCE(m.is_generic,      false)           AS is_generic,
        COALESCE(m.bucket_name, 'General')           AS pre_rollup_bucket_name,
        m.source                                     AS map_source,
        m.identity_confidence,
        m.sub_identity_confidence,
        m.sector_confidence,
        m.confidence,
        m.canonical_classification,
        m.llm_reason
    FROM contacts c
    LEFT JOIN enrichments e ON e.contact_id = c.contact_id
    LEFT JOIN bucket_industry_map m
        ON m.bucketing_run_id = p_run_id
       AND m.industry_string  = COALESCE(NULLIF(TRIM(e.classification), ''), c.industry)
    WHERE c.lead_list_name = ANY(v_list_names);

    -- Volume per (identity, sub-identity) pair — disqualified contacts excluded.
    CREATE TEMP TABLE _sv ON COMMIT DROP AS
    SELECT primary_identity, sub_identity, COUNT(*)::BIGINT AS n
    FROM _ct
    WHERE primary_identity IS NOT NULL
      AND sub_identity     IS NOT NULL
      AND NOT is_disqualified
    GROUP BY primary_identity, sub_identity;

    -- Volume per identity (ignoring sub-identity) — disqualified excluded.
    CREATE TEMP TABLE _iv ON COMMIT DROP AS
    SELECT primary_identity, COUNT(*)::BIGINT AS n
    FROM _ct
    WHERE primary_identity IS NOT NULL
      AND NOT is_disqualified
    GROUP BY primary_identity;

    -- Final per-contact decision.
    --   sub ≥ v_sub_min          → bucket = sub-identity
    --   else identity ≥ v_id_min → bucket = primary_identity (rolled up)
    --   else                     → General (identity itself too small)
    CREATE TEMP TABLE _assign ON COMMIT DROP AS
    SELECT
        ct.contact_id,
        ct.lead_list_name,
        ct.industry_string,
        ct.primary_identity,
        ct.sub_identity,
        ct.sector,
        ct.is_disqualified,
        ct.is_generic,
        ct.pre_rollup_bucket_name,
        ct.canonical_classification,
        ct.llm_reason,
        ct.identity_confidence,
        ct.sub_identity_confidence,
        ct.sector_confidence,
        ct.confidence,
        CASE
            WHEN ct.is_disqualified                                                          THEN 'Disqualified'
            WHEN ct.primary_identity IS NULL                                                 THEN 'General'
            WHEN ct.sub_identity IS NOT NULL AND COALESCE(sv.n, 0) >= v_sub_min              THEN ct.sub_identity
            WHEN COALESCE(iv.n, 0) >= v_id_min                                               THEN ct.primary_identity
            ELSE 'General'
        END                                                                                  AS bucket_name,
        CASE
            WHEN ct.is_disqualified                                                          THEN 'disqualified'
            WHEN ct.primary_identity IS NULL                                                 THEN 'general'
            WHEN ct.sub_identity IS NOT NULL AND COALESCE(sv.n, 0) >= v_sub_min              THEN 'sub_identity'
            WHEN COALESCE(iv.n, 0) >= v_id_min                                               THEN 'identity'
            ELSE 'general'
        END                                                                                  AS rollup_level,
        CASE
            WHEN ct.is_disqualified                                                          THEN 'disqualified by Phase 1a'
            WHEN ct.primary_identity IS NULL                                                 THEN 'no Phase 1a taxonomy tag'
            WHEN ct.sub_identity IS NULL                                                     THEN 'no sub-identity tagged'
            WHEN COALESCE(sv.n, 0) <  v_sub_min AND COALESCE(iv.n, 0) >= v_id_min            THEN format('sub-identity below sub_min (%s < %s) — rolled up to identity', COALESCE(sv.n, 0), v_sub_min)
            WHEN COALESCE(iv.n, 0) <  v_id_min                                               THEN format('identity below identity_min (%s < %s) — routed to General', COALESCE(iv.n, 0), v_id_min)
            ELSE NULL
        END                                                                                  AS general_reason
    FROM _ct ct
    LEFT JOIN _sv sv USING (primary_identity, sub_identity)
    LEFT JOIN _iv iv USING (primary_identity);

    -- Wipe prior writes for the run before we re-insert.
    DELETE FROM bucket_contact_map  WHERE bucketing_run_id = p_run_id;
    DELETE FROM bucket_assignments  WHERE bucketing_run_id = p_run_id;

    -- bucket_contact_map: per-contact, with the pre-rollup taxonomy.
    INSERT INTO bucket_contact_map (
        bucketing_run_id, contact_id, bucket_name, source, confidence,
        primary_identity, sub_identity, sector,
        is_disqualified, is_generic,
        pre_rollup_bucket_name, rollup_level, general_reason,
        canonical_classification, bucket_reason,
        identity_confidence, sub_identity_confidence, sector_confidence,
        industry_string
    )
    SELECT
        p_run_id, contact_id, bucket_name, 'deterministic_rollup', 1.0,
        primary_identity, sub_identity, sector,
        is_disqualified, is_generic,
        pre_rollup_bucket_name, rollup_level, general_reason,
        canonical_classification, llm_reason,
        identity_confidence, sub_identity_confidence, sector_confidence,
        industry_string
    FROM _assign;

    -- bucket_assignments: final per-contact result, what the UI reads.
    INSERT INTO bucket_assignments (
        bucketing_run_id, contact_id, bucket_name, source, confidence,
        primary_identity, sub_identity, sector,
        is_disqualified, is_generic,
        pre_rollup_bucket_name, rollup_level, general_reason,
        canonical_classification, bucket_reason,
        identity_confidence, sub_identity_confidence, sector_confidence
    )
    SELECT
        p_run_id, contact_id, bucket_name, 'deterministic_rollup', 1.0,
        primary_identity, sub_identity, sector,
        is_disqualified, is_generic,
        pre_rollup_bucket_name, rollup_level, general_reason,
        canonical_classification, llm_reason,
        identity_confidence, sub_identity_confidence, sector_confidence
    FROM _assign;

    -- bucket_industry_map.assigned_bucket_name mirrors the rollup so the
    -- existing Discovered Buckets view stays consistent.
    UPDATE bucket_industry_map m
    SET assigned_bucket_name = CASE
            WHEN m.is_disqualified                                                                THEN 'Disqualified'
            WHEN m.primary_identity IS NULL                                                       THEN 'General'
            WHEN m.sub_identity IS NOT NULL
                 AND COALESCE((SELECT n FROM _sv WHERE primary_identity = m.primary_identity
                                                   AND sub_identity     = m.sub_identity), 0)
                     >= v_sub_min                                                                  THEN m.sub_identity
            WHEN COALESCE((SELECT n FROM _iv WHERE primary_identity = m.primary_identity), 0)
                 >= v_id_min                                                                       THEN m.primary_identity
            ELSE 'General'
        END,
        assigned_bucket_primary_identity = m.primary_identity,
        is_new_bucket = false,
        bucket_assignment_reason = 'deterministic_rollup',
        bucket_assignment_confidence = 1.0
    WHERE m.bucketing_run_id = p_run_id;

    -- Run counters + progress.
    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE rollup_level = 'sub_identity'),
        COUNT(*) FILTER (WHERE rollup_level = 'identity'),
        COUNT(*) FILTER (WHERE bucket_name  = 'General'),
        COUNT(*) FILTER (WHERE bucket_name  = 'Disqualified')
    INTO v_total, v_sub_level, v_id_level, v_general, v_disq
    FROM _assign;

    UPDATE bucketing_runs
    SET assigned_contacts        = v_total,
        assignment_completed_at  = NOW(),
        status                   = 'completed',
        progress                 = jsonb_build_object(
            'phase',           'phase1b',
            'step',            'rollup_complete',
            'current',         v_total,
            'total',           v_total,
            'pct',             100,
            'note',            'Deterministic rollup complete',
            'elapsed_seconds', 0,
            'eta_seconds',     0,
            'updated_at',      to_jsonb(NOW())
        )
    WHERE id = p_run_id;

    RETURN jsonb_build_object(
        'total_contacts',         v_total,
        'at_sub_identity',        v_sub_level,
        'rolled_up_to_identity',  v_id_level,
        'general',                v_general,
        'disqualified',           v_disq,
        'sub_min_volume',         v_sub_min,
        'identity_min_volume',    v_id_min
    );
END;
$$;

GRANT EXECUTE ON FUNCTION public.apply_rollup_bucket_assignments(UUID, INTEGER, INTEGER)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
