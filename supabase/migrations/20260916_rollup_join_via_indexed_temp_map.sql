-- =====================================================================
-- Remove the plan shape that made Phase 1b quadratic
--
-- Companion to 20260916_rollup_stale_stats_quadratic_plan.sql. That
-- migration keeps the planner's estimates accurate; this one removes the
-- dependency on them being accurate.
--
-- The failure it prevents (run 7258e616, 124k contacts / 84k industries):
-- when contacts.lead_list_name statistics were stale, the planner
-- estimated 1 row instead of 124,075 and joined bucket_industry_map on
-- bucketing_run_id alone, demoting the industry_string equality to a join
-- filter — 124,075 x 84,479 comparisons, >30 minutes, cancelled.
--
-- The reason a bad estimate could do that much damage is that the run's
-- slice of bucket_industry_map has no index on industry_string *by
-- itself*. The primary key is (bucketing_run_id, industry_string), so a
-- lookup by industry_string alone is not indexable, and the planner's only
-- nested-loop option is "scan the whole run and filter".
--
-- Fix: materialize the run's slice into a temp table keyed on
-- industry_string, then join against that. Now every join strategy is
-- cheap — a hash join reads it once, and a nested loop does a unique index
-- lookup per contact instead of an 84k scan. There is no longer a plan the
-- planner can pick that degrades quadratically, whatever it estimates.
--
-- The extra materialization costs one pass over ~84k rows (sub-second)
-- against a worst case of half an hour.
--
-- Behavior is otherwise unchanged: same COALESCE(classification, industry)
-- precedence, same columns, same outputs. Both functions get the same
-- treatment because both contained the identical join.
--
-- Idempotent: safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Shared helper: the run's industry map, keyed for lookup by
-- industry_string. Creates TEMP TABLE _map (dropped at commit).
--
-- ANALYZE matters as much as the index: a freshly created temp table has
-- no statistics at all, and the planner would otherwise fall back to a
-- hardcoded guess for it — reintroducing exactly the class of misestimate
-- this migration exists to defuse.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public._bucketing_build_run_map(p_run_id UUID)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    CREATE TEMP TABLE _map ON COMMIT DROP AS
    SELECT
        industry_string,
        primary_identity,
        sub_identity,
        sector,
        COALESCE(is_disqualified, false) AS is_disqualified,
        COALESCE(is_generic,      false) AS is_generic,
        bucket_name,
        source,
        identity_confidence,
        sub_identity_confidence,
        sector_confidence,
        confidence,
        canonical_classification,
        llm_reason
    FROM bucket_industry_map
    WHERE bucketing_run_id = p_run_id;

    -- Unique: bucket_industry_map's PK is (bucketing_run_id,
    -- industry_string), so within one run industry_string is unique. Saying
    -- so lets the planner treat the join as at-most-one-row and keeps the
    -- LEFT JOIN from ever fanning out.
    CREATE UNIQUE INDEX _map_industry_string_idx ON _map (industry_string);

    ANALYZE _map;
END;
$$;

COMMENT ON FUNCTION public._bucketing_build_run_map(UUID) IS
    'Internal to bucketing Phase 1a/1b. Builds TEMP TABLE _map — the run''s '
    'bucket_industry_map slice, uniquely indexed on industry_string — so the '
    'per-contact join cannot degrade into a per-contact scan of the run. '
    'See 20260916_rollup_join_via_indexed_temp_map.sql.';

GRANT EXECUTE ON FUNCTION public._bucketing_build_run_map(UUID)
    TO anon, authenticated, service_role;

-- =====================================================================
-- Phase 1b rollup, rebuilt on the indexed temp map.
--
-- Unchanged from 20260520_rollup_two_thresholds_and_null_fix.sql except:
--   * joins _map instead of bucket_industry_map directly
--   * indexes + ANALYZEs every temp table before it is joined
--   * no `SET statement_timeout` (it never applied — see the companion
--     migration; the caller sets it on the connection)
-- =====================================================================
CREATE OR REPLACE FUNCTION public.apply_rollup_bucket_assignments(
    p_run_id             UUID,
    p_sub_min_volume     INTEGER,
    p_identity_min_volume INTEGER
)
RETURNS JSONB
LANGUAGE plpgsql
VOLATILE
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

    PERFORM public._bucketing_build_run_map(p_run_id);

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
    LEFT JOIN _map m
        ON m.industry_string = COALESCE(NULLIF(TRIM(e.classification), ''), c.industry)
    WHERE c.lead_list_name = ANY(v_list_names);

    ANALYZE _ct;

    -- Volume per (identity, sub-identity) pair — disqualified contacts excluded.
    CREATE TEMP TABLE _sv ON COMMIT DROP AS
    SELECT primary_identity, sub_identity, COUNT(*)::BIGINT AS n
    FROM _ct
    WHERE primary_identity IS NOT NULL
      AND sub_identity     IS NOT NULL
      AND NOT is_disqualified
    GROUP BY primary_identity, sub_identity;

    CREATE UNIQUE INDEX _sv_idx ON _sv (primary_identity, sub_identity);
    ANALYZE _sv;

    -- Volume per identity (ignoring sub-identity) — disqualified excluded.
    CREATE TEMP TABLE _iv ON COMMIT DROP AS
    SELECT primary_identity, COUNT(*)::BIGINT AS n
    FROM _ct
    WHERE primary_identity IS NOT NULL
      AND NOT is_disqualified
    GROUP BY primary_identity;

    CREATE UNIQUE INDEX _iv_idx ON _iv (primary_identity);
    ANALYZE _iv;

    -- Final per-contact decision.
    --   sub >= v_sub_min          -> bucket = sub-identity
    --   else identity >= v_id_min -> bucket = primary_identity (rolled up)
    --   else                      -> General (identity itself too small)
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

    ANALYZE _assign;

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
    -- existing Discovered Buckets view stays consistent. _sv and _iv are
    -- indexed above, so these correlated subqueries are index lookups
    -- rather than a full scan of _sv per map row.
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
        error_message            = NULL,
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

-- =====================================================================
-- Phase 1a per-contact explosion, rebuilt on the same indexed temp map.
--
-- Unchanged from 20260520_finalize_per_contact_taxonomy.sql except:
--   * joins _map instead of bucket_industry_map directly
--   * the two DELETEs moved from before the join to just before the
--     INSERTs. They took row locks on every existing per-contact row for
--     the run and then held them for the entire duration of the join —
--     which is how a slow explosion used to wedge the rollup behind it
--     (see 20260804_bucketing_scale_safety.sql, same root incident). The
--     work between them is read-only, so nothing depends on the old
--     ordering; this just shrinks the lock window to the writes.
--   * no `SET statement_timeout` (it never applied — see the companion
--     migration; the caller sets it on the connection)
-- =====================================================================
CREATE OR REPLACE FUNCTION public.finalize_per_contact_taxonomy(
    p_run_id UUID
)
RETURNS JSONB
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_list_names  TEXT[];
    v_total       BIGINT;
    v_tagged_id   BIGINT;
    v_tagged_sub  BIGINT;
    v_tagged_sec  BIGINT;
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'finalize_per_contact_taxonomy: run % not found', p_run_id;
    END IF;

    PERFORM public._bucketing_build_run_map(p_run_id);

    -- Per-contact join.
    CREATE TEMP TABLE _ct ON COMMIT DROP AS
    SELECT
        c.contact_id,
        c.lead_list_name,
        COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_string,
        m.primary_identity,
        m.sub_identity,
        m.sector,
        COALESCE(m.is_disqualified, false)           AS is_disqualified,
        COALESCE(m.is_generic,      false)           AS is_generic,
        COALESCE(m.bucket_name, 'General')           AS pre_rollup_bucket_name,
        m.canonical_classification,
        m.llm_reason,
        m.identity_confidence,
        m.sub_identity_confidence,
        m.sector_confidence,
        m.confidence
    FROM contacts c
    LEFT JOIN enrichments e ON e.contact_id = c.contact_id
    LEFT JOIN _map m
        ON m.industry_string = COALESCE(NULLIF(TRIM(e.classification), ''), c.industry)
    WHERE c.lead_list_name = ANY(v_list_names);

    ANALYZE _ct;

    -- Finalize is the terminal Phase 1a step — anything older is stale.
    DELETE FROM bucket_contact_map  WHERE bucketing_run_id = p_run_id;
    DELETE FROM bucket_assignments  WHERE bucketing_run_id = p_run_id;

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
        p_run_id, contact_id, 'Pending', 'phase1a_finalize', 1.0,
        primary_identity, sub_identity, sector,
        is_disqualified, is_generic,
        pre_rollup_bucket_name, 'pending', NULL,
        canonical_classification, NULL,
        identity_confidence, sub_identity_confidence, sector_confidence,
        industry_string
    FROM _ct;

    INSERT INTO bucket_assignments (
        bucketing_run_id, contact_id, bucket_name, source, confidence,
        primary_identity, sub_identity, sector,
        is_disqualified, is_generic,
        pre_rollup_bucket_name, rollup_level, general_reason,
        canonical_classification, bucket_reason,
        identity_confidence, sub_identity_confidence, sector_confidence
    )
    SELECT
        p_run_id, contact_id, 'Pending', 'phase1a_finalize', 1.0,
        primary_identity, sub_identity, sector,
        is_disqualified, is_generic,
        pre_rollup_bucket_name, 'pending', NULL,
        canonical_classification, NULL,
        identity_confidence, sub_identity_confidence, sector_confidence
    FROM _ct;

    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE primary_identity IS NOT NULL),
        COUNT(*) FILTER (WHERE sub_identity     IS NOT NULL),
        COUNT(*) FILTER (WHERE sector           IS NOT NULL)
    INTO v_total, v_tagged_id, v_tagged_sub, v_tagged_sec
    FROM _ct;

    RETURN jsonb_build_object(
        'total_contacts',        v_total,
        'with_primary_identity', v_tagged_id,
        'with_sub_identity',     v_tagged_sub,
        'with_sector',           v_tagged_sec
    );
END;
$$;

GRANT EXECUTE ON FUNCTION public.finalize_per_contact_taxonomy(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
