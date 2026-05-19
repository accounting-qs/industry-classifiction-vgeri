-- Phase 1b v2: deterministic bucket assignment via taxonomy + volume rollup.
--
-- Old flow: LLM cascade (JOIN → library match → embedding → LLM routing) over
-- every contact + a separate "Bucket Assignment" LLM step that mapped Phase 1a
-- tags onto campaign buckets. Two LLM passes, ~$0.10 cost, 3+ hours wall time,
-- and an in-memory `assignedRows[]` accumulator that OOMed the 2 GB Render
-- instance after ~3 hours.
--
-- New flow: bucket name IS the taxonomy name.
--   - If the (identity, sub-identity) pair has >= min_volume contacts in the
--     run, bucket_name = sub_identity.
--   - Else if the identity has >= min_volume contacts in the run,
--     bucket_name = primary_identity (rolled up).
--   - Else bucket_name = 'General' (rolled up further).
--   - is_disqualified=true → bucket_name = 'Disqualified'.
--   - No taxonomy tags at all → bucket_name = 'General'.
--
-- All in one SQL transaction. No LLM. No Node-side accumulator. Runs in
-- seconds even for 293k-contact lists. statement_timeout=600s for headroom.

CREATE OR REPLACE FUNCTION public.apply_rollup_bucket_assignments(
    p_run_id     UUID,
    p_min_volume INTEGER
)
RETURNS JSONB
LANGUAGE plpgsql
VOLATILE
SET statement_timeout TO '600s'
AS $$
DECLARE
    v_list_names  TEXT[];
    v_effective_min INTEGER := GREATEST(COALESCE(p_min_volume, 1), 1);
    v_total       BIGINT;
    v_sub_level   BIGINT;
    v_id_level    BIGINT;
    v_general     BIGINT;
    v_disq        BIGINT;
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'apply_rollup_bucket_assignments: run % not found', p_run_id;
    END IF;

    -- Per-contact pre-rollup taxonomy. Join contacts → enrichments → Phase 1a
    -- map via the same COALESCE(classification, industry) used everywhere else
    -- so the bucket choice matches what discovered-buckets shows.
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
        m.bucket_name                                AS pre_rollup_bucket_name,
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

    -- Final per-contact decision. Materialise so we can drive both
    -- bucket_assignments INSERT and bucket_contact_map INSERT off it.
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
            WHEN ct.is_disqualified                                                            THEN 'Disqualified'
            WHEN ct.primary_identity IS NULL                                                   THEN 'General'
            WHEN ct.sub_identity IS NOT NULL AND COALESCE(sv.n, 0) >= v_effective_min          THEN ct.sub_identity
            WHEN COALESCE(iv.n, 0) >= v_effective_min                                          THEN ct.primary_identity
            ELSE 'General'
        END                                                                                    AS bucket_name,
        CASE
            WHEN ct.is_disqualified                                                            THEN 'disqualified'
            WHEN ct.primary_identity IS NULL                                                   THEN 'general'
            WHEN ct.sub_identity IS NOT NULL AND COALESCE(sv.n, 0) >= v_effective_min          THEN 'sub_identity'
            WHEN COALESCE(iv.n, 0) >= v_effective_min                                          THEN 'identity'
            ELSE 'general'
        END                                                                                    AS rollup_level,
        CASE
            WHEN ct.is_disqualified                                                            THEN 'disqualified by Phase 1a'
            WHEN ct.primary_identity IS NULL                                                   THEN 'no Phase 1a taxonomy tag'
            WHEN ct.sub_identity IS NULL                                                       THEN 'no sub-identity tagged'
            WHEN COALESCE(sv.n, 0) <  v_effective_min AND COALESCE(iv.n, 0) >= v_effective_min THEN format('sub-identity below min_volume (%s < %s) — rolled up to identity', COALESCE(sv.n, 0), v_effective_min)
            WHEN COALESCE(sv.n, 0) <  v_effective_min                                          THEN format('identity below min_volume (%s < %s) — routed to General', COALESCE(iv.n, 0), v_effective_min)
            ELSE NULL
        END                                                                                    AS general_reason
    FROM _ct ct
    LEFT JOIN _sv sv USING (primary_identity, sub_identity)
    LEFT JOIN _iv iv USING (primary_identity);

    -- Wipe prior writes for the run before we re-insert.
    DELETE FROM bucket_contact_map  WHERE bucketing_run_id = p_run_id;
    DELETE FROM bucket_assignments  WHERE bucketing_run_id = p_run_id;

    -- bucket_contact_map: per-contact, with the pre-rollup taxonomy. Source
    -- stamps this as a deterministic rollup so future code paths can
    -- distinguish from LLM-routed legacy rows.
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

    -- Update bucket_industry_map.assigned_bucket_name so the existing
    -- "Discovered buckets" view (which groups by assigned_bucket_name +
    -- assigned_bucket_primary_identity) shows the rollup result.
    UPDATE bucket_industry_map m
    SET assigned_bucket_name = CASE
            WHEN m.is_disqualified                                                                    THEN 'Disqualified'
            WHEN m.primary_identity IS NULL                                                           THEN 'General'
            WHEN m.sub_identity IS NOT NULL
                 AND COALESCE((SELECT n FROM _sv WHERE primary_identity = m.primary_identity
                                                   AND sub_identity     = m.sub_identity), 0)
                     >= v_effective_min                                                                THEN m.sub_identity
            WHEN COALESCE((SELECT n FROM _iv WHERE primary_identity = m.primary_identity), 0)
                 >= v_effective_min                                                                    THEN m.primary_identity
            ELSE 'General'
        END,
        assigned_bucket_primary_identity = m.primary_identity,
        is_new_bucket = false,
        bucket_assignment_reason = 'deterministic_rollup',
        bucket_assignment_confidence = 1.0
    WHERE m.bucketing_run_id = p_run_id;

    -- Update bucketing_runs counters so the UI reflects the final state.
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
        'min_volume',             v_effective_min
    );
END;
$$;

GRANT EXECUTE ON FUNCTION public.apply_rollup_bucket_assignments(UUID, INTEGER)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
