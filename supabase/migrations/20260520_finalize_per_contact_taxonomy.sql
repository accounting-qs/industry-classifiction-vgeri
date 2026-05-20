-- Finalize Taxonomy step: explode per-industry tags to per-contact rows.
--
-- The user-defined process is that Finalize Taxonomy (Phase 1a's terminal
-- step, before Phase 1b's rollup) must populate `(primary_identity,
-- sub_identity, sector)` for every contact in the run's lists — so the
-- user can SEE per-contact taxonomy in the DB / CSV export before clicking
-- Assign Buckets. Previously the explosion happened only inside
-- apply_rollup_bucket_assignments (Phase 1b), coupling two distinct
-- concerns and hiding the gap whenever Phase 1b hadn't been run.
--
-- This RPC does only the explosion. It:
--   1. DELETEs any prior per-contact rows for the run (Finalize is the
--      terminal Phase 1a step — anything older is stale).
--   2. JOINs contacts × enrichments × bucket_industry_map using the
--      same COALESCE(classification, industry) precedence the rollup
--      uses, so the per-contact taxonomy matches what Phase 1b sees.
--   3. INSERTs into bucket_assignments + bucket_contact_map with
--      bucket_name = 'Pending' (rollup hasn't run yet — that becomes a
--      real bucket name when apply_rollup_bucket_assignments executes).
--      Source is 'phase1a_finalize' to distinguish from Phase 1b's
--      'deterministic_rollup' writes.
--
-- Phase 1b's apply_rollup_bucket_assignments still DELETEs + INSERTs,
-- so this preview gets cleanly overwritten when the user runs Assign
-- Buckets. The trade-off is one extra round-trip of writes per Finalize
-- click; in exchange the user always has per-contact taxonomy visible.

CREATE OR REPLACE FUNCTION public.finalize_per_contact_taxonomy(
    p_run_id UUID
)
RETURNS JSONB
LANGUAGE plpgsql
VOLATILE
SET statement_timeout TO '600s'
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

    DELETE FROM bucket_contact_map  WHERE bucketing_run_id = p_run_id;
    DELETE FROM bucket_assignments  WHERE bucketing_run_id = p_run_id;

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
    LEFT JOIN bucket_industry_map m
        ON m.bucketing_run_id = p_run_id
       AND m.industry_string  = COALESCE(NULLIF(TRIM(e.classification), ''), c.industry)
    WHERE c.lead_list_name = ANY(v_list_names);

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
