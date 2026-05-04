-- =====================================================================
-- v5 — taxonomy schema simplification
--
-- Rationale: Phase 1a's LLM output was being persisted under TWO sets of
-- column names (canonical: identity / characteristic / sector; legacy
-- aliases: primary_identity / functional_specialization / sector_focus)
-- with a mirror trigger keeping them in sync. We also had functional_core
-- and sector_core "intermediate rollup" columns that only existed to add
-- two extra fallback levels to computeContactRollup.
--
-- This migration:
--   1. Updates RPCs to read the canonical columns BEFORE we drop the
--      legacy aliases (so nothing breaks mid-deploy).
--   2. Drops the bucket_industry_map mirror trigger (no longer needed).
--   3. Drops legacy alias columns: functional_specialization, sector_focus.
--   4. Drops intermediate rollup columns: functional_core, sector_core
--      (and their is_new_* flags).
--   5. Drops functional_core from taxonomy_characteristics, sector_core
--      from taxonomy_sectors (those library tables no longer model the
--      intermediate layer).
--
-- Run this AFTER deploying the v5 application code — the new code stops
-- writing the dropped columns and reads only the canonical ones, so the
-- two systems can coexist briefly during the deploy window.
-- =====================================================================

-- ── 1) Update RPCs to use canonical column names ─────────────────────

-- get_bucket_sector_mix: switch sector_focus → sector.
CREATE OR REPLACE FUNCTION public.get_bucket_sector_mix(p_run_id UUID)
RETURNS TABLE (
    bucket_name TEXT,
    sectors JSONB
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT bucket_name,
           jsonb_agg(
               jsonb_build_object('sector', sector, 'count', n)
               ORDER BY n DESC
           ) AS sectors
    FROM (
        SELECT bucket_name, sector, COUNT(*)::BIGINT AS n
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
          AND sector IS NOT NULL
          AND sector <> ''
        GROUP BY bucket_name, sector
    ) sub
    GROUP BY bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_sector_mix(UUID)
    TO anon, authenticated, service_role;

-- get_bucketing_run_diagnostics: switch functional_specialization → characteristic.
CREATE OR REPLACE FUNCTION public.get_bucketing_run_diagnostics(p_run_id UUID)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SET statement_timeout TO '60s'
AS $$
DECLARE
    v_list_names TEXT[];
    v_selected BIGINT;
    v_assigned BIGINT;
    v_contact_map BIGINT;
    v_unclassifiable BIGINT;
    v_usable BIGINT;
    v_general BIGINT;
    v_pre JSONB;
    v_post JSONB;
    v_breakdown JSONB;
    v_samples JSONB;
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'bucketing run % not found', p_run_id;
    END IF;

    SELECT COUNT(*)::BIGINT INTO v_selected
    FROM contacts
    WHERE lead_list_name = ANY(v_list_names);

    SELECT COUNT(*)::BIGINT INTO v_assigned
    FROM bucket_assignments
    WHERE bucketing_run_id = p_run_id;

    SELECT COUNT(*)::BIGINT INTO v_contact_map
    FROM bucket_contact_map
    WHERE bucketing_run_id = p_run_id;

    SELECT COUNT(*)::BIGINT INTO v_unclassifiable
    FROM bucket_assignments
    WHERE bucketing_run_id = p_run_id
      AND source = 'unclassifiable';

    v_usable := v_assigned - COALESCE(v_unclassifiable, 0);

    SELECT COUNT(*)::BIGINT INTO v_general
    FROM bucket_assignments
    WHERE bucketing_run_id = p_run_id
      AND bucket_name = 'General';

    SELECT COALESCE(jsonb_object_agg(pre_rollup_bucket_name, n), '{}'::JSONB)
      INTO v_pre
    FROM (
        SELECT pre_rollup_bucket_name, COUNT(*)::BIGINT AS n
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY pre_rollup_bucket_name
    ) s;

    SELECT COALESCE(jsonb_object_agg(bucket_name, n), '{}'::JSONB)
      INTO v_post
    FROM (
        SELECT bucket_name, COUNT(*)::BIGINT AS n
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY bucket_name
    ) s;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'general_reason', general_reason,
        'source', source,
        'contact_count', contact_count
    )), '[]'::JSONB)
      INTO v_breakdown
    FROM get_bucket_general_breakdown(p_run_id);

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'contact_id', contact_id,
        'industry_string', industry_string,
        'primary_identity', primary_identity,
        'characteristic', characteristic,
        'pre_rollup_bucket_name', pre_rollup_bucket_name,
        'source', source,
        'confidence', confidence,
        'general_reason', general_reason,
        'reasons', reasons
    )), '[]'::JSONB)
      INTO v_samples
    FROM (
        SELECT *
        FROM bucket_contact_map
        WHERE bucketing_run_id = p_run_id
          AND bucket_name = 'General'
        ORDER BY assigned_at DESC
        LIMIT 30
    ) s;

    RETURN jsonb_build_object(
        'selected_contacts', v_selected,
        'assigned_contacts', v_assigned,
        'contact_map_rows', v_contact_map,
        'usable_contacts', v_usable,
        'unclassifiable_contacts', COALESCE(v_unclassifiable, 0),
        'general_contacts', v_general,
        'general_pct', CASE WHEN v_assigned > 0 THEN ROUND((v_general::NUMERIC / v_assigned::NUMERIC) * 100, 2) ELSE 0 END,
        'pre_rollup_counts', v_pre,
        'post_rollup_counts', v_post,
        'general_breakdown', v_breakdown,
        'general_samples', v_samples
    );
END;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucketing_run_diagnostics(UUID)
    TO anon, authenticated, service_role;

-- ── 2) Drop the mirror trigger ───────────────────────────────────────
DROP TRIGGER  IF EXISTS bucket_industry_map_mirror_tags ON bucket_industry_map;
DROP FUNCTION IF EXISTS public.bucket_industry_map_mirror_tags();

-- ── 3) Drop legacy alias columns ─────────────────────────────────────
ALTER TABLE bucket_industry_map  DROP COLUMN IF EXISTS functional_specialization;
ALTER TABLE bucket_industry_map  DROP COLUMN IF EXISTS sector_focus;
ALTER TABLE bucket_contact_map   DROP COLUMN IF EXISTS functional_specialization;
ALTER TABLE bucket_contact_map   DROP COLUMN IF EXISTS sector_focus;
ALTER TABLE bucket_assignments   DROP COLUMN IF EXISTS functional_specialization;
ALTER TABLE bucket_assignments   DROP COLUMN IF EXISTS sector_focus;
ALTER TABLE bucket_library       DROP COLUMN IF EXISTS functional_specialization;

-- ── 4) Drop intermediate rollup columns (functional_core / sector_core)
ALTER TABLE bucket_industry_map  DROP COLUMN IF EXISTS functional_core;
ALTER TABLE bucket_industry_map  DROP COLUMN IF EXISTS sector_core;
ALTER TABLE bucket_industry_map  DROP COLUMN IF EXISTS is_new_functional_core;
ALTER TABLE bucket_industry_map  DROP COLUMN IF EXISTS is_new_sector_core;
ALTER TABLE bucket_contact_map   DROP COLUMN IF EXISTS functional_core;
ALTER TABLE bucket_contact_map   DROP COLUMN IF EXISTS sector_core;
ALTER TABLE bucket_assignments   DROP COLUMN IF EXISTS functional_core;
ALTER TABLE bucket_assignments   DROP COLUMN IF EXISTS sector_core;

-- ── 5) Drop intermediate columns from the taxonomy library tables ────
ALTER TABLE taxonomy_characteristics DROP COLUMN IF EXISTS functional_core;
ALTER TABLE taxonomy_sectors         DROP COLUMN IF EXISTS sector_core;

-- ── 6) PostgREST schema reload ───────────────────────────────────────
NOTIFY pgrst, 'reload schema';
