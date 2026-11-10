-- =====================================================================
-- get_bucketing_run_diagnostics: read sub_identity, not characteristic
--
-- WHY
-- 20260515_rename_characteristic_to_sub_identity renamed
-- bucket_contact_map.characteristic to sub_identity and redefined the
-- functions that read it, but missed this one. Since then every call has
-- failed with: column "characteristic" does not exist. Its only caller is
-- scripts/bucketing-diagnostics.ts.
--
-- WHAT
-- Same function, one change: the general_samples entries carry
-- 'sub_identity' (from the sub_identity column) where they carried
-- 'characteristic'. The key follows the column's new name, as the export
-- did in the rename migration. Everything else is unchanged, including
-- SET statement_timeout = '60s'.
--
-- It also sets search_path = public, extensions, the path it already
-- resolves through. CREATE OR REPLACE replaces a function's SET clauses,
-- so leaving it out would undo 20261110_pin_function_search_path if that
-- one is applied first. plpgsql is never inlined, so this costs nothing.
--
-- APPLYING
-- Replaces one pg_proc row, no table locks. Idempotent.
-- =====================================================================

CREATE OR REPLACE FUNCTION public.get_bucketing_run_diagnostics(p_run_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET statement_timeout TO '60s'
 SET search_path = public, extensions
AS $function$
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
        'sub_identity', sub_identity,
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
$function$;

-- Unchanged from 20261109_lock_down_anon_access: only service_role runs it.
REVOKE ALL ON FUNCTION public.get_bucketing_run_diagnostics(uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_bucketing_run_diagnostics(uuid) TO service_role;
