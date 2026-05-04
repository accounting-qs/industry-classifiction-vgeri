-- Single-shot bucketing-pipeline schema validator.
--
-- Replaces the per-column probe loop in services/bucketingSchemaCheck.ts
-- (one PostgREST round-trip per (table, column) pair × ~25 columns × 8
-- tables = ~200 round-trips, ~10s overhead at the top of every bucketing
-- run). This RPC returns the same answer in ONE round-trip by joining the
-- required-columns list against information_schema.columns.
--
-- Returned rows = MISSING (table, column) pairs. Empty result = schema OK.
--
-- Idempotent: CREATE OR REPLACE.

CREATE OR REPLACE FUNCTION public.bucketing_schema_gaps()
RETURNS TABLE (table_name TEXT, column_name TEXT)
LANGUAGE sql
STABLE
AS $$
    WITH required(t, c) AS (VALUES
        -- bucket_industry_map (Phase 1a output)
        ('bucket_industry_map', 'bucketing_run_id'),
        ('bucket_industry_map', 'industry_string'),
        ('bucket_industry_map', 'bucket_name'),
        ('bucket_industry_map', 'source'),
        ('bucket_industry_map', 'confidence'),
        ('bucket_industry_map', 'identity'),
        ('bucket_industry_map', 'characteristic'),
        ('bucket_industry_map', 'sector'),
        ('bucket_industry_map', 'functional_core'),
        ('bucket_industry_map', 'sector_core'),
        ('bucket_industry_map', 'is_new_identity'),
        ('bucket_industry_map', 'is_new_characteristic'),
        ('bucket_industry_map', 'is_new_sector'),
        ('bucket_industry_map', 'is_new_functional_core'),
        ('bucket_industry_map', 'is_new_sector_core'),
        ('bucket_industry_map', 'is_disqualified'),
        ('bucket_industry_map', 'is_generic'),
        ('bucket_industry_map', 'needs_qa'),
        ('bucket_industry_map', 'raw_industry'),
        ('bucket_industry_map', 'llm_reason'),
        ('bucket_industry_map', 'canonical_classification'),
        ('bucket_industry_map', 'identity_confidence'),
        ('bucket_industry_map', 'characteristic_confidence'),
        ('bucket_industry_map', 'sector_confidence'),
        ('bucket_industry_map', 'primary_identity'),
        ('bucket_industry_map', 'functional_specialization'),
        ('bucket_industry_map', 'sector_focus'),

        -- bucket_assignments (Phase 1b output)
        ('bucket_assignments', 'bucketing_run_id'),
        ('bucket_assignments', 'contact_id'),
        ('bucket_assignments', 'bucket_name'),
        ('bucket_assignments', 'source'),
        ('bucket_assignments', 'confidence'),
        ('bucket_assignments', 'bucket_leaf'),
        ('bucket_assignments', 'bucket_ancestor'),
        ('bucket_assignments', 'bucket_root'),
        ('bucket_assignments', 'primary_identity'),
        ('bucket_assignments', 'functional_specialization'),
        ('bucket_assignments', 'sector_focus'),
        ('bucket_assignments', 'functional_core'),
        ('bucket_assignments', 'sector_core'),
        ('bucket_assignments', 'pre_rollup_bucket_name'),
        ('bucket_assignments', 'rollup_level'),
        ('bucket_assignments', 'general_reason'),
        ('bucket_assignments', 'reasons'),
        ('bucket_assignments', 'is_generic'),
        ('bucket_assignments', 'is_disqualified'),
        ('bucket_assignments', 'canonical_classification'),
        ('bucket_assignments', 'bucket_reason'),
        ('bucket_assignments', 'identity_confidence'),
        ('bucket_assignments', 'characteristic_confidence'),
        ('bucket_assignments', 'sector_confidence'),
        ('bucket_assignments', 'assigned_at'),

        -- bucket_contact_map (Phase 1b pre-rollup decisions)
        ('bucket_contact_map', 'bucketing_run_id'),
        ('bucket_contact_map', 'contact_id'),
        ('bucket_contact_map', 'industry_string'),
        ('bucket_contact_map', 'primary_identity'),
        ('bucket_contact_map', 'functional_specialization'),
        ('bucket_contact_map', 'sector_focus'),
        ('bucket_contact_map', 'functional_core'),
        ('bucket_contact_map', 'sector_core'),
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
        ('bucket_contact_map', 'characteristic_confidence'),
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

        -- Editable taxonomy library
        ('taxonomy_identities', 'id'),
        ('taxonomy_identities', 'name'),
        ('taxonomy_identities', 'description'),
        ('taxonomy_identities', 'is_disqualified'),
        ('taxonomy_identities', 'created_by'),
        ('taxonomy_identities', 'archived'),
        ('taxonomy_characteristics', 'id'),
        ('taxonomy_characteristics', 'name'),
        ('taxonomy_characteristics', 'parent_identity'),
        ('taxonomy_characteristics', 'description'),
        ('taxonomy_characteristics', 'created_by'),
        ('taxonomy_characteristics', 'archived'),
        ('taxonomy_characteristics', 'functional_core'),
        ('taxonomy_sectors', 'id'),
        ('taxonomy_sectors', 'name'),
        ('taxonomy_sectors', 'synonyms'),
        ('taxonomy_sectors', 'description'),
        ('taxonomy_sectors', 'created_by'),
        ('taxonomy_sectors', 'archived'),
        ('taxonomy_sectors', 'sector_core'),

        -- Run logs
        ('bucketing_run_logs', 'id'),
        ('bucketing_run_logs', 'bucketing_run_id'),
        ('bucketing_run_logs', 'timestamp'),
        ('bucketing_run_logs', 'level'),
        ('bucketing_run_logs', 'message')
    )
    SELECT r.t::TEXT, r.c::TEXT
    FROM required r
    WHERE NOT EXISTS (
        SELECT 1
          FROM information_schema.columns ic
         WHERE ic.table_schema = 'public'
           AND ic.table_name   = r.t
           AND ic.column_name  = r.c
    )
    ORDER BY r.t, r.c;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_schema_gaps()
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
