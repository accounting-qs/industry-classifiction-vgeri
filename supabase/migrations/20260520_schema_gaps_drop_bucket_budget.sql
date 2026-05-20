-- Update bucketing_schema_gaps() to reflect the new bucketing_runs shape:
--   - REMOVE bucket_budget (dropped in 20260520_drop_bucket_budget).
--   - ADD identity_min_volume (added in 20260520_rollup_two_thresholds_and_null_fix).
--
-- The TS-side checkBucketingSchema (services/bucketingSchemaCheck.ts) calls
-- this RPC as its fast path; the TS required-columns list is only used as a
-- fallback when the RPC doesn't exist. So the TS edit alone was insufficient
-- — every new run fired off, hit the RPC, and got "Missing pieces:
-- bucketing_runs.bucket_budget" because the DB function was still asking
-- for the column we just dropped.

CREATE OR REPLACE FUNCTION public.bucketing_schema_gaps()
RETURNS TABLE (table_name TEXT, column_name TEXT)
LANGUAGE sql
STABLE
AS $$
    WITH required(t, c) AS (VALUES
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

        ('bucketing_runs', 'id'),
        ('bucketing_runs', 'name'),
        ('bucketing_runs', 'list_names'),
        ('bucketing_runs', 'min_volume'),
        ('bucketing_runs', 'identity_min_volume'),
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

NOTIFY pgrst, 'reload schema';
