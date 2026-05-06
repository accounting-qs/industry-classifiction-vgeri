-- =====================================================================
-- Bucket Assignment step — separates taxonomy tagging from final bucket
--
-- Until now bucket_industry_map.bucket_name was SYNTHESIZED from the
-- per-row taxonomy tags via the volume rollup in Phase 1b. The user's
-- mental model is cleaner: tag every contact with the full taxonomy
-- (no min_volume), then ASSIGN each contact to a bucket from the
-- curated bucket_library (or propose a new one if nothing fits), then
-- gate FINAL bucket sizes with min_volume.
--
-- New columns capture the bucket-assignment step output:
--
--   assigned_bucket_id           FK into bucket_library (null when the
--                                LLM proposed a brand-new bucket)
--   assigned_bucket_name         denormalized name for fast display
--                                + fallback when the FK row gets renamed
--   assigned_bucket_primary_identity  required for new proposals so the
--                                Discovered Buckets panel can group them
--   is_new_bucket                proposal flag — Accept moves it into
--                                bucket_library (mirrors the existing
--                                is_new_identity / is_new_characteristic
--                                / is_new_sector workflow on taxonomy)
--   bucket_assignment_reason     LLM's one-sentence rationale
--   bucket_assignment_confidence 0..1 confidence on the assignment
--
-- Idempotent (ADD COLUMN IF NOT EXISTS).
-- =====================================================================

ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS assigned_bucket_id UUID;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS assigned_bucket_name TEXT;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS assigned_bucket_primary_identity TEXT;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS is_new_bucket BOOLEAN DEFAULT false;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS bucket_assignment_reason TEXT;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS bucket_assignment_confidence NUMERIC(3,2);

CREATE INDEX IF NOT EXISTS bucket_industry_map_assigned_bucket_idx
    ON bucket_industry_map (bucketing_run_id, assigned_bucket_name)
    WHERE assigned_bucket_name IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────
-- Update get_bucket_map_counts to expose per-assigned-bucket counts.
-- The Review screen's new Discovered Buckets panel needs counts grouped
-- by assigned_bucket_name (not the synthesized bucket_name). Adding a
-- companion RPC instead of overloading the existing one keeps the JOIN
-- semantics + the legacy Discovered Characteristics panel intact.
-- ─────────────────────────────────────────────────────────────────────

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
    -- Same JOIN-key precedence as Phase 1b: enrichments.classification
    -- first, contacts.industry as fallback. Mirrors the fix in 20260512.
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

-- ─────────────────────────────────────────────────────────────────────
-- Extend the schema-gaps RPC with the new columns so pre-flight
-- catches half-applied migrations the same way it does for v5.
-- ─────────────────────────────────────────────────────────────────────

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
        ('bucket_industry_map', 'characteristic'),
        ('bucket_industry_map', 'sector'),
        ('bucket_industry_map', 'is_new_identity'),
        ('bucket_industry_map', 'is_new_characteristic'),
        ('bucket_industry_map', 'is_new_sector'),
        ('bucket_industry_map', 'is_disqualified'),
        ('bucket_industry_map', 'is_generic'),
        ('bucket_industry_map', 'needs_qa'),
        ('bucket_industry_map', 'raw_industry'),
        ('bucket_industry_map', 'llm_reason'),
        ('bucket_industry_map', 'canonical_classification'),
        ('bucket_industry_map', 'identity_confidence'),
        ('bucket_industry_map', 'characteristic_confidence'),
        ('bucket_industry_map', 'sector_confidence'),
        -- New v6 bucket-assignment columns
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
        ('bucket_assignments', 'characteristic'),
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
        ('bucket_assignments', 'characteristic_confidence'),
        ('bucket_assignments', 'sector_confidence'),
        ('bucket_assignments', 'assigned_at'),

        ('bucket_contact_map', 'bucketing_run_id'),
        ('bucket_contact_map', 'contact_id'),
        ('bucket_contact_map', 'industry_string'),
        ('bucket_contact_map', 'primary_identity'),
        ('bucket_contact_map', 'characteristic'),
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
        ('bucket_contact_map', 'characteristic_confidence'),
        ('bucket_contact_map', 'sector_confidence'),
        ('bucket_contact_map', 'assigned_at'),

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
