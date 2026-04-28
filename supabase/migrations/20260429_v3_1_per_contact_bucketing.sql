-- v3.1: Per-contact bucketing map + diagnostics.
--
-- Phase 1b now routes every selected contact, not just each distinct
-- industry string. The contact map stores the pre-rollup routing decision,
-- final campaign bucket, rollup level, and General reason for audit.

ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS quality_warnings JSONB DEFAULT '[]'::JSONB,
    ADD COLUMN IF NOT EXISTS coverage_summary JSONB;

CREATE TABLE IF NOT EXISTS bucket_contact_map (
    bucketing_run_id UUID NOT NULL REFERENCES bucketing_runs(id) ON DELETE CASCADE,
    contact_id TEXT NOT NULL,
    industry_string TEXT,
    primary_identity TEXT,
    functional_specialization TEXT,
    sector_focus TEXT,
    pre_rollup_bucket_name TEXT NOT NULL,
    bucket_name TEXT NOT NULL,
    rollup_level TEXT NOT NULL,
    source TEXT NOT NULL,
    confidence NUMERIC(4, 2),
    leaf_score NUMERIC(4, 2),
    ancestor_score NUMERIC(4, 2),
    root_score NUMERIC(4, 2),
    is_generic BOOLEAN DEFAULT false,
    is_disqualified BOOLEAN DEFAULT false,
    general_reason TEXT,
    reasons JSONB,
    assigned_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (bucketing_run_id, contact_id)
);

CREATE INDEX IF NOT EXISTS bucket_contact_map_run_bucket_idx
    ON bucket_contact_map (bucketing_run_id, bucket_name);

CREATE INDEX IF NOT EXISTS bucket_contact_map_run_pre_rollup_idx
    ON bucket_contact_map (bucketing_run_id, pre_rollup_bucket_name);

CREATE INDEX IF NOT EXISTS bucket_contact_map_run_general_reason_idx
    ON bucket_contact_map (bucketing_run_id, general_reason);

ALTER TABLE bucket_assignments
    ADD COLUMN IF NOT EXISTS primary_identity TEXT,
    ADD COLUMN IF NOT EXISTS functional_specialization TEXT,
    ADD COLUMN IF NOT EXISTS pre_rollup_bucket_name TEXT,
    ADD COLUMN IF NOT EXISTS rollup_level TEXT,
    ADD COLUMN IF NOT EXISTS general_reason TEXT,
    ADD COLUMN IF NOT EXISTS reasons JSONB;

-- Correct per-bucket final assignment counts. The original RPC counted
-- distinct source rows per bucket instead of summing contacts per source.
CREATE OR REPLACE FUNCTION public.get_bucket_assignment_counts(p_run_id UUID)
RETURNS TABLE (
    bucket_name TEXT,
    contact_count BIGINT,
    other_sources JSONB
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT sub.bucket_name,
           SUM(sub.source_count)::BIGINT AS contact_count,
           jsonb_object_agg(sub.source, sub.source_count) AS other_sources
    FROM (
        SELECT bucket_name, source, COUNT(*)::BIGINT AS source_count
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY bucket_name, source
    ) sub
    GROUP BY sub.bucket_name
    ORDER BY contact_count DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_assignment_counts(UUID)
    TO anon, authenticated, service_role;

-- SQL-aggregated sector mix to avoid PostgREST row caps.
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
               jsonb_build_object('sector', sector_focus, 'count', n)
               ORDER BY n DESC
           ) AS sectors
    FROM (
        SELECT bucket_name, sector_focus, COUNT(*)::BIGINT AS n
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
          AND sector_focus IS NOT NULL
          AND sector_focus <> ''
        GROUP BY bucket_name, sector_focus
    ) sub
    GROUP BY bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_sector_mix(UUID)
    TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_bucket_general_breakdown(p_run_id UUID)
RETURNS TABLE (
    general_reason TEXT,
    source TEXT,
    contact_count BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT COALESCE(general_reason, 'unspecified') AS general_reason,
           source,
           COUNT(*)::BIGINT AS contact_count
    FROM bucket_assignments
    WHERE bucketing_run_id = p_run_id
      AND bucket_name = 'General'
    GROUP BY COALESCE(general_reason, 'unspecified'), source
    ORDER BY contact_count DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_general_breakdown(UUID)
    TO anon, authenticated, service_role;

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
    FROM bucket_contact_map
    WHERE bucketing_run_id = p_run_id
      AND (
        source = 'unclassifiable'
        OR general_reason IN ('failed_enrichment', 'missing_industry', 'scrape_site_unknown')
      );

    v_usable := GREATEST(v_selected - COALESCE(v_unclassifiable, 0), 0);

    SELECT COUNT(*)::BIGINT INTO v_general
    FROM bucket_assignments
    WHERE bucketing_run_id = p_run_id
      AND bucket_name = 'General';

    SELECT COALESCE(jsonb_object_agg(pre_rollup_bucket_name, n), '{}'::JSONB)
      INTO v_pre
    FROM (
        SELECT pre_rollup_bucket_name, COUNT(*)::BIGINT AS n
        FROM bucket_contact_map
        WHERE bucketing_run_id = p_run_id
        GROUP BY pre_rollup_bucket_name
        ORDER BY n DESC
    ) s;

    SELECT COALESCE(jsonb_object_agg(bucket_name, n), '{}'::JSONB)
      INTO v_post
    FROM (
        SELECT bucket_name, COUNT(*)::BIGINT AS n
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY bucket_name
        ORDER BY n DESC
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
        'functional_specialization', functional_specialization,
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

ALTER TABLE bucket_contact_map DISABLE ROW LEVEL SECURITY;

NOTIFY pgrst, 'reload schema';
