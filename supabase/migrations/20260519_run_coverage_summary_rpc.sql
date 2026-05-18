-- get_run_coverage_summary: single-shot diagnostic for a bucketing run.
-- Surfaces "what fraction of the 293k contacts are actually represented
-- at each stage" so the user can choose a sensible min_volume rollup
-- threshold instead of guessing against the visible-but-partial bucket
-- counts.
--
-- Returns JSON so the shape stays flexible without future ALTER TYPE
-- dances. statement_timeout=300s because the distinct-industry and
-- contacts-covered counts scan up to ~300k rows.

CREATE OR REPLACE FUNCTION public.get_run_coverage_summary(p_run_id UUID)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SET statement_timeout TO '300s'
AS $$
DECLARE
    v_list_names        TEXT[];
    v_total_contacts    BIGINT;
    v_distinct_industry BIGINT;
    v_blank_industry    BIGINT;
    v_map_total         BIGINT;
    v_map_llm           BIGINT;
    v_map_dq_passthru   BIGINT;
    v_map_dq_flagged    BIGINT;
    v_map_bucket_set    BIGINT;
    v_covered_contacts  BIGINT;
    v_assign_total      BIGINT;
    v_assign_by_source  JSONB;
BEGIN
    SELECT list_names INTO v_list_names
    FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RETURN jsonb_build_object('error', 'run not found');
    END IF;

    -- contacts pool
    SELECT
        COUNT(*),
        COUNT(DISTINCT NULLIF(TRIM(industry), '')),
        COUNT(*) FILTER (WHERE industry IS NULL OR TRIM(industry) = '')
    INTO v_total_contacts, v_distinct_industry, v_blank_industry
    FROM contacts
    WHERE lead_list_name = ANY(v_list_names);

    -- bucket_industry_map partition for this run
    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE source = 'llm_phase1a'),
        COUNT(*) FILTER (WHERE source = 'general_passthrough'),
        COUNT(*) FILTER (WHERE is_disqualified = TRUE),
        COUNT(*) FILTER (WHERE assigned_bucket_name IS NOT NULL)
    INTO v_map_total, v_map_llm, v_map_dq_passthru, v_map_dq_flagged, v_map_bucket_set
    FROM bucket_industry_map
    WHERE bucketing_run_id = p_run_id;

    -- contacts whose industry text appears in the run's map (the JOIN
    -- that powers get_assigned_bucket_counts). Same prefer-enrichment
    -- fallback rule the discovered-buckets view uses, so the number
    -- matches what the user sees there.
    WITH ci AS (
        SELECT c.contact_id,
               COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_str
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE c.lead_list_name = ANY(v_list_names)
    )
    SELECT COUNT(DISTINCT ci.contact_id) INTO v_covered_contacts
    FROM ci
    JOIN bucket_industry_map m
      ON m.industry_string = ci.industry_str
     AND m.bucketing_run_id = p_run_id;

    -- bucket_assignments per-source breakdown (Phase 1b output)
    SELECT
        COUNT(*),
        COALESCE(jsonb_object_agg(source, source_count), '{}'::jsonb)
    INTO v_assign_total, v_assign_by_source
    FROM (
        SELECT source, COUNT(*) AS source_count
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY source
    ) src;

    RETURN jsonb_build_object(
        'total_contacts',            v_total_contacts,
        'distinct_industry_strings', v_distinct_industry,
        'blank_industry_contacts',   v_blank_industry,
        'phase1a_map', jsonb_build_object(
            'total_rows',               v_map_total,
            'llm_phase1a_rows',         v_map_llm,
            'general_passthrough_rows', v_map_dq_passthru,
            'disqualified_rows',        v_map_dq_flagged,
            'bucket_assigned_rows',     v_map_bucket_set,
            'covered_contacts',         v_covered_contacts
        ),
        'phase1b_assignments', jsonb_build_object(
            'total_rows', v_assign_total,
            'by_source',  v_assign_by_source
        )
    );
END;
$$;

GRANT EXECUTE ON FUNCTION public.get_run_coverage_summary(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
