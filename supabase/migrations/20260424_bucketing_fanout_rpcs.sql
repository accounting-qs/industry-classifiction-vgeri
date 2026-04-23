-- Move Phase 2's contacts-table fan-out into SQL. The original client-side
-- implementation paged through `contacts WHERE lead_list_name IN (...)` via
-- PostgREST, which (a) makes 20+ round trips for a 100k list, and (b) hits
-- the "in.(\"value,with,commas\") + ORDER BY → full sort" planner bug that
-- times out at 8s on large lists. A single INSERT … SELECT with its own
-- statement_timeout is both faster and immune to the quoting issue.
--
-- Two RPCs:
--   bucketing_deterministic_fanout — writes a bucket_assignments row for
--     every contact in the run's lists whose industry is in the map.
--     Idempotent (ON CONFLICT DO NOTHING).
--   bucketing_catchall_other — writes an "Other" row for every contact in
--     the lists that doesn't already have an assignment. Runs last so it
--     only fills gaps.

CREATE OR REPLACE FUNCTION public.bucketing_deterministic_fanout(p_run_id UUID)
RETURNS BIGINT
LANGUAGE plpgsql
SET statement_timeout TO '180s'
AS $$
DECLARE
    inserted BIGINT;
    v_list_names TEXT[];
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'bucketing run % not found', p_run_id;
    END IF;

    INSERT INTO bucket_assignments (bucketing_run_id, contact_id, bucket_name, source, confidence)
    SELECT
        p_run_id,
        c.contact_id,
        m.bucket_name,
        CASE
            WHEN m.source IN ('llm_phase1', 'manual') THEN 'deterministic'
            ELSE m.source
        END,
        COALESCE(m.confidence, 1.0)
    FROM contacts c
    JOIN bucket_industry_map m
      ON m.industry_string = c.industry
     AND m.bucketing_run_id = p_run_id
    WHERE c.lead_list_name = ANY(v_list_names)
    ON CONFLICT (bucketing_run_id, contact_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    RETURN inserted;
END;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_deterministic_fanout(UUID)
    TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.bucketing_catchall_other(p_run_id UUID)
RETURNS BIGINT
LANGUAGE plpgsql
SET statement_timeout TO '180s'
AS $$
DECLARE
    inserted BIGINT;
    v_list_names TEXT[];
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'bucketing run % not found', p_run_id;
    END IF;

    INSERT INTO bucket_assignments (bucketing_run_id, contact_id, bucket_name, source, confidence)
    SELECT p_run_id, c.contact_id, 'Other', 'other', 0
    FROM contacts c
    WHERE c.lead_list_name = ANY(v_list_names)
    ON CONFLICT (bucketing_run_id, contact_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    RETURN inserted;
END;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_catchall_other(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
