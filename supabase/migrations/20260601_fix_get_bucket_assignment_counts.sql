-- Fix get_bucket_assignment_counts — the outer aggregation counted ROWS of
-- the inner subquery instead of summing per-source contact counts. Result:
-- every bucket returned contact_count=1 (one row per (bucket, source) pair,
-- and every bucket has exactly one source 'deterministic_rollup'), so the
-- panel showed every bucket as having a single contact. The panel for
-- completed runs has been misreporting since this RPC was introduced.
--
-- Effect after this fix: panel for any 'completed' or 'assigning' run shows
-- real per-bucket contact counts that match bucket_contact_map / CSV export.

CREATE OR REPLACE FUNCTION public.get_bucket_assignment_counts(p_run_id UUID)
RETURNS TABLE (
    bucket_name    TEXT,
    contact_count  BIGINT,
    other_sources  JSONB
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT
        bucket_name,
        SUM(source_count)::BIGINT                AS contact_count,
        jsonb_object_agg(source, source_count)   AS other_sources
    FROM (
        SELECT bucket_name, source, COUNT(*) AS source_count
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY bucket_name, source
    ) sub
    GROUP BY bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_assignment_counts(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
