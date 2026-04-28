-- v3.0: Fix get_bucket_assignment_counts double-counting bug.
--
-- Bug: outer SELECT used COUNT(*) over `sub`, which is grouped by
-- (bucket_name, source). So the returned `contact_count` was the
-- number of distinct sources for the bucket, not the contact count.
-- Symptom: a bucket with 84,640 contacts split across 2 sources
-- ('catchall' + 'llm_phase1b') reported contact_count=2 in the UI.
--
-- Fix: SUM(source_count) instead. The inner sub already aggregates
-- contacts per (bucket, source), so summing those source_counts
-- gives the true per-bucket total.

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

-- Sector_focus distribution per bucket. Server previously did this by
-- pulling every (bucket_name, sector_focus) row to Node and tallying;
-- on a 100k-row run that's both slow and clipped at PostgREST's
-- 1000-row default. Aggregate in SQL instead.
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
        GROUP BY bucket_name, sector_focus
    ) sub
    GROUP BY bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_sector_mix(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
