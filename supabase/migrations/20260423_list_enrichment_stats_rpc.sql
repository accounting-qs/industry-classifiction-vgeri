-- Single query that returns completed/failed/total counts for every
-- lead list. The per-list PostgREST `count=estimated` calls we were
-- making came back with 0 unpredictably (the planner returns 0 when
-- it can satisfy the query without scanning rows) — an RPC that does
-- one aggregate pass is both more accurate and cheaper.
--
-- The `SET statement_timeout TO '60s'` function attribute bumps the
-- timeout for just this function's execution. At ~600k contacts ×
-- ~400k enrichments the LEFT JOIN + GROUP BY runs past PostgREST's
-- 8s default. We use the function-attribute form (not `SET LOCAL`
-- in the body) because `SET LOCAL` is only legal inside VOLATILE
-- functions; keeping this STABLE preserves the planner's ability
-- to cache results within a single outer query.

CREATE OR REPLACE FUNCTION public.get_list_enrichment_stats()
RETURNS TABLE(
    lead_list_name TEXT,
    completed_count BIGINT,
    failed_count BIGINT,
    total_count BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT
        c.lead_list_name,
        COUNT(*) FILTER (WHERE e.status = 'completed')::BIGINT AS completed_count,
        COUNT(*) FILTER (WHERE e.status = 'failed')::BIGINT    AS failed_count,
        COUNT(*)::BIGINT                                        AS total_count
    FROM contacts c
    LEFT JOIN enrichments e ON e.contact_id = c.contact_id
    WHERE c.lead_list_name IS NOT NULL
    GROUP BY c.lead_list_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_stats()
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
