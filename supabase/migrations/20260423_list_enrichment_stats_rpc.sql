-- Single query that returns completed/failed/total counts for every
-- lead list. The per-list PostgREST `count=estimated` calls we were
-- making came back with 0 unpredictably (the planner returns 0 when
-- it can satisfy the query without scanning rows) — an RPC that does
-- one aggregate pass is both more accurate and cheaper.
--
-- Uses plpgsql + `SET LOCAL statement_timeout = '60s'` because at
-- ~600k contacts × ~400k enrichments the LEFT JOIN + GROUP BY runs
-- past PostgREST's default 8s timeout. Same pattern as
-- resolve_enrichment_targets. Without this the function returns
-- 57014 ("canceling statement due to statement timeout") and the
-- server falls back to its estimated-count path.

CREATE OR REPLACE FUNCTION public.get_list_enrichment_stats()
RETURNS TABLE(
    lead_list_name TEXT,
    completed_count BIGINT,
    failed_count BIGINT,
    total_count BIGINT
) AS $$
BEGIN
    SET LOCAL statement_timeout = '60s';
    RETURN QUERY
    SELECT
        c.lead_list_name,
        COUNT(*) FILTER (WHERE e.status = 'completed')::BIGINT AS completed_count,
        COUNT(*) FILTER (WHERE e.status = 'failed')::BIGINT    AS failed_count,
        COUNT(*)::BIGINT                                        AS total_count
    FROM contacts c
    LEFT JOIN enrichments e ON e.contact_id = c.contact_id
    WHERE c.lead_list_name IS NOT NULL
    GROUP BY c.lead_list_name;
END;
$$ LANGUAGE plpgsql STABLE;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_stats()
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
