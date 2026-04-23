-- Single query that returns completed/failed/total counts for every
-- lead list. The per-list PostgREST `count=estimated` calls we were
-- making came back with 0 unpredictably (the planner returns 0 when
-- it can satisfy the query without scanning rows) — an RPC that does
-- one aggregate pass is both more accurate and cheaper.
--
-- total_count = every contact on the list regardless of enrichment
-- state, so pending = total - completed - failed on the client.

CREATE OR REPLACE FUNCTION public.get_list_enrichment_stats()
RETURNS TABLE(
    lead_list_name TEXT,
    completed_count BIGINT,
    failed_count BIGINT,
    total_count BIGINT
) AS $$
    SELECT
        c.lead_list_name,
        COUNT(*) FILTER (WHERE e.status = 'completed') AS completed_count,
        COUNT(*) FILTER (WHERE e.status = 'failed')    AS failed_count,
        COUNT(*)                                        AS total_count
    FROM contacts c
    LEFT JOIN enrichments e ON e.contact_id = c.contact_id
    WHERE c.lead_list_name IS NOT NULL
    GROUP BY c.lead_list_name;
$$ LANGUAGE sql STABLE;

-- Explicit GRANTs so the server's service_role key (and the anon key
-- used by the browser fallback) can invoke the function. Default
-- `CREATE FUNCTION` grants to PUBLIC, but hardened projects sometimes
-- revoke that.
GRANT EXECUTE ON FUNCTION public.get_list_enrichment_stats()
    TO anon, authenticated, service_role;

-- Force PostgREST to refresh its function cache immediately. Supabase's
-- event-trigger auto-reload usually handles this, but it occasionally
-- misses — and when it does the RPC returns `PGRST202 / not found in
-- schema cache` until the next boot.
NOTIFY pgrst, 'reload schema';
