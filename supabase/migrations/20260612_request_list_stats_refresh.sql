-- Anon-callable, abuse-proof wrapper around refresh_list_enrichment_stats.
--
-- Why: the Render deployment runs without SUPABASE_SERVICE_ROLE_KEY, so
-- server.ts/jobProcessor silently fall back to the anon key
-- (server.ts:55). refresh_list_enrichment_stats is service_role-only —
-- every kick from the deployed server 401'd and the stats cache froze
-- at its seeded values. Granting the raw refresh to anon would reopen
-- the stampede this whole design exists to prevent (the anon key ships
-- in the browser bundle), so instead expose a wrapper that anyone may
-- CALL but that only does work the system would do anyway:
--
--   * single-flight: pg_try_advisory_xact_lock — concurrent callers
--     return empty instead of stacking aggregate scans;
--   * freshness gate: targeted calls are trimmed to lists whose
--     snapshot is missing or older than 5s; full refresh runs only if
--     some list is missing or older than 30s. A hostile loop degrades
--     into no-ops; the worst sustained cost is one full pass per 30s —
--     the same load the app itself produced per-pageload before the
--     cache existed.
--
-- The underlying refresh_list_enrichment_stats stays service_role-only.
-- Setting SUPABASE_SERVICE_ROLE_KEY on Render remains the right
-- hygiene fix, but app correctness no longer depends on it.

CREATE OR REPLACE FUNCTION public.request_list_stats_refresh(p_lists TEXT[] DEFAULT NULL)
RETURNS TABLE(
    lead_list_name  TEXT,
    completed_count BIGINT,
    failed_count    BIGINT,
    total_count     BIGINT,
    refreshed_at    TIMESTAMPTZ
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
SET statement_timeout TO '120s'
AS $$
DECLARE
    v_targets TEXT[];
BEGIN
    -- Single-flight across every caller (server, worker, strangers).
    -- xact-scoped: releases automatically when this call's transaction
    -- ends, so a crashed refresh can't wedge the lock.
    IF NOT pg_try_advisory_xact_lock(hashtext('list_stats_refresh')) THEN
        RETURN;
    END IF;

    IF p_lists IS NULL THEN
        -- Full refresh only when something actually needs it: a list
        -- with no snapshot at all, or one older than 30s.
        IF NOT EXISTS (
            SELECT 1
            FROM import_lists il
            LEFT JOIN list_enrichment_stats_cache lc ON lc.lead_list_name = il.name
            WHERE lc.lead_list_name IS NULL
               OR lc.refreshed_at < now() - interval '30 seconds'
        ) THEN
            RETURN;
        END IF;

        RETURN QUERY SELECT * FROM public.refresh_list_enrichment_stats(NULL);
    ELSE
        -- Trim the request down to real lists whose snapshot is missing
        -- or older than 5s — repeat calls inside the window are no-ops.
        v_targets := ARRAY(
            SELECT il.name
            FROM unnest(p_lists) AS req(name)
            JOIN import_lists il ON il.name = req.name
            LEFT JOIN list_enrichment_stats_cache lc ON lc.lead_list_name = il.name
            WHERE lc.lead_list_name IS NULL
               OR lc.refreshed_at < now() - interval '5 seconds'
        );
        IF v_targets IS NULL OR cardinality(v_targets) = 0 THEN
            RETURN;
        END IF;

        RETURN QUERY SELECT * FROM public.refresh_list_enrichment_stats(v_targets);
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION public.request_list_stats_refresh(TEXT[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.request_list_stats_refresh(TEXT[]) TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
