-- =====================================================================
-- Stop two read paths from re-deriving contact→industry from raw tables
--
-- Both of these recomputed, on every call, something the per-contact
-- tables already store:
--
--   get_dashboard_stats         ~90 s   (landing page, manual refresh)
--   get_assigned_bucket_counts  ~5.3 s  (Discovered Buckets panel)
--
-- Measured warm. Cold they were far worse, and because each one scans
-- `contacts` (6.0M rows) and `enrichments` (6.0M rows) it evicted the
-- buffer cache for everything else — which is why unrelated bucketing
-- reads swung between 0.6 s and 17 s depending on what ran before them.
--
-- Same principle the import page already follows with
-- list_enrichment_stats_cache: never aggregate contacts/enrichments on a
-- read path.
--
-- Idempotent: safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Discovered Buckets: read the per-contact table, not raw contacts.
--
-- The old body re-derived each contact's industry string by joining
-- contacts -> enrichments -> bucket_industry_map. But bucket_contact_map
-- already stores industry_string per contact, keyed by run, and
-- bucket_industry_map's primary key is exactly
-- (bucketing_run_id, industry_string) — so the join becomes a PK lookup
-- over ~74k-124k rows instead of a scan of two 6M-row tables.
--
-- Verified identical on runs 7258e616 (54 buckets / 124,075 contacts) and
-- 6e7b8f69 (45 / 74,428): same row count, same totals, zero rows
-- differing in either direction. ~250-370 ms warm versus 1.9-5.9 s.
--
-- The fallback matters: bucket_contact_map is only populated once
-- Finalize (or Phase 1b) has run. Before that the old derivation is the
-- only source, and returning nothing would silently empty the panel — so
-- when the run has no per-contact rows yet, behave exactly as before.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_assigned_bucket_counts(p_run_id UUID)
RETURNS TABLE(
    assigned_bucket_name             TEXT,
    assigned_bucket_primary_identity TEXT,
    is_new_bucket                    BOOLEAN,
    contact_count                    BIGINT
)
LANGUAGE sql
STABLE
AS $$
    WITH has_per_contact AS (
        SELECT EXISTS (
            SELECT 1 FROM bucket_contact_map
            WHERE bucketing_run_id = p_run_id
            LIMIT 1
        ) AS ok
    ),
    -- Fast path: the per-contact rows exist, so just count them.
    fast AS (
        SELECT
            m.assigned_bucket_name,
            m.assigned_bucket_primary_identity,
            m.is_new_bucket,
            COUNT(*)::BIGINT AS contact_count
        FROM bucket_contact_map cm
        JOIN bucket_industry_map m
          ON m.bucketing_run_id = cm.bucketing_run_id
         AND m.industry_string  = cm.industry_string
        WHERE cm.bucketing_run_id = p_run_id
          AND m.assigned_bucket_name IS NOT NULL
          AND (SELECT ok FROM has_per_contact)
        GROUP BY 1, 2, 3
    ),
    -- Slow path, unchanged: pre-Finalize there is nothing else to read.
    slow AS (
        SELECT
            m.assigned_bucket_name,
            m.assigned_bucket_primary_identity,
            m.is_new_bucket,
            COUNT(*)::BIGINT AS contact_count
        FROM (
            SELECT
                c.contact_id,
                COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_str
            FROM contacts c
            LEFT JOIN enrichments e ON e.contact_id = c.contact_id
            WHERE NOT (SELECT ok FROM has_per_contact)
              AND c.lead_list_name = ANY(
                  SELECT UNNEST(list_names) FROM bucketing_runs WHERE id = p_run_id
              )
        ) ci
        JOIN bucket_industry_map m ON m.industry_string = ci.industry_str
        WHERE m.bucketing_run_id = p_run_id
          AND m.assigned_bucket_name IS NOT NULL
        GROUP BY 1, 2, 3
    )
    SELECT * FROM fast
    UNION ALL
    SELECT * FROM slow;
$$;

GRANT EXECUTE ON FUNCTION public.get_assigned_bucket_counts(UUID)
    TO anon, authenticated, service_role;

-- ---------------------------------------------------------------------
-- 2. Dashboard: serve the two distinct-counts from a cache.
--
-- get_dashboard_stats did
--     (SELECT COUNT(DISTINCT contact_id) FROM bucket_contact_map)
--     (SELECT COUNT(DISTINCT contact_id) FROM bucket_assignments)
-- across every run ever — 4.8M rows / 3.3GB and 4.8M rows / 2.9GB. There
-- is no shortcut available: 4,828,587 rows hold 4,798,901 distinct
-- contacts, so a loose index scan saves nothing, and an index-only scan
-- is not usable while the visibility map is stale. Measured 19-26 s per
-- count warm, ~90 s for the pair cold.
--
-- Phase 0 of this same function already reads a cache
-- (list_enrichment_stats_cache) for exactly this reason. This extends the
-- pattern to Phase 1a/1b rather than inventing a second one.
--
-- Single row, enforced by the CHECK — there is nothing to key it by.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.bucketing_global_stats_cache (
    only_row            BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (only_row),
    taxonomy_finalized  BIGINT      NOT NULL DEFAULT 0,
    bucket_assigned     BIGINT      NOT NULL DEFAULT 0,
    refreshed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    refresh_ms          INTEGER
);

ALTER TABLE public.bucketing_global_stats_cache DISABLE ROW LEVEL SECURITY;
GRANT SELECT ON public.bucketing_global_stats_cache TO anon, authenticated, service_role;

-- Recompute and store. Deliberately VOLATILE and slow — it is the thing
-- the read path no longer has to do. Runs on a schedule (below) and can
-- be invoked on demand.
--
-- No SET statement_timeout: on a plpgsql function it would be a no-op
-- anyway (the timer is armed before the function is entered), and the
-- caller sets its own. See 20260916_uninlinable_read_rpcs.sql.
CREATE OR REPLACE FUNCTION public.refresh_bucketing_global_stats()
RETURNS public.bucketing_global_stats_cache
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_t0        TIMESTAMPTZ := clock_timestamp();
    v_finalized BIGINT;
    v_assigned  BIGINT;
    v_row       public.bucketing_global_stats_cache;
BEGIN
    SELECT COUNT(*) INTO v_finalized
      FROM (SELECT DISTINCT contact_id FROM bucket_contact_map) d;
    SELECT COUNT(*) INTO v_assigned
      FROM (SELECT DISTINCT contact_id FROM bucket_assignments) d;

    INSERT INTO public.bucketing_global_stats_cache AS t
        (only_row, taxonomy_finalized, bucket_assigned, refreshed_at, refresh_ms)
    VALUES
        (TRUE, v_finalized, v_assigned, now(),
         (EXTRACT(EPOCH FROM (clock_timestamp() - v_t0)) * 1000)::INTEGER)
    ON CONFLICT (only_row) DO UPDATE SET
        taxonomy_finalized = EXCLUDED.taxonomy_finalized,
        bucket_assigned    = EXCLUDED.bucket_assigned,
        refreshed_at       = EXCLUDED.refreshed_at,
        refresh_ms         = EXCLUDED.refresh_ms
    RETURNING t.* INTO v_row;

    RETURN v_row;
END;
$$;

GRANT EXECUTE ON FUNCTION public.refresh_bucketing_global_stats()
    TO service_role;

-- Read path. Signature is unchanged so the server and UI need no edit:
-- same eight columns, same order, same types.
CREATE OR REPLACE FUNCTION public.get_dashboard_stats()
RETURNS TABLE(
    total_imported      BIGINT,
    enriched            BIGINT,
    failed              BIGINT,
    pending             BIGINT,
    taxonomy_finalized  BIGINT,
    bucket_assigned     BIGINT,
    run_count           BIGINT,
    completed_run_count BIGINT
)
LANGUAGE sql
STABLE
SET search_path TO 'public'
AS $$
    WITH p0 AS (
        SELECT
            COALESCE(SUM(total_count), 0)::BIGINT     AS total_imported,
            COALESCE(SUM(completed_count), 0)::BIGINT AS enriched,
            COALESCE(SUM(failed_count), 0)::BIGINT    AS failed
        FROM list_enrichment_stats_cache
    )
    SELECT
        p0.total_imported,
        p0.enriched,
        p0.failed,
        GREATEST(p0.total_imported - p0.enriched - p0.failed, 0)::BIGINT AS pending,
        -- Cached; refreshed on a schedule. Zero until the first refresh,
        -- which the cron below performs within the hour of deploy.
        COALESCE((SELECT c.taxonomy_finalized FROM bucketing_global_stats_cache c), 0)::BIGINT,
        COALESCE((SELECT c.bucket_assigned    FROM bucketing_global_stats_cache c), 0)::BIGINT,
        (SELECT COUNT(*) FROM bucketing_runs)::BIGINT,
        (SELECT COUNT(*) FROM bucketing_runs WHERE status = 'completed')::BIGINT
    FROM p0;
$$;

GRANT EXECUTE ON FUNCTION public.get_dashboard_stats()
    TO anon, authenticated, service_role;

-- Hourly, offset off the :17 purge job.
--
-- The refresh measured 113 s, and it reads ~6GB, so it is not free — it
-- evicts buffer cache for everything else while it runs. These counts
-- only move when a bucketing run finishes, which happens a few times a
-- day, so scanning every 20 minutes would spend hours of I/O a day
-- recomputing numbers that had not changed. Hourly is the safety net;
-- freshness when it actually matters comes from the on-demand trigger
-- below, which the dashboard endpoint fires when the cache is stale.
SELECT cron.unschedule('refresh-bucketing-global-stats')
 WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'refresh-bucketing-global-stats');

SELECT cron.schedule(
    'refresh-bucketing-global-stats',
    '33 * * * *',
    $cron$SELECT public.refresh_bucketing_global_stats()$cron$
);

-- On-demand, non-blocking from the caller's point of view: it returns the
-- cache row as it stands and only schedules the recompute when the data is
-- older than p_max_age. The dashboard endpoint calls this on load, so a
-- user who just finished a run sees current numbers on their next visit
-- rather than waiting up to an hour.
--
-- pg_background would be needed to make this truly async in-database;
-- instead the server calls it fire-and-forget, which keeps the endpoint
-- fast without another extension.
CREATE OR REPLACE FUNCTION public.refresh_bucketing_global_stats_if_stale(
    p_max_age INTERVAL DEFAULT '30 minutes'
)
RETURNS public.bucketing_global_stats_cache
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_row public.bucketing_global_stats_cache;
BEGIN
    SELECT * INTO v_row FROM public.bucketing_global_stats_cache;
    IF v_row IS NULL OR v_row.refreshed_at < now() - p_max_age THEN
        RETURN public.refresh_bucketing_global_stats();
    END IF;
    RETURN v_row;
END;
$$;

GRANT EXECUTE ON FUNCTION public.refresh_bucketing_global_stats_if_stale(INTERVAL)
    TO service_role;

-- NOTE: at ~113 s neither refresh function can be reached over PostgREST —
-- the gateway severs the request at ~60 s and service_role carries a 120 s
-- statement_timeout. Both callers avoid that: pg_cron runs in-database,
-- and the dashboard endpoint goes through services/pgClient.ts on a direct
-- connection with its own timeout.

-- Seed immediately so the dashboard is not showing zeros until the first
-- scheduled run.
SELECT public.refresh_bucketing_global_stats();

NOTIFY pgrst, 'reload schema';
