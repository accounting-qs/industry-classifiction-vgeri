-- App-wide phase-funnel stats for the Enrichment Dashboard (/dashboard).
--
-- Answers "how many leads are in each phase?" in one round trip, without
-- any expensive aggregation on a hot read path:
--   * Phase 0 (enrichment) sums the already-materialized
--     list_enrichment_stats_cache (the same source the import page reads),
--     so this part is a 56-row scan, not a 2.6M-row aggregate.
--   * Phase 1a/1b use count(DISTINCT contact_id) over the run-scoped
--     bucket tables. A contact that appears in multiple bucketing runs is
--     counted once — a true app-wide lead count, not a sum of per-run
--     totals (which would double-count). The dashboard fetches this only
--     on load + manual Refresh (no polling), so a single on-demand
--     aggregate here is fine.

CREATE OR REPLACE FUNCTION public.get_dashboard_stats()
RETURNS TABLE (
    total_imported      BIGINT,   -- Phase 0: live contacts across all lists
    enriched            BIGINT,   -- Phase 0: enrichments.status = 'completed'
    failed              BIGINT,   -- Phase 0: enrichments.status = 'failed'
    pending             BIGINT,   -- Phase 0: total - enriched - failed (>= 0)
    taxonomy_finalized  BIGINT,   -- Phase 1a: distinct contacts in bucket_contact_map
    bucket_assigned     BIGINT,   -- Phase 1b: distinct contacts in bucket_assignments
    run_count           BIGINT,   -- bucketing runs total
    completed_run_count BIGINT    -- bucketing runs in 'completed' status
)
LANGUAGE sql
STABLE
SET search_path TO 'public'
SET statement_timeout TO '30s'
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
        GREATEST(p0.total_imported - p0.enriched - p0.failed, 0)::BIGINT       AS pending,
        (SELECT COUNT(DISTINCT contact_id) FROM bucket_contact_map)::BIGINT    AS taxonomy_finalized,
        (SELECT COUNT(DISTINCT contact_id) FROM bucket_assignments)::BIGINT    AS bucket_assigned,
        (SELECT COUNT(*) FROM bucketing_runs)::BIGINT                          AS run_count,
        (SELECT COUNT(*) FROM bucketing_runs WHERE status = 'completed')::BIGINT AS completed_run_count
    FROM p0;
$$;

GRANT EXECUTE ON FUNCTION public.get_dashboard_stats()
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
