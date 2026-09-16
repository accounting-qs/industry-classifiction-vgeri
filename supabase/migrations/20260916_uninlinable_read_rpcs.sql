-- =====================================================================
-- Let the read-path RPCs inline again
--
-- Symptom: opening a bucketing run's detail page got very slow, and the
-- page polls /api/bucketing/runs/:id once a second, so the calls stacked —
-- eight-plus copies of the same query were observed running concurrently,
-- each waiting on IO behind the others.
--
-- Cause: every one of these functions carries `SET statement_timeout`.
-- A SQL function with a SET clause **cannot be inlined** into the calling
-- query. Without inlining the planner cannot push the caller's context
-- into the function body, and these all degrade badly:
--
--   get_bucket_map_counts       12.8 s  ->  0.61 s   (20x)
--   get_assigned_bucket_counts  16.0 s  ->  see below
--
-- Measured on run 6e7b8f69 (74,428 contacts / 54,244 industries) by
-- toggling nothing but the SET clause.
--
-- The clause was not buying anything to begin with. A function-level SET
-- statement_timeout never applies to the call that sets it: Postgres arms
-- the statement timer when the top-level command starts and does not
-- re-arm it when the GUC changes mid-statement. Demonstrated by
-- `SELECT set_config('statement_timeout','2s',false), pg_sleep(5)`
-- returning after 5 s, and in production by a "600s" function observed
-- running at 29 minutes (see 20260916_rollup_stale_stats_quadratic_plan).
--
-- So these clauses cost a large amount of read latency and bought no
-- timeout protection at all. The timeout that actually applies is the
-- caller's: service_role carries 120s, and the direct-Postgres path sets
-- its own per-statement value in services/pgClient.ts.
--
-- Only `LANGUAGE sql` functions are touched. plpgsql is never inlined, so
-- the clause is merely misleading there rather than expensive, and those
-- are left alone to keep this change to what it needs to be.
--
-- Idempotent: safe to re-run.
-- =====================================================================

-- Hot read paths behind the bucketing run detail page.
ALTER FUNCTION public.get_bucket_map_counts(uuid)           RESET statement_timeout;
ALTER FUNCTION public.get_bucket_assignment_counts(uuid)    RESET statement_timeout;
ALTER FUNCTION public.get_bucket_sector_mix(uuid)           RESET statement_timeout;
ALTER FUNCTION public.get_bucket_general_breakdown(uuid)    RESET statement_timeout;
ALTER FUNCTION public.get_bucket_chain_counts(uuid)         RESET statement_timeout;
ALTER FUNCTION public.get_assigned_bucket_counts(uuid)      RESET statement_timeout;
ALTER FUNCTION public.get_proposed_tag_contact_counts(uuid) RESET statement_timeout;

-- Dashboard and list views. get_dashboard_stats was the worst of the set
-- at 137 s; it is the landing page.
ALTER FUNCTION public.get_dashboard_stats()       RESET statement_timeout;
ALTER FUNCTION public.get_list_enrichment_stats() RESET statement_timeout;

-- Bulk readers used by bucketing, enrichment and export.
ALTER FUNCTION public.bucketing_run_export_rows(uuid)             RESET statement_timeout;
ALTER FUNCTION public.finalize_taxonomy_candidates(uuid)          RESET statement_timeout;
ALTER FUNCTION public.get_classification_vocabulary(text[], integer) RESET statement_timeout;
ALTER FUNCTION public.get_industry_vocabulary(text[], integer)       RESET statement_timeout;
ALTER FUNCTION public.get_contact_export_page(uuid, text, integer)   RESET statement_timeout;
ALTER FUNCTION public.get_contacts_chunk(text[], text, integer)      RESET statement_timeout;

NOTIFY pgrst, 'reload schema';
