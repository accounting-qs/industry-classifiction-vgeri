-- =====================================================================
-- Fix the quadratic plan that stalled Phase 1b at 124k contacts
--
-- Context: run 7258e616 (124,075 contacts / 84,479 distinct industries)
-- hung on its last step. Both finalize_per_contact_taxonomy and
-- apply_rollup_bucket_assignments ran past 30 minutes and were cancelled
-- by the caller's statement_timeout.
--
-- Root cause was the planner, not the volume. Both functions build their
-- per-contact temp table with the same join:
--
--   FROM contacts c
--   LEFT JOIN enrichments e ON e.contact_id = c.contact_id
--   LEFT JOIN bucket_industry_map m
--          ON m.bucketing_run_id = p_run_id
--         AND m.industry_string  = COALESCE(NULLIF(TRIM(e.classification),''), c.industry)
--   WHERE c.lead_list_name = ANY(v_list_names)
--
-- `contacts` was last analyzed 2026-08-17; the run's two lists were
-- imported 2026-09-10/11. Neither list value was in the most_common_vals
-- array for contacts.lead_list_name, so the planner estimated 1 row where
-- there were 124,075. On that estimate it chose a nested loop and dropped
-- the industry_string equality to a *join filter*, indexing only on
-- bucketing_run_id:
--
--   Nested Loop Left Join
--     Join Filter: (m.industry_string = COALESCE(...))
--     ->  Index Scan ... on contacts c  (rows=1)      -- actually 124,075
--     ->  Index Scan using bucket_industry_map_run_bucket_idx on m
--           Index Cond: (bucketing_run_id = '7258e616...')   -- 84,479 rows
--
-- That is 124,075 x 84,479 = ~10.5 billion comparisons per call. With
-- correct estimates the same query plans as a single-pass hash join and
-- finishes in seconds.
--
-- Autoanalyze could not self-heal it: the threshold is
-- 0.1 * 6M = ~600k modifications and only 257k had accumulated, so the
-- stale estimate would have persisted for months.
--
-- Three independent layers below, because any one of them alone still
-- leaves a plan that *depends* on a good estimate:
--   1. keep the statistics accurate (fixes the estimate)
--   2. make every list value exactly represented (keeps it accurate)
--   3. restructure the join so a bad estimate can no longer produce a
--      quadratic plan (removes the dependency)
--
-- Idempotent: safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Analyze these tables far more often than the 10% default.
--
-- A lead-list import adds 20k-120k rows to a 6M-row table — 0.3%-2%, so
-- nowhere near the default 10% trigger, yet it introduces a brand new
-- lead_list_name value that every bucketing query filters on. That is the
-- exact shape that produces a catastrophic misestimate.
--
-- 0.002 puts the trigger at ~12k modified rows. ANALYZE on all three of
-- these tables together measured 25 s, and it samples rather than scans,
-- so the cost of running it more often is negligible next to a 30-minute
-- nested loop.
-- ---------------------------------------------------------------------
ALTER TABLE contacts            SET (autovacuum_analyze_scale_factor = 0.002);
ALTER TABLE enrichments         SET (autovacuum_analyze_scale_factor = 0.002);
ALTER TABLE bucket_industry_map SET (autovacuum_analyze_scale_factor = 0.002);

-- ---------------------------------------------------------------------
-- 2. Hold every lead list in most_common_vals, not just the top 100.
--
-- default_statistics_target is 100, which caps most_common_vals at 100
-- entries. There are 84 distinct lead_list_name values today, so the array
-- happens to hold all of them — but at 101 lists the planner would start
-- estimating some lists from the residual-frequency fallback, which is
-- how this failure started. Since the column is low-cardinality and every
-- bucketing query filters on it, store exact per-value frequencies with
-- room to grow.
--
-- Same for enrichments.lead_list_name, which the enrichment paths filter
-- on identically.
--
-- Takes effect at the next ANALYZE, which the statement below forces.
-- ---------------------------------------------------------------------
ALTER TABLE contacts    ALTER COLUMN lead_list_name SET STATISTICS 1000;
ALTER TABLE enrichments ALTER COLUMN lead_list_name SET STATISTICS 1000;

-- ---------------------------------------------------------------------
-- 3. Drop the statement_timeout settings that never did anything.
--
-- Both functions carry `SET statement_timeout TO '600s'`. A function-level
-- SET is applied when the function is entered — which is *mid-statement*
-- for the top-level `SELECT fn(...)` that invoked it. Postgres arms the
-- statement timer once, when the command starts, and never re-arms it when
-- the GUC changes, so the value is simply ignored for the call that sets
-- it. Demonstrated two ways:
--
--   -- returns successfully after 5 s despite the 2 s timeout:
--   SELECT set_config('statement_timeout','2s',false), pg_sleep(5);
--
--   -- and in production, a call to the "600s" function observed at
--   -- 29 minutes and still running.
--
-- The timeout that actually applied was the 1,800,000 ms the caller sets
-- on the connection in services/pgClient.ts, which matches the 1801481 ms
-- recorded in bucketing_run_logs to the millisecond. Removing the clause
-- so nobody reads a 600 s ceiling that does not exist; the real one is,
-- and should stay, the caller's.
-- ---------------------------------------------------------------------
ALTER FUNCTION public.apply_rollup_bucket_assignments(UUID, INTEGER, INTEGER)
    RESET statement_timeout;
ALTER FUNCTION public.finalize_per_contact_taxonomy(UUID)
    RESET statement_timeout;

-- ---------------------------------------------------------------------
-- 4. Refresh the statistics now.
--
-- Points 1 and 2 only change when the *next* analyze happens and what it
-- records. The currently-stale estimates need one explicit pass, which
-- also populates the widened most_common_vals arrays.
-- ---------------------------------------------------------------------
ANALYZE contacts;
ANALYZE enrichments;
ANALYZE bucket_industry_map;
