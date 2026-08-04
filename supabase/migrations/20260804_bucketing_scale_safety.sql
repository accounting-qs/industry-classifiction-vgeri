-- =====================================================================
-- Safety nets for large bucketing runs
--
-- Context: a 109k-contact run failed with
--   "rollup failed: canceling statement due to lock timeout"
--
-- The chain was:
--   1. finalize_per_contact_taxonomy took 76 s (measured).
--   2. Supabase's PostgREST gateway cancels the HTTP request at ~60 s, so
--      the caller saw "upstream request timeout" — but Postgres kept
--      executing, because service_role inherits statement_timeout=120s.
--   3. The application treated that as non-fatal and moved on.
--   4. Three minutes later the rollup's
--      DELETE FROM bucket_contact_map WHERE bucketing_run_id = ...
--      hit rows the orphaned transaction still held, and was cancelled by
--      lock_timeout (8 s, inherited from the `authenticator` role).
--
-- The application side is fixed in services/pgClient.ts: heavy run-scoped
-- RPCs now run over a direct Postgres connection, with no HTTP gateway in
-- front of them and their own statement/lock timeouts. This migration
-- covers the database-side gap that made step 4 fatal rather than
-- momentary.
--
-- Idempotent: safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Stop abandoned transactions holding locks forever.
--
-- idle_in_transaction_session_timeout is currently 0 (infinite) at every
-- level. A PostgREST request whose client vanished mid-transaction can
-- therefore keep its row locks indefinitely, and everything that touches
-- those rows fails until the connection is reaped by TCP timeout.
--
-- This only cancels sessions sitting IDLE *inside* a transaction — it
-- never interrupts a statement that is actually executing, so a genuinely
-- long rollup is unaffected. 2 min is generous for an interactive request
-- while still bounding the damage.
--
-- `authenticator` is the login role PostgREST uses (it SET ROLEs to
-- anon/authenticated/service_role afterwards, which does not reset
-- session GUCs), so setting it here covers every PostgREST session.
-- ---------------------------------------------------------------------
ALTER ROLE authenticator SET idle_in_transaction_session_timeout = '2min';

-- ---------------------------------------------------------------------
-- 2. Index the column the rollup deletes by.
--
-- apply_rollup_bucket_assignments starts with
--   DELETE FROM bucket_contact_map WHERE bucketing_run_id = p_run_id;
--   DELETE FROM bucket_assignments WHERE bucketing_run_id = p_run_id;
--
-- bucket_contact_map has (bucketing_run_id, general_reason) and
-- bucket_assignments has (bucketing_run_id, bucket_name), so the leading
-- column already serves these — but both indexes exist to serve *other*
-- queries and could be reshaped later without anyone noticing they are
-- load-bearing for the delete. A dedicated single-column index makes the
-- dependency explicit and keeps the delete an index scan as these tables
-- grow (3.3M rows across 19 runs today; ~700k more per run at target).
--
-- CONCURRENTLY: never lock a live table. Cannot run inside a transaction.
-- ---------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bucket_contact_map_run
    ON bucket_contact_map (bucketing_run_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bucket_assignments_run
    ON bucket_assignments (bucketing_run_id);

-- ---------------------------------------------------------------------
-- 3. Retention helper — NOT scheduled, and nothing is deleted by
--    applying this migration.
--
-- bucket_contact_map holds 3,303,701 rows across 19 runs and nothing is
-- ever purged; at the 700k target each run adds ~700k more. The per-run
-- operations are index-scoped so they stay roughly O(run), but index
-- depth, autovacuum load and disk all grow without bound.
--
-- Deleting run history is a product decision, so this only provides the
-- tool. Call it deliberately, e.g. keep the 5 most recent runs:
--     SELECT purge_old_bucketing_run_rows(5);
-- It never touches the newest runs, and never a run that is mid-flight.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION purge_old_bucketing_run_rows(p_keep_runs integer DEFAULT 5)
RETURNS TABLE (purged_runs bigint, purged_map_rows bigint, purged_assignment_rows bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_doomed uuid[];
    v_map bigint;
    v_asg bigint;
BEGIN
    IF p_keep_runs < 1 THEN
        RAISE EXCEPTION 'p_keep_runs must be >= 1 (refusing to purge every run)';
    END IF;

    SELECT array_agg(id) INTO v_doomed FROM (
        SELECT id FROM bucketing_runs
        -- Never touch a run that is still working.
        WHERE status NOT IN ('assigning', 'taxonomy_pending')
        ORDER BY created_at DESC
        OFFSET p_keep_runs
    ) old_runs;

    IF v_doomed IS NULL OR array_length(v_doomed, 1) IS NULL THEN
        RETURN QUERY SELECT 0::bigint, 0::bigint, 0::bigint;
        RETURN;
    END IF;

    WITH d AS (DELETE FROM bucket_contact_map WHERE bucketing_run_id = ANY(v_doomed) RETURNING 1)
    SELECT count(*) INTO v_map FROM d;

    WITH d AS (DELETE FROM bucket_assignments WHERE bucketing_run_id = ANY(v_doomed) RETURNING 1)
    SELECT count(*) INTO v_asg FROM d;

    RETURN QUERY SELECT array_length(v_doomed, 1)::bigint, v_map, v_asg;
END;
$$;

REVOKE ALL ON FUNCTION purge_old_bucketing_run_rows(integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION purge_old_bucketing_run_rows(integer) TO service_role;
