-- =====================================================================
-- Self-cleaning retention for bucketing run history
--
-- bucket_contact_map and bucket_assignments each hold one row per contact
-- per run and nothing was ever purged: 3,303,701 rows each / 3,860 MB
-- combined across 19 runs today. At the 700k-contact target every run adds
-- ~700k rows to each table — roughly 820 MB a run. Left alone that fills
-- the disk (currently ~70% used) and slowly degrades every per-run
-- operation through index depth and autovacuum load.
--
-- WHY SIZE-TRIGGERED, NOT TIME-BASED
-- ----------------------------------
-- Age does not correlate with cost. A 20k-contact run from three months
-- ago occupies almost nothing; a 700k run from yesterday occupies ~820 MB.
-- Purging by age would either discard cheap old history or leave the
-- expensive recent runs untouched — the opposite of what is needed.
--
-- Purging by run count has the same flaw: "keep the last 5" is anywhere
-- between 100k and 3.5M rows depending on how big those runs were.
--
-- So the trigger is the thing that actually hurts — total row count —
-- with a floor of recent runs that is never purged regardless. The timer
-- is only the mechanism that evaluates the threshold; it is not the policy.
--
-- Idempotent: safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Purge oldest-first until under the row cap.
--
-- Guarantees:
--   * never runs while any bucketing run is in flight — a concurrent
--     DELETE on these tables would contend with the run's own writes
--   * never purges the p_min_keep_runs most recent runs, whatever the cap
--   * stops as soon as it is under the cap, so it keeps as much history
--     as fits rather than trimming to a fixed number
--   * terminates when eligible runs are exhausted, even if still over cap
--     (a single run larger than the cap must not cause an infinite loop)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION purge_bucketing_rows_over_limit(
    p_max_map_rows  bigint  DEFAULT 5000000,
    p_min_keep_runs integer DEFAULT 5
)
RETURNS TABLE (purged_runs bigint, purged_map_rows bigint, purged_assignment_rows bigint, remaining_map_rows bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_total     bigint;
    v_runs      bigint := 0;
    v_map       bigint := 0;
    v_asg       bigint := 0;
    r           record;
    v_this_map  bigint;
    v_this_asg  bigint;
BEGIN
    IF p_min_keep_runs < 1 THEN
        RAISE EXCEPTION 'p_min_keep_runs must be >= 1 (refusing to purge every run)';
    END IF;

    -- Bail out entirely if a run is working. Retention is never urgent
    -- enough to risk contending with a live rollup.
    IF EXISTS (SELECT 1 FROM bucketing_runs WHERE status IN ('assigning', 'taxonomy_pending')) THEN
        RETURN QUERY SELECT 0::bigint, 0::bigint, 0::bigint,
                            (SELECT count(*) FROM bucket_contact_map)::bigint;
        RETURN;
    END IF;

    SELECT count(*) INTO v_total FROM bucket_contact_map;
    IF v_total <= p_max_map_rows THEN
        RETURN QUERY SELECT 0::bigint, 0::bigint, 0::bigint, v_total;
        RETURN;
    END IF;

    FOR r IN
        SELECT id FROM bucketing_runs
        WHERE status NOT IN ('assigning', 'taxonomy_pending')
          AND id NOT IN (SELECT id FROM bucketing_runs ORDER BY created_at DESC LIMIT p_min_keep_runs)
        ORDER BY created_at ASC        -- oldest first
    LOOP
        EXIT WHEN v_total <= p_max_map_rows;

        WITH d AS (DELETE FROM bucket_contact_map WHERE bucketing_run_id = r.id RETURNING 1)
        SELECT count(*) INTO v_this_map FROM d;

        WITH d AS (DELETE FROM bucket_assignments WHERE bucketing_run_id = r.id RETURNING 1)
        SELECT count(*) INTO v_this_asg FROM d;

        v_map   := v_map + v_this_map;
        v_asg   := v_asg + v_this_asg;
        v_total := v_total - v_this_map;
        v_runs  := v_runs + 1;

        RAISE NOTICE 'purged bucketing run % — % map rows, % assignment rows (remaining %)',
                     r.id, v_this_map, v_this_asg, v_total;
    END LOOP;

    RETURN QUERY SELECT v_runs, v_map, v_asg, v_total;
END;
$$;

REVOKE ALL ON FUNCTION purge_bucketing_rows_over_limit(bigint, integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION purge_bucketing_rows_over_limit(bigint, integer) TO service_role;

-- ---------------------------------------------------------------------
-- Schedule it in the database, not the app.
--
-- pg_cron rather than another setInterval in server.ts: retention must
-- not depend on the web service being up, not mid-deploy, and not
-- mid-restart. It is pure data hygiene with no application logic, and the
-- app already restarts on every push.
--
-- Hourly, because the check itself is a single count(*) and does nothing
-- until the cap is crossed. Cost when under the cap is negligible.
--
-- Cap rationale: 5,000,000 rows ≈ 5.8 GB across both tables at the
-- current ~1.17 KB/row-pair. Today's 3.3M / 3,860 MB sits comfortably
-- under it, leaving room for ~2 more 700k runs before anything is purged,
-- and it caps growth well inside the ~7 GB of free disk.
-- Adjust with: SELECT cron.unschedule('bucketing-retention'); then re-run
-- this block with a different cap.
-- ---------------------------------------------------------------------
DO $$
BEGIN
    PERFORM cron.unschedule('bucketing-retention');
EXCEPTION WHEN OTHERS THEN
    NULL;  -- not scheduled yet
END $$;

SELECT cron.schedule(
    'bucketing-retention',
    '17 * * * *',   -- hourly at :17, off the top of the hour so it does not
                    -- pile onto other scheduled work
    $$SELECT purge_bucketing_rows_over_limit(5000000, 5)$$
);
