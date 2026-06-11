-- Atomic job completion + orphan reaper.
--
-- Prior to this migration, the worker marked a job complete in a bare
-- UPDATE that left any remaining open job_items dangling. The 90a7b34
-- patch fixed the most common path (retry-backoff items kept the job
-- in 'processing'), but a crash mid-claim, the unique-violation rollback
-- path, and the global auto-stop branch all flipped jobs to 'completed'
-- without atomically retiring open items.
--
-- This RPC makes the transition atomic in one SQL function:
--   1) Retire any still-open job_items belonging to the job (set
--      status='failed', error_message marker, finished_at=now()).
--   2) Bump jobs.failed_items by the retired count.
--   3) Flip jobs.status to 'completed' and stamp finished_at.
-- Returns the number of items reaped so the caller can log it.
--
-- A separate reap_orphan_job_items() function catches anything that
-- slipped past — items whose parent job is ALREADY terminal but they
-- themselves are still open. The worker runs it on startup (one-shot
-- cleanup) and on a slow (60s) interval inside the poll loop. The
-- 60s grace prevents racing a fresh processNextChunk completion that
-- just flipped its job but hasn't yet finished its own open-count
-- probe; the grace also gives a crashed-and-restarted worker time to
-- run recoverStaleJobs first.

CREATE OR REPLACE FUNCTION public.reconcile_and_complete_job(p_job_id UUID)
RETURNS TABLE (reaped INTEGER, prior_status TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_reaped       INTEGER := 0;
    v_prior_status TEXT;
BEGIN
    SELECT status INTO v_prior_status FROM jobs WHERE id = p_job_id;
    IF v_prior_status IS NULL THEN
        RAISE EXCEPTION 'job % does not exist', p_job_id;
    END IF;

    -- Only act when the caller still believes the job is processing.
    -- Re-running this RPC on an already-completed job is a no-op.
    IF v_prior_status <> 'processing' THEN
        RETURN QUERY SELECT 0::INTEGER, v_prior_status;
        RETURN;
    END IF;

    WITH reaped_rows AS (
        UPDATE job_items
        SET status        = 'failed',
            finished_at   = COALESCE(finished_at, now()),
            error_message = COALESCE(error_message, 'Job terminated with open items')
        WHERE job_id = p_job_id
          AND status IN ('pending', 'retrying', 'processing')
          AND finished_at IS NULL
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_reaped FROM reaped_rows;

    UPDATE jobs
    SET status       = 'completed',
        failed_items = failed_items + v_reaped,
        finished_at  = COALESCE(finished_at, now())
    WHERE id     = p_job_id
      AND status = 'processing';

    RETURN QUERY SELECT v_reaped::INTEGER, v_prior_status;
END;
$$;

GRANT EXECUTE ON FUNCTION public.reconcile_and_complete_job(UUID) TO anon, authenticated, service_role;


-- Defence-in-depth reaper. Retires job_items in open statuses whose
-- parent job is already terminal AND finished at least p_grace_seconds
-- ago. Returns the number retired. Safe to call frequently; touches
-- only rows that match the orphan predicate.

CREATE OR REPLACE FUNCTION public.reap_orphan_job_items(p_grace_seconds INTEGER DEFAULT 60)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_reaped INTEGER;
BEGIN
    WITH reaped_rows AS (
        UPDATE job_items ji
        SET status        = 'failed',
            finished_at   = COALESCE(ji.finished_at, now()),
            error_message = COALESCE(ji.error_message, 'Orphaned: parent job already terminal')
        FROM jobs j
        WHERE ji.job_id = j.id
          AND ji.status IN ('pending', 'retrying', 'processing')
          AND ji.finished_at IS NULL
          AND j.status IN ('completed', 'cancelled', 'failed')
          AND COALESCE(j.finished_at, j.started_at, j.created_at)
              < now() - make_interval(secs => GREATEST(COALESCE(p_grace_seconds, 60), 0))
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_reaped FROM reaped_rows;

    RETURN v_reaped;
END;
$$;

GRANT EXECUTE ON FUNCTION public.reap_orphan_job_items(INTEGER) TO anon, authenticated, service_role;
