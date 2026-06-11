-- Make jobs.status the single source of truth for queue liveness.
--
-- The earlier get_list_enrichment_queue_state() RPC scanned job_items in
-- pending/retrying/processing with no awareness of the parent jobs.status.
-- A job_item whose parent job is already 'completed' or 'cancelled' will
-- never be picked up again by the worker (FIFO only walks 'processing'
-- and 'pending' jobs), but the old RPC still reported the list as
-- 'queued' — that's the leftover amber badge the user observed on
-- "FL, Oct 14, All Other Industries Bad + Too Big" after the FIFO-drain-
-- while-retrying bug (fixed in 90a7b34) left orphan rows behind.
--
-- New behaviour: a list shows in the queue-state RPC only if it has at
-- least one open job_item whose parent job is still live (pending OR
-- processing). Items whose parent is terminal are filtered out — the
-- reaper added in the next commit will clean those up at the row level
-- so the discrepancy can never recur.

CREATE OR REPLACE FUNCTION public.get_list_enrichment_queue_state()
RETURNS TABLE (
    lead_list_name TEXT,
    queue_state    TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT
        c.lead_list_name,
        CASE WHEN bool_or(ji.status = 'processing') THEN 'running' ELSE 'queued' END AS queue_state
    FROM job_items ji
    JOIN jobs     j ON j.id = ji.job_id AND j.status IN ('pending', 'processing')
    JOIN contacts c ON c.contact_id = ji.contact_id
    WHERE ji.status IN ('pending', 'retrying', 'processing')
      AND c.lead_list_name IS NOT NULL
    GROUP BY c.lead_list_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_queue_state() TO anon, authenticated, service_role;

-- Helper for the worker's orphan-rescue path. Returns the ids of jobs
-- in 'pending' / 'cancelled' / 'completed' that STILL have at least one
-- job_item in pending/retrying/processing — i.e. jobs the FIFO walker
-- would miss because they're not in 'processing'. Uses EXISTS so the
-- query stops at the first matching item per job rather than counting
-- (cheap on workspaces with millions of historical jobs).

CREATE OR REPLACE FUNCTION public.list_jobs_with_open_items(p_limit INTEGER DEFAULT 50)
RETURNS TABLE (id UUID, status TEXT, created_at TIMESTAMPTZ)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT j.id, j.status, j.created_at
    FROM jobs j
    WHERE j.status IN ('pending', 'cancelled', 'completed')
      AND EXISTS (
        SELECT 1 FROM job_items ji
        WHERE ji.job_id = j.id
          AND ji.status IN ('pending', 'retrying', 'processing')
      )
    ORDER BY j.created_at ASC
    LIMIT GREATEST(COALESCE(p_limit, 50), 1);
$$;

GRANT EXECUTE ON FUNCTION public.list_jobs_with_open_items(INTEGER) TO anon, authenticated, service_role;
