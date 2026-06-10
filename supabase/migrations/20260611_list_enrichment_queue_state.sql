-- Per-list enrichment queue state — running vs queued vs nothing-pending.
--
-- Backs the queue-state badge on the Import History list. With multi-list
-- FIFO, more than one list can have job_items pending at the same time —
-- exactly one runs (active job has items in 'processing'), the rest sit
-- queued behind it. The UI uses this to mark which list the worker is on
-- and which are next in line, so re-clicking Enrich on an already-queued
-- list is visibly redundant instead of silently a no-op.
--
-- Returns one row per distinct lead_list_name that currently has at least
-- one job_item in pending / retrying / processing. Lists with everything
-- done (or never enriched) are omitted; the client renders no badge for
-- those.

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
    JOIN contacts c ON c.contact_id = ji.contact_id
    WHERE ji.status IN ('pending', 'retrying', 'processing')
      AND c.lead_list_name IS NOT NULL
    GROUP BY c.lead_list_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_queue_state() TO anon, authenticated, service_role;
