-- ============================================================
-- Per-list run controls: Pause / Continue / Cancel
--
-- Adds a `paused` state to the enrichment pipeline that is scoped to a
-- single list, plus a persistent `cancelled` signal so the Import History
-- row can offer the right action after the run is stopped.
--
-- Design (leans on the existing FIFO worker with ZERO worker-loop change):
--   * jobs.lead_list_name — denormalized list name on the job row so a
--     "pause/cancel list X" action maps directly to that list's job(s).
--     (Every enqueue is single-list, so one job == one list.)
--   * PAUSE   = UPDATE jobs SET status='paused'. The FIFO walker only claims
--     'processing' jobs, and list_jobs_with_open_items() only returns
--     pending/cancelled/completed jobs — so a 'paused' job is skipped by
--     BOTH claim paths without touching jobProcessor. Its job_items stay
--     'pending', so CONTINUE just flips the status back to 'processing'.
--   * CONTINUE = UPDATE jobs SET status='processing' (worker resumes the
--     remaining un-enriched items — the ones that were still queued).
--   * CANCEL  = delete the list's open job_items (mirrors /api/reset so the
--     orphan-rescue path can't resurrect it) + UPDATE jobs SET
--     status='cancelled'. The cancelled job row persists as the signal.
--
-- get_list_enrichment_queue_state() is rewritten to emit four states:
--   running | queued | paused | cancelled
-- ============================================================

-- 1. Denormalized list name on jobs -------------------------------------
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lead_list_name TEXT;

-- Backfill existing rows from their job_items (mode = the dominant list for
-- the job; every job is single-list in practice so this is exact).
UPDATE jobs j
SET lead_list_name = sub.name
FROM (
    SELECT job_id, mode() WITHIN GROUP (ORDER BY lead_list_name) AS name
    FROM job_items
    WHERE lead_list_name IS NOT NULL
    GROUP BY job_id
) sub
WHERE sub.job_id = j.id
  AND j.lead_list_name IS NULL;

-- Index the per-list control lookups (pause/continue/cancel filter on this).
CREATE INDEX IF NOT EXISTS idx_jobs_lead_list_name_status
    ON jobs (lead_list_name, status);

-- 2. Per-list queue-state RPC (now including paused + cancelled) ---------
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
    WITH open_items AS (
        -- Lists that currently have at least one open job_item.
        SELECT c.lead_list_name AS name,
               bool_or(ji.status = 'processing') AS any_processing
        FROM job_items ji
        JOIN contacts c ON c.contact_id = ji.contact_id
        WHERE ji.status IN ('pending', 'retrying', 'processing')
          AND c.lead_list_name IS NOT NULL
        GROUP BY c.lead_list_name
    ),
    paused_lists AS (
        -- A list is 'paused' when its job is parked in 'paused' but still
        -- has open (un-drained) items waiting to resume.
        SELECT DISTINCT j.lead_list_name AS name
        FROM jobs j
        WHERE j.status = 'paused'
          AND j.lead_list_name IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM job_items ji
              WHERE ji.job_id = j.id
                AND ji.status IN ('pending', 'retrying', 'processing')
          )
    ),
    latest_job AS (
        -- Most recent job per list — used to surface a persistent
        -- 'cancelled' signal once the open items have been deleted.
        SELECT DISTINCT ON (j.lead_list_name)
               j.lead_list_name AS name,
               j.status
        FROM jobs j
        WHERE j.lead_list_name IS NOT NULL
        ORDER BY j.lead_list_name, j.created_at DESC
    )
    -- Active lists (have open items): running / paused / queued.
    SELECT o.name AS lead_list_name,
           CASE
               WHEN p.name IS NOT NULL   THEN 'paused'
               WHEN o.any_processing     THEN 'running'
               ELSE 'queued'
           END AS queue_state
    FROM open_items o
    LEFT JOIN paused_lists p ON p.name = o.name

    UNION ALL

    -- Cancelled lists (no open items left, latest job is cancelled).
    SELECT lj.name AS lead_list_name,
           'cancelled' AS queue_state
    FROM latest_job lj
    WHERE lj.status = 'cancelled'
      AND lj.name NOT IN (SELECT name FROM open_items);
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_queue_state()
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
