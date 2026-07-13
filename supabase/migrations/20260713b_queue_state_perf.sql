-- ============================================================
-- Perf hotfix for get_list_enrichment_queue_state (from
-- 20260713_list_run_controls.sql).
--
-- The previous rewrite joined job_items → contacts to get the list name,
-- which at volume (~390k open job_items) took ~6.5s — over PostgREST's 8s
-- statement timeout once combined with the parallel stats RPC. The server
-- soft-fails on that error, so EVERY list's queue_state came back null and
-- the Import History rows fell back to their idle button (a running list
-- showed "Resume" instead of Pause/Cancel).
--
-- job_items.lead_list_name is denormalized (trigger-populated on insert),
-- so we drop the contacts join entirely. The open-items aggregation then
-- rides idx_job_items_active_status and runs in ~110ms. Logic is otherwise
-- identical to the previous definition (running / queued / paused / cancelled).
-- ============================================================

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
        -- Lists with at least one open job_item. Uses the denormalized
        -- job_items.lead_list_name (no contacts join) so this rides the
        -- partial index idx_job_items_active_status.
        SELECT ji.lead_list_name AS name,
               bool_or(ji.status = 'processing') AS any_processing
        FROM job_items ji
        WHERE ji.status IN ('pending', 'retrying', 'processing')
          AND ji.lead_list_name IS NOT NULL
        GROUP BY ji.lead_list_name
    ),
    paused_lists AS (
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
        SELECT DISTINCT ON (j.lead_list_name)
               j.lead_list_name AS name,
               j.status
        FROM jobs j
        WHERE j.lead_list_name IS NOT NULL
        ORDER BY j.lead_list_name, j.created_at DESC
    )
    SELECT o.name AS lead_list_name,
           CASE
               WHEN p.name IS NOT NULL THEN 'paused'
               WHEN o.any_processing   THEN 'running'
               ELSE 'queued'
           END AS queue_state
    FROM open_items o
    LEFT JOIN paused_lists p ON p.name = o.name

    UNION ALL

    SELECT lj.name AS lead_list_name,
           'cancelled' AS queue_state
    FROM latest_job lj
    WHERE lj.status = 'cancelled'
      AND lj.name NOT IN (SELECT name FROM open_items);
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_queue_state()
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
