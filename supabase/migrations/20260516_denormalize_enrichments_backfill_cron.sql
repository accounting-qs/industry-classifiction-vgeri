-- OPTIONAL: pg_cron alternative to the manual paste-and-rerun
-- backfill script. Run this once and walk away — the cron job
-- processes 10k rows every 30 seconds until there's nothing left to
-- backfill, then unschedules itself.
--
-- Requires pg_cron, which is on by default for Supabase Pro projects.
-- If `CREATE EXTENSION` fails with a permission error, fall back to
-- the manual script (20260516_denormalize_enrichments_backfill.sql).
--
-- To monitor progress while it runs:
--   SELECT COUNT(*) FROM enrichments WHERE lead_list_name IS NULL;
--
-- To inspect the cron job:
--   SELECT * FROM cron.job WHERE jobname = 'backfill_enrichments_list_name';
--   SELECT * FROM cron.job_run_details
--    WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = 'backfill_enrichments_list_name')
--    ORDER BY start_time DESC LIMIT 10;
--
-- To stop it manually before completion:
--   SELECT cron.unschedule('backfill_enrichments_list_name');

CREATE EXTENSION IF NOT EXISTS pg_cron;

-- The chunk function the cron job calls. Split out so we can grant
-- explicit execute permission and tweak the batch size without
-- editing the schedule. Auto-unschedules when there's nothing left
-- so the job stops cleanly instead of running forever.
CREATE OR REPLACE FUNCTION public.backfill_enrichments_list_name_chunk()
RETURNS BIGINT
LANGUAGE plpgsql
SET statement_timeout TO '45s'
AS $$
DECLARE
    v_updated BIGINT;
BEGIN
    WITH batch AS (
        SELECT e.contact_id, c.lead_list_name
          FROM enrichments e
          JOIN contacts c ON c.contact_id = e.contact_id
         WHERE e.lead_list_name IS NULL
           AND c.lead_list_name IS NOT NULL
         LIMIT 10000
    ),
    upd AS (
        UPDATE enrichments e
           SET lead_list_name = b.lead_list_name
          FROM batch b
         WHERE e.contact_id = b.contact_id
        RETURNING e.contact_id
    )
    SELECT COUNT(*) INTO v_updated FROM upd;

    -- Self-terminate: if nothing was updated this tick, every
    -- remaining NULL row is either an orphan or has a NULL list name
    -- on its contact — no future run will touch them either. Drop
    -- the cron job so it stops occupying a worker slot.
    IF v_updated = 0 THEN
        PERFORM cron.unschedule('backfill_enrichments_list_name');
    END IF;

    RETURN v_updated;
END;
$$;

-- Run every 30 seconds. At 10k rows/run that's 20k rows/min, so a
-- ~600k row table finishes in ~30 minutes without any user input.
SELECT cron.schedule(
    'backfill_enrichments_list_name',
    '30 seconds',
    $$ SELECT public.backfill_enrichments_list_name_chunk(); $$
);
