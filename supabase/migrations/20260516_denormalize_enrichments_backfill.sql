-- Batched backfill for enrichments.lead_list_name.
--
-- Paired with 20260516_denormalize_enrichments_lead_list_name.sql,
-- which adds the column, the index, the trigger, and the new stats
-- RPC. That migration is fast (DDL only); this one moves data and
-- is the part that can't run in a single shot — at ~600k rows the
-- single-statement UPDATE blows past the Supabase SQL Editor's
-- HTTP timeout (~60s) and the dashboard reports "Failed to fetch
-- (api.supabase.com)" before Postgres is done.
--
-- HOW TO RUN:
--   1. Paste this whole file into the Supabase SQL Editor and hit Run.
--   2. Postgres reports `UPDATE N` where N is the rows touched this run.
--   3. If N > 0, hit Run again. Repeat until N = 0.
--      Each batch processes 10k rows and finishes in a few seconds —
--      well inside the editor's HTTP window.
--   4. When N reaches 0, run the orphan check at the bottom (commented
--      out) to confirm no contact-less enrichments are blocking
--      progress. If it returns 0, you're done.
--
-- Idempotent: only touches rows where lead_list_name IS NULL, so
-- re-running after the column is fully backfilled is a no-op.
--
-- The batch CTE joins to contacts so we only ever pick backfillable
-- rows — without that filter the LIMIT could land on orphaned
-- enrichments and the script would loop forever with UPDATE 0.

WITH batch AS (
    SELECT e.contact_id, c.lead_list_name
      FROM enrichments e
      JOIN contacts c ON c.contact_id = e.contact_id
     WHERE e.lead_list_name IS NULL
       AND c.lead_list_name IS NOT NULL
     LIMIT 10000
)
UPDATE enrichments e
   SET lead_list_name = b.lead_list_name
  FROM batch b
 WHERE e.contact_id = b.contact_id;

-- ─────────────────────────────────────────────────────────────────
-- Run these one at a time, separately, when you want a status check.
-- They're NOT wrapped into the batch above because counting NULL rows
-- across the whole 600k-row table after a fresh UPDATE makes the
-- editor's HTTP request time out before Postgres finishes.
-- ─────────────────────────────────────────────────────────────────

-- How many rows are still waiting:
-- SELECT COUNT(*) AS remaining FROM enrichments WHERE lead_list_name IS NULL;

-- How many of those are orphans (can never be backfilled):
-- SELECT COUNT(*) AS unbackfillable
--   FROM enrichments e
--  WHERE e.lead_list_name IS NULL
--    AND NOT EXISTS (
--      SELECT 1 FROM contacts c
--       WHERE c.contact_id = e.contact_id
--         AND c.lead_list_name IS NOT NULL
--    );
