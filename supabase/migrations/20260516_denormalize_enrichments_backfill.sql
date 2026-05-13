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
--   2. The result row shows `updated_this_batch` and `remaining_after`.
--   3. If `remaining_after > 0`, hit Run again. Repeat until it's 0.
--      Each batch processes 25k rows and typically returns in ~10s
--      even on the current dataset.
--
-- Idempotent: only touches rows where lead_list_name IS NULL, so
-- re-running after the column is fully backfilled is a no-op.

WITH batch AS (
    SELECT contact_id
      FROM enrichments
     WHERE lead_list_name IS NULL
     LIMIT 25000
),
updated AS (
    UPDATE enrichments e
       SET lead_list_name = c.lead_list_name
      FROM contacts c
     WHERE e.contact_id = c.contact_id
       AND e.contact_id IN (SELECT contact_id FROM batch)
       AND c.lead_list_name IS NOT NULL
    RETURNING e.contact_id
)
SELECT
    (SELECT COUNT(*) FROM updated)                                  AS updated_this_batch,
    (SELECT COUNT(*) FROM enrichments WHERE lead_list_name IS NULL) AS remaining_after;
