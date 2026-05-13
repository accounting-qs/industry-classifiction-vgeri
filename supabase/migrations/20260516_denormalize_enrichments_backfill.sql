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

-- The batch CTE filters on `c.lead_list_name IS NOT NULL` (not just
-- on the enrichment side) so we don't keep re-picking orphaned rows
-- — enrichments whose contact is missing or whose contact has a NULL
-- list name. Without that filter the LIMIT would land on the same
-- ~25k stuck rows on every run and the script would loop forever
-- with `updated_this_batch = 0`.

WITH batch AS (
    SELECT e.contact_id, c.lead_list_name
      FROM enrichments e
      JOIN contacts c ON c.contact_id = e.contact_id
     WHERE e.lead_list_name IS NULL
       AND c.lead_list_name IS NOT NULL
     LIMIT 25000
),
updated AS (
    UPDATE enrichments e
       SET lead_list_name = b.lead_list_name
      FROM batch b
     WHERE e.contact_id = b.contact_id
    RETURNING e.contact_id
)
SELECT
    (SELECT COUNT(*) FROM updated) AS updated_this_batch,
    -- Total rows still waiting on the column. Drops by `updated_this_batch`
    -- every run. Stop when this hits 0.
    (SELECT COUNT(*) FROM enrichments WHERE lead_list_name IS NULL) AS remaining_after,
    -- Enrichments we can never backfill because their contact is
    -- gone or carries no list name. Subtract this from
    -- `remaining_after` to find the true end-state: when
    -- `remaining_after = unbackfillable`, the run is finished.
    (
        SELECT COUNT(*)
          FROM enrichments e
         WHERE e.lead_list_name IS NULL
           AND NOT EXISTS (
               SELECT 1 FROM contacts c
                WHERE c.contact_id = e.contact_id
                  AND c.lead_list_name IS NOT NULL
           )
    ) AS unbackfillable;
