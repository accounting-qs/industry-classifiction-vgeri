-- One-shot diagnostic for the lead_list_name denormalization.
-- Run this in the Supabase SQL Editor and paste me the four result
-- blocks. Together they pinpoint why the progress bars are still 0%.

-- 1. Column-level coverage.
--    backfilled = enrichments with lead_list_name set
--    nullable   = enrichments still waiting on the backfill script
--    Compare against the row count in `contacts` for the same list —
--    a healthy state is backfilled ≈ contacts and nullable ≈ 0.
SELECT
    COUNT(*) FILTER (WHERE lead_list_name IS NOT NULL) AS backfilled,
    COUNT(*) FILTER (WHERE lead_list_name IS NULL)     AS still_null,
    COUNT(*)                                           AS total_enrichments
  FROM enrichments;

-- 2. Per-list raw aggregate. If `completed_via_join` is non-zero but
--    `completed_via_denorm` is zero, the column never got backfilled
--    for that list — the data is there, the new column just isn't.
SELECT
    il.name AS list_name,
    il.contact_count AS list_total,
    (
        SELECT COUNT(*)
          FROM enrichments e
          JOIN contacts c ON c.contact_id = e.contact_id
         WHERE c.lead_list_name = il.name
           AND e.status = 'completed'
    ) AS completed_via_join,
    (
        SELECT COUNT(*)
          FROM enrichments e
         WHERE e.lead_list_name = il.name
           AND e.status = 'completed'
    ) AS completed_via_denorm,
    (
        SELECT COUNT(*)
          FROM enrichments e
         WHERE e.lead_list_name = il.name
    ) AS denorm_rows_for_list
  FROM import_lists il
  ORDER BY il.created_at DESC
  LIMIT 10;

-- 3. What the RPC the API actually calls is returning right now.
--    If this is empty or all zeros, the API's stats endpoint will
--    return zeros too — and the spinner will resolve to 0/0/total.
SELECT * FROM public.get_list_enrichment_stats()
 ORDER BY total_count DESC
 LIMIT 10;

-- 4. PostgREST schema cache freshness check. If the function exists
--    in pg_proc but PostgREST still doesn't see it, the API call will
--    fall through to the fallback path and you'll see "Approximate
--    stats" in the UI banner.
SELECT proname, pg_get_function_arguments(oid) AS args
  FROM pg_proc
 WHERE proname IN ('get_list_enrichment_stats', 'enrichments_set_lead_list_name', 'rename_import_list');
