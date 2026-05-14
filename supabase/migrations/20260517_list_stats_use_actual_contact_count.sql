-- Fix the inflated denominator in get_list_enrichment_stats.
--
-- import_lists.contact_count is the CSV row count submitted at upload
-- time. The import flow upserts contacts on `email`, so duplicates
-- inside a single CSV (or duplicates across earlier imports) collapse
-- — the actual number of contacts that end up in the table for a
-- given list can be meaningfully smaller than the CSV row count.
-- Example seen in prod: 116,968 CSV rows → 106,502 contacts; the UI
-- showed 91% "done" with 10,466 phantom-pending rows that don't
-- exist anywhere in the database.
--
-- Compute total_count from the contacts table directly. Falls back
-- to import_lists.contact_count only if there's no contacts row for
-- the list yet (mid-import state — the row appears in import_lists
-- the moment POST /api/import-lists lands, but the contacts INSERTs
-- stream in over the next few seconds for big files).

CREATE OR REPLACE FUNCTION public.get_list_enrichment_stats()
RETURNS TABLE(
    lead_list_name TEXT,
    completed_count BIGINT,
    failed_count BIGINT,
    total_count BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '30s'
AS $$
    WITH stats AS (
        SELECT
            e.lead_list_name,
            COUNT(*) FILTER (WHERE e.status = 'completed')::BIGINT AS completed_count,
            COUNT(*) FILTER (WHERE e.status = 'failed')::BIGINT    AS failed_count
        FROM enrichments e
        WHERE e.lead_list_name IS NOT NULL
        GROUP BY e.lead_list_name
    ),
    contact_counts AS (
        SELECT
            c.lead_list_name,
            COUNT(*)::BIGINT AS total_count
        FROM contacts c
        WHERE c.lead_list_name IS NOT NULL
        GROUP BY c.lead_list_name
    )
    SELECT
        il.name                                                        AS lead_list_name,
        COALESCE(s.completed_count, 0)                                 AS completed_count,
        COALESCE(s.failed_count, 0)                                    AS failed_count,
        -- Prefer the live contacts count (post-dedup). Only fall back
        -- to import_lists.contact_count when the contacts haven't
        -- landed yet (mid-import) so brand-new lists still show a
        -- non-zero denominator while their inserts stream in.
        COALESCE(cc.total_count, il.contact_count, 0)::BIGINT          AS total_count
    FROM import_lists il
    LEFT JOIN stats s ON s.lead_list_name = il.name
    LEFT JOIN contact_counts cc ON cc.lead_list_name = il.name;
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_stats()
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
