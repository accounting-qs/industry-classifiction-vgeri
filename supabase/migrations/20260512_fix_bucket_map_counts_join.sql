-- =====================================================================
-- Fix get_bucket_map_counts JOIN key — preview was massively under-counting
--
-- Phase 1a writes one bucket_industry_map row per distinct industry. The
-- industry_string it stores comes from enrichments.classification (per
-- get_industry_vocabulary in 20260504_vocab_rpc_perf.sql, line 48):
--
--     COALESCE(NULLIF(TRIM(e.classification), ''), 'Scrape Error') AS industry
--
-- ...but the original get_bucket_map_counts (20260423_bucketing.sql) joins
-- bucket_industry_map.industry_string against contacts.industry — which is
-- the raw imported field, NOT the AI-classified field. For most contacts
-- those two columns hold different strings, so the JOIN dropped rows and
-- the Review screen's per-bucket contact counts were under-reported.
--
-- Phase 1b's in-memory routing already does this right (see
-- bucketingService.ts ~line 2752):
--     const industryKey = (c.classification || c.industry || '').trim();
--
-- So the post-assign Results numbers were always correct; only the
-- pre-assign preview was lying. This migration aligns the SQL JOIN with
-- the same precedence so the preview matches what Phase 1b will actually
-- produce.
--
-- Idempotent: CREATE OR REPLACE.
-- =====================================================================

CREATE OR REPLACE FUNCTION public.get_bucket_map_counts(p_run_id UUID)
RETURNS TABLE (
    bucket_name     TEXT,
    contact_count   BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '120s'
AS $$
    -- Resolve each contact's industry the same way Phase 1b does:
    -- enrichments.classification first, contacts.industry as fallback.
    -- A CTE keeps the COALESCE out of the join predicate so the planner
    -- can hash-join cleanly against bucket_industry_map.
    WITH contact_industry AS (
        SELECT
            c.contact_id,
            COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_str
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE c.lead_list_name = ANY(
            SELECT UNNEST(list_names) FROM bucketing_runs WHERE id = p_run_id
        )
    )
    SELECT
        m.bucket_name,
        COUNT(*)::BIGINT AS contact_count
    FROM contact_industry ci
    JOIN bucket_industry_map m ON m.industry_string = ci.industry_str
    WHERE m.bucketing_run_id = p_run_id
    GROUP BY m.bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_map_counts(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
