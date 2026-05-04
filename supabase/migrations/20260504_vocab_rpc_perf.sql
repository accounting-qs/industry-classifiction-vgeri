-- v4.2: get_industry_vocabulary perf fix.
--
-- The RPC was timing out at 60s on lists with many distinct industries
-- because two of its return columns forced expensive in-memory work:
--
--   sample_reasoning - ARRAY_AGG of long reasoning strings ordered by
--                      confidence. Materialised every reasoning string
--                      for every (industry, status) group, sorted, then
--                      sliced [1:2]. Memory + CPU heavy, and the column
--                      isn't read by anything (per user policy: only
--                      enrichments.classification feeds the LLM).
--
--   sample_companies - ARRAY_AGG with ORDER BY confidence. Same problem
--                      at smaller scale. We only need 3 sample names
--                      for the Sonnet prompt; the order doesn't matter.
--                      Without ORDER BY, Postgres can stream.
--
-- Also bumps statement_timeout 60s → 300s as a safety net for very
-- large lists (1M+ contacts).

DROP FUNCTION IF EXISTS public.get_industry_vocabulary(TEXT[], INTEGER);

CREATE FUNCTION public.get_industry_vocabulary(
    p_list_names TEXT[],
    p_limit INTEGER DEFAULT 10000
)
RETURNS TABLE (
    industry TEXT,
    n BIGINT,
    enrichment_status TEXT,
    sample_companies TEXT[]
)
LANGUAGE sql
STABLE
SET statement_timeout TO '300s'
AS $$
    SELECT industry,
           n,
           enrichment_status,
           sample_companies
    FROM (
        SELECT industry,
               COUNT(*)::BIGINT AS n,
               enrichment_status,
               (ARRAY_AGG(company_name) FILTER (WHERE company_name IS NOT NULL))[1:3] AS sample_companies
        FROM (
            SELECT
                COALESCE(NULLIF(TRIM(e.classification), ''), 'Scrape Error') AS industry,
                CASE
                    WHEN e.classification IS NULL OR TRIM(e.classification) = '' THEN 'unenriched'
                    WHEN e.classification ILIKE 'scrape error%' OR e.classification ILIKE 'site error%' THEN 'scrape_error'
                    WHEN e.status = 'failed' THEN 'failed'
                    WHEN e.status = 'completed' THEN 'completed'
                    ELSE 'pending'
                END AS enrichment_status,
                c.company_name
            FROM contacts c
            LEFT JOIN enrichments e ON e.contact_id = c.contact_id
            WHERE c.lead_list_name = ANY(p_list_names)
        ) sub
        GROUP BY industry, enrichment_status
        ORDER BY n DESC
        LIMIT p_limit
    ) outer_q;
$$;

GRANT EXECUTE ON FUNCTION public.get_industry_vocabulary(TEXT[], INTEGER)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
