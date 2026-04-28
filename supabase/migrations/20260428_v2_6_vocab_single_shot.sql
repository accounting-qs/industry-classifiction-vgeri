-- v2.6: get_industry_vocabulary takes an optional p_limit and runs with a
-- generous 300s statement_timeout, so the service can fetch the full
-- vocabulary in ONE call instead of paginating via PostgREST .range() —
-- which re-executed the entire JOIN+GROUP+aggregation on every page and
-- caused multi-minute hangs on 100k+ long-tail lists.
--
-- Idempotent (CREATE OR REPLACE).

CREATE OR REPLACE FUNCTION public.get_industry_vocabulary(
    p_list_names TEXT[],
    p_limit INTEGER DEFAULT NULL
)
RETURNS TABLE (
    industry         TEXT,
    n                BIGINT,
    avg_conf         NUMERIC,
    sample_companies TEXT[],
    sample_reasoning TEXT[]
)
LANGUAGE sql
STABLE
SET statement_timeout TO '300s'
AS $$
    SELECT
        c.industry,
        COUNT(*)::BIGINT                                                     AS n,
        AVG(e.confidence)::NUMERIC                                           AS avg_conf,
        (ARRAY_AGG(c.company_name   ORDER BY e.confidence DESC NULLS LAST))[1:3] AS sample_companies,
        (ARRAY_AGG(e.reasoning      ORDER BY e.confidence DESC NULLS LAST))[1:2] AS sample_reasoning
    FROM contacts c
    JOIN enrichments e ON e.contact_id = c.contact_id
    WHERE c.lead_list_name = ANY(p_list_names)
      AND e.status = 'completed'
      AND c.industry IS NOT NULL
      AND c.industry <> ''
    GROUP BY c.industry
    ORDER BY n DESC
    LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION public.get_industry_vocabulary(TEXT[], INTEGER)
    TO anon, authenticated, service_role;

-- The previous one-arg version is also dropped to keep the API clean.
-- (Idempotent: only drops if it still exists.)
DROP FUNCTION IF EXISTS public.get_industry_vocabulary(TEXT[]);

NOTIFY pgrst, 'reload schema';
