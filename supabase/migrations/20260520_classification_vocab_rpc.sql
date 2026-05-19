-- Phase 1a v2: classification-keyed vocabulary.
--
-- The old get_industry_vocabulary groups on contacts.industry (raw imported
-- field) AND returned SETOF rows, which Supabase's PostgREST silently caps
-- at db-max-rows (1000 on this project) regardless of LIMIT or Range. The
-- effect was Phase 1a only ever saw the top-1000 most frequent industries
-- and the long tail (78% of contacts on a 293k run) never got tagged.
--
-- v2 fixes both at once:
--   1. Groups on COALESCE(NULLIF(TRIM(e.classification),''), c.industry) —
--      the same expression used downstream by get_assigned_bucket_counts —
--      so Phase 1a's vocab matches the string Phase 1b's JOIN looks up.
--   2. Returns a single JSONB array, not SETOF rows. Scalar return values
--      bypass db-max-rows entirely, so the full vocab (≥160k entries on a
--      large run) comes back in one response.
--
-- statement_timeout=300s. Caller is responsible for picking a safe p_limit
-- via BUCKETING_VOCAB_HARD_LIMIT; passing NULL means "no limit".

CREATE OR REPLACE FUNCTION public.get_classification_vocabulary(
    p_list_names TEXT[],
    p_limit      INTEGER DEFAULT NULL
)
RETURNS JSONB
LANGUAGE sql
STABLE
SET statement_timeout TO '300s'
AS $$
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'classification',    classification,
            'enrichment_status', enrichment_status,
            'n',                 n,
            'sample_companies',  sample_companies,
            'sample_reasoning',  sample_reasoning
        )
        ORDER BY n DESC
    ), '[]'::jsonb)
    FROM (
        WITH ci AS (
            SELECT
                c.contact_id,
                c.company_name,
                COALESCE(e.status, 'missing')                                AS enrichment_status,
                COALESCE(NULLIF(TRIM(e.classification), ''), c.industry)     AS classification,
                e.confidence,
                e.reasoning
            FROM contacts c
            LEFT JOIN enrichments e ON e.contact_id = c.contact_id
            WHERE c.lead_list_name = ANY(p_list_names)
        )
        SELECT
            ci.classification,
            ci.enrichment_status,
            COUNT(*)::BIGINT                                                          AS n,
            (ARRAY_AGG(ci.company_name ORDER BY ci.confidence DESC NULLS LAST))[1:3]  AS sample_companies,
            (ARRAY_AGG(ci.reasoning    ORDER BY ci.confidence DESC NULLS LAST))[1:2]  AS sample_reasoning
        FROM ci
        WHERE ci.classification IS NOT NULL AND ci.classification <> ''
        GROUP BY ci.classification, ci.enrichment_status
        ORDER BY n DESC
        LIMIT p_limit
    ) sub;
$$;

GRANT EXECUTE ON FUNCTION public.get_classification_vocabulary(TEXT[], INTEGER)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
