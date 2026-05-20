-- Inherit Phase 1a per-industry tags from previous successful runs.
--
-- User-defined behavior: each bucketing run owns its own per-contact
-- view, but per-INDUSTRY tags are a property of the industry_string
-- itself. If a previous run already paid the LLM cost to tag exactly
-- the same industry_string, the new run should copy the tag instead of
-- re-tagging. This is byte-exact string matching only — no fuzzy /
-- semantic merging.
--
-- Runs automatically at Phase 1a startup, before the LLM tagger loop.
-- Idempotent via ON CONFLICT DO NOTHING — re-running is safe.
--
-- Source preference: most-recently created run wins via DISTINCT ON +
-- ORDER BY br.created_at DESC. We also exclude any row whose
-- primary_identity is NULL (general_passthrough rows for scrape errors
-- and the like are run-specific and shouldn't propagate). Inherited
-- rows are marked source='inherited_phase1a' so they're distinguishable
-- from llm_phase1a (paid this run) in audits.

CREATE OR REPLACE FUNCTION public.inherit_phase1a_tags(
    p_new_run_id UUID
)
RETURNS JSONB
LANGUAGE plpgsql
VOLATILE
SET statement_timeout TO '300s'
AS $$
DECLARE
    v_list_names  TEXT[];
    v_inherited   BIGINT;
    v_candidates  BIGINT;
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_new_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'inherit_phase1a_tags: run % not found', p_new_run_id;
    END IF;

    -- Distinct industry_strings the new run will encounter.
    WITH new_vocab AS (
        SELECT DISTINCT COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_string
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE c.lead_list_name = ANY(v_list_names)
          AND COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) IS NOT NULL
    ),
    -- Best (most recent successful tag) per industry_string from any OTHER run.
    inheritable AS (
        SELECT DISTINCT ON (bim.industry_string)
            bim.industry_string,
            bim.primary_identity,
            bim.sub_identity,
            bim.sector,
            bim.bucket_name,
            bim.confidence,
            bim.identity_confidence,
            bim.sub_identity_confidence,
            bim.sector_confidence,
            bim.is_disqualified,
            bim.is_generic,
            bim.canonical_classification,
            bim.llm_reason,
            bim.raw_industry
        FROM bucket_industry_map bim
        JOIN bucketing_runs br ON br.id = bim.bucketing_run_id
        WHERE bim.bucketing_run_id <> p_new_run_id
          AND bim.primary_identity IS NOT NULL
          AND br.status IN ('taxonomy_ready', 'completed')
          AND bim.industry_string IN (SELECT industry_string FROM new_vocab)
        ORDER BY bim.industry_string, br.created_at DESC
    )
    INSERT INTO bucket_industry_map (
        bucketing_run_id, industry_string,
        primary_identity, sub_identity, sector,
        bucket_name, source, confidence,
        identity_confidence, sub_identity_confidence, sector_confidence,
        is_disqualified, is_generic, needs_qa,
        canonical_classification, llm_reason, raw_industry,
        is_new_identity, is_new_sub_identity, is_new_sector
    )
    SELECT
        p_new_run_id, industry_string,
        primary_identity, sub_identity, sector,
        bucket_name, 'inherited_phase1a', confidence,
        identity_confidence, sub_identity_confidence, sector_confidence,
        is_disqualified, is_generic, false,
        canonical_classification, llm_reason, raw_industry,
        false, false, false
    FROM inheritable
    ON CONFLICT (bucketing_run_id, industry_string) DO NOTHING;

    GET DIAGNOSTICS v_inherited = ROW_COUNT;

    SELECT COUNT(*) INTO v_candidates FROM (
        SELECT DISTINCT COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_string
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE c.lead_list_name = ANY(v_list_names)
          AND COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) IS NOT NULL
    ) v;

    RETURN jsonb_build_object(
        'inherited_rows',      v_inherited,
        'distinct_industries', v_candidates
    );
END;
$$;

GRANT EXECUTE ON FUNCTION public.inherit_phase1a_tags(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
