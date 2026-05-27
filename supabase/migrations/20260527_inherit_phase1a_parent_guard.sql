-- Add parent-consistency guard to inherit_phase1a_tags.
--
-- The original RPC byte-copied (primary_identity, sub_identity) from prior
-- runs without checking whether the pairing is still valid against the
-- CURRENT taxonomy. After the 5/26 + 5/27 taxonomy reorganizations
-- (Healthcare split, Distribution & Wholesale promoted, sub re-parents,
-- Vertical SaaS / Managed IT Services deletions, several identity renames),
-- old runs hold stale (identity, sub) pairs:
--
--   * primary_identity points to a name that no longer exists in
--     taxonomy_identities (e.g. "Healthcare Operator" — renamed). Inheriting
--     it preserves a name the LLM can no longer disambiguate downstream.
--   * sub_identity exists in the library but its current parent_identity
--     differs from the inherited primary_identity (e.g. Software & SaaS +
--     Custom Software Development — Custom Software Development was moved
--     under IT Services on 5/26). Cross-parent pairs slipped past the
--     Review screen because they read as "library entries" rather than
--     proposals.
--
-- One observed run (e77124ba) inherited 47 such cross-parent pairs that the
-- fresh-LLM snapTaggingsToLibrary guard would have caught for fresh tags.
--
-- Fix: when selecting rows to inherit, validate against the current library.
--   - Drop inheritance entirely if primary_identity is no longer a live
--     identity (force re-tag rather than persist a stale name).
--   - Null out sub_identity if its current parent != primary_identity.
--   - Null out sub_identity if the sub no longer exists in the library.
-- Identity-level inheritance still saves the LLM cost; sub-level mismatches
-- get nulled so Phase 1b rolls them up to identity-level cleanly.

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
    ),
    -- Parent-consistency guard. Validate each inherited row against the
    -- CURRENT taxonomy library:
    --   * skip rows whose primary_identity is no longer a live identity
    --   * null sub_identity if cross-parent or no longer in library
    -- LEFT JOIN keeps rows that wouldn't otherwise match (sub_identity NULL
    -- or genuinely missing from library — these pass through unchanged
    -- because the guard cares about WRONG pairings, not missing data).
    guarded AS (
        SELECT
            i.industry_string,
            i.primary_identity,
            CASE
                WHEN i.sub_identity IS NULL OR i.sub_identity = '' THEN NULL
                WHEN s.parent_identity IS NULL THEN NULL  -- sub no longer exists
                WHEN s.parent_identity <> i.primary_identity THEN NULL  -- cross-parent
                ELSE i.sub_identity
            END AS sub_identity,
            -- If the inherited sub got nulled, drop its confidence too so it
            -- doesn't read as a high-confidence library hit downstream.
            CASE
                WHEN i.sub_identity IS NULL OR i.sub_identity = '' THEN i.sub_identity_confidence
                WHEN s.parent_identity IS NULL OR s.parent_identity <> i.primary_identity THEN 0
                ELSE i.sub_identity_confidence
            END AS sub_identity_confidence,
            i.sector,
            i.bucket_name,
            i.confidence,
            i.identity_confidence,
            i.sector_confidence,
            i.is_disqualified,
            i.is_generic,
            i.canonical_classification,
            i.llm_reason,
            i.raw_industry
        FROM inheritable i
        JOIN taxonomy_identities ti
          ON ti.name = i.primary_identity AND ti.archived = false
        LEFT JOIN taxonomy_sub_identities s
          ON s.name = i.sub_identity AND s.archived = false
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
    FROM guarded
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
