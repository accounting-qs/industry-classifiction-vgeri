-- Per-proposed-tag contact counts.
--
-- The AI-Proposed panel currently shows usage counts in terms of how many
-- distinct industry strings hold a proposal ("3× under Agency"). What the
-- user actually needs to decide accept/reject is contact-level: "this
-- proposal covers 1,247 contacts — worth keeping" vs "covers 4 contacts —
-- not worth a library entry".
--
-- This RPC joins each is_new_* proposal back through the same
-- COALESCE(e.classification, c.industry) JOIN that powers
-- get_assigned_bucket_counts, so the contact counts match what Phase 1b
-- would see. Returns one row per (layer, name) where layer ∈
-- {identity, sub_identity, sector}.
--
-- statement_timeout=300s — joins ~293k contacts with ~160k industry rows.

CREATE OR REPLACE FUNCTION public.get_proposed_tag_contact_counts(p_run_id UUID)
RETURNS TABLE (
    layer            TEXT,
    name             TEXT,
    parent_identity  TEXT,
    industry_count   BIGINT,
    contact_count    BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '300s'
AS $$
    WITH list_names_for_run AS (
        SELECT UNNEST(list_names) AS list_name FROM bucketing_runs WHERE id = p_run_id
    ),
    ci AS (
        SELECT
            c.contact_id,
            COALESCE(NULLIF(TRIM(e.classification), ''), c.industry) AS industry_str
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE c.lead_list_name IN (SELECT list_name FROM list_names_for_run)
    ),
    -- For each new tag layer, group bucket_industry_map rows by the
    -- proposed name. industry_count is how many distinct industry rows
    -- carry the proposal; contact_count joins back to contacts via the
    -- industry text to give the user the "worth keeping" number.
    proposed_identities AS (
        SELECT 'identity'::TEXT AS layer,
               m.primary_identity AS name,
               NULL::TEXT AS parent_identity,
               COUNT(*)::BIGINT AS industry_count,
               COUNT(DISTINCT ci.contact_id)::BIGINT AS contact_count
        FROM bucket_industry_map m
        LEFT JOIN ci ON ci.industry_str = m.industry_string
        WHERE m.bucketing_run_id = p_run_id
          AND m.is_new_identity = TRUE
          AND m.primary_identity IS NOT NULL
        GROUP BY m.primary_identity
    ),
    proposed_sub_identities AS (
        SELECT 'sub_identity'::TEXT AS layer,
               m.sub_identity AS name,
               -- Pick the most-frequent parent identity for this sub-identity
               -- (the LLM should pin one but defends against drift).
               (SELECT primary_identity FROM bucket_industry_map m2
                WHERE m2.bucketing_run_id = p_run_id
                  AND m2.sub_identity = m.sub_identity
                  AND m2.primary_identity IS NOT NULL
                GROUP BY m2.primary_identity
                ORDER BY COUNT(*) DESC NULLS LAST LIMIT 1) AS parent_identity,
               COUNT(*)::BIGINT AS industry_count,
               COUNT(DISTINCT ci.contact_id)::BIGINT AS contact_count
        FROM bucket_industry_map m
        LEFT JOIN ci ON ci.industry_str = m.industry_string
        WHERE m.bucketing_run_id = p_run_id
          AND m.is_new_sub_identity = TRUE
          AND m.sub_identity IS NOT NULL
        GROUP BY m.sub_identity
    ),
    proposed_sectors AS (
        SELECT 'sector'::TEXT AS layer,
               m.sector AS name,
               NULL::TEXT AS parent_identity,
               COUNT(*)::BIGINT AS industry_count,
               COUNT(DISTINCT ci.contact_id)::BIGINT AS contact_count
        FROM bucket_industry_map m
        LEFT JOIN ci ON ci.industry_str = m.industry_string
        WHERE m.bucketing_run_id = p_run_id
          AND m.is_new_sector = TRUE
          AND m.sector IS NOT NULL
        GROUP BY m.sector
    )
    SELECT * FROM proposed_identities
    UNION ALL
    SELECT * FROM proposed_sub_identities
    UNION ALL
    SELECT * FROM proposed_sectors
    ORDER BY contact_count DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_proposed_tag_contact_counts(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
