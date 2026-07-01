-- Volume-safe read for the Finalize Taxonomy scan.
--
-- Finalize (finalizeTaxonomyAgainstLibrary) needs one full read of the run's
-- bucket_industry_map partition to (a) collect the "orphan" candidate rows —
-- source='llm_phase1a' with an is_new_* flag still set — for the deterministic
-- keep/null pass, and (b) accumulate the distinct taxonomy that non-candidate
-- rows already carry so it can re-synthesize taxonomy_proposal.
--
-- The old JS path did this by keyset-paginating the partition over PostgREST:
-- one request per 1000 rows. Every REST request runs under the calling role's
-- statement_timeout (~8s for anon/authenticated). On a large run (200k+
-- industry strings → 200+ page requests) the probability that ANY single page
-- trips that 8s cap under production load approaches 1 — observed as
-- "bucket_industry_map scan failed after N candidates: canceling statement due
-- to statement timeout" partway through the scan.
--
-- This RPC does the whole read set-based in ONE server-side statement with
-- statement_timeout raised to 600s (same pattern as
-- finalize_per_contact_taxonomy). It returns only:
--   • candidates      — the orphan rows the keep/null pass mutates (small: the
--                       rows carrying a not-yet-accepted proposed tag).
--   • used_identities — DISTINCT primary_identity over non-candidate rows.
--   • used_sub_pairs  — DISTINCT (identity, sub) over non-candidate rows.
--   • used_sectors    — DISTINCT sector over non-candidate rows.
--
-- The JS folds each candidate's POST-finalize taxonomy into the used-* sets in
-- its keep/null loop, exactly as the old per-row scan did, so taxonomy_proposal
-- re-synthesis is unchanged. Read-only: the deterministic keep/null UPDATE and
-- the per-contact explosion stay in their existing (proven) code paths.

CREATE OR REPLACE FUNCTION public.finalize_taxonomy_candidates(
    p_run_id UUID
)
RETURNS JSONB
LANGUAGE sql
STABLE
SET statement_timeout TO '600s'
AS $$
    WITH parts AS (
        SELECT
            industry_string,
            primary_identity,
            sub_identity,
            sector,
            is_new_identity,
            is_new_sub_identity,
            is_new_sector,
            is_disqualified,
            source,
            raw_industry,
            (source = 'llm_phase1a'
             AND (COALESCE(is_new_identity, false)
                  OR COALESCE(is_new_sub_identity, false)
                  OR COALESCE(is_new_sector, false))) AS is_candidate
        FROM public.bucket_industry_map
        WHERE bucketing_run_id = p_run_id
    )
    SELECT jsonb_build_object(
        'candidates', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'industry_string',    industry_string,
                'primary_identity',   primary_identity,
                'sub_identity',       sub_identity,
                'sector',             sector,
                'is_new_identity',    is_new_identity,
                'is_new_sub_identity',is_new_sub_identity,
                'is_new_sector',      is_new_sector,
                'is_disqualified',    is_disqualified,
                'source',             source,
                'raw_industry',       raw_industry
            ))
            FROM parts WHERE is_candidate
        ), '[]'::jsonb),
        'used_identities', COALESCE((
            SELECT jsonb_agg(DISTINCT primary_identity)
            FROM parts
            WHERE NOT is_candidate AND primary_identity IS NOT NULL
        ), '[]'::jsonb),
        'used_sub_pairs', COALESCE((
            SELECT jsonb_agg(DISTINCT jsonb_build_object(
                'identity', primary_identity,
                'sub',      sub_identity
            ))
            FROM parts
            WHERE NOT is_candidate
              AND primary_identity IS NOT NULL
              AND sub_identity IS NOT NULL
        ), '[]'::jsonb),
        'used_sectors', COALESCE((
            SELECT jsonb_agg(DISTINCT sector)
            FROM parts
            WHERE NOT is_candidate AND sector IS NOT NULL
        ), '[]'::jsonb)
    );
$$;

GRANT EXECUTE ON FUNCTION public.finalize_taxonomy_candidates(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
