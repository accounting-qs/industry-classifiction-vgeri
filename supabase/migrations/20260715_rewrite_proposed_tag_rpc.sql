-- Volume-safe rewrite of a run's orphan proposal rows in bucket_industry_map.
--
-- Two API paths rewrite bucket_industry_map when a user resolves an
-- AI-proposed tag on the Bucketing review screen:
--
--   1. Accept-with-edit  — POST /api/bucketing/taxonomy/:kind  (with run_id).
--      Saves the proposal to the taxonomy library, then clears the orphan
--      flag on every row in the run that carries that proposed name (and,
--      for a sub-identity, rewrites primary_identity to keep the pair
--      consistent).
--   2. Route-to-existing — POST /api/bucketing/runs/:runId/proposed-tags/:layer/remap.
--      Re-points the run's orphan rows at an existing library entry —
--      same-layer, or cross-layer identity -> sub-identity.
--
-- Both previously issued a PostgREST `UPDATE ... .select()` straight against
-- bucket_industry_map. That runs under the caller role's statement_timeout,
-- and on a large run (100k-200k+ distinct industry strings in the partition)
-- the UPDATE + RETURNING trips that cap partway through — surfaced to the user
-- as "library row saved but run rewrite failed: canceling statement due to
-- statement timeout". The library row committed but the run rewrite didn't, so
-- the proposal kept surfacing as pending and the orphan rows stayed orphaned.
--
-- This RPC does the identical write set-based in ONE server-side statement with
-- statement_timeout raised to 600s (same pattern as finalize_taxonomy_candidates
-- and finalize_per_contact_taxonomy). It returns the number of rows rewritten so
-- the endpoints can keep reporting `rewritten`.
--
-- The column / flag arguments come from a fixed 3-layer whitelist that lives
-- entirely server-side (TAXONOMY_LAYER_COLS in server.ts). The function
-- re-validates every one against that whitelist and RAISEs on anything
-- off-list, and the SET / WHERE targets are resolved with static CASE
-- expressions rather than dynamic SQL — so there is no injection surface.

CREATE OR REPLACE FUNCTION public.rewrite_proposed_tag(
    p_run_id          UUID,
    p_match_col       TEXT,           -- from-layer column: primary_identity | sub_identity | sector
    p_match_name      TEXT,           -- existing (proposed) name to match on
    p_match_flag      TEXT,           -- from-layer flag: is_new_identity | is_new_sub_identity | is_new_sector
    p_set_col         TEXT,           -- to-layer column (= match_col unless cross-layer)
    p_set_name        TEXT,           -- new name to write
    p_set_flag        TEXT,           -- to-layer flag to clear
    p_also_clear_flag TEXT DEFAULT NULL,  -- second flag to clear (cross-layer remap); NULL otherwise
    p_set_parent      TEXT DEFAULT NULL     -- primary_identity to write (sub-identity target); NULL otherwise
)
RETURNS BIGINT
LANGUAGE plpgsql
VOLATILE
SET statement_timeout TO '600s'
AS $$
DECLARE
    v_cols  CONSTANT TEXT[] := ARRAY['primary_identity', 'sub_identity', 'sector'];
    v_flags CONSTANT TEXT[] := ARRAY['is_new_identity', 'is_new_sub_identity', 'is_new_sector'];
    v_count BIGINT;
BEGIN
    -- Whitelist guard. Callers already draw these from TAXONOMY_LAYER_COLS,
    -- but re-check here so an off-list column can never reach the CASE
    -- expressions (where it would silently match nothing).
    IF NOT (p_match_col  = ANY(v_cols))  THEN RAISE EXCEPTION 'rewrite_proposed_tag: bad match_col "%"',  p_match_col;  END IF;
    IF NOT (p_set_col    = ANY(v_cols))  THEN RAISE EXCEPTION 'rewrite_proposed_tag: bad set_col "%"',    p_set_col;    END IF;
    IF NOT (p_match_flag = ANY(v_flags)) THEN RAISE EXCEPTION 'rewrite_proposed_tag: bad match_flag "%"', p_match_flag; END IF;
    IF NOT (p_set_flag   = ANY(v_flags)) THEN RAISE EXCEPTION 'rewrite_proposed_tag: bad set_flag "%"',   p_set_flag;   END IF;
    IF p_also_clear_flag IS NOT NULL AND NOT (p_also_clear_flag = ANY(v_flags)) THEN
        RAISE EXCEPTION 'rewrite_proposed_tag: bad also_clear_flag "%"', p_also_clear_flag;
    END IF;

    UPDATE public.bucket_industry_map AS m
    SET
        primary_identity = CASE
            WHEN p_set_col = 'primary_identity' THEN p_set_name
            WHEN p_set_parent IS NOT NULL       THEN p_set_parent
            ELSE m.primary_identity END,
        sub_identity = CASE
            WHEN p_set_col = 'sub_identity' THEN p_set_name
            ELSE m.sub_identity END,
        sector = CASE
            WHEN p_set_col = 'sector' THEN p_set_name
            ELSE m.sector END,
        is_new_identity = CASE
            WHEN 'is_new_identity'     IN (p_set_flag, p_also_clear_flag) THEN false
            ELSE m.is_new_identity END,
        is_new_sub_identity = CASE
            WHEN 'is_new_sub_identity' IN (p_set_flag, p_also_clear_flag) THEN false
            ELSE m.is_new_sub_identity END,
        is_new_sector = CASE
            WHEN 'is_new_sector'       IN (p_set_flag, p_also_clear_flag) THEN false
            ELSE m.is_new_sector END
    WHERE m.bucketing_run_id = p_run_id
      AND CASE p_match_col
            WHEN 'primary_identity' THEN m.primary_identity
            WHEN 'sub_identity'     THEN m.sub_identity
            WHEN 'sector'           THEN m.sector
          END = p_match_name
      AND CASE p_match_flag
            WHEN 'is_new_identity'     THEN m.is_new_identity
            WHEN 'is_new_sub_identity' THEN m.is_new_sub_identity
            WHEN 'is_new_sector'       THEN m.is_new_sector
          END IS TRUE;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.rewrite_proposed_tag(UUID, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
