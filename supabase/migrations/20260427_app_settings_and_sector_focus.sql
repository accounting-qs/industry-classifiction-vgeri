-- Two adjacent additions, kept together because they ship in the same release:
--
-- 1) app_settings: a tiny KV table for runtime-configurable secrets/options
--    that we don't want hard-coded in env vars. Today: the Anthropic API key
--    that powers Phase 1a discovery. Future: any other UI-managed connector.
--
-- 2) sector_focus column on bucket_industry_map and bucket_assignments. The
--    Phase 1b prompt now classifies by core business identity FIRST and
--    sector served SECOND, storing the sector served as a separate piece of
--    metadata so it can drive copy ("…private equity firms focused on
--    healthcare…") without hijacking the primary bucket.

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE app_settings DISABLE ROW LEVEL SECURITY;

ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS sector_focus TEXT;
ALTER TABLE bucket_assignments  ADD COLUMN IF NOT EXISTS sector_focus TEXT;

-- Update fanout to copy sector_focus into bucket_assignments alongside the chain.
CREATE OR REPLACE FUNCTION public.bucketing_deterministic_fanout(p_run_id UUID)
RETURNS BIGINT
LANGUAGE plpgsql
SET statement_timeout TO '180s'
AS $$
DECLARE
    inserted BIGINT;
    v_list_names TEXT[];
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'bucketing run % not found', p_run_id;
    END IF;

    INSERT INTO bucket_assignments (
        bucketing_run_id, contact_id, bucket_name, source, confidence,
        bucket_leaf, bucket_ancestor, bucket_root, sector_focus,
        is_generic, is_disqualified
    )
    SELECT
        p_run_id,
        c.contact_id,
        m.bucket_name,
        CASE
            WHEN m.source IN ('llm_phase1', 'manual') THEN 'deterministic'
            ELSE m.source
        END,
        COALESCE(m.confidence, 1.0),
        m.bucket_leaf,
        m.bucket_ancestor,
        m.bucket_root,
        m.sector_focus,
        COALESCE(m.is_generic, false),
        COALESCE(m.is_disqualified, false)
    FROM contacts c
    JOIN bucket_industry_map m
      ON m.industry_string = c.industry
     AND m.bucketing_run_id = p_run_id
    WHERE c.lead_list_name = ANY(v_list_names)
    ON CONFLICT (bucketing_run_id, contact_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    RETURN inserted;
END;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_deterministic_fanout(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
