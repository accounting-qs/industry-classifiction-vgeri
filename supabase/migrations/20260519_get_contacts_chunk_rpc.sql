-- get_contacts_chunk: keyset-paginated contact fetch with a 300s
-- statement_timeout, so Phase 1b's streaming routing isn't capped by the
-- service_role HTTP path's tight default (we've seen it cancel at ~3s on
-- 293K-row runs, killing /assign on chunk 1 before any progress).
--
-- Same shape as the direct PostgREST call in fetchContactsChunk:
--   - filter on lead_list_name = ANY(...)
--   - keyset cursor via contact_id > p_last_id (or whole range when null)
--   - ORDER BY contact_id ASC, LIMIT p_limit
--
-- The (lead_list_name, contact_id) composite index from
-- 20260423_contacts_list_contactid_composite.sql gives us O(log N) range
-- scans on this exact predicate.

CREATE OR REPLACE FUNCTION public.get_contacts_chunk(
    p_list_names TEXT[],
    p_last_id    TEXT DEFAULT NULL,
    p_limit      INTEGER DEFAULT 1000
)
RETURNS TABLE (
    contact_id       TEXT,
    company_name     TEXT,
    company_website  TEXT,
    industry         TEXT,
    lead_list_name   TEXT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '300s'
AS $$
    SELECT
        c.contact_id::TEXT,
        c.company_name,
        c.company_website,
        c.industry,
        c.lead_list_name
    FROM contacts c
    WHERE c.lead_list_name = ANY(p_list_names)
      AND (p_last_id IS NULL OR c.contact_id::TEXT > p_last_id)
    ORDER BY c.contact_id ASC
    LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION public.get_contacts_chunk(TEXT[], TEXT, INTEGER)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
