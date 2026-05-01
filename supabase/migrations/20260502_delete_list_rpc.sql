-- Atomic per-page deletion of (enrichments + contacts) for an import list.
--
-- Why an RPC: the previous Node-side loop did
--   .from('contacts').select('contact_id').eq(...).limit(1000)
-- then
--   .from('enrichments').delete().in('contact_id', ids)
--   .from('contacts').delete().in('contact_id', ids)
-- which sends ~40 KB of UUIDs in the URL — past PostgREST's default
-- request-line cap. Lists where the first page returned the full 1,000
-- IDs failed with HTTP 400 Bad Request before any row was deleted, so
-- contactsDeleted stuck at 0 (visible in the delete-jobs network panel).
--
-- The RPC keeps the contact-id list inside Postgres. One round trip per
-- page, no URL bloat, atomic per chunk. Returns the per-table counts so
-- the Node-side runDeleteJob can keep updating progress.
CREATE OR REPLACE FUNCTION public.delete_import_list_page(
    p_list_name TEXT,
    p_limit INTEGER DEFAULT 5000
)
RETURNS TABLE (contacts_deleted BIGINT, enrichments_deleted BIGINT)
LANGUAGE plpgsql
SET statement_timeout TO '120s'
AS $$
DECLARE
    v_contacts_deleted BIGINT;
    v_enrichments_deleted BIGINT;
BEGIN
    WITH page AS (
        SELECT contact_id
          FROM contacts
         WHERE lead_list_name = p_list_name
         LIMIT p_limit
    ),
    e_del AS (
        DELETE FROM enrichments
         WHERE contact_id IN (SELECT contact_id FROM page)
        RETURNING 1
    ),
    c_del AS (
        DELETE FROM contacts
         WHERE contact_id IN (SELECT contact_id FROM page)
        RETURNING 1
    )
    SELECT
        (SELECT COUNT(*) FROM c_del),
        (SELECT COUNT(*) FROM e_del)
      INTO v_contacts_deleted, v_enrichments_deleted;

    contacts_deleted := v_contacts_deleted;
    enrichments_deleted := v_enrichments_deleted;
    RETURN NEXT;
END;
$$;

GRANT EXECUTE ON FUNCTION public.delete_import_list_page(TEXT, INTEGER)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
