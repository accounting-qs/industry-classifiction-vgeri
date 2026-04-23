-- ============================================
-- resolve_enrichment_targets v3
--
-- Resolves the set of contact_ids matching (lead_list_names, statuses,
-- search) in one DB roundtrip and returns them as a UUID[] — bypassing
-- PostgREST's 1000-row page limit entirely. The caller (server.ts) then
-- inserts them into job_items in chunks.
--
-- v3 change: timeout is now declared as a function attribute
-- (`SET statement_timeout TO '120s'` on CREATE FUNCTION) instead of
-- `SET LOCAL` inside the body. The attribute form is applied on
-- function entry regardless of volatility and is more reliable than
-- `SET LOCAL`, which on the deployed DB was hitting PostgREST's 8s
-- default timeout before ever taking effect.
-- ============================================

DROP FUNCTION IF EXISTS resolve_enrichment_targets(TEXT[], TEXT[], TEXT);

CREATE OR REPLACE FUNCTION resolve_enrichment_targets(
    p_lead_list_names TEXT[] DEFAULT NULL,
    p_statuses TEXT[] DEFAULT NULL,
    p_search TEXT DEFAULT NULL
)
RETURNS UUID[]
LANGUAGE plpgsql
SET statement_timeout TO '120s'
AS $$
DECLARE
    has_new BOOLEAN := ('new' = ANY(COALESCE(p_statuses, ARRAY[]::TEXT[])));
    other_statuses TEXT[] := ARRAY(
        SELECT unnest(COALESCE(p_statuses, ARRAY[]::TEXT[]))
        EXCEPT SELECT 'new'
    );
    status_filter_active BOOLEAN := (p_statuses IS NOT NULL AND array_length(p_statuses, 1) > 0);
    result UUID[];
BEGIN
    IF status_filter_active AND has_new AND array_length(other_statuses, 1) IS NULL THEN
        -- Status = "new" only: contacts WITHOUT enrichment records
        SELECT array_agg(c.contact_id ORDER BY c.id)
        INTO result
        FROM contacts c
        WHERE NOT EXISTS (SELECT 1 FROM enrichments e WHERE e.contact_id = c.contact_id)
        AND (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%');

    ELSIF status_filter_active AND has_new THEN
        -- Mixed: "new" + other statuses
        SELECT array_agg(c.contact_id ORDER BY c.id)
        INTO result
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE (e.contact_id IS NULL OR e.status = ANY(other_statuses))
        AND (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%');

    ELSIF status_filter_active THEN
        -- Non-new statuses only
        SELECT array_agg(c.contact_id ORDER BY c.id)
        INTO result
        FROM contacts c
        INNER JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE e.status = ANY(other_statuses)
        AND (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%');

    ELSE
        -- No status filter
        SELECT array_agg(c.contact_id ORDER BY c.id)
        INTO result
        FROM contacts c
        WHERE (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%');
    END IF;

    RETURN COALESCE(result, ARRAY[]::UUID[]);
END;
$$;

GRANT EXECUTE ON FUNCTION resolve_enrichment_targets(TEXT[], TEXT[], TEXT)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
