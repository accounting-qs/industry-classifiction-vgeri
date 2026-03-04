-- ============================================
-- resolve_enrichment_targets: Server-side contact ID resolution
-- Runs entirely in the database with 120s timeout.
-- No PostgREST row limits or statement timeout issues.
-- ============================================

CREATE OR REPLACE FUNCTION resolve_enrichment_targets(
    p_lead_list_names TEXT[] DEFAULT NULL,
    p_statuses TEXT[] DEFAULT NULL,
    p_search TEXT DEFAULT NULL
)
RETURNS TABLE(cid UUID) AS $$
DECLARE
    has_new BOOLEAN := ('new' = ANY(COALESCE(p_statuses, ARRAY[]::TEXT[])));
    other_statuses TEXT[] := ARRAY(
        SELECT unnest(COALESCE(p_statuses, ARRAY[]::TEXT[]))
        EXCEPT SELECT 'new'
    );
    status_filter_active BOOLEAN := (p_statuses IS NOT NULL AND array_length(p_statuses, 1) > 0);
BEGIN
    -- Set generous timeout for large queries
    SET LOCAL statement_timeout = '120s';

    IF status_filter_active AND has_new AND array_length(other_statuses, 1) IS NULL THEN
        -- Status = "new" only: contacts WITHOUT enrichment records
        RETURN QUERY
        SELECT c.contact_id
        FROM contacts c
        WHERE NOT EXISTS (SELECT 1 FROM enrichments e WHERE e.contact_id = c.contact_id)
        AND (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%')
        ORDER BY c.id;

    ELSIF status_filter_active AND has_new THEN
        -- Mixed: "new" + other statuses
        RETURN QUERY
        SELECT c.contact_id
        FROM contacts c
        LEFT JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE (e.contact_id IS NULL OR e.status = ANY(other_statuses))
        AND (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%')
        ORDER BY c.id;

    ELSIF status_filter_active THEN
        -- Non-new statuses only
        RETURN QUERY
        SELECT c.contact_id
        FROM contacts c
        INNER JOIN enrichments e ON e.contact_id = c.contact_id
        WHERE e.status = ANY(other_statuses)
        AND (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%')
        ORDER BY c.id;

    ELSE
        -- No status filter
        RETURN QUERY
        SELECT c.contact_id
        FROM contacts c
        WHERE (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%')
        ORDER BY c.id;
    END IF;
END;
$$ LANGUAGE plpgsql;
