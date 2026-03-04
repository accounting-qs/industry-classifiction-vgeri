-- ============================================
-- MUST DROP the old version first because the return type changed
-- from TABLE(cid UUID) → UUID[]
-- ============================================
DROP FUNCTION IF EXISTS resolve_enrichment_targets(TEXT[], TEXT[], TEXT);

-- ============================================
-- resolve_enrichment_targets v2: Returns UUID[] (single array value)
-- to bypass PostgREST row limits entirely.
-- Runs with 120s statement timeout for large datasets (70K+).
-- ============================================

CREATE OR REPLACE FUNCTION resolve_enrichment_targets(
    p_lead_list_names TEXT[] DEFAULT NULL,
    p_statuses TEXT[] DEFAULT NULL,
    p_search TEXT DEFAULT NULL
)
RETURNS UUID[] AS $$
DECLARE
    has_new BOOLEAN := ('new' = ANY(COALESCE(p_statuses, ARRAY[]::TEXT[])));
    other_statuses TEXT[] := ARRAY(
        SELECT unnest(COALESCE(p_statuses, ARRAY[]::TEXT[]))
        EXCEPT SELECT 'new'
    );
    status_filter_active BOOLEAN := (p_statuses IS NOT NULL AND array_length(p_statuses, 1) > 0);
    result UUID[];
BEGIN
    -- Set generous timeout for large queries (70K+ contacts)
    SET LOCAL statement_timeout = '120s';

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
$$ LANGUAGE plpgsql;
