-- Defense in depth against duplicate open job_items.
--
-- Today nothing prevents the same contact_id from sitting in two open
-- job_items rows under two different jobs. Two near-simultaneous clicks
-- of Resume, a double-click during a backoff window, or two concurrent
-- /api/enrich resolutions can all produce this state — already observed
-- in prod (6 contacts each had three rows after the FIFO-drain bug).
-- Duplicates cause N× scrape cost, N× AI cost, and the per-list "X
-- pending" badge keeps showing the higher of the two counts.
--
-- A partial UNIQUE index on (contact_id) WHERE status IN open-statuses
-- makes "two open rows for the same contact" a hard DB error rather
-- than a silent bug. Combined with the resolve-RPC pre-filter below,
-- the partial index is mostly a safety net — it should never be hit in
-- normal operation.

CREATE UNIQUE INDEX IF NOT EXISTS job_items_open_contact_uniq
    ON job_items (contact_id)
    WHERE status IN ('pending', 'retrying', 'processing');


-- Update resolve_enrichment_targets v4: exclude contacts that already
-- have at least one open job_item under a still-live parent job. This
-- is the upstream guard — Resume mode (status='new') no longer returns
-- contacts that are currently mid-enrichment under another job. The
-- partial UNIQUE index above is the defense-in-depth backstop.
--
-- "Open under a live parent" = job_items.status IN (pending, retrying,
-- processing) AND jobs.status IN (pending, processing). A contact
-- whose only open row is an orphan under a terminal parent SHOULD be
-- re-resolved — the orphan will be reaped within 60s. By the time the
-- new chunk insert lands, the partial unique slot is free.

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
        -- Status = "new" only
        SELECT array_agg(c.contact_id ORDER BY c.id)
        INTO result
        FROM contacts c
        WHERE NOT EXISTS (SELECT 1 FROM enrichments e WHERE e.contact_id = c.contact_id)
        AND NOT EXISTS (
            SELECT 1 FROM job_items ji
            JOIN jobs j ON j.id = ji.job_id
            WHERE ji.contact_id = c.contact_id
              AND ji.status IN ('pending', 'retrying', 'processing')
              AND j.status IN ('pending', 'processing')
        )
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
        AND NOT EXISTS (
            SELECT 1 FROM job_items ji
            JOIN jobs j ON j.id = ji.job_id
            WHERE ji.contact_id = c.contact_id
              AND ji.status IN ('pending', 'retrying', 'processing')
              AND j.status IN ('pending', 'processing')
        )
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
        AND NOT EXISTS (
            SELECT 1 FROM job_items ji
            JOIN jobs j ON j.id = ji.job_id
            WHERE ji.contact_id = c.contact_id
              AND ji.status IN ('pending', 'retrying', 'processing')
              AND j.status IN ('pending', 'processing')
        )
        AND (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
        AND (p_search IS NULL OR p_search = '' OR
             c.first_name ILIKE '%' || p_search || '%' OR
             c.last_name ILIKE '%' || p_search || '%' OR
             c.email ILIKE '%' || p_search || '%' OR
             c.company_website ILIKE '%' || p_search || '%' OR
             c.company_name ILIKE '%' || p_search || '%');

    ELSE
        -- No status filter (Re-enrich mode). Still de-dup against open
        -- jobs so a Re-enrich click during a backoff window doesn't
        -- queue a second copy of every contact.
        SELECT array_agg(c.contact_id ORDER BY c.id)
        INTO result
        FROM contacts c
        WHERE NOT EXISTS (
            SELECT 1 FROM job_items ji
            JOIN jobs j ON j.id = ji.job_id
            WHERE ji.contact_id = c.contact_id
              AND ji.status IN ('pending', 'retrying', 'processing')
              AND j.status IN ('pending', 'processing')
        )
        AND (p_lead_list_names IS NULL OR c.lead_list_name = ANY(p_lead_list_names))
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
