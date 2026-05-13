-- Denormalize `lead_list_name` from contacts onto enrichments so the
-- per-list progress aggregate stops needing a join. The previous
-- `get_list_enrichment_stats` RPC did:
--
--   FROM contacts c LEFT JOIN enrichments e ON e.contact_id = c.contact_id
--   GROUP BY c.lead_list_name
--
-- which at ~600k rows on each side ran past the Supabase HTTP gateway
-- timeout (the function-level statement_timeout buys headroom on the
-- planner side but PostgREST still gives up at ~60s). Aggregating
-- enrichments alone, with an index on (lead_list_name, status), comes
-- back in well under a second.
--
-- Apply order: this whole file is idempotent — re-running it is safe.
-- The backfill UPDATE only touches rows where the new column is NULL.

-- 1. Column ----------------------------------------------------------

ALTER TABLE enrichments
    ADD COLUMN IF NOT EXISTS lead_list_name TEXT;

-- 2. Backfill from contacts -----------------------------------------
--
-- Bumped statement_timeout on this single statement because at the
-- current ~600k-row scale even an indexed UPDATE … FROM (contacts)
-- can run a few minutes. Wrapped in DO so the SET LOCAL is scoped to
-- the backfill and we don't bleed the timeout into anything that
-- runs after this migration in the same session.

DO $$
BEGIN
    SET LOCAL statement_timeout TO '600s';
    UPDATE enrichments e
       SET lead_list_name = c.lead_list_name
      FROM contacts c
     WHERE e.contact_id = c.contact_id
       AND e.lead_list_name IS NULL
       AND c.lead_list_name IS NOT NULL;
END$$;

-- 3. Index for the stats aggregate ----------------------------------
--
-- (lead_list_name, status) is the exact filter+grouping shape the RPC
-- uses. Postgres can satisfy `COUNT(*) FILTER (WHERE status = …)` from
-- this index alone (index-only scan) once the table is vacuumed.

CREATE INDEX IF NOT EXISTS idx_enrichments_list_status
    ON enrichments (lead_list_name, status);

-- 4. Auto-fill trigger ----------------------------------------------
--
-- The application layer (jobProcessor) sets lead_list_name on every
-- upsert, but this trigger is defense-in-depth: if any code path
-- forgets, we still derive the value from contacts at INSERT time so
-- the stats RPC stays accurate.

CREATE OR REPLACE FUNCTION public.enrichments_set_lead_list_name()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.lead_list_name IS NULL THEN
        SELECT c.lead_list_name INTO NEW.lead_list_name
          FROM contacts c
         WHERE c.contact_id = NEW.contact_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_enrichments_set_lead_list_name ON enrichments;
CREATE TRIGGER trg_enrichments_set_lead_list_name
BEFORE INSERT ON enrichments
FOR EACH ROW
EXECUTE FUNCTION public.enrichments_set_lead_list_name();

-- 5. Fast stats RPC -------------------------------------------------
--
-- Replaces the join-based version with a single GROUP BY over
-- enrichments, plus a LEFT JOIN onto import_lists so lists that have
-- zero enrichments still show up with 0/0/total counts (otherwise the
-- UI would just hide brand-new lists from the import history banner
-- until their first enrichment landed).
--
-- `total_count` deliberately comes from import_lists.contact_count
-- (which is set at import time) rather than COUNT(enrichments). A
-- contact without an enrichment row is still part of the list — it's
-- just pending — and the progress bar needs that denominator to
-- reflect the actual list size, not the work that's been started.

CREATE OR REPLACE FUNCTION public.get_list_enrichment_stats()
RETURNS TABLE(
    lead_list_name TEXT,
    completed_count BIGINT,
    failed_count BIGINT,
    total_count BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '30s'
AS $$
    WITH stats AS (
        SELECT
            e.lead_list_name,
            COUNT(*) FILTER (WHERE e.status = 'completed')::BIGINT AS completed_count,
            COUNT(*) FILTER (WHERE e.status = 'failed')::BIGINT    AS failed_count
        FROM enrichments e
        WHERE e.lead_list_name IS NOT NULL
        GROUP BY e.lead_list_name
    )
    SELECT
        il.name                              AS lead_list_name,
        COALESCE(s.completed_count, 0)       AS completed_count,
        COALESCE(s.failed_count, 0)          AS failed_count,
        COALESCE(il.contact_count, 0)::BIGINT AS total_count
    FROM import_lists il
    LEFT JOIN stats s ON s.lead_list_name = il.name;
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_stats()
    TO anon, authenticated, service_role;

-- 6. Cascade list renames into enrichments --------------------------
--
-- The original rename RPC only updated contacts.lead_list_name; now
-- that enrichments carries its own copy, the rename has to touch both
-- in the same transaction or the stats RPC will under-count the
-- renamed list until every enrichment row gets rewritten by some
-- other code path. statement_timeout is already lifted to 300s on
-- this function, which covers two indexed UPDATEs at the current
-- scale.

CREATE OR REPLACE FUNCTION public.rename_import_list(
    p_id UUID,
    p_new_name TEXT
)
RETURNS TABLE (
    list_id UUID,
    old_name TEXT,
    new_name TEXT,
    contacts_updated BIGINT
)
LANGUAGE plpgsql
SET statement_timeout TO '300s'
AS $$
DECLARE
    v_old_name TEXT;
    v_clash UUID;
    v_updated BIGINT;
BEGIN
    IF p_id IS NULL THEN
        RAISE EXCEPTION 'p_id is required';
    END IF;
    IF p_new_name IS NULL OR length(btrim(p_new_name)) = 0 THEN
        RAISE EXCEPTION 'p_new_name is required';
    END IF;

    SELECT name INTO v_old_name FROM import_lists WHERE id = p_id FOR UPDATE;
    IF v_old_name IS NULL THEN
        RAISE EXCEPTION 'list % not found', p_id;
    END IF;

    IF v_old_name = p_new_name THEN
        RETURN QUERY SELECT p_id, v_old_name, v_old_name, 0::BIGINT;
        RETURN;
    END IF;

    SELECT id INTO v_clash FROM import_lists WHERE name = p_new_name AND id <> p_id LIMIT 1;
    IF v_clash IS NOT NULL THEN
        RAISE EXCEPTION 'duplicate_list_name: a list named "%" already exists', p_new_name
            USING ERRCODE = 'unique_violation';
    END IF;

    UPDATE import_lists SET name = p_new_name WHERE id = p_id;

    UPDATE contacts SET lead_list_name = p_new_name WHERE lead_list_name = v_old_name;
    GET DIAGNOSTICS v_updated = ROW_COUNT;

    UPDATE enrichments SET lead_list_name = p_new_name WHERE lead_list_name = v_old_name;

    RETURN QUERY SELECT p_id, v_old_name, p_new_name, v_updated;
END;
$$;

-- 7. Let PostgREST pick up the new function signatures --------------

NOTIFY pgrst, 'reload schema';
