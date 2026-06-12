-- Replace the per-pageload full-table stats aggregate with a cached
-- snapshot table + explicit refresh RPC.
--
-- Why: /api/import-lists/stats called get_list_enrichment_stats() on
-- every 5s poll tick. At 2.6M enrichments + 3.15M contacts that
-- aggregate measures ~9s — so polls overlapped, stacked concurrent
-- 9s scans on the DB, and periodically blew the 30s statement_timeout.
-- The endpoint then fell back to PostgREST count=estimated numbers
-- whose totals come from import_lists.contact_count (CSV row count,
-- pre-dedup) instead of the live contacts count — so every refresh
-- the UI flipped between two different realities (93,779 DONE vs
-- 95,193 "1,414 pending"), buttons included.
--
-- New shape:
--   * list_enrichment_stats_cache — one row per list, the only thing
--     page loads read. Reading 56 rows is instant and always
--     internally consistent.
--   * refresh_list_enrichment_stats(p_lists) — recomputes the cache
--     from the base tables; NULL = all lists (~9s, rare), array =
--     just those lists via the (lead_list_name, status) indexes
--     (~0.3-1s, the steady-state path while a list is enriching).
--     Only the server (service_role) may call it; clients can't
--     stampede the DB with aggregate scans anymore.
--   * get_list_enrichment_stats() — same name/callsites, but now a
--     thin import_lists ⟕ cache join. Gains a refreshed_at column so
--     the server can decide when a refresh is due.
--
-- The queue-state RPC gets the same treatment via denormalization:
-- job_items.lead_list_name (filled by trigger, mirroring the
-- enrichments pattern from 20260516) removes the 90k+ row join to
-- contacts that made get_list_enrichment_queue_state() take ~3.3s.

-- 1. Cache table -----------------------------------------------------

CREATE TABLE IF NOT EXISTS public.list_enrichment_stats_cache (
    lead_list_name  TEXT PRIMARY KEY,
    completed_count BIGINT NOT NULL DEFAULT 0,
    failed_count    BIGINT NOT NULL DEFAULT 0,
    total_count     BIGINT NOT NULL DEFAULT 0,
    refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

GRANT SELECT ON public.list_enrichment_stats_cache TO anon, authenticated, service_role;
GRANT INSERT, UPDATE, DELETE ON public.list_enrichment_stats_cache TO service_role;

-- 2. Refresh RPC ------------------------------------------------------
--
-- Two explicit branches instead of `p_lists IS NULL OR col = ANY(..)`
-- so each gets its own plan: the targeted branch must hit the
-- per-list indexes, the full branch wants the single-pass GROUP BY.
-- total_count semantics carried over from 20260517: live contacts
-- count (post-dedup truth), falling back to import_lists.contact_count
-- only while a brand-new list's contact INSERTs are still streaming in.

CREATE OR REPLACE FUNCTION public.refresh_list_enrichment_stats(p_lists TEXT[] DEFAULT NULL)
RETURNS TABLE(
    lead_list_name  TEXT,
    completed_count BIGINT,
    failed_count    BIGINT,
    total_count     BIGINT,
    refreshed_at    TIMESTAMPTZ
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
SET statement_timeout TO '120s'
AS $$
-- The RETURNS TABLE names double as plpgsql variables; without this
-- pragma the INSERT's column list trips "column reference is
-- ambiguous". Columns win everywhere in this body.
#variable_conflict use_column
BEGIN
    IF p_lists IS NULL THEN
        -- Full refresh: also drop cache rows for lists that no longer
        -- exist (deleted or renamed lists would otherwise linger).
        DELETE FROM list_enrichment_stats_cache lc
        WHERE NOT EXISTS (SELECT 1 FROM import_lists il WHERE il.name = lc.lead_list_name);

        RETURN QUERY
        WITH stats AS (
            SELECT e.lead_list_name AS name,
                   COUNT(*) FILTER (WHERE e.status = 'completed')::BIGINT AS completed,
                   COUNT(*) FILTER (WHERE e.status = 'failed')::BIGINT    AS failed
            FROM enrichments e
            WHERE e.lead_list_name IS NOT NULL
            GROUP BY e.lead_list_name
        ),
        contact_counts AS (
            SELECT c.lead_list_name AS name, COUNT(*)::BIGINT AS total
            FROM contacts c
            WHERE c.lead_list_name IS NOT NULL
            GROUP BY c.lead_list_name
        ),
        up AS (
            INSERT INTO list_enrichment_stats_cache AS lc
                (lead_list_name, completed_count, failed_count, total_count, refreshed_at)
            SELECT il.name,
                   COALESCE(s.completed, 0),
                   COALESCE(s.failed, 0),
                   COALESCE(cc.total, il.contact_count, 0)::BIGINT,
                   now()
            FROM import_lists il
            LEFT JOIN stats s          ON s.name  = il.name
            LEFT JOIN contact_counts cc ON cc.name = il.name
            ON CONFLICT (lead_list_name) DO UPDATE
                SET completed_count = EXCLUDED.completed_count,
                    failed_count    = EXCLUDED.failed_count,
                    total_count     = EXCLUDED.total_count,
                    refreshed_at    = EXCLUDED.refreshed_at
            RETURNING lc.lead_list_name, lc.completed_count, lc.failed_count,
                      lc.total_count, lc.refreshed_at
        )
        SELECT * FROM up;
    ELSE
        RETURN QUERY
        WITH stats AS (
            SELECT e.lead_list_name AS name,
                   COUNT(*) FILTER (WHERE e.status = 'completed')::BIGINT AS completed,
                   COUNT(*) FILTER (WHERE e.status = 'failed')::BIGINT    AS failed
            FROM enrichments e
            WHERE e.lead_list_name = ANY(p_lists)
            GROUP BY e.lead_list_name
        ),
        contact_counts AS (
            SELECT c.lead_list_name AS name, COUNT(*)::BIGINT AS total
            FROM contacts c
            WHERE c.lead_list_name = ANY(p_lists)
            GROUP BY c.lead_list_name
        ),
        up AS (
            INSERT INTO list_enrichment_stats_cache AS lc
                (lead_list_name, completed_count, failed_count, total_count, refreshed_at)
            SELECT il.name,
                   COALESCE(s.completed, 0),
                   COALESCE(s.failed, 0),
                   COALESCE(cc.total, il.contact_count, 0)::BIGINT,
                   now()
            FROM import_lists il
            LEFT JOIN stats s          ON s.name  = il.name
            LEFT JOIN contact_counts cc ON cc.name = il.name
            WHERE il.name = ANY(p_lists)
            ON CONFLICT (lead_list_name) DO UPDATE
                SET completed_count = EXCLUDED.completed_count,
                    failed_count    = EXCLUDED.failed_count,
                    total_count     = EXCLUDED.total_count,
                    refreshed_at    = EXCLUDED.refreshed_at
            RETURNING lc.lead_list_name, lc.completed_count, lc.failed_count,
                      lc.total_count, lc.refreshed_at
        )
        SELECT * FROM up;
    END IF;
END;
$$;

-- Server + worker only. Page loads must never be able to trigger the
-- heavy aggregate — that's exactly the stampede this migration removes.
REVOKE ALL ON FUNCTION public.refresh_list_enrichment_stats(TEXT[]) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.refresh_list_enrichment_stats(TEXT[]) TO service_role;

-- 3. Reader RPC: same name, now instant ------------------------------
--
-- DROP first: the return type gains refreshed_at, which CREATE OR
-- REPLACE can't do. Lists with no cache row yet (brand-new import,
-- cache never refreshed) come back with zero counts, the import-time
-- contact_count as total, and refreshed_at NULL — the server treats
-- NULL as "refresh overdue".

DROP FUNCTION IF EXISTS public.get_list_enrichment_stats();

CREATE FUNCTION public.get_list_enrichment_stats()
RETURNS TABLE(
    lead_list_name  TEXT,
    completed_count BIGINT,
    failed_count    BIGINT,
    total_count     BIGINT,
    refreshed_at    TIMESTAMPTZ
)
LANGUAGE sql
STABLE
SET statement_timeout TO '10s'
AS $$
    SELECT
        il.name                                          AS lead_list_name,
        COALESCE(lc.completed_count, 0)                  AS completed_count,
        COALESCE(lc.failed_count, 0)                     AS failed_count,
        COALESCE(lc.total_count, il.contact_count, 0)::BIGINT AS total_count,
        lc.refreshed_at                                  AS refreshed_at
    FROM import_lists il
    LEFT JOIN list_enrichment_stats_cache lc ON lc.lead_list_name = il.name;
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_stats()
    TO anon, authenticated, service_role;

-- 4. Denormalize lead_list_name onto job_items ------------------------
--
-- Same defense-in-depth trigger pattern as enrichments (20260516):
-- the BEFORE INSERT trigger derives the list from contacts when the
-- inserting code didn't set it. Backfill below only touches OPEN
-- items — terminal rows never feed the queue-state RPC, so rewriting
-- 2.7M historical rows would be pure churn.

ALTER TABLE public.job_items
    ADD COLUMN IF NOT EXISTS lead_list_name TEXT;

CREATE OR REPLACE FUNCTION public.job_items_set_lead_list_name()
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

DROP TRIGGER IF EXISTS trg_job_items_set_lead_list_name ON public.job_items;
CREATE TRIGGER trg_job_items_set_lead_list_name
BEFORE INSERT ON public.job_items
FOR EACH ROW
EXECUTE FUNCTION public.job_items_set_lead_list_name();

UPDATE public.job_items ji
   SET lead_list_name = c.lead_list_name
  FROM public.contacts c
 WHERE c.contact_id = ji.contact_id
   AND ji.status IN ('pending', 'retrying', 'processing')
   AND ji.lead_list_name IS NULL;

-- Partial index keeps the open-items scan tight; mirrors the existing
-- idx_job_items_active_status predicate so it stays small (only rows
-- currently in the queue).
CREATE INDEX IF NOT EXISTS idx_job_items_open_list
    ON public.job_items (lead_list_name)
    WHERE status IN ('pending', 'retrying', 'processing');

-- 5. Queue-state RPC v3: no more contacts join ------------------------
--
-- Semantics identical to 20260612_queue_liveness_via_jobs_status: a
-- list is live only via open items whose parent job is itself live.
-- The contacts fallback is a scalar subquery inside COALESCE on
-- purpose — COALESCE evaluates lazily, so the probe fires ONLY for
-- rows the backfill/trigger somehow missed (lead_list_name IS NULL).
-- A LEFT JOIN variant probed contacts for every open item before
-- applying the join filter (51k probes / ~4s); this form is ~60ms.

CREATE OR REPLACE FUNCTION public.get_list_enrichment_queue_state()
RETURNS TABLE (
    lead_list_name TEXT,
    queue_state    TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
SET statement_timeout TO '15s'
AS $$
    SELECT
        COALESCE(
            ji.lead_list_name,
            (SELECT c.lead_list_name FROM contacts c WHERE c.contact_id = ji.contact_id)
        ) AS lead_list_name,
        CASE WHEN bool_or(ji.status = 'processing') THEN 'running' ELSE 'queued' END AS queue_state
    FROM job_items ji
    JOIN jobs j ON j.id = ji.job_id AND j.status IN ('pending', 'processing')
    WHERE ji.status IN ('pending', 'retrying', 'processing')
    GROUP BY 1
    HAVING COALESCE(
            ji.lead_list_name,
            (SELECT c.lead_list_name FROM contacts c WHERE c.contact_id = ji.contact_id)
        ) IS NOT NULL;
$$;

GRANT EXECUTE ON FUNCTION public.get_list_enrichment_queue_state() TO anon, authenticated, service_role;

-- 6. Seed the cache so the first deploy doesn't serve zeros ----------

SELECT public.refresh_list_enrichment_stats(NULL);

NOTIFY pgrst, 'reload schema';
