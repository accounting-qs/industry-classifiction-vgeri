-- Speed up the app-wide COUNT(DISTINCT contact_id) over the bucket tables that
-- get_dashboard_stats() runs for the Enrichment Dashboard's Phase 1a/1b counts.
--
-- The existing indexes on these tables lead with bucketing_run_id, so a GLOBAL
-- distinct over contact_id could not use them and fell back to a full scan +
-- sort: ~17s per table at ~2.1M rows. Both counts run sequentially inside the
-- RPC, so the function blew past its 30s statement_timeout (error 57014) and
-- the dashboard 503-ed with the misleading "is ...rpc.sql applied?" message.
--
-- A plain btree on contact_id lets Postgres do an index-only scan and cuts each
-- count from ~17s to <1s — the whole RPC drops from ~34s to ~1.6s.
--
-- NOTE: on production these were built with CREATE INDEX CONCURRENTLY (no write
-- lock, since the bucket tables are large). This migration uses a plain
-- CREATE INDEX IF NOT EXISTS so it can run inside the migration transaction;
-- it is a no-op where the concurrent index already exists and builds normally
-- on a fresh database.

CREATE INDEX IF NOT EXISTS idx_bucket_contact_map_contact_id
    ON public.bucket_contact_map (contact_id);

CREATE INDEX IF NOT EXISTS idx_bucket_assignments_contact_id
    ON public.bucket_assignments (contact_id);
