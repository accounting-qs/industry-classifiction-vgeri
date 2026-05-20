-- Drop duplicate / unused indexes flagged by Supabase performance advisor.
--
-- For each duplicate pair, the index with the higher pg_stat_user_indexes.idx_scan
-- count is kept; the lower-scan one is dropped. Postgres picks indexes by column
-- shape, not by name, so app code does not reference these — the planner will
-- silently switch to the kept index. Sizes reclaimed: ~330 MB total disk + write
-- amplification on every INSERT/UPDATE to the affected tables.

BEGIN;

-- contacts.lead_list_name — keep idx_contacts_lead_list_name (17,774 scans)
DROP INDEX IF EXISTS public.contacts_lead_list_name_idx;

-- enrichments.contact_id (non-unique) — keep idx_enrichments_contact_id (138M scans)
DROP INDEX IF EXISTS public.enrichments_contact_id_idx;

-- enrichments.status — keep enrichments_status_idx (991k scans)
DROP INDEX IF EXISTS public.idx_enrichments_status;

-- enrichments.contact_id UNIQUE — keep enrichments_contact_id_unique (53M scans).
-- Both back UNIQUE constraints, so the loser must be dropped via ALTER TABLE.
-- The kept constraint still enforces dedup, so the invariant is preserved.
ALTER TABLE public.enrichments DROP CONSTRAINT IF EXISTS unique_contact_id;

-- Unused indexes (idx_scan = 0 since stats reset) — pure overhead.
DROP INDEX IF EXISTS public.idx_job_items_contact;
DROP INDEX IF EXISTS public.bucket_contact_map_run_bucket_idx;
DROP INDEX IF EXISTS public.bucket_contact_map_run_pre_rollup_idx;
DROP INDEX IF EXISTS public.bucket_library_canonical_idx;

COMMIT;
