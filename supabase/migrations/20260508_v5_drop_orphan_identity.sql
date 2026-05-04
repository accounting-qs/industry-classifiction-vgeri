-- =====================================================================
-- v5 follow-up — drop orphan `identity` column from bucket_industry_map
--
-- v5's main migration (20260507) dropped the bucket_industry_map mirror
-- trigger that copied identity → primary_identity but kept both columns.
-- Code was still writing to `identity` while every reader looked at
-- `primary_identity`, so JOIN-first Phase 1b silently saw NULLs on every
-- new row.
--
-- The companion code change (commit hash in this PR) flips the writer
-- to populate `primary_identity` directly, matching the canonical
-- column name already used by bucket_contact_map and bucket_assignments.
-- This migration drops the now-orphan `identity` column and the
-- `is_new_identity` flag's no-longer-needed sibling columns weren't
-- introduced — `is_new_identity` itself is still useful and stays.
--
-- Run AFTER deploying the code change. Idempotent.
-- =====================================================================

ALTER TABLE bucket_industry_map DROP COLUMN IF EXISTS identity;

-- Reload PostgREST schema cache.
NOTIFY pgrst, 'reload schema';
