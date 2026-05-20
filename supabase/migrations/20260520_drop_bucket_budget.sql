-- Drop the bucket_budget column. The new deterministic Phase 1b rollup
-- (apply_rollup_bucket_assignments) uses only the two volume thresholds
-- (min_volume = sub-identity floor, identity_min_volume = identity floor)
-- and does not cap the total bucket count. The old in-process
-- computeContactRollup function — the only consumer of bucket_budget —
-- is being removed in the same release.
--
-- Idempotent: IF EXISTS guard so re-running is a no-op.

ALTER TABLE bucketing_runs
    DROP COLUMN IF EXISTS bucket_budget;

NOTIFY pgrst, 'reload schema';
