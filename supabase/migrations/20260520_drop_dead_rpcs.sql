-- Drop dead RPCs left over from earlier architecture iterations.
--
-- bucketing_apply_volume_rollup:
--   References bucket_budget (dropped 20260520_drop_bucket_budget) and
--   pre-rename columns functional_specialization / sector_focus (renamed
--   to sub_identity / sector in 20260515_rename_characteristic_to_sub_identity).
--   No code path calls it.
--
-- bucketing_deterministic_fanout:
--   Also references functional_specialization. No code path calls it.
--
-- Both were replaced by apply_rollup_bucket_assignments (the deterministic
-- two-threshold rollup). Keeping them around just means anyone who tries
-- to call one by name from psql gets a confusing
-- "column functional_specialization does not exist" instead of a clear
-- "function not found".

DROP FUNCTION IF EXISTS public.bucketing_apply_volume_rollup(UUID);
DROP FUNCTION IF EXISTS public.bucketing_deterministic_fanout(UUID);

NOTIFY pgrst, 'reload schema';
