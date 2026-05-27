-- Partial index to speed up /api/bucketing/runs/:id/qa-queue.
--
-- The route filters bucket_industry_map by (bucketing_run_id, needs_qa=true)
-- and orders by confidence. Existing indexes only cover (bucketing_run_id,
-- bucket_name) and the PK (bucketing_run_id, industry_string), so the
-- planner falls back to a parallel seq scan and reads ~10k buffers per
-- request. With 132k qa-flagged rows in one observed run, this is enough
-- to trip Supabase's REST timeout under production load — visible as 500s
-- in the UI when the Review screen first loads.
--
-- The partial index limits storage to only the rows needs_qa actually flags,
-- and includes confidence so the planner can serve the ORDER BY from the
-- index itself (no separate sort step).

CREATE INDEX IF NOT EXISTS bucket_industry_map_qa_queue_idx
    ON public.bucket_industry_map (bucketing_run_id, confidence)
    WHERE needs_qa = true;
