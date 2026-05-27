-- Persist Finalize Taxonomy state on bucketing_runs so the UI can show
-- "already finalized" after a page reload.
--
-- Today the finalize result lives only in React state on the page that
-- clicked the button. Reloading the page (or revisiting the run later)
-- shows no indication that finalize was ever run, even though the
-- bucket_industry_map rows are clearly already library-bound.
--
-- Columns:
--   finalize_completed_at   - timestamp of last successful finalize
--   finalize_rerouted_count - rows whose proposed tags survived to the library
--   finalize_nullified_count- rows whose tags were rejected → General
--   finalize_failed_count   - reserved (current finalize uses no LLM and
--                             always reports 0; kept for forward-compat in
--                             case a future variant uses retries)

ALTER TABLE public.bucketing_runs
    ADD COLUMN IF NOT EXISTS finalize_completed_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS finalize_rerouted_count  INTEGER,
    ADD COLUMN IF NOT EXISTS finalize_nullified_count INTEGER,
    ADD COLUMN IF NOT EXISTS finalize_failed_count    INTEGER;

NOTIFY pgrst, 'reload schema';
