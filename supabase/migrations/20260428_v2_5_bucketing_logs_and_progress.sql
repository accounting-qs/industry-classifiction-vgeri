-- v2.5: per-run live logs + progress tracking for the Bucketing UI.
--
-- bucketing_run_logs is a dedicated append-only stream so the Review/
-- in-flight screen can poll for new lines without scanning the global
-- pipeline_logs table. progress is a JSONB column on bucketing_runs that
-- holds the current phase/step + current/total counters; the UI reads it
-- to render the bar + ETA.
--
-- Idempotent.

CREATE TABLE IF NOT EXISTS bucketing_run_logs (
    id BIGSERIAL PRIMARY KEY,
    bucketing_run_id UUID NOT NULL REFERENCES bucketing_runs(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ DEFAULT now(),
    level TEXT NOT NULL DEFAULT 'info',         -- 'info' | 'warn' | 'error' | 'phase'
    message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS bucketing_run_logs_run_id_idx
    ON bucketing_run_logs (bucketing_run_id, id);

ALTER TABLE bucketing_run_logs DISABLE ROW LEVEL SECURITY;

ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS progress JSONB;

NOTIFY pgrst, 'reload schema';
