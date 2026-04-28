-- v2.8: stop/resume for in-flight bucketing runs.
-- cancel_requested is the signal flag the service polls at every phase
-- boundary. status='cancelled' is a new terminal-but-resumable state.
-- Idempotent.

ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN DEFAULT false;

-- bucketing_runs.status is a free-form TEXT column, no enum, no constraint.
-- We just append 'cancelled' to the conceptual set: 'taxonomy_pending' |
-- 'taxonomy_ready' | 'assigning' | 'completed' | 'failed' | 'cancelled'.

NOTIFY pgrst, 'reload schema';
