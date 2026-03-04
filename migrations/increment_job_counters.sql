-- ============================================
-- Pipeline Fix Migrations - Run ALL in Supabase SQL Editor
-- ============================================

-- 1. Set default for attempt_count (prevents NOT NULL violations)
ALTER TABLE job_items ALTER COLUMN attempt_count SET DEFAULT 0;

-- 2. Fix existing NULL rows
UPDATE job_items SET attempt_count = 0 WHERE attempt_count IS NULL;

-- 3. Atomic counter increment RPC
CREATE OR REPLACE FUNCTION increment_job_counters(
    job_id_input UUID,
    completed_increment INT DEFAULT 0,
    failed_increment INT DEFAULT 0
)
RETURNS VOID AS $$
BEGIN
    UPDATE jobs
    SET
        completed_items = COALESCE(completed_items, 0) + completed_increment,
        failed_items = COALESCE(failed_items, 0) + failed_increment
    WHERE id = job_id_input;
END;
$$ LANGUAGE plpgsql;
