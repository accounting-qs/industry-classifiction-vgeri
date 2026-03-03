-- Atomic Job Counter Increment Function
-- Run this in the Supabase SQL Editor to enable atomic counter updates.
-- This prevents race conditions where two chunks finishing simultaneously
-- could overwrite each other's counter increments.

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
