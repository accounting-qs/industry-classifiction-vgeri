-- v3.3: Backfill the v3.1 bits that never actually applied to the live DB.
--
-- A schema audit against production found:
--
--   1. Table `bucket_contact_map` is missing — Phase 1b writes to it on
--      every run, so an unfixed prod would fail with
--      "Could not find the table 'public.bucket_contact_map' in the
--      schema cache".
--
--   2. Columns `bucketing_runs.quality_warnings` and
--      `bucketing_runs.coverage_summary` are missing — Phase 1b's final
--      UPDATE writes both, so it would fail with
--      "Could not find the 'coverage_summary' column".
--
-- These were declared in 20260429_v3_1_per_contact_bucketing.sql but
-- only the table-less ALTER for bucket_assignments and the simpler RPCs
-- got committed; the CREATE TABLE and ALTER bucketing_runs blocks never
-- did. v3.2 carried the bucket_assignments columns but missed these.
--
-- This file is fully idempotent and only adds objects, never drops.

-- =====================================================================
-- 1. bucket_contact_map (Phase 1b per-contact pre-rollup decisions)
-- =====================================================================

CREATE TABLE IF NOT EXISTS bucket_contact_map (
    bucketing_run_id UUID NOT NULL REFERENCES bucketing_runs(id) ON DELETE CASCADE,
    contact_id TEXT NOT NULL,
    industry_string TEXT,
    primary_identity TEXT,
    functional_specialization TEXT,
    sector_focus TEXT,
    pre_rollup_bucket_name TEXT NOT NULL,
    bucket_name TEXT NOT NULL,
    rollup_level TEXT NOT NULL,
    source TEXT NOT NULL,
    confidence NUMERIC(4, 2),
    leaf_score NUMERIC(4, 2),
    ancestor_score NUMERIC(4, 2),
    root_score NUMERIC(4, 2),
    is_generic BOOLEAN DEFAULT false,
    is_disqualified BOOLEAN DEFAULT false,
    general_reason TEXT,
    reasons JSONB,
    assigned_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (bucketing_run_id, contact_id)
);

CREATE INDEX IF NOT EXISTS bucket_contact_map_run_bucket_idx
    ON bucket_contact_map (bucketing_run_id, bucket_name);
CREATE INDEX IF NOT EXISTS bucket_contact_map_run_pre_rollup_idx
    ON bucket_contact_map (bucketing_run_id, pre_rollup_bucket_name);
CREATE INDEX IF NOT EXISTS bucket_contact_map_run_general_reason_idx
    ON bucket_contact_map (bucketing_run_id, general_reason);

ALTER TABLE bucket_contact_map DISABLE ROW LEVEL SECURITY;

-- =====================================================================
-- 2. bucketing_runs telemetry columns
-- =====================================================================

ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS quality_warnings JSONB DEFAULT '[]'::JSONB,
    ADD COLUMN IF NOT EXISTS coverage_summary JSONB;

NOTIFY pgrst, 'reload schema';
