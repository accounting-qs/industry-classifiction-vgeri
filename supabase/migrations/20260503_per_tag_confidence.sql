-- v4.1: Per-tag confidence scores + identity-DQ cascade toggle.
--
-- Sonnet now returns three independent confidence scores per industry
-- (identity / characteristic / sector) instead of one overall score.
-- The Phase 1b rollup uses each score to gate the layer it can enter:
--   identity confidence < 6      → row goes straight to General
--   characteristic conf < 6      → row skips combo / sector_core+spec / spec
--   sector confidence < 6        → row skips combo and sector_core+spec
--
-- This recovers volume that was previously lost to General just because
-- one tag was uncertain. The single legacy `confidence` column stays in
-- place and is populated with the min of the three so existing UI bits
-- (QA queue, results) keep working.
--
-- Plus a per-run toggle: apply_identity_dq_cascade. Default FALSE — we
-- trust Sonnet's per-row is_disqualified decision and don't auto-DQ a
-- contact just because its identity is library-flagged [DQ]. Setting
-- this back to TRUE on a run restores the old behavior.

ALTER TABLE bucket_industry_map
    ADD COLUMN IF NOT EXISTS identity_confidence NUMERIC(4, 2),
    ADD COLUMN IF NOT EXISTS characteristic_confidence NUMERIC(4, 2),
    ADD COLUMN IF NOT EXISTS sector_confidence NUMERIC(4, 2);

ALTER TABLE bucket_assignments
    ADD COLUMN IF NOT EXISTS identity_confidence NUMERIC(4, 2),
    ADD COLUMN IF NOT EXISTS characteristic_confidence NUMERIC(4, 2),
    ADD COLUMN IF NOT EXISTS sector_confidence NUMERIC(4, 2);

ALTER TABLE bucket_contact_map
    ADD COLUMN IF NOT EXISTS identity_confidence NUMERIC(4, 2),
    ADD COLUMN IF NOT EXISTS characteristic_confidence NUMERIC(4, 2),
    ADD COLUMN IF NOT EXISTS sector_confidence NUMERIC(4, 2);

ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS apply_identity_dq_cascade BOOLEAN NOT NULL DEFAULT FALSE;

NOTIFY pgrst, 'reload schema';
