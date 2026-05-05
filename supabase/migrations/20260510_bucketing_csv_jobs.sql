-- =====================================================================
-- bucketing CSV export jobs — async + Supabase-Storage backed
--
-- The synchronous /taxonomy-contacts.csv endpoint streams direct to the
-- response and works for ~10k-row runs, but at 200k–300k contacts the
-- HTTP request runs longer than typical browser/proxy timeouts. This
-- table tracks an async job: a server worker streams the per-contact
-- bucket_assignments × enrichments × bucket_industry_map join in pages,
-- gzips the output, uploads to the 'bucketing-csv' storage bucket, and
-- exposes a signed download URL that expires after 24 hours.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS, ON CONFLICT DO NOTHING).
-- =====================================================================

CREATE TABLE IF NOT EXISTS bucketing_csv_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucketing_run_id    UUID NOT NULL REFERENCES bucketing_runs(id) ON DELETE CASCADE,
    status              TEXT NOT NULL DEFAULT 'pending',
        -- pending | running | ready | failed
    progress_rows       BIGINT NOT NULL DEFAULT 0,
    total_rows          BIGINT,
    storage_path        TEXT,
    download_url        TEXT,
    file_size_bytes     BIGINT,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    expires_at          TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '24 hours')
);

CREATE INDEX IF NOT EXISTS bucketing_csv_jobs_run_idx
    ON bucketing_csv_jobs (bucketing_run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS bucketing_csv_jobs_expires_idx
    ON bucketing_csv_jobs (expires_at);

-- Storage bucket for the gzipped CSVs. Private (no public read) — the
-- server hands out short-lived signed URLs that expire with the job row.
-- 5 GB per-file cap covers any realistic export (300k rows ≈ 30 MB gz).
INSERT INTO storage.buckets (id, name, public, file_size_limit)
VALUES ('bucketing-csv', 'bucketing-csv', false, 5368709120)
ON CONFLICT (id) DO NOTHING;

NOTIFY pgrst, 'reload schema';
