-- =====================================================================
-- Storage-backed contact import jobs
--
-- Replaces the browser-driven import (Papa.parse in the tab → 2000-row
-- JSON chunks → /api/import) with a durable server-side pipeline:
--
--   browser → signed-URL PUT → 'contact-imports' bucket
--          → server streams the object into a TEMP table via COPY
--          → one server-side SQL merge into contacts
--          → object deleted once the merge is verified
--
-- Why: the old path held every un-sent chunk alive in the tab (no
-- parser backpressure), ran 170–4,900 sequential POSTs, and died with
-- the tab. Nothing survived a refresh. This flow puts the file
-- somewhere durable *first*, so mapping and ingest are both resumable
-- and neither depends on the browser staying open.
--
-- NOTE: the pre-existing 'bucketing-csv' bucket is NOT reused. It was
-- created for the export direction, has never held a single object
-- (the export worker writes to the Render persistent disk instead —
-- see CSV_FILES_DIR in server.ts), and its policies grant anon blanket
-- write/delete. Contact PII staged pre-ingest gets its own bucket with
-- service_role-only policies.
--
-- Idempotent: safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Private bucket for uploaded CSVs
--
-- 2 GB per-object cap. Objects are transient — deleted immediately on a
-- verified successful merge, and TTL-swept otherwise (see the cleanup
-- cron in server.ts).
--
-- IMPORTANT: the project-level "Global file size limit" in Storage
-- Settings takes precedence over this value. Setting 2 GB here does
-- nothing if the global limit is lower (the Free-plan default is 50 MB).
-- ---------------------------------------------------------------------
INSERT INTO storage.buckets (id, name, public, file_size_limit)
VALUES ('contact-imports', 'contact-imports', false, 2147483648)
ON CONFLICT (id) DO UPDATE SET file_size_limit = EXCLUDED.file_size_limit;

-- ---------------------------------------------------------------------
-- 2. RLS — service_role only.
--
-- Browser uploads use a short-lived signed upload URL minted server-side
-- (createSignedUploadUrl). Verified empirically: a bare PUT to that URL
-- with NO apikey and NO Authorization header succeeds, i.e. Storage
-- authorises from the URL's own token and does not consult RLS on that
-- path. So nothing here needs granting to anon, and no credential is
-- ever handed to the browser.
--
-- Deliberately narrower than the 'bucketing-csv' policies, which opened
-- insert/update/delete to anon as a workaround for a service-role key
-- that wasn't loading. Note that granting anon INSERT here would be the
-- only way to make resumable/TUS uploads work on this storage version —
-- that trade was considered and declined.
-- ---------------------------------------------------------------------
DROP POLICY IF EXISTS "contact-imports: service_role all" ON storage.objects;

CREATE POLICY "contact-imports: service_role all"
    ON storage.objects FOR ALL
    TO service_role
    USING (bucket_id = 'contact-imports')
    WITH CHECK (bucket_id = 'contact-imports');

-- ---------------------------------------------------------------------
-- 3. Job table
--
-- Mirrors the bucketing_csv_jobs shape (status / progress / expires_at
-- + a cleanup cron) so the two async flows stay operationally similar.
--
-- Lifecycle:
--   awaiting_upload → uploaded → queued → running → completed
--                                                 ↘ failed
-- 'uploaded' is the resumable resting state: the object is durable and
-- the job sits waiting for the user to finish column mapping. A tab
-- crash here loses nothing.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contact_import_jobs (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status                TEXT NOT NULL DEFAULT 'awaiting_upload',
        -- awaiting_upload | uploaded | queued | running | completed | failed

    -- Source object
    filename              TEXT,
    storage_path          TEXT,        -- object key within 'contact-imports'
    file_size_bytes       BIGINT,

    -- Header/preview snapshot, captured by range-reading the first
    -- ~256 KB of the object (never the whole file).
    csv_headers           TEXT[],
    preview_rows          JSONB,

    -- User's choices, captured at /start
    mapping               JSONB,       -- { "CSV Header": "contacts_column" }
    list_name             TEXT,
    overwrite_duplicates  BOOLEAN NOT NULL DEFAULT false,

    -- Progress. stage distinguishes the two long phases so the UI can
    -- show something honest: 'copy' is byte-driven (row count is not
    -- known until COPY finishes), 'merge' is row-driven.
    stage                 TEXT,        -- copy | merge
    progress_bytes        BIGINT NOT NULL DEFAULT 0,
    progress_rows         BIGINT NOT NULL DEFAULT 0,
    total_rows            BIGINT,      -- exact, set once COPY completes

    -- Outcome counters (same semantics as the old /api/import response)
    inserted_count        BIGINT NOT NULL DEFAULT 0,
    updated_count         BIGINT NOT NULL DEFAULT 0,
    duplicate_count       BIGINT NOT NULL DEFAULT 0,
    within_file_dupes     BIGINT NOT NULL DEFAULT 0,
    cross_list_dupes      BIGINT NOT NULL DEFAULT 0,
    invalid_count         BIGINT NOT NULL DEFAULT 0,
    invalid_by_reason     JSONB,
    cross_list_breakdown  JSONB,

    error_message         TEXT,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    -- 24 h is the resumable-mapping window. The TUS upload URL itself
    -- also expires after 24 h, so nothing outlives its own upload.
    expires_at            TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '24 hours')
);

CREATE INDEX IF NOT EXISTS contact_import_jobs_status_idx
    ON contact_import_jobs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS contact_import_jobs_expires_idx
    ON contact_import_jobs (expires_at);
CREATE INDEX IF NOT EXISTS contact_import_jobs_created_idx
    ON contact_import_jobs (created_at DESC);
