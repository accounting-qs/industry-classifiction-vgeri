-- Manual "bucketed" override on import_lists.
--
-- The existing `bucketed` flag is derived from membership in any
-- `bucketing_runs.list_names` array — useful, but it doesn't cover
-- lists that were bucketed outside the in-app flow (manual SQL,
-- exported CSV processed elsewhere, legacy data). Add a manual
-- override the UI can toggle, and OR it with the derived flag in
-- the import-lists endpoint so a list is "bucketed" if either
-- source says so.
--
-- Idempotent: re-running is a no-op.

ALTER TABLE import_lists
    ADD COLUMN IF NOT EXISTS manually_bucketed BOOLEAN NOT NULL DEFAULT false;
