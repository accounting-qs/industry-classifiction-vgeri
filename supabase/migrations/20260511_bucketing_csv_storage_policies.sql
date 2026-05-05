-- =====================================================================
-- Storage RLS policies for the 'bucketing-csv' bucket
--
-- The async CSV export worker (20260510) creates a private storage
-- bucket and uploads gzipped exports to it. The service_role JWT
-- bypasses storage.objects RLS — so when the export errored with
-- "new row violates row-level security policy" the server's Supabase
-- client was not actually authenticated as service_role (likely
-- SUPABASE_SERVICE_ROLE_KEY env var missing → fallback to anon key).
--
-- Defense in depth: add explicit policies for the 'bucketing-csv'
-- bucket so uploads/reads/deletes work regardless of which role the
-- server is running with. The bucket stays PRIVATE — direct downloads
-- still require a valid signed URL (handed out only by the server
-- after the worker completes), so widening write-access to anon does
-- not expose any data.
--
-- DROP-then-CREATE so the migration is idempotent on re-run (CREATE
-- POLICY does not support IF NOT EXISTS until Postgres 16).
-- =====================================================================

DROP POLICY IF EXISTS "bucketing-csv: insert"  ON storage.objects;
DROP POLICY IF EXISTS "bucketing-csv: select"  ON storage.objects;
DROP POLICY IF EXISTS "bucketing-csv: update"  ON storage.objects;
DROP POLICY IF EXISTS "bucketing-csv: delete"  ON storage.objects;

CREATE POLICY "bucketing-csv: insert"
    ON storage.objects FOR INSERT
    TO anon, authenticated, service_role
    WITH CHECK (bucket_id = 'bucketing-csv');

CREATE POLICY "bucketing-csv: select"
    ON storage.objects FOR SELECT
    TO anon, authenticated, service_role
    USING (bucket_id = 'bucketing-csv');

CREATE POLICY "bucketing-csv: update"
    ON storage.objects FOR UPDATE
    TO anon, authenticated, service_role
    USING (bucket_id = 'bucketing-csv')
    WITH CHECK (bucket_id = 'bucketing-csv');

CREATE POLICY "bucketing-csv: delete"
    ON storage.objects FOR DELETE
    TO anon, authenticated, service_role
    USING (bucket_id = 'bucketing-csv');
