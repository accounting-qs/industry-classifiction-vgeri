-- =====================================================================
-- Lock the anon and authenticated roles out of the database
--
-- WHY
-- The anon key shipped in the public JS bundle for months, so treat it as
-- known. Until now anon could SELECT/INSERT/UPDATE/DELETE every public
-- table (contacts ~7.9M rows, enrichments, job_items, app_settings with
-- the saved Anthropic key) and EXECUTE 47 of the 50 public functions,
-- delete_import_list_page included. RLS was off everywhere.
--
-- The app no longer needs either role:
--   - The browser talks only to the password-gated Express server. Its
--     last direct Supabase call, the Contacts table, now goes through
--     POST /api/contacts/page.
--   - The server's PostgREST client is service_role. It refuses to start
--     without SUPABASE_SERVICE_ROLE_KEY (services/supabaseServiceRole.ts).
--   - DATABASE_URL (services/pgClient.ts) logs in as postgres. On the live
--     project, the import worker's COPY statements run as postgres.
--   - Both pg_cron jobs (bucketing-retention,
--     refresh-bucketing-global-stats) run as postgres.
--   service_role and postgres both have BYPASSRLS, and postgres owns every
--   object below. Nothing they do changes.
--
-- Checked on the live project (zxnaxtdeujunujnjaweo) on 2026-11-09:
--   - 7 days of API logs: every anon request is the browser's GET
--     /rest/v1/contacts. Everything else is service_role from Render, or
--     signed Storage URLs, which carry their own token.
--   - All 24 tables, 3 sequences and 50 functions in public are owned by
--     postgres. service_role has explicit grants on all of them, so
--     revoking PUBLIC's EXECUTE does not take anything from it.
--   - The bucketing-csv anon/authenticated storage policies
--     (20260511_bucketing_csv_storage_policies.sql) are already gone.
--
-- WHAT
--   1. RLS on for every public table, with no policies. anon and
--      authenticated then see no rows even if a grant comes back.
--   2. Revoke all privileges on public tables, sequences and functions
--      from anon and authenticated. Also revoke EXECUTE from PUBLIC:
--      functions get it by default and anon inherits it. Re-grant
--      service_role explicitly, which changes nothing today.
--   3. Default privileges, so objects postgres creates later are not
--      granted to anon, authenticated or PUBLIC again.
--   4. Drop the bucketing-csv anon/authenticated storage policies if
--      present. This is a no-op live; it keeps a database rebuilt from
--      these migrations the same as the live one.
--
-- NOT CHANGED
--   - USAGE on schema public. It is harmless with no object privileges.
--   - The supabase_admin default privileges in public. Only Supabase
--     itself creates objects as supabase_admin, and postgres cannot alter
--     them (it is not a member of that role).
--   - The "contact-imports: service_role all" storage policy. Signed
--     upload/download URLs keep working, because Storage checks the
--     signed token, not RLS.
--
-- FOR FUTURE MIGRATIONS
--   - A new table gets no grants for anon/authenticated (step 3), but RLS
--     is not switched on for it automatically. Add
--     `ALTER TABLE ... ENABLE ROW LEVEL SECURITY;` to keep the security
--     advisor clean.
--   - A new function in public gets EXECUTE for postgres and service_role
--     only.
--
-- APPLYING
-- Ship the code first (the Contacts tab breaks for anyone still on the old
-- bundle). Then apply this while no enrichment, bucketing, import or
-- export job is running. ENABLE ROW LEVEL SECURITY takes an ACCESS
-- EXCLUSIVE lock on each table. lock_timeout makes the whole migration
-- fail and roll back instead of stalling the app behind a long query; if
-- that happens, just run it again later. It is one transaction: it either
-- all applies or none of it does.
-- =====================================================================

BEGIN;

SET LOCAL lock_timeout = '3s';

-- 1. RLS on every table in public, no policies -------------------------
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND NOT c.relrowsecurity
    LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', r.relname);
    END LOOP;
END $$;

-- 2. Revoke existing privileges ------------------------------------------
REVOKE ALL ON ALL TABLES    IN SCHEMA public FROM anon, authenticated;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM anon, authenticated;
REVOKE ALL ON ALL ROUTINES  IN SCHEMA public FROM PUBLIC, anon, authenticated;

GRANT ALL     ON ALL TABLES    IN SCHEMA public TO service_role;
GRANT ALL     ON ALL SEQUENCES IN SCHEMA public TO service_role;
GRANT EXECUTE ON ALL ROUTINES  IN SCHEMA public TO service_role;

-- 3. Default privileges for objects postgres creates later ---------------
-- Supabase's per-schema defaults grant anon and authenticated everything.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON TABLES    FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON SEQUENCES FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON FUNCTIONS FROM anon, authenticated;

-- PUBLIC's EXECUTE on new functions is a built-in global default, and a
-- per-schema REVOKE cannot take back a global grant (see ALTER DEFAULT
-- PRIVILEGES in the Postgres docs). So revoke it globally for postgres, then
-- give it back in the extensions schema, where postgres owns the
-- pgcrypto/uuid-ossp functions. An extension update creating new functions
-- there must keep working for everyone, exactly as today. postgres creates
-- functions in no other schema.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA extensions GRANT EXECUTE ON FUNCTIONS TO PUBLIC;

-- 4. Old bucketing-csv storage policies for anon/authenticated -----------
-- Only touch storage.objects if one of them is actually there. DROP POLICY
-- locks the table, even with IF EXISTS.
DO $$
DECLARE p text;
BEGIN
    FOR p IN
        SELECT policyname FROM pg_policies
        WHERE schemaname = 'storage' AND tablename = 'objects'
          AND policyname IN ('bucketing-csv: insert', 'bucketing-csv: select',
                             'bucketing-csv: update', 'bucketing-csv: delete')
    LOOP
        EXECUTE format('DROP POLICY %I ON storage.objects', p);
    END LOOP;
END $$;

COMMIT;

-- =====================================================================
-- CHECK AFTERWARDS (each should return zero rows)
--
--   -- tables without RLS
--   SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
--   WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND NOT c.relrowsecurity;
--
--   -- anything anon or authenticated can still touch
--   SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
--   WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','S')
--     AND (has_table_privilege('anon', c.oid, 'SELECT,INSERT,UPDATE,DELETE')
--       OR has_table_privilege('authenticated', c.oid, 'SELECT,INSERT,UPDATE,DELETE'));
--   SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--   WHERE n.nspname = 'public'
--     AND (has_function_privilege('anon', p.oid, 'EXECUTE')
--       OR has_function_privilege('authenticated', p.oid, 'EXECUTE'));
--
--   -- something service_role lost (must be empty too)
--   SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--   WHERE n.nspname = 'public' AND NOT has_function_privilege('service_role', p.oid, 'EXECUTE');
--
-- ROLLBACK (re-opens the hole; only if something that truly needs anon
-- turns up, and then grant just that):
--   ALTER TABLE public.<table> DISABLE ROW LEVEL SECURITY;
--   GRANT SELECT ON public.<table> TO anon;
--   GRANT EXECUTE ON FUNCTION public.<fn>(<args>) TO anon;
-- =====================================================================
