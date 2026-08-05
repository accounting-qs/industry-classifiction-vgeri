-- =====================================================================
-- Give service_role its own statement_timeout
--
-- A 421k-contact run failed with
--   "finalize upsert failed: canceling statement due to statement timeout"
--
-- The cause is systemic, not specific to finalize. Measured through the
-- real REST path:
--
--   POST /rest/v1/rpc/_diag_timeouts  (service_role key)
--   -> {"role":"service_role","statement_timeout":"8s","lock_timeout":"8s"}
--
-- service_role has no rolconfig of its own, so it inherits the settings
-- `authenticator` applies at login — 8 s. That ceiling applies to EVERY
-- PostgREST call the server makes: every .select(), .insert(), .upsert()
-- and every .rpc() that does not override it internally.
--
-- The heavy bucketing RPCs only ever worked because they SET
-- statement_timeout inside the function body. Plain table writes cannot
-- do that, so all six bulk-upsert sites in services/bucketingService.ts
-- have been running against an 8 s limit. A 1000-row upsert on
-- bucket_industry_map measures 751 ms in isolation — comfortably under
-- 8 s, right up until real contention pushes a chunk over it. That is why
-- this surfaced as an intermittent failure at volume rather than
-- immediately.
--
-- 120 s is chosen to remove the cliff, not to permit unbounded queries:
-- Supabase's gateway still severs any PostgREST request at ~60 s, so this
-- is effectively "let it use the full request budget". Work that
-- legitimately needs minutes must go through the direct connection in
-- services/pgClient.ts instead — no gateway, its own timeouts.
--
-- anon (3 s) and authenticated (8 s) are deliberately NOT changed: those
-- are reachable from the browser and should keep failing fast. Only
-- service_role, which is server-side only, gets the longer budget.
--
-- Idempotent: safe to re-run.
-- =====================================================================

ALTER ROLE service_role SET statement_timeout = '120s';

-- Lock waits get a separate, shorter budget. A statement that cannot get
-- its lock in 30 s is contending with something, and failing fast with a
-- retry (see callHeavyRpc in services/pgClient.ts) beats occupying a
-- connection for two minutes to find that out.
ALTER ROLE service_role SET lock_timeout = '30s';

-- PostgREST caches role settings; without this the change only takes
-- effect for connections opened after the next natural reload.
NOTIFY pgrst, 'reload schema';
