-- =====================================================================
-- Pin search_path on the functions where it costs nothing
--
-- WHY
-- The Supabase security advisor flags 40 functions in public with
-- function_search_path_mutable: they resolve table and function names
-- through the caller's search_path. Someone who could put an object with
-- the same name earlier on that path could make a function use it. Since
-- 20261109_lock_down_anon_access, only service_role and postgres can run
-- these functions, and only postgres can create objects in public or
-- extensions, so this is hardening rather than a hole. Still worth closing
-- where it is free.
--
-- WHAT
-- SET search_path = public, extensions on 22 of the 40. That is exactly the
-- path these functions resolve through today: postgres has
-- search_path = "$user", public, extensions (no schema is named after it),
-- and PostgREST runs service_role with public, extensions. pg_catalog and
-- pg_temp are still searched implicitly first, as now, so the functions
-- that create temp tables keep finding them. No name resolves differently.
--
--   - 19 plpgsql functions, including the 3 row triggers. plpgsql is
--     never inlined, so the only cost is the SET itself: about 2 us per
--     call (measured: 4.4 us vs 2.4 us per call over 200k calls).
--   - get_proxy_stats: a VOLATILE SQL function, which is never inlined.
--   - finalize_taxonomy_candidates, get_classification_vocabulary: scalar
--     SQL functions whose body reads tables, which is never inlined either.
--
-- NOT CHANGED: the other 18, all LANGUAGE sql functions that Postgres
-- inlines into the calling query today:
--   bucketing_run_export_rows, bucketing_schema_gaps,
--   get_assigned_bucket_counts, get_bucket_assignment_counts,
--   get_bucket_chain_counts, get_bucket_general_breakdown,
--   get_bucket_map_counts, get_bucket_sector_mix, get_contact_export_page,
--   get_contacts_by_status, get_contacts_chunk,
--   get_distinct_lead_list_names, get_enrichment_source_stats,
--   get_industry_vocabulary, get_list_enrichment_stats,
--   get_new_contact_ids, get_proposed_tag_contact_counts, normalize_domain
-- Any SET clause stops a SQL function from being inlined. That is the
-- 20x slowdown that 20260916_uninlinable_read_rpcs.sql fixed by removing
-- SET statement_timeout from these same functions. Checked again on
-- 2026-11-10: bucketing_run_export_rows, called through PostgREST or with
-- a literal run id as the CSV export does, reads only the run's contacts
-- through idx_contacts_lead_list_name. Planned on its own without the
-- value, which is what a non-inlined SQL function gets, it picks a
-- parallel seq scan of all of contacts, and a set-returning SQL function
-- runs that without workers.
-- normalize_domain also backs idx_contacts_norm_domain. Making it
-- non-inlinable would add a function call to every index update on
-- contacts, and connections that are already open could stop matching
-- the index (they cache its inlined expression).
-- These 18 keep the advisor warning. They only use names from public and
-- pg_catalog, and nobody but postgres can create objects there.
--
-- FOR FUTURE FUNCTIONS
--   - plpgsql: add SET search_path = public, extensions.
--   - LANGUAGE sql read paths: leave out any SET clause, or they stop
--     being inlined.
--
-- APPLYING
-- Each ALTER FUNCTION only rewrites one pg_proc row. Calls already running
-- finish with the old setting. Idempotent: safe to re-run.
-- =====================================================================

BEGIN;

-- plpgsql (never inlined)
ALTER FUNCTION public._bucketing_build_run_map(uuid)                                       SET search_path = public, extensions;
ALTER FUNCTION public.apply_rollup_bucket_assignments(uuid, integer, integer)              SET search_path = public, extensions;
ALTER FUNCTION public.backfill_enrichments_list_name_chunk()                               SET search_path = public, extensions;
ALTER FUNCTION public.bucketing_catchall_other(uuid)                                       SET search_path = public, extensions;
ALTER FUNCTION public.delete_import_list_page(text, integer)                               SET search_path = public, extensions;
ALTER FUNCTION public.enqueue_filtered_contacts(uuid, uuid[])                              SET search_path = public, extensions;
ALTER FUNCTION public.finalize_per_contact_taxonomy(uuid)                                  SET search_path = public, extensions;
ALTER FUNCTION public.get_bucketing_run_diagnostics(uuid)                                  SET search_path = public, extensions;
ALTER FUNCTION public.get_run_coverage_summary(uuid)                                       SET search_path = public, extensions;
ALTER FUNCTION public.increment_job_counters(uuid, integer, integer)                       SET search_path = public, extensions;
ALTER FUNCTION public.inherit_phase1a_tags(uuid)                                           SET search_path = public, extensions;
ALTER FUNCTION public.refresh_bucketing_global_stats()                                     SET search_path = public, extensions;
ALTER FUNCTION public.refresh_bucketing_global_stats_if_stale(interval)                    SET search_path = public, extensions;
ALTER FUNCTION public.rename_import_list(uuid, text)                                       SET search_path = public, extensions;
ALTER FUNCTION public.resolve_enrichment_targets(text[], text[], text)                     SET search_path = public, extensions;
ALTER FUNCTION public.rewrite_proposed_tag(uuid, text, text, text, text, text, text, text, text) SET search_path = public, extensions;

-- plpgsql row triggers
ALTER FUNCTION public.enrichments_set_lead_list_name()                                     SET search_path = public, extensions;
ALTER FUNCTION public.job_items_set_lead_list_name()                                       SET search_path = public, extensions;
ALTER FUNCTION public.set_updated_at()                                                     SET search_path = public, extensions;

-- LANGUAGE sql functions that are never inlined anyway
ALTER FUNCTION public.get_proxy_stats()                                                    SET search_path = public, extensions;
ALTER FUNCTION public.finalize_taxonomy_candidates(uuid)                                   SET search_path = public, extensions;
ALTER FUNCTION public.get_classification_vocabulary(text[], integer)                       SET search_path = public, extensions;

COMMIT;

-- =====================================================================
-- CHECK AFTERWARDS
--
--   -- exactly the 18 inlined SQL functions listed above
--   SELECT p.proname, l.lanname
--   FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--   JOIN pg_language l ON l.oid = p.prolang
--   WHERE n.nspname = 'public'
--     AND NOT EXISTS (SELECT 1 FROM unnest(coalesce(p.proconfig, '{}')) c
--                     WHERE c LIKE 'search_path=%')
--   ORDER BY 1;
--
-- ROLLBACK, per function:
--   ALTER FUNCTION public.<fn>(<args>) RESET search_path;
-- =====================================================================
