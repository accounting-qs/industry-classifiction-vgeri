-- v2.3 fixes from team review:
--   1) Collapse "Generic" + "Disqualified" + legacy "Other" into a single
--      catch-all bucket named "General". is_disqualified column is kept for
--      audit but no longer drives a separate bucket.
--   2) Extend bucket_library with primary_identity + functional_specialization
--      so saved buckets fit the new model. Old direct_ancestor / root_category
--      columns stay for backward compat but are no longer used.
--   3) Update bucketing_apply_volume_rollup and bucketing_catchall_other to
--      use 'General' everywhere.
--
-- Idempotent.

-- 1) Library schema extension
ALTER TABLE bucket_library
    ADD COLUMN IF NOT EXISTS primary_identity         TEXT,
    ADD COLUMN IF NOT EXISTS functional_specialization TEXT;

-- Backfill from old columns where useful (only on rows that have nothing in
-- the new columns yet; this is safe to run repeatedly).
UPDATE bucket_library
   SET functional_specialization = bucket_name
 WHERE functional_specialization IS NULL OR functional_specialization = '';

UPDATE bucket_library
   SET primary_identity = direct_ancestor
 WHERE (primary_identity IS NULL OR primary_identity = '')
   AND direct_ancestor IS NOT NULL AND direct_ancestor <> '';

-- 2) Rename existing campaign buckets
UPDATE bucket_industry_map
   SET bucket_name = 'General'
 WHERE bucket_name IN ('Generic', 'Disqualified', 'Other');

UPDATE bucket_assignments
   SET bucket_name = 'General'
 WHERE bucket_name IN ('Generic', 'Disqualified', 'Other');

-- 3) Replace the volume rollup to emit 'General' instead of Generic / Disqualified
CREATE OR REPLACE FUNCTION public.bucketing_apply_volume_rollup(p_run_id UUID)
RETURNS TABLE (
    level TEXT,
    bucket_name TEXT,
    contact_count BIGINT
)
LANGUAGE plpgsql
SET statement_timeout TO '180s'
AS $$
DECLARE
    v_min_volume INTEGER;
    v_bucket_budget INTEGER;
    v_list_names TEXT[];
    v_distinct_count INTEGER;
    v_smallest_bucket TEXT;
    v_safety INTEGER := 0;
BEGIN
    SELECT min_volume, COALESCE(bucket_budget, 30), list_names
      INTO v_min_volume, v_bucket_budget, v_list_names
    FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'bucketing run % not found', p_run_id;
    END IF;
    v_min_volume := COALESCE(v_min_volume, 0);

    DROP TABLE IF EXISTS _vol_pairs;
    CREATE TEMP TABLE _vol_pairs ON COMMIT DROP AS
    SELECT m.industry_string,
           m.primary_identity,
           m.functional_specialization,
           m.sector_focus,
           COALESCE(m.is_generic, false) AS is_generic,
           COALESCE(m.is_disqualified, false) AS is_disqualified,
           COUNT(c.contact_id)::BIGINT AS n
    FROM bucket_industry_map m
    JOIN contacts c ON c.industry = m.industry_string
    WHERE m.bucketing_run_id = p_run_id
      AND c.lead_list_name = ANY(v_list_names)
    GROUP BY m.industry_string, m.primary_identity, m.functional_specialization,
             m.sector_focus, m.is_generic, m.is_disqualified;

    DROP TABLE IF EXISTS _combo_counts;
    CREATE TEMP TABLE _combo_counts ON COMMIT DROP AS
    SELECT functional_specialization AS spec,
           sector_focus AS sec,
           SUM(n)::BIGINT AS n
    FROM _vol_pairs
    WHERE NOT is_generic AND NOT is_disqualified
      AND functional_specialization IS NOT NULL AND functional_specialization <> ''
      AND sector_focus IS NOT NULL AND sector_focus <> '' AND sector_focus <> 'Multi-industry'
    GROUP BY functional_specialization, sector_focus;

    DROP TABLE IF EXISTS _spec_counts;
    CREATE TEMP TABLE _spec_counts ON COMMIT DROP AS
    SELECT functional_specialization AS spec, SUM(n)::BIGINT AS n
    FROM _vol_pairs
    WHERE NOT is_generic AND NOT is_disqualified
      AND functional_specialization IS NOT NULL AND functional_specialization <> ''
    GROUP BY functional_specialization;

    DROP TABLE IF EXISTS _ident_counts;
    CREATE TEMP TABLE _ident_counts ON COMMIT DROP AS
    SELECT primary_identity AS ident, SUM(n)::BIGINT AS n
    FROM _vol_pairs
    WHERE NOT is_generic AND NOT is_disqualified
      AND primary_identity IS NOT NULL AND primary_identity <> ''
    GROUP BY primary_identity;

    -- Step 1: assign initial campaign bucket per industry_string.
    -- Disqualified and Generic both fall into 'General'.
    UPDATE bucket_industry_map m
    SET bucket_name = CASE
        WHEN COALESCE(m.is_disqualified, false)
            OR COALESCE(m.is_generic, false)
            OR m.functional_specialization IS NULL
            OR m.functional_specialization = ''
        THEN 'General'
        WHEN m.sector_focus IS NOT NULL AND m.sector_focus <> '' AND m.sector_focus <> 'Multi-industry'
            AND COALESCE((SELECT n FROM _combo_counts
                          WHERE spec = m.functional_specialization AND sec = m.sector_focus), 0)
                >= v_min_volume
        THEN m.sector_focus || ' ' || m.functional_specialization
        WHEN COALESCE((SELECT n FROM _spec_counts WHERE spec = m.functional_specialization), 0)
                >= v_min_volume
        THEN m.functional_specialization
        WHEN m.primary_identity IS NOT NULL AND m.primary_identity <> ''
            AND COALESCE((SELECT n FROM _ident_counts WHERE ident = m.primary_identity), 0)
                >= v_min_volume
        THEN m.primary_identity
        ELSE 'General'
    END
    WHERE m.bucketing_run_id = p_run_id;

    -- Step 2: enforce bucket budget. Iteratively roll up the smallest
    -- non-General bucket until the count fits.
    LOOP
        v_safety := v_safety + 1;
        IF v_safety > 200 THEN EXIT; END IF;

        SELECT COUNT(DISTINCT bucket_name) INTO v_distinct_count
        FROM bucket_industry_map
        WHERE bucketing_run_id = p_run_id
          AND bucket_name <> 'General';

        EXIT WHEN v_distinct_count <= v_bucket_budget;

        SELECT bucket_name INTO v_smallest_bucket
        FROM (
            SELECT m.bucket_name, COUNT(c.contact_id)::BIGINT AS n
            FROM bucket_industry_map m
            JOIN contacts c ON c.industry = m.industry_string
            WHERE m.bucketing_run_id = p_run_id
              AND m.bucket_name <> 'General'
              AND c.lead_list_name = ANY(v_list_names)
            GROUP BY m.bucket_name
            ORDER BY n ASC
            LIMIT 1
        ) sub;

        EXIT WHEN v_smallest_bucket IS NULL;

        UPDATE bucket_industry_map m
        SET bucket_name = CASE
            WHEN m.bucket_name = m.sector_focus || ' ' || m.functional_specialization
                THEN m.functional_specialization
            WHEN m.bucket_name = m.functional_specialization
                THEN COALESCE(NULLIF(m.primary_identity, ''), 'General')
            WHEN m.bucket_name = m.primary_identity
                THEN 'General'
            ELSE 'General'
        END
        WHERE m.bucketing_run_id = p_run_id
          AND m.bucket_name = v_smallest_bucket;
    END LOOP;

    RETURN QUERY
        SELECT 'combo'::TEXT, spec || ' × ' || sec, n FROM _combo_counts
        UNION ALL
        SELECT 'specialization'::TEXT, spec, n FROM _spec_counts
        UNION ALL
        SELECT 'identity'::TEXT, ident, n FROM _ident_counts;
END;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_apply_volume_rollup(UUID)
    TO anon, authenticated, service_role;

-- 4) Replace catchall to write 'General' instead of 'Other'
CREATE OR REPLACE FUNCTION public.bucketing_catchall_other(p_run_id UUID)
RETURNS BIGINT
LANGUAGE plpgsql
SET statement_timeout TO '180s'
AS $$
DECLARE
    inserted BIGINT;
    v_list_names TEXT[];
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'bucketing run % not found', p_run_id;
    END IF;

    INSERT INTO bucket_assignments (bucketing_run_id, contact_id, bucket_name, source, confidence)
    SELECT p_run_id, c.contact_id, 'General', 'catchall', 0
    FROM contacts c
    WHERE c.lead_list_name = ANY(v_list_names)
    ON CONFLICT (bucketing_run_id, contact_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    RETURN inserted;
END;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_catchall_other(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
