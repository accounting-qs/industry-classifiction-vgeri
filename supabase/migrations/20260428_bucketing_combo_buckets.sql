-- Bucketing v2.2 — 4-layer model with combination campaign buckets.
--
-- Layer 1: primary_identity        (Agency, Consulting, Software & SaaS, …)
-- Layer 2: functional_specialization (SEO Agency, Private Equity, …)
-- Layer 3: sector_focus            (Healthcare, Real Estate, …) — optional
-- Layer 4: campaign_bucket         (decided here in SQL by combination + threshold)
--
-- Routing rule, applied per industry_string:
--   1) Disqualified  → "Disqualified"
--   2) Generic / no specialization → "Generic"
--   3) "{sector_focus} {specialization}" if combo count >= min_volume
--   4) specialization                if specialization count >= min_volume
--   5) primary_identity              if identity count >= min_volume
--   6) "Generic"
--
-- Plus bucket budget: if total distinct effective buckets exceeds
-- bucket_budget, the smallest are iteratively rolled up to the next
-- level until the count fits.

ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS primary_identity         TEXT;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS functional_specialization TEXT;
ALTER TABLE bucket_assignments  ADD COLUMN IF NOT EXISTS primary_identity         TEXT;
ALTER TABLE bucket_assignments  ADD COLUMN IF NOT EXISTS functional_specialization TEXT;

ALTER TABLE bucketing_runs ADD COLUMN IF NOT EXISTS bucket_budget INTEGER DEFAULT 30;

-- ────────────────────────────────────────────────────────────
-- New volume rollup with combination buckets + bucket budget.
-- ────────────────────────────────────────────────────────────

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

    -- Per-industry contact counts for this run, computed once.
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

    -- Counts per (specialization × sector) combo.
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
    -- Combo > Specialization > Identity > Generic. Disqualified always wins.
    UPDATE bucket_industry_map m
    SET bucket_name = CASE
        WHEN COALESCE(m.is_disqualified, false) THEN 'Disqualified'
        WHEN COALESCE(m.is_generic, false)
            OR m.functional_specialization IS NULL
            OR m.functional_specialization = ''
        THEN 'Generic'
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
        ELSE 'Generic'
    END
    WHERE m.bucketing_run_id = p_run_id;

    -- Step 2: enforce bucket budget. Iteratively roll up the smallest
    -- non-reserved bucket until the count of distinct campaign buckets
    -- fits within v_bucket_budget. Reserved buckets (Generic, Disqualified)
    -- are exempt from the budget cap and cannot be rolled up.
    LOOP
        v_safety := v_safety + 1;
        IF v_safety > 200 THEN EXIT; END IF;

        SELECT COUNT(DISTINCT bucket_name) INTO v_distinct_count
        FROM bucket_industry_map
        WHERE bucketing_run_id = p_run_id
          AND bucket_name NOT IN ('Generic', 'Disqualified');

        EXIT WHEN v_distinct_count <= v_bucket_budget;

        SELECT bucket_name INTO v_smallest_bucket
        FROM (
            SELECT m.bucket_name, COUNT(c.contact_id)::BIGINT AS n
            FROM bucket_industry_map m
            JOIN contacts c ON c.industry = m.industry_string
            WHERE m.bucketing_run_id = p_run_id
              AND m.bucket_name NOT IN ('Generic', 'Disqualified')
              AND c.lead_list_name = ANY(v_list_names)
            GROUP BY m.bucket_name
            ORDER BY n ASC
            LIMIT 1
        ) sub;

        EXIT WHEN v_smallest_bucket IS NULL;

        -- Roll the smallest bucket up to the next level.
        --   Combo "{Sector} {Spec}" → Spec
        --   Spec → Identity
        --   Identity → Generic
        UPDATE bucket_industry_map m
        SET bucket_name = CASE
            WHEN m.bucket_name = m.sector_focus || ' ' || m.functional_specialization
                THEN m.functional_specialization
            WHEN m.bucket_name = m.functional_specialization
                THEN COALESCE(NULLIF(m.primary_identity, ''), 'Generic')
            WHEN m.bucket_name = m.primary_identity
                THEN 'Generic'
            ELSE 'Generic'
        END
        WHERE m.bucketing_run_id = p_run_id
          AND m.bucket_name = v_smallest_bucket;
    END LOOP;

    -- Surface the per-level counts for logging.
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

-- ────────────────────────────────────────────────────────────
-- Update fanout to copy primary_identity + functional_specialization too.
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.bucketing_deterministic_fanout(p_run_id UUID)
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

    INSERT INTO bucket_assignments (
        bucketing_run_id, contact_id, bucket_name, source, confidence,
        bucket_leaf, bucket_ancestor, bucket_root,
        primary_identity, functional_specialization, sector_focus,
        is_generic, is_disqualified
    )
    SELECT
        p_run_id,
        c.contact_id,
        m.bucket_name,
        CASE
            WHEN m.source IN ('llm_phase1', 'manual') THEN 'deterministic'
            ELSE m.source
        END,
        COALESCE(m.confidence, 1.0),
        m.bucket_leaf,
        m.bucket_ancestor,
        m.bucket_root,
        m.primary_identity,
        m.functional_specialization,
        m.sector_focus,
        COALESCE(m.is_generic, false),
        COALESCE(m.is_disqualified, false)
    FROM contacts c
    JOIN bucket_industry_map m
      ON m.industry_string = c.industry
     AND m.bucketing_run_id = p_run_id
    WHERE c.lead_list_name = ANY(v_list_names)
    ON CONFLICT (bucketing_run_id, contact_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    RETURN inserted;
END;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_deterministic_fanout(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
