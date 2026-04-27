-- Bucketing v2: 3-level taxonomy (leaf → ancestor → root), per-row chain
-- with alignment scores, generic/disqualified flags, ICP filtering, and a
-- reusable bucket library shared across runs.
--
-- Migration shape:
--   1) Extend bucket_industry_map with the full chain + scores + ICP flags.
--      bucket_name (existing) becomes the EFFECTIVE bucket after volume rollup.
--   2) Create bucket_library + bucket_library_links (which run reused which).
--   3) Replace bucketing_deterministic_fanout to copy chain into assignments.
--   4) New volume-rollup RPC that walks leaf → ancestor → root → Generic/DQ.
--   5) New per-run bucket-counts RPC that knows about the chain.
--
-- Idempotent: ALTER … IF NOT EXISTS, CREATE OR REPLACE, ON CONFLICT.

-- ────────────────────────────────────────────────────────────
-- 0) bucketing_runs: add preferred_library_ids
-- ────────────────────────────────────────────────────────────

ALTER TABLE bucketing_runs ADD COLUMN IF NOT EXISTS preferred_library_ids UUID[] DEFAULT ARRAY[]::UUID[];

-- ────────────────────────────────────────────────────────────
-- 1) bucket_industry_map: store the full chain + scores
-- ────────────────────────────────────────────────────────────

ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS bucket_leaf      TEXT;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS bucket_ancestor  TEXT;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS bucket_root      TEXT;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS leaf_score       NUMERIC(3,2);
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS ancestor_score   NUMERIC(3,2);
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS root_score       NUMERIC(3,2);
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS is_generic       BOOLEAN DEFAULT false;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS is_disqualified  BOOLEAN DEFAULT false;
ALTER TABLE bucket_industry_map ADD COLUMN IF NOT EXISTS reasons          JSONB;

-- Same for assignments — useful for per-contact audit + downstream filtering.
ALTER TABLE bucket_assignments ADD COLUMN IF NOT EXISTS bucket_leaf      TEXT;
ALTER TABLE bucket_assignments ADD COLUMN IF NOT EXISTS bucket_ancestor  TEXT;
ALTER TABLE bucket_assignments ADD COLUMN IF NOT EXISTS bucket_root      TEXT;
ALTER TABLE bucket_assignments ADD COLUMN IF NOT EXISTS is_generic       BOOLEAN DEFAULT false;
ALTER TABLE bucket_assignments ADD COLUMN IF NOT EXISTS is_disqualified  BOOLEAN DEFAULT false;

-- ────────────────────────────────────────────────────────────
-- 2) bucket_library + link table
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bucket_library (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_name      TEXT NOT NULL UNIQUE,
    description      TEXT,
    direct_ancestor  TEXT,
    root_category    TEXT,
    include_terms    TEXT[] DEFAULT ARRAY[]::TEXT[],
    exclude_terms    TEXT[] DEFAULT ARRAY[]::TEXT[],
    example_strings  TEXT[] DEFAULT ARRAY[]::TEXT[],
    notes            TEXT,
    times_used       INTEGER DEFAULT 0,
    last_used_at     TIMESTAMPTZ,
    archived         BOOLEAN DEFAULT false,
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS bucket_library_active_idx
    ON bucket_library (archived, last_used_at DESC);

ALTER TABLE bucket_library DISABLE ROW LEVEL SECURITY;

-- Which library buckets a run reused, so we can tally usage.
CREATE TABLE IF NOT EXISTS bucket_library_run_links (
    bucketing_run_id UUID REFERENCES bucketing_runs(id) ON DELETE CASCADE,
    library_bucket_id UUID REFERENCES bucket_library(id) ON DELETE CASCADE,
    bucket_name_in_run TEXT NOT NULL,
    PRIMARY KEY (bucketing_run_id, library_bucket_id)
);

ALTER TABLE bucket_library_run_links DISABLE ROW LEVEL SECURITY;

-- ────────────────────────────────────────────────────────────
-- 3) Replace deterministic_fanout: write the full chain to assignments
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
        bucket_leaf, bucket_ancestor, bucket_root, is_generic, is_disqualified
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

-- ────────────────────────────────────────────────────────────
-- 4) Volume rollup — walk leaf → ancestor → root → Generic
-- ────────────────────────────────────────────────────────────
--
-- For each row in bucket_industry_map for this run, decide bucket_name
-- based on which level of the chain has enough total contact volume
-- (counted across the run's selected lists) to clear min_volume:
--   • disqualified → "Disqualified" (always, never rolls up)
--   • generic OR no leaf → "Generic"
--   • else: leaf if leaf_count >= min_volume
--           else ancestor if ancestor_count >= min_volume
--           else root if root_count >= min_volume
--           else "Generic"
-- bucket_name is what fan-out reads.

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
    v_list_names TEXT[];
BEGIN
    SELECT min_volume, list_names INTO v_min_volume, v_list_names
    FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'bucketing run % not found', p_run_id;
    END IF;
    v_min_volume := COALESCE(v_min_volume, 0);

    -- Per-(industry_string) contact count for this run, computed once.
    DROP TABLE IF EXISTS _vol_pairs;
    CREATE TEMP TABLE _vol_pairs ON COMMIT DROP AS
    SELECT m.industry_string,
           m.bucket_leaf, m.bucket_ancestor, m.bucket_root,
           COALESCE(m.is_generic, false) AS is_generic,
           COALESCE(m.is_disqualified, false) AS is_disqualified,
           COUNT(c.contact_id)::BIGINT AS n
    FROM bucket_industry_map m
    JOIN contacts c ON c.industry = m.industry_string
    WHERE m.bucketing_run_id = p_run_id
      AND c.lead_list_name = ANY(v_list_names)
    GROUP BY m.industry_string, m.bucket_leaf, m.bucket_ancestor, m.bucket_root,
             m.is_generic, m.is_disqualified;

    -- Aggregate counts per leaf, ancestor, root.
    DROP TABLE IF EXISTS _leaf_counts;
    CREATE TEMP TABLE _leaf_counts ON COMMIT DROP AS
    SELECT bucket_leaf AS name, SUM(n)::BIGINT AS n
    FROM _vol_pairs
    WHERE NOT is_generic AND NOT is_disqualified AND bucket_leaf IS NOT NULL AND bucket_leaf <> ''
    GROUP BY bucket_leaf;

    DROP TABLE IF EXISTS _ancestor_counts;
    CREATE TEMP TABLE _ancestor_counts ON COMMIT DROP AS
    SELECT bucket_ancestor AS name, SUM(n)::BIGINT AS n
    FROM _vol_pairs
    WHERE NOT is_generic AND NOT is_disqualified AND bucket_ancestor IS NOT NULL AND bucket_ancestor <> ''
    GROUP BY bucket_ancestor;

    DROP TABLE IF EXISTS _root_counts;
    CREATE TEMP TABLE _root_counts ON COMMIT DROP AS
    SELECT bucket_root AS name, SUM(n)::BIGINT AS n
    FROM _vol_pairs
    WHERE NOT is_generic AND NOT is_disqualified AND bucket_root IS NOT NULL AND bucket_root <> ''
    GROUP BY bucket_root;

    -- Decide effective bucket per industry_string.
    UPDATE bucket_industry_map m
    SET bucket_name = CASE
        WHEN COALESCE(m.is_disqualified, false) THEN 'Disqualified'
        WHEN COALESCE(m.is_generic, false) OR m.bucket_leaf IS NULL OR m.bucket_leaf = '' THEN 'Generic'
        WHEN (SELECT n FROM _leaf_counts WHERE name = m.bucket_leaf) >= v_min_volume
            THEN m.bucket_leaf
        WHEN m.bucket_ancestor IS NOT NULL AND m.bucket_ancestor <> ''
            AND (SELECT n FROM _ancestor_counts WHERE name = m.bucket_ancestor) >= v_min_volume
            THEN m.bucket_ancestor
        WHEN m.bucket_root IS NOT NULL AND m.bucket_root <> ''
            AND (SELECT n FROM _root_counts WHERE name = m.bucket_root) >= v_min_volume
            THEN m.bucket_root
        ELSE 'Generic'
    END
    WHERE m.bucketing_run_id = p_run_id;

    -- Return per-level summary so the caller can log it.
    RETURN QUERY
        SELECT 'leaf'::TEXT, name, n FROM _leaf_counts
        UNION ALL
        SELECT 'ancestor'::TEXT, name, n FROM _ancestor_counts
        UNION ALL
        SELECT 'root'::TEXT, name, n FROM _root_counts;
END;
$$;

GRANT EXECUTE ON FUNCTION public.bucketing_apply_volume_rollup(UUID)
    TO anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────
-- 5) Counts that surface chain levels
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_bucket_chain_counts(p_run_id UUID)
RETURNS TABLE (
    bucket_leaf      TEXT,
    bucket_ancestor  TEXT,
    bucket_root      TEXT,
    contact_count    BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT
        ba.bucket_leaf,
        ba.bucket_ancestor,
        ba.bucket_root,
        COUNT(*)::BIGINT
    FROM bucket_assignments ba
    WHERE ba.bucketing_run_id = p_run_id
    GROUP BY ba.bucket_leaf, ba.bucket_ancestor, ba.bucket_root;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_chain_counts(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
