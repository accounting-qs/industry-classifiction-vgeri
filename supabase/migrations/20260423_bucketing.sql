-- Bucketing feature: per-campaign taxonomy + assignments.
--
-- Three tables:
--   bucketing_runs        — one row per bucketing job (taxonomy + status)
--   bucket_industry_map   — vocabulary → bucket lookup, scoped per run
--   bucket_assignments    — final per-contact assignment, scoped per run
--
-- Plus get_industry_vocabulary(p_list_names) RPC, used by Phase 1 to pull the
-- distinct industry strings (with sample companies + reasoning) we feed to the
-- LLM. Function-attribute statement_timeout matches the pattern already used
-- by get_list_enrichment_stats — the LEFT JOIN + GROUP BY against contacts +
-- enrichments runs past the 8s default on large lists.

CREATE TABLE IF NOT EXISTS bucketing_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    list_names TEXT[] NOT NULL,
    min_volume INTEGER NOT NULL DEFAULT 50,
    status TEXT NOT NULL,
    -- 'taxonomy_pending' | 'taxonomy_ready' | 'assigning' | 'completed' | 'failed'
    taxonomy_model TEXT,
    taxonomy_proposal JSONB,
    taxonomy_final JSONB,
    total_contacts INTEGER,
    assigned_contacts INTEGER DEFAULT 0,
    cost_usd NUMERIC(10, 4) DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    taxonomy_completed_at TIMESTAMPTZ,
    assignment_completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS bucketing_runs_created_at_idx
    ON bucketing_runs (created_at DESC);

CREATE TABLE IF NOT EXISTS bucket_industry_map (
    bucketing_run_id UUID NOT NULL REFERENCES bucketing_runs(id) ON DELETE CASCADE,
    industry_string TEXT NOT NULL,
    bucket_name TEXT NOT NULL,
    source TEXT NOT NULL,
    -- 'llm_phase1' | 'embedding' | 'llm_phase2' | 'manual'
    confidence NUMERIC(4, 2),
    PRIMARY KEY (bucketing_run_id, industry_string)
);

CREATE INDEX IF NOT EXISTS bucket_industry_map_run_bucket_idx
    ON bucket_industry_map (bucketing_run_id, bucket_name);

CREATE TABLE IF NOT EXISTS bucket_assignments (
    bucketing_run_id UUID NOT NULL REFERENCES bucketing_runs(id) ON DELETE CASCADE,
    contact_id TEXT NOT NULL,
    bucket_name TEXT NOT NULL,
    source TEXT NOT NULL,
    -- 'deterministic' | 'embedding' | 'llm_phase2' | 'other'
    confidence NUMERIC(4, 2),
    assigned_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (bucketing_run_id, contact_id)
);

CREATE INDEX IF NOT EXISTS bucket_assignments_run_bucket_idx
    ON bucket_assignments (bucketing_run_id, bucket_name);

-- Phase 1 vocabulary extraction. Returns one row per distinct industry string
-- across the selected lead lists, with row count, average confidence, and a
-- few sample company names + reasoning strings for LLM prompt context.
CREATE OR REPLACE FUNCTION public.get_industry_vocabulary(p_list_names TEXT[])
RETURNS TABLE (
    industry         TEXT,
    n                BIGINT,
    avg_conf         NUMERIC,
    sample_companies TEXT[],
    sample_reasoning TEXT[]
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT
        c.industry,
        COUNT(*)::BIGINT                                                                     AS n,
        AVG(e.confidence)::NUMERIC                                                           AS avg_conf,
        (ARRAY_AGG(c.company_name   ORDER BY e.confidence DESC NULLS LAST))[1:3]             AS sample_companies,
        (ARRAY_AGG(e.reasoning      ORDER BY e.confidence DESC NULLS LAST))[1:2]             AS sample_reasoning
    FROM contacts c
    JOIN enrichments e ON e.contact_id = c.contact_id
    WHERE c.lead_list_name = ANY(p_list_names)
      AND e.status = 'completed'
      AND c.industry IS NOT NULL
      AND c.industry <> ''
    GROUP BY c.industry
    ORDER BY n DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_industry_vocabulary(TEXT[])
    TO anon, authenticated, service_role;

-- Per-bucket counts for a run. Aggregates the deterministic map against the
-- contacts table — used by the review screen to show real bucket sizes
-- (vs. the LLM's estimated_count) before the user applies the threshold.
CREATE OR REPLACE FUNCTION public.get_bucket_map_counts(p_run_id UUID)
RETURNS TABLE (
    bucket_name     TEXT,
    contact_count   BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT
        m.bucket_name,
        COUNT(*)::BIGINT AS contact_count
    FROM bucket_industry_map m
    JOIN contacts c ON c.industry = m.industry_string
    WHERE m.bucketing_run_id = p_run_id
      AND c.lead_list_name = ANY(
          SELECT UNNEST(list_names) FROM bucketing_runs WHERE id = p_run_id
      )
    GROUP BY m.bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_map_counts(UUID)
    TO anon, authenticated, service_role;

-- Per-bucket counts of final assignments. Used by the results view.
CREATE OR REPLACE FUNCTION public.get_bucket_assignment_counts(p_run_id UUID)
RETURNS TABLE (
    bucket_name     TEXT,
    contact_count   BIGINT,
    other_sources   JSONB
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT
        bucket_name,
        COUNT(*)::BIGINT AS contact_count,
        jsonb_object_agg(source, source_count) AS other_sources
    FROM (
        SELECT bucket_name, source, COUNT(*) AS source_count
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY bucket_name, source
    ) sub
    GROUP BY bucket_name;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_assignment_counts(UUID)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
