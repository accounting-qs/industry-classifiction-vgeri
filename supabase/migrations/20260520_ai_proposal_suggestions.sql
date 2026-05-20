-- Per-run AI routing suggestions for the "AI-proposed taxonomy additions"
-- panel. Populated by POST /api/bucketing/runs/:id/suggest-routings; read
-- inline with /proposed-tags. JSONB matches the existing pattern of
-- taxonomy_proposal / taxonomy_final on this table.
--
-- Blob shape (see ProposalSuggestionsBlob in services/bucketingService.ts):
--   { identities:     { "<name>":          Suggestion },
--     sub_identities: { "<name>|<parent>": Suggestion },
--     sectors:        { "<name>":          Suggestion },
--     _meta: { model, cost_usd, generated_at, counts } }
--
--   Suggestion = { route_to?, route_to_parent?, accept_as_new?,
--                  confidence: 1-10, reason: string }

ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS ai_proposal_suggestions JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Job status: NULL = idle, 'running' = job in flight, 'failed:<msg>' = last attempt failed.
-- Acts as a cross-process lock alongside the in-memory Set in server.ts.
ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS ai_proposal_suggestions_status TEXT;

NOTIFY pgrst, 'reload schema';
