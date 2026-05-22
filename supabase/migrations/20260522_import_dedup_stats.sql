-- Dedup stats per imported list. Captured at upload time and frozen — the
-- breakdown reflects which source list each duplicate's existing contact
-- belonged to at the moment of import, even if a later overwrite re-tags
-- the contact to a different list.
--
-- One row per import_list. Upserted by import_list_id so a re-upload that
-- folds into the same import_lists row (idempotent-on-name path in
-- POST /api/import-lists) accumulates rather than replaces.
CREATE TABLE IF NOT EXISTS import_dedup_stats (
  import_list_id UUID PRIMARY KEY REFERENCES import_lists(id) ON DELETE CASCADE,
  total_rows INTEGER NOT NULL DEFAULT 0,
  inserted INTEGER NOT NULL DEFAULT 0,
  updated INTEGER NOT NULL DEFAULT 0,
  within_file_dupes INTEGER NOT NULL DEFAULT 0,
  cross_list_dupes INTEGER NOT NULL DEFAULT 0,
  invalid INTEGER NOT NULL DEFAULT 0,
  -- { "List A": 234, "List B": 102 } — counts of how many duplicates came
  -- from each existing list. Does not include the within_file_dupes bucket.
  source_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
  -- { "Missing @ symbol": 3, "Contains invalid character: \" \"": 1 }
  invalid_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
