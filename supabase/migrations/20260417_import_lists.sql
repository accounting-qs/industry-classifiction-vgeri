-- Import lists: track each CSV import with list name and contact count
CREATE TABLE IF NOT EXISTS import_lists (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  name TEXT NOT NULL,
  contact_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_import_lists_created_at ON import_lists (created_at DESC);
