-- Index contacts.lead_list_name so per-list filtering (enrich/export/list view)
-- uses an index scan instead of a full table scan.
CREATE INDEX IF NOT EXISTS idx_contacts_lead_list_name
  ON contacts (lead_list_name);
