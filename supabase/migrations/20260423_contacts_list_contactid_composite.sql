-- Composite index so `WHERE lead_list_name = $1 ORDER BY contact_id LIMIT N`
-- (the export paginator) can satisfy both the filter and the order from a
-- single index scan. Without this, Postgres alternates between
-- `idx_contacts_lead_list_name` + in-memory sort and the PK + full filter,
-- and picks the slow plan often enough to trip the 8s statement_timeout on
-- 40k–50k-row lists.
CREATE INDEX IF NOT EXISTS idx_contacts_list_contactid
  ON contacts (lead_list_name, contact_id);
