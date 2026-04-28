-- v4.0: Align bucketing schema with the 5-layer Custom GPT v4 spec.
--
-- Adds two new truth-layer fields:
--   functional_core — broad function family between primary_identity and
--                     functional_specialization (e.g. "Marketing Services"
--                     under Agency, "Investment Services" under Financial
--                     Services).
--   sector_core     — broad served-market family above sector_focus
--                     (e.g. "Healthcare" as the broad family of "Medical
--                     Practice", "Behavioral Health", etc.).
--
-- These enable a 5-level rollup fallback (combo → sector_core+spec →
-- specialization → functional_core → identity) instead of the current
-- 3-level fallback that jumps straight from specialization to identity.
--
-- Also adds canonical_classification + bucket_reason persistence so the
-- enriched CSV export carries the spec's full output column set.

-- =====================================================================
-- 1. Add functional_core to taxonomy_characteristics + seed mapping
-- =====================================================================

ALTER TABLE taxonomy_characteristics
    ADD COLUMN IF NOT EXISTS functional_core TEXT;

-- 17 functional_cores, one or more per parent identity. Each
-- characteristic's parent_identity narrows it; functional_core is the
-- broader function family used for rollup before falling to identity.
UPDATE taxonomy_characteristics
SET functional_core = CASE
    -- Software & SaaS variants
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%FinTech%' THEN 'Payments / FinTech Infrastructure'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%Healthcare%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%AI%' THEN 'AI / Data Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%Data%' THEN 'AI / Data Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%Cybersecurity%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%Marketing%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%Legal%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%HR%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%Real Estate%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%Operations%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%Accounting%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' AND name ILIKE '%E-commerce%' THEN 'Software Platform'
    WHEN parent_identity = 'Software & SaaS' THEN 'Software Platform'
    -- Technology Services
    WHEN parent_identity = 'Technology Services' AND name ILIKE '%Cybersecurity%' THEN 'IT / Systems Services'
    WHEN parent_identity = 'Technology Services' AND name ILIKE '%Cloud%' THEN 'IT / Systems Services'
    WHEN parent_identity = 'Technology Services' AND name ILIKE '%Government%' THEN 'IT / Systems Services'
    WHEN parent_identity = 'Technology Services' AND (name ILIKE '%Consulting%' OR name ILIKE '%Transformation%') THEN 'Management / Strategy Advisory'
    WHEN parent_identity = 'Technology Services' AND name ILIKE '%Custom Software%' THEN 'Web / Product Development'
    WHEN parent_identity = 'Technology Services' THEN 'IT / Systems Services'
    -- Financial Services
    WHEN parent_identity = 'Financial Services' AND name ILIKE '%Advisory%' THEN 'Finance / Investment Advisory'
    WHEN parent_identity = 'Financial Services' AND name ILIKE '%M&A%' THEN 'Finance / Investment Advisory'
    WHEN parent_identity = 'Financial Services' AND name ILIKE '%Wealth%' THEN 'Finance / Investment Advisory'
    WHEN parent_identity = 'Financial Services' AND name ILIKE '%Family Office%' THEN 'Finance / Investment Advisory'
    WHEN parent_identity = 'Financial Services' AND name ILIKE '%Fund Admin%' THEN 'Finance / Investment Advisory'
    WHEN parent_identity = 'Financial Services' THEN 'Investment Services'
    -- Banking & Credit
    WHEN parent_identity = 'Banking & Credit' THEN 'Banking & Lending Services'
    -- Real Estate
    WHEN parent_identity = 'Real Estate' AND name ILIKE '%Brokerage%' THEN 'Real Estate Services'
    WHEN parent_identity = 'Real Estate' AND name ILIKE '%Property Management%' THEN 'Real Estate Services'
    WHEN parent_identity = 'Real Estate' AND name ILIKE '%Title%' THEN 'Real Estate Services'
    WHEN parent_identity = 'Real Estate' AND name ILIKE '%Development%' THEN 'Real Estate Operating Companies'
    WHEN parent_identity = 'Real Estate' AND name ILIKE '%Investment%' THEN 'Investment Services'
    WHEN parent_identity = 'Real Estate' AND name ILIKE '%Finance%' THEN 'Banking & Lending Services'
    WHEN parent_identity = 'Real Estate' THEN 'Real Estate Services'
    -- Legal
    WHEN parent_identity = 'Legal Services' THEN 'Legal / Compliance Services'
    -- Accounting & Tax
    WHEN parent_identity = 'Accounting & Tax' THEN 'Accounting / Finance Services'
    -- Insurance
    WHEN parent_identity = 'Insurance' THEN 'Insurance Services'
    -- Consulting & Advisory
    WHEN parent_identity = 'Consulting & Advisory' AND name ILIKE '%HR%' THEN 'HR / People Advisory'
    WHEN parent_identity = 'Consulting & Advisory' AND name ILIKE '%Benefits%' THEN 'HR / People Advisory'
    WHEN parent_identity = 'Consulting & Advisory' AND name ILIKE '%Operations%' THEN 'Management / Strategy Advisory'
    WHEN parent_identity = 'Consulting & Advisory' THEN 'Management / Strategy Advisory'
    -- Agency
    WHEN parent_identity = 'Agency' AND (name ILIKE '%Creative%' OR name ILIKE '%Branding%') THEN 'Creative / Brand Services'
    WHEN parent_identity = 'Agency' AND name ILIKE '%PR%' THEN 'Creative / Brand Services'
    WHEN parent_identity = 'Agency' THEN 'Marketing Services'
    -- Healthcare & Medical
    WHEN parent_identity = 'Healthcare & Medical' AND name ILIKE '%Digital Health%' THEN 'Software Platform'
    WHEN parent_identity = 'Healthcare & Medical' AND name ILIKE '%Device%' THEN 'Healthcare Manufacturing'
    WHEN parent_identity = 'Healthcare & Medical' AND name ILIKE '%Revenue Cycle%' THEN 'Healthcare Business Services'
    WHEN parent_identity = 'Healthcare & Medical' THEN 'Healthcare Services'
    -- Non-Profit & Associations
    WHEN parent_identity = 'Non-Profit & Associations' THEN 'Non-Profit / Association Services'
    -- Manufacturing & Industrial
    WHEN parent_identity = 'Manufacturing & Industrial' THEN 'Manufacturing & Industrial Operations'
    -- Staffing & Recruiting
    WHEN parent_identity = 'Staffing & Recruiting' THEN 'Recruiting / Talent Services'
    -- Construction & Engineering
    WHEN parent_identity = 'Construction & Engineering' THEN 'Construction / Engineering Services'
    -- Government Contractor
    WHEN parent_identity = 'Government Contractor' THEN 'Government Contracting Services'
    -- Consumer & Retail (DQ identity, but still tag a core for completeness)
    WHEN parent_identity = 'Consumer & Retail' THEN 'Consumer & Retail Operations'
    ELSE NULL
END
WHERE functional_core IS NULL;

-- =====================================================================
-- 2. Add sector_core to taxonomy_sectors + map existing sectors
-- =====================================================================

ALTER TABLE taxonomy_sectors
    ADD COLUMN IF NOT EXISTS sector_core TEXT;

-- 9 sector_cores grouping the 19 seed sectors. sector_core is the
-- broad served-market family — sector_focus narrows within it.
UPDATE taxonomy_sectors SET sector_core = CASE name
    WHEN 'Healthcare / Medical'         THEN 'Healthcare'
    WHEN 'Life Sciences & Biotech'      THEN 'Healthcare'
    WHEN 'Financial Services & FinTech' THEN 'Financial Services'
    WHEN 'Real Estate & Property'       THEN 'Real Estate'
    WHEN 'Legal'                        THEN 'Legal & Professional'
    WHEN 'Professional Services'        THEN 'Legal & Professional'
    WHEN 'Technology & Software'        THEN 'Technology'
    WHEN 'Manufacturing & Industrial'   THEN 'Industrial'
    WHEN 'Construction & Infrastructure' THEN 'Industrial'
    WHEN 'Energy & Utilities'           THEN 'Industrial'
    WHEN 'Government & Defense'         THEN 'Public Sector'
    WHEN 'Education & Training'         THEN 'Public Sector'
    WHEN 'Non-Profit & Social Impact'   THEN 'Public Sector'
    WHEN 'Retail & E-commerce'          THEN 'Consumer'
    WHEN 'Hospitality & Travel'         THEN 'Consumer'
    WHEN 'Media & Entertainment'        THEN 'Consumer'
    WHEN 'Transportation & Logistics'   THEN 'Logistics & Supply Chain'
    WHEN 'Environmental & Climate'      THEN 'Industrial'
    WHEN 'Agriculture & Food'           THEN 'Industrial'
    ELSE NULL
END
WHERE sector_core IS NULL;

-- =====================================================================
-- 3. Add tagging columns on bucket_industry_map (per-industry truth)
-- =====================================================================

ALTER TABLE bucket_industry_map
    ADD COLUMN IF NOT EXISTS functional_core TEXT,
    ADD COLUMN IF NOT EXISTS sector_core TEXT,
    ADD COLUMN IF NOT EXISTS is_new_functional_core BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_new_sector_core BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS canonical_classification TEXT;

-- =====================================================================
-- 4. Add per-contact persistence of canonical_classification +
--    bucket_reason on bucket_assignments + bucket_contact_map.
-- =====================================================================

ALTER TABLE bucket_assignments
    ADD COLUMN IF NOT EXISTS functional_core TEXT,
    ADD COLUMN IF NOT EXISTS sector_core TEXT,
    ADD COLUMN IF NOT EXISTS canonical_classification TEXT,
    ADD COLUMN IF NOT EXISTS bucket_reason TEXT;

ALTER TABLE bucket_contact_map
    ADD COLUMN IF NOT EXISTS functional_core TEXT,
    ADD COLUMN IF NOT EXISTS sector_core TEXT,
    ADD COLUMN IF NOT EXISTS canonical_classification TEXT,
    ADD COLUMN IF NOT EXISTS bucket_reason TEXT;

-- =====================================================================
-- 5. Generic Audit telemetry on bucketing_runs
-- =====================================================================

ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS generic_audit JSONB;

NOTIFY pgrst, 'reload schema';
