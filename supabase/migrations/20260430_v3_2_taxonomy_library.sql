-- v3.2: Taxonomy library + classification field + missing diagnostic RPCs.
--
-- Phase 1a is rewritten to tag each distinct industry against an editable
-- library of Identities / Characteristics / Sectors instead of inventing
-- buckets each run.
--
-- This migration ALSO carries the two RPCs from 20260429_v3_1 that
-- partially failed to apply (get_bucket_general_breakdown and
-- get_bucketing_run_diagnostics) so this single file brings the DB up to
-- date.

-- =====================================================================
-- 1. Taxonomy library tables
-- =====================================================================

CREATE TABLE IF NOT EXISTS taxonomy_identities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    is_disqualified BOOLEAN NOT NULL DEFAULT false,
    created_by TEXT NOT NULL CHECK (created_by IN ('seed','user','ai')),
    archived BOOLEAN NOT NULL DEFAULT false,
    sort_order INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS taxonomy_characteristics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    parent_identity TEXT NOT NULL,
    description TEXT,
    created_by TEXT NOT NULL CHECK (created_by IN ('seed','user','ai')),
    archived BOOLEAN NOT NULL DEFAULT false,
    sort_order INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS taxonomy_sectors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    synonyms TEXT,
    description TEXT,
    created_by TEXT NOT NULL CHECK (created_by IN ('seed','user','ai')),
    archived BOOLEAN NOT NULL DEFAULT false,
    sort_order INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE taxonomy_identities       DISABLE ROW LEVEL SECURITY;
ALTER TABLE taxonomy_characteristics  DISABLE ROW LEVEL SECURITY;
ALTER TABLE taxonomy_sectors          DISABLE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS taxonomy_characteristics_parent_idx
    ON taxonomy_characteristics (parent_identity);

-- =====================================================================
-- 2. Seed library — user-confirmed reference (17 / 99 / 19)
-- =====================================================================
-- Seeded as created_by='user' so the user owns them and can edit/archive.

-- Identities
INSERT INTO taxonomy_identities (name, description, is_disqualified, created_by, sort_order) VALUES
    ('Software & SaaS',          'B2B SaaS, AI platforms, enterprise software, cloud products', false, 'user', 1),
    ('Technology Services',      'IT MSPs, custom dev, cybersecurity services, digital transformation', false, 'user', 2),
    ('Financial Services',       'PE, VC, investment banks, M&A advisory, asset managers, hedge funds, wealth management', false, 'user', 3),
    ('Banking & Credit',         'Community banks, credit unions, mortgage lenders, specialty lenders', false, 'user', 4),
    ('Real Estate',              'Brokerages, development, investment firms, property management, title companies', false, 'user', 5),
    ('Legal Services',           'All law firms regardless of specialty', false, 'user', 6),
    ('Accounting & Tax',         'CPA firms, outsourced CFO, tax advisory, bookkeeping, audit', false, 'user', 7),
    ('Insurance',                'Independent agencies, commercial brokerages, MGAs, underwriters', false, 'user', 8),
    ('Consulting & Advisory',    'Management consulting, strategy, operations, employee benefits advisory', false, 'user', 9),
    ('Agency',                   'Marketing, creative, PR, digital, performance, SEO agencies', false, 'user', 10),
    ('Healthcare & Medical',     'Practices, clinics, medical devices, digital health, care services', false, 'user', 11),
    ('Non-Profit & Associations','Nonprofits, trade associations, foundations, advocacy orgs', false, 'user', 12),
    ('Manufacturing & Industrial','Physical product manufacturers, industrial operators, contract manufacturing', false, 'user', 13),
    ('Staffing & Recruiting',    'Executive search, staffing agencies, RPO firms', false, 'user', 14),
    ('Construction & Engineering','General contractors, civil engineering, MEP engineering, construction management', false, 'user', 15),
    ('Government Contractor',    'Companies that sell TO government: federal/defense IT, public sector services', false, 'user', 16),
    ('Consumer & Retail',        'DTC brands, retail shops, e-commerce, wholesale, hospitality, luxury consumer', true, 'user', 17)
ON CONFLICT (name) DO NOTHING;

-- Characteristics
INSERT INTO taxonomy_characteristics (name, parent_identity, description, created_by, sort_order) VALUES
    ('FinTech SaaS',                       'Software & SaaS',          'Payment processing, lending tech, financial compliance, banking software', 'user', 1),
    ('Healthcare SaaS',                    'Software & SaaS',          'EMR, revenue cycle, patient management, clinical workflow', 'user', 2),
    ('Legal Tech SaaS',                    'Software & SaaS',          'Case management, billing, e-discovery, contract management software', 'user', 3),
    ('Real Estate Tech SaaS',              'Software & SaaS',          'Property management, CRE platforms, MLS tools, RE transaction software', 'user', 4),
    ('HR & Workforce SaaS',                'Software & SaaS',          'Employee benefits platforms, HR systems, payroll, workforce management', 'user', 5),
    ('Operations & Logistics SaaS',        'Software & SaaS',          '3PL, supply chain, fleet management, fulfillment, WMS', 'user', 6),
    ('Marketing & Sales SaaS',             'Software & SaaS',          'CRM, marketing automation, lead gen, sales enablement', 'user', 7),
    ('Cybersecurity SaaS',                 'Software & SaaS',          'Security platforms, GRC, compliance automation, identity management', 'user', 8),
    ('Data & Analytics SaaS',              'Software & SaaS',          'BI platforms, data pipelines, analytics tools', 'user', 9),
    ('AI Infrastructure Platform',         'Software & SaaS',          'AI-first companies building foundational AI tools, agents, or infrastructure', 'user', 10),
    ('Vertical AI SaaS',                   'Software & SaaS',          'AI-powered software for a specific vertical (not general-purpose)', 'user', 11),
    ('E-commerce & Retail SaaS',           'Software & SaaS',          'Shopify apps, e-commerce platforms, retail tech, POS systems', 'user', 12),
    ('Accounting & Finance SaaS',          'Software & SaaS',          'Accounting automation, FP&A, treasury, bookkeeping software', 'user', 13),
    ('General B2B SaaS',                   'Software & SaaS',          'B2B SaaS that does not fit a specific vertical category', 'user', 14),
    ('IT Managed Services (MSP)',          'Technology Services',      'Managed IT provider for SMBs or enterprises; help desk, network, cloud mgmt', 'user', 15),
    ('Cybersecurity Services',             'Technology Services',      'Penetration testing, MSSP, security consulting, incident response', 'user', 16),
    ('Custom Software Development',        'Technology Services',      'Bespoke app/web/mobile dev for clients; software shops', 'user', 17),
    ('IT Consulting',                      'Technology Services',      'Technology strategy, architecture, IT advisory — not a product', 'user', 18),
    ('Cloud Services & Infrastructure',    'Technology Services',      'Cloud hosting, migration, infrastructure management, data center', 'user', 19),
    ('Digital Transformation Consulting',  'Technology Services',      'Enterprise DX, ERP implementations, systems integration, DevOps consulting', 'user', 20),
    ('Government IT Services',             'Technology Services',      'IT services delivered specifically to government clients', 'user', 21),
    ('Private Equity Firm',                'Financial Services',       'Buyout, growth equity, lower/middle market PE', 'user', 22),
    ('Venture Capital Firm',               'Financial Services',       'Early/growth stage venture investing', 'user', 23),
    ('Investment Bank / M&A Advisory',     'Financial Services',       'M&A advisory, sell-side/buy-side, capital raising, ESOP advisory', 'user', 24),
    ('Asset Management / Hedge Fund',      'Financial Services',       'Long/short equity, alt strategies, institutional asset management', 'user', 25),
    ('Wealth Management / Financial Advisory','Financial Services',    'RIA, financial planning, retirement advisory for HNW individuals', 'user', 26),
    ('Family Office',                      'Financial Services',       'Single or multi-family office; private wealth management for families', 'user', 27),
    ('Alternative Investment Manager',     'Financial Services',       'Private credit, real assets, infrastructure, fund of funds, interval funds', 'user', 28),
    ('Fund Administration / Placement',    'Financial Services',       'Fund admin, placement agents, capital intro, SPV management', 'user', 29),
    ('Community Bank',                     'Banking & Credit',         'Small-to-mid community banks offering retail and business banking', 'user', 30),
    ('Commercial Bank',                    'Banking & Credit',         'Regional or national commercial banks', 'user', 31),
    ('Credit Union',                       'Banking & Credit',         'Member-owned credit unions', 'user', 32),
    ('Specialty Lender / Mortgage',        'Banking & Credit',         'Non-bank lenders, bridge lenders, mortgage brokers, equipment finance', 'user', 33),
    ('Commercial RE Brokerage',            'Real Estate',              'CRE brokerage for office, retail, industrial, investment properties', 'user', 34),
    ('Residential RE Brokerage',           'Real Estate',              'Residential sales, apartment rentals, residential property agents', 'user', 35),
    ('RE Development',                     'Real Estate',              'Ground-up development: residential, multifamily, mixed-use, commercial', 'user', 36),
    ('RE Investment / Private Equity',     'Real Estate',              'RE private equity, REITs, real asset investing', 'user', 37),
    ('Property Management',                'Real Estate',              'Third-party property management for residential or commercial', 'user', 38),
    ('Title & Escrow',                     'Real Estate',              'Title insurance, escrow services, RE settlement', 'user', 39),
    ('RE Finance / Bridge Lending',        'Real Estate',              'CRE mortgage banking, bridge loans, construction lending', 'user', 40),
    ('Full-Service Law Firm',              'Legal Services',           'General practice or multi-practice law firm', 'user', 41),
    ('Personal Injury Law Firm',           'Legal Services',           'PI, auto accidents, medical malpractice, workers comp', 'user', 42),
    ('Litigation Law Firm',                'Legal Services',           'Commercial litigation, class action, civil litigation', 'user', 43),
    ('Corporate & Business Law',           'Legal Services',           'M&A, corporate transactions, business formation, securities', 'user', 44),
    ('Real Estate Law',                    'Legal Services',           'RE transactions, title disputes, landlord-tenant', 'user', 45),
    ('Employment Law',                     'Legal Services',           'Labor, employment defense, HR legal, EEOC', 'user', 46),
    ('Estate & Trusts Law',                'Legal Services',           'Estate planning, trust admin, probate, elder law', 'user', 47),
    ('Specialty Law Firm',                 'Legal Services',           'IP, immigration, tax law, healthcare law, environmental law, telecom law', 'user', 48),
    ('CPA / Full-Service Accounting',      'Accounting & Tax',         'Full-service CPA firms: tax, audit, advisory, assurance', 'user', 49),
    ('Tax Advisory Firm',                  'Accounting & Tax',         'Tax planning, tax consulting, tax compliance specialists', 'user', 50),
    ('Outsourced CFO / Fractional Finance','Accounting & Tax',         'Fractional CFO, outsourced controller, embedded finance leadership', 'user', 51),
    ('Audit & Assurance',                  'Accounting & Tax',         'Independent audit, SOC reports, assurance services', 'user', 52),
    ('Bookkeeping Services',               'Accounting & Tax',         'Outsourced bookkeeping, accounts payable/receivable, basic accounting', 'user', 53),
    ('Independent Insurance Agency',       'Insurance',                'Independent agents selling personal and commercial lines', 'user', 54),
    ('Commercial Insurance Brokerage',     'Insurance',                'Brokers focused on commercial/business insurance lines', 'user', 55),
    ('Personal Lines Insurance',           'Insurance',                'Auto, home, life, health, Medicare insurance agents', 'user', 56),
    ('Specialty Insurance',                'Insurance',                'Cyber, professional liability, D&O, E&O, specialty lines', 'user', 57),
    ('Insurance MGA / Underwriting',       'Insurance',                'Managing general agents, program underwriters, risk carriers', 'user', 58),
    ('Management Consulting',              'Consulting & Advisory',    'Strategy, operations, organizational change management', 'user', 59),
    ('Business Advisory / Exit Planning',  'Consulting & Advisory',    'Business valuation, exit planning, business brokerage advisory', 'user', 60),
    ('HR & Organizational Consulting',     'Consulting & Advisory',    'HR strategy, fractional HR, organizational development, culture', 'user', 61),
    ('Operations Consulting',              'Consulting & Advisory',    'Process improvement, supply chain, operational efficiency', 'user', 62),
    ('Employee Benefits Consulting',       'Consulting & Advisory',    'Group benefits design, ERISA compliance, retirement plan consulting', 'user', 63),
    ('Digital Marketing Agency',           'Agency',                   'Full-service digital: SEO, paid, social, email, web design', 'user', 64),
    ('SEO / SEM Agency',                   'Agency',                   'Search engine optimization and paid search specialists', 'user', 65),
    ('Paid Media / Performance Agency',    'Agency',                   'Google Ads, Meta Ads, programmatic, performance marketing', 'user', 66),
    ('PR & Communications Agency',         'Agency',                   'Public relations, media relations, communications strategy', 'user', 67),
    ('Creative & Branding Agency',         'Agency',                   'Brand strategy, identity, design, creative production', 'user', 68),
    ('Full-Service Marketing Agency',      'Agency',                   'Integrated agency: brand + digital + PR + creative', 'user', 69),
    ('Specialty Marketing Agency',         'Agency',                   'Vertical-focused agency (healthcare, fintech, legal, RE marketing)', 'user', 70),
    ('Medical Practice / Clinic',          'Healthcare & Medical',     'Physician practices, dental, optometry, specialty clinics', 'user', 71),
    ('Medical Device Company',             'Healthcare & Medical',     'Medical device manufacturers, med-tech hardware/software', 'user', 72),
    ('Digital Health Platform',            'Healthcare & Medical',     'Healthcare SaaS, patient engagement, telehealth, digital health tools', 'user', 73),
    ('Healthcare Revenue Cycle',           'Healthcare & Medical',     'RCM services, medical billing, coding, claims management', 'user', 74),
    ('Home Health / Care Services',        'Healthcare & Medical',     'Home health agencies, assisted living, senior care, infusion services', 'user', 75),
    ('Mental Health Services',             'Healthcare & Medical',     'Behavioral health, counseling, psychology, addiction services', 'user', 76),
    ('Non-Profit Organization',            'Non-Profit & Associations','Mission-driven 501c3 orgs: human services, advocacy, community', 'user', 77),
    ('Trade Association',                  'Non-Profit & Associations','Industry trade groups, professional associations, chambers', 'user', 78),
    ('Foundation / Grantmaking',           'Non-Profit & Associations','Private foundations, community foundations, grantmaking orgs', 'user', 79),
    ('B2B Product Manufacturer',           'Manufacturing & Industrial','Manufactures products sold to other businesses', 'user', 80),
    ('Industrial Equipment / Machinery',   'Manufacturing & Industrial','Industrial machines, manufacturing equipment, tooling', 'user', 81),
    ('Consumer Product Manufacturer',      'Manufacturing & Industrial','Makes products sold to consumers (B2B2C)', 'user', 82),
    ('Contract Manufacturing',             'Manufacturing & Industrial','Contract manufacturers, OEM fabrication, toll processing', 'user', 83),
    ('Executive Search',                   'Staffing & Recruiting',    'C-suite and VP-level retained/contingency search', 'user', 84),
    ('Technology Staffing',                'Staffing & Recruiting',    'IT, engineering, software dev staffing', 'user', 85),
    ('Healthcare Staffing',                'Staffing & Recruiting',    'Nurses, physicians, allied health staffing', 'user', 86),
    ('General Staffing / RPO',             'Staffing & Recruiting',    'Multi-industry staffing, RPO, talent acquisition outsourcing', 'user', 87),
    ('General Contractor',                 'Construction & Engineering','Commercial and residential general contracting', 'user', 88),
    ('Civil / Infrastructure Engineering', 'Construction & Engineering','Roads, bridges, utilities, municipal infrastructure engineering', 'user', 89),
    ('Construction Management',            'Construction & Engineering','Owner''s rep, construction management, project management', 'user', 90),
    ('MEP / Specialty Engineering',        'Construction & Engineering','Mechanical, electrical, plumbing, structural engineering', 'user', 91),
    ('Federal / Defense Contractor',       'Government Contractor',    'DOD, IC, civilian federal agency contractor', 'user', 92),
    ('State & Local Gov Services',         'Government Contractor',    'Services sold to state, county, or municipal government', 'user', 93),
    ('Government IT Services (Gov)',       'Government Contractor',    'IT, cyber, data services specifically for government clients', 'user', 94),
    ('E-commerce / DTC Brand',             'Consumer & Retail',        'DQ — direct-to-consumer e-commerce, retail brands', 'user', 95),
    ('Retail Business',                    'Consumer & Retail',        'DQ — brick-and-mortar retail, consumer storefronts', 'user', 96),
    ('Wholesale Distributor',              'Consumer & Retail',        'DQ — wholesale, distribution to consumer channels', 'user', 97),
    ('Consumer Services',                  'Consumer & Retail',        'DQ — pet services, personal care, consumer lifestyle services', 'user', 98),
    ('Hospitality / Travel',               'Consumer & Retail',        'DQ — hotels, restaurants, travel, tourism, luxury experiences', 'user', 99)
ON CONFLICT (name) DO NOTHING;

-- Sectors
INSERT INTO taxonomy_sectors (name, synonyms, created_by, sort_order) VALUES
    ('Healthcare / Medical',         'medical, clinical, hospital, health system, health services, dental, pharma', 'user', 1),
    ('Financial Services & FinTech', 'finance, banking, fintech, financial institutions, capital markets, investment', 'user', 2),
    ('Real Estate & Property',       'real estate, property, housing, CRE, multifamily, mortgage, title', 'user', 3),
    ('Legal',                        'legal, law firms, attorneys, courts, legal services', 'user', 4),
    ('Technology & Software',        'tech, software, SaaS, IT, cloud, data, AI, digital, technology companies', 'user', 5),
    ('Manufacturing & Industrial',   'manufacturing, industrial, production, fabrication, factory, plant', 'user', 6),
    ('Government & Defense',         'government, public sector, federal, defense, military, DOD, municipal', 'user', 7),
    ('Education & Training',         'education, edtech, higher ed, K-12, schools, universities, workforce training', 'user', 8),
    ('Retail & E-commerce',          'retail, e-commerce, DTC, consumer brands, wholesale, distribution', 'user', 9),
    ('Construction & Infrastructure','construction, infrastructure, engineering, building, contractor, civil', 'user', 10),
    ('Energy & Utilities',           'energy, oil, gas, utilities, renewables, power, cleantech, solar, wind', 'user', 11),
    ('Life Sciences & Biotech',      'biotech, pharmaceutical, life sciences, clinical, drug, medical device, biopharma', 'user', 12),
    ('Media & Entertainment',        'media, entertainment, publishing, content, streaming, broadcast', 'user', 13),
    ('Transportation & Logistics',   'transportation, logistics, supply chain, freight, shipping, trucking, 3PL', 'user', 14),
    ('Hospitality & Travel',         'hospitality, hotel, travel, tourism, restaurant, food service', 'user', 15),
    ('Environmental & Climate',      'environmental, sustainability, climate, ESG, clean energy, remediation', 'user', 16),
    ('Non-Profit & Social Impact',   'nonprofit, NGO, charitable, social impact, advocacy, foundation', 'user', 17),
    ('Agriculture & Food',           'agriculture, farming, food, agtech, food processing, agribusiness', 'user', 18),
    ('Professional Services',        'professional services, consulting, advisory — use only when no more specific sector applies', 'user', 19)
ON CONFLICT (name) DO NOTHING;

-- =====================================================================
-- 3. Tagging columns on bucket_industry_map
-- =====================================================================

ALTER TABLE bucket_industry_map
    ADD COLUMN IF NOT EXISTS identity TEXT,
    ADD COLUMN IF NOT EXISTS characteristic TEXT,
    ADD COLUMN IF NOT EXISTS sector TEXT,
    ADD COLUMN IF NOT EXISTS is_new_identity BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_new_characteristic BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS is_new_sector BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS needs_qa BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS raw_industry TEXT,
    ADD COLUMN IF NOT EXISTS llm_reason TEXT;

-- Mirror new tagging columns into the legacy column names that the
-- existing volume rollup function reads from. This way we don't have to
-- rewrite Phase 1b SQL in the same migration. When primary_identity is
-- empty but identity is set, copy across.
CREATE OR REPLACE FUNCTION public.bucket_industry_map_mirror_tags()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.identity IS NOT NULL AND (NEW.primary_identity IS NULL OR NEW.primary_identity = '') THEN
        NEW.primary_identity := NEW.identity;
    END IF;
    IF NEW.characteristic IS NOT NULL AND (NEW.functional_specialization IS NULL OR NEW.functional_specialization = '') THEN
        NEW.functional_specialization := NEW.characteristic;
    END IF;
    IF NEW.sector IS NOT NULL AND (NEW.sector_focus IS NULL OR NEW.sector_focus = '') THEN
        NEW.sector_focus := NEW.sector;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS bucket_industry_map_mirror_tags ON bucket_industry_map;
CREATE TRIGGER bucket_industry_map_mirror_tags
BEFORE INSERT OR UPDATE ON bucket_industry_map
FOR EACH ROW EXECUTE FUNCTION public.bucket_industry_map_mirror_tags();

-- =====================================================================
-- 4. Run-level taxonomy version snapshot
-- =====================================================================

ALTER TABLE bucketing_runs
    ADD COLUMN IF NOT EXISTS taxonomy_version INTEGER,
    ADD COLUMN IF NOT EXISTS taxonomy_snapshot JSONB;

-- =====================================================================
-- 5. Vocabulary RPC sourced from enrichments.classification
-- =====================================================================

DROP FUNCTION IF EXISTS public.get_industry_vocabulary(TEXT[]);
DROP FUNCTION IF EXISTS public.get_industry_vocabulary(TEXT[], INTEGER);

CREATE FUNCTION public.get_industry_vocabulary(
    p_list_names TEXT[],
    p_limit INTEGER DEFAULT 10000
)
RETURNS TABLE (
    industry TEXT,
    n BIGINT,
    enrichment_status TEXT,
    avg_conf NUMERIC,
    sample_companies TEXT[],
    sample_reasoning TEXT[]
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT industry,
           n,
           enrichment_status,
           avg_conf,
           sample_companies,
           sample_reasoning
    FROM (
        SELECT industry,
               COUNT(*)::BIGINT AS n,
               enrichment_status,
               AVG(NULLIF(confidence, 0))::NUMERIC AS avg_conf,
               (ARRAY_AGG(company_name ORDER BY confidence DESC NULLS LAST) FILTER (WHERE company_name IS NOT NULL))[1:3] AS sample_companies,
               (ARRAY_AGG(reasoning ORDER BY confidence DESC NULLS LAST) FILTER (WHERE reasoning IS NOT NULL))[1:2] AS sample_reasoning
        FROM (
            SELECT
                COALESCE(NULLIF(TRIM(e.classification), ''), 'Scrape Error') AS industry,
                CASE
                    WHEN e.classification IS NULL OR TRIM(e.classification) = '' THEN 'unenriched'
                    WHEN e.classification ILIKE 'scrape error%' OR e.classification ILIKE 'site error%' THEN 'scrape_error'
                    WHEN e.status = 'failed' THEN 'failed'
                    WHEN e.status = 'completed' THEN 'completed'
                    ELSE 'pending'
                END AS enrichment_status,
                e.confidence,
                c.company_name,
                e.reasoning
            FROM contacts c
            LEFT JOIN enrichments e ON e.contact_id = c.contact_id
            WHERE c.lead_list_name = ANY(p_list_names)
        ) sub
        GROUP BY industry, enrichment_status
        ORDER BY n DESC
        LIMIT p_limit
    ) outer_q;
$$;

GRANT EXECUTE ON FUNCTION public.get_industry_vocabulary(TEXT[], INTEGER)
    TO anon, authenticated, service_role;

-- =====================================================================
-- 6. Backfill missing v3.1 RPCs (general breakdown + run diagnostics)
-- =====================================================================

CREATE OR REPLACE FUNCTION public.get_bucket_general_breakdown(p_run_id UUID)
RETURNS TABLE (
    general_reason TEXT,
    source TEXT,
    contact_count BIGINT
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $$
    SELECT COALESCE(general_reason, 'unspecified') AS general_reason,
           source,
           COUNT(*)::BIGINT AS contact_count
    FROM bucket_assignments
    WHERE bucketing_run_id = p_run_id
      AND bucket_name IN ('General', 'Disqualified')
    GROUP BY COALESCE(general_reason, 'unspecified'), source
    ORDER BY contact_count DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucket_general_breakdown(UUID)
    TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_bucketing_run_diagnostics(p_run_id UUID)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SET statement_timeout TO '60s'
AS $$
DECLARE
    v_list_names TEXT[];
    v_selected BIGINT;
    v_assigned BIGINT;
    v_contact_map BIGINT;
    v_unclassifiable BIGINT;
    v_usable BIGINT;
    v_general BIGINT;
    v_disqualified BIGINT;
    v_pre JSONB;
    v_post JSONB;
    v_breakdown JSONB;
    v_samples JSONB;
BEGIN
    SELECT list_names INTO v_list_names FROM bucketing_runs WHERE id = p_run_id;
    IF v_list_names IS NULL THEN
        RAISE EXCEPTION 'bucketing run % not found', p_run_id;
    END IF;

    SELECT COUNT(*)::BIGINT INTO v_selected
    FROM contacts WHERE lead_list_name = ANY(v_list_names);

    SELECT COUNT(*)::BIGINT INTO v_assigned
    FROM bucket_assignments WHERE bucketing_run_id = p_run_id;

    SELECT COUNT(*)::BIGINT INTO v_contact_map
    FROM bucket_contact_map WHERE bucketing_run_id = p_run_id;

    SELECT COUNT(*)::BIGINT INTO v_unclassifiable
    FROM bucket_contact_map
    WHERE bucketing_run_id = p_run_id
      AND (source = 'unclassifiable'
           OR general_reason IN ('failed_enrichment','missing_industry','scrape_site_unknown'));

    v_usable := GREATEST(v_selected - COALESCE(v_unclassifiable, 0), 0);

    SELECT COUNT(*)::BIGINT INTO v_general
    FROM bucket_assignments WHERE bucketing_run_id = p_run_id AND bucket_name = 'General';

    SELECT COUNT(*)::BIGINT INTO v_disqualified
    FROM bucket_assignments WHERE bucketing_run_id = p_run_id AND bucket_name = 'Disqualified';

    SELECT COALESCE(jsonb_object_agg(pre_rollup_bucket_name, n), '{}'::JSONB) INTO v_pre
    FROM (
        SELECT pre_rollup_bucket_name, COUNT(*)::BIGINT AS n
        FROM bucket_contact_map
        WHERE bucketing_run_id = p_run_id
        GROUP BY pre_rollup_bucket_name
        ORDER BY n DESC
    ) s;

    SELECT COALESCE(jsonb_object_agg(bucket_name, n), '{}'::JSONB) INTO v_post
    FROM (
        SELECT bucket_name, COUNT(*)::BIGINT AS n
        FROM bucket_assignments
        WHERE bucketing_run_id = p_run_id
        GROUP BY bucket_name
        ORDER BY n DESC
    ) s;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'general_reason', general_reason,
        'source', source,
        'contact_count', contact_count
    )), '[]'::JSONB) INTO v_breakdown
    FROM get_bucket_general_breakdown(p_run_id);

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'contact_id', contact_id,
        'industry_string', industry_string,
        'primary_identity', primary_identity,
        'functional_specialization', functional_specialization,
        'pre_rollup_bucket_name', pre_rollup_bucket_name,
        'source', source,
        'confidence', confidence,
        'general_reason', general_reason,
        'reasons', reasons
    )), '[]'::JSONB) INTO v_samples
    FROM (
        SELECT * FROM bucket_contact_map
        WHERE bucketing_run_id = p_run_id
          AND bucket_name IN ('General', 'Disqualified')
        ORDER BY assigned_at DESC
        LIMIT 30
    ) s;

    RETURN jsonb_build_object(
        'selected_contacts', v_selected,
        'assigned_contacts', v_assigned,
        'contact_map_rows', v_contact_map,
        'usable_contacts', v_usable,
        'unclassifiable_contacts', COALESCE(v_unclassifiable, 0),
        'general_contacts', v_general,
        'disqualified_contacts', v_disqualified,
        'general_pct', CASE WHEN v_assigned > 0 THEN ROUND((v_general::NUMERIC / v_assigned::NUMERIC) * 100, 2) ELSE 0 END,
        'pre_rollup_counts', v_pre,
        'post_rollup_counts', v_post,
        'general_breakdown', v_breakdown,
        'general_samples', v_samples
    );
END;
$$;

GRANT EXECUTE ON FUNCTION public.get_bucketing_run_diagnostics(UUID)
    TO anon, authenticated, service_role;

-- =====================================================================
-- 7. Reload PostgREST schema cache
-- =====================================================================

NOTIFY pgrst, 'reload schema';
