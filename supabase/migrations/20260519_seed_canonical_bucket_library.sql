-- v6.x: Seed bucket_library with ~40 canonical campaign buckets so every
-- run starts with stable outreach segments instead of an empty library.
-- Phase 1b's runBucketAssignment already lets the LLM PREFER library
-- entries or PROPOSE new ones when nothing fits — this migration just
-- gives it a baseline list to anchor on.
--
-- Mapping notes:
--   - primary_identity values MUST match taxonomy_identities.name in the
--     live DB (audited 2026-05-15 — 22 active identities).
--   - bucket_name is UNIQUE (DB constraint); ON CONFLICT DO NOTHING makes
--     the migration idempotent.
--   - is_canonical / created_by columns added here for traceability; user
--     edits and AI-proposed buckets continue to land with default values.
--   - The runBucketAssignment LLM still proposes new buckets (is_new=true)
--     whenever nothing in the library fits — canonical buckets do NOT
--     close off the proposal path.

-- 1. Schema extension — tag-style columns for provenance.
ALTER TABLE bucket_library
    ADD COLUMN IF NOT EXISTS is_canonical BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS created_by   TEXT    NOT NULL DEFAULT 'user'
        CHECK (created_by IN ('seed', 'user', 'ai'));

CREATE INDEX IF NOT EXISTS bucket_library_canonical_idx
    ON bucket_library (is_canonical) WHERE archived = false;

-- 2. Seed canonical buckets. Each row: (bucket_name, primary_identity,
--    description, include_terms, exclude_terms, is_canonical=true,
--    created_by='seed'). Listed by identity for readability.

INSERT INTO bucket_library
    (bucket_name, primary_identity, description, include_terms, exclude_terms, is_canonical, created_by)
VALUES
    -- Software & SaaS family
    ('FinTech / Financial Software', 'Software & SaaS',
     'B2B FinTech platforms — payments, lending tech, banking software, regtech, financial automation.',
     ARRAY['fintech', 'payments software', 'lending tech', 'banking software', 'regtech'],
     ARRAY['investor', 'pe firm', 'venture fund'],
     true, 'seed'),

    ('Marketing & Sales Software', 'Software & SaaS',
     'CRM, marketing automation, sales enablement, MarTech / RevOps SaaS.',
     ARRAY['crm', 'martech', 'marketing automation', 'sales enablement', 'revops'],
     ARRAY['marketing agency', 'marketing services'],
     true, 'seed'),

    ('HR & Workforce Software', 'Software & SaaS',
     'HRIS, payroll, benefits platforms, workforce management SaaS.',
     ARRAY['hris', 'payroll software', 'benefits platform', 'workforce management'],
     ARRAY['staffing', 'recruiting agency'],
     true, 'seed'),

    ('Cybersecurity Software & Platforms', 'Software & SaaS',
     'Security software products — threat detection, identity, GRC, compliance automation.',
     ARRAY['cybersecurity software', 'security platform', 'grc software', 'identity management'],
     ARRAY['cybersecurity services', 'mssp', 'pen testing'],
     true, 'seed'),

    ('Vertical & Industry SaaS', 'Software & SaaS',
     'Software built for a specific vertical (healthcare, legal, real estate, education, hospitality, etc.).',
     ARRAY['vertical saas', 'industry software', 'healthtech', 'edtech', 'proptech', 'legaltech'],
     ARRAY['horizontal'],
     true, 'seed'),

    ('Custom Software & Engineering', 'Software & SaaS',
     'Bespoke software development, contract engineering, custom application work.',
     ARRAY['custom software', 'bespoke development', 'software shop', 'application development'],
     ARRAY['own product', 'platform'],
     true, 'seed'),

    -- IT Services family
    ('IT Managed Services & MSPs', 'IT Services',
     'Managed IT, help desk, network management, MSP for SMB / enterprise.',
     ARRAY['msp', 'managed it', 'help desk', 'it support', 'network management'],
     ARRAY['software product', 'platform'],
     true, 'seed'),

    ('Cybersecurity Services & Consulting', 'IT Services',
     'MSSP, penetration testing, security consulting, incident response — service-delivered.',
     ARRAY['mssp', 'pen testing', 'security consulting', 'incident response'],
     ARRAY['security software', 'security platform'],
     true, 'seed'),

    ('Data & Migration Services', 'IT Services',
     'Data migration, ETL engineering, data engineering services.',
     ARRAY['data migration', 'etl', 'data engineering'],
     ARRAY['data platform', 'analytics product'],
     true, 'seed'),

    -- Financial Services family
    ('Private Equity & Buyout Firms', 'Financial Services',
     'PE buyout, growth equity, lower / middle market private equity.',
     ARRAY['private equity', 'pe firm', 'buyout', 'growth equity'],
     ARRAY['venture', 'portfolio company'],
     true, 'seed'),

    ('Venture Capital', 'Financial Services',
     'Early / growth stage venture investing.',
     ARRAY['venture capital', 'vc fund', 'seed fund', 'series a'],
     ARRAY['pe firm', 'portfolio company', 'venture-backed startup'],
     true, 'seed'),

    ('Investment Banking & M&A Advisory', 'Financial Services',
     'M&A advisory, sell-side / buy-side, capital raising, ESOP advisory.',
     ARRAY['investment bank', 'm&a advisory', 'sell-side', 'buy-side'],
     ARRAY['retail bank', 'commercial bank'],
     true, 'seed'),

    ('Wealth Management & Financial Advisory', 'Financial Services',
     'RIA, financial planning, retirement advisory for HNW individuals and families.',
     ARRAY['wealth management', 'ria', 'financial advisor', 'family office'],
     ARRAY['retail bank'],
     true, 'seed'),

    ('Asset Management & Hedge Funds', 'Financial Services',
     'Long / short equity, alt strategies, institutional asset management.',
     ARRAY['asset management', 'hedge fund', 'institutional investor', 'alt strategy'],
     ARRAY['wealth advisor', 'retail'],
     true, 'seed'),

    ('Banking & Lending', 'Financial Services',
     'Banks, credit unions, mortgage / specialty lenders, commercial banking.',
     ARRAY['bank', 'credit union', 'mortgage lender', 'lending', 'commercial bank'],
     ARRAY['fintech', 'banking software'],
     true, 'seed'),

    -- Insurance
    ('Insurance Brokerage & Agencies', 'Insurance Services',
     'Independent agencies, commercial brokerages, MGAs, underwriters.',
     ARRAY['insurance broker', 'insurance agency', 'mga', 'underwriter'],
     ARRAY['insurance software'],
     true, 'seed'),

    -- Real Estate
    ('Commercial Real Estate Services', 'Real Estate',
     'CRE brokerage, advisory, commercial leasing, office / retail / industrial real estate.',
     ARRAY['cre brokerage', 'commercial real estate', 'office leasing', 'industrial real estate'],
     ARRAY['residential', 'real estate software'],
     true, 'seed'),

    ('Real Estate Investment & Development', 'Real Estate',
     'Ground-up development, multifamily, mixed-use, REIT, real estate investment firms.',
     ARRAY['real estate development', 'reit', 'multifamily', 'real estate investment'],
     ARRAY['brokerage'],
     true, 'seed'),

    ('Property Management Services', 'Real Estate',
     'Third-party property management — residential, commercial, multifamily.',
     ARRAY['property management', 'property manager'],
     ARRAY['development', 'investment'],
     true, 'seed'),

    -- Legal
    ('Law Firms (All Specialties)', 'Legal Services',
     'Law firms across all specialties: corporate, litigation, IP, employment, real estate, family, immigration, tax law.',
     ARRAY['law firm', 'attorney', 'litigation', 'corporate law', 'ip law'],
     ARRAY['legal software', 'legaltech'],
     true, 'seed'),

    -- Accounting
    ('CPA & Accounting Firms', 'Accounting & Tax',
     'Full-service CPA firms, tax advisory, audit, bookkeeping, outsourced finance.',
     ARRAY['cpa', 'accounting firm', 'tax advisor', 'audit', 'bookkeeping'],
     ARRAY['accounting software', 'tax software'],
     true, 'seed'),

    -- Consulting & Advisory
    ('Management & Strategy Consulting', 'Consulting & Advisory',
     'Strategy consulting, management consulting, business / operational consulting.',
     ARRAY['strategy consulting', 'management consulting', 'business consulting'],
     ARRAY['software', 'agency'],
     true, 'seed'),

    ('IT & Digital Transformation Consulting', 'Consulting & Advisory',
     'IT consulting, digital transformation, ERP implementation, systems integration advisory.',
     ARRAY['it consulting', 'digital transformation', 'erp implementation', 'systems integration'],
     ARRAY['msp', 'managed services'],
     true, 'seed'),

    ('Specialty Advisory & Consulting', 'Consulting & Advisory',
     'Specialty / vertical advisory — HR, environmental, engineering, educational, risk management consulting.',
     ARRAY['hr consulting', 'environmental consulting', 'engineering consulting', 'risk management'],
     ARRAY['software', 'agency'],
     true, 'seed'),

    -- Agency
    ('Digital Marketing & Performance Agencies', 'Agency',
     'Digital, performance, SEO/SEM, paid media — measurable acquisition agencies.',
     ARRAY['digital agency', 'performance marketing', 'seo agency', 'paid media', 'sem agency'],
     ARRAY['software', 'martech platform'],
     true, 'seed'),

    ('Creative & Branding Agencies', 'Agency',
     'Brand strategy, identity, design, creative production agencies.',
     ARRAY['branding agency', 'creative agency', 'design studio'],
     ARRAY['media buying', 'performance marketing'],
     true, 'seed'),

    ('PR & Communications Agencies', 'Agency',
     'Public relations, media relations, communications strategy agencies.',
     ARRAY['pr agency', 'public relations', 'communications agency'],
     ARRAY['digital advertising'],
     true, 'seed'),

    ('Full-Service Marketing Agencies', 'Agency',
     'Integrated agencies covering brand + digital + PR + creative under one roof.',
     ARRAY['full service agency', 'integrated agency', 'marketing agency'],
     ARRAY['single-channel'],
     true, 'seed'),

    ('Talent & Event Agencies', 'Agency',
     'Talent management, artist representation, sports management, event management agencies.',
     ARRAY['talent management', 'artist management', 'event management', 'sports management'],
     ARRAY['recruiting', 'staffing'],
     true, 'seed'),

    -- Healthcare Operator
    ('Healthcare Providers & Services', 'Healthcare Operator',
     'Clinics, hospitals, medical practices, healthcare support and care services.',
     ARRAY['clinic', 'hospital', 'medical practice', 'physician group', 'care services'],
     ARRAY['healthtech', 'medical device software'],
     true, 'seed'),

    ('Biotech, Pharma & Medical Devices', 'Healthcare Operator',
     'Biotech, pharmaceuticals, medical device manufacturers, life sciences operators.',
     ARRAY['biotech', 'pharmaceutical', 'medical device', 'life sciences'],
     ARRAY['health software', 'digital health platform'],
     true, 'seed'),

    -- Manufacturing & Industrial
    ('B2B Manufacturing & Industrial', 'Manufacturing & Industrial',
     'B2B product manufacturers, industrial equipment, custom / contract manufacturing.',
     ARRAY['manufacturer', 'industrial equipment', 'fabrication', 'contract manufacturing'],
     ARRAY['equipment rental', 'maintenance services'],
     true, 'seed'),

    -- Staffing & Recruiting
    ('Executive Search & Staffing', 'Staffing & Recruiting',
     'Executive search, technical staffing, healthcare staffing, professional staffing / RPO.',
     ARRAY['executive search', 'staffing agency', 'recruiting', 'rpo', 'placement'],
     ARRAY['hr software', 'hr consulting'],
     true, 'seed'),

    -- Construction & Engineering
    ('Construction & General Contracting', 'Construction & Engineering',
     'General contractors, civil engineering, MEP, custom home builders, construction management.',
     ARRAY['general contractor', 'civil engineering', 'home builder', 'construction management', 'mep'],
     ARRAY['construction software'],
     true, 'seed'),

    -- Government Contractor
    ('Government Contractors & Federal Services', 'Government Contractor',
     'Government / defense / federal contractors, systems integrators, public defense services.',
     ARRAY['government contractor', 'federal contractor', 'defense contractor', 'systems integrator'],
     ARRAY['government agency', 'public sector employee'],
     true, 'seed'),

    -- Logistics & Transportation
    ('Logistics & 3PL Services', 'Logistics & Transportation',
     '3PL providers, freight, trucking, transportation, fleet management.',
     ARRAY['3pl', 'logistics', 'freight', 'trucking', 'fleet management'],
     ARRAY['logistics software', 'supply chain software'],
     true, 'seed'),

    -- Field Services & Maintenance
    ('Field Services & Equipment', 'Field Services & Maintenance',
     'Field service operators, equipment rental / leasing, electrical contracting, IT asset disposition.',
     ARRAY['field services', 'equipment rental', 'electrical contracting', 'it asset disposition'],
     ARRAY['field service software', 'maintenance software'],
     true, 'seed'),

    -- Energy & Utilities
    ('Energy & Utilities', 'Energy & Utilities',
     'Energy producers, utilities, renewable energy, oil & gas, energy project developers.',
     ARRAY['energy', 'utility', 'oil and gas', 'renewable energy', 'solar', 'wind'],
     ARRAY['energy software', 'energy tech'],
     true, 'seed'),

    -- Hospitality & Travel
    ('Hospitality & Travel Operators', 'Hospitality & Travel',
     'Hotels, restaurants, tour operators, hospitality and travel operators.',
     ARRAY['hotel', 'restaurant', 'tour operator', 'hospitality operator'],
     ARRAY['hospitality software', 'restaurant software'],
     true, 'seed'),

    -- Education Operator
    ('Education & Training Providers', 'Education Operator',
     'Schools, universities, training providers, vocational training operators.',
     ARRAY['school', 'university', 'training provider', 'vocational training', 'edu operator'],
     ARRAY['edtech', 'education software'],
     true, 'seed'),

    -- Non-Profit & Association
    ('Nonprofits & Associations', 'Non-Profit & Association',
     'Non-profits, foundations, trade associations, advocacy / religious / professional associations.',
     ARRAY['non-profit', 'nonprofit', '501c3', 'foundation', 'trade association', 'chamber'],
     ARRAY['nonprofit software'],
     true, 'seed'),

    -- Media & Entertainment
    ('Media & Publishing', 'Media & Entertainment',
     'Trade publications, production companies, media operators.',
     ARRAY['trade publication', 'production company', 'media operator', 'broadcaster'],
     ARRAY['adtech', 'media software'],
     true, 'seed'),

    -- Retail
    ('Retail & Distribution', 'Retail',
     'Retail businesses, distributors, wholesale, auto dealerships, manufacturers'' representatives.',
     ARRAY['retail', 'distribution', 'wholesale', 'auto dealership'],
     ARRAY['retail software', 'ecommerce platform'],
     true, 'seed')
ON CONFLICT (bucket_name) DO NOTHING;

-- 3. Reload PostgREST schema cache so the new columns are visible to the API.
NOTIFY pgrst, 'reload schema';
