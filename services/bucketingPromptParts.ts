// Shared building blocks for the Phase 1a (tagIndustries) and Phase 1b
// (classifyContactBatch) system prompts. Single source of truth for the
// load-bearing classification rules — keeps both phases in lockstep so
// per-contact routing doesn't drift from per-industry tagging.
//
// Identity / sub-identity / sector names referenced below MUST exist in
// the live DB taxonomy. If you rename or remove a taxonomy entry, audit
// these constants too.

export const HARD_KEYWORD_ROUTING = `═══════════════════════════════════════════════════════════════════════════
HARD KEYWORD ROUTING (apply BEFORE anything else — fixes the identity)
═══════════════════════════════════════════════════════════════════════════

If the input matches a rule below, the identity is FIXED to the stated value.
The vertical / served market belongs in SECTOR, never the identity.

 1. SaaS / software platform / software for X / B2B software / app / AI platform
    / regtech / compliance software / collaboration software
       → identity = Software & SaaS  (NEVER Agency, NEVER Consulting & Advisory)

 2. Staffing / recruiting / recruitment / talent acquisition / executive search /
    placement agency / RPO
       → identity = Staffing & Recruiting

 3. Law firm / attorney / litigation / legal services / paralegal /
    court reporting / legal videography
       → identity = Legal Services

 4. Private equity / PE firm / venture capital / VC fund / hedge fund /
    asset management / wealth management / family office / fund administration /
    investment bank / M&A advisory / bank / credit union / mortgage lender
       → identity = Financial Services  (even if focused on a vertical;
          "PE-BACKED" or "venture-backed" describing a startup is NOT this rule —
          that's a financing status, not the company's identity)

 5. Manufacturer / maker of / manufactures X / produces X / fabrication / OEM
       → identity = Manufacturing & Industrial
          EXCEPTION: medical device manufacturer → Healthcare Operator

 6. Marketing / advertising / creative / branding / PR / media / digital /
    performance / SEO / SEM agency / talent management / artist management /
    sports management
       → identity = Agency  (NEVER Consulting & Advisory)

 7. Non-profit / 501(c) / foundation / charity / land trust / trade association /
    chamber of commerce / advocacy / professional association
       → identity = Non-Profit & Association

 8. Architecture / architectural design / interior design / engineering firm /
    general contractor / home builder / civil engineering / construction management /
    MEP engineering
       → identity = Construction & Engineering  (NEVER Manufacturing & Industrial)

 9. Real estate brokerage / real estate developer / property management /
    real estate investment / title & escrow
       → identity = Real Estate

10. Insurance agency / insurance brokerage / insurance broker / MGA / underwriter
       → identity = Insurance Services  (NEVER Financial Services)

11. CPA / accounting firm / tax advisor / outsourced CFO / bookkeeping / audit
       → identity = Accounting & Tax

12. Managed IT / MSP / cybersecurity services / cloud services / network services /
    custom software development services / data migration services /
    IT asset disposition
       → identity = IT Services  (service-delivered, not a product)

13. Government contractor / federal contractor / defense contractor /
    public sector contractor / systems integrator for government /
    facilities management for government
       → identity = Government Contractor

14. Hospital / clinic / dental practice / medical practice / physician group /
    pharmacy / pharmaceutical / biotech / medical device / care services
       → identity = Healthcare Operator

15. School / university / college / training provider / vocational training /
    learning company
       → identity = Education Operator

16. Hotel / restaurant / tour operator / hospitality operator
       → identity = Hospitality & Travel  (often DQ if family-owned / local /
          single-location consumer-only)

17. 3PL / freight / trucking / logistics provider / fleet management /
    transportation services / business aviation
       → identity = Logistics & Transportation

18. Energy producer / utility / oil & gas / solar / wind / renewable energy /
    energy project developer
       → identity = Energy & Utilities

19. Retail business / e-commerce / DTC brand / wholesale distributor /
    auto dealership / manufacturers' representative
       → identity = Retail  (DQ unless they have a clear B2B / wholesale / SaaS arm)

20. Field service / equipment rental / equipment leasing / electrical contracting /
    HVAC services / maintenance services
       → identity = Field Services & Maintenance

21. Publication / production company / trade publication / broadcaster /
    streaming / entertainment
       → identity = Media & Entertainment

If two patterns conflict, the one appearing earlier in the input wins.`;

export const CORE_PRINCIPLES = `═══════════════════════════════════════════════════════════════════════════
CORE PRINCIPLES
═══════════════════════════════════════════════════════════════════════════

• "X for Y" means identity=X, sector=Y. NEVER let the served vertical become
  the identity.
    ✓ "SEO agency for healthcare clinics" → identity=Agency, sector=Healthcare
    ✓ "Software platform for K-12 schools" → identity=Software & SaaS, sector=Education
    ✓ "Healthcare PE firm" → identity=Financial Services, sector=Healthcare
    ✗ "Healthcare PE firm" → identity=Healthcare Operator   ← WRONG

• Operator vs Enabler. An operator IN a vertical (hospital, restaurant, school,
  bank) belongs to that vertical's identity. A service provider TO that vertical
  keeps its own service identity.

• Services vs Products. Companies that rent / maintain / repair / operate-as-
  a-service must NEVER be tagged as a manufacturer sub-identity. Use the
  relevant services sub-identity, or leave sub-identity null.

• Sector defaults to NULL. Only set sector when the input EXPLICITLY states the
  vertical the company serves. Do not infer.
    ✓ "Digital marketing agency" → sector=null
    ✓ "Digital marketing agency for restaurants" → sector=Hospitality & Travel
  Do NOT use these as sectors (they are identities or non-verticals):
    Marketing · Advertising · IT Services · Professional Services · Consulting ·
    Corporate · B2B · Small Business · Subscription · Holding Company

• When uncertain, prefer NULL over a wrong guess. The pipeline gates on
  per-tag confidence; a wrong tag with high confidence is the worst outcome.
  Generic inputs like "Professional services firm" or "Technology company"
  should return identity=null (or low-confidence identity) and sub_identity=null.`;

export const DISQUALIFICATION_RULES = `═══════════════════════════════════════════════════════════════════════════
DISQUALIFICATION (be conservative — false negatives are worse than false positives)
═══════════════════════════════════════════════════════════════════════════

Set is_disqualified=true ONLY when the text gives clear evidence the company is
a pure-consumer / hyper-local / low-ticket business with no plausible B2B angle.

  DQ:                                      KEEP INVITEABLE:
  Family-owned restaurant in Austin        Hospitality SaaS for boutique hotels
  Independent dog grooming salon           Wholesale distribution platform for restaurants
  Local plumbing, residential only         Multi-location dental group
  DTC candle brand (no B2B mention)        DTC brand with B2B wholesale channel
  Solo influencer / lifestyle creator      Game / software studio (even consumer-sold)

When uncertain, set is_disqualified=false. Don't auto-DQ just because the input
mentions retail / hospitality / consumer terms. Identities marked [DQ] in the
library are a strong hint, not a hard rule.`;

// Build the dynamic LIBRARIES section from the live taxonomy snapshot. Used by
// both Phase 1a (full library) and Phase 1b (same library). The "snapshot"
// shape matches TaxonomySnapshot in bucketingService.ts.
export interface TaxonomySnapshotLike {
    identities: Array<{ name: string; description?: string | null; is_disqualified?: boolean }>;
    sub_identities: Array<{ name: string; parent_identity?: string; description?: string | null }>;
    sectors: Array<{ name: string; description?: string | null; synonyms?: string | null }>;
}

export function renderLibraryMenu(s: TaxonomySnapshotLike): string {
    const identityNames = JSON.stringify(s.identities.map(i => i.name));
    const sectorNames = JSON.stringify(s.sectors.map(sec => sec.name));
    const subByParent: Record<string, string[]> = {};
    for (const c of s.sub_identities) {
        const parent = c.parent_identity || '(unknown)';
        if (!subByParent[parent]) subByParent[parent] = [];
        subByParent[parent].push(c.name);
    }
    const subMenu = Object.entries(subByParent)
        .map(([parent, names]) => `  ${JSON.stringify(parent)}: ${JSON.stringify(names)}`)
        .join(',\n');
    return `VALID_IDENTITIES = ${identityNames}

VALID_SECTORS = ${sectorNames}

VALID_SUB_IDENTITIES (grouped by parent identity — the sub_identity's parent
MUST match the identity you chose):
{
${subMenu}
}`;
}

export function renderLibraryReference(s: TaxonomySnapshotLike): string {
    const idLines = s.identities.map(i =>
        `  - ${i.name}${i.is_disqualified ? ' [DQ]' : ''}: ${i.description || ''}`
    ).join('\n');
    const chLines = s.sub_identities.map(c =>
        `  - ${c.name} (under ${c.parent_identity}): ${c.description || ''}`
    ).join('\n');
    const secLines = s.sectors.map(sec =>
        `  - ${sec.name}: ${sec.synonyms || sec.description || ''}`
    ).join('\n');
    return `== IDENTITY LIBRARY ==
${idLines}

== SUB-IDENTITY LIBRARY (parent_identity must match the identity you chose) ==
${chLines}

== SECTOR LIBRARY ==
${secLines}`;
}

export const EXACT_SPELLING_RULE = `═══════════════════════════════════════════════════════════════════════════
ALLOWED VALUES — these are the ONLY valid strings. Copy them VERBATIM.
═══════════════════════════════════════════════════════════════════════════

The identity, sub_identity, and sector fields in your output MUST be one of
the strings listed under VALID_* below, copied character-for-character (or
null). Do not paraphrase, shorten, pluralize, or substitute. Common mistakes
to avoid:

  - Don't shorten ("Private Equity Firm" → "Private Equity") — keep the exact
    library spelling, suffixes and all.
  - Don't substitute synonyms ("Cybersecurity Software" instead of the actual
    library entry) — read the library and pick the closest existing string.
  - Don't add a noun like "Services" to an identity that doesn't include it
    in the library.

If no listed value is even loosely applicable, set is_new_*=true with your
proposed name.`;
