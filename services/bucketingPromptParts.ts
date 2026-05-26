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
       → identity = Software & SaaS  (NEVER Agency, NEVER Consulting & Advisory,
          NEVER IT Services — products live here, services live in IT Services)

 1b. "X-tech" terms describing a STARTUP / COMPANY / PRODUCT / PLATFORM are
     software, not the served vertical. Specifically:
       fintech · regtech · insurtech · edtech · proptech · healthtech /
       medtech · legaltech · martech · adtech · agritech · climatetech
       → identity = Software & SaaS  (NEVER Financial Services / Insurance Services
         / Healthcare Provider / Life Sciences / Education Operator / etc.)
     Trigger phrases: "fintech startup", "regtech platform", "insurtech app",
     "edtech company", "healthtech product".
     EXCEPTION — when the X-tech term modifies an INVESTOR or OPERATOR noun, the
     investor/operator identity wins:
       "fintech investor" / "fintech-focused PE firm" → Financial Services
       "healthtech VC fund" → Financial Services
       "edtech operator running 12 schools" → Education Operator
     In ambiguous cases (no investor/operator noun), DEFAULT TO Software & SaaS.

 2. Staffing / recruiting / recruitment / talent acquisition / executive search /
    placement agency / RPO
       → identity = Staffing & Recruiting

 3. Law firm / attorney / litigation / legal services / paralegal /
    court reporting / legal videography
       → identity = Legal Services
       NOTE: Legal Services has NO sub-identities — always set sub_identity = null.

 4. Private equity / PE firm / venture capital / VC fund / hedge fund /
    asset management / wealth management / family office / fund administration /
    investment bank / M&A advisory / bank / credit union / mortgage lender /
    consumer lender / commercial lender
       → identity = Financial Services  (even if focused on a vertical;
          "PE-BACKED" or "venture-backed" describing a startup is NOT this rule —
          that's a financing status, not the company's identity)
       Sub-identity hint: banks / credit unions / lenders / mortgage brokers /
       consumer & commercial credit → Banking & Lending (single combined sub).

 4b. ANTI-MISTAKE for the word "advisory". When "advisory" is attached to an
     investment-finance verb, the identity is Financial Services, NOT
     Consulting & Advisory. Trigger phrases:
       "VC advisory", "PE advisory", "M&A advisory", "investment banking advisory",
       "capital raising advisory", "secondary market advisory",
       "pre-IPO advisory / financing advisory", "fund formation advisory",
       "transaction advisory"
       → identity = Financial Services
     The word "advisory" alone (e.g. "strategy advisory", "business advisory")
     stays with Consulting & Advisory under rule 11b.

 5. Manufacturer / maker of / manufactures X / produces X / fabrication / OEM
       → identity = Manufacturing & Industrial
          EXCEPTIONS:
            • medical device / pharmaceutical / biotech / diagnostics /
              contract manufacturing & sterilization for medical → Life Sciences
            • agricultural manufacturing → still Manufacturing & Industrial

 6. Marketing / advertising / creative / branding / PR / media / digital /
    performance / SEO / SEM agency / talent management / artist management /
    sports management
       → identity = Agency  (NEVER Consulting & Advisory)

 7. Non-profit / 501(c) / foundation / charity / land trust / trade association /
    chamber of commerce / advocacy / professional association /
    religious organization
       → identity = Non-Profit & Association
       EXCEPTIONS (mission-driven but the WORK is in another vertical):
         • affordable / supportive housing operator → Real Estate (Housing Operator)
         • workforce / job training programs → Education Operator (Workforce
           Development Services)

 8. Architecture / architectural design / interior design / engineering firm /
    general contractor / home builder / civil engineering / construction management /
    MEP engineering / infrastructure development (rail, ports, roads, bridges)
       → identity = Construction & Engineering  (NEVER Manufacturing & Industrial,
          NEVER Logistics & Transportation)

 9. Real estate brokerage / real estate developer / property management /
    real estate investment / title & escrow / housing operator
       → identity = Real Estate

10. Insurance agency / insurance brokerage / insurance broker / MGA / underwriter /
    insurance carrier / reinsurance
       → identity = Insurance Services  (NEVER Financial Services)

11. CPA / accounting firm / tax advisor / outsourced CFO / bookkeeping / audit firm
       → identity = Accounting & Tax  (NOT Consulting & Advisory)

11b. Management consulting / strategy consulting / business advisory /
     generalist consulting / specialty consulting / engineering consulting /
     environmental consulting / HR consulting / IT consulting / educational
     consulting / risk management & insurance advisory
       → identity = Consulting & Advisory  (NOT Accounting & Tax, NOT Agency)
     Sub-identity hint: generic management/strategy/business/specialty firms
     all collapse into "Strategy & Management Consulting" — only set one of the
     specialist subs (Engineering / Environmental / HR / IT / Educational
     Consulting / Risk Management and Insurance Advisory Services) when the
     specialty is explicit.

12. Managed IT / MSP / cybersecurity services / cloud services / network services /
    custom software development services / data migration services /
    IT asset disposition / ITAD
       → identity = IT Services  (service-delivered, not a product)
     Anti-mistake: "custom software development" is IT Services, NOT Software & SaaS.
     "IT asset disposition / ITAD" is IT Services, NOT Field Services & Maintenance.

13. Government contractor / federal contractor / defense contractor /
    public sector contractor / systems integrator for government /
    facilities management for government
       → identity = Government Contractor

14. Hospital / clinic / dental practice / medical practice / physician group /
    pharmacy (retail/operator) / urgent care / ambulatory surgical center /
    home health / hospice / managed care / health coaching /
    healthcare support services
       → identity = Healthcare Provider  (companies that TREAT PATIENTS or provide
          care services). NOT drug/device makers (those are Life Sciences). NOT
          health software (Software & SaaS).

14b. Pharmaceutical company / pharma / biotech / biotechnology / drug developer /
     medical device manufacturer / diagnostics maker / therapeutics company /
     contract manufacturing & sterilization (medical) / CDMO / CRO
       → identity = Life Sciences  (DEVELOPS or PRODUCES drugs, devices,
          diagnostics, or biologic therapies). NOT a care provider, NOT software.

15. School / university / college / training provider / vocational training /
    learning company / workforce development
       → identity = Education Operator

16. Hotel / restaurant / hotel management / hospitality operator / tour operator /
    business aviation / private aviation / charter flight operator
       → identity = Hospitality & Travel  (often DQ if family-owned / local /
          single-location consumer-only)
       Note: business aviation OPERATORS (own/fly the jets) live here, NOT in
       Logistics & Transportation.

17. 3PL / freight / trucking / logistics provider / fleet leasing & management /
    transportation services
       → identity = Logistics & Transportation
       Note: infrastructure construction (rail, ports, roads) is Construction &
       Engineering (rule 8), NOT here.

18. Energy producer / utility / oil & gas / solar / wind / renewable energy /
    energy project developer / utility services
       → identity = Energy & Utilities

19. Distributor / wholesaler / wholesale distribution / master distributor /
    value-added reseller / jobber / manufacturers' representative
       → identity = Distribution & Wholesale  (B2B middleman — NOT Retail,
          NOT Logistics & Transportation, NOT Manufacturing & Industrial)

20. Consumer-facing retail business / e-commerce / DTC brand / auto dealership
       → identity = Retail  (DQ unless they have a clear B2B / wholesale / SaaS arm)
       Anti-mistake: B2B distributors/wholesalers go to Distribution & Wholesale
       (rule 19), NOT Retail.

21. Field service / equipment rental & leasing / electrical contracting /
    HVAC services / painting contractor / maintenance services
       → identity = Field Services & Maintenance

22. Publication / production company / film studio / video production /
    post-production / trade publication / broadcaster / streaming /
    entertainment / record label
       → identity = Media & Entertainment  (NEVER Agency — a production company
          that makes film/video/audio content is Media, not a marketing agency.
          Only call it Agency when the input explicitly says "advertising
          agency", "creative agency", "branding agency", "PR agency",
          "marketing agency", or similar.)

23. Telecom carrier / telecommunications / telco / wireless carrier / ISP
       → identity = Telecommunications

24. Farm operator / agricultural producer / grower / ranch / commodity producer
       → identity = Agriculture  (NOT Manufacturing & Industrial; agricultural
          MANUFACTURING — e.g. fertilizer plant, ag equipment — is Manufacturing
          & Industrial)

If two patterns conflict, the EARLIER-numbered rule wins (more specific identity
wins over the generic "manufacturer / consulting / agency" catch-all).`;

export const CORE_PRINCIPLES = `═══════════════════════════════════════════════════════════════════════════
CORE PRINCIPLES
═══════════════════════════════════════════════════════════════════════════

• DECISION ORDER — always identity FIRST, then sub from that identity's
  children only. Never pick a sub_identity before you've committed to an
  identity. If you start by spotting "Healthcare Support Services" in the
  text, that does NOT mean you can write it as sub — first decide whether
  the company's identity is Healthcare Provider or something else. If the
  identity ends up being anything OTHER than the sub's parent, you must drop
  the sub.

• PAIRED-OR-EMPTY for identity + sub_identity. These two fields move together:
    - If identity is null, sub_identity MUST be null.
    - If sub_identity is set, identity MUST be set to that sub's parent in
      VALID_SUB_IDENTITIES (verify before returning).
    - If you can't pick an identity confidently, set BOTH to null and leave
      identity_confidence ≤ 3. Don't ship a sub without an identity — that
      row goes to "General" downstream.

• "X for Y" means identity=X, sector=Y. NEVER let the served vertical become
  the identity, and NEVER let it become the sub_identity.
    ✓ "SEO agency for healthcare clinics" → identity=Agency, sector=Healthcare
    ✓ "Software platform for K-12 schools" → identity=Software & SaaS, sector=Education
    ✓ "Healthcare PE firm" → identity=Financial Services, sector=Healthcare
    ✗ "Healthcare PE firm" → identity=Healthcare Provider   ← WRONG
    ✗ "Digital consulting for non-profits" → sub_identity=Non-Profit Organization ← WRONG
       (Non-Profit Organization's parent is Non-Profit & Association, not
        Consulting & Advisory. Set sub_identity=null, sector=Non-Profit.)

• sub_identity ≠ sector, EVER. They are independent fields. If the only
  meaningful descriptor you can put in sub_identity is a vertical name,
  the vertical belongs in sector and sub_identity stays null.
    ✗ sub_identity="Healthcare", sector="Healthcare"  ← never duplicate
    ✗ sub_identity="Non-Profit & Social Impact", sector="Non-Profit & Social Impact"  ← never duplicate

• Operator vs Enabler. An operator IN a vertical (hospital, restaurant, school,
  bank) belongs to that vertical's identity. A service provider TO that vertical
  keeps its own service identity.

• Healthcare Provider vs Life Sciences vs Health-software. Three different
  identities, decided by what the company actually DOES:
    - Treats patients / delivers care → Healthcare Provider
    - Develops or makes drugs / devices / diagnostics → Life Sciences
    - Sells software to either of the above → Software & SaaS (sector=Healthcare)
  When the input is vague ("healthcare company"), prefer NULL identity over a guess.

• Distribution & Wholesale vs Retail. A B2B distributor / wholesaler / jobber /
  master distributor / VAR / manufacturers' rep belongs to Distribution &
  Wholesale. A consumer-facing retailer or DTC brand belongs to Retail (and is
  usually DQ). Do NOT lump them together.

• Services vs Products. Companies that rent / maintain / repair / operate-as-
  a-service must NEVER be tagged as a manufacturer sub-identity. Use the
  relevant services sub-identity, or leave sub-identity null. Custom software
  development DELIVERED AS A SERVICE → IT Services, not Software & SaaS.

• Parent/sub consistency check (do this BEFORE returning a row).
  After you pick identity X and sub_identity Y, verify that Y's parent in the
  VALID_SUB_IDENTITIES menu is exactly X. If not, you've made an error — either
  re-pick the sub_identity from X's children, or set sub_identity = null.
  Common past-mistake patterns to avoid:
    - sub_identity "Private Equity" paired with identity ≠ Financial Services
    - sub_identity "Custom Software Development" paired with identity = Software & SaaS  ← WRONG, parent is IT Services
    - sub_identity "Management Consulting" paired with identity = Accounting & Tax  ← WRONG, parent is Consulting & Advisory
    - sub_identity "IT Asset Disposition" paired with identity = Field Services & Maintenance  ← WRONG, parent is IT Services
    - any sub_identity paired with identity = Legal Services  ← WRONG, Legal Services has no subs

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
  Single-location auto repair              Auto dealership group (multi-location)

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
  - Don't pair a sub_identity with the wrong parent identity. Look up the
    sub's parent in VALID_SUB_IDENTITIES before returning it. If the parent
    doesn't match the identity you picked, set sub_identity = null.
  - Don't invent an identity that already exists ("Healthcare Operator" no
    longer exists — use "Healthcare Provider" or "Life Sciences"). Check the
    current VALID_IDENTITIES list before proposing.

REMOVED LIBRARY VALUES — do NOT use these strings; they are not in VALID_*
and proposing them as new is forbidden. Use the canonical replacement.

  Removed                          → Use instead
  ────────────────────────────────────────────────────────────────────────
  Healthcare Operator              → Healthcare Provider OR Life Sciences
  Specialty Consulting             → Strategy & Management Consulting
  Business Consulting              → Strategy & Management Consulting
  Strategy Consulting              → Strategy & Management Consulting
  Management Consulting            → Strategy & Management Consulting
  Hospitality Operator             → Hotel & Hospitality Operator
  Hotel Management                 → Hotel & Hospitality Operator
  Banking                          → Banking & Lending
  Lending / Credit                 → Banking & Lending
  Mortgage Brokerage               → Banking & Lending
  Custom Manufacturing             → B2B / Custom Manufacturer
  B2B Product Manufacturer         → B2B / Custom Manufacturer
  Tax Advisory                     → Tax & Audit Advisory
  Biotech                          → Biotechnology
  Fleet Leasing                    → Fleet Leasing & Management
  Manufacturer's Representative    → Manufacturers' Representative
  Business Aviation Services       → Business Aviation Operator (under Hospitality & Travel)
  Managed IT Services              → sub_identity = null (MSPs are the default
                                     flavor of IT Services — no dedicated sub.
                                     Only use Cybersecurity Services / Custom
                                     Software Development / Data Migration
                                     Services / IT Asset Disposition for the
                                     non-MSP flavors.)
  (any Legal Services sub-identity — Corporate Law, Estate Planning Law, etc.)
                                   → sub_identity = null (Legal Services has no subs)

If no listed value is even loosely applicable, set is_new_*=true with your
proposed name. Do NOT propose a new value that is a near-rewrite of an
existing library entry, and do NOT propose any of the removed values above.`;
