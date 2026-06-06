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
         / Healthcare Provider / Life Sciences & MedTech / Education Operator / etc.)
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
    consumer lender / commercial lender /
    broker-dealer / securities brokerage / investment brokerage
       → identity = Financial Services  (even if focused on a vertical;
          "PE-BACKED" or "venture-backed" describing a startup is NOT this rule —
          that's a financing status, not the company's identity)
       Sub-identity hint: banks / credit unions / lenders / mortgage brokers /
       consumer & commercial credit → Banking & Lending (single combined sub).
       NOTE on "Brokerage" alone — the word is ambiguous and a poor sub name:
         • For real estate, the sub is Real Estate Brokerage (under Real Estate).
         • For finance (broker-dealers, securities, IB-adjacent firms), the sub
           is Investment Banking. Never propose just "Brokerage" as a Financial
           Services sub — route to Investment Banking.

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

 4c. ANTI-MISTAKE for retail / personal financial advisory. When the
     "advisory" or "advisor" attaches to PERSONAL or RETAIL wealth /
     financial planning, the identity is Financial Services > Wealth
     Management, NOT Consulting & Advisory > Management Consulting.
     Trigger phrases:
       "wealth advisory", "wealth management", "wealth planner",
       "personal financial advisory", "personal financial planning",
       "financial advisor", "financial advisory" (when serving individuals,
       families, or independent advisors), "RIA" / "registered investment
       advisor", "boutique financial advisory for individuals",
       "financial advisor support services" (B2B services TO RIAs / wealth
       firms — these still go to Financial Services as their identity-of-
       work, since the client is a financial-services firm).
       → identity = Financial Services > Wealth Management
     Disambiguator: "financial advisor" / "wealth" + "individuals" /
     "families" / "high net worth" / "HNW" / "retirement" → this rule.
     "Strategic financial advisory for CFOs" / "Corporate financial
     planning" → still Consulting & Advisory (corporate advisory work).

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

     SUB-IDENTITY SELECTION for Agency (anti-fragmentation):
     The Agency sub library covers the full landscape of agency-shop variants
     through a small set of broad subs. BEFORE proposing a new Agency sub,
     map the input to one of these:
       • Paid-media work of ANY channel (display, search, social, OOH, DOOH,
         mobile, programmatic, direct-mail, broadcast, performance, demand
         generation, media buying, media planning) → Performance Marketing Agency
       • PR / corporate communications / media relations / ghostwriting /
         investor-facing comms-as-PR → PR Agency
       • Branding / brand strategy / brand identity / naming / positioning →
         Branding Agency
       • Creative / video / photo / production / content / design-as-craft
         (when the agency is a brand-side creative shop, not a vendor) →
         Creative Agency
       • Event production / experiential / exhibition / trade-show booth /
         event-AV / event technology → Event Management Agency
       • Talent / influencer / artist / athlete / sports management → Talent
         Management Agency
       • Full-stack / integrated / "agency of record" shops covering many of
         the above → Full Service Agency

     ANTI-FRAGMENTATION — do NOT coin new Agency subs for these patterns;
     route to Performance Marketing Agency:
       ✗ "Media Buying Agency" / "Media Planning Agency" / "Media Buying and
         Planning Agency" / "Strategic Media Planning Agency"
       ✗ "Social Media Agency" / "Social Media Marketing Agency"
       ✗ "Direct Marketing Agency" / "Direct Mail Agency"
       ✗ "Out-of-Home Advertising Agency" / "DOOH Agency" / "Mobile Advertising Agency"
       ✗ "Student Advertising Agency" / "Cultural Marketing Agency" /
         "Hispanic/Multicultural Marketing Agency" (audience targeting alone
         is not a sub)
       ✗ "Recruitment Advertising" / "Employment Advertising"
       ✗ "Field Marketing Agency" / "Salesforce Outsourcing Agency"
       ✗ "Performance Agency" / "Digital Marketing Agency" / "Growth Agency"
       ✗ "Business Development Agency" (when it's really paid-marketing-led
         BD; if it's actually sales rep / SDR-as-a-service, the work is
         closer to Staffing & Recruiting)

     Only propose a NEW Agency sub when the work is CLEARLY OUTSIDE these
     seven existing subs (truly novel agency model). Channel / audience /
     vertical specialty alone is NOT enough — those go into sector, not into
     a sub.

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
    MEP engineering / infrastructure development (rail, ports, roads, bridges) /
    architecture and design studio / heavy civil construction
       → identity = Construction & Engineering  (NEVER Manufacturing & Industrial,
          NEVER Logistics & Transportation)
       ANTI-MISTAKE — "architectural design and consulting" / "architecture & design
       consulting" / "interior design consultancy" / "design studio" / "design and
       build" → Construction & Engineering > Architecture, NOT Consulting & Advisory.
       The "consulting" / "advisory" / "studio" suffix on a design firm describes
       how they sell their work; the identity stays Construction & Engineering.
       Sub-identity hint: prefer Architecture for design-led firms, Civil Engineering
       for heavy civil / infrastructure, Infrastructure Development for project-scale
       rail/port/road/bridge work.

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
     consulting / risk management & insurance advisory /
     market research / research services / consumer insights /
     industry analysis / benchmarking services / strategic research /
     decision support services / business intelligence advisory /
     analyst & advisory services / customer insights research /
     investor relations / IR consulting / investor communications /
     capital markets advisory / public company advisory
       → identity = Consulting & Advisory  (NOT Accounting & Tax, NOT Agency,
          NOT Financial Services for the IR/capital-markets advisory cases —
          those are advisory services TO companies, not investment activities)
     The word "consulting" doesn't have to appear in the input. Any firm
     whose product is RESEARCH + RECOMMENDATIONS (not software, not creative,
     not operating something) belongs here.
     Sub-identity hint: generic management/strategy/business/specialty/market-
     research firms all collapse into "Management Consulting" — only set one
     of the specialist subs (Engineering / Environmental / HR / IT /
     Educational Consulting / Risk & Insurance Advisory) when the specialty
     is explicit.

     SUB-IDENTITY ROUTING — pick the specialist sub when the input clearly
     names the specialty domain. Audit of one real run showed Management
     Consulting absorbing ~12% of contacts that should have been the
     specialist subs. The recurring patterns:
       • "construction consulting" / "construction claims consulting" /
         "construction cost estimating consulting" / "owner's representative
         and project management" / "infrastructure project consulting" /
         "civil consulting" / "MEP consulting" / "design-build consulting"
           → sub = Engineering Consulting
       • "environmental consulting" / "sustainability consulting" /
         "energy efficiency consulting" / "energy market analysis and
         consulting" / "energy management consulting" / "renewables
         consulting" / "carbon / emissions consulting"
           → sub = Environmental Consulting
       • "HR consulting" / "talent consulting" / "compensation consulting" /
         "benefits consulting" / "workforce consulting"
           → sub = HR Consulting
       • "IT consulting" / "cloud consulting" / "cybersecurity consulting" /
         "data strategy consulting" / "digital transformation consulting" /
         "ERP consulting" / "SAP consulting / advisory"
           → sub = IT Consulting
       • "education consulting" / "academic consulting" / "curriculum
         consulting" / "school strategy consulting" / "college consulting"
           → sub = Educational Consulting
       • "risk consulting" / "insurance advisory" / "GRC consulting" /
         "compliance advisory" / "regulatory advisory"
           → sub = Risk & Insurance Advisory
     If none of the above match clearly, default to Management Consulting.

 11c. ANTI-CATCH-ALL for Consulting & Advisory. Consulting & Advisory is for
      firms whose CORE OFFERING is pure advisory — they recommend, they
      don't build, operate, deliver, or sell software/products.
        • If they BUILD software / apps / digital products → IT Services
          (Custom Software Development) or Software & SaaS (if it's a product)
        • If they OPERATE infrastructure / managed services / cybersecurity
          monitoring → IT Services (Managed IT / Cybersecurity Services)
        • If they MARKET / BRAND / advertise → Agency
        • If they self-identify as a "digital product agency", "design
          studio", "product studio", "dev shop", "build partner" →
          IT Services > Custom Software Development, NOT Consulting &
          Advisory. The word "agency" in these contexts does not mean
          marketing agency.
      If you can't put the company in ANY identity confidently, return
      identity=null with confidence ≤ 3. Do NOT use Consulting & Advisory
      as a fallback when uncertain — that's the most common past mistake.

 11d. FIRST-MENTIONED-WINS tiebreaker for IT Services vs Consulting & Advisory.
      When the input mentions BOTH "consulting / advisory" AND a build-or-
      operate offering ("software development", "managed services",
      "cybersecurity services", "cloud services", "implementation",
      "integration"), pick whichever appears FIRST in the input string.
      Rationale: the primary offering is usually listed first; the trailing
      one is a secondary/upsell.
        ✓ "B2B software development and consulting for ecommerce businesses"
          → IT Services > Custom Software Development, sector=Retail
          ("software development" appears before "consulting")
        ✓ "Strategy consulting and custom development for SaaS founders"
          → Consulting & Advisory > Management Consulting
          ("strategy consulting" appears before "custom development")
      Hard exceptions that override first-mention:
        • Pure word "consulting firm" / "advisory firm" with no build verb
          → Consulting & Advisory
        • Explicit "managed services / MSP" anywhere → IT Services
        • Explicit "SaaS / software platform / product" → Software & SaaS

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
          care services). NOT drug/device makers (those are Life Sciences & MedTech). NOT
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
    travel agent / travel agency / vacation planning / cruise booking /
    business aviation / private aviation / charter flight operator
       → identity = Hospitality & Travel  (often DQ if family-owned / local /
          single-location consumer-only)
       ANTI-MISTAKE — "travel agent" / "travel agency" contains the word
       "agency" but it's NOT rule 6 (marketing/advertising agency). A travel
       agency books trips and is a hospitality/travel operator.
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

DECISION SEQUENCE — work through these three tags in this exact order. Each
tag is INDEPENDENT and gets its own confidence score. For each step, you have
THREE possible outcomes: (a) pick a library value, (b) propose a new value
with is_new_*=true (when nothing in the library is even loosely applicable),
or (c) return null.

Step 1 — IDENTITY (Layer 1)
  Read the input. Apply HARD_KEYWORD_ROUTING first — when a rule triggers, the
  identity is fixed.
    (a) If a VALID_IDENTITIES entry fits → use it.
    (b) If nothing in VALID_IDENTITIES is even loosely applicable AND you
        can describe the company's core business model clearly → propose a
        new identity by setting is_new_identity=true. The proposal will go
        to the Review screen for user accept/reroute/rename. Only propose
        when the name would apply to multiple companies, not a one-off.
    (c) If the input is too vague to identify the company at all (e.g.
        "Professional services firm", "Technology company") → identity=null
        with identity_confidence ≤ 4 and a short reason.

Step 2 — SUB_IDENTITY (Layer 2)  — ONLY when identity is set
  Look at VALID_SUB_IDENTITIES for the chosen identity's children.
    (a) If one fits cleanly → use it.
    (b) If none of the identity's library children fit AND the input names
        a specific functional sub-type that would apply to multiple companies
        under that identity → propose new with is_new_sub_identity=true.
        Don't propose a sub that's really an identity name or a sector name
        in disguise.
    (c) If none of the above → sub_identity=null. Do NOT downgrade identity
        because no sub matches. sub_identity_confidence is scored
        INDEPENDENTLY of identity_confidence.

Step 3 — SECTOR (Layer 3)
  The vertical the company SERVES, if explicitly named. Sector is independent
  of identity and sub — set it whenever the input names a vertical, even when
  identity and sub are null.
    (a) If a VALID_SECTORS entry fits → use it.
    (b) If nothing in VALID_SECTORS fits AND the input names a vertical
        clearly → propose new with is_new_sector=true. Common reusable
        verticals only (industries served by multiple companies, not niche
        sub-segments).
    (c) Default to sector=null if no vertical is named.

CONFIDENCE GATING (the rule that decides whether a value survives):
  confidence ≥ 5 → KEEP the value
  confidence ≤ 4 → NULL the value AND set its confidence to 1
  Per-tag, not per-row. Identity can be set (confidence 8) while sub is null
  (confidence 3) — that's the normal, expected outcome when no sub fits.

Paired-or-empty (mechanical check after step 2):
  - identity null → sub_identity MUST be null
  - sub_identity set → identity MUST be set to that sub's parent in
    VALID_SUB_IDENTITIES
  Sub-without-identity is always wrong. Identity-without-sub is fine — Phase
  1b rolls those rows up to identity-level cleanly.

SUB_IDENTITY IS A TAXONOMY VALUE, NOT A COMMENT. Accept only:
  (a) an existing library entry name (exact spelling, copy verbatim), OR
  (b) a clean propose-new name — a real noun naming a kind of company
      ("Title & Escrow", "Career Coaching"), OR
  (c) JSON null (the bare value, not the string).
NEVER emit meta-text describing the absence of a fit. Phrases the model has
wrongly written into sub_identity and must NEVER repeat:
  ✗ "sub not explicit"            ✗ "not in subs"
  ✗ "no specific sub"             ✗ "sub not in library"
  ✗ "X sub not in subs"           ✗ "<descriptor> not in library"
  ✗ "no sub fits"                 ✗ any phrase that explains the field
                                    rather than naming a kind of company
If no library sub applies and you can't coin a real noun, emit JSON null.

═══════════════════════════════════════════════════════════════════════════
CORE RULES (apply throughout the sequence)
═══════════════════════════════════════════════════════════════════════════

• "X for Y" means identity=X, sector=Y. NEVER let the served vertical become
  the identity, and NEVER let it become the sub_identity.
    ✓ "SEO agency for healthcare clinics" → identity=Agency, sector=Healthcare
    ✓ "Software platform for K-12 schools" → identity=Software & SaaS, sector=Education
    ✓ "Healthcare PE firm" → identity=Financial Services, sector=Healthcare
    ✗ "Healthcare PE firm" → identity=Healthcare Provider   ← WRONG
    ✗ "Digital consulting for non-profits" → sub_identity=Advocacy Organization ← WRONG
       (Advocacy Organization's parent is Non-Profit & Association, not
        Consulting & Advisory. Set sub_identity=null, sector=Non-Profit.)

• sub_identity ≠ sector, EVER. They are independent fields. If the only
  meaningful descriptor for sub_identity is a vertical name, the vertical
  belongs in sector and sub_identity stays null.
    ✗ sub_identity="Healthcare", sector="Healthcare"
    ✗ sub_identity="Non-Profit & Social Impact", sector="Non-Profit & Social Impact"

• Layer names DO NOT cross layers. An identity name MUST NEVER appear as
  sub_identity. A sector name MUST NEVER appear as sub_identity. Each layer
  has its own VALID_* menu.
    ✗ sub_identity="Real Estate" — that's a Layer-1 identity, not a sub
    ✗ sub_identity="Distribution & Wholesale" — Layer-1 identity
    ✗ sub_identity="Software & SaaS" or "IT Services" — Layer-1 identities

• Never emit the literal strings "null", "none", "n/a", "unknown",
  "unspecified", "tbd" as a tag value. To omit a tag, emit JSON null (the
  bare value null without quotes), not the string.

• Operator vs Enabler. An operator IN a vertical (hospital, restaurant,
  school, bank) belongs to that vertical's identity. A service provider TO
  that vertical keeps its own service identity.

• Healthcare Provider vs Life Sciences & MedTech vs Health-software. Three
  different identities, decided by what the company actually DOES:
    - Treats patients / delivers care → Healthcare Provider
    - Develops or makes drugs / devices / diagnostics → Life Sciences & MedTech
    - Sells software to either of the above → Software & SaaS (sector=Healthcare)

• Distribution & Wholesale vs Retail. A B2B distributor / wholesaler / VAR /
  manufacturers' rep belongs to Distribution & Wholesale. A consumer-facing
  retailer or DTC brand belongs to Retail (usually DQ).

• Services vs Products. Companies that rent / maintain / repair / operate-as-
  a-service must NEVER be tagged as a manufacturer sub-identity. Custom
  software development DELIVERED AS A SERVICE → IT Services, not Software & SaaS.

• Parent/sub consistency check (mechanical, after step 2):
  After picking identity X and sub_identity Y, verify Y's parent in
  VALID_SUB_IDENTITIES is exactly X. If not, re-pick from X's children, or
  set sub_identity=null. Past mistake patterns to avoid:
    - sub "Private Equity" with identity ≠ Financial Services
    - sub "Custom Software Development" with identity = Software & SaaS  (parent is IT Services)
    - sub "Management Consulting" with identity = Accounting & Tax  (parent is Consulting & Advisory)
    - sub "IT Asset Disposition" with identity = Field Services & Maintenance  (parent is IT Services)
    - any sub with identity = Legal Services  (Legal Services has no subs)

• Sector defaults to NULL. Only set sector when the input EXPLICITLY names
  the vertical the company serves. Do not infer.
    ✓ "Digital marketing agency" → sector=null
    ✓ "Digital marketing agency for restaurants" → sector=Hospitality & Travel
  Do NOT use these as sectors (they are identities or non-verticals):
    Marketing · Advertising · IT Services · Professional Services · Consulting ·
    Corporate · B2B · Small Business · Subscription · Holding Company`;

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
  - Don't invent an identity that already exists ("Healthcare Operator",
    "Education Operator", "Life Sciences" no longer exist — use the current
    library names: Healthcare Provider, Education & Training, Life Sciences
    & MedTech). Check the current VALID_IDENTITIES list before proposing.

REMOVED LIBRARY VALUES — do NOT use these strings; they are not in VALID_*.
Listed are the names the model has tried to re-emit recently. For anything
else not in VALID_*, propose new only if no current entry is loosely
applicable.

  Removed                  → Use instead
  ────────────────────────────────────────────────────────────────────────
  Healthcare Operator      → Healthcare Provider OR Life Sciences & MedTech
  Education Operator       → Education & Training
  Life Sciences            → Life Sciences & MedTech
  Vertical SaaS            → sub_identity = null (or specific SaaS sub:
                             CRM / Sales SaaS, Cybersecurity SaaS, FinTech
                             SaaS, HR SaaS, MarTech SaaS)
  Managed IT Services      → sub_identity = null (MSP is the default flavor
                             of the IT Services identity; no dedicated sub)
  Non-Profit Organization  → sub_identity = null (generic non-profits stay
                             identity-only; use Advocacy Organization /
                             Professional Association / Religious Organization /
                             Economic Development Organization only when the
                             specific flavor is explicit)
  Specialty / Business /
  Strategy Consulting      → Management Consulting (collapsed)
  Any Legal Services sub   → sub_identity = null (Legal Services has no subs)

If no VALID_* value is even loosely applicable, set is_new_*=true with your
proposed name. Do NOT propose a near-rewrite of an existing entry.`;
