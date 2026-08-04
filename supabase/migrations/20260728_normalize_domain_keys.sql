-- =====================================================================
-- Canonical domain keys for the enrichment caches
--
-- Every URL form of the same company was a separate cache key:
--   acme.com · https://www.acme.com/ · https://acme.com/
--   http://www.acme.com/ · http://acme.com/ · https://www.acme.com
-- so the same site was scraped and stored repeatedly, and an
-- email-derived domain (always bare, e.g. acme.com) could not match a
-- website-derived one (97% of which are stored as http://www.x.com).
--
-- Measured before this migration:
--   scraped_data              1,633,851 rows
--   distinct once normalised  1,427,323
--   redundant duplicates        206,528  (12.6%)
--   rows needing a new key    1,463,907  (90%)
--   contacts with a website   4,353,812, of which 4,220,851 non-bare
--
-- Scope decision: normalise the *key*, never the stored data.
-- contacts.company_website keeps the user's original imported URL —
-- rewriting it would discard paths, churn ~1.4 GB, and gain nothing
-- because the lookup key is computed, not read from that column.
--
-- Idempotent: safe to re-run.
--
-- Apply with:  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f <this file>
-- in autocommit (no -1/--single-transaction) — CREATE/DROP INDEX
-- CONCURRENTLY below cannot run inside a transaction block.
-- =====================================================================

-- The default statement_timeout on this project is 2 min, which the
-- 1.46M-row rewrite in step 2 exceeds. Must be set in-session: PGOPTIONS
-- does not survive the Supabase connection pooler.
SET statement_timeout = 0;

-- ---------------------------------------------------------------------
-- 1. The canonical form.
--
-- MUST stay byte-identical to normalizeDomain() in
-- services/jobProcessor.ts — a divergence silently misses the cache
-- rather than erroring, which is exactly how the two earlier
-- cache bugs hid. Parity is asserted against real data before shipping.
--
-- Subdomains are preserved on purpose: careers.acme.com is genuinely a
-- different site, and collapsing arbitrary subdomains would require a
-- public-suffix list. Only the ubiquitous `www.` is stripped.
--
-- IMMUTABLE so it can back an index. NOTE: if this definition ever
-- changes, every index built on it must be REINDEXed — Postgres will
-- not detect the change and will silently return wrong results.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION normalize_domain(raw text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT nullif(
        rtrim(
            regexp_replace(                                  -- 4. drop path/query/fragment/port
                regexp_replace(                              -- 3. drop leading www.
                    regexp_replace(                          -- 2. drop scheme
                        lower(btrim(coalesce(raw, ''), E' \t\r\n\f\v')),   -- 1. trim + lowercase
                        '^[a-z][a-z0-9+.-]*://', ''),
                    '^www\.', ''),
                '[/?#:].*$', ''),
            '.'),                                            -- 5. drop trailing dots
        '');
$$;

-- ---------------------------------------------------------------------
-- 2. Collapse the scrape cache onto canonical keys.
--
-- This is what makes the change worth doing: without it, every one of
-- the 1.63M existing scrapes would become unreachable under the new key
-- and would be paid for again.
--
-- Dedupe first, then rewrite — the reverse order would violate the
-- unique constraint on `domain` mid-statement. Wrapped in one
-- transaction so there is never a partially-normalised state.
-- Keeps the freshest row per canonical domain.
-- ---------------------------------------------------------------------
BEGIN;

DELETE FROM scraped_data
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               row_number() OVER (
                   PARTITION BY normalize_domain(domain)
                   ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
               ) AS rn
        FROM scraped_data
    ) ranked
    WHERE rn > 1
);

UPDATE scraped_data
   SET domain = normalize_domain(domain)
 WHERE domain IS DISTINCT FROM normalize_domain(domain);

COMMIT;

-- ---------------------------------------------------------------------
-- 3. Drop the redundant index.
--
-- `domain` already has a UNIQUE constraint (scraped_data_domain_key),
-- which is itself a btree index; idx_scraped_data_domain duplicated it
-- for no benefit and cost ~100 MB.
-- ---------------------------------------------------------------------
DROP INDEX IF EXISTS idx_scraped_data_domain;

-- ---------------------------------------------------------------------
-- 4. Rebuild the contacts resolved-domain index on the canonical form.
--
-- Replaces the previous index, which used the raw expression. The index
-- expression must match get_domain_intelligence()'s join and the JS
-- resolver exactly, or the planner will not use it (and the lookup
-- reverts to a ~9 s seq scan over all 4.4M contacts).
--
-- CONCURRENTLY so it never takes a write lock on a live table. Cannot
-- run inside a transaction — apply this file in autocommit.
-- ---------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contacts_norm_domain
    ON contacts (normalize_domain(COALESCE(NULLIF(btrim(company_website), ''), split_part(email, '@', 2))));

DROP INDEX CONCURRENTLY IF EXISTS idx_contacts_resolved_domain;

-- ---------------------------------------------------------------------
-- 5. Domain intelligence, keyed on the canonical domain.
--
-- Same filters as before (completed, confidence >= 7); only the join key
-- changes, so a website-derived and an email-derived contact at the same
-- company now share one cache entry.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_domain_intelligence(p_domains text[])
RETURNS TABLE (
    domain         text,
    classification text,
    confidence     smallint,
    reasoning      text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT DISTINCT ON (r.domain)
           r.domain,
           e.classification,
           e.confidence,
           e.reasoning
    FROM unnest(p_domains) AS r(domain)
    JOIN contacts c
      ON normalize_domain(COALESCE(NULLIF(btrim(c.company_website), ''), split_part(c.email, '@', 2))) = r.domain
    JOIN enrichments e
      ON e.contact_id = c.contact_id
    WHERE e.status = 'completed'
      AND e.confidence >= 7
    ORDER BY r.domain, e.confidence DESC;
$$;

GRANT EXECUTE ON FUNCTION normalize_domain(text) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION get_domain_intelligence(text[]) TO anon, authenticated, service_role;
