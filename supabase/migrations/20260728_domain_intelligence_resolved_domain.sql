-- =====================================================================
-- Domain intelligence for email-derived domains
--
-- Enrichment resolves a contact's scrape target as:
--     company_website, else the domain part of the email
-- (resolveContactDomain in services/jobProcessor.ts).
--
-- The digest cache already keys on that resolved domain. The
-- domain-intelligence cache did not: it looked up prior high-confidence
-- classifications by joining contacts on `company_website` directly, so
-- a contact whose domain came from its email could only ever match if
-- some *other* contact happened to carry that exact string in its
-- website column. Measured: 0 domain_intelligence hits across 122,703
-- enriched website-less contacts.
--
-- This migration makes the lookup key the *resolved* domain for both
-- kinds of contact, so an email-derived domain reuses a classification
-- exactly like a website-derived one.
--
-- Chosen over adding an `enrichments.domain` column + backfill: that
-- would have rewritten ~4M rows, producing ~3 GB of dead tuples on a
-- disk already past 60%. An expression index costs a fraction of that
-- and needs no rewrite of existing data.
--
-- Idempotent: safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Expression index on the resolved domain.
--
-- Without this the lookup below is a Parallel Seq Scan over every
-- contact — measured at 9.1 s, and it runs once per enrichment chunk.
--
-- The expression must match the RPC's join condition character for
-- character or the planner will not use it.
--
-- Deliberately NOT lowercased: services/jobProcessor.ts does not
-- lowercase the domain it scrapes with or caches under (only the
-- personal-email check lowercases), so folding case here would make the
-- index disagree with the runtime key. Normalising case/protocol/www is
-- a separate, larger change that would also restate the scraped_data
-- key space.
--
-- CONCURRENTLY so this never takes a write lock on a live contacts
-- table. Cannot run inside a transaction block — apply this file with
-- psql in autocommit (no -1 / --single-transaction).
-- ---------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contacts_resolved_domain
    ON contacts ((COALESCE(NULLIF(btrim(company_website), ''), split_part(email, '@', 2))));

-- ---------------------------------------------------------------------
-- 2. Batch lookup: resolved domain -> best high-confidence classification
--
-- Takes the chunk's domain list and returns at most one row per domain,
-- the highest-confidence completed classification for it. Mirrors the
-- previous inline PostgREST query's filters (status='completed',
-- confidence >= 7) so reuse behaviour is unchanged for contacts that
-- already worked — they simply arrive via the resolved domain now.
--
-- STABLE + SECURITY DEFINER: the enrichment worker may run as anon when
-- SUPABASE_SERVICE_ROLE_KEY is misconfigured (see the config check in
-- server.ts), and this function only reads and returns already-derived
-- classification data, no PII beyond the domain the caller supplied.
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
      ON COALESCE(NULLIF(btrim(c.company_website), ''), split_part(c.email, '@', 2)) = r.domain
    JOIN enrichments e
      ON e.contact_id = c.contact_id
    WHERE e.status = 'completed'
      AND e.confidence >= 7
    ORDER BY r.domain, e.confidence DESC;
$$;

GRANT EXECUTE ON FUNCTION get_domain_intelligence(text[]) TO anon, authenticated, service_role;
