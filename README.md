<div align="center">
<img width="1200" height="475" alt="GHBanner" src="https://github.com/user-attachments/assets/0aa67016-6eaf-458a-adb2-6e31a0763ed6" />
</div>

# Run and deploy your AI Studio app

This contains everything you need to run your app locally.

View your app in AI Studio: https://ai.studio/apps/drive/1SsZS6oTE5W2czc9diNkXfHeyPalSAtHX

## Run Locally

**Prerequisites:**  Node.js


1. Install dependencies:
   `npm install`
2. Set the `GEMINI_API_KEY` in [.env.local](.env.local) to your Gemini API key
3. Run the app:
   `npm run dev`
# industry-classifiction-vgeri
# industry-classifiction-vgeri
# industry-classifiction-vgeri

## Bucketing V3 process

### Deleting a bucketing run

Deleting a run from the Bucketing UI calls:

```http
DELETE /api/bucketing/runs/:id
```

The server deletes the row from `bucketing_runs`. The run-scoped tables use
`ON DELETE CASCADE`, so the related rows are removed with it:

- `bucket_industry_map`
- `bucket_contact_map`
- `bucket_assignments`
- `bucketing_run_logs`
- `bucket_library_usage`

If the UI delete succeeds, assigned buckets for that run are removed. If cleanup
ever needs to be run manually, delete the run row:

```sql
DELETE FROM bucketing_runs
WHERE id = '<bucketing_run_id>';
```

If you need to clear assignments while keeping the run for forensics, clear only
the run-scoped assignment/map rows:

```sql
DELETE FROM bucket_contact_map
WHERE bucketing_run_id = '<bucketing_run_id>';

DELETE FROM bucket_assignments
WHERE bucketing_run_id = '<bucketing_run_id>';

DELETE FROM bucket_industry_map
WHERE bucketing_run_id = '<bucketing_run_id>';
```

### Phase 1A: taxonomy discovery

Phase 1A looks at the selected lists' enriched industry vocabulary and discovers
the reusable taxonomy. It does not assign final campaign buckets yet.

It produces:

- `primary_identity`: the broad functional core of the company, such as
  `Agency`, `Consulting & Advisory`, `Software & SaaS`, or `Financial Services`.
- `functional_specialization`: the subtype under that identity, such as
  `SEO Agency`, `Private Equity Firm`, or `Managed IT Services`.
- `sector_focus_vocabulary`: allowed sector terms used later for served-market
  context, such as `Healthcare`, `Real Estate`, or `Manufacturing`.

There is no separate persisted `sector_core` field in the current schema.
Sector information is stored as `sector_focus` during Phase 1B.

### Phase 1B: per-contact routing

Phase 1B routes every selected contact, not just distinct industry strings.
For each contact it stores a pre-rollup row in `bucket_contact_map` and a final
row in `bucket_assignments`.

Per contact, the assignment stores:

- `primary_identity`
- `functional_specialization`
- `sector_focus`
- `pre_rollup_bucket_name`
- final `bucket_name`
- `rollup_level`
- `general_reason`, confidence, source, and model reasons

Contacts with failed enrichment, `Site Error`, `Scrape Error`, `Unknown`, or
empty industry data go to `General` with an explicit reason. Usable contacts
are routed with library matching, strict embedding shortlisting/auto-match, and
then the Phase 1B LLM.

### Volume rollup

After every selected contact has a pre-rollup decision, the app counts the total
contacts at each level and applies the user-configured `min_volume` and
`bucket_budget`.

The rollup order is:

1. `{sector_focus} {functional_specialization}`
2. `functional_specialization`
3. `primary_identity`
4. `General`

Example with `min_volume = 1000`:

- If `Real Estate SEO Agency` has at least 1000 contacts, use that bucket.
- If it has fewer than 1000, roll those contacts to `SEO Agency`.
- If `SEO Agency` also has fewer than 1000, roll them to `Agency`.
- If `Agency` has fewer than 1000, roll them to `General`.

If the number of populated non-General buckets exceeds `bucket_budget`, the app
rolls up the smallest buckets one level at a time until the budget is satisfied.
The diagnostic script is for inspection after a run; it is not what performs
rollup:

```bash
npx tsx scripts/bucketing-diagnostics.ts <bucketing_run_id>
```


## Enrichment `source` values

Every row in `enrichments` is tagged with a `source` string describing
*how* that classification was obtained. The Proxy Performance dashboard
(`/api/stats/proxies`) groups on this column and expects every completed
or failed enrichment to fall into exactly one bucket. If you add a new
code path that writes to `enrichments`, add a new `source` value here and
in `server.ts` (`REUSE_LABELS` / `ERROR_LABELS`).

| source | bucket | meaning |
|---|---|---|
| `Direct Fetch` | Proxy | Scraped directly from the origin (no proxy), then classified by OpenAI. |
| `Corsproxy.io (Business)` | Proxy | Scraped via Corsproxy.io paid tier (`CORSPROXY_API_KEY`), then classified. |
| `Codetabs` | Proxy | Scraped via Codetabs free proxy, then classified. |
| `Corsfix` | Proxy | Scraped via Corsfix free proxy, then classified. |
| `AllOrigins` | Proxy | Scraped via AllOrigins free proxy, then classified. |
| `digest_cache` | Reuse | Same domain was already scraped in this chunk (or earlier and persisted in `scraped_data`) — reused the HTML digest, still ran OpenAI. |
| `domain_intelligence` | Reuse | Same domain already has a high-confidence (≥7) classification in `enrichments` — short-circuited: no scrape, no AI call. |
| `error:no_domain` | Error | Contact had no `company_website` and no usable email domain — nothing to scrape. |
| `error:personal_email` | Error | Domain is a personal-email provider (`gmail.com`, `outlook.com`, …) — intentionally skipped. |
| `error:scrape` | Error | Terminal scrape failure after all proxies retried and exhausted. |
| `error:ai` | Error | Terminal OpenAI failure after `MAX_RETRIES_TRANSIENT` (for 5xx / timeout) or `MAX_RETRIES` (for 4xx / parse) attempts. |
| `unknown` / `NULL` | — | Legacy rows written before the `source` column was added. Count toward total volume but aren't attributed to any bucket in the dashboard. |

The reuse sources (`digest_cache`, `domain_intelligence`) are the reason
the dashboard's "Total Volume" is usually larger than the sum of live
proxy scrapes in the same date range.



# CSV EXPORT SCRIPT (SUPABASE) 
## Also saved in supabase SQLs
with contacts_dedup as (
  -- 1 row per unique email (case-insensitive). If duplicates, keep the most recently updated/created.
  select distinct on (lower(trim(email)))
    contact_id,
    lead_list_name,
    first_name,
    last_name,
    email,
    company_website,
    -- normalize website -> domain to match scraped_data.domain
    nullif(
      lower(
        split_part(
          replace(replace(coalesce(company_website,''), 'https://', ''), 'http://', ''),
          '/',
          1
        )
      ),
      ''
    ) as website_domain,
    updated_at,
    created_at
  from contacts
  where email is not null
    and trim(email) <> ''
    and lead_list_name = 'Wealth management - Phase 0 Industry Classification 25 - 50 part 2'
  order by lower(trim(email)), updated_at desc nulls last, created_at desc nulls last
)

select
  -- contacts
  c.contact_id,
  c.lead_list_name,
  c.first_name,
  c.last_name,
  c.email,
  c.company_website,

  -- enrichments (latest per contact)
  e.classification,
  e.confidence,
  e.reasoning,
  e.cost,
  e.status,

  -- scraped_data (latest per domain)
  s.proxy_used

from contacts_dedup c

left join lateral (
  select
    classification,
    confidence,
    reasoning,
    cost,
    status
  from enrichments
  where contact_id = c.contact_id
  order by updated_at desc nulls last, created_at desc nulls last
  limit 1
) e on true

left join lateral (
  select
    proxy_used
  from scraped_data
  where domain = c.website_domain
  order by updated_at desc nulls last, created_at desc nulls last
  limit 1
) s on true

order by lower(trim(c.email));
