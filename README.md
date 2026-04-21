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