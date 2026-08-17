-- ════════════════════════════════════════════════════════════════════
-- Repair employees_count values that a spreadsheet turned into dates
-- ════════════════════════════════════════════════════════════════════
--
-- The three "Zoom Info Good Industries 2" lists were exported from
-- ZoomInfo and opened in Excel/Google Sheets before upload. The employee
-- *range* column is text like "10-19", which a spreadsheet happily reads
-- as a date and rewrites. By the time the CSV reached the import it was
-- already wrong; the import is a faithful TEXT passthrough (COPY into a
-- staging table, no coercion), so nothing in the app could have caught
-- it. Nothing downstream reads employees_count either — it never touched
-- enrichment, classification or bucketing, only the export.
--
-- Observed damage (738,077 contacts, confirmed as the entire date-like
-- population across every list and every firmographic column — founded
-- year, funding and revenue are clean):
--
--   PART 1     28,831  '20/4/2025'  →  '4 - 20'    (20 Apr → month 4, day 20)
--   PART 2    669,301  '20/4/2025'  →  '4 - 20'
--   PART 3     39,945  '19-Oct'     →  '10 - 19'   (19 Oct → month 10, day 19)
--   PART 3          1  'E-Pal'      →  NULL        (a company name; one shifted row)
--
-- The '4 - 20' reading is the user's confirmed ZoomInfo filter, and is
-- the same month-day interpretation that demonstrably produced '19-Oct'
-- from '10 - 19' in PART 3. Output spacing matches PART 3's surviving
-- clean values ('0 - 9', '15 - 20').
--
-- Idempotent: each statement is scoped to the exact mangled literal, so
-- a re-run matches nothing.

BEGIN;

UPDATE public.contacts
   SET employees_count = '4 - 20'
 WHERE lead_list_name IN (
           'Zoom Info Good Industries 2 PART 1',
           'Zoom Info Good Industries 2 PART 2'
       )
   AND employees_count = '20/4/2025';

UPDATE public.contacts
   SET employees_count = '10 - 19'
 WHERE lead_list_name = 'Zoom Info Good Industries 2 PART 3'
   AND employees_count = '19-Oct';

-- Not a mangled date but garbage all the same: a company name sitting in
-- the employee-count column of a single shifted row.
UPDATE public.contacts
   SET employees_count = NULL
 WHERE lead_list_name = 'Zoom Info Good Industries 2 PART 3'
   AND employees_count = 'E-Pal';

COMMIT;
