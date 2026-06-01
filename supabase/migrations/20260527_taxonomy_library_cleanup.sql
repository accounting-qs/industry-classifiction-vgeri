-- Taxonomy library cleanup (2026-05-27)
--
-- Identity changes:
--   1. Set explicit description on Healthcare Provider (was NULL) to stop
--      AI cross-classifying R&D/manufacturer contacts here.
--   2. Rename Life Sciences -> Life Sciences & MedTech + set description.
--   3. Rename Education Operator -> Education & Training.
--
-- Sub-identity changes:
--   4. Strategy & Management Consulting -> Management Consulting.
--   5. Risk Management and Insurance Advisory Services -> Risk & Insurance Advisory.
--   6. CRM / Sales Software -> CRM / Sales SaaS.
--   7. Cybersecurity Software -> Cybersecurity SaaS.
--   8. Brokerage (under Real Estate) -> Real Estate Brokerage.
--   9. Archive Healthcare Nonprofit (use Non-Profit Organization + Healthcare sector).
--  10. Archive Educational Nonprofit (use Non-Profit Organization + Education sector).

BEGIN;

-- =====================================================================
-- 1-3. Identity table changes
-- =====================================================================

UPDATE taxonomy_identities
SET description = 'Delivers care directly to patients. Clinics, hospitals, physician groups, home health, mental health services, care coordination, healthcare support services. Does NOT include companies that manufacture drugs, devices, or diagnostics, or run R&D — those are Life Sciences & MedTech.',
    updated_at = NOW()
WHERE name = 'Healthcare Provider';

UPDATE taxonomy_identities
SET name = 'Life Sciences & MedTech',
    description = 'R&D and manufacturing of therapies, devices, and diagnostics. Biotech, pharmaceuticals, medical device manufacturers, diagnostics labs, contract manufacturing & sterilization. Does NOT include companies that operate clinics or deliver care to patients — those are Healthcare Provider.',
    updated_at = NOW()
WHERE name = 'Life Sciences';

UPDATE taxonomy_identities
SET name = 'Education & Training',
    updated_at = NOW()
WHERE name = 'Education Operator';

-- Propagate identity renames to the sub-identity parent pointer.
UPDATE taxonomy_sub_identities SET parent_identity = 'Life Sciences & MedTech', updated_at = NOW() WHERE parent_identity = 'Life Sciences';
UPDATE taxonomy_sub_identities SET parent_identity = 'Education & Training',    updated_at = NOW() WHERE parent_identity = 'Education Operator';

-- =====================================================================
-- 4-8. Sub-identity renames
-- =====================================================================

UPDATE taxonomy_sub_identities SET name = 'Management Consulting',       updated_at = NOW() WHERE name = 'Strategy & Management Consulting'           AND parent_identity = 'Consulting & Advisory';
UPDATE taxonomy_sub_identities SET name = 'Risk & Insurance Advisory',   updated_at = NOW() WHERE name = 'Risk Management and Insurance Advisory Services' AND parent_identity = 'Consulting & Advisory';
UPDATE taxonomy_sub_identities SET name = 'CRM / Sales SaaS',            updated_at = NOW() WHERE name = 'CRM / Sales Software'                      AND parent_identity = 'Software & SaaS';
UPDATE taxonomy_sub_identities SET name = 'Cybersecurity SaaS',          updated_at = NOW() WHERE name = 'Cybersecurity Software'                    AND parent_identity = 'Software & SaaS';
UPDATE taxonomy_sub_identities SET name = 'Real Estate Brokerage',       updated_at = NOW() WHERE name = 'Brokerage'                                 AND parent_identity = 'Real Estate';

-- =====================================================================
-- 9-10. Archive (soft-delete) two sub-identities better expressed via sector
-- =====================================================================

UPDATE taxonomy_sub_identities SET archived = true, updated_at = NOW() WHERE name = 'Healthcare Nonprofit'  AND parent_identity = 'Non-Profit & Association';
UPDATE taxonomy_sub_identities SET archived = true, updated_at = NOW() WHERE name = 'Educational Nonprofit' AND parent_identity = 'Non-Profit & Association';

COMMIT;

NOTIFY pgrst, 'reload schema';
