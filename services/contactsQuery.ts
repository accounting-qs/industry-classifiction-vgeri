import type { SupabaseClient } from '@supabase/supabase-js';
import type { FilterCondition, MergedContact } from '../types';

/**
 * The Contacts table query: one page of contacts with their enrichment,
 * after the filter builder, the list pill and the search box.
 *
 * Server side only, run with the service_role client behind
 * POST /api/contacts/page. The browser used to run this exact query
 * itself with the anon key; that was the browser's only direct use of
 * Supabase, and moving it here is what lets the anon role be revoked in
 * the database (supabase/migrations/20261109_lock_down_anon_access.sql).
 */

// Columns the global "Search contacts..." box matches. All are columns
// on the contacts table so the search stays a single-table OR. `industry`
// is the RAW imported value — the enrichment pipeline no longer overwrites
// it (the AI classification lives on enrichments.classification, reachable
// via the dedicated "classification" filter column, not this free-text box).
const CONTACT_SEARCH_COLUMNS = [
  'first_name', 'last_name', 'email', 'company_website',
  'company_name', 'industry', 'lead_list_name',
];

// Filter columns that live on enrichments, not contacts.
const ENRICHMENT_COLS = ['status', 'classification', 'confidence', 'cost', 'processed_at'];

export const FILTER_OPERATORS = [
  'equals', 'contains', 'starts_with', 'greater_than', 'less_than', 'in', 'not_in',
] as const;

// Escape a free-text term for use inside a PostgREST or= ilike value.
// Two layers: (1) LIKE metacharacters (\ % _) so the term matches
// literally rather than as wildcards, then (2) PostgREST's
// double-quoted-value metacharacters (" and the backslashes introduced
// by step 1) so a stray quote/backslash can't break out of the or=
// grammar. The result is wrapped by the caller as "%<term>%".
function escapeSearchTerm(raw: string): string {
  return raw
    .replace(/([\\%_])/g, '\\$1')  // \ % _  ->  \\ \% \_   (literal in ILIKE)
    .replace(/(["\\])/g, '\\$1');  // " \    ->  \" \\       (safe in or= value)
}

// Builds the PostgREST .or() filter string for a search term, or null
// when the (trimmed) term is empty — so a blank/whitespace-only query is
// a no-op instead of an everything-matches "%   %" scan.
function buildContactSearchOr(searchQuery?: string): string | null {
  const trimmed = searchQuery?.trim();
  if (!trimmed) return null;
  const q = `"%${escapeSearchTerm(trimmed)}%"`;
  return CONTACT_SEARCH_COLUMNS.map(col => `${col}.ilike.${q}`).join(',');
}

function applyFilters(query: any, filters: FilterCondition[], searchQuery?: string) {
  // Free-text search across contacts-table columns only (single-table
  // OR). See CONTACT_SEARCH_COLUMNS / buildContactSearchOr above for the
  // field list, the blank-query guard, and the escaping rationale.
  const searchOr = buildContactSearchOr(searchQuery);
  if (searchOr) {
    query = query.or(searchOr);
  }

  filters.forEach(f => {
    // Skip incomplete scalar filters (e.g. a freshly-added cost/date/text
    // filter whose value hasn't been typed yet) so we never send an empty
    // string to eq/ilike/gt/lt — Postgres rejects '' on numeric/timestamp
    // columns. Array-valued filters (status, lead_list_name) are unaffected.
    if (f.value === '' || f.value === null || f.value === undefined) return;

    const isEnrichmentCol = ENRICHMENT_COLS.includes(f.column);
    const colPath = isEnrichmentCol ? `enrichments.${f.column}` : f.column;

    if (f.column === 'status' && Array.isArray(f.value)) {
      const statuses = f.value;
      const hasNew = statuses.includes('new');
      const others = statuses.filter(s => s !== 'new');

      if (hasNew && others.length === 0) {
        // Find contacts with NO record
        query = query.filter('enrichments', 'is', 'null');
      } else if (!hasNew && others.length > 0) {
        // Find contacts with specific existing statuses
        query = query.in('enrichments.status', others);
      } else if (hasNew && others.length > 0) {
        // Mixed: No record OR specific status
        query = query.or(`status.in.(${others.join(',')}),contact_id.is.null`, { foreignTable: 'enrichments' });
      }
    } else {
      // Standard filters (e.g., confidence = 1)
      // If it's an enrichment column, it uses the child path.
      // Because we use !inner join for enrichment cols, this filter
      // will correctly exclude parent rows that don't match the child condition.
      switch (f.operator) {
        case 'equals': query = query.eq(colPath, f.value); break;
        case 'contains': query = query.ilike(colPath, `%${f.value}%`); break;
        case 'starts_with': query = query.ilike(colPath, `${f.value}%`); break;
        case 'greater_than': query = query.gt(colPath, f.value); break;
        case 'less_than': query = query.lt(colPath, f.value); break;
        case 'in': {
          // Single-value `in.("quoted,with,commas")` defeats the index on
          // contacts.lead_list_name (PostgREST wraps comma-containing values
          // in double-quotes and Postgres picks a full-sort plan). Collapse
          // the 1-element case to `eq`, which uses the index and avoids the
          // 8s statement_timeout on large lists.
          const vals = Array.isArray(f.value) ? f.value : [f.value];
          if (vals.length === 1) query = query.eq(colPath, vals[0]);
          else query = query.in(colPath, vals);
          break;
        }
        case 'not_in': {
          const vals = Array.isArray(f.value) ? f.value : [f.value];
          if (vals.length === 1) query = query.neq(colPath, vals[0]);
          else if (vals.length > 0) query = query.not(colPath, 'in', `(${vals.join(',')})`);
          break;
        }
      }
    }
  });
  return query;
}

function flattenData(data: any[] | null): MergedContact[] {
  return ((data as any[]) || []).map((item: any) => {
    const enrichmentList = item.enrichments;
    const enrichmentDataRaw = Array.isArray(enrichmentList) && enrichmentList.length > 0
      ? enrichmentList[0]
      : (enrichmentList && !Array.isArray(enrichmentList) ? enrichmentList : {});

    // Prevent enrichmentData.id from overwriting contactData.id
    const { id: eId, ...safeEnrichmentData } = enrichmentDataRaw;
    const { enrichments, ...contactData } = item;
    const finalStatus = safeEnrichmentData.status || 'new';

    return {
      ...contactData,
      ...safeEnrichmentData,
      status: finalStatus,
      enrichment_id: eId || null
    };
  });
}

function getJoinConfig(filters: FilterCondition[]) {
  const statusFilter = filters.find(f => f.column === 'status');
  const includesNew = statusFilter && Array.isArray(statusFilter.value) && statusFilter.value.includes('new');

  // Check if any enrichment column (confidence, cost, classification) is being filtered
  const enrichmentFilters = filters.filter(f => ENRICHMENT_COLS.includes(f.column) && f.column !== 'status');
  const hasEnrichmentFilters = enrichmentFilters.length > 0;

  // We use !inner join if filtering for a specific enrichment attribute to exclude rows without that attribute.
  const needsEnrichmentRecord = (hasEnrichmentFilters || (statusFilter && !includesNew)) && !includesNew;

  return needsEnrichmentRecord ? `*, enrichments!inner(*)` : `*, enrichments(*)`;
}

/**
 * Detects mixed status filter: includes 'new' (no enrichment) AND other statuses (has enrichment).
 * This case cannot be handled in a single PostgREST query because:
 * - 'new' requires LEFT JOIN + enrichments IS null
 * - Other statuses require INNER JOIN + enrichments.status IN (...)
 * - Combining with LEFT JOIN + .or({foreignTable}) doesn't filter parent rows
 */
function getMixedStatusInfo(filters: FilterCondition[]): { isMixed: boolean; otherStatuses: string[]; nonStatusFilters: FilterCondition[] } {
  const statusFilter = filters.find(f => f.column === 'status');
  if (!statusFilter || !Array.isArray(statusFilter.value)) {
    return { isMixed: false, otherStatuses: [], nonStatusFilters: filters };
  }
  const statuses = statusFilter.value as string[];
  const hasNew = statuses.includes('new');
  const others = statuses.filter(s => s !== 'new');
  return {
    isMixed: hasNew && others.length > 0,
    otherStatuses: others,
    nonStatusFilters: filters.filter(f => f.column !== 'status'),
  };
}

export async function getPaginatedContacts(
  client: SupabaseClient,
  page: number,
  pageSize: number,
  filters: FilterCondition[] = [],
  searchQuery?: string
): Promise<{ data: MergedContact[], count: number }> {
  const { isMixed, otherStatuses, nonStatusFilters } = getMixedStatusInfo(filters);

  // Mixed status: split into two queries and merge
  if (isMixed) {
    // Query 1: contacts with enrichment status in otherStatuses (INNER JOIN)
    let q1 = client.from('contacts')
      .select('*, enrichments!inner(*)', { count: 'estimated' });
    q1 = applyFilters(q1, nonStatusFilters, searchQuery);
    q1 = q1.in('enrichments.status', otherStatuses);

    // Query 2: "new" contacts with no enrichment (LEFT JOIN + is null)
    let q2 = client.from('contacts')
      .select('*, enrichments(*)', { count: 'estimated' });
    q2 = applyFilters(q2, nonStatusFilters, searchQuery);
    q2 = q2.filter('enrichments', 'is', 'null');

    // Get counts from both
    const [r1Count, r2Count] = await Promise.all([
      q1.order('created_at', { ascending: true }).limit(0),
      q2.order('created_at', { ascending: true }).limit(0)
    ]);

    const count1 = r1Count.count || 0;
    const count2 = r2Count.count || 0;
    const totalCount = count1 + count2;
    const from = (page - 1) * pageSize;

    // Fetch data: "other status" contacts first, then "new" contacts
    let data: any[] = [];

    if (from < count1) {
      // Need items from query 1 (enrichment-based statuses)
      const q1Data = client.from('contacts')
        .select('*, enrichments!inner(*)');
      const applied1 = applyFilters(q1Data, nonStatusFilters, searchQuery);
      const r1 = await applied1.in('enrichments.status', otherStatuses)
        .order('created_at', { ascending: true })
        .range(from, Math.min(from + pageSize - 1, count1 - 1));
      data = data.concat(r1.data || []);
    }

    const remaining = pageSize - data.length;
    if (remaining > 0 && from + data.length >= count1) {
      // Need items from query 2 ("new" contacts)
      const newOffset = Math.max(0, from - count1);
      const q2Data = client.from('contacts')
        .select('*, enrichments(*)');
      const applied2 = applyFilters(q2Data, nonStatusFilters, searchQuery);
      const r2 = await applied2.filter('enrichments', 'is', 'null')
        .order('created_at', { ascending: true })
        .range(newOffset, newOffset + remaining - 1);
      data = data.concat(r2.data || []);
    }

    return { data: flattenData(data), count: totalCount };
  }

  // Standard (non-mixed) query
  let query = client
    .from('contacts')
    .select(getJoinConfig(filters), { count: 'estimated' });

  query = applyFilters(query, filters, searchQuery);

  const { data, error, count } = await query
    .order('created_at', { ascending: true })
    .range((page - 1) * pageSize, page * pageSize - 1);

  if (error) throw error;

  return {
    data: flattenData(data),
    count: count || 0
  };
}
