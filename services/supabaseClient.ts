
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { Contact, Enrichment, MergedContact, FilterCondition } from '../types';

const getEnv = (key: string) => {
  if (typeof import.meta !== 'undefined' && (import.meta as any).env && (import.meta as any).env[key]) {
    return (import.meta as any).env[key];
  }
  if (typeof process !== 'undefined' && process.env && process.env[key]) {
    return process.env[key];
  }
  return undefined;
};

const SUPABASE_URL = getEnv('VITE_SUPABASE_URL') || getEnv('SUPABASE_URL');
const DEFAULT_ANON_KEY = getEnv('VITE_SUPABASE_ANON_KEY') || getEnv('SUPABASE_ANON_KEY');

console.log('🔍 Supabase Config Check:', {
  url: SUPABASE_URL ? 'PRESENT' : 'MISSING',
  key: DEFAULT_ANON_KEY ? 'PRESENT' : 'MISSING'
});

class SupabaseService {
  private client: SupabaseClient | null = null;
  public isConnected: boolean = false;

  constructor() {
    const isBrowser = typeof window !== 'undefined';
    const key = (isBrowser ? localStorage.getItem('supabase_anon_key') : null) || DEFAULT_ANON_KEY;
    if (key) {
      this.init(key);
    }
  }

  init(key: string) {
    try {
      if (!SUPABASE_URL || !key) {
        throw new Error(`Missing Supabase URL (${!!SUPABASE_URL}) or Key (${!!key})`);
      }
      this.client = createClient(SUPABASE_URL, key);
      this.isConnected = true;
      if (typeof window !== 'undefined') {
        localStorage.setItem('supabase_anon_key', key);
      }
      console.log("✅ Supabase initialized successfully");
      return true;
    } catch (e) {
      console.error("❌ Supabase initialization failed:", e);
      return false;
    }
  }

  private applyFilters(query: any, filters: FilterCondition[], enrichmentCols: string[], searchQuery?: string) {
    if (searchQuery) {
      const q = `"%${searchQuery}%"`;
      // Search across contacts table columns only
      // Quoting the value (q) is necessary for PostgREST to parse values with dots or special characters
      query = query.or(`first_name.ilike.${q},last_name.ilike.${q},email.ilike.${q},company_website.ilike.${q},company_name.ilike.${q},industry.ilike.${q},lead_list_name.ilike.${q}`);
    }

    filters.forEach(f => {
      const isEnrichmentCol = enrichmentCols.includes(f.column);
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
          case 'in': query = query.in(colPath, Array.isArray(f.value) ? f.value : [f.value]); break;
        }
      }
    });
    return query;
  }

  private flattenData(data: any[] | null): MergedContact[] {
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

  private getJoinConfig(filters: FilterCondition[], enrichmentCols: string[], enrichedOnly: boolean) {
    const statusFilter = filters.find(f => f.column === 'status');
    const includesNew = statusFilter && Array.isArray(statusFilter.value) && statusFilter.value.includes('new');

    // Check if any enrichment column (confidence, cost, classification) is being filtered
    const enrichmentFilters = filters.filter(f => enrichmentCols.includes(f.column) && f.column !== 'status');
    const hasEnrichmentFilters = enrichmentFilters.length > 0;

    // We use !inner join if filtering for a specific enrichment attribute to exclude rows without that attribute.
    const needsEnrichmentRecord = (hasEnrichmentFilters || (statusFilter && !includesNew)) && !includesNew;

    const useInnerJoin = needsEnrichmentRecord || enrichedOnly;
    return useInnerJoin ? `*, enrichments!inner(*)` : `*, enrichments(*)`;
  }

  async getPaginatedContacts(
    page: number,
    pageSize: number,
    enrichedOnly: boolean = false,
    filters: FilterCondition[] = [],
    searchQuery?: string
  ): Promise<{ data: MergedContact[], count: number }> {
    if (!this.client) return { data: [], count: 0 };

    const from = (page - 1) * pageSize;
    const to = from + pageSize - 1;
    const enrichmentCols = ['status', 'classification', 'confidence', 'cost', 'processed_at'];

    const selectStr = this.getJoinConfig(filters, enrichmentCols, enrichedOnly);

    let query = this.client
      .from('contacts')
      .select(selectStr, { count: 'exact' });

    query = this.applyFilters(query, filters, enrichmentCols, searchQuery);

    const { data, error, count } = await query
      .order('created_at', { ascending: true })
      .range(from, to);

    if (error) throw error;

    return {
      data: this.flattenData(data),
      count: count || 0
    };
  }

  async getAllFilteredContacts(filters: FilterCondition[], searchQuery?: string): Promise<MergedContact[]> {
    if (!this.client) return [];

    const enrichmentCols = ['status', 'classification', 'confidence', 'cost', 'processed_at'];
    const selectStr = this.getJoinConfig(filters, enrichmentCols, false);

    let allData: any[] = [];
    let page = 0;
    const pageSize = 1000;

    while (true) {
      let query = this.client
        .from('contacts')
        .select(selectStr);

      query = this.applyFilters(query, filters, enrichmentCols, searchQuery);

      const { data, error } = await query
        .order('created_at', { ascending: true })
        .range(page * pageSize, (page + 1) * pageSize - 1);

      if (error) throw error;

      if (!data || data.length === 0) break;
      allData = allData.concat(data);
      if (data.length < pageSize) break;

      page++;
    }

    return this.flattenData(allData);
  }

  /**
   * Lightweight: only fetches contact_id for the enrich endpoint.
   * Avoids full join which causes statement timeout on large filtered queries.
   */
  async getAllFilteredContactIds(filters: FilterCondition[], searchQuery?: string): Promise<string[]> {
    if (!this.client) return [];

    const enrichmentCols = ['status', 'classification', 'confidence', 'cost', 'processed_at'];

    // Determine if we need enrichment join for the filter
    const hasEnrichmentFilter = filters.some(f => enrichmentCols.includes(f.column));
    const selectStr = hasEnrichmentFilter
      ? 'contact_id, enrichments!inner(status)'
      : 'contact_id';

    let allIds: string[] = [];
    let page = 0;
    const pageSize = 5000; // Larger page since we only fetch IDs

    while (true) {
      let query = this.client
        .from('contacts')
        .select(selectStr);

      query = this.applyFilters(query, filters, enrichmentCols, searchQuery);

      const { data, error } = await query
        .order('created_at', { ascending: true })
        .range(page * pageSize, (page + 1) * pageSize - 1);

      if (error) throw error;

      if (!data || data.length === 0) break;
      allIds = allIds.concat(data.map((d: any) => d.contact_id));
      if (data.length < pageSize) break;

      page++;
    }

    return allIds;
  }

  async getDistinctValues(column: string): Promise<string[]> {
    if (!this.client) return [];
    const allValues: string[] = [];
    let page = 0;
    const pageSize = 1000;

    while (true) {
      const { data, error } = await this.client
        .from('contacts')
        .select(column)
        .not(column, 'is', null)
        .neq(column, '')
        .range(page * pageSize, (page + 1) * pageSize - 1);

      if (error || !data || data.length === 0) break;
      allValues.push(...data.map((d: any) => d[column]));
      if (data.length < pageSize) break;
      page++;
    }

    return [...new Set(allValues)].sort();
  }

  async enqueueContacts(contactIds: string[]) {
    if (!this.client) return;

    const CHUNK_SIZE = 2000;

    for (let i = 0; i < contactIds.length; i += CHUNK_SIZE) {
      const chunk = contactIds.slice(i, i + CHUNK_SIZE);
      const payloads = chunk.map(id => ({
        contact_id: id,
        status: 'pending',
        processed_at: null,
        error_message: null
      }));

      const { error } = await this.client
        .from('enrichments')
        .upsert(payloads, { onConflict: 'contact_id' });

      if (error) {
        console.error(`Error enqueuing chunk ${i}-${i + CHUNK_SIZE}:`, error);
        throw error;
      }
    }
  }

  async bulkUpsertEnrichments(enrichments: Partial<Enrichment>[]) {
    if (!this.client) return;
    const { error } = await this.client
      .from('enrichments')
      .upsert(enrichments, { onConflict: 'contact_id' });
    if (error) throw error;
  }

  async bulkUpdateContacts(contacts: Partial<Contact>[]) {
    if (!this.client) return;

    // Filter out items without an ID to satisfy "Upsert Only / No New Rows"
    const existingItems = contacts.filter(c => c.id !== undefined && c.id !== null);
    if (existingItems.length === 0) return;

    const cleaned = existingItems.map(c => {
      const { created_at, updated_at, ...payload } = c as any;
      // Null handling for lead_list_name
      if (payload.lead_list_name === undefined) {
        payload.lead_list_name = null;
      }
      return payload;
    });

    const { error } = await this.client
      .from('contacts')
      .upsert(cleaned, { onConflict: 'email' });

    if (error) throw error;
  }
}

export const db = new SupabaseService();
