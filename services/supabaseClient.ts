
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { Contact, Enrichment, MergedContact, FilterCondition } from '../types';

const getEnv = (key: string) => {
  if (typeof process !== 'undefined' && process.env) return process.env[key];
  // @ts-ignore - Vite specific
  if (typeof import.meta !== 'undefined' && import.meta.env) return import.meta.env[key];
  return undefined;
};

const SUPABASE_URL = getEnv('VITE_SUPABASE_URL') || 'https://zxnaxtdeujunujnjaweo.supabase.co';
const DEFAULT_ANON_KEY = getEnv('VITE_SUPABASE_ANON_KEY');

class SupabaseService {
  private client: SupabaseClient | null = null;
  public isConnected: boolean = false;

  constructor() {
    const key = localStorage.getItem('supabase_anon_key') || DEFAULT_ANON_KEY;
    if (key) {
      this.init(key);
    }
  }

  init(key: string) {
    try {
      this.client = createClient(SUPABASE_URL, key);
      this.isConnected = true;
      localStorage.setItem('supabase_anon_key', key);
      return true;
    } catch (e) {
      console.error("Initialization failed", e);
      return false;
    }
  }

  private applyFilters(query: any, filters: FilterCondition[], enrichmentCols: string[]) {
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
      const enrichmentData = Array.isArray(enrichmentList) && enrichmentList.length > 0
        ? enrichmentList[0]
        : (enrichmentList && !Array.isArray(enrichmentList) ? enrichmentList : {});

      const { enrichments, ...contactData } = item;
      const finalStatus = enrichmentData.status || 'new';

      return {
        ...contactData,
        ...enrichmentData,
        status: finalStatus,
        enrichment_id: enrichmentData.id || null
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
    filters: FilterCondition[] = []
  ): Promise<{ data: MergedContact[], count: number }> {
    if (!this.client) return { data: [], count: 0 };

    const from = (page - 1) * pageSize;
    const to = from + pageSize - 1;
    const enrichmentCols = ['status', 'classification', 'confidence', 'cost', 'processed_at'];

    const selectStr = this.getJoinConfig(filters, enrichmentCols, enrichedOnly);

    let query = this.client
      .from('contacts')
      .select(selectStr, { count: 'exact' });

    query = this.applyFilters(query, filters, enrichmentCols);

    const { data, error, count } = await query
      .order('id', { ascending: true })
      .range(from, to);

    if (error) throw error;

    return {
      data: this.flattenData(data),
      count: count || 0
    };
  }

  async getAllFilteredContacts(filters: FilterCondition[]): Promise<MergedContact[]> {
    if (!this.client) return [];

    const enrichmentCols = ['status', 'classification', 'confidence', 'cost', 'processed_at'];
    const selectStr = this.getJoinConfig(filters, enrichmentCols, false);

    let query = this.client
      .from('contacts')
      .select(selectStr);

    query = this.applyFilters(query, filters, enrichmentCols);

    const { data, error } = await query.order('id', { ascending: true });

    if (error) throw error;

    return this.flattenData(data);
  }

  async enqueueContacts(contactIds: string[]) {
    if (!this.client) return;

    const payloads = contactIds.map(id => ({
      contact_id: id,
      status: 'pending',
      processed_at: null,
      error_message: null
    }));

    const { error } = await this.client
      .from('enrichments')
      .upsert(payloads, { onConflict: 'contact_id' });

    if (error) throw error;
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
