
export interface Contact {
  id: number;
  contact_id: string; // uuid
  lead_list_name: string;
  first_name: string;
  last_name: string;
  email: string;
  company_website: string;
  company_name: string;
  industry: string;
  linkedin_url: string;
  title: string;
  created_at?: string;
  updated_at?: string;
}

export interface Enrichment {
  id: number;
  contact_id: string; // uuid
  page_html: string;
  classification_json: any;
  classification: string;
  confidence: number;
  reasoning: string;
  input_tokens: number;
  output_tokens: number;
  cost: number;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  attempts: number;
  processed_at?: string;
  error_message?: string;
  created_at?: string;
  updated_at?: string;
  started_at?: string;
}

export type MergedContact = Contact & Partial<Enrichment> & {
  enrichment_id?: number;
  processing_stage?: 'scraping' | 'classifying' | 'syncing' | 'idle';
};

export enum AppTab {
  MANAGER = 'contacts',
  ENRICHMENT = 'enrichment',
  IMPORT = 'import',
  PROXIES = 'proxies'
}

export interface BatchStats {
  total: number;
  completed: number;
  failed: number;
  isProcessing: boolean;
}

export type FilterOperator = 'equals' | 'contains' | 'starts_with' | 'greater_than' | 'less_than' | 'in' | 'not_in';

export interface FilterCondition {
  id: string;
  column: string;
  operator: FilterOperator;
  value: any;
}
