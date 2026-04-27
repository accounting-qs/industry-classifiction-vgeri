
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
  PROXIES = 'proxies',
  BUCKETING = 'bucketing',
  CONNECTORS = 'connectors'
}

export type BucketingRunStatus =
  | 'taxonomy_pending'
  | 'taxonomy_ready'
  | 'assigning'
  | 'completed'
  | 'failed';

// v2: leaf bucket from Phase 1a, with ancestor chain.
export interface BucketProposal {
  bucket_name: string;
  description: string;
  direct_ancestor: string;
  root_category: string;
  include?: string[];
  exclude?: string[];
  example_strings?: string[];
  estimated_usage_label?: string;
  rough_volume_estimate?: string;
  library_match_id?: string | null;
}

export interface BucketingRun {
  id: string;
  name: string;
  list_names: string[];
  min_volume: number;
  status: BucketingRunStatus;
  taxonomy_model?: string | null;
  taxonomy_proposal?: { observed_patterns?: string[]; buckets: BucketProposal[] } | null;
  taxonomy_final?: { observed_patterns?: string[]; buckets: BucketProposal[] } | null;
  preferred_library_ids?: string[] | null;
  total_contacts?: number | null;
  assigned_contacts?: number | null;
  cost_usd?: number | null;
  error_message?: string | null;
  created_at: string;
  taxonomy_completed_at?: string | null;
  assignment_completed_at?: string | null;
}

export interface LibraryBucket {
  id: string;
  bucket_name: string;
  description: string | null;
  direct_ancestor: string | null;
  root_category: string | null;
  include_terms: string[];
  exclude_terms: string[];
  example_strings: string[];
  notes: string | null;
  times_used: number;
  last_used_at: string | null;
  archived: boolean;
  created_at: string;
  updated_at: string;
}

export interface BucketAssignmentRow {
  bucketing_run_id: string;
  contact_id: string;
  bucket_name: string;
  source: 'deterministic' | 'embedding' | 'llm_phase2' | 'other';
  confidence: number | null;
  assigned_at: string;
}

export interface BucketCount {
  bucket_name: string;
  contact_count: number;
}

export interface BucketAssignmentCount extends BucketCount {
  other_sources?: Record<string, number>;
}

export interface BatchStats {
  total: number;
  completed: number;
  failed: number;
  isProcessing: boolean;
  queueingPhase?: boolean;
  queued?: number;
  inQueue?: number;
}

export type FilterOperator = 'equals' | 'contains' | 'starts_with' | 'greater_than' | 'less_than' | 'in' | 'not_in';

export interface FilterCondition {
  id: string;
  column: string;
  operator: FilterOperator;
  value: any;
}
