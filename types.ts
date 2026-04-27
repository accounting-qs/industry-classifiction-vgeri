
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

// v2.2: Layer-1 primary identity + Layer-2 functional specialization.
// Layer-3 sector_focus and Layer-4 campaign bucket are computed elsewhere.
export interface PrimaryIdentity {
  name: string;
  description: string;
  identity_type: string;
  operator_required: boolean;
}

export interface BucketProposal {
  functional_specialization: string;
  primary_identity: string;
  description: string;
  identity_type?: string;
  operator_required?: boolean;
  priority_rank?: number;
  include?: string[];
  exclude?: string[];
  example_strings?: string[];
  strong_identity_signals?: string[];
  weak_sector_signals?: string[];
  disqualifying_signals?: string[];
  estimated_usage_label?: string;
  rough_volume_estimate?: string;
  library_match_id?: string | null;
}

export interface TaxonomyDoc {
  observed_patterns?: string[];
  sector_focus_vocabulary?: string[];
  primary_identities?: PrimaryIdentity[];
  buckets: BucketProposal[];
}

export interface BucketingRun {
  id: string;
  name: string;
  list_names: string[];
  min_volume: number;
  bucket_budget?: number;
  status: BucketingRunStatus;
  taxonomy_model?: string | null;
  taxonomy_proposal?: TaxonomyDoc | null;
  taxonomy_final?: TaxonomyDoc | null;
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
  bucket_name: string;                         // legacy mirror of functional_specialization
  primary_identity: string | null;             // v2.3
  functional_specialization: string | null;    // v2.3
  description: string | null;
  direct_ancestor: string | null;              // legacy mirror of primary_identity
  root_category: string | null;                // legacy
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
  // 'deterministic'      — fanout copied a map row that came from any source below
  // 'library_match'      — embedding match against a library bucket (Phase 1a)
  // 'preview_embedding'  — Phase 1a preview seed (replaced on assignment)
  // 'embedding'          — Phase 1b sector-aware embedding match
  // 'llm_phase1b'        — Phase 1b routing LLM
  // 'catchall'           — bucketing_catchall_other RPC (no map row, contact had no industry)
  // legacy: 'llm_phase2', 'llm_phase1', 'manual', 'other'
  source: 'deterministic' | 'library_match' | 'preview_embedding' | 'embedding'
        | 'llm_phase1b' | 'catchall' | 'llm_phase2' | 'llm_phase1' | 'manual' | 'other';
  confidence: number | null;
  assigned_at: string;
  bucket_leaf?: string | null;
  bucket_ancestor?: string | null;
  bucket_root?: string | null;
  primary_identity?: string | null;
  functional_specialization?: string | null;
  sector_focus?: string | null;
  is_generic?: boolean | null;
  is_disqualified?: boolean | null;
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
