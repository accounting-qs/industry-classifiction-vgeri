/**
 * Bucketing UI v5 — 3-layer model (identity → sub-identity → sector) → campaign bucket.
 *
 * Five views:
 *   - Index    : past runs + library shortcut
 *   - Setup    : pick lists, name, min_volume, bucket_budget, optional library
 *   - Review   : Phase 1a proposal — observed patterns + sub-identities grouped
 *                under primary identities, keep/drop/rename/add, threshold preview
 *   - Results  : Phase 1b assignments rolled up to campaign buckets, save-to-library
 *   - Library  : CRUD for reusable sub-identities across runs
 */

import React, { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import {
  Layers, Loader2, AlertCircle, ArrowLeft, Plus, X, Trash2, Download,
  Play, BookMarked, CheckCircle2, Edit3, Archive, Upload, Square, RotateCcw,
  ChevronDown, ChevronRight
} from 'lucide-react';
import Papa from 'papaparse';
import type { BucketingRun, BucketProposal, LibraryBucket } from './types';

type BucketingView = 'index' | 'setup' | 'detail' | 'library' | 'taxonomy';

const RESERVED_GENERAL = 'General';
// Recognize legacy names too, in case a run was created before v2.3.
const RESERVED_NAMES = new Set(['general', 'generic', 'disqualified', 'other']);

// Hover tooltips for the 3 taxonomy column headers on the AI-proposed
// additions panel. Plain strings — rendered via the browser's native
// title= tooltip on the column label + ⓘ icon.
const TAXONOMY_LAYER_HELP = {
  identities:
    'IDENTITY (Layer 1) — what kind of company this IS at its core. The top-level business model. Examples: Agency, Consulting & Advisory, Software & SaaS, Manufacturing & Industrial, Real Estate, Healthcare Operator, Financial Services. Aim for ~10–15 in a typical run.',
  sub_identities:
    'SUB-IDENTITY (Layer 2) — the specific functional sub-type within the identity. Narrows what kind of {identity} the company is. Examples: under Agency → "SEO Agency" / "Performance Marketing Agency"; under Software & SaaS → "FinTech SaaS" / "Vertical SaaS"; under Financial Services → "Private Equity Firm" / "Wealth Management". ~3–8 per identity.',
  sectors:
    'SECTOR (Layer 3) — the vertical the company SERVES, if explicitly stated. Independent of identity — a "Marketing Agency for Healthcare" has identity=Agency + sector=Healthcare (not identity=Healthcare). Often blank. Examples: Healthcare, Real Estate, Government, Energy & Utilities, Financial Services.'
} as const;

export function BucketingTab({ view = 'index', importLists }: {
  view?: BucketingView;
  importLists: { id: string; name: string; contact_count: number; created_at: string; enriched_count?: number; bucketed?: boolean; bucketing_run_count?: number; manually_bucketed?: boolean }[]
}) {
  const navigate = useNavigate();
  const params = useParams<{ runId?: string }>();
  const activeRunId = params.runId ?? null;
  const setView = useCallback((next: BucketingView) => {
    switch (next) {
      case 'index': navigate('/bucketing'); break;
      case 'setup': navigate('/bucketing/setup'); break;
      case 'library': navigate('/bucketing/library'); break;
      case 'taxonomy': navigate('/bucketing/taxonomy'); break;
      case 'detail':
        if (activeRunId) navigate(`/bucketing/runs/${activeRunId}`);
        break;
    }
  }, [navigate, activeRunId]);
  const [runs, setRuns] = useState<BucketingRun[]>([]);
  const [library, setLibrary] = useState<LibraryBucket[]>([]);
  const [activeRun, setActiveRun] = useState<BucketingRun | null>(null);
  const [bucketCounts, setBucketCounts] = useState<any[]>([]);
  const [sectorMix, setSectorMix] = useState<any[]>([]);
  const [generalBreakdown, setGeneralBreakdown] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refreshRuns = useCallback(async () => {
    try {
      const res = await fetch('/api/bucketing/runs');
      const data = await res.json();
      if (Array.isArray(data.runs)) setRuns(data.runs);
    } catch (e: any) {
      setError(e.message);
    }
  }, []);

  const refreshLibrary = useCallback(async () => {
    try {
      const res = await fetch('/api/bucketing/library');
      const data = await res.json();
      if (Array.isArray(data.buckets)) setLibrary(data.buckets);
    } catch (e: any) {
      setError(e.message);
    }
  }, []);

  const fetchActive = useCallback(async () => {
    if (!activeRunId) return;
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(activeRunId)}`);
      const data = await res.json();
      if (data.run) {
        setActiveRun(data.run);
        setBucketCounts(Array.isArray(data.bucket_counts) ? data.bucket_counts : []);
        setSectorMix(Array.isArray(data.sector_mix) ? data.sector_mix : []);
        setGeneralBreakdown(Array.isArray(data.general_breakdown) ? data.general_breakdown : []);
      }
    } catch (e: any) {
      setError(e.message);
    }
  }, [activeRunId]);

  useEffect(() => { refreshRuns(); refreshLibrary(); }, [refreshRuns, refreshLibrary]);

  useEffect(() => {
    if (!activeRunId) return;
    fetchActive();
    if (!activeRun) return;
    // Poll while in-flight or cancelled (cancelled state needs to react
    // to a Resume click). Stop polling once we land on a terminal stable
    // state the user is reviewing.
    if (activeRun.status === 'completed' || activeRun.status === 'failed' || activeRun.status === 'taxonomy_ready') return;
    const t = setInterval(fetchActive, 1500);
    return () => clearInterval(t);
  }, [activeRunId, activeRun?.status, fetchActive]);

  useEffect(() => {
    if (view !== 'index') return;
    const hasInflight = runs.some(r => r.status === 'taxonomy_pending' || r.status === 'assigning');
    if (!hasInflight) return;
    const t = setInterval(refreshRuns, 4000);
    return () => clearInterval(t);
  }, [view, runs, refreshRuns]);

  const openRun = (id: string) => { navigate(`/bucketing/runs/${id}`); };

  const startNew = async (payload: { name: string; list_names: string[]; apply_identity_dq_cascade: boolean; phase1a_model: string }) => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/bucketing/determine', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
      await refreshRuns();
      openRun(data.id);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const deleteRun = async (id: string) => {
    if (!confirm('Delete this bucketing run? Assignments will be removed.')) return;
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(id)}`, { method: 'DELETE' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
      if (activeRunId === id) { setActiveRun(null); navigate('/bucketing'); }
      await refreshRuns();
    } catch (e: any) {
      setError(e.message);
    }
  };

  // Per-row CSV export state. Lives at the parent (App tab) level so a
  // user can navigate away and back without losing in-flight job state.
  // Each entry: 'pending' | 'running' (still generating) → 'ready'
  // (auto-downloads) → cleared. 'failed' surfaces the error inline.
  type RunCsvJobState = {
    jobId: string;
    status: 'pending' | 'running' | 'ready' | 'failed';
    progressRows: number;
    totalRows: number | null;
    downloadUrl: string | null;
    error: string | null;
  };
  const [runCsvJobs, setRunCsvJobs] = useState<Record<string, RunCsvJobState>>({});
  const runCsvPollers = useRef<Record<string, number>>({});

  // Stop a poller cleanly (used on completion + on unmount).
  const stopRunCsvPoller = useCallback((runId: string) => {
    const t = runCsvPollers.current[runId];
    if (t) { window.clearInterval(t); delete runCsvPollers.current[runId]; }
  }, []);

  useEffect(() => () => {
    // Cleanup all pollers on unmount.
    for (const t of Object.values(runCsvPollers.current)) window.clearInterval(t as number);
    runCsvPollers.current = {};
  }, []);

  const exportRunCsv = useCallback(async (runId: string) => {
    setError(null);
    setRunCsvJobs(prev => ({
      ...prev,
      [runId]: { jobId: '', status: 'pending', progressRows: 0, totalRows: null, downloadUrl: null, error: null }
    }));
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(runId)}/csv-jobs`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
      const job = data.job;
      setRunCsvJobs(prev => ({
        ...prev,
        [runId]: { jobId: job.id, status: job.status, progressRows: 0, totalRows: null, downloadUrl: null, error: null }
      }));
      // Poll every 2 s until ready / failed. Auto-trigger download
      // when ready by setting window.location.href to the (server-
      // relative) download endpoint.
      stopRunCsvPoller(runId);
      runCsvPollers.current[runId] = window.setInterval(async () => {
        try {
          const r = await fetch(`/api/bucketing/csv-jobs/${job.id}`);
          const d = await r.json();
          if (!r.ok || !d.job) return;
          const j = d.job;
          setRunCsvJobs(prev => ({
            ...prev,
            [runId]: {
              jobId: j.id,
              status: j.status,
              progressRows: Number(j.progress_rows || 0),
              totalRows: j.total_rows ? Number(j.total_rows) : null,
              downloadUrl: j.download_url || null,
              error: j.error_message || null
            }
          }));
          if (j.status === 'ready' && j.download_url) {
            stopRunCsvPoller(runId);
            // Trigger the browser download. Using a hidden <a> so it
            // doesn't navigate away from the runs list.
            const a = document.createElement('a');
            a.href = j.download_url;
            a.download = '';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
          } else if (j.status === 'failed') {
            stopRunCsvPoller(runId);
          }
        } catch { /* keep polling */ }
      }, 2000);
    } catch (e: any) {
      setError(e.message);
      setRunCsvJobs(prev => {
        const next = { ...prev };
        delete next[runId];
        return next;
      });
    }
  }, [stopRunCsvPoller]);

  return (
    <div className="flex-1 overflow-y-auto custom-scrollbar p-6 bg-[#1c1c1c] text-[#ededed]">
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-lg font-bold text-white flex items-center gap-2">
              <Layers className="w-5 h-5 text-[#3ecf8e]" /> Bucketing
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              Phase 1a discovers identities + sub-identities. Phase 1b matches every contact (identity + sub-identity + sector). Volume rollup combines into campaign buckets within your bucket budget.
            </p>
          </div>
          <div className="flex gap-2">
            {view !== 'index' && (
              <button
                onClick={() => { setActiveRun(null); navigate('/bucketing'); refreshRuns(); }}
                className="px-3 py-1.5 rounded-md text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e]"
              >
                <ArrowLeft className="w-3 h-3 inline mr-1" /> All runs
              </button>
            )}
            {view === 'index' && (
              <>
                <button
                  onClick={() => { setView('taxonomy'); setError(null); }}
                  className="px-3 py-1.5 rounded-md text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e] flex items-center gap-1"
                >
                  <Layers className="w-3 h-3" /> Taxonomy
                </button>
                <button
                  onClick={() => { setView('library'); refreshLibrary(); setError(null); }}
                  className="px-3 py-1.5 rounded-md text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e] flex items-center gap-1"
                >
                  <BookMarked className="w-3 h-3" /> Library ({library.length})
                </button>
                <button
                  onClick={() => { setView('setup'); setError(null); }}
                  className="px-3 py-1.5 rounded-md text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] flex items-center gap-1"
                >
                  <Plus className="w-3 h-3" /> New Bucketing Run
                </button>
              </>
            )}
          </div>
        </div>

        {error && (
          <div className="mb-4 px-4 py-2.5 bg-red-500/10 border border-red-500/30 rounded text-red-400 text-xs flex items-start gap-2">
            <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
            <span className="flex-1">{error}</span>
            <button onClick={() => setError(null)} className="text-red-400 hover:text-white"><X className="w-3 h-3" /></button>
          </div>
        )}

        {view === 'index' && (
          <BucketingIndex
            runs={runs}
            onOpen={openRun}
            onDelete={deleteRun}
            onExportCsv={exportRunCsv}
            csvJobs={runCsvJobs}
          />
        )}

        {view === 'setup' && (
          <BucketingSetup
            importLists={importLists}
            onCancel={() => setView('index')}
            onStart={startNew}
            loading={loading}
          />
        )}

        {view === 'library' && (
          <BucketingLibrary library={library} onRefresh={refreshLibrary} onError={setError} />
        )}

        {view === 'taxonomy' && (
          <TaxonomyLibrary onError={setError} />
        )}

        {view === 'detail' && activeRun && (
          <BucketingDetail
            run={activeRun}
            library={library}
            bucketCounts={bucketCounts}
            sectorMix={sectorMix}
            generalBreakdown={generalBreakdown}
            onRefresh={fetchActive}
            onError={setError}
            onLibrarySaved={refreshLibrary}
          />
        )}
      </div>
    </div>
  );
}

// ───── INDEX VIEW ──────────────────────────────────────────────────

function BucketingIndex({ runs, onOpen, onDelete, onExportCsv, csvJobs }: {
  runs: BucketingRun[];
  onOpen: (id: string) => void;
  onDelete: (id: string) => void;
  // Per-row CSV export trigger + live job state. Polled by the parent
  // so navigation away/back preserves the in-flight job.
  onExportCsv: (runId: string) => void;
  csvJobs: Record<string, {
    jobId: string;
    status: 'pending' | 'running' | 'ready' | 'failed';
    progressRows: number;
    totalRows: number | null;
    downloadUrl: string | null;
    error: string | null;
  }>;
}) {
  // Statuses that have at least Phase 1a output to export. taxonomy_pending
  // (mid-tagging) and failed have nothing useful yet.
  const exportable = (status: string) => status === 'taxonomy_ready'
    || status === 'assigning'
    || status === 'completed'
    || status === 'cancelled';
  if (runs.length === 0) {
    return (
      <div className="border border-[#2e2e2e] rounded-xl p-12 text-center bg-[#0e0e0e]">
        <Layers className="w-10 h-10 text-gray-700 mx-auto mb-3" />
        <p className="text-sm font-bold text-gray-400">No bucketing runs yet</p>
        <p className="text-[11px] text-gray-600 mt-1">Click "New Bucketing Run" to create one.</p>
      </div>
    );
  }
  return (
    <div className="border border-[#2e2e2e] rounded-xl overflow-hidden bg-[#0e0e0e]">
      <table className="w-full text-[11px]">
        <thead className="bg-[#0e0e0e]">
          <tr className="border-b border-[#2e2e2e] text-[9px] font-bold text-gray-500 uppercase tracking-wider">
            <th className="px-5 py-3 text-left">Name</th>
            <th className="px-5 py-3 text-left">Lists</th>
            <th className="px-5 py-3 text-left">Status</th>
            <th className="px-5 py-3 text-right">Contacts</th>
            <th className="px-5 py-3 text-right">Cost</th>
            <th className="px-5 py-3 text-right">Created</th>
            <th className="px-5 py-3 text-right"></th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[#2e2e2e]">
          {runs.map(r => (
            <tr key={r.id} className="hover:bg-white/[0.02] cursor-pointer" onClick={() => onOpen(r.id)}>
              <td className="px-5 py-3 font-medium text-white">{r.name}</td>
              <td className="px-5 py-3 text-gray-400">{r.list_names.slice(0, 2).join(', ')}{r.list_names.length > 2 ? ` +${r.list_names.length - 2}` : ''}</td>
              <td className="px-5 py-3"><BucketingStatusBadge status={r.status} /></td>
              <td className="px-5 py-3 text-right text-gray-300 font-mono">
                {r.assigned_contacts ? r.assigned_contacts.toLocaleString() : (r.total_contacts?.toLocaleString() || '—')}
              </td>
              <td className="px-5 py-3 text-right text-gray-400 font-mono">${(Number(r.cost_usd) || 0).toFixed(3)}</td>
              <td className="px-5 py-3 text-right text-gray-500">{new Date(r.created_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</td>
              <td className="px-5 py-3 text-right">
                <div className="flex items-center justify-end gap-1">
                  {exportable(r.status) && (() => {
                    const job = csvJobs[r.id];
                    const inFlight = job && (job.status === 'pending' || job.status === 'running');
                    const ready = job?.status === 'ready';
                    const failed = job?.status === 'failed';
                    const pct = inFlight && job?.totalRows
                      ? Math.min(100, Math.round((job.progressRows / job.totalRows) * 100))
                      : null;
                    const label = inFlight
                      ? (pct !== null ? `${pct}%` : '…')
                      : ready ? 'Re-download'
                      : failed ? 'Retry CSV'
                      : 'Download CSV';
                    const title = inFlight
                      ? `Generating: ${job.progressRows.toLocaleString()}${job.totalRows ? ` / ${job.totalRows.toLocaleString()}` : ''} rows`
                      : ready ? 'Click to download again (file expires after 24 h)'
                      : failed ? `Last attempt failed: ${job?.error || 'unknown error'} — click to retry`
                      : 'Stream the full per-contact CSV (email, name, website, classification, taxonomy, bucket if assigned). Generates async + auto-downloads when ready.';
                    return (
                      <button
                        onClick={e => {
                          e.stopPropagation();
                          if (ready && job?.downloadUrl) {
                            const a = document.createElement('a');
                            a.href = job.downloadUrl;
                            a.download = '';
                            document.body.appendChild(a);
                            a.click();
                            document.body.removeChild(a);
                          } else {
                            onExportCsv(r.id);
                          }
                        }}
                        disabled={!!inFlight}
                        className={`px-2 py-1 rounded-md text-[10px] font-bold border flex items-center gap-1 ${ready
                          ? 'bg-[#3ecf8e]/15 text-[#3ecf8e] border-[#3ecf8e]/40 hover:bg-[#3ecf8e]/25'
                          : failed
                            ? 'bg-amber-500/15 text-amber-300 border-amber-500/40 hover:bg-amber-500/25'
                            : 'bg-[#1c1c1c] text-gray-300 border-[#2e2e2e] hover:text-white hover:border-gray-500'} disabled:opacity-70`}
                        title={title}
                      >
                        {inFlight ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                        {label}
                      </button>
                    );
                  })()}
                  <button
                    onClick={e => { e.stopPropagation(); onDelete(r.id); }}
                    className="p-1.5 rounded-md bg-[#1c1c1c] border border-[#2e2e2e] text-gray-500 hover:text-red-400 hover:border-red-500/40"
                    title="Delete run"
                  >
                    <Trash2 className="w-3 h-3" />
                  </button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function BucketingStatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    'taxonomy_pending': 'bg-blue-500/15 text-blue-400 border-blue-500/30',
    'taxonomy_ready':   'bg-amber-500/15 text-amber-400 border-amber-500/30',
    'assigning':        'bg-blue-500/15 text-blue-400 border-blue-500/30 animate-pulse',
    'completed':        'bg-[#3ecf8e]/15 text-[#3ecf8e] border-[#3ecf8e]/40',
    'failed':           'bg-red-500/15 text-red-400 border-red-500/30',
    'cancelled':        'bg-gray-500/15 text-gray-400 border-gray-500/30',
  };
  const labels: Record<string, string> = {
    'taxonomy_pending': 'Discovering…',
    'taxonomy_ready':   'Awaiting review',
    'assigning':        'Assigning…',
    'completed':        'Completed',
    'failed':           'Failed',
    'cancelled':        'Cancelled',
  };
  return (
    <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${styles[status] || 'bg-gray-500/15 text-gray-400 border-gray-500/30'}`}>
      {labels[status] || status}
    </span>
  );
}

// ───── SETUP VIEW ──────────────────────────────────────────────────

// Phase 1a model options surfaced on the Setup screen. The cost field is
// "approx for 100k contacts" — based on the typical 32k distinct
// classification strings we've measured (dedup ratio ~1.45) and the
// pricing table in services/bucketingService.ts. Accuracy numbers are
// MEASURED from the 30-case battery in /api/bucketing/debug/test-tag
// (12 clear cases, 10 identity-vs-sector traps, 4 vague single-word
// inputs, 4 disqualification edge cases). Surprising finding: Haiku 4.5
// beat the bigger models because it follows the hard-rule prompt
// strictly without over-committing on vague inputs (Sonnet/Opus tend to
// propose sub-identities even when the prompt says "return null").
const PHASE1A_MODEL_OPTIONS = [
  { id: 'gpt-4.1-mini',     label: 'gpt-4.1-mini',                       approxCost100k: '~$15–25',   recommended: true,  note: 'Default. 28/30 identity, 26/30 sub-identity on the live-taxonomy goldens. Cheapest option; equivalent identity accuracy to Haiku at ~1/3 the cost.' },
  { id: 'claude-haiku-4-5', label: 'Claude Haiku 4.5',                   approxCost100k: '~$40–70',   recommended: false, note: '27/30 identity, 26/30 sub-identity — equivalent to mini on identity, slightly better on sector. Worth the cost if sector accuracy matters a lot.' },
  { id: 'gpt-4.1',          label: 'gpt-4.1',                            approxCost100k: '~$60–90',   recommended: false, note: '29/30 identity, 16/16 sub-identity, 4/4 DQ. Equivalent to mini on accuracy, ~5× the cost — no reason to pick this over mini/Haiku.' },
  { id: 'claude-sonnet-4-6', label: 'Claude Sonnet 4.6',                 approxCost100k: '~$100–150', recommended: false, note: '29/30 identity, 14/16 sub-identity, 4/4 DQ. Over-commits sub-identities on vague inputs; the extra reasoning works against the strict-prompt design.' },
  { id: 'claude-opus-4-7',  label: 'Claude Opus 4.7',                    approxCost100k: '~$450–600', recommended: false, note: '29/30 identity, 14/16 sub-identity, 4/4 DQ. Same over-commit issue as Sonnet. Overkill for this task; only worth it on small, high-stakes lists.' },
] as const;
type Phase1aModel = typeof PHASE1A_MODEL_OPTIONS[number]['id'];

function BucketingSetup({ importLists, onCancel, onStart, loading }: {
  importLists: { name: string; contact_count: number; enriched_count?: number; bucketed?: boolean; bucketing_run_count?: number; manually_bucketed?: boolean }[];
  onCancel: () => void;
  onStart: (p: { name: string; list_names: string[]; apply_identity_dq_cascade: boolean; phase1a_model: Phase1aModel }) => void;
  loading: boolean;
}) {
  const [name, setName] = useState('');
  const [selectedLists, setSelectedLists] = useState<Set<string>>(new Set());
  // Default OFF — trust the tagger's per-row is_disqualified decision instead of
  // auto-DQ'ing every contact whose identity is library-flagged [DQ].
  const [applyIdentityDqCascade, setApplyIdentityDqCascade] = useState(false);
  // Default to the option flagged `recommended: true` — falls back to
  // gpt-4.1-mini if no recommendation is set so the picker is never empty.
  const defaultModel: Phase1aModel = (PHASE1A_MODEL_OPTIONS.find(o => o.recommended)?.id || 'gpt-4.1-mini') as Phase1aModel;
  const [phase1aModel, setPhase1aModel] = useState<Phase1aModel>(defaultModel);

  const toggleList = (n: string) => {
    const s = new Set(selectedLists);
    s.has(n) ? s.delete(n) : s.add(n);
    setSelectedLists(s);
  };

  const totalSelected = importLists.filter(l => selectedLists.has(l.name)).reduce((s, l) => s + (l.enriched_count || 0), 0);
  const totalRaw = importLists.filter(l => selectedLists.has(l.name)).reduce((s, l) => s + (l.contact_count || 0), 0);
  const canStart = !!name.trim() && selectedLists.size > 0 && !loading;

  return (
    <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-6 space-y-5">
      <div>
        <label className="block text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2">Run Name</label>
        <input
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder="e.g. Q2 outreach segmentation"
          className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-sm text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]"
        />
      </div>

      <div>
        <label className="block text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2">
          Select lists ({selectedLists.size} selected · {totalSelected.toLocaleString()} enriched / {totalRaw.toLocaleString()} total)
        </label>
        {importLists.length === 0 ? (
          <div className="text-xs text-gray-500 italic px-3 py-2 border border-[#2e2e2e] rounded">No lists available — import a CSV first.</div>
        ) : (
          <div className="border border-[#2e2e2e] rounded max-h-64 overflow-y-auto custom-scrollbar">
            {importLists.map(l => {
              const isSel = selectedLists.has(l.name);
              // Show a "Bucketed" badge when this list has appeared in at
              // least one prior bucketing run (or been manually marked) so
              // the user can spot already-processed lists at a glance. The
              // badge is informational only — re-bucketing is still allowed.
              const runCount = l.bucketing_run_count || 0;
              const bucketed = !!l.bucketed;
              const badgeTitle = runCount > 0
                ? `Previously bucketed ${runCount} run${runCount === 1 ? '' : 's'}`
                : (l.manually_bucketed ? 'Manually marked as bucketed' : '');
              return (
                <button
                  key={l.name}
                  onClick={() => toggleList(l.name)}
                  className={`w-full flex items-center justify-between px-3 py-2 text-left text-xs border-b border-[#2e2e2e] last:border-b-0 transition-colors ${isSel ? 'bg-[#3ecf8e]/10 text-[#3ecf8e]' : 'text-gray-300 hover:bg-white/[0.02]'}`}
                >
                  <span className="flex items-center gap-2 min-w-0">
                    <input type="checkbox" checked={isSel} onChange={() => {}} className="w-3 h-3 flex-shrink-0" />
                    <span className="font-medium truncate">{l.name}</span>
                    {bucketed && (
                      <span
                        title={badgeTitle}
                        className="flex-shrink-0 px-1.5 py-[1px] rounded text-[9px] font-bold uppercase tracking-wider bg-[#3ecf8e]/15 text-[#3ecf8e] border border-[#3ecf8e]/30"
                      >
                        {l.manually_bucketed && runCount === 0 ? 'Marked' : `Bucketed${runCount > 1 ? ` ×${runCount}` : ''}`}
                      </span>
                    )}
                  </span>
                  <span className="font-mono text-[10px] text-gray-500 flex-shrink-0 ml-2">
                    {(l.enriched_count || 0).toLocaleString()} enriched / {l.contact_count.toLocaleString()}
                  </span>
                </button>
              );
            })}
          </div>
        )}
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-3 text-[11px] text-gray-400">
        <span className="text-gray-300 font-bold">Bucket sizing &amp; library reuse</span> are set on the next screen, after Phase 1a proposes a taxonomy — that way you size against the actual sub-identities the tagger found.
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-3">
        <div className="block text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2">Phase 1a model (taxonomy tagging)</div>
        <div className="space-y-2">
          {PHASE1A_MODEL_OPTIONS.map(opt => {
            const isSel = phase1aModel === opt.id;
            return (
              <label
                key={opt.id}
                className={`flex items-start gap-2 cursor-pointer p-2 rounded border ${isSel ? 'border-[#3ecf8e] bg-[#3ecf8e]/5' : 'border-[#2e2e2e] hover:bg-white/[0.02]'}`}
              >
                <input
                  type="radio"
                  name="phase1a_model"
                  checked={isSel}
                  onChange={() => setPhase1aModel(opt.id)}
                  className="mt-0.5 accent-[#3ecf8e]"
                />
                <span className="flex-1 min-w-0">
                  <span className="flex items-baseline justify-between gap-2 flex-wrap">
                    <span className="flex items-center gap-1.5">
                      <span className={`text-[11px] font-bold ${isSel ? 'text-[#3ecf8e]' : 'text-gray-200'}`}>{opt.label}</span>
                      {opt.recommended && (
                        <span className="text-[9px] font-bold uppercase tracking-widest bg-[#3ecf8e]/20 text-[#3ecf8e] border border-[#3ecf8e]/40 px-1.5 py-0.5 rounded">
                          Recommended
                        </span>
                      )}
                    </span>
                    <span className="text-[10px] font-mono text-gray-500">{opt.approxCost100k} / 100k contacts</span>
                  </span>
                  <span className="block text-[10px] text-gray-500 italic mt-0.5">{opt.note}</span>
                </span>
              </label>
            );
          })}
        </div>
        <p className="text-[10px] text-gray-600 italic mt-2">Cost depends on the number of <em>distinct</em> enrichment.classification strings, not raw contact count. Phase 1b uses gpt-4.1-mini regardless and now mostly skips the LLM via JOIN-first lookup.</p>
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-3">
        <label className="flex items-start gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={applyIdentityDqCascade}
            onChange={e => setApplyIdentityDqCascade(e.target.checked)}
            className="w-3.5 h-3.5 mt-0.5 accent-[#3ecf8e]"
          />
          <span className="flex-1">
            <span className="block text-[11px] font-bold text-gray-200">
              Auto-disqualify contacts whose identity is library-flagged [DQ]
            </span>
            <span className="block text-[10px] text-gray-500 italic mt-0.5">
              Off (recommended): trust the tagger's per-row decision — even contacts tagged "Consumer & Retail" stay inviteable when they show a B2B angle. On: any contact tagged with a [DQ] identity routes straight to Disqualified, no exceptions.
            </span>
          </span>
        </label>
      </div>

      <div className="flex justify-end gap-2 pt-3 border-t border-[#2e2e2e]">
        <button onClick={onCancel} className="px-4 py-2 rounded text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e]">Cancel</button>
        <button
          onClick={() => onStart({
            name: name.trim(),
            list_names: Array.from(selectedLists),
            apply_identity_dq_cascade: applyIdentityDqCascade,
            phase1a_model: phase1aModel
          })}
          disabled={!canStart}
          className={`px-4 py-2 rounded text-xs font-bold flex items-center gap-1 ${canStart ? 'bg-[#3ecf8e] text-black hover:bg-[#2fb37a]' : 'bg-[#2e2e2e] text-gray-500 cursor-not-allowed'}`}
        >
          {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
          Start Bucket Discovery
        </button>
      </div>
    </div>
  );
}

// ───── DETAIL ROUTER ──────────────────────────────────────────────

// ───── CANCELLED PANEL ────────────────────────────────────────────
// Shown after the user clicks Stop. Resume button retriggers whichever
// phase was running (1a if no taxonomy_proposal yet, 1b otherwise).
function BucketingCancelledPanel({ run, onRefresh, onError }: {
  run: BucketingRun;
  onRefresh: () => void;
  onError: (msg: string | null) => void;
}) {
  const [resuming, setResuming] = useState(false);
  const resumePhase = run.taxonomy_proposal ? 'Phase 1b (matching)' : 'Phase 1a (discovery)';

  const onResume = async () => {
    setResuming(true);
    onError(null);
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/resume`, { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setResuming(false);
    }
  };

  return (
    <div className="border border-gray-500/30 rounded-xl bg-gray-500/5 p-6">
      <div className="flex items-center gap-3 mb-3">
        <Square className="w-5 h-5 text-gray-400" />
        <div className="flex-1">
          <p className="text-sm font-bold text-gray-300">Run cancelled</p>
          <p className="text-[11px] text-gray-500 mt-0.5">
            {run.error_message || 'The run was stopped by the user.'} Resume restarts {resumePhase} from the start.
          </p>
        </div>
      </div>
      <button
        onClick={onResume}
        disabled={resuming}
        className="px-4 py-2 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1"
      >
        {resuming ? <Loader2 className="w-3 h-3 animate-spin" /> : <RotateCcw className="w-3 h-3" />}
        Resume
      </button>
    </div>
  );
}

// ───── LIVE PROGRESS PANEL ────────────────────────────────────────
// Renders the current step, % bar, ETA, and an auto-scrolling tail of
// log lines. Polls /runs/:id/logs every ~1.5s for new entries.

function BucketingProgressPanel({ run, title, onError }: {
  run: BucketingRun;
  title: string;
  onError?: (msg: string | null) => void;
}) {
  const [logs, setLogs] = useState<{ id: number; timestamp: string; level: string; message: string }[]>([]);
  const [sinceId, setSinceId] = useState(0);
  const [stopping, setStopping] = useState(false);
  const logBoxRef = useRef<HTMLDivElement>(null);

  const onStop = async () => {
    if (!confirm('Stop this bucketing run? You can resume it from where it left off (current phase will restart).')) return;
    setStopping(true);
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/cancel`, { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
    } catch (e: any) {
      onError?.(e.message);
    } finally {
      setStopping(false);
    }
  };

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/logs?since=${sinceId}`);
        const data = await res.json();
        if (!cancelled && Array.isArray(data.logs) && data.logs.length > 0) {
          setLogs(prev => [...prev, ...data.logs].slice(-500));
          setSinceId(data.logs[data.logs.length - 1].id);
        }
      } catch { /* swallow polling errors */ }
    };
    tick();
    const t = setInterval(tick, 1500);
    return () => { cancelled = true; clearInterval(t); };
  }, [run.id, sinceId]);

  // Auto-scroll on new logs
  useEffect(() => {
    if (logBoxRef.current) logBoxRef.current.scrollTop = logBoxRef.current.scrollHeight;
  }, [logs]);

  const p = run.progress;
  const pct = (p && typeof p.pct === 'number') ? p.pct : null;
  const note = p?.note || 'Working…';
  const etaTxt = (p && typeof p.eta_seconds === 'number')
    ? formatDuration(p.eta_seconds)
    : null;
  const elapsedTxt = (p && typeof p.elapsed_seconds === 'number')
    ? formatDuration(p.elapsed_seconds)
    : null;

  // Pipeline step chain. We surface the three automated steps so the
  // user can see where in the whole bucketing run they are. Bucket
  // Assignment is a manual click that lives between Phase 1a and 1b,
  // but its progress flows through the same panel via step=bucket_assign.
  type PhaseState = 'done' | 'active' | 'pending';
  const phaseFromProgress = String(p?.phase || '');
  const stepFromProgress = String(p?.step || '');
  const isBucketAssign = stepFromProgress === 'bucket_assign';
  const isPhase1a = phaseFromProgress === 'phase1a' && !isBucketAssign;
  const isPhase1b = phaseFromProgress === 'phase1b';
  const status = String((run as any).status || '');
  const phase1aState: PhaseState =
    isPhase1a ? 'active'
    : (status === 'taxonomy_pending') ? 'active'
    : 'done';                                // any later status implies 1a finished
  const bucketAssignState: PhaseState =
    isBucketAssign ? 'active'
    : isPhase1b || status === 'completed' ? 'done'
    : 'pending';
  const phase1bState: PhaseState =
    isPhase1b ? 'active'
    : status === 'completed' ? 'done'
    : 'pending';
  const phaseChain: Array<{ label: string; state: PhaseState; sub?: string }> = [
    { label: 'Phase 1a Discovery', state: phase1aState, sub: 'tag every distinct industry' },
    { label: 'Bucket Assignment', state: bucketAssignState, sub: 'industry → campaign bucket' },
    { label: 'Phase 1b Routing', state: phase1bState, sub: 'per-contact assignment' }
  ];
  const stepNumber =
    phase1bState === 'active' ? 3
    : bucketAssignState === 'active' ? 2
    : 1;
  const etaLabel = phase1bState === 'active' ? 'TOTAL REMAINING'
    : phase1aState === 'active' ? 'PHASE 1a ETA'
    : 'STEP ETA';

  return (
    <div className="space-y-3">
      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-5">
        {/* Pipeline step chain — surfaces total progress across the
            automated steps so the ETA below is interpretable. */}
        <div className="flex items-center gap-2 mb-4 text-[10px] font-bold uppercase tracking-widest">
          {phaseChain.map((step, idx) => {
            const color =
              step.state === 'done' ? 'text-[#3ecf8e]'
              : step.state === 'active' ? 'text-white'
              : 'text-gray-600';
            const dotBg =
              step.state === 'done' ? 'bg-[#3ecf8e] text-black'
              : step.state === 'active' ? 'bg-[#3ecf8e]/20 text-[#3ecf8e] border border-[#3ecf8e]'
              : 'bg-[#1c1c1c] text-gray-500 border border-[#2e2e2e]';
            return (
              <div key={step.label} className="flex items-center gap-2 min-w-0">
                <span className={`flex items-center justify-center w-5 h-5 rounded-full text-[10px] font-bold ${dotBg}`}>
                  {step.state === 'done' ? <CheckCircle2 className="w-3 h-3" /> : (idx + 1)}
                </span>
                <div className="min-w-0">
                  <div className={`truncate ${color}`}>{step.label}</div>
                  <div className="text-gray-600 normal-case tracking-normal font-normal text-[9px] truncate">{step.sub}</div>
                </div>
                {idx < phaseChain.length - 1 && (
                  <div className={`h-px w-6 mx-1 ${step.state === 'done' ? 'bg-[#3ecf8e]/60' : 'bg-[#2e2e2e]'}`} />
                )}
              </div>
            );
          })}
        </div>

        <div className="flex items-center gap-3 mb-3">
          <Loader2 className="w-5 h-5 text-[#3ecf8e] animate-spin" />
          <div className="flex-1 min-w-0">
            <div className="text-sm font-bold text-white">{title} <span className="text-gray-500 font-normal text-[11px]">· step {stepNumber} of 3</span></div>
            <div className="text-[11px] text-gray-500 truncate">{note}</div>
          </div>
          <div className="text-right shrink-0">
            <div className="text-2xl font-bold font-mono text-[#3ecf8e]">
              {pct !== null ? `${pct}%` : '—'}
            </div>
            <div className="text-[10px] text-gray-500 uppercase tracking-widest">
              {etaTxt ? `${etaLabel} ${etaTxt}` : (elapsedTxt ? `${elapsedTxt} elapsed` : 'estimating…')}
            </div>
          </div>
          <button
            onClick={onStop}
            disabled={stopping}
            className="px-3 py-2 rounded text-xs font-bold bg-red-500/10 border border-red-500/40 text-red-400 hover:bg-red-500/20 disabled:opacity-50 flex items-center gap-1 shrink-0"
            title="Stop the run cleanly. You can resume afterwards."
          >
            {stopping ? <Loader2 className="w-3 h-3 animate-spin" /> : <Square className="w-3 h-3" />}
            Stop
          </button>
        </div>
        <div className="h-2 bg-[#1c1c1c] rounded-full overflow-hidden">
          <div
            className="h-full bg-[#3ecf8e] transition-all duration-300"
            style={{ width: pct !== null ? `${pct}%` : '0%' }}
          />
        </div>
        {p && (typeof p.current === 'number' || typeof p.total === 'number') && (
          <div className="text-[10px] font-mono text-gray-500 mt-1.5">
            step: <span className="text-gray-300">{p.step}</span>
            {typeof p.current === 'number' && typeof p.total === 'number'
              ? <> · {p.current.toLocaleString()} / {p.total.toLocaleString()}</>
              : null}
            {elapsedTxt && <> · elapsed {elapsedTxt}</>}
          </div>
        )}
        {/* Backgrounded-job hint. The run continues server-side regardless
            of whether this tab is open — the polling above just resumes
            when the user reopens the run. */}
        <div className="text-[10px] text-gray-600 mt-2 italic">
          Safe to close this tab or navigate away — the run continues on the server. Reopen this run from the index to resume live progress.
        </div>
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] overflow-hidden">
        <div className="px-4 py-2 border-b border-[#2e2e2e] text-[10px] font-bold text-gray-500 uppercase tracking-widest flex items-center justify-between">
          <span>Live log stream</span>
          <span className="text-gray-600 normal-case font-normal">{logs.length} lines</span>
        </div>
        <div
          ref={logBoxRef}
          className="h-72 overflow-y-auto custom-scrollbar font-mono text-[11px] leading-relaxed px-4 py-2 bg-black/30"
        >
          {logs.length === 0 ? (
            <div className="text-gray-600 italic">Waiting for first log line…</div>
          ) : logs.map(l => {
            const colour = l.level === 'error' ? 'text-red-400'
              : l.level === 'warn' ? 'text-amber-400'
              : l.level === 'phase' ? 'text-[#3ecf8e]'
              : 'text-gray-300';
            const time = new Date(l.timestamp).toLocaleTimeString('en-US', { hour12: false });
            return (
              <div key={l.id} className={colour}>
                <span className="text-gray-600">{time}</span> {l.message}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.max(0, seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function BucketingDetail({ run, library, bucketCounts, sectorMix, generalBreakdown, onRefresh, onError, onLibrarySaved }: {
  run: BucketingRun;
  library: LibraryBucket[];
  bucketCounts: any[];
  sectorMix: any[];
  generalBreakdown: any[];
  onRefresh: () => void;
  onError: (msg: string | null) => void;
  onLibrarySaved: () => void;
}) {
  if (run.status === 'taxonomy_pending') {
    return <BucketingProgressPanel run={run} title="Phase 1a — Discovering taxonomy" onError={onError} />;
  }
  if (run.status === 'cancelled') {
    return <BucketingCancelledPanel run={run} onRefresh={onRefresh} onError={onError} />;
  }
  if (run.status === 'failed') {
    return (
      <div className="border border-red-500/30 rounded-xl bg-red-500/5 p-6">
        <p className="text-sm font-bold text-red-400 flex items-center gap-2">
          <AlertCircle className="w-4 h-4" /> Run failed
        </p>
        <pre className="text-xs text-gray-400 mt-2 whitespace-pre-wrap font-sans">
          {run.error_message || 'Unknown error.'}
        </pre>
      </div>
    );
  }
  if (run.status === 'assigning') {
    return <BucketingProgressPanel run={run} title="Phase 1b — Matching contacts to buckets" onError={onError} />;
  }
  if (run.status === 'taxonomy_ready') {
    return <BucketingReview run={run} library={library} bucketCounts={bucketCounts} onRefresh={onRefresh} onError={onError} />;
  }
  return <BucketingResults run={run} bucketCounts={bucketCounts} sectorMix={sectorMix} generalBreakdown={generalBreakdown} onRefresh={onRefresh} onError={onError} onLibrarySaved={onLibrarySaved} />;
}

// ───── REVIEW VIEW ──────────────────────────────────────────────

function BucketingReview({ run, library, bucketCounts, onRefresh, onError }: {
  run: BucketingRun;
  library: LibraryBucket[];
  bucketCounts: any[];
  onRefresh: () => void;
  onError: (msg: string | null) => void;
}) {
  // Recalc triggers a re-pass against the now-current library: refreshes
  // is_new_* flags on every Phase 1a row + re-runs the LLM consolidation
  // passes so STILL-proposed-new entries can merge INTO newly-accepted
  // library entries. Auto-fires after each Accept-all / Apply-selected
  // batch in Phase1aProposedTagsPanel; also exposed as a manual button
  // next to the Discovered Sub-Identities header (covers the case where
  // the user edited the library outside this run).
  const [recalcing, setRecalcing] = useState(false);
  const [lastRecalc, setLastRecalc] = useState<{ at: Date; merges: number } | null>(null);

  // Collapse state for the two big "Discovered ..." panels — stored in
  // localStorage so the preference survives page reloads. Default open
  // (the panels are the main thing the user looks at on this screen).
  const [discoveredBucketsOpen, setDiscoveredBucketsOpen] = useState<boolean>(() => {
    try { return localStorage.getItem('bucketing.discoveredBucketsOpen') !== '0'; }
    catch { return true; }
  });
  const [discoveredSubIdentitiesOpen, setDiscoveredSubIdentitiesOpen] = useState<boolean>(() => {
    try { return localStorage.getItem('bucketing.discoveredSubIdentitiesOpen') !== '0'; }
    catch { return true; }
  });
  const toggleDiscoveredBuckets = () => setDiscoveredBucketsOpen(v => {
    try { localStorage.setItem('bucketing.discoveredBucketsOpen', v ? '0' : '1'); } catch {}
    return !v;
  });
  const toggleDiscoveredSubIdentities = () => setDiscoveredSubIdentitiesOpen(v => {
    try { localStorage.setItem('bucketing.discoveredSubIdentitiesOpen', v ? '0' : '1'); } catch {}
    return !v;
  });

  const triggerRecalc = useCallback(async () => {
    setRecalcing(true);
    onError(null);
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/recalculate`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Recalc failed (${res.status})`);
      const totalMerges = (data.identityMerges || 0) + (data.sub_identityMerges || 0) + (data.sectorMerges || 0);
      setLastRecalc({ at: new Date(), merges: totalMerges });
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setRecalcing(false);
    }
  }, [run.id, onRefresh, onError]);

  // Finalize: re-tag every still-orphan row (is_new_*=true) against the
  // library only — no new proposals. After this, the AI-Proposed panel
  // empties out and every contact is library-bound or routes to General.
  // Run BEFORE Apply & Assign once the user has decided which proposals
  // to keep.
  const [finalizing, setFinalizing] = useState(false);
  const [lastFinalize, setLastFinalize] = useState<{ at: Date; rerouted: number; nullified: number; failed: number } | null>(null);
  // Live progress polled while the finalize POST is in flight. The
  // service writes bucketing_runs.progress every 40 industries (debounced
  // to 700 ms server-side), so 1 s polling sees every update without
  // hammering the DB.
  const [finalizeProgress, setFinalizeProgress] = useState<{ current: number; total: number; note?: string } | null>(null);
  const finalizePollRef = useRef<number | null>(null);

  // Stop polling on unmount so we don't leak intervals if the user
  // navigates away mid-finalize.
  useEffect(() => () => {
    if (finalizePollRef.current) { window.clearInterval(finalizePollRef.current); finalizePollRef.current = null; }
  }, []);

  const triggerFinalize = useCallback(async () => {
    if (!confirm('Re-tag all remaining AI-proposed entries against the library only? Anything that doesn\'t fit a library entry will route to General.')) return;
    setFinalizing(true);
    setFinalizeProgress({ current: 0, total: 0 });
    onError(null);
    // Poll bucketing_runs.progress while the finalize call is in flight.
    if (finalizePollRef.current) window.clearInterval(finalizePollRef.current);
    finalizePollRef.current = window.setInterval(async () => {
      try {
        const r = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}`);
        const d = await r.json();
        const p = d?.run?.progress;
        if (p && p.step === 'finalize') {
          setFinalizeProgress({
            current: Number(p.current || 0),
            total: Number(p.total || 0),
            note: typeof p.note === 'string' ? p.note : undefined
          });
        }
      } catch { /* keep polling */ }
    }, 1000);

    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/finalize-taxonomy`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Finalize failed (${res.status})`);
      setLastFinalize({
        at: new Date(),
        rerouted: data.rerouted || 0,
        nullified: data.nullified || 0,
        failed: data.failed || 0
      });
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    } finally {
      if (finalizePollRef.current) {
        window.clearInterval(finalizePollRef.current);
        finalizePollRef.current = null;
      }
      setFinalizeProgress(null);
      setFinalizing(false);
    }
  }, [run.id, onRefresh, onError]);

  // Bucket Assignment: separate from taxonomy. After taxonomy is finalized,
  // run an LLM pass that maps each industry → a bucket from bucket_library
  // (or proposes a new bucket with primary_identity). Output goes into
  // bucket_industry_map.assigned_bucket_*; computeContactRollup picks it
  // up automatically with min_volume gating.
  const [assigningBuckets, setAssigningBuckets] = useState(false);
  const [lastBucketAssign, setLastBucketAssign] = useState<{ at: Date; matched: number; proposed: number; nullified: number; failed: number } | null>(null);
  const [bucketProposals, setBucketProposals] = useState<{ name: string; parent: string; count: number; samples: string[]; reasons: string[] }[]>([]);
  const [discoveredBuckets, setDiscoveredBuckets] = useState<{ assigned_bucket_name: string; assigned_bucket_primary_identity: string; is_new_bucket: boolean; contact_count: number }[]>([]);
  // Live progress polled while the bucket-assign POST is in flight.
  // The service writes bucketing_runs.progress every 5 batches
  // (50 industries) — 1 s polling sees every update without DB hammering.
  const [bucketAssignProgress, setBucketAssignProgress] = useState<{ current: number; total: number; note?: string } | null>(null);
  const bucketAssignPollRef = useRef<number | null>(null);
  // Wall-clock start of the in-flight bucket-assign POST. Used to compute
  // an ETA in the progress bar — refreshed each time we kick off the run.
  const lastBucketAssignStartRef = useRef<number | null>(null);

  // Stop polling on unmount so the interval doesn't outlive the component.
  useEffect(() => () => {
    if (bucketAssignPollRef.current) { window.clearInterval(bucketAssignPollRef.current); bucketAssignPollRef.current = null; }
  }, []);

  // Phase 1a diagnostic stats: counts + identity distribution straight
  // from bucket_industry_map. Surfaces the "all-null sub-identity" case
  // that bricks the existing Discovered Sub-Identities panel for runs
  // tagged before the May 9 prompt-key fix.
  const [phase1aStats, setPhase1aStats] = useState<{
    total_rows: number;
    llm_rows: number;
    dq_passthrough_rows: number;
    needs_qa_rows: number;
    with_identity: number;
    with_sub_identity: number;
    with_sector: number;
    distinct_identities: number;
    identity_distribution: { identity: string; industry_count: number }[];
    sample_null_sub_identity: { industry_string: string; primary_identity: string | null; llm_reason: string | null }[];
    all_null_sub_identity: boolean;
  } | null>(null);
  const [retagging, setRetagging] = useState(false);

  const refreshBucketPanels = useCallback(async () => {
    try {
      const [pRes, dRes, sRes] = await Promise.all([
        fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/proposed-buckets`).then(r => r.json()),
        fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/discovered-buckets`).then(r => r.json()),
        fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/phase1a-stats`).then(r => r.json())
      ]);
      setBucketProposals(Array.isArray(pRes.buckets) ? pRes.buckets : []);
      setDiscoveredBuckets(Array.isArray(dRes.buckets) ? dRes.buckets : []);
      if (sRes && typeof sRes === 'object' && !('error' in sRes)) setPhase1aStats(sRes);
    } catch { /* silent — surfacing here would be noise */ }
  }, [run.id]);

  useEffect(() => { refreshBucketPanels(); }, [refreshBucketPanels]);

  // Re-tag from scratch. Used to recover runs whose Phase 1a ran with
  // the broken prompt key (May 8 and earlier — sub_identity came back
  // null on every row). Confirm before firing because it costs LLM
  // tokens and overwrites every map row for the run.
  const triggerRetagPhase1a = useCallback(async () => {
    if (!confirm('Re-tag Phase 1a from scratch? This wipes the current bucket_industry_map for this run and re-calls the LLM tagger on every industry. Costs LLM tokens; takes 1–5 minutes for a 70k-contact run.')) return;
    setRetagging(true);
    onError(null);
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/retag-phase1a`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Re-tag failed (${res.status})`);
      // The endpoint returns 202 immediately and the tagger runs in
      // the background. Bump the parent so it re-polls run status.
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setRetagging(false);
    }
  }, [run.id, onError, onRefresh]);

  // Run a bucket-assignment POST with live progress polling. Both the
  // standalone "Run Bucket Assignment" button and the "Accept all +
  // re-assign" path share this so the user always sees a live percent.
  const runAssignWithProgress = useCallback(async () => {
    setBucketAssignProgress({ current: 0, total: 0 });
    lastBucketAssignStartRef.current = Date.now();
    if (bucketAssignPollRef.current) window.clearInterval(bucketAssignPollRef.current);
    bucketAssignPollRef.current = window.setInterval(async () => {
      try {
        const r = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}`);
        const d = await r.json();
        const p = d?.run?.progress;
        if (p && p.step === 'bucket_assign') {
          setBucketAssignProgress({
            current: Number(p.current || 0),
            total: Number(p.total || 0),
            note: typeof p.note === 'string' ? p.note : undefined
          });
        }
      } catch { /* keep polling */ }
    }, 1000);
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/assign-buckets`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Bucket assignment failed (${res.status})`);
      setLastBucketAssign({
        at: new Date(),
        matched: data.matched || 0,
        proposed: data.proposed || 0,
        nullified: data.nullified || 0,
        failed: data.failed || 0
      });
    } finally {
      if (bucketAssignPollRef.current) {
        window.clearInterval(bucketAssignPollRef.current);
        bucketAssignPollRef.current = null;
      }
      setBucketAssignProgress(null);
    }
  }, [run.id]);

  const triggerBucketAssign = useCallback(async () => {
    setAssigningBuckets(true);
    onError(null);
    try {
      await runAssignWithProgress();
      await refreshBucketPanels();
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setAssigningBuckets(false);
    }
  }, [runAssignWithProgress, refreshBucketPanels, onRefresh, onError]);

  const acceptProposedBucket = async (name: string, parent: string) => {
    onError(null);
    try {
      const res = await fetch('/api/bucketing/library', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ bucket_name: name, primary_identity: parent })
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
    } catch (e: any) { onError(e.message); }
  };

  const acceptAllProposedBuckets = async () => {
    if (bucketProposals.length === 0) return;
    if (!confirm(`Accept all ${bucketProposals.length} proposed bucket(s) into bucket_library, then re-run bucket assignment?`)) return;
    setAssigningBuckets(true);
    onError(null);
    try {
      for (const p of bucketProposals) {
        await acceptProposedBucket(p.name, p.parent);
      }
      // Re-run bucket assignment so newly-accepted entries flip from
      // is_new_bucket=true to false, and the library list now contains
      // them as canonical merge targets for any remaining proposals.
      await runAssignWithProgress();
      await refreshBucketPanels();
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setAssigningBuckets(false);
    }
  };
  const sourceBuckets = run.taxonomy_final?.buckets || run.taxonomy_proposal?.buckets || [];
  const primaryIdentities = (run.taxonomy_final?.primary_identities || run.taxonomy_proposal?.primary_identities) || [];
  const observedPatterns = (run.taxonomy_final?.observed_patterns || run.taxonomy_proposal?.observed_patterns) || [];
  const [kept, setKept] = useState<Set<string>>(new Set(sourceBuckets.map(b => b.sub_identity)));
  const [renames, setRenames] = useState<Record<string, string>>({});
  const [adds, setAdds] = useState<{ sub_identity: string; primary_identity: string; description: string }[]>([]);
  const [newSpec, setNewSpec] = useState('');
  const [newDesc, setNewDesc] = useState('');
  const [newIdentity, setNewIdentity] = useState('');
  const [minVolume, setMinVolume] = useState<number>(run.min_volume);
  const [bucketBudget, setBucketBudget] = useState<number>(run.bucket_budget || 30);
  const [busy, setBusy] = useState<'none' | 'saving' | 'assigning'>('none');
  const [showPatterns, setShowPatterns] = useState(false);
  // Library selection moved here from Setup. Default ON — pre-select every
  // non-archived library bucket the first time this Review screen renders;
  // Phase 1b's library_first match short-circuits the LLM for any contact
  // whose industry maps to one of these. The user can untick individual ones
  // before clicking Apply & Assign. We treat an empty saved value as "not
  // yet decided" and re-default on mount, matching the Setup-screen UX.
  const [selectedLib, setSelectedLib] = useState<Set<string>>(new Set());
  const libInitialized = useRef(false);
  useEffect(() => {
    if (libInitialized.current) return;
    if (!Array.isArray(library) || library.length === 0) return;
    const saved = Array.isArray(run.preferred_library_ids) ? run.preferred_library_ids : [];
    setSelectedLib(saved.length > 0
      ? new Set(saved)
      : new Set(library.filter(b => !b.archived).map(b => b.id)));
    libInitialized.current = true;
  }, [library, run.preferred_library_ids]);
  const [showLib, setShowLib] = useState(false);
  const toggleLib = (id: string) => {
    const s = new Set(selectedLib);
    s.has(id) ? s.delete(id) : s.add(id);
    setSelectedLib(s);
  };

  const toggle = (name: string) => {
    const s = new Set(kept);
    s.has(name) ? s.delete(name) : s.add(name);
    setKept(s);
  };

  const exportTaxonomyCsv = () => {
    // Hits the server-side streaming endpoint — single GET, browser shows
    // native download progress, no client-side memory usage. Beats the
    // previous client-side paginate-then-Papa.unparse approach which froze
    // the tab for ~45s on 46k contacts.
    onError(null);
    window.location.href = `/api/bucketing/runs/${encodeURIComponent(run.id)}/taxonomy-contacts.csv`;
  };
  const exporting = false;

  const apply = async (alsoAssign: boolean) => {
    setBusy(alsoAssign ? 'assigning' : 'saving');
    onError(null);
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/taxonomy`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            keep: Array.from(kept),
            rename: renames,
            add: adds,
            min_volume: minVolume,
            bucket_budget: bucketBudget,
            preferred_library_ids: Array.from(selectedLib)
        })
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
      if (alsoAssign) {
        const ar = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/assign`, { method: 'POST' });
        if (!ar.ok) {
          const data = await ar.json().catch(() => ({}));
          throw new Error(data.error || `Assign failed (${ar.status})`);
        }
      }
      // Await the parent refresh so Phase 1b's status='assigning' lands
      // BEFORE we clear the busy spinner — without the await, busy flips
      // back to 'none' for ~500 ms while fetchActive is still in flight,
      // and the user sees the Review screen with no spinner before the
      // parent finally swaps to BucketingProgressPanel.
      await Promise.resolve(onRefresh());
    } catch (e: any) {
      onError(e.message);
    } finally {
      setBusy('none');
    }
  };

  return (
    <div className="space-y-4">
      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
        <div className="text-xs text-gray-300">
          <span className="font-bold text-white">{primaryIdentities.length}</span> primary identities · <span className="font-bold text-white" title="Pairs where Phase 1a committed BOTH identity and sub-identity. Zero is normal if the LLM only committed identity-level tags.">{sourceBuckets.length}</span> sub-identity pairs · <span className="font-bold text-white" title="Buckets the bucket-assignment pass mapped industries to (post Run Bucket Assignment).">{discoveredBuckets.length}</span> discovered buckets · <span className="font-bold text-white">{run.total_contacts?.toLocaleString() || '?'}</span> contacts
          {run.taxonomy_model && <span className="text-gray-500"> · model: {run.taxonomy_model}</span>}
        </div>
        {observedPatterns.length > 0 && (
          <div className="mt-2">
            <button onClick={() => setShowPatterns(s => !s)} className="text-[11px] text-[#3ecf8e] hover:underline">
              {showPatterns ? 'Hide' : 'Show'} observed patterns ({observedPatterns.length})
            </button>
            {showPatterns && (
              <ul className="mt-2 list-disc list-inside text-[11px] text-gray-400 space-y-0.5">
                {observedPatterns.map((p, i) => <li key={i}>{p}</li>)}
              </ul>
            )}
          </div>
        )}
      </div>

      {/* Stale-prompt detection. Old runs (tagged before commit 5e51a5e
          on May 9) returned null for every sub_identity — the original
          prompt JSON key was hyphenated but the parser expected
          underscore. Recalculate / Finalize can't fix it (they read the
          existing data). The only path is a full re-tag. */}
      {phase1aStats?.all_null_sub_identity && (
        <div className="border border-amber-500/40 rounded-xl bg-amber-500/5 p-4">
          <div className="flex items-start justify-between gap-3 flex-wrap">
            <div className="flex-1 min-w-0">
              <div className="text-[10px] font-bold text-amber-300 uppercase tracking-widest mb-1">
                Phase 1a missing sub-identities
              </div>
              <div className="text-[11px] text-gray-300">
                This run has <span className="font-mono text-white">{phase1aStats.llm_rows.toLocaleString()}</span> Phase 1a rows but <span className="font-mono text-white">0</span> have a sub-identity — meaning the LLM was called with a buggy prompt that returned null sub_identity on every row (fixed May 9 via prompt key change). Recalculate / Finalize cannot recover these because they re-read the same null values. Re-tag from scratch to fix.
              </div>
            </div>
            <button
              onClick={triggerRetagPhase1a}
              disabled={retagging || finalizing || recalcing || assigningBuckets}
              className="shrink-0 px-3 py-1.5 rounded text-[10px] font-bold bg-amber-500 text-black hover:bg-amber-400 disabled:opacity-50 flex items-center gap-1"
              title="Wipe this run's bucket_industry_map and re-call the Phase 1a tagger on every industry. Costs LLM tokens."
            >
              {retagging ? <Loader2 className="w-3 h-3 animate-spin" /> : <RotateCcw className="w-3 h-3" />}
              {retagging ? 'Re-tagging…' : 'Re-tag Phase 1a'}
            </button>
          </div>
          {phase1aStats.sample_null_sub_identity.length > 0 && (
            <details className="mt-2 text-[10px] text-gray-400">
              <summary className="cursor-pointer hover:text-gray-200">Sample null-sub-identity rows ({phase1aStats.sample_null_sub_identity.length})</summary>
              <ul className="mt-1.5 space-y-0.5 font-mono">
                {phase1aStats.sample_null_sub_identity.map((s, i) => (
                  <li key={i} className="truncate">
                    <span className="text-gray-300">{s.industry_string}</span> → {s.primary_identity || '(no identity)'} · {s.llm_reason || '(no reason)'}
                  </li>
                ))}
              </ul>
            </details>
          )}
        </div>
      )}

      {/* Always-visible identity distribution. Works even when sub-identity
          pairs are 0 (the old Discovered Sub-Identities panel renders
          nothing in that case, leaving the user staring at a blank
          screen). */}
      {phase1aStats && phase1aStats.identity_distribution.length > 0 && (
        <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e]">
          <div className="px-4 py-3 border-b border-[#2e2e2e] text-[10px] font-bold text-gray-500 uppercase tracking-widest flex items-center justify-between gap-3 flex-wrap">
            <span>Phase 1a identity distribution ({phase1aStats.distinct_identities})</span>
            <span className="text-gray-600 normal-case tracking-normal font-normal">
              {phase1aStats.with_identity.toLocaleString()} industries with identity · {phase1aStats.with_sub_identity.toLocaleString()} with sub-identity · {phase1aStats.with_sector.toLocaleString()} with sector
            </span>
          </div>
          <div className="divide-y divide-[#2e2e2e]/40 max-h-72 overflow-y-auto custom-scrollbar">
            {phase1aStats.identity_distribution.map(row => (
              <div key={row.identity} className="px-4 py-1.5 flex items-center justify-between gap-3 text-[11px]">
                <span className="text-gray-200 truncate">{row.identity}</span>
                <span className="font-mono text-gray-500 shrink-0">{row.industry_count.toLocaleString()} industries</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <Phase1aProposedTagsPanel
        runId={run.id}
        onError={onError}
        recalcing={recalcing}
        onFinalize={triggerFinalize}
        finalizing={finalizing}
        finalizeProgress={finalizeProgress}
        lastFinalize={lastFinalize}
      />
      <Phase1aQAQueuePanel runId={run.id} onError={onError} />

      {/* ── Bucket Assignment (separates taxonomy from final campaign buckets) ── */}
      <div className="border border-[#3ecf8e]/30 rounded-xl bg-[#3ecf8e]/5 p-4">
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div className="flex-1 min-w-0">
            <div className="text-[10px] font-bold text-[#3ecf8e] uppercase tracking-widest mb-1">
              Bucket Assignment — map taxonomy to campaign buckets
            </div>
            <div className="text-[11px] text-gray-400">
              Asks the LLM to pick a bucket from <span className="text-gray-300">bucket_library</span> for every Phase 1a-tagged industry, or propose a new bucket if nothing fits. After this runs, Phase 1b uses these buckets directly (with min_volume gating). Run AFTER Finalize Taxonomy.
            </div>
            {lastBucketAssign && (
              <div className="text-[10px] text-gray-500 mt-1.5 font-mono">
                Last assignment {lastBucketAssign.at.toLocaleTimeString()} · {lastBucketAssign.matched} matched library, {lastBucketAssign.proposed} new proposals, {lastBucketAssign.nullified} → General{lastBucketAssign.failed > 0 ? `, ${lastBucketAssign.failed} batch failures` : ''}
              </div>
            )}
          </div>
          <button
            onClick={triggerBucketAssign}
            disabled={assigningBuckets || finalizing || recalcing}
            className="shrink-0 px-3 py-1.5 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1"
          >
            {assigningBuckets ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
            {assigningBuckets ? 'Assigning…' : 'Run Bucket Assignment'}
          </button>
        </div>

        {assigningBuckets && (() => {
          const cur = bucketAssignProgress?.current || 0;
          const tot = bucketAssignProgress?.total || 0;
          const pct = tot > 0 ? Math.min(100, Math.round((cur / tot) * 100)) : null;
          // Rough ETA: ~0.5 s per industry on average (10/batch, 30 concurrent
          // batches → 1 wave/0.5 s ≈ 60 industries/s ≈ 30s per 1k). Surface
          // it once we have enough samples for the math to mean something.
          const etaSec = (cur > 50 && tot > cur)
            ? Math.round(((tot - cur) / cur) * (Date.now() - (lastBucketAssignStartRef.current || Date.now())) / 1000)
            : null;
          const etaTxt = etaSec !== null && etaSec > 0
            ? (etaSec >= 60 ? `${Math.floor(etaSec / 60)}m ${etaSec % 60}s` : `${etaSec}s`)
            : null;
          return (
            <div className="mt-3">
              <div className="flex items-center justify-between text-[10px] font-mono mb-1">
                <span className="text-[#3ecf8e]/80">
                  {bucketAssignProgress?.note || 'Asking the LLM to map every industry to a bucket…'}
                </span>
                <span className="text-gray-400">
                  {cur.toLocaleString()}{tot > 0 ? ` / ${tot.toLocaleString()}` : ''}{pct !== null ? ` · ${pct}%` : ''}{etaTxt ? ` · ETA ${etaTxt}` : ''}
                </span>
              </div>
              <div className="h-1.5 bg-[#1c1c1c] rounded overflow-hidden">
                <div
                  className="h-full bg-[#3ecf8e] transition-all duration-300"
                  style={{ width: pct !== null ? `${pct}%` : '15%' }}
                />
              </div>
            </div>
          );
        })()}

        {bucketProposals.length > 0 && (
          <div className="mt-4 pt-3 border-t border-[#3ecf8e]/20">
            <div className="flex items-center justify-between gap-2 mb-2">
              <span className="text-[10px] font-bold text-amber-400 uppercase tracking-widest">
                AI-proposed buckets ({bucketProposals.length})
              </span>
              <button
                onClick={acceptAllProposedBuckets}
                disabled={assigningBuckets}
                className="px-2 py-0.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-200 border border-amber-500/40 hover:bg-amber-500/30 disabled:opacity-50"
              >
                Accept all + re-assign
              </button>
            </div>
            <p className="text-[11px] text-gray-400 mb-2">
              Buckets the LLM proposed because nothing in your library fit. Accepting one adds it to <span className="text-gray-300">bucket_library</span> and re-runs assignment so other industries can merge into it.
            </p>
            <ul className="space-y-1 max-h-72 overflow-y-auto pr-1">
              {bucketProposals.map(p => (
                <li
                  key={`${p.parent}::${p.name}`}
                  className="flex items-center gap-2 text-[11px] bg-[#1c1c1c] border border-[#2e2e2e] rounded px-2 py-1.5"
                >
                  <div className="flex-1 min-w-0">
                    <div className="font-bold text-white truncate" title={p.name}>{p.name}</div>
                    <div className="text-[10px] text-gray-500 truncate">
                      {p.count}× under {p.parent || '(no parent)'} · ex: {p.samples.slice(0, 2).join(' · ')}
                    </div>
                  </div>
                  <button
                    onClick={async () => {
                      await acceptProposedBucket(p.name, p.parent);
                      await triggerBucketAssign();
                    }}
                    disabled={assigningBuckets}
                    className="shrink-0 px-2 py-1 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50"
                  >
                    Accept
                  </button>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>

      {discoveredBuckets.length > 0 && (() => {
        // General is the final rollup catch-all, not a "discovered" bucket.
        // The SQL aggregation groups by (bucket_name, primary_identity), so
        // General appears once per identity that contributed contacts to it.
        // Surface it ONCE at the bottom with the total — splitting it per
        // identity made it look like every identity had its own General.
        const namedRows = discoveredBuckets.filter(b => b.assigned_bucket_name !== RESERVED_GENERAL);
        const generalTotal = discoveredBuckets
          .filter(b => b.assigned_bucket_name === RESERVED_GENERAL)
          .reduce((s, b) => s + Number(b.contact_count || 0), 0);
        const byIdentity = new Map<string, typeof discoveredBuckets>();
        for (const b of namedRows) {
          const key = b.assigned_bucket_primary_identity || '(no identity)';
          if (!byIdentity.has(key)) byIdentity.set(key, []);
          byIdentity.get(key)!.push(b);
        }
        return (
          <div className="border border-[#3ecf8e]/30 rounded-xl bg-[#0e0e0e]">
            <div className="px-4 py-3 border-b border-[#3ecf8e]/20 text-[10px] font-bold text-[#3ecf8e] uppercase tracking-widest flex items-center justify-between gap-3 flex-wrap">
              <button
                type="button"
                onClick={toggleDiscoveredBuckets}
                className="flex items-center gap-2 hover:text-[#3ecf8e]/70 transition-colors text-left"
                aria-expanded={discoveredBucketsOpen}
              >
                {discoveredBucketsOpen
                  ? <ChevronDown className="w-3 h-3" />
                  : <ChevronRight className="w-3 h-3" />}
                <span>Discovered buckets (after assignment, grouped by primary identity)</span>
              </button>
              <span className="text-gray-600 normal-case tracking-normal font-normal">
                {discoveredBucketsOpen
                  ? 'Min_volume gates these; small buckets roll up to identity → General.'
                  : `${byIdentity.size.toLocaleString()} ${byIdentity.size === 1 ? 'identity' : 'identities'} · click to expand`}
              </span>
            </div>
            {discoveredBucketsOpen && <div className="divide-y divide-[#2e2e2e]">
              {Array.from(byIdentity.entries()).map(([ident, items]) => {
                const total = items.reduce((s, b) => s + Number(b.contact_count || 0), 0);
                return (
                  <div key={ident} className="py-3">
                    <div className="px-4 pb-2 flex items-center gap-2 flex-wrap">
                      <span className="text-[9px] font-bold uppercase tracking-widest text-purple-400 bg-purple-500/10 border border-purple-500/30 px-1.5 py-0.5 rounded">Primary Identity</span>
                      <span className="text-sm font-bold text-white">{ident}</span>
                      <span className="text-[10px] font-mono text-gray-500">{total.toLocaleString()} contacts</span>
                    </div>
                    <div className="pl-4 border-l-2 border-[#2e2e2e] ml-4 divide-y divide-[#2e2e2e]/40">
                      {items.sort((a, b) => Number(b.contact_count) - Number(a.contact_count)).map(b => {
                        const cnt = Number(b.contact_count || 0);
                        const willRollUp = cnt > 0 && cnt < minVolume;
                        return (
                          <div key={b.assigned_bucket_name} className="py-2 pl-4 pr-3">
                            <div className="flex items-center gap-2 flex-wrap">
                              <span className="text-[9px] font-bold uppercase tracking-widest text-gray-500">↳ Bucket</span>
                              <span className="font-bold text-white">{b.assigned_bucket_name}</span>
                              {b.is_new_bucket && (
                                <span className="text-[9px] font-bold text-amber-400 bg-amber-500/10 border border-amber-500/30 px-1.5 py-0.5 rounded">AI-proposed</span>
                              )}
                              <span className="text-[10px] font-mono text-gray-400">{cnt.toLocaleString()} contacts</span>
                              {willRollUp && (
                                <span className="text-[10px] font-bold text-blue-400 bg-blue-500/10 border border-blue-500/30 px-2 py-0.5 rounded">
                                  Below threshold → identity "{ident}"
                                </span>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
              {generalTotal > 0 && (
                <div className="py-3 px-4 flex items-center gap-2 flex-wrap bg-[#1a1a1a]">
                  <span className="text-[9px] font-bold uppercase tracking-widest text-gray-500 bg-gray-500/10 border border-gray-500/30 px-1.5 py-0.5 rounded">Catch-all</span>
                  <span className="font-bold text-white">{RESERVED_GENERAL}</span>
                  <span className="text-[10px] font-mono text-gray-400">{generalTotal.toLocaleString()} contacts</span>
                  <span className="text-[10px] text-gray-500">— final rollup target across all identities</span>
                </div>
              )}
            </div>}
          </div>
        );
      })()}

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e]">
        <div className="px-4 py-3 border-b border-[#2e2e2e] text-[10px] font-bold text-gray-500 uppercase tracking-widest flex items-center justify-between gap-3 flex-wrap">
          <button
            type="button"
            onClick={toggleDiscoveredSubIdentities}
            className="flex items-center gap-2 hover:text-gray-300 transition-colors"
            aria-expanded={discoveredSubIdentitiesOpen}
          >
            {discoveredSubIdentitiesOpen
              ? <ChevronDown className="w-3 h-3" />
              : <ChevronRight className="w-3 h-3" />}
            <span>Discovered sub-identities (grouped by primary identity)</span>
          </button>
          <div className="flex items-center gap-3">
            {lastRecalc && (
              <span className="text-[10px] text-gray-500 normal-case tracking-normal font-normal">
                Last recalc: {lastRecalc.at.toLocaleTimeString()} · {lastRecalc.merges} merges
              </span>
            )}
            <button
              onClick={triggerRecalc}
              disabled={recalcing || finalizing}
              className="px-2 py-1 rounded text-[10px] font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e] disabled:opacity-50 normal-case tracking-normal flex items-center gap-1"
              title="Re-pass Phase 1a sub-identities against the current taxonomy library: refresh is_new flags + run consolidation so accepted entries become canonical merge targets. Does NOT re-run Bucket Assignment — use the green button above for that."
            >
              {recalcing ? <Loader2 className="w-3 h-3 animate-spin" /> : <RotateCcw className="w-3 h-3" />}
              {recalcing ? 'Recalculating…' : 'Recalculate sub-identities'}
            </button>
          </div>
        </div>
        {discoveredSubIdentitiesOpen && (
          <>
            <div className="px-4 py-2 border-b border-[#2e2e2e] text-[10px] text-gray-600 normal-case tracking-normal">
              Phase 1b counts decide the campaign bucket: combo → sub-identity → identity → General.
            </div>
            <BucketChainList
              buckets={sourceBuckets}
              identities={primaryIdentities}
              kept={kept}
              renames={renames}
              countByBucket={new Map(bucketCounts.map(c => [c.bucket_name, Number(c.contact_count) || 0]))}
              minVolume={minVolume}
              onToggle={toggle}
              onRename={(oldName, val) => setRenames({ ...renames, [oldName]: val })}
            />
          </>
        )}
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
        <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-3">Add custom sub-identity</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
          <input value={newSpec} onChange={e => setNewSpec(e.target.value)} placeholder="Sub-Identity (Layer 2)"
            className="px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]" />
          <select value={newIdentity} onChange={e => setNewIdentity(e.target.value)}
            className="px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white focus:outline-none focus:border-[#3ecf8e]">
            <option value="">Pick a primary identity (Layer 1)…</option>
            {primaryIdentities.map(p => <option key={p.name} value={p.name}>{p.name}</option>)}
          </select>
          <input value={newDesc} onChange={e => setNewDesc(e.target.value)} placeholder="One-sentence description"
            className="px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]" />
        </div>
        <button
          onClick={() => {
            if (!newSpec.trim() || !newIdentity.trim()) return;
            setAdds([...adds, {
              sub_identity: newSpec.trim(),
              primary_identity: newIdentity.trim(),
              description: newDesc.trim()
            }]);
            setNewSpec(''); setNewDesc(''); setNewIdentity('');
          }}
          className="mt-2 px-3 py-1.5 rounded text-[10px] font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e] flex items-center gap-1"
        >
          <Plus className="w-3 h-3" /> Add
        </button>
        {adds.length > 0 && (
          <div className="mt-3 space-y-1">
            {adds.map((a, i) => (
              <div key={i} className="flex items-center justify-between px-3 py-1.5 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-gray-300">
                <span><span className="font-bold text-white">{a.sub_identity}</span> · under {a.primary_identity} — {a.description}</span>
                <button onClick={() => setAdds(adds.filter((_, j) => j !== i))} className="text-gray-500 hover:text-red-400">
                  <X className="w-3 h-3" />
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
        <div className="flex items-center justify-between flex-wrap gap-2">
          <button
            onClick={() => setShowLib(s => !s)}
            className="text-[11px] text-gray-400 hover:text-white flex items-center gap-1"
          >
            <BookMarked className="w-3 h-3" />
            {showLib ? 'Hide' : 'Reuse'} library buckets ({selectedLib.size}/{library.length} selected)
          </button>
          {library.length > 0 && (
            <div className="flex gap-1">
              <button
                onClick={() => setSelectedLib(new Set(library.filter(b => !b.archived).map(b => b.id)))}
                className="px-2 py-0.5 rounded text-[10px] font-bold bg-[#2e2e2e] text-gray-200 hover:bg-[#3e3e3e]"
                title="Re-select every non-archived library bucket"
              >
                Select all
              </button>
              <button
                onClick={() => setSelectedLib(new Set())}
                disabled={selectedLib.size === 0}
                className="px-2 py-0.5 rounded text-[10px] font-bold bg-[#2e2e2e] text-gray-200 hover:bg-[#3e3e3e] disabled:opacity-40"
              >
                Select none
              </button>
            </div>
          )}
        </div>
        <p className="text-[10px] text-gray-500 italic mt-1">
          Selected library buckets short-circuit the Phase 1b per-contact LLM via embedding match — independent of Bucket Assignment, which decides industry → bucket. Keep a curated set selected as the cheapest way to bucket overlapping lists.
        </p>
        {showLib && (
          <div className="mt-2 border border-[#2e2e2e] rounded max-h-48 overflow-y-auto custom-scrollbar">
            {library.length === 0 ? (
              <div className="text-xs text-gray-500 italic px-3 py-2">No saved library buckets yet. After completing a run, save useful buckets to reuse here.</div>
            ) : library.map(b => {
              const isSel = selectedLib.has(b.id);
              return (
                <button
                  key={b.id}
                  onClick={() => toggleLib(b.id)}
                  className={`w-full flex items-start justify-between px-3 py-2 text-left text-xs border-b border-[#2e2e2e] last:border-b-0 transition-colors ${isSel ? 'bg-[#3ecf8e]/10' : 'hover:bg-white/[0.02]'}`}
                >
                  <span className="flex items-start gap-2 flex-1 min-w-0">
                    <input type="checkbox" checked={isSel} onChange={() => {}} className="w-3 h-3 mt-0.5" />
                    <span className="min-w-0">
                      <span className={`font-medium block ${isSel ? 'text-[#3ecf8e]' : 'text-gray-200'}`}>
                        {b.bucket_name}
                      </span>
                      <span className="text-[10px] text-gray-500 truncate block">
                        under {b.primary_identity || b.direct_ancestor || '—'} · used {b.times_used}×
                      </span>
                    </span>
                  </span>
                </button>
              );
            })}
          </div>
        )}
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4 grid grid-cols-1 md:grid-cols-2 gap-4">
        <div>
          <span className="block text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">Min volume</span>
          <input
            type="number"
            min={0}
            value={minVolume}
            onChange={e => setMinVolume(Math.max(0, parseInt(e.target.value || '0', 10)))}
            className="w-28 px-2 py-1 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white focus:outline-none focus:border-[#3ecf8e]"
          />
          <p className="text-[10px] text-gray-500 italic mt-1">Combos below this fall to sub-identity; sub-identities below to identity; identities below to General.</p>
          {(run.total_contacts || 0) > 30000 && (
            <p className="text-[10px] text-amber-400 italic mt-1">
              ↑ For lists this size ({(run.total_contacts || 0).toLocaleString()} contacts), try min_volume = 250–500 to keep buckets meaningful and avoid 100+ small specs.
            </p>
          )}
        </div>
        <div>
          <span className="block text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">Bucket budget</span>
          <input
            type="number"
            min={5}
            max={100}
            value={bucketBudget}
            onChange={e => setBucketBudget(Math.max(5, Math.min(100, parseInt(e.target.value || '30', 10))))}
            className="w-28 px-2 py-1 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white focus:outline-none focus:border-[#3ecf8e]"
          />
          <p className="text-[10px] text-gray-500 italic mt-1">Cap on total campaign buckets. Smallest are rolled up further until the count fits.</p>
        </div>
      </div>

      {/* Inline starting banner — visible during the brief window between
          POSTing /assign and the parent re-rendering to the Phase 1b
          progress panel. Makes it obvious that the click landed and that
          the run is now backgrounded. */}
      {busy === 'assigning' && (
        <div className="border border-[#3ecf8e]/40 rounded-xl bg-[#3ecf8e]/5 p-4 flex items-start gap-3">
          <Loader2 className="w-4 h-4 text-[#3ecf8e] animate-spin shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0">
            <div className="text-[11px] font-bold text-[#3ecf8e]">
              Phase 1b started — opening live progress view…
            </div>
            <div className="text-[10px] text-gray-400 mt-0.5">
              You can safely close this tab or navigate away — Phase 1b runs in the background on the server.
              When you return, open this run from the index to see the live progress + ETA.
            </div>
          </div>
        </div>
      )}

      <div className="flex justify-between items-center gap-2 flex-wrap">
        <button
          onClick={exportTaxonomyCsv}
          disabled={exporting}
          className="px-4 py-2 rounded text-xs font-bold bg-[#1c1c1c] border border-[#2e2e2e] text-gray-300 hover:bg-[#2e2e2e] disabled:opacity-50 flex items-center gap-1"
          title="Download every contact in this run with its Phase 1a taxonomy assignment. Phase 1b not required."
        >
          {exporting ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
          Export Phase 1a taxonomy (CSV)
        </button>
        <div className="flex gap-2">
          <button
            onClick={() => apply(false)}
            disabled={busy !== 'none'}
            className="px-4 py-2 rounded text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e] disabled:opacity-50"
          >
            {busy === 'saving' ? <Loader2 className="w-3 h-3 inline animate-spin mr-1" /> : null}
            Save changes
          </button>
          <button
            onClick={() => apply(true)}
            disabled={busy !== 'none' || recalcing || finalizing || assigningBuckets}
            title={
              recalcing ? 'Recalculation in progress…'
              : finalizing ? 'Finalize in progress…'
              : assigningBuckets ? 'Bucket assignment in progress…'
              : (discoveredBuckets.length === 0 && !lastBucketAssign)
                  ? 'No bucket assignment yet — every contact will route to General. Click "Run Bucket Assignment" first if you want library buckets used.'
                  : 'Save taxonomy edits and run Phase 1b (per-contact routing).'
            }
            className="px-4 py-2 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1"
          >
            {busy === 'assigning' ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
            Apply &amp; Assign
          </button>
        </div>
      </div>
    </div>
  );
}

// ───── IDENTITY → SPECIALIZATION TREE ─────────────────────────────

function BucketChainList({
  buckets, identities, kept, renames, countByBucket, minVolume, onToggle, onRename
}: {
  buckets: BucketProposal[];
  identities: { name: string; description?: string; identity_type?: string; operator_required?: boolean }[];
  kept: Set<string>;
  renames: Record<string, string>;
  countByBucket: Map<string, number>;
  minVolume: number;
  onToggle: (name: string) => void;
  onRename: (oldName: string, val: string) => void;
}) {
  const displayName = (b: BucketProposal) => renames[b.sub_identity] ?? b.sub_identity;
  const baseCountFor = (b: BucketProposal) =>
    countByBucket.get(displayName(b)) ?? countByBucket.get(b.sub_identity) ?? 0;

  // Group specializations by primary_identity. Use the order of `identities`
  // when available, then any leftover identities in the proposal.
  const byIdent = new Map<string, BucketProposal[]>();
  for (const b of buckets) {
    const ident = b.primary_identity || '(unassigned)';
    if (!byIdent.has(ident)) byIdent.set(ident, []);
    byIdent.get(ident)!.push(b);
  }
  const orderedIdents = [
    ...identities.filter(p => byIdent.has(p.name)).map(p => p.name),
    ...Array.from(byIdent.keys()).filter(n => !identities.some(p => p.name === n))
  ];

  return (
    <div className="divide-y divide-[#2e2e2e]">
      {orderedIdents.map(identName => {
        const specs = byIdent.get(identName) || [];
        const meta = identities.find(p => p.name === identName);
        const identCount = specs.reduce((s, l) => s + (kept.has(l.sub_identity) ? baseCountFor(l) : 0), 0);
        return (
          <div key={identName} className="py-3">
            <div className="px-4 pb-2 flex items-center gap-2 flex-wrap">
              <span className="text-[9px] font-bold uppercase tracking-widest text-purple-400 bg-purple-500/10 border border-purple-500/30 px-1.5 py-0.5 rounded">Primary Identity</span>
              <span className="text-sm font-bold text-white">{identName}</span>
              {meta?.operator_required && (
                <span className="text-[9px] font-bold uppercase text-amber-400 bg-amber-500/10 border border-amber-500/30 px-1.5 py-0.5 rounded">Operator</span>
              )}
              <span className="text-[10px] font-mono text-gray-500">{identCount.toLocaleString()} contacts</span>
              {meta?.description && <span className="text-[10px] text-gray-500 italic ml-1">— {meta.description}</span>}
            </div>
            <div className="pl-4 border-l-2 border-[#2e2e2e] ml-4 divide-y divide-[#2e2e2e]/40">
              {specs.map(b => {
                const isKept = kept.has(b.sub_identity);
                const count = baseCountFor(b);
                const willRollUp = isKept && count > 0 && count < minVolume;
                return (
                  <div key={b.sub_identity} className={`py-2 pl-4 pr-3 ${isKept ? '' : 'opacity-50'}`}>
                    <div className="flex items-start gap-2">
                      <input type="checkbox" checked={isKept} onChange={() => onToggle(b.sub_identity)} className="mt-1.5 w-3.5 h-3.5 shrink-0" />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1 flex-wrap">
                          <span className="text-[9px] font-bold uppercase tracking-widest text-gray-500 shrink-0">↳ Sub-Identity</span>
                          <input
                            value={displayName(b)}
                            onChange={e => onRename(b.sub_identity, e.target.value)}
                            className="flex-1 min-w-[300px] bg-[#1c1c1c] border border-[#2e2e2e] rounded px-3 py-1.5 text-sm font-bold text-white focus:outline-none focus:border-[#3ecf8e]"
                          />
                          {b.library_match_id && (
                            <span className="text-[9px] font-bold text-blue-400 bg-blue-500/10 border border-blue-500/30 px-1.5 py-0.5 rounded shrink-0">📚 from library</span>
                          )}
                        </div>
                        <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                          <span className="text-[10px] font-mono text-gray-400">{count.toLocaleString()} contacts (pre-rollup)</span>
                          {willRollUp && (
                            <span className="text-[10px] font-bold text-blue-400 bg-blue-500/10 border border-blue-500/30 px-2 py-0.5 rounded">
                              Below threshold → identity "{identName}"
                            </span>
                          )}
                          {b.estimated_usage_label && (
                            <span className="text-[9px] text-gray-600 italic">{b.estimated_usage_label}</span>
                          )}
                        </div>
                        <p className="text-[11px] text-gray-400">{b.description}</p>
                        {b.example_strings && b.example_strings.length > 0 && (
                          <div className="text-[10px] text-gray-600 mt-1.5 truncate">
                            Examples: {b.example_strings.slice(0, 5).join(' · ')}{b.example_strings.length > 5 ? ` · +${b.example_strings.length - 5}` : ''}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ───── PIPELINE RE-RUN PANEL ──────────────────────────────────────
// Lets the user re-run any single phase of the bucketing pipeline on
// a finished (or failed) run without having to delete it and start
// over. Each re-run fires the same backend endpoint that ran the
// phase originally:
//   - Phase 1a    → POST /retag-phase1a   (LLM re-tag every industry)
//   - Bucket Assignment → POST /assign-buckets (industry → bucket)
//   - Phase 1b    → POST /assign           (per-contact routing)
// Each endpoint flips bucketing_runs.status to its phase-specific
// "in flight" value before returning 202, so the parent BucketingDetail
// auto-routes to BucketingProgressPanel within the next 1.5 s poll tick.

function PipelineRerunPanel({ run, onRefresh, onError }: {
  run: BucketingRun;
  onRefresh: () => void;
  onError: (msg: string | null) => void;
}) {
  // One state machine so only one re-run can be in-flight at a time —
  // the others get disabled while a phase is starting up.
  const [busy, setBusy] = useState<null | 'phase1a' | 'bucket_assign' | 'phase1b'>(null);

  // Inline progress polled while the bucket-assign POST is in flight.
  // Phase 1a / Phase 1b flip bucketing_runs.status so the parent
  // BucketingDetail auto-routes to BucketingProgressPanel; Bucket
  // Assignment doesn't flip status, so we surface its live progress
  // right here on the Results screen via the same poll-bucketing_runs
  // pattern the Review screen uses.
  const [bucketAssignProgress, setBucketAssignProgress] = useState<{ current: number; total: number; note?: string } | null>(null);
  const bucketAssignPollRef = useRef<number | null>(null);
  useEffect(() => () => {
    if (bucketAssignPollRef.current) { window.clearInterval(bucketAssignPollRef.current); bucketAssignPollRef.current = null; }
  }, []);

  const post = async (endpoint: string, label: typeof busy, errorPrefix: string) => {
    setBusy(label);
    onError(null);
    // Spin up live progress polling for the synchronous bucket-assign
    // POST so the user sees forward motion (the call can take minutes).
    if (label === 'bucket_assign') {
      setBucketAssignProgress({ current: 0, total: 0 });
      if (bucketAssignPollRef.current) window.clearInterval(bucketAssignPollRef.current);
      bucketAssignPollRef.current = window.setInterval(async () => {
        try {
          const r = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}`);
          const d = await r.json();
          const p = d?.run?.progress;
          if (p && p.step === 'bucket_assign') {
            setBucketAssignProgress({
              current: Number(p.current || 0),
              total: Number(p.total || 0),
              note: typeof p.note === 'string' ? p.note : undefined
            });
          }
        } catch { /* keep polling */ }
      }, 1000);
    }
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}${endpoint}`, { method: 'POST' });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || `${errorPrefix} failed (${res.status})`);
      // Wait one tick before refreshing so the status flip on the server
      // lands before the parent's fetchActive picks it up — without this
      // we sometimes see status=completed for one beat after the click.
      await new Promise(r => setTimeout(r, 200));
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    } finally {
      if (bucketAssignPollRef.current) {
        window.clearInterval(bucketAssignPollRef.current);
        bucketAssignPollRef.current = null;
      }
      setBucketAssignProgress(null);
      setBusy(null);
    }
  };

  const rerunPhase1a = () => {
    if (!confirm(
      'Re-tag Phase 1a from scratch?\n\n' +
      'This wipes bucket_industry_map for this run and re-calls the LLM tagger ' +
      'on every distinct industry. Bucket Assignment + Phase 1b results stay ' +
      'in the DB but will be stale until you re-run them too.\n\n' +
      'Cost: ~$15–150 depending on the model. Takes 1–5 min for a 70k-contact run.'
    )) return;
    post('/retag-phase1a', 'phase1a', 'Phase 1a re-tag');
  };

  const rerunBucketAssign = () => {
    if (!confirm(
      'Re-run Bucket Assignment?\n\n' +
      'Re-asks the LLM to map every Phase 1a-tagged industry to a campaign ' +
      'bucket from bucket_library. Phase 1a tags stay intact. Phase 1b\'s ' +
      'per-contact assignments will be stale until you re-run Phase 1b too.\n\n' +
      'Cost: a few cents typically (industries, not contacts).'
    )) return;
    post('/assign-buckets', 'bucket_assign', 'Bucket Assignment');
  };

  const rerunPhase1b = () => {
    if (!confirm(
      'Re-run Phase 1b (per-contact routing)?\n\n' +
      'Wipes bucket_contact_map + bucket_assignments and re-routes every ' +
      'contact through the library / embedding / LLM cascade using the ' +
      'current Phase 1a tags + Bucket Assignment + run settings ' +
      '(min_volume, bucket_budget, library reuse).\n\n' +
      'Cost: usually small thanks to the JOIN-first lookup; takes minutes-to-hours ' +
      'depending on contact count and cache hit rate.'
    )) return;
    post('/assign', 'phase1b', 'Phase 1b');
  };

  const inFlight = busy !== null;

  return (
    <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
      <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">
        Re-run pipeline steps
      </div>
      <p className="text-[11px] text-gray-400 mb-3">
        Re-run any single phase on this run without deleting it. Each phase is
        independent at the backend; re-running an earlier phase invalidates the
        downstream output until you re-run those phases too.
      </p>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
        <button
          onClick={rerunPhase1a}
          disabled={inFlight}
          className="px-3 py-2 rounded text-[11px] font-bold bg-[#1c1c1c] border border-[#2e2e2e] text-gray-200 hover:bg-[#2e2e2e] hover:border-[#3ecf8e]/40 disabled:opacity-50 flex items-center justify-center gap-1.5"
          title="Wipe bucket_industry_map for this run and re-call the Phase 1a LLM tagger on every industry."
        >
          {busy === 'phase1a'
            ? <Loader2 className="w-3 h-3 animate-spin" />
            : <RotateCcw className="w-3 h-3" />}
          {busy === 'phase1a' ? 'Starting…' : 'Re-tag Phase 1a'}
        </button>
        <button
          onClick={rerunBucketAssign}
          disabled={inFlight}
          className="px-3 py-2 rounded text-[11px] font-bold bg-[#1c1c1c] border border-[#2e2e2e] text-gray-200 hover:bg-[#2e2e2e] hover:border-[#3ecf8e]/40 disabled:opacity-50 flex items-center justify-center gap-1.5"
          title="Re-map every industry to a campaign bucket. Phase 1a tags stay intact."
        >
          {busy === 'bucket_assign'
            ? <Loader2 className="w-3 h-3 animate-spin" />
            : <RotateCcw className="w-3 h-3" />}
          {busy === 'bucket_assign' ? 'Starting…' : 'Re-run Bucket Assignment'}
        </button>
        <button
          onClick={rerunPhase1b}
          disabled={inFlight}
          className="px-3 py-2 rounded text-[11px] font-bold bg-[#3ecf8e]/10 border border-[#3ecf8e]/40 text-[#3ecf8e] hover:bg-[#3ecf8e]/20 disabled:opacity-50 flex items-center justify-center gap-1.5"
          title="Wipe bucket_contact_map and re-route every contact. Uses current Phase 1a + Bucket Assignment + run settings."
        >
          {busy === 'phase1b'
            ? <Loader2 className="w-3 h-3 animate-spin" />
            : <Play className="w-3 h-3" />}
          {busy === 'phase1b' ? 'Starting…' : 'Re-run Phase 1b'}
        </button>
      </div>
      {busy === 'bucket_assign' && (() => {
        const cur = bucketAssignProgress?.current || 0;
        const tot = bucketAssignProgress?.total || 0;
        const pct = tot > 0 ? Math.min(100, Math.round((cur / tot) * 100)) : null;
        return (
          <div className="mt-3">
            <div className="flex items-center justify-between text-[10px] font-mono mb-1">
              <span className="text-[#3ecf8e]/80">
                {bucketAssignProgress?.note || 'Asking the LLM to map every industry to a bucket…'}
              </span>
              <span className="text-gray-400">
                {cur.toLocaleString()}{tot > 0 ? ` / ${tot.toLocaleString()}` : ''}{pct !== null ? ` · ${pct}%` : ''}
              </span>
            </div>
            <div className="h-1.5 bg-[#1c1c1c] rounded overflow-hidden">
              <div
                className="h-full bg-[#3ecf8e] transition-all duration-300"
                style={{ width: pct !== null ? `${pct}%` : '15%' }}
              />
            </div>
          </div>
        );
      })()}
      <p className="text-[10px] text-gray-600 italic mt-2">
        After clicking, you'll be switched to the live progress view automatically
        (Phase 1a / Phase 1b) or see progress here (Bucket Assignment).
        Safe to close the tab — runs continue server-side.
      </p>
    </div>
  );
}

// ───── RESULTS VIEW ───────────────────────────────────────────────

function BucketingResults({ run, bucketCounts, sectorMix, generalBreakdown, onRefresh, onError, onLibrarySaved }: {
  run: BucketingRun;
  bucketCounts: any[];
  sectorMix: any[];
  generalBreakdown: any[];
  onRefresh: () => void;
  onError: (msg: string | null) => void;
  onLibrarySaved: () => void;
}) {
  const sectorByBucket = new Map<string, { sector: string; count: number }[]>();
  for (const row of sectorMix || []) sectorByBucket.set(row.bucket_name, row.sectors || []);
  const [exportingBucket, setExportingBucket] = useState<string | null>(null);
  const [savingLibrary, setSavingLibrary] = useState(false);
  const [librarySelection, setLibrarySelection] = useState<Set<string>>(new Set());

  // ── Async full-CSV export ───────────────────────────────────────────
  // Job lifecycle: pending → running → ready (or failed). The UI polls
  // every 2 s while a job is in flight and shows the latest job for this
  // run (within 24 h) as a download link so the user can re-grab it
  // without re-running the export.
  type CsvJob = {
    id: string;
    status: 'pending' | 'running' | 'ready' | 'failed';
    progress_rows: number;
    total_rows: number | null;
    download_url: string | null;
    file_size_bytes: number | null;
    error_message: string | null;
    created_at: string;
    completed_at: string | null;
    expires_at: string;
  };
  const [csvJob, setCsvJob] = useState<CsvJob | null>(null);
  const [csvJobLoading, setCsvJobLoading] = useState(false);
  const csvPollRef = useRef<number | null>(null);

  const fetchLatestCsvJob = useCallback(async () => {
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/csv-jobs`);
      const data = await res.json();
      if (!res.ok) return;
      const latest = (data.jobs || [])[0] || null;
      setCsvJob(latest);
    } catch {
      // silent — surfacing this in the UI would be noise on a polling loop
    }
  }, [run.id]);

  useEffect(() => { fetchLatestCsvJob(); }, [fetchLatestCsvJob]);

  // Poll while a job is in-flight; stop once it lands on ready/failed.
  useEffect(() => {
    if (csvPollRef.current) { window.clearInterval(csvPollRef.current); csvPollRef.current = null; }
    if (!csvJob || (csvJob.status !== 'pending' && csvJob.status !== 'running')) return;
    csvPollRef.current = window.setInterval(async () => {
      try {
        const res = await fetch(`/api/bucketing/csv-jobs/${csvJob.id}`);
        const data = await res.json();
        if (res.ok && data.job) setCsvJob(data.job);
      } catch { /* keep polling */ }
    }, 2000);
    return () => {
      if (csvPollRef.current) { window.clearInterval(csvPollRef.current); csvPollRef.current = null; }
    };
  }, [csvJob?.id, csvJob?.status]);

  const startCsvJob = async () => {
    setCsvJobLoading(true);
    onError(null);
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/csv-jobs`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
      setCsvJob(data.job);
    } catch (e: any) {
      onError(e.message);
    } finally {
      setCsvJobLoading(false);
    }
  };

  const sorted = [...bucketCounts].sort((a, b) => Number(b.contact_count) - Number(a.contact_count));
  const total = sorted.reduce((s, b) => s + Number(b.contact_count), 0);
  const max = sorted.length > 0 ? Number(sorted[0].contact_count) : 1;

  const bucketsInRun = (run.taxonomy_final?.buckets || run.taxonomy_proposal?.buckets || []) as BucketProposal[];
  const identityNames = new Set(((run.taxonomy_final?.primary_identities || run.taxonomy_proposal?.primary_identities) || []).map(p => p.name));
  const specNames = new Set(bucketsInRun.map(b => b.sub_identity));

  const toggleLibSel = (name: string) => {
    const s = new Set(librarySelection);
    s.has(name) ? s.delete(name) : s.add(name);
    setLibrarySelection(s);
  };

  const downloadCsv = async (bucket: string) => {
    setExportingBucket(bucket);
    onError(null);
    try {
      const PAGE = 500;
      const rows: any[] = [];
      let page = 1;
      while (true) {
        const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/contacts?bucket=${encodeURIComponent(bucket)}&page=${page}&pageSize=${PAGE}`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
        rows.push(...(data.data || []));
        if (!data.data || data.data.length < PAGE) break;
        page++;
      }
      const csvRows = rows.map(r => ({
        contact_id: r.contact_id,
        email: r.contacts?.email,
        first_name: r.contacts?.first_name,
        last_name: r.contacts?.last_name,
        company_name: r.contacts?.company_name,
        company_website: r.contacts?.company_website,
        lead_list_name: r.contacts?.lead_list_name,
        classification_text: r.contacts?.industry,
        // 3-layer truth schema (v5)
        primary_identity: r.primary_identity || r.bucket_ancestor || '',
        sub_identity: r.sub_identity || r.bucket_leaf || '',
        sector: r.sector || '',
        canonical_classification: r.canonical_classification || '',
        // Routing decision
        final_campaign_bucket: r.bucket_name,
        fallback_level_used: r.rollup_level,
        pre_rollup_bucket: r.pre_rollup_bucket_name,
        bucket_reason: r.bucket_reason || r.general_reason || '',
        classification_reason: r.reasons?.classification_reason || r.reasons?.llm_reason || '',
        // Status flags
        generic: r.is_generic,
        disqualified: r.is_disqualified,
        confidence_score: r.confidence,
        identity_confidence: r.identity_confidence,
        sub_identity_confidence: r.sub_identity_confidence,
        sector_confidence: r.sector_confidence,
        source: r.source,
      }));
      const csv = Papa.unparse(csvRows);
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      const safe = bucket.replace(/[^a-z0-9-_]+/gi, '_');
      a.href = url;
      a.download = `${run.name.replace(/[^a-z0-9-_]+/gi, '_')}_${safe}.csv`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      onError(e.message);
    } finally {
      setExportingBucket(null);
    }
  };

  const saveSelectedToLibrary = async () => {
    if (librarySelection.size === 0) return;
    setSavingLibrary(true);
    onError(null);
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(run.id)}/save-to-library`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ bucket_names: Array.from(librarySelection) })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
      setLibrarySelection(new Set());
      onLibrarySaved();
      alert(`Saved ${data.saved} bucket(s) to library`);
    } catch (e: any) {
      onError(e.message);
    } finally {
      setSavingLibrary(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <StatCard label="Buckets" value={sorted.length.toString()} color="text-white" />
        <StatCard label="Contacts assigned" value={total.toLocaleString()} color="text-[#3ecf8e]" />
        <StatCard label="Total cost" value={`$${(Number(run.cost_usd) || 0).toFixed(3)}`} color="text-white" />
      </div>

      <PipelineRerunPanel run={run} onRefresh={onRefresh} onError={onError} />

      {Array.isArray(run.quality_warnings) && run.quality_warnings.length > 0 && (
        <div className="border border-amber-500/30 rounded-xl bg-amber-500/5 p-4">
          <div className="text-[10px] font-bold text-amber-400 uppercase tracking-widest mb-2">Quality warnings</div>
          <ul className="space-y-1 text-xs text-amber-100/80">
            {run.quality_warnings.map((w, i) => <li key={i}>{w}</li>)}
          </ul>
        </div>
      )}

      {(run as any).generic_audit && (
        <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
          <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2">Generic Audit</div>
          {(() => {
            const audit = (run as any).generic_audit;
            const reclaimed = Number(audit.reclaimed || 0);
            const targets: { bucket: string; count: number; from: string }[] = audit.targets || [];
            return (
              <div>
                <div className="text-xs text-gray-300">
                  {reclaimed > 0
                    ? <>Reclaimed <span className="font-bold text-[#3ecf8e]">{reclaimed.toLocaleString()}</span> rows from General into <span className="font-bold text-white">{targets.length}</span> bucket{targets.length === 1 ? '' : 's'}.</>
                    : <span className="text-gray-500">No rows needed reclaiming — General was already minimal.</span>
                  }
                </div>
                {targets.length > 0 && (
                  <div className="mt-2 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                    {targets.slice(0, 12).map((t, i) => (
                      <div key={i} className="px-3 py-2 rounded border border-[#2e2e2e] bg-[#1c1c1c]">
                        <div className="text-xs font-bold text-white truncate" title={t.bucket}>{t.bucket}</div>
                        <div className="text-[10px] text-gray-400">+{Number(t.count).toLocaleString()} <span className="text-gray-600">via {t.from}</span></div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      )}

      {generalBreakdown.length > 0 && (
        <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
          <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-3">General breakdown</div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            {generalBreakdown.map((g, i) => (
              <div key={`${g.general_reason}-${g.source}-${i}`} className="px-3 py-2 rounded border border-[#2e2e2e] bg-[#1c1c1c]">
                <div className="text-xs font-bold text-amber-300">{Number(g.contact_count || 0).toLocaleString()}</div>
                <div className="text-[10px] text-gray-400">{g.general_reason || 'unspecified'}</div>
                <div className="text-[9px] text-gray-600 font-mono">{g.source}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {bucketsInRun.length > 0 && (
        <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
          <div className="flex items-center justify-between mb-3">
            <span className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">
              Save proven sub-identities to library ({librarySelection.size} selected)
            </span>
            <button
              onClick={saveSelectedToLibrary}
              disabled={savingLibrary || librarySelection.size === 0}
              className="px-3 py-1.5 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1"
            >
              {savingLibrary ? <Loader2 className="w-3 h-3 animate-spin" /> : <BookMarked className="w-3 h-3" />}
              Save selected
            </button>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {bucketsInRun.map(b => {
              const sel = librarySelection.has(b.sub_identity);
              return (
                <button
                  key={b.sub_identity}
                  onClick={() => toggleLibSel(b.sub_identity)}
                  className={`px-2 py-1 rounded text-[10px] font-bold border transition-colors ${sel ? 'bg-[#3ecf8e]/15 text-[#3ecf8e] border-[#3ecf8e]/40' : 'bg-[#1c1c1c] text-gray-300 border-[#2e2e2e] hover:border-gray-500'}`}
                  title={`under ${b.primary_identity}`}
                >
                  {b.sub_identity}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Async full-CSV export — Phase 1b assignments × enrichments × Phase 1a tags */}
      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div className="flex-1 min-w-0">
            <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">
              Full CSV export (all contacts × bucket assignments)
            </div>
            <div className="text-[11px] text-gray-400">
              Streams every contact with its enrichment, taxonomy (identity / sub-identity / sector), final bucket, and bucketing reasoning. Built async + gzipped — link expires after 24 hours.
            </div>
          </div>
          {(!csvJob || csvJob.status === 'failed' || csvJob.status === 'ready') && (
            <button
              onClick={startCsvJob}
              disabled={csvJobLoading}
              className="px-3 py-1.5 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1 shrink-0"
            >
              {csvJobLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
              {csvJob?.status === 'ready' ? 'Re-export' : csvJob?.status === 'failed' ? 'Retry export' : 'Generate full CSV'}
            </button>
          )}
        </div>

        {csvJob && (csvJob.status === 'pending' || csvJob.status === 'running') && (() => {
          const total = csvJob.total_rows || 0;
          const done = csvJob.progress_rows || 0;
          const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : null;
          return (
            <div className="mt-3">
              <div className="flex items-center justify-between text-[11px] font-mono mb-1">
                <span className="text-gray-300">
                  {csvJob.status === 'pending' ? 'Queued…' : 'Streaming rows…'}
                </span>
                <span className="text-gray-400">
                  {done.toLocaleString()}{total > 0 ? ` / ${total.toLocaleString()}` : ''}{pct !== null ? ` · ${pct}%` : ''}
                </span>
              </div>
              <div className="h-1.5 bg-[#1c1c1c] rounded overflow-hidden">
                <div
                  className="h-full bg-[#3ecf8e] transition-all"
                  style={{ width: pct !== null ? `${pct}%` : '15%' }}
                />
              </div>
            </div>
          );
        })()}

        {csvJob && csvJob.status === 'ready' && csvJob.download_url && (() => {
          const sizeMb = csvJob.file_size_bytes ? (csvJob.file_size_bytes / 1024 / 1024).toFixed(1) : null;
          const expiresAt = new Date(csvJob.expires_at);
          const expiresHrs = Math.max(0, Math.round((expiresAt.getTime() - Date.now()) / 3_600_000));
          return (
            <div className="mt-3 flex items-center justify-between gap-3 flex-wrap">
              <div className="text-[11px] text-gray-400">
                Ready · {csvJob.progress_rows.toLocaleString()} rows{sizeMb ? ` · ${sizeMb} MB gzipped` : ''} · expires in ~{expiresHrs}h
              </div>
              <a
                href={csvJob.download_url}
                target="_blank"
                rel="noopener noreferrer"
                className="px-3 py-1.5 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] flex items-center gap-1 shrink-0"
              >
                <Download className="w-3 h-3" /> Download CSV (.gz)
              </a>
            </div>
          );
        })()}

        {csvJob && csvJob.status === 'failed' && (
          <div className="mt-3 text-[11px] text-amber-300">
            Export failed: {csvJob.error_message || 'unknown error'}
          </div>
        )}
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e]">
        <div className="px-4 py-3 border-b border-[#2e2e2e] text-[10px] font-bold text-gray-500 uppercase tracking-widest">
          Campaign buckets (after combo + threshold + bucket-budget rollup)
        </div>
        <div className="divide-y divide-[#2e2e2e]">
          {sorted.map(b => {
            const count = Number(b.contact_count);
            const pct = total > 0 ? (count / total) * 100 : 0;
            const barWidth = max > 0 ? (count / max) * 100 : 0;
            const name: string = b.bucket_name;
            const isGeneral = RESERVED_NAMES.has(name.toLowerCase());
            // Bucket level: combo (sector + sub-identity) > sub-identity > identity > general
            const isSpec = !isGeneral && specNames.has(name);
            const isIdentity = !isGeneral && identityNames.has(name) && !isSpec;
            const isCombo = !isSpec && !isIdentity && !isGeneral
                && Array.from(specNames).some(s => name.endsWith(' ' + s));
            const levelLabel = isCombo ? 'sector × sub-identity'
                : isSpec ? 'sub_identity'
                : isIdentity ? 'identity (rolled up)'
                : isGeneral ? 'general (catch-all)'
                : 'rolled up';
            const levelColor = isCombo ? 'text-[#3ecf8e]'
                : isSpec ? 'text-[#3ecf8e]'
                : isIdentity ? 'text-blue-400'
                : 'text-amber-400';
            return (
              <div key={name} className="px-4 py-3 hover:bg-white/[0.02]">
                <div className="flex items-center gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between mb-1.5">
                      <div className="flex items-center gap-2 min-w-0">
                        <span className={`text-xs font-bold ${isGeneral ? 'text-amber-400' : 'text-white'} truncate`}>
                          {name}
                        </span>
                        <span className={`text-[9px] font-bold uppercase tracking-widest ${levelColor} bg-white/[0.03] border border-current/30 px-1.5 py-0.5 rounded shrink-0`}>
                          {levelLabel}
                        </span>
                      </div>
                      <span className="text-[10px] font-mono text-gray-400 shrink-0 ml-2">
                        {count.toLocaleString()} <span className="text-gray-600">({pct.toFixed(1)}%)</span>
                      </span>
                    </div>
                    <div className="h-1.5 bg-[#1c1c1c] rounded-full overflow-hidden">
                      <div
                        className={`h-full transition-all ${isGeneral ? 'bg-amber-500/70' : isIdentity ? 'bg-blue-500/70' : 'bg-[#3ecf8e]'}`}
                        style={{ width: `${barWidth}%` }}
                      />
                    </div>
                    {(() => {
                      const sectors = sectorByBucket.get(b.bucket_name) || [];
                      if (sectors.length === 0) return null;
                      const top = sectors.slice(0, 3);
                      return (
                        <div className="mt-1.5 flex flex-wrap gap-1 text-[9px]">
                          <span className="text-gray-600 uppercase tracking-widest font-bold">Sectors:</span>
                          {top.map(s => (
                            <span key={s.sector} className="text-gray-400 font-mono">
                              {s.sector} <span className="text-gray-600">({s.count.toLocaleString()})</span>
                            </span>
                          ))}
                          {sectors.length > 3 && <span className="text-gray-600">+{sectors.length - 3} more</span>}
                        </div>
                      );
                    })()}
                  </div>
                  <button
                    onClick={() => downloadCsv(b.bucket_name)}
                    disabled={exportingBucket === b.bucket_name}
                    className="px-2 py-1 rounded-md text-[10px] font-bold bg-[#1c1c1c] border border-[#2e2e2e] text-gray-300 hover:border-gray-500 hover:text-white flex items-center gap-1 shrink-0"
                  >
                    {exportingBucket === b.bucket_name ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                    CSV
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ───── LIBRARY VIEW ───────────────────────────────────────────────

function BucketingLibrary({ library, onRefresh, onError }: {
  library: LibraryBucket[];
  onRefresh: () => void;
  onError: (msg: string | null) => void;
}) {
  const [editing, setEditing] = useState<LibraryBucket | null>(null);
  const [creating, setCreating] = useState(false);
  const [bulkOpen, setBulkOpen] = useState(false);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [bulkDeleting, setBulkDeleting] = useState(false);

  // Drop selections that point to rows that no longer exist (e.g. after a
  // bulk delete or external refresh). Without this, the toolbar's "N
  // selected" count drifts from what's actually selectable in the table.
  useEffect(() => {
    setSelectedIds(prev => {
      const live = new Set(library.map(b => b.id));
      let changed = false;
      const next = new Set<string>();
      for (const id of prev) {
        if (live.has(id)) next.add(id);
        else changed = true;
      }
      return changed ? next : prev;
    });
  }, [library]);

  const allIds = library.map(b => b.id);
  const allSelected = allIds.length > 0 && allIds.every(id => selectedIds.has(id));
  const someSelected = selectedIds.size > 0 && !allSelected;

  const toggleOne = (id: string) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    setSelectedIds(prev => prev.size === allIds.length && allIds.every(id => prev.has(id))
      ? new Set()
      : new Set(allIds));
  };

  const onArchive = async (id: string, archived: boolean) => {
    onError(null);
    try {
      const res = await fetch(`/api/bucketing/library/${encodeURIComponent(id)}/archive`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ archived })
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    }
  };

  const onDelete = async (id: string) => {
    if (!confirm('Delete this library bucket permanently?')) return;
    try {
      const res = await fetch(`/api/bucketing/library/${encodeURIComponent(id)}`, { method: 'DELETE' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    }
  };

  const onBulkDelete = async () => {
    const ids = Array.from(selectedIds);
    if (ids.length === 0) return;
    if (!confirm(`Delete ${ids.length} library bucket${ids.length === 1 ? '' : 's'} permanently? This cannot be undone.`)) return;
    setBulkDeleting(true);
    onError(null);
    try {
      const res = await fetch('/api/bucketing/library/bulk-delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids })
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
      setSelectedIds(new Set());
      onRefresh();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setBulkDeleting(false);
    }
  };

  return (
    <div className="space-y-3">
      <div className="flex justify-end gap-2">
        <button
          onClick={() => setBulkOpen(true)}
          className="px-3 py-1.5 rounded text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e] flex items-center gap-1"
        >
          <Upload className="w-3 h-3" /> Bulk import
        </button>
        <button
          onClick={() => setCreating(true)}
          className="px-3 py-1.5 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] flex items-center gap-1"
        >
          <Plus className="w-3 h-3" /> New library bucket
        </button>
      </div>
      {selectedIds.size > 0 && (
        <div className="flex items-center justify-between gap-3 px-4 py-2 rounded-xl border border-red-500/30 bg-red-500/5">
          <span className="text-xs text-red-200 font-bold">
            {selectedIds.size} bucket{selectedIds.size === 1 ? '' : 's'} selected
          </span>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setSelectedIds(new Set())}
              className="px-3 py-1.5 rounded text-[10px] font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e]"
            >
              Clear
            </button>
            <button
              onClick={onBulkDelete}
              disabled={bulkDeleting}
              className="px-3 py-1.5 rounded text-[10px] font-bold bg-red-500/20 text-red-200 border border-red-500/40 hover:bg-red-500/30 disabled:opacity-50 flex items-center gap-1"
            >
              {bulkDeleting ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
              Delete selected
            </button>
          </div>
        </div>
      )}
      {bulkOpen && (
        <LibraryBulkImport
          onCancel={() => setBulkOpen(false)}
          onDone={() => { setBulkOpen(false); onRefresh(); }}
          onError={onError}
        />
      )}
      {(creating || editing) && (
        <LibraryBucketEditor
          existing={editing}
          onCancel={() => { setCreating(false); setEditing(null); }}
          onSaved={() => { setCreating(false); setEditing(null); onRefresh(); }}
          onError={onError}
        />
      )}
      {library.length === 0 ? (
        <div className="border border-[#2e2e2e] rounded-xl p-12 text-center bg-[#0e0e0e]">
          <BookMarked className="w-10 h-10 text-gray-700 mx-auto mb-3" />
          <p className="text-sm font-bold text-gray-400">Library is empty</p>
          <p className="text-[11px] text-gray-600 mt-1">Save useful buckets from completed runs to reuse them across campaigns.</p>
        </div>
      ) : (() => {
        // Group buckets by primary_identity so the table reads as
        // "Identity A → bucket 1, bucket 2 …" instead of a flat list.
        // Buckets without an identity (General / Disqualified or any orphan)
        // get bucketed under "—" and rendered last.
        const groups = new Map<string, LibraryBucket[]>();
        for (const b of library) {
          const key = ((b.primary_identity || b.direct_ancestor || '') as string).trim() || '—';
          if (!groups.has(key)) groups.set(key, []);
          groups.get(key)!.push(b);
        }
        const orderedGroups = Array.from(groups.entries()).sort((a, b) => {
          if (a[0] === '—' && b[0] !== '—') return 1;
          if (a[0] !== '—' && b[0] === '—') return -1;
          return a[0].localeCompare(b[0]);
        });

        return (
          <div className="border border-[#2e2e2e] rounded-xl overflow-hidden bg-[#0e0e0e]">
            <table className="w-full text-[11px]">
              <thead className="bg-[#0e0e0e]">
                <tr className="border-b border-[#2e2e2e] text-[9px] font-bold text-gray-500 uppercase tracking-wider">
                  <th className="px-3 py-3 text-left w-[34px]">
                    <input
                      type="checkbox"
                      checked={allSelected}
                      ref={el => { if (el) el.indeterminate = someSelected; }}
                      onChange={toggleAll}
                      className="w-3.5 h-3.5 align-middle"
                      title={allSelected ? 'Clear all' : 'Select all'}
                    />
                  </th>
                  <th className="px-5 py-3 text-left">Primary identity / Bucket</th>
                  <th className="px-5 py-3 text-right">Used</th>
                  <th className="px-5 py-3 text-right">Last used</th>
                  <th className="px-5 py-3 text-right"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[#2e2e2e]">
                {orderedGroups.map(([identity, buckets]) => {
                  const sortedBuckets = [...buckets].sort((a, b) => a.bucket_name.localeCompare(b.bucket_name));
                  return (
                    <React.Fragment key={identity}>
                      <tr className="bg-[#161616] border-t border-[#2e2e2e]">
                        <td colSpan={5} className="px-5 py-2">
                          <span className="text-[10px] font-bold text-[#3ecf8e] uppercase tracking-wider">{identity}</span>
                          <span className="ml-2 text-[10px] text-gray-500 font-mono">
                            {buckets.length} bucket{buckets.length === 1 ? '' : 's'}
                          </span>
                        </td>
                      </tr>
                      {sortedBuckets.map(b => (
                        <tr key={b.id} className={`hover:bg-white/[0.02] ${b.archived ? 'opacity-50' : ''} ${selectedIds.has(b.id) ? 'bg-red-500/5' : ''}`}>
                          <td className="px-3 py-3">
                            <input
                              type="checkbox"
                              checked={selectedIds.has(b.id)}
                              onChange={() => toggleOne(b.id)}
                              className="w-3.5 h-3.5 align-middle"
                            />
                          </td>
                          <td className="px-5 py-3 pl-10">
                            <div className="font-bold text-white">{b.bucket_name}</div>
                            {b.description && <div className="text-[10px] text-gray-500 truncate max-w-md">{b.description}</div>}
                          </td>
                          <td className="px-5 py-3 text-right text-gray-300 font-mono">{b.times_used}×</td>
                          <td className="px-5 py-3 text-right text-gray-500">
                            {b.last_used_at ? new Date(b.last_used_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '—'}
                          </td>
                          <td className="px-5 py-3 text-right">
                            <div className="flex items-center justify-end gap-1">
                              <button onClick={() => setEditing(b)} className="p-1.5 rounded-md bg-[#1c1c1c] border border-[#2e2e2e] text-gray-500 hover:text-white" title="Edit">
                                <Edit3 className="w-3 h-3" />
                              </button>
                              <button onClick={() => onArchive(b.id, !b.archived)} className="p-1.5 rounded-md bg-[#1c1c1c] border border-[#2e2e2e] text-gray-500 hover:text-amber-400" title={b.archived ? 'Unarchive' : 'Archive'}>
                                <Archive className="w-3 h-3" />
                              </button>
                              <button onClick={() => onDelete(b.id)} className="p-1.5 rounded-md bg-[#1c1c1c] border border-[#2e2e2e] text-gray-500 hover:text-red-400" title="Delete">
                                <Trash2 className="w-3 h-3" />
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        );
      })()}
    </div>
  );
}

function LibraryBucketEditor({ existing, onCancel, onSaved, onError }: {
  existing: LibraryBucket | null;
  onCancel: () => void;
  onSaved: () => void;
  onError: (msg: string | null) => void;
}) {
  const [spec, setSpec] = useState(existing?.bucket_name || '');
  const [identity, setIdentity] = useState(existing?.primary_identity || existing?.direct_ancestor || '');
  const [desc, setDesc] = useState(existing?.description || '');
  const [include, setInclude] = useState((existing?.include_terms || []).join(', '));
  const [exclude, setExclude] = useState((existing?.exclude_terms || []).join(', '));
  const [examples, setExamples] = useState((existing?.example_strings || []).join('\n'));
  const [busy, setBusy] = useState(false);
  const [nameError, setNameError] = useState<string | null>(null);

  const originalName = existing?.bucket_name || '';

  const save = async () => {
    if (!spec.trim()) return;
    setBusy(true);
    setNameError(null);
    onError(null);
    try {
      const trimmedName = spec.trim();
      // Rename first when the name changed on an existing bucket. The
      // upsert path below is keyed on bucket_name — without a separate
      // rename call it would either CREATE a second row (new name) or
      // 409 on the unique index.
      if (existing && trimmedName !== originalName) {
        const renameRes = await fetch(`/api/bucketing/library/${encodeURIComponent(existing.id)}/rename`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: trimmedName })
        });
        if (!renameRes.ok) {
          const data = await renameRes.json().catch(() => ({}));
          if (renameRes.status === 409) {
            setNameError(data.error || 'A bucket with this name already exists');
            setBusy(false);
            return;
          }
          throw new Error(data.error || `Rename failed (${renameRes.status})`);
        }
      }
      const payload = {
        bucket_name: trimmedName,
        primary_identity: identity.trim(),
        description: desc.trim(),
        include_terms: include.split(',').map(s => s.trim()).filter(Boolean),
        exclude_terms: exclude.split(',').map(s => s.trim()).filter(Boolean),
        example_strings: examples.split('\n').map(s => s.trim()).filter(Boolean)
      };
      const res = await fetch('/api/bucketing/library', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
      onSaved();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="border border-[#3ecf8e]/30 rounded-xl bg-[#0e0e0e] p-4 space-y-3">
      <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest">{existing ? 'Edit' : 'New'} library bucket</div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
        <div>
          <input value={spec} onChange={e => { setSpec(e.target.value); if (nameError) setNameError(null); }} placeholder="Sub-Identity (Layer 2, unique)"
            className={`w-full px-3 py-2 bg-[#1c1c1c] border rounded text-xs text-white placeholder-gray-600 focus:outline-none ${nameError ? 'border-red-500/60 focus:border-red-500' : 'border-[#2e2e2e] focus:border-[#3ecf8e]'}`} />
          {nameError && <div className="text-[10px] text-red-300 mt-1">{nameError}</div>}
        </div>
        <input value={identity} onChange={e => setIdentity(e.target.value)} placeholder="Primary identity (Layer 1)"
          className="px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]" />
      </div>
      <input value={desc} onChange={e => setDesc(e.target.value)} placeholder="Description"
        className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]" />
      <input value={include} onChange={e => setInclude(e.target.value)} placeholder="Include keywords (comma-separated)"
        className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]" />
      <input value={exclude} onChange={e => setExclude(e.target.value)} placeholder="Exclude keywords (comma-separated)"
        className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]" />
      <textarea value={examples} onChange={e => setExamples(e.target.value)} placeholder="Example classification strings (one per line)" rows={4}
        className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e] font-mono" />
      <div className="flex justify-end gap-2">
        <button onClick={onCancel} className="px-3 py-1.5 rounded text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e]">Cancel</button>
        <button onClick={save} disabled={busy || !spec.trim()} className="px-3 py-1.5 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1">
          {busy ? <Loader2 className="w-3 h-3 animate-spin" /> : <CheckCircle2 className="w-3 h-3" />}
          Save
        </button>
      </div>
    </div>
  );
}

// Local StatCard so this file has no dependency on App.tsx internals.
function LibraryBulkImport({ onCancel, onDone, onError }: {
  onCancel: () => void;
  onDone: () => void;
  onError: (msg: string | null) => void;
}) {
  const [text, setText] = useState('');
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ saved: number; skipped: { name: string; reason: string }[] } | null>(null);

  const sample = `# One bucket per line. Optional: spec | identity | description
# Empty lines and # comments are ignored.

SEO Agency | Agency | Search engine optimization for B2B
Performance Marketing Agency | Agency
Branding & Creative Agency | Agency
Private Equity Firm | Financial Services
Venture Capital Fund | Financial Services
Managed IT Services | IT Services
MarTech SaaS | Software & SaaS`;

  const importNow = async () => {
    setBusy(true);
    onError(null);
    try {
      const res = await fetch('/api/bucketing/library/bulk-import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
      setResult(data);
      if (data.saved > 0) onDone();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="border border-[#3ecf8e]/30 rounded-xl bg-[#0e0e0e] p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest">Bulk import library buckets</div>
        <button onClick={onCancel} className="text-gray-500 hover:text-white"><X className="w-4 h-4" /></button>
      </div>
      <p className="text-[11px] text-gray-500">
        One bucket per line. Use <code className="bg-[#1c1c1c] px-1 rounded text-[10px]">|</code> to separate optional fields:
        <code className="ml-1 bg-[#1c1c1c] px-1 rounded text-[10px]">spec | primary_identity | description</code>.
        Names alone are fine — you can edit identity later. Existing names are upserted (no duplicates created).
      </p>
      <textarea
        value={text}
        onChange={e => setText(e.target.value)}
        placeholder={sample}
        rows={12}
        className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-700 focus:outline-none focus:border-[#3ecf8e] font-mono"
      />
      {result && (
        <div className="text-[11px]">
          <div className="text-[#3ecf8e] font-bold">Saved {result.saved} bucket(s).</div>
          {result.skipped.length > 0 && (
            <details className="mt-1">
              <summary className="text-amber-400 cursor-pointer">Skipped {result.skipped.length}</summary>
              <ul className="text-amber-400/80 ml-4 mt-1">
                {result.skipped.slice(0, 20).map((s, i) => (
                  <li key={i}><span className="font-mono">{s.name}</span> — {s.reason}</li>
                ))}
                {result.skipped.length > 20 && <li className="text-gray-500">…and {result.skipped.length - 20} more</li>}
              </ul>
            </details>
          )}
        </div>
      )}
      <div className="flex justify-end gap-2">
        <button onClick={onCancel} className="px-3 py-1.5 rounded text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e]">Close</button>
        <button
          onClick={importNow}
          disabled={busy || !text.trim()}
          className="px-3 py-1.5 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1"
        >
          {busy ? <Loader2 className="w-3 h-3 animate-spin" /> : <Upload className="w-3 h-3" />}
          Import
        </button>
      </div>
    </div>
  );
}

function StatCard({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-6 shadow-sm">
      <p className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">{label}</p>
      <p className={`text-3xl font-bold ${color}`}>{value}</p>
    </div>
  );
}

// ───── Phase 1a: AI-proposed tag review panel ─────────────────────

type TaxKind = 'identities' | 'sub_identities' | 'sectors';

function Phase1aProposedTagsPanel({ runId, onError, recalcing, onFinalize, finalizing, finalizeProgress, lastFinalize }: {
  runId: string;
  onError: (m: string | null) => void;
  // Surfaced to disable batch buttons while a parent-driven recalc is
  // in progress, so the user can't queue a second recalc on top.
  recalcing?: boolean;
  // Finalize: re-tag every still-orphan row against the library only.
  // Renders the button and surfaces the last-run summary inside this
  // panel so the action lives next to the proposals it operates on.
  onFinalize?: () => Promise<void> | void;
  finalizing?: boolean;
  // Live progress while the finalize POST is in flight (parent polls
  // bucketing_runs.progress at 1 Hz). null when not finalizing.
  finalizeProgress?: { current: number; total: number; note?: string } | null;
  lastFinalize?: { at: Date; rerouted: number; nullified: number; failed: number } | null;
}) {
  const [proposed, setProposed] = useState<{ identities: any[]; sub_identities: any[]; sectors: any[] } | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [busyAllKind, setBusyAllKind] = useState<string | null>(null);
  const [bulkProgress, setBulkProgress] = useState<{ done: number; total: number } | null>(null);
  // Per-kind selection sets — drives the "Apply selected (N)" buttons.
  // Stored as Sets keyed by item name; reset whenever proposals change.
  const [selected, setSelected] = useState<Record<TaxKind, Set<string>>>({
    identities: new Set(), sub_identities: new Set(), sectors: new Set()
  });
  // Per-kind sets of items the user accepted this session. /proposed-tags
  // keeps returning accepted items until a recalc/finalize clears the
  // is_new_* flags on bucket_industry_map, so we need local memory to
  // mark them as Accepted in the UI. Persisted in sessionStorage keyed
  // by runId so a tab refresh doesn't lose the visual state.
  const acceptedStorageKey = `bucketing.acceptedTags.${runId}`;
  const [accepted, setAccepted] = useState<Record<TaxKind, Set<string>>>(() => {
    try {
      const raw = sessionStorage.getItem(acceptedStorageKey);
      if (raw) {
        const parsed = JSON.parse(raw);
        return {
          identities: new Set(parsed.identities || []),
          sub_identities: new Set(parsed.sub_identities || []),
          sectors: new Set(parsed.sectors || [])
        };
      }
    } catch { /* fall through */ }
    return { identities: new Set(), sub_identities: new Set(), sectors: new Set() };
  });
  const persistAccepted = useCallback((next: Record<TaxKind, Set<string>>) => {
    try {
      sessionStorage.setItem(acceptedStorageKey, JSON.stringify({
        identities: Array.from(next.identities),
        sub_identities: Array.from(next.sub_identities),
        sectors: Array.from(next.sectors)
      }));
    } catch { /* quota — non-fatal */ }
  }, [acceptedStorageKey]);
  const markAccepted = useCallback((kind: TaxKind, names: string[]) => {
    setAccepted(prev => {
      const next = { ...prev, [kind]: new Set([...prev[kind], ...names]) };
      persistAccepted(next);
      return next;
    });
  }, [persistAccepted]);

  const refresh = useCallback(async () => {
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(runId)}/proposed-tags`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
      setProposed(data);
      // Drop any selection entries whose item is no longer in the panel
      // (e.g. accepted in a prior pass). Avoids stale checkmarks.
      setSelected(prev => {
        const next: Record<TaxKind, Set<string>> = { identities: new Set(), sub_identities: new Set(), sectors: new Set() };
        for (const k of ['identities', 'sub_identities', 'sectors'] as TaxKind[]) {
          const live = new Set((data[k] || []).map((p: any) => p.name));
          for (const n of prev[k]) if (live.has(n)) next[k].add(n);
        }
        return next;
      });
    } catch (e: any) { onError(e.message); }
  }, [runId, onError]);

  useEffect(() => { refresh(); }, [refresh]);

  const acceptOne = async (kind: TaxKind, name: string, parent?: string): Promise<void> => {
    const body: any = { name, created_by: 'ai' };
    if (kind === 'sub_identities' && parent) body.parent_identity = parent;
    const res = await fetch(`/api/bucketing/taxonomy/${kind}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
  };

  const accept = async (kind: TaxKind, name: string, parent?: string) => {
    setBusyKey(`${kind}:${name}`);
    onError(null);
    try {
      await acceptOne(kind, name, parent);
      // Mark accepted FIRST so the refresh-driven re-render reflects the
      // accepted state immediately (otherwise the item flickers back to
      // its un-accepted style for a tick).
      markAccepted(kind, [name]);
      await refresh();
    } catch (e: any) { onError(e.message); }
    finally { setBusyKey(null); }
  };

  // Generic batch-apply for a set of items (used by Accept-all + Apply-selected).
  // For sub-identities, parent identities must exist first — we accept any
  // selected/proposed parents before the children to avoid FK-by-name fails.
  const applyBatch = async (kind: TaxKind, items: any[]) => {
    if (!proposed || items.length === 0) return;
    setBusyAllKind(kind);
    setBulkProgress({ done: 0, total: items.length });
    onError(null);
    let done = 0;
    let firstError: string | null = null;
    if (kind === 'sub_identities') {
      const neededParents = new Set(items.map(p => p.parent).filter(Boolean));
      for (const ip of (proposed.identities || [])) {
        if (neededParents.has(ip.name)) {
          try { await acceptOne('identities', ip.name); } catch { /* best-effort */ }
        }
      }
    }
    const succeeded: string[] = [];
    for (const p of items) {
      try {
        await acceptOne(kind, p.name, p.parent);
        succeeded.push(p.name);
      } catch (e: any) {
        if (!firstError) firstError = e.message;
      }
      done += 1;
      setBulkProgress({ done, total: items.length });
    }
    setBusyAllKind(null);
    setBulkProgress(null);
    if (firstError) onError(`Some ${kind} couldn't be added: ${firstError}`);
    if (succeeded.length > 0) markAccepted(kind, succeeded);
    setSelected(prev => ({ ...prev, [kind]: new Set() }));
    await refresh();
    // No auto-recalc here — the user runs that explicitly via the
    // Finalize button (or the manual Recalculate next to the Discovered
    // Sub-Identities header). Auto-firing on every batch was burning
    // LLM tokens for partial accepts.
  };

  const applySelected = (kind: TaxKind) => {
    if (!proposed) return;
    const want = selected[kind];
    const items = proposed[kind].filter((p: any) => want.has(p.name));
    return applyBatch(kind, items);
  };

  const toggleSelect = (kind: TaxKind, name: string) => {
    setSelected(prev => {
      const s = new Set(prev[kind]);
      s.has(name) ? s.delete(name) : s.add(name);
      return { ...prev, [kind]: s };
    });
  };

  const toggleSelectAll = (kind: TaxKind) => {
    if (!proposed) return;
    // Exclude already-accepted items — selecting them would no-op against
    // the library (unique-constraint conflict) and clutter the count.
    const all = proposed[kind]
      .map((p: any) => p.name as string)
      .filter((n: string) => !accepted[kind].has(n));
    setSelected(prev => {
      const cur = prev[kind];
      const nextSet = cur.size === all.length ? new Set<string>() : new Set(all);
      return { ...prev, [kind]: nextSet };
    });
  };

  if (!proposed) return null;
  const total = proposed.identities.length + proposed.sub_identities.length + proposed.sectors.length;
  if (total === 0) return null;

  return (
    <div className="border border-amber-500/30 rounded-xl bg-amber-500/5 p-4">
      <div className="flex items-center justify-between gap-2 mb-2">
        <div className="text-[10px] font-bold text-amber-400 uppercase tracking-widest">
          AI-proposed taxonomy additions ({total})
        </div>
        {recalcing && (
          <div className="flex items-center gap-1.5 text-[10px] text-amber-200/70">
            <Loader2 className="w-3 h-3 animate-spin" />
            <span>Recalculating taxonomy with updated library…</span>
          </div>
        )}
      </div>
      <p className="text-[11px] text-gray-400 mb-3">
        The tagger proposed entries that aren't in the library. Tick the ones you want to keep (use the column header checkbox to tick all at once) and click "Apply selected", or use the per-row "Accept" button. Accepted entries are saved to the library; click "Finalize taxonomy" once you're done so the remaining proposals get re-tagged against the library only.
      </p>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {(['identities','sub_identities','sectors'] as const).map(kind => {
          const items = proposed[kind];
          const sel = selected[kind];
          // "All selected" is relative to the un-accepted items only —
          // accepted items aren't selectable and shouldn't gate the header.
          const selectableCount = items.filter((p: any) => !accepted[kind].has(p.name)).length;
          const allSelected = selectableCount > 0 && sel.size === selectableCount;
          const someSelected = sel.size > 0;
          const acceptedCount = items.filter((p: any) => accepted[kind].has(p.name)).length;
          // Layer-explanation tooltips. Native title= attribute so the
          // browser's built-in tooltip handles hover (no extra deps).
          const tooltip = TAXONOMY_LAYER_HELP[kind];
          return (
            <div key={kind}>
              <div className="flex flex-wrap items-center justify-between gap-1 mb-1.5">
                <label
                  className="flex items-center gap-1.5 text-[10px] font-bold text-gray-400 uppercase cursor-pointer select-none"
                  title={tooltip}
                >
                  <input
                    type="checkbox"
                    checked={allSelected}
                    ref={el => { if (el) el.indeterminate = someSelected && !allSelected; }}
                    onChange={() => toggleSelectAll(kind)}
                    disabled={selectableCount === 0}
                    className="w-3 h-3 accent-[#3ecf8e]"
                  />
                  <span>
                    {kind} ({items.length})
                    {acceptedCount > 0 && <span className="text-emerald-400/80 font-normal normal-case"> · {acceptedCount} accepted</span>}
                  </span>
                  <span
                    className="text-gray-600 hover:text-amber-400 text-[11px] leading-none"
                    title={tooltip}
                    aria-label={`What is ${kind}?`}
                  >ⓘ</span>
                </label>
                {items.length > 0 && (
                  <button
                    onClick={() => applySelected(kind)}
                    disabled={busyAllKind !== null || sel.size === 0 || !!recalcing}
                    className="px-2 py-0.5 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-30"
                    title={`Add the checked ${kind} to the library — use the column header checkbox to tick them all at once`}
                  >
                    {busyAllKind === kind && bulkProgress
                      ? `${bulkProgress.done}/${bulkProgress.total}…`
                      : `Apply selected (${sel.size})`}
                  </button>
                )}
              </div>
              <ul className="space-y-1">
                {items.map((p: any) => {
                  const key = `${kind}:${p.name}`;
                  const isChecked = sel.has(p.name);
                  const isAccepted = accepted[kind].has(p.name);
                  return (
                    <li
                      key={key}
                      className={`flex items-center justify-between gap-2 px-2 py-1.5 border rounded text-[11px] transition-colors ${
                        isAccepted ? 'bg-emerald-500/10 border-emerald-500/40'
                          : isChecked ? 'bg-[#3ecf8e]/10 border-[#3ecf8e]/40'
                          : 'bg-[#1c1c1c] border-[#2e2e2e]'
                      }`}
                      title={isAccepted ? 'Accepted in this session — added to the library. Run Finalize Taxonomy / Recalculate to clear it from this list.' : undefined}
                    >
                      <input
                        type="checkbox"
                        checked={isChecked}
                        onChange={() => toggleSelect(kind, p.name)}
                        disabled={isAccepted}
                        className="w-3 h-3 accent-[#3ecf8e] shrink-0 disabled:opacity-30"
                      />
                      <div className="min-w-0 flex-1">
                        <div className={`font-bold truncate ${isAccepted ? 'text-emerald-300' : 'text-white'}`} title={p.name}>{p.name}</div>
                        <div className="text-[10px] text-gray-500 truncate" title={p.samples?.join(' · ')}>
                          {p.count}× · ex: {(p.samples || []).slice(0, 2).join(' · ')}
                        </div>
                        {kind === 'sub_identities' && p.parent && (
                          <div className="text-[9px] text-gray-600">under {p.parent}</div>
                        )}
                      </div>
                      {isAccepted ? (
                        <span
                          className="shrink-0 px-2 py-1 rounded text-[10px] font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/40 flex items-center gap-1"
                          title="Already added to the taxonomy library in this session"
                        >
                          <CheckCircle2 className="w-3 h-3" />
                          Accepted
                        </span>
                      ) : (
                        <button
                          onClick={() => accept(kind, p.name, p.parent)}
                          disabled={busyKey === key}
                          className="shrink-0 px-2 py-1 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50"
                        >
                          {busyKey === key ? '…' : 'Accept'}
                        </button>
                      )}
                    </li>
                  );
                })}
                {items.length === 0 && <li className="text-[10px] text-gray-600 italic">none</li>}
              </ul>
            </div>
          );
        })}
      </div>
      {onFinalize && (
        <div className="mt-4 pt-3 border-t border-amber-500/20">
          <div className="flex items-start justify-between gap-3 flex-wrap">
            <div className="flex-1 min-w-0">
              <div className="text-[11px] font-bold text-amber-200">
                Finalize taxonomy — re-tag remaining proposals against the library only
              </div>
              <div className="text-[10px] text-gray-400 mt-0.5">
                Forces every still-orphan tag to a library entry (or null → General). No new entries are proposed in this pass. Run this once you're done accepting AI suggestions, BEFORE Apply &amp; Assign.
              </div>
              {lastFinalize && !finalizing && (
                <div className="text-[10px] text-gray-500 mt-1.5 font-mono">
                  Last finalize {lastFinalize.at.toLocaleTimeString()} · {lastFinalize.rerouted} → library, {lastFinalize.nullified} → General{lastFinalize.failed > 0 ? `, ${lastFinalize.failed} batch failures` : ''}
                </div>
              )}
            </div>
            <button
              onClick={onFinalize}
              disabled={!!finalizing || !!recalcing}
              className="shrink-0 px-3 py-1.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-200 border border-amber-500/40 hover:bg-amber-500/30 disabled:opacity-50 flex items-center gap-1"
            >
              {finalizing ? <Loader2 className="w-3 h-3 animate-spin" /> : <CheckCircle2 className="w-3 h-3" />}
              {finalizing ? 'Finalizing…' : 'Finalize taxonomy'}
            </button>
          </div>
          {finalizing && (() => {
            const cur = finalizeProgress?.current || 0;
            const tot = finalizeProgress?.total || 0;
            const pct = tot > 0 ? Math.min(100, Math.round((cur / tot) * 100)) : null;
            return (
              <div className="mt-3">
                <div className="flex items-center justify-between text-[10px] font-mono mb-1">
                  <span className="text-amber-200/80">
                    {finalizeProgress?.note || 'Re-tagging orphan industries against the library…'}
                  </span>
                  <span className="text-gray-400">
                    {cur.toLocaleString()}{tot > 0 ? ` / ${tot.toLocaleString()}` : ''}{pct !== null ? ` · ${pct}%` : ''}
                  </span>
                </div>
                <div className="h-1.5 bg-[#1c1c1c] rounded overflow-hidden">
                  <div
                    className="h-full bg-amber-400/70 transition-all duration-300"
                    style={{ width: pct !== null ? `${pct}%` : '15%' }}
                  />
                </div>
              </div>
            );
          })()}
        </div>
      )}
    </div>
  );
}

function Phase1aQAQueuePanel({ runId, onError }: { runId: string; onError: (m: string | null) => void }) {
  const [queue, setQueue] = useState<any[] | null>(null);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    fetch(`/api/bucketing/runs/${encodeURIComponent(runId)}/qa-queue`)
      .then(r => r.json())
      .then(d => { if (!cancelled) setQueue(d.queue || []); })
      .catch(e => onError(e.message));
    return () => { cancelled = true; };
  }, [runId, onError]);

  if (!queue || queue.length === 0) return null;

  return (
    <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e]">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full px-4 py-3 flex items-center justify-between text-left hover:bg-white/[0.02]"
      >
        <span className="text-[10px] font-bold text-gray-400 uppercase tracking-widest">
          Low-confidence tagger decisions ({queue.length}) — inspection only, all rows still bucketed via fallback
        </span>
        <span className="text-[11px] text-gray-500">{open ? 'Hide' : 'Review'}</span>
      </button>
      {open && (
        <div className="border-t border-[#2e2e2e] max-h-96 overflow-y-auto">
          <table className="w-full text-[11px]">
            <thead className="bg-[#0e0e0e] sticky top-0">
              <tr className="border-b border-[#2e2e2e] text-[9px] font-bold text-gray-500 uppercase tracking-wider">
                <th className="px-4 py-2 text-left">Industry</th>
                <th className="px-4 py-2 text-left">Identity</th>
                <th className="px-4 py-2 text-left">Sub-Identity</th>
                <th className="px-4 py-2 text-left">Sector</th>
                <th className="px-4 py-2 text-right">Conf</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[#2e2e2e]">
              {queue.map((q, i) => (
                <tr key={i} className="hover:bg-white/[0.02]">
                  <td className="px-4 py-2 text-gray-200 max-w-md truncate" title={q.industry_string}>{q.industry_string}</td>
                  <td className="px-4 py-2 text-gray-400">{q.primary_identity || '—'}</td>
                  <td className="px-4 py-2 text-gray-400">{q.sub_identity || '—'}</td>
                  <td className="px-4 py-2 text-gray-400">{q.sector || '—'}</td>
                  <td className="px-4 py-2 text-right text-amber-400 font-mono">{q.confidence != null ? Number(q.confidence).toFixed(2) : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ───── Taxonomy Library: editable Identity / Sub-Identity / Sector lists ─────

interface TaxRow {
  id: string;
  name: string;
  description?: string | null;
  parent_identity?: string | null;
  is_disqualified?: boolean;
  synonyms?: string | null;
  created_by: 'seed' | 'user' | 'ai';
  archived: boolean;
}

function TaxonomyLibrary({ onError }: { onError: (m: string | null) => void }) {
  const [tab, setTab] = useState<'identities' | 'sub_identities' | 'sectors'>('identities');
  const [data, setData] = useState<{ identities: TaxRow[]; sub_identities: TaxRow[]; sectors: TaxRow[] }>({ identities: [], sub_identities: [], sectors: [] });
  const [loading, setLoading] = useState(false);
  const [editing, setEditing] = useState<TaxRow | null>(null);
  const [creating, setCreating] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/bucketing/taxonomy');
      const d = await res.json();
      if (!res.ok) throw new Error(d.error || `Failed (${res.status})`);
      setData(d);
    } catch (e: any) { onError(e.message); }
    finally { setLoading(false); }
  }, [onError]);

  useEffect(() => { refresh(); }, [refresh]);

  const rows = data[tab];

  const saveRow = async (kind: typeof tab, payload: Partial<TaxRow> & { id?: string }) => {
    onError(null);
    try {
      const res = payload.id
        ? await fetch(`/api/bucketing/taxonomy/${kind}/${encodeURIComponent(payload.id)}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          })
        : await fetch(`/api/bucketing/taxonomy/${kind}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ...payload, created_by: 'user' })
          });
      const d = await res.json();
      if (!res.ok) throw new Error(d.error || `Failed (${res.status})`);
      setEditing(null); setCreating(false);
      refresh();
    } catch (e: any) { onError(e.message); }
  };

  const deleteRow = async (kind: typeof tab, id: string) => {
    if (!confirm('Delete permanently? Existing runs are unaffected.')) return;
    try {
      const res = await fetch(`/api/bucketing/taxonomy/${kind}/${encodeURIComponent(id)}`, { method: 'DELETE' });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.error || `Failed (${res.status})`);
      }
      refresh();
    } catch (e: any) { onError(e.message); }
  };

  const archiveRow = async (kind: typeof tab, row: TaxRow) => {
    try {
      const res = await fetch(`/api/bucketing/taxonomy/${kind}/${encodeURIComponent(row.id)}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ archived: !row.archived })
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.error || `Failed (${res.status})`);
      }
      refresh();
    } catch (e: any) { onError(e.message); }
  };

  return (
    <div className="space-y-3">
      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
        <p className="text-[11px] text-gray-400">
          The Phase 1a tagger uses these lists as the allowed set of tags. AI-proposed additions surface in the run review with Accept controls. Editing here is global — affects all future runs.
        </p>
      </div>
      <div className="flex justify-between items-center">
        <div className="flex gap-1">
          {(['identities', 'sub_identities', 'sectors'] as const).map(t => (
            <button
              key={t}
              onClick={() => { setTab(t); setEditing(null); setCreating(false); }}
              className={`px-3 py-1.5 rounded-md text-xs font-bold ${tab === t ? 'bg-[#3ecf8e] text-black' : 'bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e]'}`}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)} ({data[t].length})
            </button>
          ))}
        </div>
        <button
          onClick={() => { setCreating(true); setEditing(null); }}
          className="px-3 py-1.5 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] flex items-center gap-1"
        >
          <Plus className="w-3 h-3" /> New {tab.slice(0, -1)}
        </button>
      </div>

      {(creating || editing) && (
        <TaxonomyEditor
          kind={tab}
          existing={editing}
          identities={data.identities}
          onCancel={() => { setCreating(false); setEditing(null); }}
          onSave={(payload) => saveRow(tab, payload)}
        />
      )}

      <div className="border border-[#2e2e2e] rounded-xl overflow-hidden bg-[#0e0e0e]">
        <table className="w-full text-[11px]">
          <thead className="bg-[#0e0e0e]">
            <tr className="border-b border-[#2e2e2e] text-[9px] font-bold text-gray-500 uppercase tracking-wider">
              <th className="px-5 py-3 text-left">Name</th>
              {tab === 'sub_identities' && <th className="px-5 py-3 text-left">Parent identity</th>}
              <th className="px-5 py-3 text-left">{tab === 'sectors' ? 'Synonyms' : 'Description'}</th>
              {tab === 'identities' && <th className="px-5 py-3 text-center">DQ</th>}
              <th className="px-5 py-3 text-center">Source</th>
              <th className="px-5 py-3 text-right"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[#2e2e2e]">
            {rows.length === 0 && !loading && (
              <tr><td colSpan={6} className="px-5 py-8 text-center text-gray-600 text-[11px] italic">No {tab} yet — run the migration to seed defaults.</td></tr>
            )}
            {rows.map(r => (
              <tr key={r.id} className={`hover:bg-white/[0.02] ${r.archived ? 'opacity-50' : ''}`}>
                <td className="px-5 py-2 font-bold text-white">{r.name}</td>
                {tab === 'sub_identities' && <td className="px-5 py-2 text-gray-300">{r.parent_identity || '—'}</td>}
                <td className="px-5 py-2 text-gray-400 max-w-md truncate">
                  {tab === 'sectors' ? (r.synonyms || '—') : (r.description || '—')}
                </td>
                {tab === 'identities' && (
                  <td className="px-5 py-2 text-center">{r.is_disqualified ? <span className="text-red-400 font-bold">DQ</span> : ''}</td>
                )}
                <td className="px-5 py-2 text-center">
                  <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${
                    r.created_by === 'ai' ? 'bg-amber-500/20 text-amber-400' :
                    r.created_by === 'seed' ? 'bg-gray-500/20 text-gray-400' :
                    'bg-blue-500/20 text-blue-400'
                  }`}>{r.created_by}</span>
                </td>
                <td className="px-5 py-2 text-right">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => { setEditing(r); setCreating(false); }} className="p-1.5 rounded-md bg-[#1c1c1c] border border-[#2e2e2e] text-gray-500 hover:text-white" title="Edit">
                      <Edit3 className="w-3 h-3" />
                    </button>
                    <button onClick={() => archiveRow(tab, r)} className="p-1.5 rounded-md bg-[#1c1c1c] border border-[#2e2e2e] text-gray-500 hover:text-amber-400" title={r.archived ? 'Unarchive' : 'Archive'}>
                      <Archive className="w-3 h-3" />
                    </button>
                    <button onClick={() => deleteRow(tab, r.id)} className="p-1.5 rounded-md bg-[#1c1c1c] border border-[#2e2e2e] text-gray-500 hover:text-red-400" title="Delete">
                      <Trash2 className="w-3 h-3" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TaxonomyEditor({ kind, existing, identities, onCancel, onSave }: {
  kind: 'identities' | 'sub_identities' | 'sectors';
  existing: TaxRow | null;
  identities: TaxRow[];
  onCancel: () => void;
  onSave: (payload: any) => void;
}) {
  const [name, setName] = useState(existing?.name || '');
  const [description, setDescription] = useState(existing?.description || '');
  const [parent, setParent] = useState(existing?.parent_identity || (identities[0]?.name || ''));
  const [synonyms, setSynonyms] = useState(existing?.synonyms || '');
  const [isDq, setIsDq] = useState(!!existing?.is_disqualified);

  return (
    <div className="border border-[#3ecf8e]/40 rounded-xl bg-[#0e0e0e] p-4 space-y-3">
      <div className="text-[10px] font-bold text-[#3ecf8e] uppercase tracking-widest">
        {existing ? 'Edit' : 'Create'} {kind.slice(0, -1)}
      </div>
      <input value={name} onChange={e => setName(e.target.value)} placeholder="Name"
        className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white focus:outline-none focus:border-[#3ecf8e]" />
      {kind === 'sub_identities' && (
        <select value={parent} onChange={e => setParent(e.target.value)}
          className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white focus:outline-none focus:border-[#3ecf8e]">
          {identities.filter(i => !i.archived).map(i => <option key={i.id} value={i.name}>{i.name}</option>)}
        </select>
      )}
      {kind === 'sectors' ? (
        <textarea value={synonyms} onChange={e => setSynonyms(e.target.value)} placeholder="Synonyms / signals (comma-separated)" rows={2}
          className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white focus:outline-none focus:border-[#3ecf8e]" />
      ) : (
        <textarea value={description} onChange={e => setDescription(e.target.value)} placeholder="Description / signals" rows={2}
          className="w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white focus:outline-none focus:border-[#3ecf8e]" />
      )}
      {kind === 'identities' && (
        <label className="flex items-center gap-2 text-[11px] text-gray-300">
          <input type="checkbox" checked={isDq} onChange={e => setIsDq(e.target.checked)} className="rounded" />
          Disqualified — contacts with this identity route to the Disqualified bucket
        </label>
      )}
      <div className="flex justify-end gap-2">
        <button onClick={onCancel} className="px-3 py-1.5 rounded text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e]">Cancel</button>
        <button
          onClick={() => onSave({
            id: existing?.id,
            name: name.trim(),
            description,
            ...(kind === 'sub_identities' ? { parent_identity: parent } : {}),
            ...(kind === 'sectors' ? { synonyms } : {}),
            ...(kind === 'identities' ? { is_disqualified: isDq } : {})
          })}
          disabled={!name.trim()}
          className="px-3 py-1.5 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50"
        >
          {existing ? 'Save' : 'Create'}
        </button>
      </div>
    </div>
  );
}
