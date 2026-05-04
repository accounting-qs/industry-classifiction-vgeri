/**
 * Bucketing UI v2.2 — 4-layer model (identity → specialization → sector → campaign bucket).
 *
 * Five views:
 *   - Index    : past runs + library shortcut
 *   - Setup    : pick lists, name, min_volume, bucket_budget, optional library
 *   - Review   : Phase 1a proposal — observed patterns + specializations grouped
 *                under primary identities, keep/drop/rename/add, threshold preview
 *   - Results  : Phase 1b assignments rolled up to campaign buckets, save-to-library
 *   - Library  : CRUD for reusable specializations across runs
 */

import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Layers, Loader2, AlertCircle, ArrowLeft, Plus, X, Trash2, Download,
  Play, BookMarked, CheckCircle2, Edit3, Archive, Upload, Square, RotateCcw
} from 'lucide-react';
import Papa from 'papaparse';
import type { BucketingRun, BucketProposal, LibraryBucket } from './types';

type BucketingView = 'index' | 'setup' | 'detail' | 'library' | 'taxonomy';

const RESERVED_GENERAL = 'General';
// Recognize legacy names too, in case a run was created before v2.3.
const RESERVED_NAMES = new Set(['general', 'generic', 'disqualified', 'other']);

export function BucketingTab({ importLists }: {
  importLists: { id: string; name: string; contact_count: number; created_at: string; enriched_count?: number }[]
}) {
  const [view, setView] = useState<BucketingView>('index');
  const [runs, setRuns] = useState<BucketingRun[]>([]);
  const [library, setLibrary] = useState<LibraryBucket[]>([]);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
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

  const openRun = (id: string) => { setActiveRunId(id); setView('detail'); };

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
      if (activeRunId === id) { setActiveRunId(null); setActiveRun(null); setView('index'); }
      await refreshRuns();
    } catch (e: any) {
      setError(e.message);
    }
  };

  return (
    <div className="flex-1 overflow-y-auto custom-scrollbar p-6 bg-[#1c1c1c] text-[#ededed]">
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-lg font-bold text-white flex items-center gap-2">
              <Layers className="w-5 h-5 text-[#3ecf8e]" /> Bucketing
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              Phase 1a discovers identities + specializations. Phase 1b matches every contact (identity + specialization + sector). Volume rollup combines into campaign buckets within your bucket budget.
            </p>
          </div>
          <div className="flex gap-2">
            {view !== 'index' && (
              <button
                onClick={() => { setView('index'); setActiveRunId(null); setActiveRun(null); refreshRuns(); }}
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
          <BucketingIndex runs={runs} onOpen={openRun} onDelete={deleteRun} />
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

function BucketingIndex({ runs, onOpen, onDelete }: {
  runs: BucketingRun[];
  onOpen: (id: string) => void;
  onDelete: (id: string) => void;
}) {
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
                <button
                  onClick={e => { e.stopPropagation(); onDelete(r.id); }}
                  className="p-1.5 rounded-md bg-[#1c1c1c] border border-[#2e2e2e] text-gray-500 hover:text-red-400 hover:border-red-500/40"
                  title="Delete run"
                >
                  <Trash2 className="w-3 h-3" />
                </button>
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
// classification strings we've measured (dedup ratio ~1.45). Actual cost
// scales with vocab size, not raw contact count.
const PHASE1A_MODEL_OPTIONS = [
  { id: 'gpt-4.1-mini',     label: 'gpt-4.1-mini (default)', approxCost100k: '~$15–25', note: 'Cheapest. Good quality on structured taxonomy picks.' },
  { id: 'claude-haiku-4-5', label: 'Claude Haiku 4.5',       approxCost100k: '~$40–70', note: 'Slightly stronger on edge-case identities.' },
] as const;
type Phase1aModel = typeof PHASE1A_MODEL_OPTIONS[number]['id'];

function BucketingSetup({ importLists, onCancel, onStart, loading }: {
  importLists: { name: string; contact_count: number; enriched_count?: number }[];
  onCancel: () => void;
  onStart: (p: { name: string; list_names: string[]; apply_identity_dq_cascade: boolean; phase1a_model: Phase1aModel }) => void;
  loading: boolean;
}) {
  const [name, setName] = useState('');
  const [selectedLists, setSelectedLists] = useState<Set<string>>(new Set());
  // Default OFF — trust Sonnet's per-row is_disqualified decision instead of
  // auto-DQ'ing every contact whose identity is library-flagged [DQ].
  const [applyIdentityDqCascade, setApplyIdentityDqCascade] = useState(false);
  const [phase1aModel, setPhase1aModel] = useState<Phase1aModel>('gpt-4.1-mini');

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
              return (
                <button
                  key={l.name}
                  onClick={() => toggleList(l.name)}
                  className={`w-full flex items-center justify-between px-3 py-2 text-left text-xs border-b border-[#2e2e2e] last:border-b-0 transition-colors ${isSel ? 'bg-[#3ecf8e]/10 text-[#3ecf8e]' : 'text-gray-300 hover:bg-white/[0.02]'}`}
                >
                  <span className="flex items-center gap-2">
                    <input type="checkbox" checked={isSel} onChange={() => {}} className="w-3 h-3" />
                    <span className="font-medium">{l.name}</span>
                  </span>
                  <span className="font-mono text-[10px] text-gray-500">
                    {(l.enriched_count || 0).toLocaleString()} enriched / {l.contact_count.toLocaleString()}
                  </span>
                </button>
              );
            })}
          </div>
        )}
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-3 text-[11px] text-gray-400">
        <span className="text-gray-300 font-bold">Bucket sizing &amp; library reuse</span> are set on the next screen, after Phase 1a proposes a taxonomy — that way you size against the actual specializations the LLM found.
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
                    <span className={`text-[11px] font-bold ${isSel ? 'text-[#3ecf8e]' : 'text-gray-200'}`}>{opt.label}</span>
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
              Off (recommended): trust Sonnet's per-row decision — even contacts tagged "Consumer & Retail" stay inviteable when they show a B2B angle. On: any contact tagged with a [DQ] identity routes straight to Disqualified, no exceptions.
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

  return (
    <div className="space-y-3">
      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-5">
        <div className="flex items-center gap-3 mb-3">
          <Loader2 className="w-5 h-5 text-[#3ecf8e] animate-spin" />
          <div className="flex-1 min-w-0">
            <div className="text-sm font-bold text-white">{title}</div>
            <div className="text-[11px] text-gray-500 truncate">{note}</div>
          </div>
          <div className="text-right shrink-0">
            <div className="text-2xl font-bold font-mono text-[#3ecf8e]">
              {pct !== null ? `${pct}%` : '—'}
            </div>
            <div className="text-[10px] text-gray-500 uppercase tracking-widest">
              {etaTxt ? `ETA ${etaTxt}` : (elapsedTxt ? `${elapsedTxt} elapsed` : 'estimating…')}
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
  return <BucketingResults run={run} bucketCounts={bucketCounts} sectorMix={sectorMix} generalBreakdown={generalBreakdown} onError={onError} onLibrarySaved={onLibrarySaved} />;
}

// ───── REVIEW VIEW ──────────────────────────────────────────────

function BucketingReview({ run, library, bucketCounts, onRefresh, onError }: {
  run: BucketingRun;
  library: LibraryBucket[];
  bucketCounts: any[];
  onRefresh: () => void;
  onError: (msg: string | null) => void;
}) {
  const sourceBuckets = run.taxonomy_final?.buckets || run.taxonomy_proposal?.buckets || [];
  const primaryIdentities = (run.taxonomy_final?.primary_identities || run.taxonomy_proposal?.primary_identities) || [];
  const observedPatterns = (run.taxonomy_final?.observed_patterns || run.taxonomy_proposal?.observed_patterns) || [];
  const [kept, setKept] = useState<Set<string>>(new Set(sourceBuckets.map(b => b.characteristic)));
  const [renames, setRenames] = useState<Record<string, string>>({});
  const [adds, setAdds] = useState<{ characteristic: string; primary_identity: string; description: string }[]>([]);
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
      onRefresh();
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
          <span className="font-bold text-white">{primaryIdentities.length}</span> primary identities · <span className="font-bold text-white">{sourceBuckets.length}</span> functional specializations · <span className="font-bold text-white">{run.total_contacts?.toLocaleString() || '?'}</span> contacts
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

      <Phase1aProposedTagsPanel runId={run.id} onError={onError} />
      <Phase1aQAQueuePanel runId={run.id} onError={onError} />

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e]">
        <div className="px-4 py-3 border-b border-[#2e2e2e] text-[10px] font-bold text-gray-500 uppercase tracking-widest flex items-center justify-between">
          <span>Discovered specializations (grouped by primary identity)</span>
          <span className="text-gray-600 normal-case tracking-normal font-normal">
            Phase 1b counts decide the campaign bucket: combo → spec → identity → General.
          </span>
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
      </div>

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-4">
        <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-3">Add custom specialization</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
          <input value={newSpec} onChange={e => setNewSpec(e.target.value)} placeholder="Functional specialization (Layer 2)"
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
              characteristic: newSpec.trim(),
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
                <span><span className="font-bold text-white">{a.characteristic}</span> · under {a.primary_identity} — {a.description}</span>
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
          Selected library buckets short-circuit the Phase 1b LLM via embedding match — keeping a curated set selected is the cheapest way to bucket overlapping lists.
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
                        {b.characteristic || b.bucket_name}
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
          <p className="text-[10px] text-gray-500 italic mt-1">Combos below this fall to spec; specs below to identity; identities below to General.</p>
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
            disabled={busy !== 'none' || kept.size === 0}
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
  const displayName = (b: BucketProposal) => renames[b.characteristic] ?? b.characteristic;
  const baseCountFor = (b: BucketProposal) =>
    countByBucket.get(displayName(b)) ?? countByBucket.get(b.characteristic) ?? 0;

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
        const identCount = specs.reduce((s, l) => s + (kept.has(l.characteristic) ? baseCountFor(l) : 0), 0);
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
                const isKept = kept.has(b.characteristic);
                const count = baseCountFor(b);
                const willRollUp = isKept && count > 0 && count < minVolume;
                return (
                  <div key={b.characteristic} className={`py-2 pl-4 pr-3 ${isKept ? '' : 'opacity-50'}`}>
                    <div className="flex items-start gap-2">
                      <input type="checkbox" checked={isKept} onChange={() => onToggle(b.characteristic)} className="mt-1.5 w-3.5 h-3.5 shrink-0" />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1 flex-wrap">
                          <span className="text-[9px] font-bold uppercase tracking-widest text-gray-500 shrink-0">↳ Specialization</span>
                          <input
                            value={displayName(b)}
                            onChange={e => onRename(b.characteristic, e.target.value)}
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

// ───── RESULTS VIEW ───────────────────────────────────────────────

function BucketingResults({ run, bucketCounts, sectorMix, generalBreakdown, onError, onLibrarySaved }: {
  run: BucketingRun;
  bucketCounts: any[];
  sectorMix: any[];
  generalBreakdown: any[];
  onError: (msg: string | null) => void;
  onLibrarySaved: () => void;
}) {
  const sectorByBucket = new Map<string, { sector: string; count: number }[]>();
  for (const row of sectorMix || []) sectorByBucket.set(row.bucket_name, row.sectors || []);
  const [exportingBucket, setExportingBucket] = useState<string | null>(null);
  const [savingLibrary, setSavingLibrary] = useState(false);
  const [librarySelection, setLibrarySelection] = useState<Set<string>>(new Set());

  const sorted = [...bucketCounts].sort((a, b) => Number(b.contact_count) - Number(a.contact_count));
  const total = sorted.reduce((s, b) => s + Number(b.contact_count), 0);
  const max = sorted.length > 0 ? Number(sorted[0].contact_count) : 1;

  const bucketsInRun = (run.taxonomy_final?.buckets || run.taxonomy_proposal?.buckets || []) as BucketProposal[];
  const identityNames = new Set(((run.taxonomy_final?.primary_identities || run.taxonomy_proposal?.primary_identities) || []).map(p => p.name));
  const specNames = new Set(bucketsInRun.map(b => b.characteristic));

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
        characteristic: r.characteristic || r.bucket_leaf || '',
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
        characteristic_confidence: r.characteristic_confidence,
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
              Save proven specializations to library ({librarySelection.size} selected)
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
              const sel = librarySelection.has(b.characteristic);
              return (
                <button
                  key={b.characteristic}
                  onClick={() => toggleLibSel(b.characteristic)}
                  className={`px-2 py-1 rounded text-[10px] font-bold border transition-colors ${sel ? 'bg-[#3ecf8e]/15 text-[#3ecf8e] border-[#3ecf8e]/40' : 'bg-[#1c1c1c] text-gray-300 border-[#2e2e2e] hover:border-gray-500'}`}
                  title={`under ${b.primary_identity}`}
                >
                  {b.characteristic}
                </button>
              );
            })}
          </div>
        </div>
      )}

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
            // Bucket level: combo (sector + spec) > spec > identity > general
            const isSpec = !isGeneral && specNames.has(name);
            const isIdentity = !isGeneral && identityNames.has(name) && !isSpec;
            const isCombo = !isSpec && !isIdentity && !isGeneral
                && Array.from(specNames).some(s => name.endsWith(' ' + s));
            const levelLabel = isCombo ? 'sector × specialization'
                : isSpec ? 'specialization'
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
      ) : (
        <div className="border border-[#2e2e2e] rounded-xl overflow-hidden bg-[#0e0e0e]">
          <table className="w-full text-[11px]">
            <thead className="bg-[#0e0e0e]">
              <tr className="border-b border-[#2e2e2e] text-[9px] font-bold text-gray-500 uppercase tracking-wider">
                <th className="px-5 py-3 text-left">Functional specialization</th>
                <th className="px-5 py-3 text-left">Primary identity</th>
                <th className="px-5 py-3 text-right">Used</th>
                <th className="px-5 py-3 text-right">Last used</th>
                <th className="px-5 py-3 text-right"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[#2e2e2e]">
              {library.map(b => (
                <tr key={b.id} className={`hover:bg-white/[0.02] ${b.archived ? 'opacity-50' : ''}`}>
                  <td className="px-5 py-3">
                    <div className="font-bold text-white">{b.characteristic || b.bucket_name}</div>
                    {b.description && <div className="text-[10px] text-gray-500 truncate max-w-md">{b.description}</div>}
                  </td>
                  <td className="px-5 py-3 text-gray-300">{b.primary_identity || b.direct_ancestor || '—'}</td>
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
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function LibraryBucketEditor({ existing, onCancel, onSaved, onError }: {
  existing: LibraryBucket | null;
  onCancel: () => void;
  onSaved: () => void;
  onError: (msg: string | null) => void;
}) {
  const [spec, setSpec] = useState(existing?.characteristic || existing?.bucket_name || '');
  const [identity, setIdentity] = useState(existing?.primary_identity || existing?.direct_ancestor || '');
  const [desc, setDesc] = useState(existing?.description || '');
  const [include, setInclude] = useState((existing?.include_terms || []).join(', '));
  const [exclude, setExclude] = useState((existing?.exclude_terms || []).join(', '));
  const [examples, setExamples] = useState((existing?.example_strings || []).join('\n'));
  const [busy, setBusy] = useState(false);

  const save = async () => {
    if (!spec.trim()) return;
    setBusy(true);
    onError(null);
    try {
      const payload = {
        characteristic: spec.trim(),
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
        <input value={spec} onChange={e => setSpec(e.target.value)} placeholder="Functional specialization (Layer 2, unique)"
          disabled={!!existing}
          className="px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e] disabled:opacity-60" />
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

type TaxKind = 'identities' | 'characteristics' | 'sectors';

function Phase1aProposedTagsPanel({ runId, onError }: { runId: string; onError: (m: string | null) => void }) {
  const [proposed, setProposed] = useState<{ identities: any[]; characteristics: any[]; sectors: any[] } | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [busyAllKind, setBusyAllKind] = useState<string | null>(null);
  const [bulkProgress, setBulkProgress] = useState<{ done: number; total: number } | null>(null);
  // Per-kind selection sets — drives the "Apply selected (N)" buttons.
  // Stored as Sets keyed by item name; reset whenever proposals change.
  const [selected, setSelected] = useState<Record<TaxKind, Set<string>>>({
    identities: new Set(), characteristics: new Set(), sectors: new Set()
  });

  const refresh = useCallback(async () => {
    try {
      const res = await fetch(`/api/bucketing/runs/${encodeURIComponent(runId)}/proposed-tags`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
      setProposed(data);
      // Drop any selection entries whose item is no longer in the panel
      // (e.g. accepted in a prior pass). Avoids stale checkmarks.
      setSelected(prev => {
        const next: Record<TaxKind, Set<string>> = { identities: new Set(), characteristics: new Set(), sectors: new Set() };
        for (const k of ['identities', 'characteristics', 'sectors'] as TaxKind[]) {
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
    if (kind === 'characteristics' && parent) body.parent_identity = parent;
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
      await refresh();
    } catch (e: any) { onError(e.message); }
    finally { setBusyKey(null); }
  };

  // Generic batch-apply for a set of items (used by Accept-all + Apply-selected).
  // For characteristics, parent identities must exist first — we accept any
  // selected/proposed parents before the children to avoid FK-by-name fails.
  const applyBatch = async (kind: TaxKind, items: any[]) => {
    if (!proposed || items.length === 0) return;
    setBusyAllKind(kind);
    setBulkProgress({ done: 0, total: items.length });
    onError(null);
    let done = 0;
    let firstError: string | null = null;
    if (kind === 'characteristics') {
      const neededParents = new Set(items.map(p => p.parent).filter(Boolean));
      for (const ip of (proposed.identities || [])) {
        if (neededParents.has(ip.name)) {
          try { await acceptOne('identities', ip.name); } catch { /* best-effort */ }
        }
      }
    }
    for (const p of items) {
      try {
        await acceptOne(kind, p.name, p.parent);
      } catch (e: any) {
        if (!firstError) firstError = e.message;
      }
      done += 1;
      setBulkProgress({ done, total: items.length });
    }
    setBusyAllKind(null);
    setBulkProgress(null);
    if (firstError) onError(`Some ${kind} couldn't be added: ${firstError}`);
    setSelected(prev => ({ ...prev, [kind]: new Set() }));
    await refresh();
  };

  const acceptAll = (kind: TaxKind) => proposed && applyBatch(kind, proposed[kind]);

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
    const all = proposed[kind].map((p: any) => p.name as string);
    setSelected(prev => {
      const cur = prev[kind];
      // If everything is selected, clear; else select all.
      const nextSet = cur.size === all.length ? new Set<string>() : new Set(all);
      return { ...prev, [kind]: nextSet };
    });
  };

  if (!proposed) return null;
  const total = proposed.identities.length + proposed.characteristics.length + proposed.sectors.length;
  if (total === 0) return null;

  return (
    <div className="border border-amber-500/30 rounded-xl bg-amber-500/5 p-4">
      <div className="text-[10px] font-bold text-amber-400 uppercase tracking-widest mb-2">
        AI-proposed taxonomy additions ({total})
      </div>
      <p className="text-[11px] text-gray-400 mb-3">
        The tagger proposed entries that aren't in the library. Tick the ones you want to keep and click "Apply selected", or use "Accept all" / individual "Accept" buttons. Rejecting just means leaving them unchecked — they're already used in this run regardless.
      </p>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {(['identities','characteristics','sectors'] as const).map(kind => {
          const items = proposed[kind];
          const sel = selected[kind];
          const allSelected = items.length > 0 && sel.size === items.length;
          const someSelected = sel.size > 0;
          return (
            <div key={kind}>
              <div className="flex flex-wrap items-center justify-between gap-1 mb-1.5">
                <label className="flex items-center gap-1.5 text-[10px] font-bold text-gray-400 uppercase cursor-pointer select-none">
                  <input
                    type="checkbox"
                    checked={allSelected}
                    ref={el => { if (el) el.indeterminate = someSelected && !allSelected; }}
                    onChange={() => toggleSelectAll(kind)}
                    disabled={items.length === 0}
                    className="w-3 h-3 accent-[#3ecf8e]"
                  />
                  {kind} ({items.length})
                </label>
                {items.length > 0 && (
                  <div className="flex gap-1">
                    <button
                      onClick={() => applySelected(kind)}
                      disabled={busyAllKind !== null || sel.size === 0}
                      className="px-2 py-0.5 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-30"
                      title={`Add only the checked ${kind} to the library`}
                    >
                      {busyAllKind === kind && bulkProgress
                        ? `${bulkProgress.done}/${bulkProgress.total}…`
                        : `Apply selected (${sel.size})`}
                    </button>
                    <button
                      onClick={() => acceptAll(kind)}
                      disabled={busyAllKind !== null}
                      className="px-2 py-0.5 rounded text-[10px] font-bold bg-[#2e2e2e] text-gray-200 hover:bg-[#3e3e3e] disabled:opacity-50"
                      title={`Add every proposed ${kind.slice(0, -1)} to the library`}
                    >
                      Accept all ({items.length})
                    </button>
                  </div>
                )}
              </div>
              <ul className="space-y-1">
                {items.map((p: any) => {
                  const key = `${kind}:${p.name}`;
                  const isChecked = sel.has(p.name);
                  return (
                    <li
                      key={key}
                      className={`flex items-center justify-between gap-2 px-2 py-1.5 border rounded text-[11px] transition-colors ${
                        isChecked ? 'bg-[#3ecf8e]/10 border-[#3ecf8e]/40' : 'bg-[#1c1c1c] border-[#2e2e2e]'
                      }`}
                    >
                      <input
                        type="checkbox"
                        checked={isChecked}
                        onChange={() => toggleSelect(kind, p.name)}
                        className="w-3 h-3 accent-[#3ecf8e] shrink-0"
                      />
                      <div className="min-w-0 flex-1">
                        <div className="font-bold text-white truncate" title={p.name}>{p.name}</div>
                        <div className="text-[10px] text-gray-500 truncate" title={p.samples?.join(' · ')}>
                          {p.count}× · ex: {(p.samples || []).slice(0, 2).join(' · ')}
                        </div>
                        {kind === 'characteristics' && p.parent && (
                          <div className="text-[9px] text-gray-600">under {p.parent}</div>
                        )}
                      </div>
                      <button
                        onClick={() => accept(kind, p.name, p.parent)}
                        disabled={busyKey === key}
                        className="shrink-0 px-2 py-1 rounded text-[10px] font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50"
                      >
                        {busyKey === key ? '…' : 'Accept'}
                      </button>
                    </li>
                  );
                })}
                {items.length === 0 && <li className="text-[10px] text-gray-600 italic">none</li>}
              </ul>
            </div>
          );
        })}
      </div>
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
          Low-confidence Sonnet decisions ({queue.length}) — inspection only, all rows still bucketed via fallback
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
                <th className="px-4 py-2 text-left">Characteristic</th>
                <th className="px-4 py-2 text-left">Sector</th>
                <th className="px-4 py-2 text-right">Conf</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[#2e2e2e]">
              {queue.map((q, i) => (
                <tr key={i} className="hover:bg-white/[0.02]">
                  <td className="px-4 py-2 text-gray-200 max-w-md truncate" title={q.industry_string}>{q.industry_string}</td>
                  <td className="px-4 py-2 text-gray-400">{q.identity || '—'}</td>
                  <td className="px-4 py-2 text-gray-400">{q.characteristic || '—'}</td>
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

// ───── Taxonomy Library: editable Identity / Characteristic / Sector lists ─────

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
  const [tab, setTab] = useState<'identities' | 'characteristics' | 'sectors'>('identities');
  const [data, setData] = useState<{ identities: TaxRow[]; characteristics: TaxRow[]; sectors: TaxRow[] }>({ identities: [], characteristics: [], sectors: [] });
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
          {(['identities', 'characteristics', 'sectors'] as const).map(t => (
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
              {tab === 'characteristics' && <th className="px-5 py-3 text-left">Parent identity</th>}
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
                {tab === 'characteristics' && <td className="px-5 py-2 text-gray-300">{r.parent_identity || '—'}</td>}
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
  kind: 'identities' | 'characteristics' | 'sectors';
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
      {kind === 'characteristics' && (
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
            ...(kind === 'characteristics' ? { parent_identity: parent } : {}),
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
