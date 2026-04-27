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

import React, { useState, useEffect, useCallback } from 'react';
import {
  Layers, Loader2, AlertCircle, ArrowLeft, Plus, X, Trash2, Download,
  Play, BookMarked, CheckCircle2, Edit3, Archive
} from 'lucide-react';
import Papa from 'papaparse';
import type { BucketingRun, BucketProposal, LibraryBucket } from './types';

type BucketingView = 'index' | 'setup' | 'detail' | 'library';

const RESERVED_GENERIC = 'Generic';
const RESERVED_DISQUALIFIED = 'Disqualified';

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
    if (activeRun.status === 'completed' || activeRun.status === 'failed' || activeRun.status === 'taxonomy_ready') return;
    const t = setInterval(fetchActive, 2500);
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

  const startNew = async (payload: { name: string; list_names: string[]; min_volume: number; bucket_budget: number; preferred_library_ids: string[] }) => {
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
            library={library}
            onCancel={() => setView('index')}
            onStart={startNew}
            loading={loading}
          />
        )}

        {view === 'library' && (
          <BucketingLibrary library={library} onRefresh={refreshLibrary} onError={setError} />
        )}

        {view === 'detail' && activeRun && (
          <BucketingDetail
            run={activeRun}
            bucketCounts={bucketCounts}
            sectorMix={sectorMix}
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
  };
  const labels: Record<string, string> = {
    'taxonomy_pending': 'Discovering…',
    'taxonomy_ready':   'Awaiting review',
    'assigning':        'Assigning…',
    'completed':        'Completed',
    'failed':           'Failed',
  };
  return (
    <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${styles[status] || 'bg-gray-500/15 text-gray-400 border-gray-500/30'}`}>
      {labels[status] || status}
    </span>
  );
}

// ───── SETUP VIEW ──────────────────────────────────────────────────

function BucketingSetup({ importLists, library, onCancel, onStart, loading }: {
  importLists: { name: string; contact_count: number; enriched_count?: number }[];
  library: LibraryBucket[];
  onCancel: () => void;
  onStart: (p: { name: string; list_names: string[]; min_volume: number; bucket_budget: number; preferred_library_ids: string[] }) => void;
  loading: boolean;
}) {
  const [name, setName] = useState('');
  const [selectedLists, setSelectedLists] = useState<Set<string>>(new Set());
  const [minVolume, setMinVolume] = useState(1000);
  const [bucketBudget, setBucketBudget] = useState(30);
  const [selectedLib, setSelectedLib] = useState<Set<string>>(new Set());
  const [showLib, setShowLib] = useState(false);

  const toggleList = (n: string) => {
    const s = new Set(selectedLists);
    s.has(n) ? s.delete(n) : s.add(n);
    setSelectedLists(s);
  };
  const toggleLib = (id: string) => {
    const s = new Set(selectedLib);
    s.has(id) ? s.delete(id) : s.add(id);
    setSelectedLib(s);
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

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div>
          <label className="block text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2">
            Minimum bucket volume
          </label>
          <input
            type="number"
            min={0}
            value={minVolume}
            onChange={e => setMinVolume(Math.max(0, parseInt(e.target.value || '0', 10)))}
            className="w-32 px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-sm text-white focus:outline-none focus:border-[#3ecf8e]"
          />
          <p className="text-[10px] text-gray-500 italic mt-1">
            Combos / specializations / identities below this roll up. Below identity → "Generic".
          </p>
        </div>
        <div>
          <label className="block text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2">
            Bucket budget (max campaign buckets)
          </label>
          <input
            type="number"
            min={5}
            max={100}
            value={bucketBudget}
            onChange={e => setBucketBudget(Math.max(5, Math.min(100, parseInt(e.target.value || '30', 10))))}
            className="w-32 px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-sm text-white focus:outline-none focus:border-[#3ecf8e]"
          />
          <p className="text-[10px] text-gray-500 italic mt-1">
            If more campaign buckets clear the threshold, the smallest are rolled up further until this cap is met. Typical: 25–35.
          </p>
        </div>
      </div>

      <div>
        <button
          onClick={() => setShowLib(s => !s)}
          className="text-[11px] text-gray-400 hover:text-white flex items-center gap-1"
        >
          <BookMarked className="w-3 h-3" />
          {showLib ? 'Hide' : 'Reuse'} library buckets ({selectedLib.size}/{library.length} selected)
        </button>
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
                      <span className={`font-medium block ${isSel ? 'text-[#3ecf8e]' : 'text-gray-200'}`}>{b.bucket_name}</span>
                      <span className="text-[10px] text-gray-500 truncate block">
                        {b.direct_ancestor || '—'} › {b.root_category || '—'} · used {b.times_used}×
                      </span>
                    </span>
                  </span>
                </button>
              );
            })}
          </div>
        )}
      </div>

      <div className="flex justify-end gap-2 pt-3 border-t border-[#2e2e2e]">
        <button onClick={onCancel} className="px-4 py-2 rounded text-xs font-bold bg-[#2e2e2e] text-gray-300 hover:bg-[#3e3e3e]">Cancel</button>
        <button
          onClick={() => onStart({
            name: name.trim(),
            list_names: Array.from(selectedLists),
            min_volume: minVolume,
            bucket_budget: bucketBudget,
            preferred_library_ids: Array.from(selectedLib)
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

function BucketingDetail({ run, bucketCounts, sectorMix, onRefresh, onError, onLibrarySaved }: {
  run: BucketingRun;
  bucketCounts: any[];
  sectorMix: any[];
  onRefresh: () => void;
  onError: (msg: string | null) => void;
  onLibrarySaved: () => void;
}) {
  if (run.status === 'taxonomy_pending') {
    return (
      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-12 text-center">
        <Loader2 className="w-8 h-8 text-[#3ecf8e] animate-spin mx-auto mb-3" />
        <p className="text-sm font-bold text-gray-300">Discovering bucket taxonomy…</p>
        <p className="text-[11px] text-gray-500 mt-1">Phase 1a — one LLM call across the full vocabulary.</p>
      </div>
    );
  }
  if (run.status === 'failed') {
    return (
      <div className="border border-red-500/30 rounded-xl bg-red-500/5 p-6">
        <p className="text-sm font-bold text-red-400 flex items-center gap-2">
          <AlertCircle className="w-4 h-4" /> Run failed
        </p>
        <p className="text-xs text-gray-400 mt-2">{run.error_message || 'Unknown error.'}</p>
      </div>
    );
  }
  if (run.status === 'assigning') {
    return (
      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] p-12 text-center">
        <Loader2 className="w-8 h-8 text-[#3ecf8e] animate-spin mx-auto mb-3" />
        <p className="text-sm font-bold text-gray-300">Matching contacts to buckets…</p>
        <p className="text-[11px] text-gray-500 mt-1">Phase 1b — embedding pre-filter + batched LLM matching + volume rollup.</p>
      </div>
    );
  }
  if (run.status === 'taxonomy_ready') {
    return <BucketingReview run={run} bucketCounts={bucketCounts} onRefresh={onRefresh} onError={onError} />;
  }
  return <BucketingResults run={run} bucketCounts={bucketCounts} sectorMix={sectorMix} onError={onError} onLibrarySaved={onLibrarySaved} />;
}

// ───── REVIEW VIEW ──────────────────────────────────────────────

function BucketingReview({ run, bucketCounts, onRefresh, onError }: {
  run: BucketingRun;
  bucketCounts: any[];
  onRefresh: () => void;
  onError: (msg: string | null) => void;
}) {
  const sourceBuckets = run.taxonomy_final?.buckets || run.taxonomy_proposal?.buckets || [];
  const primaryIdentities = (run.taxonomy_final?.primary_identities || run.taxonomy_proposal?.primary_identities) || [];
  const observedPatterns = (run.taxonomy_final?.observed_patterns || run.taxonomy_proposal?.observed_patterns) || [];
  const [kept, setKept] = useState<Set<string>>(new Set(sourceBuckets.map(b => b.functional_specialization)));
  const [renames, setRenames] = useState<Record<string, string>>({});
  const [adds, setAdds] = useState<{ functional_specialization: string; primary_identity: string; description: string }[]>([]);
  const [newSpec, setNewSpec] = useState('');
  const [newDesc, setNewDesc] = useState('');
  const [newIdentity, setNewIdentity] = useState('');
  const [minVolume, setMinVolume] = useState<number>(run.min_volume);
  const [bucketBudget, setBucketBudget] = useState<number>(run.bucket_budget || 30);
  const [busy, setBusy] = useState<'none' | 'saving' | 'assigning'>('none');
  const [showPatterns, setShowPatterns] = useState(false);

  const toggle = (name: string) => {
    const s = new Set(kept);
    s.has(name) ? s.delete(name) : s.add(name);
    setKept(s);
  };

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
            bucket_budget: bucketBudget
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

      <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e]">
        <div className="px-4 py-3 border-b border-[#2e2e2e] text-[10px] font-bold text-gray-500 uppercase tracking-widest flex items-center justify-between">
          <span>Discovered specializations (grouped by primary identity)</span>
          <span className="text-gray-600 normal-case tracking-normal font-normal">
            Phase 1b counts decide the campaign bucket: combo → spec → identity → Generic.
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
              functional_specialization: newSpec.trim(),
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
                <span><span className="font-bold text-white">{a.functional_specialization}</span> · under {a.primary_identity} — {a.description}</span>
                <button onClick={() => setAdds(adds.filter((_, j) => j !== i))} className="text-gray-500 hover:text-red-400">
                  <X className="w-3 h-3" />
                </button>
              </div>
            ))}
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
          <p className="text-[10px] text-gray-500 italic mt-1">Combos below this fall to spec; specs below to identity; identities below to Generic.</p>
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

      <div className="flex justify-end gap-2">
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
  const displayName = (b: BucketProposal) => renames[b.functional_specialization] ?? b.functional_specialization;
  const baseCountFor = (b: BucketProposal) =>
    countByBucket.get(displayName(b)) ?? countByBucket.get(b.functional_specialization) ?? 0;

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
        const identCount = specs.reduce((s, l) => s + (kept.has(l.functional_specialization) ? baseCountFor(l) : 0), 0);
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
                const isKept = kept.has(b.functional_specialization);
                const count = baseCountFor(b);
                const willRollUp = isKept && count > 0 && count < minVolume;
                return (
                  <div key={b.functional_specialization} className={`py-2 pl-4 pr-3 ${isKept ? '' : 'opacity-50'}`}>
                    <div className="flex items-start gap-2">
                      <input type="checkbox" checked={isKept} onChange={() => onToggle(b.functional_specialization)} className="mt-1.5 w-3.5 h-3.5 shrink-0" />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1 flex-wrap">
                          <span className="text-[9px] font-bold uppercase tracking-widest text-gray-500 shrink-0">↳ Specialization</span>
                          <input
                            value={displayName(b)}
                            onChange={e => onRename(b.functional_specialization, e.target.value)}
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

function BucketingResults({ run, bucketCounts, sectorMix, onError, onLibrarySaved }: {
  run: BucketingRun;
  bucketCounts: any[];
  sectorMix: any[];
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
  const specNames = new Set(bucketsInRun.map(b => b.functional_specialization));

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
        campaign_bucket: r.bucket_name,
        primary_identity: r.primary_identity || r.bucket_ancestor,
        functional_specialization: r.functional_specialization || r.bucket_leaf,
        sector_focus: r.sector_focus,
        is_generic: r.is_generic,
        is_disqualified: r.is_disqualified,
        source: r.source,
        confidence: r.confidence,
        email: r.contacts?.email,
        first_name: r.contacts?.first_name,
        last_name: r.contacts?.last_name,
        company_name: r.contacts?.company_name,
        company_website: r.contacts?.company_website,
        industry: r.contacts?.industry,
        lead_list_name: r.contacts?.lead_list_name,
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
              const sel = librarySelection.has(b.functional_specialization);
              return (
                <button
                  key={b.functional_specialization}
                  onClick={() => toggleLibSel(b.functional_specialization)}
                  className={`px-2 py-1 rounded text-[10px] font-bold border transition-colors ${sel ? 'bg-[#3ecf8e]/15 text-[#3ecf8e] border-[#3ecf8e]/40' : 'bg-[#1c1c1c] text-gray-300 border-[#2e2e2e] hover:border-gray-500'}`}
                  title={`under ${b.primary_identity}`}
                >
                  {b.functional_specialization}
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
            const isGeneric = name === RESERVED_GENERIC;
            const isDQ = name === RESERVED_DISQUALIFIED;
            // Bucket level: combo (sector + spec) > spec > identity > generic
            const isSpec = specNames.has(name);
            const isIdentity = identityNames.has(name) && !isSpec;
            const isCombo = !isSpec && !isIdentity && !isGeneric && !isDQ
                && Array.from(specNames).some(s => name.endsWith(' ' + s));
            const levelLabel = isCombo ? 'sector × specialization'
                : isSpec ? 'specialization'
                : isIdentity ? 'identity (rolled up)'
                : isGeneric ? 'generic'
                : isDQ ? 'disqualified'
                : 'rolled up';
            const levelColor = isCombo ? 'text-[#3ecf8e]'
                : isSpec ? 'text-[#3ecf8e]'
                : isIdentity ? 'text-blue-400'
                : isDQ ? 'text-red-400'
                : 'text-amber-400';
            return (
              <div key={name} className="px-4 py-3 hover:bg-white/[0.02]">
                <div className="flex items-center gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between mb-1.5">
                      <div className="flex items-center gap-2 min-w-0">
                        <span className={`text-xs font-bold ${isDQ ? 'text-red-400' : isGeneric ? 'text-amber-400' : 'text-white'} truncate`}>
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
                        className={`h-full transition-all ${isDQ ? 'bg-red-500/70' : isGeneric ? 'bg-amber-500/70' : isIdentity ? 'bg-blue-500/70' : 'bg-[#3ecf8e]'}`}
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
      <div className="flex justify-end">
        <button
          onClick={() => setCreating(true)}
          className="px-3 py-1.5 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] flex items-center gap-1"
        >
          <Plus className="w-3 h-3" /> New library bucket
        </button>
      </div>
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
                <th className="px-5 py-3 text-left">Leaf</th>
                <th className="px-5 py-3 text-left">Ancestor</th>
                <th className="px-5 py-3 text-left">Root</th>
                <th className="px-5 py-3 text-right">Used</th>
                <th className="px-5 py-3 text-right">Last used</th>
                <th className="px-5 py-3 text-right"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[#2e2e2e]">
              {library.map(b => (
                <tr key={b.id} className={`hover:bg-white/[0.02] ${b.archived ? 'opacity-50' : ''}`}>
                  <td className="px-5 py-3">
                    <div className="font-bold text-white">{b.bucket_name}</div>
                    {b.description && <div className="text-[10px] text-gray-500 truncate max-w-md">{b.description}</div>}
                  </td>
                  <td className="px-5 py-3 text-gray-300">{b.direct_ancestor || '—'}</td>
                  <td className="px-5 py-3 text-gray-300">{b.root_category || '—'}</td>
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
  const [name, setName] = useState(existing?.bucket_name || '');
  const [desc, setDesc] = useState(existing?.description || '');
  const [ancestor, setAncestor] = useState(existing?.direct_ancestor || '');
  const [root, setRoot] = useState(existing?.root_category || '');
  const [include, setInclude] = useState((existing?.include_terms || []).join(', '));
  const [exclude, setExclude] = useState((existing?.exclude_terms || []).join(', '));
  const [examples, setExamples] = useState((existing?.example_strings || []).join('\n'));
  const [busy, setBusy] = useState(false);

  const save = async () => {
    if (!name.trim()) return;
    setBusy(true);
    onError(null);
    try {
      const payload = {
        bucket_name: name.trim(),
        description: desc.trim(),
        direct_ancestor: ancestor.trim(),
        root_category: root.trim(),
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
      <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
        <input value={name} onChange={e => setName(e.target.value)} placeholder="Leaf name (unique)"
          disabled={!!existing}
          className="px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e] disabled:opacity-60" />
        <input value={ancestor} onChange={e => setAncestor(e.target.value)} placeholder="Direct ancestor"
          className="px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]" />
        <input value={root} onChange={e => setRoot(e.target.value)} placeholder="Root category"
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
        <button onClick={save} disabled={busy || !name.trim()} className="px-3 py-1.5 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1">
          {busy ? <Loader2 className="w-3 h-3 animate-spin" /> : <CheckCircle2 className="w-3 h-3" />}
          Save
        </button>
      </div>
    </div>
  );
}

// Local StatCard so this file has no dependency on App.tsx internals.
function StatCard({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-6 shadow-sm">
      <p className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">{label}</p>
      <p className={`text-3xl font-bold ${color}`}>{value}</p>
    </div>
  );
}
