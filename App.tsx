
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { AppTab, Contact, Enrichment, BatchStats, MergedContact, FilterCondition, FilterOperator } from './types';
import { db } from './services/supabaseClient';
import { fetchDigest } from './services/scraperService';
import { enrichBatch } from './services/enrichmentService';
import {
  Users,
  Zap,
  Database,
  Loader2,
  DatabaseZap,
  ChevronLeft,
  ChevronRight,
  Maximize2,
  Square,
  Trash2,
  Download,
  Filter,
  ArrowUpDown,
  MoreHorizontal,
  RefreshCw,
  Plus,
  X,
  Settings2,
  ExternalLink,
  Play
} from 'lucide-react';

/**
 * IMPLEMENTATION NOTE: Option B - Asynchronous Background Processing
 * This component handles the 'Pipeline' as a non-blocking queue.
 * startEnrichmentQueue enqueues the tasks in Supabase and returns immediately.
 * processQuantumPipeline is called without await to run in the background.
 */

export default function App() {
  const [activeTab, setActiveTab] = useState<AppTab>(AppTab.MANAGER);
  const [contacts, setContacts] = useState<MergedContact[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);

  const [activeFilters, setActiveFilters] = useState<FilterCondition[]>(() => {
    const saved = localStorage.getItem('active_filters_v3');
    return saved ? JSON.parse(saved) : [];
  });

  const [stats, setStats] = useState<BatchStats>({ total: 0, completed: 0, failed: 0, isProcessing: false });
  const [logs, setLogs] = useState<string[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [isAllFilteredSelected, setIsAllFilteredSelected] = useState(false);

  const stopRequestedRef = useRef(false);
  const [isStopping, setIsStopping] = useState(false);

  useEffect(() => {
    localStorage.setItem('active_filters_v3', JSON.stringify(activeFilters));
    setIsAllFilteredSelected(false);
  }, [activeFilters]);

  const loadData = useCallback(async () => {
    try {
      const { data, count } = await db.getPaginatedContacts(
        currentPage,
        pageSize,
        false,
        activeFilters
      );
      setContacts(data);
      setTotalCount(count);
    } catch (err: any) {
      addLog(`Error: ${err.message}`);
    }
  }, [currentPage, pageSize, activeFilters]);

  useEffect(() => { loadData(); }, [loadData]);

  const addLog = (msg: string) => {
    setLogs(prev => [`${new Date().toLocaleTimeString()}: ${msg}`, ...prev].slice(0, 500));
  };

  const updateContactUI = useCallback((contactId: string, updates: Partial<MergedContact>) => {
    setContacts(prev => prev.map(c => c.contact_id === contactId ? { ...c, ...updates } : c));
  }, []);

  const stopEnrichment = () => {
    if (stats.isProcessing && !isStopping) {
      stopRequestedRef.current = true;
      setIsStopping(true);
      addLog("⚠️ Stop signal received. Halting background process...");
    }
  };

  /**
   * Detached Background Logic (Moved to server.ts)
   */
  const processQuantumPipeline = async (batch: MergedContact[]) => {
    // This local logic is being deprecated in favor of server-side processing
    addLog("ℹ️ Local worker is now deprecated. Requests are handled by the server.");
  };


  /**
   * Core Enrichment Trigger (Calls Monolithic Backend)
   */
  const startEnrichmentQueue = async () => {
    let batch: MergedContact[] = [];

    if (isAllFilteredSelected) {
      addLog(`📦 Preparing global batch: Fetching all ${totalCount} matching contacts...`);
      try {
        batch = await db.getAllFilteredContacts(activeFilters);
      } catch (e: any) {
        addLog(`Error fetching bulk contacts: ${e.message}`);
        return;
      }
    } else {
      batch = contacts.filter(c => selectedIds.has(c.contact_id));
    }

    if (batch.length === 0) return;

    try {
      addLog(`🚀 Sending ${batch.length} records to backend for background enrichment...`);

      const response = await fetch('/api/enrich', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contactIds: batch.map(b => b.contact_id) })
      });

      if (!response.ok) throw new Error(`Server Error: ${response.status}`);

      addLog(`✅ 202 Accepted: Backend is processing ${batch.length} records.`);
      setActiveTab(AppTab.ENRICHMENT);
      setSelectedIds(new Set());
      setIsAllFilteredSelected(false);

      // Update UI stats temporarily
      setStats({ total: batch.length, completed: 0, failed: 0, isProcessing: true });

    } catch (e: any) {
      addLog(`❌ Error starting queue: ${e.message}`);
    }
  };


  const resumePendingQueue = async () => {
    addLog("🔍 Searching for pending records...");
    try {
      const { data } = await db.getPaginatedContacts(1, 1000, false, [
        { id: 'resumer', column: 'status', operator: 'in', value: ['pending'] }
      ]);
      if (data.length > 0) {
        addLog(`🔄 Resuming background execution for ${data.length} records.`);
        processQuantumPipeline(data);
      } else {
        addLog("ℹ️ No pending tasks found.");
      }
    } catch (e: any) {
      addLog(`Error: ${e.message}`);
    }
  };

  const exportToCSV = () => {
    if (!contacts.length) return;
    const headers = ['Contact ID', 'Email', 'First Name', 'Last Name', 'Website', 'Industry', 'Confidence', 'Cost', 'Status', 'Processed At'];
    const rows = contacts.map(c => [
      c.contact_id, c.email, c.first_name, c.last_name, c.company_website,
      c.classification || c.industry || '', c.confidence || '', c.cost || '0',
      c.status || 'pending', c.processed_at || ''
    ]);
    const csvContent = [headers.join(','), ...rows.map(row => row.map(v => `"${String(v).replace(/"/g, '""')}"`).join(','))].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `quantum_export_${new Date().toISOString().slice(0, 10)}.csv`;
    link.click();
  };

  return (
    <div className="flex h-screen bg-[#1c1c1c] overflow-hidden text-[#ededed] font-sans">
      <aside className="w-64 bg-[#1c1c1c] border-r border-[#2e2e2e] flex flex-col shrink-0 z-50">
        <div className="p-4 flex items-center gap-4">
          <div className="bg-[#3ecf8e] p-1.5 rounded-md min-w-[24px]">
            <DatabaseZap className="text-black w-4 h-4" />
          </div>
          <h1 className="text-sm font-bold text-white">Quantum Scaling</h1>
        </div>
        <nav className="flex-1 px-2 py-4 space-y-1 overflow-hidden">
          <SidebarIconButton active={activeTab === AppTab.MANAGER} onClick={() => setActiveTab(AppTab.MANAGER)} icon={<Users className="w-5 h-5" />} label="Contacts" />
          <SidebarIconButton active={activeTab === AppTab.ENRICHMENT} onClick={() => setActiveTab(AppTab.ENRICHMENT)} icon={<Zap className={`w-5 h-5 ${stats.isProcessing ? 'text-[#3ecf8e] animate-pulse' : ''}`} />} label="Pipeline Monitor" />
        </nav>
      </aside>

      <main className="flex-1 flex flex-col min-w-0 bg-[#1c1c1c]">
        <header className="h-12 border-b border-[#2e2e2e] px-4 flex items-center justify-between bg-[#1c1c1c] shrink-0">
          <div className="flex items-center gap-2 text-xs text-gray-400 font-medium">
            <Database className="w-4 h-4" />
            <span>/</span> <span className="text-white font-semibold capitalize">{activeTab}</span>
          </div>
          <div className="flex items-center gap-3 text-[10px] text-gray-500 font-mono uppercase">
            gpt-4.1-mini / Background Mode
          </div>
        </header>

        <div className="flex-1 overflow-hidden flex flex-col">
          {activeTab === AppTab.MANAGER ? (
            <DataTable
              data={contacts}
              activeFilters={activeFilters}
              onFiltersChange={setActiveFilters}
              columns={[
                { key: 'contact_id', label: 'contact_id', type: 'uuid', defaultWidth: 140 },
                { key: 'email', label: 'email', type: 'text', defaultWidth: 200 },
                { key: 'first_name', label: 'first_name', type: 'text', defaultWidth: 100 },
                { key: 'last_name', label: 'last_name', type: 'text', defaultWidth: 100 },
                { key: 'company_website', label: 'website', type: 'text', defaultWidth: 180 },
                { key: 'classification', label: 'industry', type: 'text', defaultWidth: 180 },
                { key: 'confidence', label: 'confidence', type: 'confidence', defaultWidth: 110 },
                { key: 'cost', label: 'cost', type: 'currency', defaultWidth: 90 },
                { key: 'status', label: 'status', type: 'status', defaultWidth: 140 },
                { key: 'processed_at', label: 'processed_at', type: 'date', defaultWidth: 130 }
              ]}
              selectedIds={selectedIds}
              isAllFilteredSelected={isAllFilteredSelected}
              onSetIsAllFilteredSelected={setIsAllFilteredSelected}
              onToggleRow={(id: string) => {
                const n = new Set(selectedIds);
                if (n.has(id)) n.delete(id); else n.add(id);
                setSelectedIds(n);
                setIsAllFilteredSelected(false);
              }}
              onToggleAll={() => {
                if (selectedIds.size === contacts.length && contacts.length > 0) {
                  setSelectedIds(new Set());
                  setIsAllFilteredSelected(false);
                } else {
                  setSelectedIds(new Set(contacts.map(c => c.contact_id)));
                }
              }}
              onEnrichSelected={startEnrichmentQueue}
              onExportCSV={exportToCSV}
              isProcessing={stats.isProcessing}
              currentPage={currentPage}
              pageSize={pageSize}
              totalCount={totalCount}
              onPageChange={(p: number) => setCurrentPage(p)}
              onPageSizeChange={setPageSize}
            />
          ) : (
            <div className="flex-1 p-8 overflow-auto bg-[#1c1c1c] space-y-6">
              <div className="max-w-4xl mx-auto space-y-6">
                <div className="flex items-center justify-between">
                  <div>
                    <h2 className="text-xl font-bold flex items-center gap-3">
                      <Zap className={stats.isProcessing ? 'text-[#3ecf8e] animate-pulse' : 'text-gray-500'} />
                      Background Pipeline Monitor
                    </h2>
                    <p className="text-[10px] text-gray-500 mt-1 uppercase tracking-widest font-bold">Asynchronous Processing Execution</p>
                  </div>
                  <div className="flex items-center gap-3">
                    {!stats.isProcessing && (
                      <button onClick={resumePendingQueue} className="flex items-center gap-2 px-4 py-2 bg-[#3ecf8e] hover:bg-[#2fb37a] text-black rounded-lg font-bold text-xs shadow-lg transition-all">
                        <Play className="w-3.5 h-3.5 fill-current" />
                        Resume Queue
                      </button>
                    )}
                    {stats.isProcessing && (
                      <button onClick={stopEnrichment} disabled={isStopping} className={`flex items-center gap-2 px-4 py-2 rounded-lg font-bold text-xs transition-all ${isStopping ? 'bg-rose-900/50 text-rose-300' : 'bg-rose-500 hover:bg-rose-600 text-white shadow-lg'}`}>
                        {isStopping ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Square className="w-3.5 h-3.5" />}
                        {isStopping ? 'Stopping...' : 'Stop Local Worker'}
                      </button>
                    )}
                    <button onClick={() => setLogs([])} className="flex items-center gap-2 px-3 py-2 text-gray-400 hover:text-white hover:bg-[#2e2e2e] rounded-lg text-xs border border-[#2e2e2e]">
                      <Trash2 className="w-3.5 h-3.5" /> Clear Logs
                    </button>
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-6">
                  <StatCard label="Completed" value={stats.completed} color="text-[#3ecf8e]" />
                  <StatCard label="Failures" value={stats.failed} color="text-rose-500" />
                  <StatCard label="In Queue" value={stats.total - (stats.completed + stats.failed)} color="text-indigo-400" />
                </div>
                <div className="bg-[#0e0e0e] rounded-lg border border-[#2e2e2e] p-5 font-mono text-[11px] h-[500px] overflow-auto custom-scrollbar shadow-inner">
                  {logs.length === 0 ? (
                    <div className="h-full flex flex-col items-center justify-center opacity-30 text-center text-gray-500">
                      <DatabaseZap className="w-12 h-12 mb-4" />
                      <p>Pipeline is idle. Enqueue records to begin background processing.</p>
                      <p className="mt-2 text-[9px]">Tasks continue to run until the current session ends.</p>
                    </div>
                  ) : logs.map((l, i) => {
                    const isSuccess = l.includes('✓') || l.includes('Result') || l.includes('Syncing') || l.includes('Accepted') || l.includes('✅');
                    const isError = l.includes('✗') || l.includes('Error') || l.includes('Failed') || l.includes('🛑');
                    const isPhase = l.includes('Phase') || l.includes('🚀') || l.includes('---') || l.includes('📦');
                    return (
                      <div key={i} className={`py-0.5 border-b border-[#1c1c1c] last:border-0 
                        ${isSuccess ? 'text-[#3ecf8e]' : ''}
                        ${isError ? 'text-rose-400' : ''}
                        ${isPhase ? 'text-indigo-400 font-bold' : ''}
                        ${!isSuccess && !isError && !isPhase ? 'text-gray-500' : ''}
                      `}>
                        {l}
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

function SidebarIconButton({ active, onClick, icon, label }: any) {
  return (
    <button onClick={onClick} className={`w-full flex items-center gap-4 p-2 rounded-md transition-all ${active ? 'bg-[#2e2e2e] text-[#3ecf8e]' : 'text-gray-400 hover:text-white hover:bg-[#2e2e2e]'}`}>
      <div className="min-w-[20px]">{icon}</div>
      <span className="text-[13px] font-semibold capitalize">{label}</span>
    </button>
  );
}

function StatCard({ label, value, color }: any) {
  return (
    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-6 shadow-sm">
      <p className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">{label}</p>
      <p className={`text-3xl font-bold ${color}`}>{value}</p>
    </div>
  );
}

function DataTable({
  data,
  columns,
  selectedIds,
  isAllFilteredSelected,
  onSetIsAllFilteredSelected,
  onToggleRow,
  onToggleAll,
  onEnrichSelected,
  onExportCSV,
  isProcessing,
  currentPage,
  pageSize,
  totalCount,
  onPageChange,
  onPageSizeChange,
  activeFilters = [],
  onFiltersChange
}: any) {
  const [showPageSizeMenu, setShowPageSizeMenu] = useState(false);
  const [showFilterBuilder, setShowFilterBuilder] = useState(false);
  const totalPages = Math.ceil(totalCount / pageSize) || 1;
  const menuRef = useRef<HTMLDivElement>(null);
  const filterRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setShowPageSizeMenu(false);
      if (filterRef.current && !filterRef.current.contains(e.target as Node)) setShowFilterBuilder(false);
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const addFilter = () => {
    const newFilter: FilterCondition = {
      id: Math.random().toString(36).substr(2, 9),
      column: 'email',
      operator: 'contains',
      value: ''
    };
    onFiltersChange([...activeFilters, newFilter]);
  };

  const removeFilter = (id: string) => onFiltersChange(activeFilters.filter((f: FilterCondition) => f.id !== id));

  const updateFilter = (id: string, updates: Partial<FilterCondition>) => {
    onFiltersChange(activeFilters.map((f: FilterCondition) => {
      if (f.id === id) {
        const next = { ...f, ...updates };
        if (next.column === 'status' && !Array.isArray(next.value)) {
          next.value = [];
          next.operator = 'in';
        }
        if (next.column === 'confidence' && (next.value === '' || Array.isArray(next.value))) {
          next.value = "1";
          next.operator = 'equals';
        }
        return next;
      }
      return f;
    }));
  };

  const statusOptions = ['new', 'pending', 'processing', 'completed', 'failed'];
  const confidenceOptions = Array.from({ length: 11 }, (_, i) => i.toString());
  const allOnPageSelected = selectedIds.size === data.length && data.length > 0;
  const showSelectAllFilteredBanner = allOnPageSelected && totalCount > data.length && !isAllFilteredSelected;

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="h-12 border-b border-[#2e2e2e] px-4 flex items-center justify-between bg-[#1c1c1c] shrink-0">
        <div className="flex items-center gap-2">
          <div className="relative" ref={filterRef}>
            <button
              onClick={() => setShowFilterBuilder(!showFilterBuilder)}
              className={`text-[11px] font-medium px-2.5 py-1.5 flex items-center gap-1.5 border border-[#2e2e2e] rounded transition-colors ${activeFilters.length > 0 ? 'bg-[#3ecf8e22] border-[#3ecf8e44] text-[#3ecf8e]' : 'text-gray-300 hover:text-white hover:bg-[#2e2e2e]'}`}
            >
              <Filter className="w-3.5 h-3.5" />
              Filters {activeFilters.length > 0 && `(${activeFilters.length})`}
            </button>

            {showFilterBuilder && (
              <div className="absolute top-full mt-2 left-0 w-[480px] bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl shadow-2xl z-50 p-4">
                <div className="flex items-center justify-between mb-4">
                  <span className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">Active Filters</span>
                  <button onClick={() => onFiltersChange([])} className="text-[10px] text-rose-400 hover:underline">Clear all</button>
                </div>

                <div className="space-y-4 max-h-[400px] overflow-auto custom-scrollbar mb-4 pr-1">
                  {activeFilters.length === 0 ? (
                    <p className="text-[11px] text-gray-600 text-center py-4">No active filters.</p>
                  ) : (
                    activeFilters.map((filter: FilterCondition) => (
                      <div key={filter.id} className="flex flex-col gap-2 p-2 bg-[#0e0e0e]/50 border border-[#2e2e2e] rounded-lg relative group">
                        <button onClick={() => removeFilter(filter.id)} className="absolute -top-2 -right-2 p-1 bg-[#1c1c1c] border border-[#2e2e2e] hover:bg-rose-900/20 hover:text-rose-400 rounded-full text-gray-500 opacity-0 group-hover:opacity-100 transition-opacity">
                          <X className="w-3 h-3" />
                        </button>

                        <div className="flex items-center gap-2">
                          <select
                            value={filter.column}
                            onChange={(e) => updateFilter(filter.id, { column: e.target.value })}
                            className="bg-[#0e0e0e] border border-[#2e2e2e] text-[11px] rounded px-2 py-1 flex-1 text-gray-300 outline-none focus:border-[#3ecf8e]"
                          >
                            {columns.map((c: any) => (
                              <option key={c.key} value={c.key}>{c.label}</option>
                            ))}
                          </select>

                          <select
                            value={filter.operator}
                            onChange={(e) => updateFilter(filter.id, { operator: e.target.value as FilterOperator })}
                            className="bg-[#0e0e0e] border border-[#2e2e2e] text-[11px] rounded px-2 py-1 w-24 text-gray-300 outline-none focus:border-[#3ecf8e]"
                            disabled={filter.column === 'status'}
                          >
                            <option value="equals">is</option>
                            <option value="contains">contains</option>
                            <option value="starts_with">starts with</option>
                            <option value="greater_than">&gt;</option>
                            <option value="less_than">&lt;</option>
                            {filter.column === 'status' && <option value="in">any of</option>}
                          </select>
                        </div>

                        {filter.column === 'status' ? (
                          <div className="flex flex-wrap gap-2 mt-1">
                            {statusOptions.map(opt => {
                              const isChecked = Array.isArray(filter.value) && filter.value.includes(opt);
                              return (
                                <button
                                  key={opt}
                                  onClick={() => {
                                    const current = Array.isArray(filter.value) ? filter.value : [];
                                    const next = isChecked ? current.filter(s => s !== opt) : [...current, opt];
                                    updateFilter(filter.id, { value: next });
                                  }}
                                  className={`px-2 py-1 rounded text-[10px] font-bold uppercase transition-all border ${isChecked ? 'bg-[#3ecf8e22] border-[#3ecf8e44] text-[#3ecf8e]' : 'bg-[#1c1c1c] border-[#2e2e2e] text-gray-500 hover:text-gray-300'}`}
                                >
                                  {opt}
                                </button>
                              );
                            })}
                          </div>
                        ) : filter.column === 'confidence' ? (
                          <div className="flex items-center gap-2 mt-1">
                            <select
                              value={filter.value}
                              onChange={(e) => updateFilter(filter.id, { value: e.target.value })}
                              className="bg-[#0e0e0e] border border-[#2e2e2e] text-[11px] rounded px-2 py-1 flex-1 text-gray-300 outline-none focus:border-[#3ecf8e]"
                            >
                              {confidenceOptions.map(opt => (
                                <option key={opt} value={opt}>{opt}</option>
                              ))}
                            </select>
                          </div>
                        ) : (
                          <input
                            type="text"
                            placeholder="Value..."
                            value={filter.value}
                            onChange={(e) => updateFilter(filter.id, { value: e.target.value })}
                            className="bg-[#0e0e0e] border border-[#2e2e2e] text-[11px] rounded px-2 py-1 w-full text-gray-300 focus:border-[#3ecf8e] outline-none mt-1"
                          />
                        )}
                      </div>
                    ))
                  )}
                </div>

                <button
                  onClick={addFilter}
                  className="w-full flex items-center justify-center gap-2 py-2 border border-dashed border-[#2e2e2e] rounded-lg text-[11px] text-gray-400 hover:text-white transition-all"
                >
                  <Plus className="w-3.5 h-3.5" /> Add Condition
                </button>
              </div>
            )}
          </div>

          <button onClick={onExportCSV} className="text-[11px] font-medium text-gray-300 hover:text-white px-2.5 py-1.5 flex items-center gap-1.5 border border-[#2e2e2e] rounded hover:bg-[#2e2e2e] transition-colors"><Download className="w-3.5 h-3.5" /> Export CSV</button>
          <div className="h-4 w-px bg-[#2e2e2e] mx-1"></div>
          {(selectedIds.size > 0 || isAllFilteredSelected) && (
            <button onClick={onEnrichSelected} disabled={isProcessing} className={`text-white text-[11px] font-bold px-3 py-1.5 rounded flex items-center gap-1.5 transition-colors ${isProcessing ? 'bg-gray-700 opacity-50' : 'bg-[#3ecf8e] text-black hover:bg-[#2fb37a]'}`}>
              <Zap className="w-3.5 h-3.5" /> {isProcessing ? 'Pipeline Running...' : `Enrich ${isAllFilteredSelected ? totalCount.toLocaleString() : selectedIds.size}`}
            </button>
          )}
        </div>
        <div className="flex items-center gap-2 text-gray-500"><Settings2 className="w-3.5 h-3.5 hover:text-white cursor-pointer" /><Maximize2 className="w-3.5 h-3.5 hover:text-white cursor-pointer" /><MoreHorizontal className="w-4 h-4 hover:text-white cursor-pointer" /></div>
      </div>

      <div className="flex-1 overflow-auto custom-scrollbar relative">
        <table className="w-full border-collapse table-fixed text-[#ededed]">
          <thead className="sticky top-0 z-20 bg-[#1c1c1c] shadow-[0_1px_0_rgba(46,46,46,1)]">
            <tr>
              <th className="w-12 p-3 text-center border-r border-[#2e2e2e] sticky left-0 bg-[#1c1c1c] z-30"><input type="checkbox" onChange={onToggleAll} checked={allOnPageSelected} className="rounded-sm bg-[#0e0e0e] text-[#3ecf8e] focus:ring-0 border-[#2e2e2e]" /></th>
              {columns.map((c: any) => (<th key={c.key} className="p-3 text-left border-r border-[#2e2e2e] uppercase text-[10px] font-bold text-gray-500 tracking-wider" style={{ width: c.defaultWidth }}>{c.label}</th>))}
            </tr>
          </thead>
          <tbody className="divide-y divide-[#2e2e2e]">
            {(showSelectAllFilteredBanner || isAllFilteredSelected) && (
              <tr className="bg-[#3ecf8e]/5 border-b border-[#3ecf8e]/20 sticky top-[37px] z-10 shadow-sm">
                <td colSpan={columns.length + 1} className="p-2 text-center text-[10px] font-medium text-gray-300">
                  {isAllFilteredSelected ? (
                    <span>All matching records selected. <button onClick={() => { onSetIsAllFilteredSelected(false); onToggleAll(); }} className="ml-2 text-[#3ecf8e] hover:underline font-bold">Clear selection</button></span>
                  ) : (
                    <span>All {data.length} records on this page selected. <button onClick={() => onSetIsAllFilteredSelected(true)} className="ml-2 text-[#3ecf8e] hover:underline font-bold underline-offset-2">Select all {totalCount.toLocaleString()} matching records?</button></span>
                  )}
                </td>
              </tr>
            )}
            {data.length === 0 ? (<tr><td colSpan={columns.length + 1} className="p-20 text-center text-gray-600 text-sm font-medium">No records matching filters</td></tr>) : data.map((r: any) => (
              <tr key={r.contact_id} className={`text-[11px] transition-colors ${selectedIds.has(r.contact_id) || isAllFilteredSelected ? 'bg-[#1c2e26]' : 'hover:bg-[#242424]'}`}>
                <td className="p-3 text-center border-r border-[#2e2e2e] sticky left-0 bg-[#1c1c1c] z-10"><input type="checkbox" checked={selectedIds.has(r.contact_id) || isAllFilteredSelected} onChange={() => onToggleRow(r.contact_id)} className="rounded-sm bg-[#0e0e0e] text-[#3ecf8e] focus:ring-0 border-[#2e2e2e]" /></td>
                {columns.map((c: any) => (<td key={c.key} className="p-3 truncate border-r border-[#2e2e2e] font-medium text-gray-300"><CellRenderer type={c.type} columnKey={c.key} value={r[c.key]} row={r} /></td>))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="h-10 border-t border-[#2e2e2e] px-4 flex items-center justify-between bg-[#1c1c1c] shrink-0 text-[10px]">
        <div className="flex items-center gap-2">
          <button disabled={currentPage <= 1} onClick={() => onPageChange(currentPage - 1)} className="p-1 text-gray-400 hover:text-white disabled:opacity-20"><ChevronLeft className="w-4 h-4" /></button>
          <span className="text-gray-400">Page <span className="text-white font-bold">{currentPage}</span> of {totalPages}</span>
          <button disabled={currentPage >= totalPages} onClick={() => onPageChange(currentPage + 1)} className="p-1 text-gray-400 hover:text-white disabled:opacity-20"><ChevronRight className="w-4 h-4" /></button>
        </div>
        <div className="flex items-center gap-4 text-gray-500 relative" ref={menuRef}>
          <button onClick={() => setShowPageSizeMenu(!showPageSizeMenu)} className="px-2 py-0.5 border border-[#2e2e2e] rounded bg-[#0e0e0e] text-white font-bold">{pageSize} rows</button>
          {showPageSizeMenu && (
            <div className="absolute bottom-full mb-1 right-0 w-24 bg-[#1c1c1c] border border-[#2e2e2e] rounded py-1 shadow-2xl">
              {[25, 50, 100, 500].map(s => (<button key={s} onClick={() => { onPageSizeChange(s); onPageChange(1); setShowPageSizeMenu(false); }} className={`w-full text-left px-3 py-1 hover:bg-[#2e2e2e] ${pageSize === s ? 'text-[#3ecf8e]' : ''}`}>{s}</button>))}
            </div>
          )}
          <span><span className="text-white font-bold">{totalCount.toLocaleString()}</span> records</span>
        </div>
      </div>
    </div>
  );
}

function CellRenderer({ type, columnKey, value, row }: any) {
  if (value === null || value === undefined) return <span className="text-gray-700 italic">NULL</span>;

  if (columnKey === 'company_website') {
    const url = String(value).startsWith('http') ? String(value) : `https://${value}`;
    return (
      <a href={url} target="_blank" rel="noopener noreferrer" className="flex items-center gap-1.5 text-[#3ecf8e] hover:underline truncate group">
        <span className="truncate">{String(value)}</span>
        <ExternalLink className="w-2.5 h-2.5 opacity-0 group-hover:opacity-100 transition-opacity" />
      </a>
    );
  }

  switch (type) {
    case 'status': return <StatusBadge status={value} stage={row?.processing_stage} errorMessage={row?.error_message} />;
    case 'confidence': return (
      <div className="flex items-center gap-2">
        <div className="w-8 bg-[#2e2e2e] h-1 rounded-full overflow-hidden"><div className="bg-[#3ecf8e] h-full" style={{ width: `${(value || 0) * 10}%` }} /></div>
        <span className="text-[10px] text-gray-500">{value}</span>
      </div>
    );
    case 'currency': return <span className="text-gray-400 tabular-nums">${parseFloat(value).toFixed(6)}</span>;
    case 'date': return <span className="text-gray-500">{new Date(value).toLocaleDateString()}</span>;
    case 'uuid': return <span className="text-gray-600 font-mono text-[9px]">{String(value).slice(0, 8)}...</span>;
    default: return <span className="truncate block" title={String(value)}>{String(value)}</span>;
  }
}

function StatusBadge({ status, stage, errorMessage }: { status: string, stage?: string, errorMessage?: string }) {
  const currentStatus = status || 'new';
  if (currentStatus === 'processing') {
    return (
      <div className="flex items-center gap-2 px-1.5 py-0.5 rounded border border-indigo-500/20 bg-indigo-500/10 text-indigo-400">
        <RefreshCw className="w-2.5 h-2.5 animate-spin" />
        <span className="text-[8px] font-bold uppercase tracking-wider">{stage || 'Working...'}</span>
      </div>
    );
  }
  const styles: any = {
    completed: 'text-[#3ecf8e] border-[#3ecf8e]/20 bg-[#3ecf8e]/10',
    failed: 'text-rose-400 border-rose-400/20 bg-rose-400/10',
    new: 'text-gray-400 border-gray-400/20 bg-gray-400/5',
    pending: 'text-amber-400 border-amber-400/20 bg-amber-400/10'
  };
  return <span className={`px-1.5 py-0.5 rounded border text-[8px] font-bold uppercase tracking-wider ${styles[currentStatus] || ''}`} title={errorMessage}>{currentStatus}</span>;
}
