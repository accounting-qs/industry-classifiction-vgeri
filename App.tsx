
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { AppTab, Contact, Enrichment, BatchStats, MergedContact, FilterCondition, FilterOperator } from './types';
import { db } from './services/supabaseClient';
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
  Search,
  Settings2,
  ExternalLink,
  Cpu,
  Activity,
  Play,
  BarChart2
} from 'lucide-react';

/**
 * IMPLEMENTATION NOTE: Option B - Asynchronous Background Processing
 * This component handles the 'Pipeline' as a non-blocking queue.
 * startEnrichmentQueue enqueues the tasks in Supabase and returns immediately.
 * processQuantumPipeline is called without await to run in the background.
 */

interface LogEntry {
  id?: string;
  timestamp: string;
  instance_id: string;
  module: string;
  message: string;
  level: 'info' | 'warn' | 'error' | 'phase';
}

export default function App() {
  const [activeTab, setActiveTab] = useState<AppTab>(AppTab.MANAGER);
  const [contacts, setContacts] = useState<MergedContact[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [currentPage, setCurrentPage] = useState(() => {
    const saved = localStorage.getItem('qs_current_page');
    return saved ? parseInt(saved, 10) : 1;
  });
  const [pageSize, setPageSize] = useState(() => {
    const saved = localStorage.getItem('qs_page_size');
    return saved ? parseInt(saved, 10) : 25;
  });

  const [activeFilters, setActiveFilters] = useState<FilterCondition[]>(() => {
    const saved = localStorage.getItem('active_filters_v3');
    return saved ? JSON.parse(saved) : [];
  });

  const [stats, setStats] = useState<BatchStats>({ total: 0, completed: 0, failed: 0, isProcessing: false });
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [logCurrentPage, setLogCurrentPage] = useState(1);
  const [logPageSize, setLogPageSize] = useState(100);
  const logContainerRef = useRef<HTMLDivElement>(null);

  // Auto-scroll logs to bottom
  useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [logs]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [isAllFilteredSelected, setIsAllFilteredSelected] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');
  const [isStopping, setIsStopping] = useState(false);

  const stopRequestedRef = useRef(false);

  useEffect(() => {
    localStorage.setItem('active_filters_v3', JSON.stringify(activeFilters));
    setIsAllFilteredSelected(false);
    setCurrentPage(1);
  }, [activeFilters]);

  // Debounce search query
  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedSearchQuery(searchQuery);
      setCurrentPage(1);
    }, 400);
    return () => clearTimeout(handler);
  }, [searchQuery]);

  useEffect(() => {
    localStorage.setItem('qs_current_page', currentPage.toString());
    localStorage.setItem('qs_page_size', pageSize.toString());
  }, [currentPage, pageSize]);

  const loadData = useCallback(async () => {
    try {
      const { data, count } = await db.getPaginatedContacts(
        currentPage,
        pageSize,
        false,
        activeFilters,
        debouncedSearchQuery
      );
      setContacts(data);
      setTotalCount(count);
    } catch (err: any) {
      addLog(`Error: ${err.message}`);
    }
  }, [currentPage, pageSize, activeFilters, debouncedSearchQuery]);

  useEffect(() => { loadData(); }, [loadData]);

  // Poll server for status
  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await fetch('/api/status');
        if (res.ok) {
          const data = await res.json();
          setLogs(data.logs);
          setStats(data.stats);
          if (activeTab === AppTab.MANAGER && data.stats.isProcessing) {
            loadData();
          }
        }
      } catch (e) {
        console.error("Status fetch error:", e);
      }
    };

    fetchStatus(); // Fetch once on mount/tab change

    let interval: any;
    if (stats.isProcessing) {
      interval = setInterval(fetchStatus, 2500);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [stats.isProcessing, activeTab, loadData]);

  const addLog = (msg: string, module: string = 'Pipeline', level: LogEntry['level'] = 'info') => {
    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      instance_id: 'local',
      module,
      message: msg,
      level
    };
    setLogs(prev => [entry, ...prev].slice(0, 500));
  };

  const updateContactUI = useCallback((contactId: string, updates: Partial<MergedContact>) => {
    setContacts(prev => prev.map(c => c.contact_id === contactId ? { ...c, ...updates } : c));
  }, []);

  const stopEnrichment = async () => {
    if (stats.isProcessing && !isStopping) {
      setIsStopping(true);
      addLog("⚠️ Stop signal sent. Halting backend pipeline...");
      try {
        const response = await fetch('/api/stop', { method: 'POST' });
        if (response.ok) {
          addLog("🛑 Worker acknowledges stop command. Finalizing current batch...");
        } else {
          addLog("❌ Failed to send stop command to backend.");
        }
      } catch (err: any) {
        addLog(`❌ Error stopping pipeline: ${err.message}`);
      } finally {
        setIsStopping(false);
      }
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
        batch = await db.getAllFilteredContacts(activeFilters, debouncedSearchQuery);
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
        <div className="p-2 border-t border-[#2e2e2e]">
          <SidebarIconButton active={activeTab === AppTab.PROXIES} onClick={() => setActiveTab(AppTab.PROXIES)} icon={<BarChart2 className={`w-5 h-5 ${activeTab === AppTab.PROXIES ? 'text-[#3ecf8e]' : ''}`} />} label="Proxy Performance" />
        </div>
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
          {activeTab === AppTab.PROXIES ? (
            <ProxyStatsDashboard />
          ) : activeTab === AppTab.ENRICHMENT ? (
            <PipelineMonitor
              stats={stats}
              logs={logs}
              resumePendingQueue={resumePendingQueue}
              stopEnrichment={stopEnrichment}
              isStopping={isStopping}
              setLogs={setLogs}
              logContainerRef={logContainerRef}
            />
          ) : activeTab === AppTab.MANAGER ? (
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
              searchQuery={searchQuery}
              onSearchQueryChange={setSearchQuery}
            />
          ) : (
            <div className="flex-1 p-6 overflow-hidden bg-[#1c1c1c] flex flex-col h-full min-h-0">
              <div className="max-w-5xl mx-auto w-full flex-1 flex flex-col space-y-6 min-h-0 h-full">
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
                <div
                  ref={logContainerRef}
                  className="bg-[#0e0e0e] rounded-xl border border-[#2e2e2e] p-0 font-mono text-[11px] flex-1 overflow-y-auto custom-scrollbar shadow-inner mb-6 relative ring-1 ring-white/5 min-h-0"
                >
                  {logs.length === 0 ? (
                    <div className="h-full flex flex-col items-center justify-center opacity-30 text-center text-gray-500 py-20">
                      <DatabaseZap className="w-12 h-12 mb-4 animate-pulse" />
                      <p className="text-sm font-bold">Pipeline Idle</p>
                      <p className="mt-2 text-[10px] uppercase tracking-wider">Queue records to begin processing</p>
                    </div>
                  ) : (
                    <div className="flex flex-col-reverse min-h-full">
                      {logs.map((l: any, i: number) => {
                        const date = new Date(l.timestamp);
                        const timeStr = date.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
                        const isError = l.level === 'error';
                        const isWarn = l.level === 'warn';
                        const isPhase = l.level === 'phase';

                        return (
                          <div key={l.id || i} className={`group flex items-start gap-4 px-4 py-1 border-b border-white/5 hover:bg-white/[0.02] transition-colors
                            ${isError ? 'bg-rose-500/10 border-rose-500/20 text-rose-300' : ''}
                            ${isWarn ? 'text-amber-300' : ''}
                            ${isPhase ? 'bg-indigo-500/5 text-indigo-300 font-bold' : 'text-gray-400'}
                          `}>
                            <span className="opacity-30 shrink-0 select-none w-16">{timeStr}</span>
                            <span className="opacity-20 shrink-0 select-none w-12 text-[10px] font-bold">[{l.instance_id}]</span>
                            <span className={`shrink-0 w-20 flex items-center gap-1.5 font-bold uppercase text-[9px] tracking-wider
                              ${l.module === 'Scraper' ? 'text-blue-400' : ''}
                              ${l.module === 'OpenAI' ? 'text-[#3ecf8e]' : ''}
                              ${l.module === 'Sync' ? 'text-purple-400' : ''}
                              ${l.module === 'Pipeline' ? 'text-indigo-400' : ''}
                            `}>
                              {l.module === 'Scraper' && <Zap className="w-2.5 h-2.5" />}
                              {l.module === 'OpenAI' && <Cpu className="w-2.5 h-2.5" />}
                              {l.module === 'Sync' && <Database className="w-2.5 h-2.5" />}
                              {l.module === 'Pipeline' && <Activity className="w-2.5 h-2.5" />}
                              {l.module}
                            </span>
                            <span className="flex-1 break-words leading-relaxed py-0.5">
                              {l.message}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  )}
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
  onFiltersChange,
  searchQuery = '',
  onSearchQueryChange
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
      <div className="h-12 border-b border-[#2e2e2e] px-4 flex items-center justify-between bg-[#1c1c1c] shrink-0 gap-4">
        <div className="flex items-center gap-4 flex-1">
          <div className="relative flex-1 max-w-sm">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-500" />
            <input
              type="text"
              placeholder="Search contacts..."
              value={searchQuery}
              onChange={(e) => onSearchQueryChange(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  // Explicitly set the debounced query now to trigger immediate search
                  onSearchQueryChange(searchQuery);
                }
              }}
              className="w-full bg-[#0e0e0e] border border-[#2e2e2e] rounded-lg py-1.5 pl-8 pr-3 text-[11px] text-gray-300 focus:border-[#3ecf8e] outline-none transition-all"
            />
          </div>

          <div className="flex items-center gap-2 overflow-x-auto no-scrollbar py-1">
            {activeFilters.map((f: FilterCondition) => (
              <div key={f.id} className="flex items-center gap-1.5 px-2 py-1 bg-[#2e2e2e] rounded-md text-[10px] text-gray-300 border border-[#3e3e3e]">
                <span className="font-bold text-[#3ecf8e]">{f.column}</span>
                <span className="opacity-50">{f.operator}</span>
                <span className="font-medium text-white truncate max-w-[80px]">
                  {Array.isArray(f.value) ? f.value.join(', ') : f.value}
                </span>
                <button onClick={() => removeFilter(f.id)} className="hover:text-rose-400">
                  <X className="w-2.5 h-2.5" />
                </button>
              </div>
            ))}
          </div>

          <div className="relative" ref={filterRef}>
            <button
              onClick={() => setShowFilterBuilder(!showFilterBuilder)}
              className={`text-[11px] font-medium px-2.5 py-1.5 flex items-center gap-1.5 border border-[#2e2e2e] rounded transition-colors ${activeFilters.length > 0 ? 'bg-[#3ecf8e22] border-[#3ecf8e44] text-[#3ecf8e]' : 'text-gray-300 hover:text-white hover:bg-[#2e2e2e]'}`}
            >
              <Filter className="w-3.5 h-3.5" />
              Filters {activeFilters.length > 0 && `(${activeFilters.length})`}
            </button>

            {showFilterBuilder && (
              <div className="absolute top-full mt-2 left-0 w-[480px] bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl shadow-2xl z-50 p-5 ring-1 ring-black/50">
                <div className="flex items-center justify-between mb-4 pb-2 border-b border-[#2e2e2e]">
                  <div className="flex items-center gap-2">
                    <Filter className="w-3.5 h-3.5 text-[#3ecf8e]" />
                    <span className="text-[11px] font-bold text-gray-200 uppercase tracking-widest">Filter Conditions</span>
                  </div>
                  <button onClick={() => onFiltersChange([])} className="text-[10px] text-rose-400 hover:text-rose-300 font-bold uppercase transition-colors">Clear all</button>
                </div>

                <div className="space-y-3 max-h-[400px] overflow-auto custom-scrollbar mb-4 pr-1">
                  {activeFilters.length === 0 ? (
                    <div className="text-center py-8 opacity-40">
                      <Filter className="w-8 h-8 mx-auto mb-2 text-gray-400" />
                      <p className="text-[11px] text-gray-400">No active filters. Add a condition to start filtering.</p>
                    </div>
                  ) : (
                    activeFilters.map((filter: FilterCondition) => (
                      <div key={filter.id} className="flex flex-col gap-3 p-3 bg-[#0e0e0e] border border-[#2e2e2e] rounded-xl relative group hover:border-[#3ecf8e44] transition-all">
                        <button onClick={() => removeFilter(filter.id)} className="absolute top-2 right-2 p-1.5 text-gray-500 hover:text-rose-400 transition-colors">
                          <X className="w-3.5 h-3.5" />
                        </button>

                        <div className="flex items-center gap-3">
                          <div className="flex-1">
                            <label className="text-[9px] font-bold text-gray-500 uppercase mb-1 block">Column</label>
                            <select
                              value={filter.column}
                              onChange={(e) => updateFilter(filter.id, { column: e.target.value })}
                              className="w-full bg-[#1c1c1c] border border-[#2e2e2e] text-[11px] rounded-lg px-2.5 py-1.5 text-gray-200 outline-none focus:border-[#3ecf8e] transition-all"
                            >
                              {columns.map((c: any) => (
                                <option key={c.key} value={c.key}>{c.label}</option>
                              ))}
                            </select>
                          </div>

                          <div className="w-32">
                            <label className="text-[9px] font-bold text-gray-500 uppercase mb-1 block">Operator</label>
                            <select
                              value={filter.operator}
                              onChange={(e) => updateFilter(filter.id, { operator: e.target.value as FilterOperator })}
                              className="w-full bg-[#1c1c1c] border border-[#2e2e2e] text-[11px] rounded-lg px-2.5 py-1.5 text-gray-200 outline-none focus:border-[#3ecf8e] disabled:opacity-50 transition-all"
                              disabled={filter.column === 'status'}
                            >
                              <option value="equals">is</option>
                              <option value="contains">contains</option>
                              <option value="starts_with">starts with</option>
                              <option value="greater_than">&gt;</option>
                              <option value="less_than">&lt;</option>
                            </select>
                          </div>
                        </div>

                        <div>
                          <label className="text-[9px] font-bold text-gray-500 uppercase mb-1 block">Value</label>
                          {filter.column === 'status' ? (
                            <div className="grid grid-cols-3 gap-2">
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
                                    className={`px-2 py-1.5 rounded-lg text-[10px] font-bold uppercase transition-all border text-center ${isChecked ? 'bg-[#3ecf8e22] border-[#3ecf8e] text-[#3ecf8e]' : 'bg-[#1c1c1c] border-[#2e2e2e] text-gray-500 hover:border-gray-600'}`}
                                  >
                                    {opt}
                                  </button>
                                );
                              })}
                            </div>
                          ) : filter.column === 'confidence' ? (
                            <select
                              value={filter.value}
                              onChange={(e) => updateFilter(filter.id, { value: e.target.value })}
                              className="w-full bg-[#1c1c1c] border border-[#2e2e2e] text-[11px] rounded-lg px-2.5 py-1.5 text-gray-200 outline-none focus:border-[#3ecf8e] transition-all"
                            >
                              {confidenceOptions.map(opt => (
                                <option key={opt} value={opt}>{opt}</option>
                              ))}
                            </select>
                          ) : (
                            <input
                              type="text"
                              placeholder="Type value..."
                              value={filter.value}
                              onChange={(e) => updateFilter(filter.id, { value: e.target.value })}
                              className="w-full bg-[#1c1c1c] border border-[#2e2e2e] text-[11px] rounded-lg px-2.5 py-1.5 text-gray-200 focus:border-[#3ecf8e] outline-none transition-all placeholder:text-gray-600"
                            />
                          )}
                        </div>
                      </div>
                    ))
                  )}
                </div>

                <button
                  onClick={addFilter}
                  className="w-full flex items-center justify-center gap-2 py-2.5 border border-dashed border-[#2e2e2e] rounded-xl text-[11px] font-bold text-gray-400 hover:text-[#3ecf8e] hover:border-[#3ecf8e] hover:bg-[#3ecf8e]/5 transition-all"
                >
                  <Plus className="w-3.5 h-3.5" /> Add New Condition
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
          <div className="flex items-center gap-1.5 text-gray-400">
            <span>Page</span>
            <input
              type="number"
              min={1}
              max={totalPages}
              value={currentPage}
              onChange={(e) => {
                const val = parseInt(e.target.value);
                if (!isNaN(val) && val >= 1 && val <= totalPages) {
                  onPageChange(val);
                }
              }}
              className="w-10 bg-[#0e0e0e] border border-[#2e2e2e] rounded px-1.5 py-0.5 text-white font-bold text-center focus:border-[#3ecf8e] outline-none [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
            />
            <span>of {totalPages}</span>
          </div>
          <button disabled={currentPage >= totalPages} onClick={() => onPageChange(currentPage + 1)} className="p-1 text-gray-400 hover:text-white disabled:opacity-20"><ChevronRight className="w-4 h-4" /></button>
        </div>
        <div className="flex items-center gap-4 text-gray-500 relative" ref={menuRef}>
          <button onClick={() => setShowPageSizeMenu(!showPageSizeMenu)} className="px-2 py-0.5 border border-[#2e2e2e] rounded bg-[#0e0e0e] text-white font-bold">{pageSize} rows</button>
          {showPageSizeMenu && (
            <div className="absolute bottom-full mb-1 right-0 w-24 bg-[#1c1c1c] border border-[#2e2e2e] rounded py-1 shadow-2xl">
              {[25, 50, 100, 500, 1000].map(s => (<button key={s} onClick={() => { onPageSizeChange(s); onPageChange(1); setShowPageSizeMenu(false); }} className={`w-full text-left px-3 py-1 hover:bg-[#2e2e2e] ${pageSize === s ? 'text-[#3ecf8e]' : ''}`}>{s}</button>))}
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

function PipelineMonitor({ stats, logs, resumePendingQueue, stopEnrichment, isStopping, setLogs, logContainerRef }: any) {
  return (
    <div className="flex-1 p-6 overflow-hidden bg-[#1c1c1c] flex flex-col h-full min-h-0">
      <div className="max-w-5xl mx-auto w-full flex-1 flex flex-col space-y-6 min-h-0 h-full">
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
        <div
          ref={logContainerRef}
          className="bg-[#0e0e0e] rounded-xl border border-[#2e2e2e] p-0 font-mono text-[11px] flex-1 overflow-y-auto custom-scrollbar shadow-inner mb-6 relative ring-1 ring-white/5 min-h-0"
        >
          {logs.length === 0 ? (
            <div className="h-full flex flex-col items-center justify-center opacity-30 text-center text-gray-500 py-20">
              <DatabaseZap className="w-12 h-12 mb-4 animate-pulse" />
              <p className="text-sm font-bold">Pipeline Idle</p>
              <p className="mt-2 text-[10px] uppercase tracking-wider">Queue records to begin processing</p>
            </div>
          ) : (
            <div className="flex flex-col-reverse min-h-full">
              {logs.map((l: any, i: number) => {
                const date = new Date(l.timestamp);
                const timeStr = date.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
                const isError = l.level === 'error';
                const isWarn = l.level === 'warn';
                const isPhase = l.level === 'phase';

                return (
                  <div key={l.id || i} className={`group flex items-start gap-4 px-4 py-1 border-b border-white/5 hover:bg-white/[0.02] transition-colors
                            ${isError ? 'bg-rose-500/10 border-rose-500/20 text-rose-300' : ''}
                            ${isWarn ? 'text-amber-300' : ''}
                            ${isPhase ? 'bg-indigo-500/5 text-indigo-300 font-bold' : 'text-gray-400'}
                          `}>
                    <span className="opacity-30 shrink-0 select-none w-16">{timeStr}</span>
                    <span className="opacity-20 shrink-0 select-none w-12 text-[10px] font-bold">[{l.instance_id}]</span>
                    <span className={`shrink-0 w-20 flex items-center gap-1.5 font-bold uppercase text-[9px] tracking-wider
                              ${l.module === 'Scraper' ? 'text-blue-400' : ''}
                              ${l.module === 'OpenAI' ? 'text-[#3ecf8e]' : ''}
                              ${l.module === 'Sync' ? 'text-purple-400' : ''}
                              ${l.module === 'Pipeline' ? 'text-indigo-400' : ''}
                            `}>
                      {l.module === 'Scraper' && <Zap className="w-2.5 h-2.5" />}
                      {l.module === 'OpenAI' && <Cpu className="w-2.5 h-2.5" />}
                      {l.module === 'Sync' && <Database className="w-2.5 h-2.5" />}
                      {l.module === 'Pipeline' && <Activity className="w-2.5 h-2.5" />}
                      {l.module}
                    </span>
                    <span className="flex-1 break-words leading-relaxed py-0.5">
                      {l.message}
                    </span>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function ProxyStatsDashboard() {
  const [stats, setStats] = useState<{ proxy_used: string, success_count: number }[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/stats/proxies')
      .then(res => res.json())
      .then(data => {
        // Some robust checking in case db payload is weird
        if (Array.isArray(data)) {
          setStats(data);
        }
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const totalSuccesses = stats.reduce((sum, s) => sum + s.success_count, 0);

  return (
    <div className="flex-1 p-8 overflow-y-auto custom-scrollbar bg-[#1c1c1c]">
      <div className="max-w-4xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h2 className="text-2xl font-bold flex items-center gap-3 text-white">
              <BarChart2 className="w-6 h-6 text-[#3ecf8e]" />
              Proxy Performance Analytics
            </h2>
            <p className="text-sm text-gray-500 mt-2">Historical success rates across all scraping tiers.</p>
          </div>
          <button
            onClick={() => {
              setLoading(true);
              fetch('/api/stats/proxies')
                .then(res => res.json())
                .then(setStats)
                .finally(() => setLoading(false));
            }}
            className="flex items-center gap-2 px-4 py-2 bg-[#2e2e2e] hover:bg-[#3e3e3e] border border-[#3e3e3e] text-white rounded-lg font-bold text-xs transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin text-[#3ecf8e]' : ''}`} />
            Refresh
          </button>
        </div>

        {loading ? (
          <div className="flex flex-col items-center justify-center p-20 opacity-50">
            <Loader2 className="w-8 h-8 animate-spin text-[#3ecf8e] mb-4" />
            <p className="text-sm font-bold tracking-widest uppercase">Loading Analytics...</p>
          </div>
        ) : stats.length === 0 ? (
          <div className="flex flex-col items-center justify-center p-20 opacity-30 border border-dashed border-[#2e2e2e] rounded-xl">
            <BarChart2 className="w-12 h-12 mb-4 text-gray-400" />
            <p className="text-sm font-bold">No Proxy Data Yet</p>
            <p className="text-xs mt-2">Run the enrichment pipeline to start tracking proxy usage.</p>
          </div>
        ) : (
          <div className="space-y-6">
            <div className="grid grid-cols-2 gap-6 mb-8">
              <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-6 shadow-sm">
                <p className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">Total Successful Scrapes</p>
                <p className="text-4xl font-bold text-white">{totalSuccesses.toLocaleString()}</p>
              </div>
              <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-6 shadow-sm">
                <p className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">Top Performing Proxy</p>
                <div className="flex items-center gap-3 mt-1">
                  <p className="text-2xl font-bold text-[#3ecf8e] truncate flex-1">{stats[0]?.proxy_used}</p>
                  <span className="text-sm font-bold px-2 py-1 bg-[#3ecf8e]/10 text-[#3ecf8e] rounded-md border border-[#3ecf8e]/20">
                    {Math.round((stats[0]?.success_count / totalSuccesses) * 100)}%
                  </span>
                </div>
              </div>
            </div>

            <div className="bg-[#0e0e0e] border border-[#2e2e2e] rounded-xl p-6">
              <h3 className="text-xs font-bold uppercase tracking-widest text-gray-400 border-b border-[#2e2e2e] pb-4 mb-6">Volume Leaderboard</h3>

              <div className="space-y-5">
                {stats.map((stat, idx) => {
                  const percentage = Math.round((stat.success_count / totalSuccesses) * 100);
                  const isPremium = stat.proxy_used.toLowerCase().includes('premium');
                  return (
                    <div key={stat.proxy_used} className="group">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-3">
                          <span className="text-xs font-bold text-gray-600 w-4">{idx + 1}.</span>
                          <span className={`text-sm font-bold ${isPremium ? 'text-amber-400' : 'text-gray-200'}`}>
                            {stat.proxy_used}
                          </span>
                          {isPremium && <span className="text-[9px] px-1.5 py-0.5 rounded border border-amber-500/20 bg-amber-500/10 text-amber-500 uppercase font-bold tracking-wider">Premium Cost</span>}
                        </div>
                        <div className="text-right">
                          <span className="text-sm font-bold text-white">{stat.success_count.toLocaleString()}</span>
                          <span className="text-xs text-gray-500 ml-2 block">scrapes ({percentage}%)</span>
                        </div>
                      </div>
                      <div className="w-full h-2 bg-[#2e2e2e] rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all duration-1000 ease-out ${isPremium ? 'bg-amber-400 shadow-[0_0_10px_rgba(251,191,36,0.3)]' : 'bg-[#3ecf8e] shadow-[0_0_10px_rgba(62,207,142,0.3)]'}`}
                          style={{ width: `${percentage}%` }}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

