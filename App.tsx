
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { AppTab, Contact, Enrichment, BatchStats, MergedContact, FilterCondition, FilterOperator } from './types';
import { db } from './services/supabaseClient';
import { enrichBatch } from './services/enrichmentService';
import Papa from 'papaparse';
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
  ChevronDown,
  Clock,
  ListFilter,
  ExternalLink,
  Cpu,
  Activity,
  Play,
  BarChart2,
  Upload,
  FileSpreadsheet,
  ArrowRight,
  ArrowLeft,
  CheckCircle2,
  AlertCircle,
  FileUp,
  Columns3,
  Import
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
  const [activeLogFilter, setActiveLogFilter] = useState('live');
  const [isLiveTail, setIsLiveTail] = useState(true);
  const [hasNewLogs, setHasNewLogs] = useState(false);
  const [isLogDropdownOpen, setIsLogDropdownOpen] = useState(false);
  const lastLogTimeRef = useRef<number>(0);

  const logContainerRef = useRef<HTMLDivElement>(null);

  // Auto-scroll logs to bottom
  useEffect(() => {
    if (logContainerRef.current) {
      if (isLiveTail && activeLogFilter === 'live') {
        logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
        setHasNewLogs(false);
        if (logs.length > 0) lastLogTimeRef.current = new Date(logs[0].timestamp).getTime();
      } else if (!isLiveTail && activeLogFilter === 'live' && logs.length > 0) {
        const latestTime = new Date(logs[0].timestamp).getTime();
        if (latestTime > lastLogTimeRef.current) {
          setHasNewLogs(true);
        }
      }
    }
  }, [logs, isLiveTail, activeLogFilter]);

  const handleLogScroll = (e: React.UIEvent<HTMLDivElement>) => {
    const { scrollTop, scrollHeight, clientHeight } = e.currentTarget;
    const isAtBottom = scrollHeight - scrollTop - clientHeight < 20;

    if (activeLogFilter === 'live') {
      if (isAtBottom && !isLiveTail) {
        setIsLiveTail(true);
        setHasNewLogs(false);
        if (logs.length > 0) lastLogTimeRef.current = new Date(logs[0].timestamp).getTime();
      } else if (!isAtBottom && isLiveTail) {
        setIsLiveTail(false);
      }
    }
  };
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [isAllFilteredSelected, setIsAllFilteredSelected] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');
  const [isStopping, setIsStopping] = useState(false);
  const [leadListOptions, setLeadListOptions] = useState<string[]>([]);

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

  // Fetch distinct lead_list_name values once at app level
  useEffect(() => {
    fetch('/api/distinct/lead_list_name')
      .then(res => res.json())
      .then(data => { if (Array.isArray(data)) setLeadListOptions(data); })
      .catch(() => { });
  }, []);

  // Poll server for status — only when on the Pipeline tab or actively processing
  useEffect(() => {
    // Skip polling entirely when on the Contacts tab and nothing is processing
    const shouldPoll = activeTab === AppTab.ENRICHMENT || stats.isProcessing;
    if (!shouldPoll) return;

    const fetchStatus = async () => {
      try {
        const res = await fetch(`/api/status?timeRange=${activeLogFilter}`);
        if (res.ok) {
          const data = await res.json();
          setLogs(data.logs);
          setStats(data.stats);
        }
      } catch (e) {
        console.error("Status fetch error:", e);
      }
    };

    fetchStatus(); // Fetch once on mount/tab change

    const interval = setInterval(fetchStatus, 2500);

    return () => {
      clearInterval(interval);
    };
  }, [stats.isProcessing, activeTab, activeLogFilter]);

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
    let payload: any = {};
    let totalBatchSize = 0;

    if (isAllFilteredSelected) {
      addLog(`📦 Generating database query for all ${totalCount} matching contacts natively on the backend...`);
      payload = { filters: activeFilters, searchQuery: debouncedSearchQuery };
      totalBatchSize = totalCount;
    } else {
      const selectedContacts = contacts.filter(c => selectedIds.has(c.contact_id));
      if (selectedContacts.length === 0) return;
      payload = { contactIds: selectedContacts.map(b => b.contact_id) };
      totalBatchSize = selectedContacts.length;
    }

    if (totalBatchSize === 0) return;

    try {
      addLog(`🚀 Deploying background enrichment cluster for ${totalBatchSize} records...`);

      const response = await fetch('/api/enrich', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errData = await response.json();
        throw new Error(`Server Error: ${errData.error || response.status}`);
      }

      addLog(`✅ 202 Accepted: Backend cluster is scaling up to process ${totalBatchSize} records asynchronously.`);
      setActiveTab(AppTab.ENRICHMENT);
      setSelectedIds(new Set());
      setIsAllFilteredSelected(false);

      // Initialize UI Stats temporarily until the polling grabs the real data 5s later
      setStats({ total: totalBatchSize, completed: 0, failed: 0, isProcessing: true });

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
          <SidebarIconButton active={activeTab === AppTab.IMPORT} onClick={() => setActiveTab(AppTab.IMPORT)} icon={<Upload className="w-5 h-5" />} label="Import CSV" />
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
          ) : activeTab === AppTab.IMPORT ? (
            <CSVImportWizard onComplete={() => { setActiveTab(AppTab.MANAGER); loadData(); }} />
          ) : activeTab === AppTab.ENRICHMENT ? (
            <PipelineMonitor
              stats={stats}
              logs={logs}
              resumePendingQueue={resumePendingQueue}
              stopEnrichment={stopEnrichment}
              isStopping={isStopping}
              setLogs={setLogs}
              logContainerRef={logContainerRef}
              activeLogFilter={activeLogFilter}
              setActiveLogFilter={setActiveLogFilter}
              isLiveTail={isLiveTail}
              setIsLiveTail={setIsLiveTail}
              hasNewLogs={hasNewLogs}
              setHasNewLogs={setHasNewLogs}
              isLogDropdownOpen={isLogDropdownOpen}
              setIsLogDropdownOpen={setIsLogDropdownOpen}
              handleLogScroll={handleLogScroll}
            />
          ) : activeTab === AppTab.MANAGER ? (
            <DataTable
              data={contacts}
              activeFilters={activeFilters}
              onFiltersChange={setActiveFilters}
              columns={[
                { key: 'contact_id', label: 'contact_id', type: 'uuid', defaultWidth: 140 },
                { key: 'lead_list_name', label: 'lead_list', type: 'text', defaultWidth: 150 },
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
              leadListOptions={leadListOptions}
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
  onSearchQueryChange,
  leadListOptions = []
}: any) {
  const [showPageSizeMenu, setShowPageSizeMenu] = useState(false);
  const [showFilterBuilder, setShowFilterBuilder] = useState(false);
  const totalPages = Math.ceil(totalCount / pageSize) || 1;
  const menuRef = useRef<HTMLDivElement>(null);
  const filterRef = useRef<HTMLDivElement>(null);

  // leadListOptions comes from props (fetched once at App level)

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
        if (next.column === 'lead_list_name' && !Array.isArray(next.value)) {
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
                              disabled={filter.column === 'status' || filter.column === 'lead_list_name'}
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
                          ) : filter.column === 'lead_list_name' ? (
                            <div className="grid grid-cols-3 gap-2 max-h-[200px] overflow-y-auto custom-scrollbar">
                              {leadListOptions.length === 0 ? (
                                <p className="text-[10px] text-gray-600 col-span-3 text-center py-2">No lead lists found</p>
                              ) : leadListOptions.map(opt => {
                                const isChecked = Array.isArray(filter.value) && filter.value.includes(opt);
                                return (
                                  <button
                                    key={opt}
                                    onClick={() => {
                                      const current = Array.isArray(filter.value) ? filter.value : [];
                                      const next = isChecked ? current.filter((s: string) => s !== opt) : [...current, opt];
                                      updateFilter(filter.id, { value: next });
                                    }}
                                    className={`px-2 py-1.5 rounded-lg text-[10px] font-bold transition-all border text-center truncate ${isChecked ? 'bg-[#3ecf8e22] border-[#3ecf8e] text-[#3ecf8e]' : 'bg-[#1c1c1c] border-[#2e2e2e] text-gray-500 hover:border-gray-600'}`}
                                    title={opt}
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

function PipelineMonitor({
  stats,
  logs,
  resumePendingQueue,
  stopEnrichment,
  isStopping,
  setLogs,
  logContainerRef,
  activeLogFilter,
  setActiveLogFilter,
  isLiveTail,
  setIsLiveTail,
  hasNewLogs,
  setHasNewLogs,
  isLogDropdownOpen,
  setIsLogDropdownOpen,
  handleLogScroll
}: any) {
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

            {/* Log Filter Dropdown */}
            <div className="relative">
              <button onClick={() => setIsLogDropdownOpen(!isLogDropdownOpen)} className="flex items-center gap-2 px-3 py-2 text-gray-300 hover:text-white bg-[#222] hover:bg-[#2e2e2e] rounded-lg text-xs border border-[#333] transition-colors relative">
                {activeLogFilter === 'live' ? <Activity className="w-3.5 h-3.5 text-[#3ecf8e]" /> : <Clock className="w-3.5 h-3.5 text-indigo-400" />}
                {activeLogFilter === 'live' ? 'Live Tail' : activeLogFilter === '1h' ? 'Last hour' : activeLogFilter === '4h' ? 'Last 4 hours' : activeLogFilter === '24h' ? 'Last 24 hours' : activeLogFilter === '2d' ? 'Last 2 days' : activeLogFilter === '7d' ? 'Last 7 days' : activeLogFilter === '14d' ? 'Last 14 days' : 'Last 30 days'}
                <ChevronDown className={`w-3.5 h-3.5 opacity-50 transition-transform ${isLogDropdownOpen ? 'rotate-180' : ''}`} />
              </button>

              {isLogDropdownOpen && (
                <div className="absolute right-0 top-full mt-2 w-48 bg-[#1e1e1e] border border-[#333] rounded-xl shadow-2xl py-1 z-50 overflow-hidden">
                  {[
                    { id: 'live', label: 'Live Tail', icon: Activity },
                    { id: '1h', label: 'Last hour', icon: Clock },
                    { id: '4h', label: 'Last 4 hours', icon: Clock },
                    { id: '24h', label: 'Last 24 hours', icon: Clock },
                    { id: '2d', label: 'Last 2 days', icon: Clock },
                    { id: '7d', label: 'Last 7 days', icon: Clock },
                    { id: '14d', label: 'Last 14 days', icon: Clock },
                    { id: '30d', label: 'Last 30 days', icon: Clock }
                  ].map(opt => (
                    <button
                      key={opt.id}
                      onClick={() => { setActiveLogFilter(opt.id); setIsLogDropdownOpen(false); }}
                      className={`w-full text-left px-4 py-2 text-xs flex items-center gap-3 transition-colors ${activeLogFilter === opt.id ? 'bg-indigo-500/10 text-indigo-300 font-bold' : 'text-gray-400 hover:bg-[#2e2e2e] hover:text-white'}`}
                    >
                      <opt.icon className={`w-3.5 h-3.5 ${activeLogFilter === opt.id ? 'text-indigo-400' : 'opacity-40'}`} />
                      {opt.label}
                    </button>
                  ))}
                </div>
              )}
            </div>

            <button onClick={() => setLogs([])} className="flex items-center gap-2 px-3 py-2 text-gray-400 hover:text-white hover:bg-[#2e2e2e] rounded-lg text-xs border border-[#2e2e2e] transition-colors">
              <Trash2 className="w-3.5 h-3.5" /> Clear Logs
            </button>
          </div>
        </div>
        <div className="grid grid-cols-3 gap-6">
          <StatCard label="Completed" value={stats.completed} color="text-[#3ecf8e]" />
          <StatCard label="Failures" value={stats.failed} color="text-rose-500" />
          <StatCard label="In Queue" value={stats.total - (stats.completed + stats.failed)} color="text-indigo-400" />
        </div>
        <div className="relative flex-1 min-h-0 mb-6 group text-xs text-mono">
          <div
            ref={logContainerRef}
            onScroll={handleLogScroll}
            className="absolute inset-0 bg-[#0e0e0e] rounded-xl border border-[#2e2e2e] p-0 font-mono text-[11px] overflow-y-auto custom-scrollbar shadow-inner ring-1 ring-white/5"
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

          {/* Floating Action Button for New Logs */}
          <div className={`absolute bottom-6 left-1/2 -translate-x-1/2 transition-all duration-300 z-20 ${hasNewLogs && !isLiveTail && activeLogFilter === 'live' ? 'opacity-100 translate-y-0 scale-100' : 'opacity-0 translate-y-4 scale-95 pointer-events-none'}`}>
            <button onClick={() => { setIsLiveTail(true); setHasNewLogs(false); if (logContainerRef.current) logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight; }} className="bg-[#111] border border-gray-600 hover:border-gray-400 text-gray-200 hover:text-white rounded-md px-4 py-2 shadow-2xl text-xs font-semibold flex items-center gap-2.5 transition-all">
              <Activity className="w-3.5 h-3.5 text-[#3ecf8e] animate-pulse" />
              New logs in Live Tail
              <div className="w-px h-3 bg-gray-600 mx-1"></div>
              <X className="w-3.5 h-3.5 text-gray-400 hover:text-white" onClick={(e) => { e.stopPropagation(); setHasNewLogs(false); }} />
            </button>
          </div>

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

// ============================================
// CSV IMPORT WIZARD COMPONENT
// ============================================

const CONTACTS_FIELDS = [
  { key: 'contact_id', label: 'contact_id', description: 'UUID identifier' },
  { key: 'lead_list_name', label: 'lead_list_name', description: 'Lead list source' },
  { key: 'first_name', label: 'first_name', description: 'First name' },
  { key: 'last_name', label: 'last_name', description: 'Last name' },
  { key: 'email', label: 'email', description: 'Email address' },
  { key: 'company_website', label: 'company_website', description: 'Company domain' },
  { key: 'company_name', label: 'company_name', description: 'Company name' },
  { key: 'industry', label: 'industry', description: 'Industry vertical' },
  { key: 'linkedin_url', label: 'linkedin_url', description: 'LinkedIn profile URL' },
  { key: 'title', label: 'title', description: 'Job title' },
];

function CSVImportWizard({ onComplete }: { onComplete: () => void }) {
  const [step, setStep] = useState<1 | 2 | 3>(1);
  const [file, setFile] = useState<File | null>(null);
  const [csvHeaders, setCsvHeaders] = useState<string[]>([]);
  const [previewRows, setPreviewRows] = useState<Record<string, string>[]>([]);
  const [mapping, setMapping] = useState<Record<string, string>>({});
  const [totalRows, setTotalRows] = useState(0);
  const [importProgress, setImportProgress] = useState(0);
  const [importStatus, setImportStatus] = useState<'idle' | 'importing' | 'done' | 'error'>('idle');
  const [importResult, setImportResult] = useState<{ inserted: number; duplicates: number; failed: number; errors: string[]; failedContacts: { email: string; row: number; reason: string }[] }>({ inserted: 0, duplicates: 0, failed: 0, errors: [], failedContacts: [] });
  const [dragOver, setDragOver] = useState(false);
  const [showFailedModal, setShowFailedModal] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Auto-map CSV headers to contacts fields (case-insensitive, common aliases)
  const autoMap = (headers: string[]) => {
    const aliases: Record<string, string[]> = {
      email: ['email', 'e-mail', 'email_address', 'emailaddress', 'mail'],
      first_name: ['first_name', 'firstname', 'first name', 'fname', 'given_name'],
      last_name: ['last_name', 'lastname', 'last name', 'lname', 'surname', 'family_name'],
      company_website: ['company_website', 'website', 'domain', 'url', 'company_domain', 'company_url', 'web'],
      company_name: ['company_name', 'company', 'organization', 'organisation', 'org'],
      industry: ['industry', 'sector', 'vertical'],
      linkedin_url: ['linkedin_url', 'linkedin', 'linkedin_profile', 'li_url'],
      title: ['title', 'job_title', 'jobtitle', 'position', 'role'],
      lead_list_name: ['lead_list_name', 'lead_list', 'list_name', 'list', 'source'],
      contact_id: ['contact_id', 'contactid', 'id', 'uuid'],
    };

    const result: Record<string, string> = {};
    const usedTargets = new Set<string>();

    headers.forEach(header => {
      const normalized = header.toLowerCase().trim().replace(/[\s-]+/g, '_');
      for (const [field, aliasList] of Object.entries(aliases)) {
        if (!usedTargets.has(field) && aliasList.includes(normalized)) {
          result[header] = field;
          usedTargets.add(field);
          break;
        }
      }
      if (!result[header]) {
        result[header] = '__skip__';
      }
    });
    return result;
  };

  const handleFileSelect = (selectedFile: File) => {
    if (!selectedFile || !selectedFile.name.endsWith('.csv')) return;
    setFile(selectedFile);
    setStep(1);
    setImportStatus('idle');
    setImportResult({ inserted: 0, errors: [] });
    setImportProgress(0);

    // Count total rows first (streaming, just counting)
    let rowCount = 0;
    Papa.parse(selectedFile, {
      header: true,
      skipEmptyLines: true,
      step: () => { rowCount++; },
      complete: () => { setTotalRows(rowCount); }
    });

    // Parse preview (first 5 rows)
    Papa.parse(selectedFile, {
      header: true,
      preview: 5,
      skipEmptyLines: true,
      complete: (results: Papa.ParseResult<Record<string, string>>) => {
        const headers = results.meta.fields || [];
        setCsvHeaders(headers);
        setPreviewRows(results.data);
        setMapping(autoMap(headers));
        setStep(2);
      }
    });
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const f = e.dataTransfer.files[0];
    if (f) handleFileSelect(f);
  };

  const updateMapping = (csvHeader: string, targetField: string) => {
    setMapping(prev => {
      const next = { ...prev };
      // If another CSV col is already mapped to this target, unmap it
      if (targetField !== '__skip__') {
        Object.keys(next).forEach(k => {
          if (next[k] === targetField && k !== csvHeader) {
            next[k] = '__skip__';
          }
        });
      }
      next[csvHeader] = targetField;
      return next;
    });
  };

  const mappedFields = Object.values(mapping).filter(v => v !== '__skip__');
  const hasRequiredField = mappedFields.includes('email') || mappedFields.includes('company_website');

  const startImport = () => {
    if (!file) return;
    setStep(3);
    setImportStatus('importing');
    setImportProgress(0);
    setImportResult({ inserted: 0, duplicates: 0, failed: 0, errors: [], failedContacts: [] });

    const activeMappings: [string, string][] = (Object.entries(mapping) as [string, string][]).filter(([_, v]) => v !== '__skip__');
    let buffer: any[] = [];
    let processed = 0;
    let totalInserted = 0;
    let totalDuplicates = 0;
    let totalFailed = 0;
    const errors: string[] = [];
    const allFailedContacts: { email: string; row: number; reason: string }[] = [];
    const CHUNK_SIZE = 2000;

    const sendChunk = async (chunk: any[]): Promise<void> => {
      try {
        const res = await fetch('/api/import', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ contacts: chunk })
        });
        const data = await res.json();
        if (!res.ok) {
          errors.push(data.error || `HTTP ${res.status}`);
        } else {
          totalInserted += data.inserted || 0;
          totalDuplicates += data.duplicates || 0;
          totalFailed += data.failed || 0;
          if (data.errors?.length) errors.push(...data.errors);
          if (data.failedContacts?.length) allFailedContacts.push(...data.failedContacts);
        }
      } catch (err: any) {
        errors.push(err.message);
      }
    };

    // Use a queue approach to handle async sending while streaming
    let sendPromise = Promise.resolve();

    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      step: (row: Papa.ParseStepResult<Record<string, string>>) => {
        const mapped: any = {};
        activeMappings.forEach(([csvCol, targetCol]) => {
          const val = row.data[csvCol];
          if (val !== undefined && val !== null && val !== '') {
            mapped[targetCol] = val;
          }
        });

        // Only include rows that have at least one mapped value
        if (Object.keys(mapped).length > 0) {
          buffer.push(mapped);
        }

        processed++;

        if (buffer.length >= CHUNK_SIZE) {
          const chunk = [...buffer];
          buffer = [];
          sendPromise = sendPromise.then(() => sendChunk(chunk)).then(() => {
            setImportProgress(processed);
            setImportResult({ inserted: totalInserted, duplicates: totalDuplicates, failed: totalFailed, errors: [...errors], failedContacts: [...allFailedContacts] });
          });
        }
      },
      complete: () => {
        // Send remaining buffer
        if (buffer.length > 0) {
          const chunk = [...buffer];
          buffer = [];
          sendPromise = sendPromise.then(() => sendChunk(chunk));
        }

        sendPromise.then(() => {
          setImportProgress(processed);
          setImportResult({ inserted: totalInserted, duplicates: totalDuplicates, failed: totalFailed, errors: [...errors], failedContacts: [...allFailedContacts] });
          setImportStatus(totalFailed > 0 || errors.length > 0 ? 'error' : 'done');
        });
      },
      error: (err: Error) => {
        errors.push(`Parse error: ${err.message}`);
        setImportResult({ inserted: totalInserted, duplicates: totalDuplicates, failed: totalFailed, errors: [...errors], failedContacts: [...allFailedContacts] });
        setImportStatus('error');
      }
    });
  };

  return (
    <div className="flex-1 p-6 overflow-y-auto custom-scrollbar bg-[#1c1c1c]">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h2 className="text-2xl font-bold flex items-center gap-3 text-white">
            <FileSpreadsheet className="w-6 h-6 text-[#3ecf8e]" />
            Import Contacts
          </h2>
          <p className="text-sm text-gray-500 mt-2">Upload a CSV file to add contacts for enrichment. Supports files up to 50MB.</p>
        </div>

        {/* Step Indicator */}
        <div className="flex items-center gap-3 mb-8">
          {[{ n: 1, label: 'Upload', icon: FileUp }, { n: 2, label: 'Map Fields', icon: Columns3 }, { n: 3, label: 'Import', icon: Import }].map((s, idx) => (
            <React.Fragment key={s.n}>
              <div className={`flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-bold uppercase tracking-wider transition-all ${step === s.n
                ? 'bg-[#3ecf8e]/10 text-[#3ecf8e] border border-[#3ecf8e]/30'
                : step > s.n
                  ? 'bg-[#3ecf8e]/5 text-[#3ecf8e]/60 border border-[#3ecf8e]/10'
                  : 'bg-[#0e0e0e] text-gray-600 border border-[#2e2e2e]'
                }`}>
                <s.icon className="w-3.5 h-3.5" />
                {s.label}
              </div>
              {idx < 2 && <div className={`flex-1 h-px ${step > s.n ? 'bg-[#3ecf8e]/30' : 'bg-[#2e2e2e]'}`} />}
            </React.Fragment>
          ))}
        </div>

        {/* Step 1: Upload */}
        {step === 1 && (
          <div
            className={`border-2 border-dashed rounded-2xl p-16 text-center transition-all cursor-pointer ${dragOver
              ? 'border-[#3ecf8e] bg-[#3ecf8e]/5'
              : 'border-[#2e2e2e] bg-[#0e0e0e] hover:border-[#3ecf8e]/40 hover:bg-[#0e0e0e]/80'
              }`}
            onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              className="hidden"
              onChange={(e) => e.target.files?.[0] && handleFileSelect(e.target.files[0])}
            />
            <Upload className={`w-12 h-12 mx-auto mb-4 ${dragOver ? 'text-[#3ecf8e]' : 'text-gray-600'}`} />
            <p className="text-lg font-bold text-gray-300 mb-2">Drop your CSV file here</p>
            <p className="text-sm text-gray-600">or click to browse • Max 50MB</p>
          </div>
        )}

        {/* Step 2: Field Mapping */}
        {step === 2 && (
          <div className="space-y-6">
            {/* File info */}
            <div className="flex items-center gap-4 p-4 bg-[#0e0e0e] border border-[#2e2e2e] rounded-xl">
              <FileSpreadsheet className="w-8 h-8 text-[#3ecf8e]" />
              <div className="flex-1">
                <p className="text-sm font-bold text-white">{file?.name}</p>
                <p className="text-[10px] text-gray-500 uppercase tracking-wider mt-0.5">
                  {(file?.size ? (file.size / 1024 / 1024).toFixed(2) : '0')} MB • {totalRows.toLocaleString()} rows • {csvHeaders.length} columns
                </p>
              </div>
              <button
                onClick={() => { setStep(1); setFile(null); setCsvHeaders([]); setPreviewRows([]); }}
                className="text-xs text-gray-500 hover:text-white px-3 py-1.5 border border-[#2e2e2e] rounded-lg hover:bg-[#2e2e2e] transition-colors"
              >
                Change File
              </button>
            </div>

            {/* Mapping Table */}
            <div className="bg-[#0e0e0e] border border-[#2e2e2e] rounded-xl overflow-hidden">
              <div className="px-5 py-3 border-b border-[#2e2e2e] flex items-center gap-2">
                <Columns3 className="w-4 h-4 text-[#3ecf8e]" />
                <span className="text-xs font-bold text-gray-300 uppercase tracking-widest">Column Mapping</span>
                <span className="text-[10px] text-gray-600 ml-auto">{mappedFields.length} of {csvHeaders.length} mapped</span>
              </div>

              <div className="divide-y divide-[#2e2e2e] max-h-[400px] overflow-y-auto custom-scrollbar">
                {csvHeaders.map(header => (
                  <div key={header} className="flex items-center gap-4 px-5 py-3 hover:bg-white/[0.02] transition-colors">
                    <div className="flex-1">
                      <span className="text-sm font-medium text-gray-300">{header}</span>
                      {previewRows[0] && (
                        <span className="text-[10px] text-gray-600 ml-3 truncate inline-block max-w-[150px] align-middle" title={previewRows[0][header]}>
                          e.g. "{previewRows[0][header]}"
                        </span>
                      )}
                    </div>
                    <ArrowRight className="w-4 h-4 text-gray-700 shrink-0" />
                    <select
                      value={mapping[header] || '__skip__'}
                      onChange={(e) => updateMapping(header, e.target.value)}
                      className={`w-48 bg-[#1c1c1c] border rounded-lg px-3 py-1.5 text-[11px] outline-none transition-all ${mapping[header] && mapping[header] !== '__skip__'
                        ? 'border-[#3ecf8e]/40 text-[#3ecf8e]'
                        : 'border-[#2e2e2e] text-gray-500'
                        } focus:border-[#3ecf8e]`}
                    >
                      <option value="__skip__">— Skip —</option>
                      {CONTACTS_FIELDS.map(f => (
                        <option key={f.key} value={f.key} disabled={mappedFields.includes(f.key) && mapping[header] !== f.key}>
                          {f.label}
                        </option>
                      ))}
                    </select>
                  </div>
                ))}
              </div>
            </div>

            {/* Validation message */}
            {!hasRequiredField && (
              <div className="flex items-center gap-3 px-4 py-3 bg-amber-500/10 border border-amber-500/20 rounded-xl text-amber-400 text-xs font-medium">
                <AlertCircle className="w-4 h-4 shrink-0" />
                You must map at least <strong>email</strong> or <strong>company_website</strong> to proceed.
              </div>
            )}

            {/* Preview Table */}
            {previewRows.length > 0 && mappedFields.length > 0 && (
              <div className="bg-[#0e0e0e] border border-[#2e2e2e] rounded-xl overflow-hidden">
                <div className="px-5 py-3 border-b border-[#2e2e2e]">
                  <span className="text-xs font-bold text-gray-300 uppercase tracking-widest">Preview (first {previewRows.length} rows)</span>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-[11px]">
                    <thead>
                      <tr className="border-b border-[#2e2e2e]">
                        {Object.entries(mapping).filter(([_, v]) => v !== '__skip__').map(([csvH, target]) => (
                          <th key={csvH} className="px-4 py-2 text-left text-[9px] font-bold text-[#3ecf8e] uppercase tracking-wider">{target}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#2e2e2e]">
                      {previewRows.map((row, i) => (
                        <tr key={i} className="hover:bg-white/[0.02]">
                          {Object.entries(mapping).filter(([_, v]) => v !== '__skip__').map(([csvH]) => (
                            <td key={csvH} className="px-4 py-2 text-gray-400 truncate max-w-[200px]">{row[csvH] || <span className="text-gray-700 italic">empty</span>}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Actions */}
            <div className="flex items-center justify-between pt-2">
              <button
                onClick={() => { setStep(1); setFile(null); setCsvHeaders([]); setPreviewRows([]); }}
                className="flex items-center gap-2 px-4 py-2.5 text-gray-400 hover:text-white border border-[#2e2e2e] rounded-lg text-xs font-bold hover:bg-[#2e2e2e] transition-colors"
              >
                <ArrowLeft className="w-3.5 h-3.5" /> Back
              </button>
              <button
                onClick={startImport}
                disabled={!hasRequiredField}
                className={`flex items-center gap-2 px-6 py-2.5 rounded-lg text-xs font-bold transition-all ${hasRequiredField
                  ? 'bg-[#3ecf8e] text-black hover:bg-[#2fb37a] shadow-lg shadow-[#3ecf8e]/20'
                  : 'bg-[#2e2e2e] text-gray-600 cursor-not-allowed'
                  }`}
              >
                Start Import <ArrowRight className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>
        )}

        {/* Step 3: Import Progress */}
        {step === 3 && (
          <div className="space-y-6">
            <div className="bg-[#0e0e0e] border border-[#2e2e2e] rounded-2xl p-8">
              {importStatus === 'importing' && (
                <div className="text-center">
                  <Loader2 className="w-12 h-12 text-[#3ecf8e] animate-spin mx-auto mb-4" />
                  <p className="text-lg font-bold text-white mb-2">Importing contacts...</p>
                  <p className="text-sm text-gray-500 mb-6">
                    {importProgress.toLocaleString()} of {totalRows.toLocaleString()} rows processed
                  </p>
                  <div className="w-full bg-[#2e2e2e] rounded-full h-3 overflow-hidden mb-2">
                    <div
                      className="h-full bg-gradient-to-r from-[#3ecf8e] to-[#2fb37a] rounded-full transition-all duration-300 shadow-[0_0_12px_rgba(62,207,142,0.4)]"
                      style={{ width: `${totalRows > 0 ? Math.min((importProgress / totalRows) * 100, 100) : 0}%` }}
                    />
                  </div>
                  <p className="text-[10px] text-gray-600 font-mono">
                    {totalRows > 0 ? Math.round((importProgress / totalRows) * 100) : 0}% complete
                  </p>
                </div>
              )}

              {importStatus === 'done' && (
                <div className="text-center">
                  <CheckCircle2 className="w-14 h-14 text-[#3ecf8e] mx-auto mb-4" />
                  <p className="text-xl font-bold text-white mb-2">Import Complete!</p>

                  {/* Stats grid */}
                  <div className="grid grid-cols-3 gap-4 mb-6 max-w-md mx-auto">
                    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-4">
                      <p className="text-2xl font-bold text-[#3ecf8e]">{importResult.inserted.toLocaleString()}</p>
                      <p className="text-[10px] text-gray-500 uppercase tracking-wider font-bold mt-1">Imported</p>
                    </div>
                    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-4">
                      <p className="text-2xl font-bold text-amber-400">{importResult.duplicates.toLocaleString()}</p>
                      <p className="text-[10px] text-gray-500 uppercase tracking-wider font-bold mt-1">Duplicates</p>
                    </div>
                    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-4">
                      <p className="text-2xl font-bold text-gray-600">{importResult.failed.toLocaleString()}</p>
                      <p className="text-[10px] text-gray-500 uppercase tracking-wider font-bold mt-1">Failed</p>
                      {importResult.failed > 0 && (
                        <button
                          onClick={() => setShowFailedModal(true)}
                          className="text-[9px] text-[#3ecf8e] hover:underline mt-1 cursor-pointer"
                        >
                          View details
                        </button>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center justify-center gap-4">
                    <button
                      onClick={() => { setStep(1); setFile(null); setCsvHeaders([]); setPreviewRows([]); setImportStatus('idle'); }}
                      className="flex items-center gap-2 px-4 py-2.5 text-gray-400 hover:text-white border border-[#2e2e2e] rounded-lg text-xs font-bold hover:bg-[#2e2e2e] transition-colors"
                    >
                      <Upload className="w-3.5 h-3.5" /> Import Another
                    </button>
                    <button
                      onClick={onComplete}
                      className="flex items-center gap-2 px-6 py-2.5 bg-[#3ecf8e] text-black rounded-lg text-xs font-bold hover:bg-[#2fb37a] shadow-lg shadow-[#3ecf8e]/20 transition-all"
                    >
                      Go to Contacts <ArrowRight className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </div>
              )}

              {importStatus === 'error' && (
                <div className="text-center">
                  <AlertCircle className="w-14 h-14 text-amber-400 mx-auto mb-4" />
                  <p className="text-xl font-bold text-white mb-2">Import Completed with Errors</p>

                  {/* Stats grid */}
                  <div className="grid grid-cols-3 gap-4 mb-4 max-w-md mx-auto">
                    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-4">
                      <p className="text-2xl font-bold text-[#3ecf8e]">{importResult.inserted.toLocaleString()}</p>
                      <p className="text-[10px] text-gray-500 uppercase tracking-wider font-bold mt-1">Imported</p>
                    </div>
                    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-4">
                      <p className="text-2xl font-bold text-amber-400">{importResult.duplicates.toLocaleString()}</p>
                      <p className="text-[10px] text-gray-500 uppercase tracking-wider font-bold mt-1">Duplicates</p>
                    </div>
                    <div className="bg-[#1c1c1c] border border-[#2e2e2e] rounded-xl p-4">
                      <p className="text-2xl font-bold text-rose-400">{importResult.failed.toLocaleString()}</p>
                      <p className="text-[10px] text-gray-500 uppercase tracking-wider font-bold mt-1">Failed</p>
                      {importResult.failedContacts.length > 0 && (
                        <button
                          onClick={() => setShowFailedModal(true)}
                          className="text-[9px] text-[#3ecf8e] hover:underline mt-1 cursor-pointer"
                        >
                          View details
                        </button>
                      )}
                    </div>
                  </div>

                  <div className="bg-rose-500/10 border border-rose-500/20 rounded-xl p-4 text-left max-h-[200px] overflow-y-auto custom-scrollbar mb-6">
                    {importResult.errors.map((err, i) => (
                      <p key={i} className="text-[11px] text-rose-300 py-1 border-b border-rose-500/10 last:border-0">{err}</p>
                    ))}
                  </div>
                  <div className="flex items-center justify-center gap-4">
                    <button
                      onClick={() => { setStep(1); setFile(null); setCsvHeaders([]); setPreviewRows([]); setImportStatus('idle'); }}
                      className="flex items-center gap-2 px-4 py-2.5 text-gray-400 hover:text-white border border-[#2e2e2e] rounded-lg text-xs font-bold hover:bg-[#2e2e2e] transition-colors"
                    >
                      <Upload className="w-3.5 h-3.5" /> Try Again
                    </button>
                    <button
                      onClick={onComplete}
                      className="flex items-center gap-2 px-6 py-2.5 bg-[#3ecf8e] text-black rounded-lg text-xs font-bold hover:bg-[#2fb37a] shadow-lg shadow-[#3ecf8e]/20 transition-all"
                    >
                      Go to Contacts <ArrowRight className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Failed Contacts Modal */}
      {showFailedModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={() => setShowFailedModal(false)}>
          <div className="bg-[#0e0e0e] border border-[#2e2e2e] rounded-2xl w-full max-w-2xl max-h-[80vh] flex flex-col shadow-2xl" onClick={e => e.stopPropagation()}>
            {/* Modal Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-[#2e2e2e]">
              <div>
                <h3 className="text-sm font-bold text-white">Failed Contacts</h3>
                <p className="text-[10px] text-gray-500 mt-0.5">{importResult.failedContacts.length} contacts failed validation</p>
              </div>
              <button
                onClick={() => setShowFailedModal(false)}
                className="text-gray-500 hover:text-white text-lg font-bold w-8 h-8 flex items-center justify-center rounded-lg hover:bg-[#2e2e2e] transition-colors"
              >
                ×
              </button>
            </div>

            {/* Modal Body */}
            <div className="flex-1 overflow-y-auto custom-scrollbar">
              <table className="w-full text-[11px]">
                <thead className="sticky top-0 bg-[#0e0e0e]">
                  <tr className="border-b border-[#2e2e2e]">
                    <th className="px-6 py-2.5 text-left text-[9px] font-bold text-gray-500 uppercase tracking-wider w-12">#</th>
                    <th className="px-4 py-2.5 text-left text-[9px] font-bold text-gray-500 uppercase tracking-wider">Email</th>
                    <th className="px-4 py-2.5 text-left text-[9px] font-bold text-gray-500 uppercase tracking-wider">Reason</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[#2e2e2e]">
                  {importResult.failedContacts.map((fc, i) => (
                    <tr key={i} className="hover:bg-white/[0.02]">
                      <td className="px-6 py-2.5 text-gray-600 font-mono">{fc.row}</td>
                      <td className="px-4 py-2.5 text-gray-300 font-mono">{fc.email}</td>
                      <td className="px-4 py-2.5 text-rose-400">{fc.reason}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Modal Footer */}
            <div className="px-6 py-3 border-t border-[#2e2e2e] flex justify-end">
              <button
                onClick={() => setShowFailedModal(false)}
                className="px-4 py-2 bg-[#2e2e2e] text-gray-300 rounded-lg text-xs font-bold hover:bg-[#3e3e3e] transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

