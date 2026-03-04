
import './loadEnv';

import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import { db } from './services/supabaseClient';
import { JobProcessor } from './services/jobProcessor';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Structured Log Type
interface LogEntry {
    id?: string;
    timestamp: string;
    instance_id: string;
    module: string;
    message: string;
    level: 'info' | 'warn' | 'error' | 'phase';
}

const INSTANCE_ID = Math.random().toString(36).substring(2, 7); // e.g. "pp9n9"

const app = express();
const PORT = process.env.PORT || 3001;

// Supabase Init (Using SERVICE_ROLE_KEY for background updates)
const SUPABASE_URL = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.VITE_SUPABASE_ANON_KEY || process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL) {
    console.error("❌ CRITICAL: SUPABASE_URL is missing!");
}
if (!SUPABASE_SERVICE_KEY) {
    console.error("❌ CRITICAL: SUPABASE_SERVICE_ROLE_KEY / ANON_KEY is missing!");
}

console.log('🔍 Server Supabase Config Check:', {
    url: SUPABASE_URL ? 'PRESENT' : 'MISSING',
    key: SUPABASE_SERVICE_KEY ? 'PRESENT' : 'MISSING'
});

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY || '');

// --- Real-time Status Tracking ---
let currentJobLogs: LogEntry[] = []; // Cache for current session
let jobStats = {
    total: 0,
    completed: 0,
    failed: 0,
    isProcessing: false,
    queueingPhase: false,
    queued: 0
};

const addServerLog = async (msg: string, module: string = 'Pipeline', level: LogEntry['level'] = 'info') => {
    const entry: LogEntry = {
        timestamp: new Date().toISOString(),
        instance_id: INSTANCE_ID,
        module,
        message: msg,
        level
    };

    console.log(`[${module}] ${msg}`);

    // 1. Internal cache (latest 100)
    currentJobLogs = [entry, ...currentJobLogs].slice(0, 100);

    // 2. Persist to Supabase logs table (fire-and-forget)
    supabase.from('pipeline_logs').insert(entry).then(({ error }) => {
        if (error) console.error("Log persistence failure:", error.message);
    });

    // 3. Sync stats singleton for high-level progress tracking
    if (level === 'phase' || msg.includes('✨') || msg.includes('🚀')) {
        persistPipelineState();
    }
};

async function persistPipelineState() {
    try {
        await supabase.from('pipeline_state').upsert({
            id: 1,
            total: jobStats.total,
            completed: jobStats.completed,
            failed: jobStats.failed,
            is_processing: jobStats.isProcessing,
            updated_at: new Date().toISOString()
        });
    } catch (e) {
        console.error("Failed to persist job stats:", e);
    }
}

async function restorePipelineState() {
    try {
        const { data, error } = await supabase.from('pipeline_state').select('*').eq('id', 1).single();
        if (data && !error) {
            jobStats = {
                total: data.total || 0,
                completed: data.completed || 0,
                failed: data.failed || 0,
                isProcessing: data.is_processing || false,
                queueingPhase: false,
                queued: 0
            };
            console.log("🟢 Restored pipeline stats from Supabase");
        }
    } catch (e) {
        console.warn("Could not restore pipeline state");
    }
}

app.use(cors());
app.use(express.json({ limit: '50mb' }));

// Serve static files from Vite build
app.use(express.static(path.join(__dirname, 'dist')));

/**
 * Background Enrichment Logic (Ported from App.tsx)
 */
/* Legacy runBackgroundEnrichment removed in favor of JobProcessor */

app.get('/api/status', async (req, res) => {
    const { timeRange } = req.query;

    let query = supabase
        .from('pipeline_logs')
        .select('*')
        .order('timestamp', { ascending: false });

    // Apply time filter if requested (e.g. '1h', '24h', '7d')
    if (timeRange && timeRange !== 'live') {
        const hoursMatch = (timeRange as string).match(/(\d+)h/);
        const daysMatch = (timeRange as string).match(/(\d+)d/);

        let msToSubtract = 0;
        if (hoursMatch) msToSubtract = parseInt(hoursMatch[1]) * 60 * 60 * 1000;
        else if (daysMatch) msToSubtract = parseInt(daysMatch[1]) * 24 * 60 * 60 * 1000;

        if (msToSubtract > 0) {
            const cutoff = new Date(Date.now() - msToSubtract).toISOString();
            query = query.gte('timestamp', cutoff);
        }
        query = query.limit(1000); // Allow more logs for historical views
    } else {
        query = query.limit(200); // Standard live tail limit
    }

    const { data: dbLogs } = await query;

    // FETCH LIVE JOB STATS FROM DB (Issue #8: job_items is the single source of truth)
    let liveStats = { ...jobStats };

    // Always fetch real counts from job_items — this is the ground truth
    const [
        { count: completedCount },
        { count: failedCount },
        { count: pendingCount },
    ] = await Promise.all([
        supabase.from('job_items').select('*', { count: 'exact', head: true }).eq('status', 'completed'),
        supabase.from('job_items').select('*', { count: 'exact', head: true }).eq('status', 'failed'),
        supabase.from('job_items').select('*', { count: 'exact', head: true }).in('status', ['pending', 'retrying']),
    ]);

    const dbCompleted = completedCount || 0;
    const dbFailed = failedCount || 0;
    const inQueue = pendingCount || 0;

    // If processing, use DB counts; if not, use whatever memory has (which may be stale but matches last known state)
    if (jobStats.isProcessing || inQueue > 0) {
        liveStats.completed = dbCompleted;
        liveStats.failed = dbFailed;
        liveStats.total = dbCompleted + dbFailed + inQueue;

        // Sync memory
        jobStats.completed = liveStats.completed;
        jobStats.failed = liveStats.failed;
        jobStats.total = liveStats.total;

        // Auto-detect if processing actually finished
        if (inQueue === 0 && !jobStats.queueingPhase) {
            const { count: processingCount } = await supabase
                .from('job_items')
                .select('*', { count: 'exact', head: true })
                .eq('status', 'processing');

            if (!processingCount || processingCount === 0) {
                jobStats.isProcessing = false;
                liveStats.isProcessing = false;
                await supabase.from('pipeline_state').update({ is_processing: false, updated_at: new Date().toISOString() }).eq('id', 1).then();
            }
        }
    }

    res.json({
        logs: dbLogs || currentJobLogs,
        stats: {
            ...liveStats,
            queueingPhase: jobStats.queueingPhase || false,
            queued: jobStats.queued || 0,
            inQueue
        }
    });
});

/**
 * Endpoints
 */

app.post('/api/enrich', async (req, res) => {
    const { contactIds, filters, searchQuery } = req.body;

    if (jobStats.isProcessing || jobStats.queueingPhase) {
        return res.status(409).json({ error: 'A job is already in progress or queueing' });
    }

    if (!contactIds && !filters) {
        return res.status(400).json({ error: 'Provide contactIds or filters' });
    }

    // Set queueing state and respond 202 immediately — no timeout risk
    jobStats = { total: 0, completed: 0, failed: 0, isProcessing: true, queueingPhase: true, queued: 0 };
    currentJobLogs = [];

    res.status(202).json({ message: 'Enrichment queueing started in background' });

    // Fire-and-forget: resolve IDs + create job + insert items in background
    backgroundEnqueue(contactIds, filters, searchQuery).catch(err => {
        console.error('Background enqueue fatal error:', err);
        addServerLog(`❌ Fatal enqueue error: ${err.message}`, 'Pipeline', 'error');
        jobStats.isProcessing = false;
        jobStats.queueingPhase = false;
    });
});

/**
 * Background enqueue: resolves contact IDs from filters, creates job + job_items.
 * Runs AFTER the HTTP response is sent, so there's no timeout risk.
 * Uses the server's service-role supabase client for ID resolution to bypass
 * the 1000-row PostgREST limit that the anon-key db singleton has.
 */
async function backgroundEnqueue(
    rawContactIds: string[] | undefined,
    filters: any | undefined,
    searchQuery: string | undefined
) {
    let contactIds = rawContactIds;

    // Step 1: Resolve IDs from filters if needed
    // IMPORTANT: Uses the server's `supabase` client (service role key) for full access,
    // NOT the `db` singleton which uses the anon key (capped at 1000 rows by PostgREST).
    if (filters && !contactIds) {
        addServerLog(`🔍 Resolving contacts from filters...`, 'Pipeline', 'phase');
        try {
            contactIds = await resolveFilteredContactIds(filters, searchQuery);
        } catch (e: any) {
            addServerLog(`❌ Failed to resolve filters: ${e.message}`, 'Pipeline', 'error');
            jobStats.isProcessing = false;
            jobStats.queueingPhase = false;
            return;
        }
    }

    if (!contactIds || contactIds.length === 0) {
        addServerLog(`⚠️ No contacts found matching filters.`, 'Pipeline', 'warn');
        jobStats.isProcessing = false;
        jobStats.queueingPhase = false;
        return;
    }

    jobStats.total = contactIds.length;
    addServerLog(`📦 Queueing ${contactIds.length} records...`, 'Pipeline', 'phase');

    try {
        // Step 2: Create a new job
        const { data: jobRaw, error: errJob } = await supabase.from('jobs').insert({
            status: 'processing',
            total_items: contactIds.length,
            started_at: new Date().toISOString()
        }).select('id').single();

        if (errJob) throw errJob;
        const jobId = jobRaw.id;

        // Step 3: Insert job_items in chunks (with progress updates)
        const ENQUEUE_CHUNK_SIZE = 5000;
        for (let i = 0; i < contactIds.length; i += ENQUEUE_CHUNK_SIZE) {
            const chunk = contactIds.slice(i, i + ENQUEUE_CHUNK_SIZE);
            const items = chunk.map(id => ({
                job_id: jobId,
                contact_id: id,
                status: 'pending',
                attempt_count: 0
            }));
            const { error: errItems } = await supabase.from('job_items').insert(items);
            if (errItems) throw errItems;

            jobStats.queued = Math.min(i + ENQUEUE_CHUNK_SIZE, contactIds.length);
            addServerLog(`📥 Enqueued ${jobStats.queued}/${contactIds.length}...`, 'Sync');
        }

        // Step 4: Queueing complete — switch to processing phase
        jobStats.queueingPhase = false;
        jobStats.queued = contactIds.length;
        await persistPipelineState();

        addServerLog(`✅ All ${contactIds.length} records queued. Starting enrichment...`, 'Pipeline', 'phase');

        // Step 5: Start the Background Processor
        JobProcessor.start();
    } catch (err: any) {
        console.error('Enqueue error:', err);
        addServerLog(`❌ Fatal enqueue error: ${err.message}`, 'Pipeline', 'error');
        jobStats.isProcessing = false;
        jobStats.queueingPhase = false;
    }
}

/**
 * Resolves contact IDs from filters.
 * Strategy 1: Try the `resolve_enrichment_targets` RPC (runs in DB with 120s timeout, no row limits)
 * Strategy 2: Fall back to paginated PostgREST queries (1000 rows per page due to max-rows)
 */
async function resolveFilteredContactIds(filters: any, searchQuery?: string): Promise<string[]> {
    // --- Strategy 1: RPC (preferred — runs in DB, no row limits, 120s timeout) ---
    try {
        const leadListFilter = (filters || []).find((f: any) => f.column === 'lead_list_name' && f.operator === 'in');
        const statusFilter = (filters || []).find((f: any) => f.column === 'status');

        const rpcParams: any = {};
        if (leadListFilter) rpcParams.p_lead_list_names = leadListFilter.value;
        if (statusFilter) rpcParams.p_statuses = statusFilter.value;
        if (searchQuery) rpcParams.p_search = searchQuery;

        addServerLog(`🔍 Trying RPC resolve_enrichment_targets...`, 'Pipeline', 'info');
        const { data: rpcData, error: rpcError } = await supabase.rpc('resolve_enrichment_targets', rpcParams);

        if (!rpcError && rpcData) {
            const ids = rpcData.map((r: any) => r.cid);
            addServerLog(`✅ RPC resolved ${ids.length} contact IDs.`, 'Pipeline', 'info');
            return ids;
        }

        if (rpcError) {
            addServerLog(`⚠️ RPC not available (${rpcError.message}), falling back to pagination...`, 'Pipeline', 'warn');
        }
    } catch (e: any) {
        addServerLog(`⚠️ RPC call failed (${e.message}), falling back to pagination...`, 'Pipeline', 'warn');
    }

    // --- Strategy 2: Paginated PostgREST fallback ---
    const allIds: string[] = [];
    const PAGE_SIZE = 1000; // Supabase PostgREST max-rows cap
    let page = 0;

    const enrichmentCols = ['status', 'classification', 'confidence', 'cost', 'processed_at'];
    const hasEnrichmentFilter = (filters || []).some((f: any) => enrichmentCols.includes(f.column));
    const statusFilter = (filters || []).find((f: any) => f.column === 'status');
    const isNewOnly = statusFilter && Array.isArray(statusFilter.value) && statusFilter.value.includes('new') && statusFilter.value.length === 1;

    while (true) {
        let selectStr = 'contact_id';
        if (hasEnrichmentFilter && !isNewOnly) {
            selectStr = 'contact_id, enrichments!inner(status)';
        } else if (isNewOnly) {
            selectStr = 'contact_id, enrichments(status)';
        }

        let query: any = supabase
            .from('contacts')
            .select(selectStr)
            .order('created_at', { ascending: true })
            .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);

        if (searchQuery) {
            const q = `"%${searchQuery}%"`;
            query = query.or(`first_name.ilike.${q},last_name.ilike.${q},email.ilike.${q},company_website.ilike.${q},company_name.ilike.${q},industry.ilike.${q},lead_list_name.ilike.${q}`);
        }

        for (const f of (filters || [])) {
            const isEnrichmentCol = enrichmentCols.includes(f.column);
            const colPath = isEnrichmentCol ? `enrichments.${f.column}` : f.column;

            if (f.column === 'status' && Array.isArray(f.value)) {
                const statuses = f.value;
                const hasNew = statuses.includes('new');
                const others = statuses.filter((s: string) => s !== 'new');
                if (hasNew && others.length === 0) {
                    query = query.filter('enrichments', 'is', 'null');
                } else if (!hasNew && others.length > 0) {
                    query = query.in('enrichments.status', others);
                }
            } else {
                switch (f.operator) {
                    case 'equals': query = query.eq(colPath, f.value); break;
                    case 'contains': query = query.ilike(colPath, `%${f.value}%`); break;
                    case 'starts_with': query = query.ilike(colPath, `${f.value}%`); break;
                    case 'greater_than': query = query.gt(colPath, f.value); break;
                    case 'less_than': query = query.lt(colPath, f.value); break;
                    case 'in': query = query.in(colPath, Array.isArray(f.value) ? f.value : [f.value]); break;
                    case 'not_in': {
                        const vals = Array.isArray(f.value) ? f.value : [f.value];
                        if (vals.length > 0) query = query.not(colPath, 'in', `(${vals.join(',')})`);
                        break;
                    }
                }
            }
        }

        const { data, error } = await query;
        if (error) {
            addServerLog(`⚠️ Filter resolve page error (page=${page}): ${error.message}`, 'Pipeline', 'warn');
            throw error;
        }
        if (!data || data.length === 0) break;

        allIds.push(...data.map((d: any) => d.contact_id));

        if (allIds.length % 10000 < PAGE_SIZE) {
            addServerLog(`🔍 Resolved ${allIds.length} contact IDs so far...`, 'Pipeline', 'info');
        }

        if (data.length < PAGE_SIZE) break;
        page++;
    }

    addServerLog(`✅ Resolved ${allIds.length} total contact IDs from filters.`, 'Pipeline', 'info');
    return allIds;
}

app.post('/api/stop', async (req, res) => {
    addServerLog(`⚠️ Stop command received. Halting background pipeline...`, 'Pipeline', 'warn');
    JobProcessor.stop();
    jobStats.isProcessing = false;
    await persistPipelineState();

    // Attempt to mark the currently active jobs as cancelled
    await supabase.from('jobs').update({ status: 'cancelled' }).eq('status', 'processing').then();

    res.json({ message: 'Pipeline stopping gracefully...' });
});

app.post('/api/resume', async (req, res) => {
    if (jobStats.isProcessing) {
        return res.status(409).json({ error: 'Pipeline is already running' });
    }

    try {
        // 1. Find jobs that were cancelled or still have pending items
        const { data: stoppedJobs, error: jobErr } = await supabase
            .from('jobs')
            .select('id, total_items')
            .in('status', ['cancelled', 'pending', 'processing'])
            .order('created_at', { ascending: false })
            .limit(1)
            .single();

        if (jobErr || !stoppedJobs) {
            // No existing jobs to resume, check if there are any pending job_items orphaned
            const { count: orphanCount } = await supabase.from('job_items')
                .select('*', { count: 'exact', head: true })
                .in('status', ['pending', 'retrying', 'processing']);

            if (!orphanCount || orphanCount === 0) {
                return res.json({ message: 'No pending jobs to resume', resumed: 0 });
            }
        }

        // 2. Reset any stuck processing items back to pending
        await supabase.from('job_items')
            .update({ status: 'pending', locked_at: null })
            .eq('status', 'processing');

        // 3. Mark the job as processing again
        if (stoppedJobs) {
            await supabase.from('jobs')
                .update({ status: 'processing' })
                .eq('id', stoppedJobs.id);
        }

        // 4. Count remaining items to process
        const { count: remainingCount } = await supabase.from('job_items')
            .select('*', { count: 'exact', head: true })
            .in('status', ['pending', 'retrying']);

        const remaining = remainingCount || 0;

        // 5. Update pipeline state and restart processor
        jobStats.isProcessing = true;
        jobStats.total = stoppedJobs?.total_items || remaining;
        await persistPipelineState();

        addServerLog(`▶️ Resuming pipeline: ${remaining} items remaining.`, 'Pipeline', 'phase');
        JobProcessor.start();

        res.json({ message: `Pipeline resumed with ${remaining} pending items`, resumed: remaining });
    } catch (err: any) {
        console.error('Resume error:', err);
        addServerLog(`❌ Resume failed: ${err.message}`, 'Pipeline', 'error');
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/stats/proxies', async (req, res) => {
    try {
        const startDate = req.query.startDate as string | undefined;
        const endDate = req.query.endDate as string | undefined;

        // Try RPC first (only when no date filters)
        if (!startDate && !endDate) {
            const { data, error } = await supabase.rpc('get_proxy_stats');
            if (!error && data) {
                return res.json(data);
            }
        }

        // Paginate through ALL scraped_data rows using offset-based pagination.
        // Supabase PostgREST enforces max-rows=1000 per request regardless of .limit(),
        // so we paginate in 1000-row pages to ensure we fetch everything.
        const stats: Record<string, number> = {};
        const PAGE_SIZE = 1000;
        let page = 0;

        while (true) {
            let query: any = supabase
                .from('scraped_data')
                .select('proxy_used')
                .not('proxy_used', 'is', null)
                .order('created_at', { ascending: true })
                .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);

            // Date range filters
            if (startDate) {
                query = query.gte('created_at', startDate);
            }
            if (endDate) {
                query = query.lte('created_at', endDate);
            }

            const { data: rawData, error: rawError } = await query;

            if (rawError) throw rawError;
            if (!rawData || rawData.length === 0) break;

            for (const row of rawData) {
                const proxy = row.proxy_used;
                if (proxy) stats[proxy] = (stats[proxy] || 0) + 1;
            }

            if (rawData.length < PAGE_SIZE) break;
            page++;
        }

        const formattedStats = Object.keys(stats)
            .map(key => ({ proxy_used: key, success_count: stats[key] }))
            .sort((a, b) => b.success_count - a.success_count);

        return res.json(formattedStats);
    } catch (e: any) {
        res.status(500).json({ error: e.message });
    }
});

// Fetch distinct values for a column (single efficient query)
app.get('/api/distinct/:column', async (req, res) => {
    const allowedColumns = ['lead_list_name', 'industry', 'company_name'];
    const column = req.params.column;

    if (!allowedColumns.includes(column)) {
        return res.status(400).json({ error: 'Column not allowed' });
    }

    try {
        // Use Supabase RPC for instant DISTINCT queries (single SQL roundtrip)
        if (column === 'lead_list_name') {
            const { data, error } = await supabase.rpc('get_distinct_lead_list_names');
            if (error) throw error;
            const unique = (data || []).map((r: any) => r.name).filter(Boolean);
            console.log(`[Distinct] ${column}: ${unique.length} values (via RPC)`);
            return res.json(unique);
        }

        // Fallback: cursor-based pagination for other columns
        const allValues = new Set<string>();
        let lastId = 0;

        while (true) {
            const { data: rows, error } = await supabase
                .from('contacts')
                .select(`id, ${column}`)
                .not(column, 'is', null)
                .neq(column, '')
                .gt('id', lastId)
                .order('id', { ascending: true })
                .limit(1000);

            if (error) throw error;
            if (!rows || rows.length === 0) break;

            (rows as any[]).forEach((d: any) => { if (d[column]) allValues.add(d[column]); });
            lastId = (rows as any[])[rows.length - 1].id;
        }

        const unique = [...allValues].sort();
        console.log(`[Distinct] ${column}: ${unique.length} values (via cursor scan)`);
        res.json(unique);
    } catch (err: any) {
        console.error(`[Distinct] Error fetching ${column}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/import', async (req, res) => {
    const { contacts } = req.body;

    if (!contacts || !Array.isArray(contacts) || contacts.length === 0) {
        return res.status(400).json({ error: 'No contacts provided' });
    }

    // Email validation: reject emails with disallowed characters
    const DISALLOWED_CHARS = /[()<>\[\]:;,\\"!#$%^&*=+{}|?~`\s]/;
    const validateEmail = (email: string): string | null => {
        if (!email || email.trim() === '') return 'Email is empty';
        const trimmed = email.trim();
        if (DISALLOWED_CHARS.test(trimmed)) {
            const bad = trimmed.match(DISALLOWED_CHARS);
            return `Contains invalid character: "${bad?.[0]}"`;
        }
        if (!trimmed.includes('@')) return 'Missing @ symbol';
        const [local, domain] = trimmed.split('@');
        if (!local || local.length === 0) return 'Missing local part before @';
        if (!domain || !domain.includes('.')) return 'Invalid domain (missing dot)';
        return null; // valid
    };

    try {
        // 1. Validate emails and separate valid from invalid
        const failedContacts: { email: string; row: number; reason: string }[] = [];
        const validContacts: any[] = [];

        contacts.forEach((c: any, idx: number) => {
            const email = c.email?.trim();
            if (email) {
                const validationError = validateEmail(email);
                if (validationError) {
                    failedContacts.push({ email, row: idx + 1, reason: validationError });
                    return; // Skip invalid
                }
            }
            validContacts.push(c);
        });

        // 2. Extract all valid emails for duplicate check
        const allEmails = validContacts
            .map((c: any) => c.email?.trim()?.toLowerCase())
            .filter(Boolean);

        // 3. Check which emails already exist in the database
        const existingEmails = new Set<string>();
        const EMAIL_CHECK_CHUNK = 500;
        for (let i = 0; i < allEmails.length; i += EMAIL_CHECK_CHUNK) {
            const emailChunk = allEmails.slice(i, i + EMAIL_CHECK_CHUNK);
            const { data: existing } = await supabase
                .from('contacts')
                .select('email')
                .in('email', emailChunk);
            if (existing) {
                existing.forEach((r: any) => existingEmails.add(r.email?.toLowerCase()));
            }
        }

        // 4. Prepare only NEW contacts (skip duplicates)
        let duplicates = 0;
        const newContacts: any[] = [];

        validContacts.forEach((c: any) => {
            const email = c.email?.trim()?.toLowerCase();
            if (email && existingEmails.has(email)) {
                duplicates++;
                return; // Skip duplicate
            }

            const row: any = {};
            row.contact_id = c.contact_id || crypto.randomUUID();
            if (c.email) row.email = c.email.trim();
            if (c.first_name) row.first_name = c.first_name;
            if (c.last_name) row.last_name = c.last_name;
            if (c.company_website) row.company_website = c.company_website;
            if (c.company_name) row.company_name = c.company_name;
            if (c.industry) row.industry = c.industry;
            if (c.linkedin_url) row.linkedin_url = c.linkedin_url;
            if (c.title) row.title = c.title;
            if (c.lead_list_name) row.lead_list_name = c.lead_list_name;

            newContacts.push(row);
        });

        // 5. Insert only new contacts in chunks
        const CHUNK_SIZE = 1000;
        let inserted = 0;
        let dbFailed = 0;
        const errors: string[] = [];

        for (let i = 0; i < newContacts.length; i += CHUNK_SIZE) {
            const chunk = newContacts.slice(i, i + CHUNK_SIZE);
            const { error } = await supabase
                .from('contacts')
                .insert(chunk);

            if (error) {
                console.error(`Import chunk error at row ${i}:`, error);
                errors.push(`Chunk ${i}: ${error.message}`);
                dbFailed += chunk.length;
            } else {
                inserted += chunk.length;
            }
        }

        const totalFailed = failedContacts.length + dbFailed;
        addServerLog(`📥 Import complete: ${inserted} new, ${duplicates} duplicates, ${totalFailed} failed (${failedContacts.length} invalid emails).`, 'Sync', 'info');
        res.json({ inserted, duplicates, failed: totalFailed, errors, failedContacts });
    } catch (err: any) {
        console.error('Import error:', err);
        res.status(500).json({ error: err.message });
    }
});

// All other GET requests serve React App
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

app.listen(PORT, async () => {
    console.log(`🟢 Job Processor Server running on port ${PORT}`);
    await restorePipelineState();

    // Resume any stale jobs from previous unexpected crashes
    await JobProcessor.recoverStaleJobs();

    // TRUTH CHECK: Verify isProcessing against the actual DB state.
    // If pipeline_state says "processing" but there are no active jobs/items,
    // force-reset to prevent the 409 guard from permanently blocking new enrichments.
    if (jobStats.isProcessing) {
        const { count } = await supabase
            .from('job_items')
            .select('*', { count: 'exact', head: true })
            .in('status', ['pending', 'processing', 'retrying']);

        if (!count || count === 0) {
            console.log('⚠️ Stale isProcessing detected — no active job items. Resetting.');
            jobStats.isProcessing = false;
            await persistPipelineState();
        }
    }
});

// Graceful Shutdown
process.on('SIGTERM', () => {
    console.log('SIGTERM received. Shutting down gracefully...');
    JobProcessor.stop();
    setTimeout(() => process.exit(0), 5000);
});
