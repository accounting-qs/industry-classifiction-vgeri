
import './loadEnv';

import express from 'express';
import cors from 'cors';
import compression from 'compression';
import path from 'path';
import fs from 'fs';
import fsp from 'fs/promises';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import { db } from './services/supabaseClient';
import { JobProcessor } from './services/jobProcessor';
import { runTaxonomyProposal, applyTaxonomyEdits, runAssignment, BucketingCancelledError } from './services/bucketingService';
import {
    listLibrary,
    upsertLibraryBucket,
    archiveLibraryBucket,
    deleteLibraryBucket,
    saveRunBucketsToLibrary,
    bulkImportLibraryFromText
} from './services/bucketLibraryService';
import { getSetting, setSetting, maskSecret } from './services/appSettings';

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

    // 1. Internal cache (latest 100) — Fix #7: ring buffer instead of spread+slice
    currentJobLogs.unshift(entry);
    if (currentJobLogs.length > 100) currentJobLogs.length = 100;

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
app.use(compression());
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
        // Self-heal: in-memory flags can get pinned (e.g. crash mid-queueing,
        // or queueingPhase=true blocks the /api/status auto-reset at L194).
        // If the DB has no active items, clear the flags and let the request
        // through instead of permanently locking out enrichment.
        const { count } = await supabase
            .from('job_items')
            .select('*', { count: 'exact', head: true })
            .in('status', ['pending', 'processing', 'retrying']);

        if (count && count > 0) {
            return res.status(409).json({ error: 'A job is already in progress or queueing' });
        }

        addServerLog(
            `⚠️ Stale pipeline state cleared (isProcessing=${jobStats.isProcessing}, queueingPhase=${jobStats.queueingPhase}); no active job_items.`,
            'Pipeline',
            'warn'
        );
        jobStats.isProcessing = false;
        jobStats.queueingPhase = false;
        jobStats.queued = 0;
        await persistPipelineState();
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

    const totalToEnrich = contactIds.length;
    jobStats.total = totalToEnrich;
    addServerLog(`📊 Total contacts to enrich: ${totalToEnrich.toLocaleString()}`, 'Pipeline', 'phase');
    addServerLog(`📦 Starting queue insertion for ${totalToEnrich.toLocaleString()} records...`, 'Pipeline', 'phase');

    try {
        // Step 2: Create a new job
        const { data: jobRaw, error: errJob } = await supabase.from('jobs').insert({
            status: 'processing',
            total_items: totalToEnrich,
            started_at: new Date().toISOString()
        }).select('id').single();

        if (errJob) throw errJob;
        const jobId = jobRaw.id;
        addServerLog(`🆔 Job created: ${jobId} (${totalToEnrich.toLocaleString()} items)`, 'Pipeline', 'info');

        // Step 3: Insert job_items in chunks (with progress updates).
        // Chunks are small (2000) so a single INSERT stays well under Supabase's
        // 8s statement_timeout even when another job's worker is already running
        // against job_items. 57014 (statement_timeout) and 40001 (serialization)
        // are retried with exponential backoff since they're transient under load.
        const ENQUEUE_CHUNK_SIZE = 2000;
        const MAX_RETRIES = 4;
        let enqueued = 0;
        for (let i = 0; i < contactIds.length; i += ENQUEUE_CHUNK_SIZE) {
            const chunk = contactIds.slice(i, i + ENQUEUE_CHUNK_SIZE);
            const items = chunk.map(id => ({
                job_id: jobId,
                contact_id: id,
                status: 'pending',
                attempt_count: 0
            }));

            let attempt = 0;
            while (true) {
                const { error: errItems } = await supabase.from('job_items').insert(items);
                if (!errItems) break;
                const transient = errItems.code === '57014' || errItems.code === '40001' || errItems.code === '40P01';
                if (!transient || attempt >= MAX_RETRIES) throw errItems;
                attempt++;
                const delay = 500 * Math.pow(2, attempt - 1);
                addServerLog(`⚠️ Enqueue chunk transient error (${errItems.code}); retry ${attempt}/${MAX_RETRIES} in ${delay}ms`, 'Pipeline', 'warn');
                await new Promise(r => setTimeout(r, delay));
            }

            enqueued += chunk.length;
            jobStats.queued = enqueued;
            addServerLog(`📥 Queued ${enqueued.toLocaleString()} / ${totalToEnrich.toLocaleString()} (${Math.round(enqueued / totalToEnrich * 100)}%)`, 'Sync');
        }

        // Step 4: Queueing complete — switch to processing phase
        jobStats.queueingPhase = false;
        jobStats.queued = totalToEnrich;
        await persistPipelineState();

        addServerLog(`✅ All ${totalToEnrich.toLocaleString()} records queued. Starting enrichment...`, 'Pipeline', 'phase');
        addServerLog(`📊 Pipeline summary: ${totalToEnrich.toLocaleString()} submitted → ${totalToEnrich.toLocaleString()} queued → 0 completed, 0 failed`, 'Pipeline', 'info');

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

        if (!rpcError && rpcData && Array.isArray(rpcData) && rpcData.length > 0) {
            addServerLog(`✅ RPC resolved ${rpcData.length} contact IDs.`, 'Pipeline', 'info');
            return rpcData as string[];
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
                    case 'in': {
                        // Same PostgREST quirk as services/supabaseClient.ts: `in.("quoted")`
                        // loses the index. Collapse 1-element `in` → `eq`.
                        const vals = Array.isArray(f.value) ? f.value : [f.value];
                        if (vals.length === 1) query = query.eq(colPath, vals[0]);
                        else query = query.in(colPath, vals);
                        break;
                    }
                    case 'not_in': {
                        const vals = Array.isArray(f.value) ? f.value : [f.value];
                        if (vals.length === 1) query = query.neq(colPath, vals[0]);
                        else if (vals.length > 0) query = query.not(colPath, 'in', `(${vals.join(',')})`);
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

// Hard reset: delete every pending/processing/retrying job_item, cancel active
// jobs, and zero the in-memory stats. Use this when you want to kick off a fresh
// enrichment without colliding with leftover state from a previously stopped job.
// Completed/failed job_items and their enrichments are left untouched.
app.post('/api/reset', async (_req, res) => {
    addServerLog(`🧹 Reset command received. Clearing queued/processing items...`, 'Pipeline', 'warn');
    JobProcessor.stop();

    try {
        const { count: beforeCount } = await supabase
            .from('job_items')
            .select('*', { count: 'exact', head: true })
            .in('status', ['pending', 'processing', 'retrying']);

        const { error: delErr } = await supabase
            .from('job_items')
            .delete()
            .in('status', ['pending', 'processing', 'retrying']);
        if (delErr) throw delErr;

        const { error: cancelErr } = await supabase
            .from('jobs')
            .update({ status: 'cancelled' })
            .in('status', ['pending', 'processing']);
        if (cancelErr) throw cancelErr;

        jobStats = { total: 0, completed: 0, failed: 0, isProcessing: false, queueingPhase: false, queued: 0 };
        currentJobLogs = [];
        await persistPipelineState();

        const cleared = beforeCount || 0;
        addServerLog(`✅ Pipeline reset: ${cleared.toLocaleString()} queued items cleared.`, 'Pipeline', 'phase');
        res.json({ cleared });
    } catch (err: any) {
        console.error('Reset error:', err);
        addServerLog(`❌ Reset failed: ${err.message}`, 'Pipeline', 'error');
        res.status(500).json({ error: err.message });
    }
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

// Human-friendly labels for the reuse bucket. Keep in sync with the
// "Enrichment `source` values" section of README.md.
const REUSE_LABELS: Record<string, string> = {
    'domain_intelligence': 'Domain Intelligence',
    'digest_cache': 'Digest Cache'
};
// Human-friendly labels for error:* sources. Any unknown error:* key falls
// back to a Title-Cased version of whatever follows the colon.
const ERROR_LABELS: Record<string, string> = {
    'error:no_domain': 'No Domain',
    'error:personal_email': 'Personal Email (Skipped)',
    'error:scrape': 'Scrape Failure',
    'error:ai': 'AI Failure'
};

app.get('/api/stats/proxies', async (req, res) => {
    try {
        const startDate = req.query.startDate as string | undefined;
        const endDate = req.query.endDate as string | undefined;

        const { data, error } = await supabase.rpc('get_enrichment_source_stats', {
            start_ts: startDate || null,
            end_ts: endDate || null
        });

        if (error) throw error;

        const byProxy: { source: string; count: number }[] = [];
        const byReuse: { source: string; label: string; count: number }[] = [];
        const byError: { source: string; label: string; count: number }[] = [];
        let unknown = 0;
        let total = 0;

        for (const row of (data || [])) {
            const src: string = row.source;
            const n = Number(row.count);
            total += n;

            if (src === 'unknown' || !src) {
                unknown += n;
            } else if (REUSE_LABELS[src]) {
                byReuse.push({ source: src, label: REUSE_LABELS[src], count: n });
            } else if (src.startsWith('error:')) {
                const label = ERROR_LABELS[src] || src.replace(/^error:/, '').replace(/_/g, ' ');
                byError.push({ source: src, label, count: n });
            } else {
                // Proxy name (known or new/unmapped).
                byProxy.push({ source: src, count: n });
            }
        }

        byProxy.sort((a, b) => b.count - a.count);
        byError.sort((a, b) => b.count - a.count);
        byReuse.sort((a, b) => b.count - a.count);

        return res.json({ byProxy, byReuse, byError, unknown, total });
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

// Treat transient transport / Postgres errors as retryable. The Postgres
// codes are statement_timeout (57014) and the two serialization conflicts
// (40001, 40P01). The string matches catch undici's "fetch failed", DNS
// blips (EAI_AGAIN), socket resets (ECONNRESET), and pooler hangs
// (ETIMEDOUT, "socket hang up"). Render → Supabase has all of these
// fire intermittently under load, and the original code only retried the
// Postgres ones — every other transient turned into a 500 that dropped
// the whole 2000-row chunk on the floor.
const isTransientImportError = (err: any): boolean => {
    if (!err) return false;
    if (err.code === '57014' || err.code === '40001' || err.code === '40P01') return true;
    const msg = String(err?.message || err || '').toLowerCase();
    return /fetch failed|econnreset|etimedout|eai_again|enotfound|socket hang up|network|aborted|connection reset/.test(msg);
};

// Wraps a Supabase builder call so transport throws and tuple-shaped
// errors are funneled through the same retry path. supabase-js usually
// returns transport errors via the {data, error} tuple, but TypeError
// fetch failures from undici can throw out of `await` directly — we have
// to handle both shapes or one of them slips past. The factory returns
// PromiseLike (not Promise) because Supabase's PostgrestFilterBuilder is
// thenable but isn't a real Promise; await coerces it either way.
async function importSupabaseRetry(
    label: string,
    fn: () => PromiseLike<{ data: any; error: any }>,
    maxRetries = 5
): Promise<{ data: any; error: any }> {
    let attempt = 0;
    while (true) {
        let result: { data: any; error: any };
        try {
            result = await fn();
        } catch (thrown: any) {
            result = { data: null, error: thrown };
        }
        if (!result.error || !isTransientImportError(result.error) || attempt >= maxRetries) {
            return result;
        }
        attempt++;
        // Capped exponential backoff. 8s ceiling so a stuck pooler doesn't
        // park the whole import waiting tens of seconds per chunk.
        const backoff = Math.min(8000, 500 * Math.pow(2, attempt));
        console.warn(`[${label}] transient error (${result.error?.message || 'unknown'}); retry ${attempt}/${maxRetries} in ${backoff}ms`);
        await new Promise(r => setTimeout(r, backoff));
    }
}

app.post('/api/import', async (req, res) => {
    const { contacts, overwriteDuplicates } = req.body;

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

        // 3. Check which emails already exist in the database. Retry
        // on Postgres statement_timeout (57014) with a smaller batch —
        // shared-pooler contention can push a 500-email IN query past
        // the 8s limit even though the column is indexed. Transport
        // errors (undici "fetch failed", ECONNRESET, etc.) are retried
        // in importSupabaseRetry without shrinking the batch.
        //
        // CRITICAL: this stage soft-fails. If a batch can't be checked
        // even after retries, we skip duplicate detection for that batch
        // and continue. The downstream upsert uses `onConflict: 'email'`
        // which is idempotent — duplicates become no-ops, never bad
        // inserts. The only side-effect of giving up here is a slightly
        // undercounted "duplicates" tile in the UI. That is *vastly*
        // preferable to throwing (the previous behavior), which surfaced
        // a 500 to the client and dropped all 2000 contacts in the chunk.
        const existingEmails = new Set<string>();
        let emailCheckChunk = 500;
        let preCheckSkipped = 0;
        for (let i = 0; i < allEmails.length;) {
            const emailBatch = allEmails.slice(i, i + emailCheckChunk);
            const { data: existing, error } = await importSupabaseRetry(
                'Import.preCheck',
                () => supabase.from('contacts').select('email').in('email', emailBatch)
            );
            if (error) {
                if (error.code === '57014' && emailCheckChunk > 50) {
                    emailCheckChunk = Math.max(50, Math.floor(emailCheckChunk / 2));
                    console.warn(`[Import] Email pre-check timeout; retrying at chunk=${emailCheckChunk}`);
                    continue;
                }
                console.warn(`[Import] Email pre-check gave up for batch of ${emailBatch.length} after retries (${error.message || 'unknown'}); skipping dedup, upsert will still be safe.`);
                preCheckSkipped += emailBatch.length;
                i += emailBatch.length;
                continue;
            }
            if (existing) existing.forEach((r: any) => existingEmails.add(r.email?.toLowerCase()));
            i += emailBatch.length;
        }
        if (preCheckSkipped > 0) {
            addServerLog(`⚠️ Import: pre-check soft-failed for ${preCheckSkipped} emails — duplicates may be undercounted but no data is lost.`, 'Sync', 'warn');
        }

        // 4. Separate new vs existing contacts
        let duplicates = 0;
        const newContacts: any[] = [];
        const updateContacts: any[] = [];

        const buildRow = (c: any) => {
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
            return row;
        };

        validContacts.forEach((c: any) => {
            const email = c.email?.trim()?.toLowerCase();
            if (email && existingEmails.has(email)) {
                if (overwriteDuplicates) {
                    updateContacts.push(buildRow(c));
                } else {
                    duplicates++;
                }
                return;
            }
            newContacts.push(buildRow(c));
        });

        // 5. Insert new contacts + upsert existing (if overwrite enabled).
        //
        // Write helper with adaptive chunking: starts at 1000, halves on
        // Postgres statement_timeout (57014) or serialization conflicts
        // (40001/40P01), retries a few times, drifts back up after a
        // clean write. We use `upsert` with `ignoreDuplicates` instead of
        // a bare `insert` so the operation is idempotent — a concurrent
        // import that beat us to an email just makes our row a no-op
        // instead of blowing up the whole chunk with a unique-violation.
        const INITIAL_CHUNK = 1000;
        const MIN_CHUNK = 100;
        const MAX_RETRIES = 4;
        let inserted = 0;
        let updated = 0;
        let dbFailed = 0;
        const errors: string[] = [];

        const writeAll = async (
            rows: any[],
            kind: 'insert' | 'upsert'
        ): Promise<number> => {
            let written = 0;
            let chunkSize = INITIAL_CHUNK;
            let i = 0;
            while (i < rows.length) {
                const chunk = rows.slice(i, i + chunkSize);
                let attempts = 0;
                let success = false;
                while (true) {
                    // importSupabaseRetry handles the transport-level retries
                    // (fetch failed, ECONNRESET, etc). The outer loop here
                    // owns the chunk-shrinking strategy for Postgres
                    // statement_timeout, which transport retry alone can't
                    // cure — a 1000-row upsert that times out at 8s will
                    // time out again unless we cut the size.
                    const { error } = await importSupabaseRetry(
                        `Import.${kind}`,
                        () => kind === 'insert'
                            ? supabase.from('contacts').upsert(chunk, { onConflict: 'email', ignoreDuplicates: true })
                            : supabase.from('contacts').upsert(chunk, { onConflict: 'email' })
                    );
                    if (!error) { success = true; break; }

                    // Only Postgres statement_timeout / serialization
                    // benefits from chunk-halving. Transport errors that
                    // survived importSupabaseRetry are genuinely terminal.
                    const sizeRetryable = error.code === '57014' || error.code === '40001' || error.code === '40P01';
                    attempts++;
                    if (!sizeRetryable || attempts >= MAX_RETRIES || chunkSize <= MIN_CHUNK) {
                        console.error(`${kind} chunk error at row ${i}:`, error);
                        errors.push(`Chunk ${i}: ${error.message}`);
                        dbFailed += chunk.length;
                        break;
                    }
                    const nextChunk = Math.max(MIN_CHUNK, Math.floor(chunkSize / 2));
                    console.warn(`[Import] ${kind} chunk timeout at size=${chunkSize}; retry ${attempts}/${MAX_RETRIES} at size=${nextChunk}`);
                    chunkSize = nextChunk;
                    chunk.length = chunkSize; // shrink in place; remainder re-enters on next outer iteration
                    await new Promise(r => setTimeout(r, 500 * attempts));
                }
                if (success) written += chunk.length;
                i += chunk.length;
                if (chunkSize < INITIAL_CHUNK) chunkSize = Math.min(INITIAL_CHUNK, chunkSize * 2);
            }
            return written;
        };

        inserted = await writeAll(newContacts, 'insert');
        updated = await writeAll(updateContacts, 'upsert');

        const totalFailed = failedContacts.length + dbFailed;
        addServerLog(`📥 Import complete: ${inserted} new, ${updated} updated, ${duplicates} duplicates, ${totalFailed} failed (${failedContacts.length} invalid emails).`, 'Sync', 'info');
        res.json({ inserted, updated, duplicates, failed: totalFailed, errors, failedContacts });
    } catch (err: any) {
        console.error('Import error:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================
// IMPORT LISTS (history)
// ============================================

app.get('/api/import-lists', async (_req, res) => {
    try {
        const { data, error } = await supabase
            .from('import_lists')
            .select('*')
            .order('created_at', { ascending: false });

        if (error) return res.status(500).json({ error: error.message });

        const lists = data || [];

        // Preferred path: `get_list_enrichment_stats` RPC returns
        // completed/failed/total for every list in one aggregate pass.
        // PostgREST's per-list count=estimated returned 0 unpredictably
        // (the planner short-circuits filtered joins), which broke the
        // "done" indicator and made the same list's count change 30%+
        // between page refreshes.
        const { data: stats, error: rpcErr } = await supabase.rpc('get_list_enrichment_stats');
        const statsByList = new Map<string, { completed: number; failed: number; total: number }>();
        let statsSource: 'rpc' | 'fallback' = 'rpc';
        if (!rpcErr && Array.isArray(stats)) {
            for (const row of stats as any[]) {
                statsByList.set(row.lead_list_name, {
                    completed: Number(row.completed_count) || 0,
                    failed: Number(row.failed_count) || 0,
                    total: Number(row.total_count) || 0,
                });
            }
        } else if (rpcErr) {
            statsSource = 'fallback';
            console.warn('[import-lists] RPC get_list_enrichment_stats unavailable — falling back to per-list estimated counts:', rpcErr.message);
        }

        // Fallback for environments where the RPC hasn't been applied yet.
        // These counts are unreliable (count=estimated can drift or return
        // 0 randomly) — the client surfaces a banner to say so.
        const needFallback = lists.some((l: any) => !statsByList.has(l.name));
        if (needFallback) {
            statsSource = 'fallback';
            await Promise.all(lists.map(async (l: any) => {
                if (statsByList.has(l.name)) return;
                const [completed, failed] = await Promise.all([
                    supabase.from('contacts')
                        .select('contact_id, enrichments!inner(status)', { count: 'estimated', head: true })
                        .eq('lead_list_name', l.name).eq('enrichments.status', 'completed'),
                    supabase.from('contacts')
                        .select('contact_id, enrichments!inner(status)', { count: 'estimated', head: true })
                        .eq('lead_list_name', l.name).eq('enrichments.status', 'failed'),
                ]);
                statsByList.set(l.name, {
                    completed: completed.count || 0,
                    failed: failed.count || 0,
                    total: l.contact_count || 0,
                });
            }));
        }

        const withCounts = lists.map((l: any) => {
            const s = statsByList.get(l.name) || { completed: 0, failed: 0, total: l.contact_count || 0 };
            return {
                ...l,
                enriched_count: s.completed,
                failed_count: s.failed,
                contact_count: s.total || l.contact_count || 0,
            };
        });
        // Object response so we can send metadata (stats_source) alongside
        // the list — wrapped in a shape the client detects while still
        // tolerating the older array response during rolling deploys.
        res.json({ lists: withCounts, stats_source: statsSource });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/import-lists', async (req, res) => {
    const { name, contact_count } = req.body;
    if (!name) return res.status(400).json({ error: 'Name is required' });

    try {
        const { data, error } = await supabase
            .from('import_lists')
            .insert({ name, contact_count: contact_count || 0 })
            .select()
            .single();

        if (error) return res.status(500).json({ error: error.message });
        res.json(data);
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Rename an import list and cascade the new name to every contact that
// references it. contacts.lead_list_name is a plain TEXT column, not a FK,
// so the rename has to touch both tables. Production lists run 50k–250k+
// rows, which trips PostgREST's 8s statement_timeout when we try to do
// the cascade through two separate REST UPDATEs — so the real work lives
// in `rename_import_list` (rename_import_list_rpc.sql), which raises the
// timeout and runs both updates inside a single transaction.
app.patch('/api/import-lists/:id', async (req, res) => {
    const id = String(req.params?.id || '').trim();
    const newName = String(req.body?.name || '').trim();
    if (!id) return res.status(400).json({ error: 'id is required' });
    if (!newName) return res.status(400).json({ error: 'name is required' });

    try {
        const { data: rpcData, error: rpcErr } = await supabase.rpc('rename_import_list', {
            p_id: id,
            p_new_name: newName,
        });

        if (!rpcErr) {
            const row = Array.isArray(rpcData) ? rpcData[0] : rpcData;
            return res.json({
                id,
                name: row?.new_name ?? newName,
                oldName: row?.old_name ?? null,
                contacts_updated: Number(row?.contacts_updated) || 0,
            });
        }

        // Surface the function's own validation errors with the right HTTP
        // status before falling back. PostgREST passes RAISE EXCEPTION as
        // `message` and the SQLSTATE as `code`.
        const msg = rpcErr.message || '';
        if (msg.startsWith('duplicate_list_name')) {
            return res.status(409).json({ error: `A list named "${newName}" already exists` });
        }
        if (msg.includes('not found')) {
            return res.status(404).json({ error: msg });
        }

        // Fallback for environments where the migration hasn't been applied
        // yet. This path is unsafe for big lists (statement_timeout) — the
        // RPC is the supported route. Logged so it's visible in deploys.
        const isMissingFn = (rpcErr as any).code === 'PGRST202' || /function .*rename_import_list/i.test(msg);
        if (!isMissingFn) {
            return res.status(500).json({ error: msg || 'Rename failed' });
        }
        console.warn('[import-lists] rename_import_list RPC unavailable — falling back to two-step update. Apply supabase/migrations/20260427_rename_import_list_rpc.sql to lift the timeout.');

        const { data: existing, error: fetchErr } = await supabase
            .from('import_lists')
            .select('id, name, contact_count, created_at')
            .eq('id', id)
            .single();
        if (fetchErr || !existing) {
            return res.status(404).json({ error: fetchErr?.message || 'List not found' });
        }
        const oldName = existing.name;
        if (oldName === newName) return res.json({ ...existing, oldName });

        const { data: clash } = await supabase
            .from('import_lists')
            .select('id')
            .eq('name', newName)
            .neq('id', id)
            .maybeSingle();
        if (clash) return res.status(409).json({ error: `A list named "${newName}" already exists` });

        const { error: updListErr } = await supabase
            .from('import_lists')
            .update({ name: newName })
            .eq('id', id);
        if (updListErr) return res.status(500).json({ error: updListErr.message });

        const { error: updContactsErr } = await supabase
            .from('contacts')
            .update({ lead_list_name: newName })
            .eq('lead_list_name', oldName);
        if (updContactsErr) {
            await supabase
                .from('import_lists')
                .update({ name: oldName })
                .eq('id', id);
            return res.status(500).json({ error: updContactsErr.message });
        }

        res.json({ id, name: newName, oldName, contact_count: existing.contact_count, created_at: existing.created_at });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// List-deletion endpoints. The actual machinery is defined further down
// (alongside the export-jobs pattern, since both share the same on-disk
// registry directory). These endpoints just wire the kick-off and
// progress polling into the existing /api/import-lists/* surface.
//
// Why a background job: a 200k-row list takes 30–60s to drain in chunks,
// which is far longer than a typical browser tab will sit idle. If the
// user navigates or closes the tab mid-request, a synchronous endpoint
// just dies — leaving a partially-deleted list in the DB. The job-based
// version keeps running independently of the HTTP connection and is
// resumable on server restart (the loop is idempotent — each iteration
// re-queries by lead_list_name, so picking up after a crash just means
// the next page's "fetch contacts" returns whatever is left).
app.delete('/api/import-lists/:id', async (req, res) => {
    const id = String(req.params?.id || '').trim();
    if (!id) return res.status(400).json({ error: 'id is required' });

    // Reuse an existing in-flight job for the same list — clicking Delete
    // twice should attach to the running job, not start a parallel loop
    // that fights it for the same rows.
    const inflight = Array.from(deleteJobs.values()).find(j => j.listId === id && j.status === 'running');
    if (inflight) return res.status(202).json(inflight);

    const { data: list, error: listErr } = await supabase
        .from('import_lists')
        .select('id, name, contact_count')
        .eq('id', id)
        .single();
    if (listErr || !list) {
        return res.status(404).json({ error: listErr?.message || 'List not found' });
    }

    const job: DeleteJob = {
        id: crypto.randomUUID(),
        listId: list.id,
        listName: list.name,
        status: 'running',
        contactsTotal: list.contact_count || 0,
        contactsDeleted: 0,
        enrichmentsDeleted: 0,
        createdAt: new Date().toISOString(),
    };
    deleteJobs.set(job.id, job);
    scheduleDeletePersist();

    // Fire-and-forget — the response below hands the job id back so the
    // client can poll /api/delete-jobs for progress. Crashes inside the
    // worker are caught and reflected in job.status='failed'.
    runDeleteJob(job).catch(err => {
        console.error(`[Delete] Job ${job.id} crashed:`, err);
    });

    res.status(202).json(job);
});

// ============================================
// EXPORT JOBS — async CSV builder
//
// Problem: large lists (50k+) can take minutes to materialize. Streaming
// through the HTTP response racing a reverse-proxy idle timeout truncates
// the file. Instead, we build CSVs on disk in the background and hand the
// client a "is it ready?" / "download it" handoff so leaving the page is
// safe.
//
// Storage: `./exports/{jobId}.csv` + `./exports/jobs.json` registry.
// Retention: files older than 7 days are pruned hourly.
// Restart semantics: any job stuck in `building` at startup is marked
// `failed` (its partial file is unlinked) — the client can re-trigger.
// ============================================

const EXPORT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const EXPORTS_DIR = path.join(__dirname, 'exports');
const JOBS_REGISTRY_PATH = path.join(EXPORTS_DIR, 'jobs.json');

type ExportJobStatus = 'building' | 'ready' | 'failed';
interface ExportJob {
    id: string;
    listName: string;
    status: ExportJobStatus;
    rowCount: number;
    totalRows: number;
    createdAt: string;
    completedAt?: string;
    error?: string;
}

const exportJobs = new Map<string, ExportJob>();
let persistTimer: NodeJS.Timeout | null = null;

const schedulePersist = () => {
    if (persistTimer) return;
    persistTimer = setTimeout(async () => {
        persistTimer = null;
        try {
            const payload = JSON.stringify(Array.from(exportJobs.values()), null, 2);
            await fsp.writeFile(JOBS_REGISTRY_PATH, payload, 'utf-8');
        } catch (e) {
            console.error('[Export] Failed to persist jobs registry:', e);
        }
    }, 200);
};

async function loadExportJobs() {
    await fsp.mkdir(EXPORTS_DIR, { recursive: true });
    try {
        const raw = await fsp.readFile(JOBS_REGISTRY_PATH, 'utf-8');
        const arr: ExportJob[] = JSON.parse(raw);
        for (const j of arr) exportJobs.set(j.id, j);
    } catch {
        // Registry doesn't exist yet — first boot.
    }

    // Any job still "building" was interrupted by a restart — unlink its
    // partial file and mark failed so the client can retrigger.
    for (const j of exportJobs.values()) {
        if (j.status === 'building') {
            j.status = 'failed';
            j.error = 'Server restarted while building';
            await fsp.unlink(path.join(EXPORTS_DIR, `${j.id}.csv`)).catch(() => {});
        }
    }
    schedulePersist();
}

async function pruneExpiredExports() {
    const now = Date.now();
    let pruned = 0;
    for (const [id, job] of exportJobs) {
        const ref = new Date(job.completedAt || job.createdAt).getTime();
        if (now - ref > EXPORT_RETENTION_MS) {
            await fsp.unlink(path.join(EXPORTS_DIR, `${id}.csv`)).catch(() => {});
            exportJobs.delete(id);
            pruned++;
        }
    }
    if (pruned > 0) {
        console.log(`[Export] Pruned ${pruned} expired export(s).`);
        schedulePersist();
    }
}

const csvEscape = (v: unknown): string => {
    if (v === null || v === undefined) return '';
    const s = String(v).replace(/"/g, '""');
    return /[",\n\r]/.test(s) ? `"${s}"` : s;
};

async function buildExportCsv(job: ExportJob) {
    const contactCols = ['contact_id', 'email', 'first_name', 'last_name', 'company_website', 'lead_list_name'];
    const enrichmentCols = ['classification', 'confidence', 'cost', 'status', 'processed_at'];
    const headerCols = [...contactCols, ...enrichmentCols];
    const selectStr = `${contactCols.join(',')},enrichments(${enrichmentCols.join(',')})`;
    const filePath = path.join(EXPORTS_DIR, `${job.id}.csv`);

    const stream = fs.createWriteStream(filePath, { encoding: 'utf-8' });
    // Single shared error latch — writes reject against it if the stream
    // dies mid-flight. Attaching a fresh listener per write leaks handlers.
    let streamError: Error | null = null;
    stream.on('error', err => { streamError = err; });

    const write = (chunk: string): Promise<void> => new Promise((resolve, reject) => {
        if (streamError) return reject(streamError);
        if (stream.write(chunk)) resolve();
        else stream.once('drain', () => streamError ? reject(streamError) : resolve());
    });

    try {
        await write(headerCols.join(',') + '\n');

        // Adaptive paging: start large for throughput, halve on Postgres
        // statement_timeout (57014) — transient pooler contention can push a
        // 1000-row page past 8s. Halving usually succeeds on retry; after a
        // clean page we drift back up toward 1000 so steady-state stays fast.
        const MAX_PAGE = 1000;
        const MIN_PAGE = 100;
        let pageSize = MAX_PAGE;
        let lastId: string | null = null;

        while (true) {
            let data: any[] | null = null;
            let attempts = 0;
            while (true) {
                let query: any = supabase
                    .from('contacts')
                    .select(selectStr)
                    .eq('lead_list_name', job.listName)
                    .order('contact_id', { ascending: true })
                    .limit(pageSize);
                if (lastId) query = query.gt('contact_id', lastId);

                const { data: pageData, error } = await query;
                if (!error) { data = pageData; break; }

                const isTimeout = error.code === '57014';
                attempts++;
                if (!isTimeout || attempts >= 5 || pageSize <= MIN_PAGE) throw error;

                const nextPage = Math.max(MIN_PAGE, Math.floor(pageSize / 2));
                console.warn(`[Export] Job ${job.id} page timeout at size=${pageSize}; retrying at size=${nextPage}`);
                pageSize = nextPage;
                await new Promise(r => setTimeout(r, 500 * attempts));
            }
            if (!data || data.length === 0) break;

            for (const row of data) {
                const enr = Array.isArray(row.enrichments) ? row.enrichments[0] : row.enrichments;
                const line = headerCols.map(c =>
                    contactCols.includes(c) ? csvEscape(row[c]) : csvEscape(enr ? enr[c] : null)
                ).join(',');
                await write(line + '\n');
            }

            job.rowCount += data.length;
            schedulePersist();

            lastId = (data[data.length - 1] as any).contact_id;
            if (data.length < pageSize) break;

            // Recover toward MAX_PAGE after a clean fetch so one blip doesn't
            // trap the rest of the run at a tiny page size.
            if (pageSize < MAX_PAGE) pageSize = Math.min(MAX_PAGE, pageSize * 2);
        }

        await new Promise<void>((resolve, reject) => stream.end((err: any) => err ? reject(err) : resolve()));

        job.status = 'ready';
        job.completedAt = new Date().toISOString();
        schedulePersist();
        console.log(`[Export] Job ${job.id} ready: ${job.rowCount} rows for "${job.listName}"`);
    } catch (err: any) {
        console.error(`[Export] Job ${job.id} failed:`, err.message);
        job.status = 'failed';
        job.error = err.message;
        job.completedAt = new Date().toISOString();
        stream.destroy();
        await fsp.unlink(filePath).catch(() => {});
        schedulePersist();
    }
}

// Starts a new export job (or returns the existing ready/building one).
// Client polls /api/export-jobs to discover when it's ready.
app.post('/api/export-list/start', async (req, res) => {
    const name = String(req.body?.name || '').trim();
    if (!name) return res.status(400).json({ error: 'name is required' });

    // Reuse existing non-expired job for the same list, regardless of status:
    // ready → client downloads, building → client polls, failed → client
    // sees the error and retries explicitly.
    const existing = Array.from(exportJobs.values())
        .filter(j => j.listName === name)
        .sort((a, b) => (b.completedAt || b.createdAt).localeCompare(a.completedAt || a.createdAt))[0];
    if (existing && existing.status !== 'failed') {
        return res.json(existing);
    }

    // Get total row count for progress reporting.
    const { count } = await supabase
        .from('contacts')
        .select('contact_id', { count: 'estimated', head: true })
        .eq('lead_list_name', name);

    const job: ExportJob = {
        id: crypto.randomUUID(),
        listName: name,
        status: 'building',
        rowCount: 0,
        totalRows: count || 0,
        createdAt: new Date().toISOString()
    };
    exportJobs.set(job.id, job);
    schedulePersist();

    // Fire-and-forget the build. Errors land on the job record, not the
    // response — the client already has the job id from this 202.
    buildExportCsv(job).catch(err => {
        console.error('[Export] Unexpected build failure:', err);
    });

    res.status(202).json(job);
});

// Returns all export jobs so the modal can render button state per list.
// Clients filter by listName locally.
app.get('/api/export-jobs', (_req, res) => {
    res.json(Array.from(exportJobs.values()));
});

app.get('/api/export-list/download', (req, res) => {
    const id = String(req.query.id || '');
    const job = exportJobs.get(id);
    if (!job || job.status !== 'ready') {
        return res.status(404).json({ error: 'Export not ready' });
    }
    const filePath = path.join(EXPORTS_DIR, `${id}.csv`);
    if (!fs.existsSync(filePath)) {
        return res.status(404).json({ error: 'Export file missing' });
    }
    const safeFile = job.listName.replace(/[^a-z0-9-_]+/gi, '_');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${safeFile}.csv"`);
    fs.createReadStream(filePath).pipe(res);
});

// Remove a prepared CSV so the next export builds fresh. Used when a job
// is stuck, stale, or the user just wants to re-run with current data.
// Deleting a `building` job leaves its background builder running but
// orphaned — the job record is gone so the file-write will still finish,
// but nothing references it and the hourly sweep will prune it.
app.delete('/api/export-jobs/:id', async (req, res) => {
    const id = req.params.id;
    const job = exportJobs.get(id);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    exportJobs.delete(id);
    await fsp.unlink(path.join(EXPORTS_DIR, `${id}.csv`)).catch(() => {});
    schedulePersist();
    res.json({ ok: true });
});

// ============================================
// DELETE JOBS — async list-deletion worker
//
// Mirrors the export-jobs pattern: in-memory Map + on-disk registry,
// debounced persistence, restart-resume. The deletion itself is the
// chunked enrichments→contacts→list_row flow that used to live inline
// on the DELETE /api/import-lists/:id endpoint.
//
// Resumability: each iteration of the worker re-queries `contacts WHERE
// lead_list_name = $1`, so an interrupted job picks up exactly where it
// left off — the page it was about to delete is the same page the next
// run sees. We don't checkpoint anything; the contacts table itself is
// the cursor. Persisted counters (contactsDeleted, enrichmentsDeleted)
// are only used for progress UI.
// ============================================

const DELETE_JOBS_REGISTRY = path.join(EXPORTS_DIR, 'delete-jobs.json');
// Keep finished jobs around for a day so the user can see the "done"
// state when they come back to the tab. After that they're noise.
const DELETE_RETENTION_MS = 24 * 60 * 60 * 1000;

type DeleteJobStatus = 'running' | 'done' | 'failed';
interface DeleteJob {
    id: string;
    listId: string;
    listName: string;
    status: DeleteJobStatus;
    contactsTotal: number;       // snapshotted at start; just for UI progress
    contactsDeleted: number;
    enrichmentsDeleted: number;
    createdAt: string;
    completedAt?: string;
    error?: string;
}

const deleteJobs = new Map<string, DeleteJob>();
let deletePersistTimer: NodeJS.Timeout | null = null;

const scheduleDeletePersist = () => {
    if (deletePersistTimer) return;
    deletePersistTimer = setTimeout(async () => {
        deletePersistTimer = null;
        try {
            const payload = JSON.stringify(Array.from(deleteJobs.values()), null, 2);
            await fsp.writeFile(DELETE_JOBS_REGISTRY, payload, 'utf-8');
        } catch (e) {
            console.error('[Delete] Failed to persist jobs registry:', e);
        }
    }, 200);
};

async function loadDeleteJobs() {
    await fsp.mkdir(EXPORTS_DIR, { recursive: true });
    try {
        const raw = await fsp.readFile(DELETE_JOBS_REGISTRY, 'utf-8');
        const arr: DeleteJob[] = JSON.parse(raw);
        for (const j of arr) deleteJobs.set(j.id, j);
    } catch {
        // First boot — registry doesn't exist yet.
    }

    // Resume any job that was mid-run when the server died. Unlike export
    // jobs (which write a partial file we have to throw away), deletion is
    // idempotent — the next page query just returns whatever's still in
    // the contacts table. Counters are reset to keep the progress bar
    // honest about the work this run is doing.
    for (const j of deleteJobs.values()) {
        if (j.status === 'running') {
            console.log(`[Delete] Resuming interrupted job ${j.id} for "${j.listName}"`);
            runDeleteJob(j).catch(err => console.error(`[Delete] Resumed job ${j.id} crashed:`, err));
        }
    }
    scheduleDeletePersist();
}

async function pruneExpiredDeleteJobs() {
    const now = Date.now();
    let pruned = 0;
    for (const [id, job] of deleteJobs) {
        if (job.status === 'running') continue;
        const ref = new Date(job.completedAt || job.createdAt).getTime();
        if (now - ref > DELETE_RETENTION_MS) {
            deleteJobs.delete(id);
            pruned++;
        }
    }
    if (pruned > 0) {
        scheduleDeletePersist();
    }
}

async function runDeleteJob(job: DeleteJob) {
    // PAGE_LIMIT > 1000 is now safe: the contact_id list never crosses
    // the wire as a URL parameter — the RPC scans contacts.lead_list_name
    // and DELETEs both tables atomically server-side. The previous
    // Node-side .in(ids) approach overflowed PostgREST's request-line
    // cap and 400'd before deleting a single row.
    const PAGE_LIMIT = 5000;
    try {
        while (true) {
            const { data, error } = await supabase
                .rpc('delete_import_list_page', {
                    p_list_name: job.listName,
                    p_limit: PAGE_LIMIT,
                });
            if (error) throw error;
            const row = (data || [])[0] || { contacts_deleted: 0, enrichments_deleted: 0 };
            const c = Number(row.contacts_deleted) || 0;
            const e = Number(row.enrichments_deleted) || 0;
            job.contactsDeleted += c;
            job.enrichmentsDeleted += e;
            scheduleDeletePersist();
            // Page returned no contacts → list is empty, exit the loop.
            // Also doubles as the defensive guard against an RLS-blocked
            // page silently returning zero.
            if (c === 0) break;
        }

        // List row goes last so a mid-flight failure leaves the entry
        // visible — the user can retry instead of being stuck with
        // orphaned contacts under a name that has no list to click.
        const { error: delListErr } = await supabase
            .from('import_lists')
            .delete()
            .eq('id', job.listId);
        if (delListErr) throw delListErr;

        job.status = 'done';
        job.completedAt = new Date().toISOString();
        scheduleDeletePersist();
        addServerLog(`🗑️ Deleted list "${job.listName}": ${job.contactsDeleted.toLocaleString()} contacts, ${job.enrichmentsDeleted.toLocaleString()} enrichments.`, 'Sync', 'info');
    } catch (err: any) {
        job.status = 'failed';
        job.error = err.message || String(err);
        job.completedAt = new Date().toISOString();
        scheduleDeletePersist();
        console.error(`[Delete] Job ${job.id} for "${job.listName}" failed:`, err);
    }
}

// Polled by the client — same idea as /api/export-jobs.
app.get('/api/delete-jobs', (_req, res) => {
    res.json(Array.from(deleteJobs.values())
        .sort((a, b) => (b.createdAt).localeCompare(a.createdAt)));
});

// Clear a finished/failed job from the registry. Refused while running
// so the client can't accidentally orphan an in-flight worker (it would
// keep deleting from the DB but no record would exist to surface
// progress or completion).
app.delete('/api/delete-jobs/:id', (req, res) => {
    const id = String(req.params?.id || '');
    const job = deleteJobs.get(id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    if (job.status === 'running') return res.status(409).json({ error: 'Cannot clear a running job' });
    deleteJobs.delete(id);
    scheduleDeletePersist();
    res.json({ ok: true });
});

// ============================================
// BUCKETING — wider campaign-segment assignment
//
// Phase 1: discover identity/specialization taxonomy from enriched labels.
// Phase 2: route every selected contact using company-specific enrichment
// context, then roll up contact-level decisions into campaign buckets.
// Each run is its own campaign — same contact can land in different buckets
// across runs, history is preserved.
// ============================================

const bucketingLog = (msg: string, level: 'info' | 'warn' | 'error' = 'info') => {
    addServerLog(msg, 'Bucketing', level === 'info' ? 'info' : level);
};

// Build a per-run BucketingCtx that the bucketing service uses to emit
// live logs and progress. Logs are persisted to bucketing_run_logs and
// also relayed to the global pipeline_logs stream. Progress writes are
// throttled (one DB write per ~700ms) so tight loops don't spam Supabase.
function buildBucketingCtx(runId: string) {
    let lastWrite = 0;
    let pending: any = null;
    const startTime = Date.now();
    const phaseStartedAt = new Map<string, number>();

    const flush = async () => {
        if (!pending) return;
        const snap = pending; pending = null;
        const elapsed = (Date.now() - (phaseStartedAt.get(snap.phase) || startTime)) / 1000;
        const pct = (typeof snap.total === 'number' && snap.total > 0 && typeof snap.current === 'number')
            ? Math.min(100, Math.round((snap.current / snap.total) * 100))
            : null;
        const eta = (typeof snap.current === 'number' && snap.current > 0
                && typeof snap.total === 'number' && snap.total > snap.current
                && elapsed > 1)
            ? Math.round((elapsed / snap.current) * (snap.total - snap.current))
            : null;
        try {
            await supabase.from('bucketing_runs').update({
                progress: { ...snap, pct, eta_seconds: eta, elapsed_seconds: Math.round(elapsed), updated_at: new Date().toISOString() }
            }).eq('id', runId);
        } catch (e) { /* swallow — progress is best-effort */ }
    };

    return {
        log: (msg: string, level: 'info' | 'warn' | 'error' | 'phase' = 'info') => {
            bucketingLog(msg, level === 'phase' ? 'info' : level);
            // Append to per-run log stream — fire-and-forget.
            supabase.from('bucketing_run_logs').insert({
                bucketing_run_id: runId,
                level,
                message: msg
            }).then(({ error }) => {
                if (error) console.warn('[bucketing_run_logs] write failed:', error.message);
            });
        },
        progress: (p: any) => {
            if (!phaseStartedAt.has(p.phase)) phaseStartedAt.set(p.phase, Date.now());
            pending = { ...(pending || {}), ...p };
            const now = Date.now();
            if (now - lastWrite < 700) return;
            lastWrite = now;
            flush();
        },
        // Polled at every phase boundary. If the user clicked Stop,
        // bucketing_runs.cancel_requested = true and we throw so the
        // service unwinds cleanly.
        checkCancel: async () => {
            const { data } = await supabase
                .from('bucketing_runs').select('cancel_requested').eq('id', runId).single();
            if (data?.cancel_requested) throw new BucketingCancelledError();
        }
    };
}

// Create a new run + fire taxonomy proposal in the background.
app.post('/api/bucketing/determine', async (req, res) => {
    const { name, list_names, min_volume, bucket_budget, preferred_library_ids } = req.body || {};
    if (!name || typeof name !== 'string') return res.status(400).json({ error: 'name is required' });
    if (!Array.isArray(list_names) || list_names.length === 0) {
        return res.status(400).json({ error: 'list_names must be a non-empty array' });
    }

    try {
        const { data, error } = await supabase.from('bucketing_runs').insert({
            name: name.trim(),
            list_names,
            min_volume: typeof min_volume === 'number' && min_volume >= 0 ? Math.floor(min_volume) : 50,
            bucket_budget: typeof bucket_budget === 'number' && bucket_budget > 0 ? Math.floor(bucket_budget) : 30,
            preferred_library_ids: Array.isArray(preferred_library_ids) ? preferred_library_ids : [],
            status: 'taxonomy_pending'
        }).select().single();
        if (error) return res.status(500).json({ error: error.message });

        // Fire-and-forget taxonomy build. Errors land on the run row, not the
        // response — the client already has the run id from this 202.
        runTaxonomyProposal(supabase, data.id, buildBucketingCtx(data.id)).catch(async (err: any) => {
            const cancelled = err instanceof BucketingCancelledError;
            console.error(`[Bucketing] Taxonomy ${cancelled ? 'cancelled' : 'failed'} for ${data.id}:`, err);
            await supabase.from('bucketing_runs').update({
                status: cancelled ? 'cancelled' : 'failed',
                cancel_requested: false,
                error_message: cancelled ? 'Cancelled by user' : (err.message?.slice(0, 1000) || String(err))
            }).eq('id', data.id);
        });

        res.status(202).json(data);
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/bucketing/runs', async (_req, res) => {
    try {
        const { data, error } = await supabase
            .from('bucketing_runs')
            .select('id,name,list_names,min_volume,status,total_contacts,assigned_contacts,cost_usd,quality_warnings,created_at,taxonomy_completed_at,assignment_completed_at,error_message')
            .order('created_at', { ascending: false });
        if (error) return res.status(500).json({ error: error.message });
        res.json({ runs: data || [] });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/bucketing/runs/:id', async (req, res) => {
    const id = req.params.id;
    try {
        const { data: run, error } = await supabase
            .from('bucketing_runs').select('*').eq('id', id).single();
        if (error) return res.status(404).json({ error: error.message });

        // Per-bucket contact counts. For taxonomy_ready: counts come from the
        // map (predicted size). For completed: counts come from actual
        // assignments table. Both via RPC for one query each.
        let bucketCounts: any[] = [];
        let sectorMix: any[] = [];
        let generalBreakdown: any[] = [];
        if (run.status === 'completed' || run.status === 'assigning') {
            const { data: counts } = await supabase
                .rpc('get_bucket_assignment_counts', { p_run_id: id });
            bucketCounts = counts || [];

            // Sector_focus distribution per bucket — drives the "served sectors"
            // chip on Results. SQL-aggregated to dodge PostgREST's 1000-row cap.
            const { data: smix } = await supabase
                .rpc('get_bucket_sector_mix', { p_run_id: id });
            sectorMix = (smix || []) as any[];

            const { data: gmix } = await supabase
                .rpc('get_bucket_general_breakdown', { p_run_id: id });
            generalBreakdown = (gmix || []) as any[];
        } else if (run.status === 'taxonomy_ready') {
            const { data: counts } = await supabase
                .rpc('get_bucket_map_counts', { p_run_id: id });
            bucketCounts = counts || [];
        }

        res.json({ run, bucket_counts: bucketCounts, sector_mix: sectorMix, general_breakdown: generalBreakdown });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Apply user edits (rename / drop / add / threshold / bucket_budget) to the proposed taxonomy.
app.patch('/api/bucketing/runs/:id/taxonomy', async (req, res) => {
    const id = req.params.id;
    const { keep, rename, add, min_volume, bucket_budget } = req.body || {};
    try {
        await applyTaxonomyEdits(supabase, id, { keep, rename, add, min_volume, bucket_budget }, buildBucketingCtx(id));
        const { data: run } = await supabase
            .from('bucketing_runs').select('*').eq('id', id).single();
        const { data: counts } = await supabase
            .rpc('get_bucket_map_counts', { p_run_id: id });
        res.json({ run, bucket_counts: counts || [] });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Kick off Phase 2 in the background.
app.post('/api/bucketing/runs/:id/assign', async (req, res) => {
    const id = req.params.id;
    try {
        const { data: run, error } = await supabase
            .from('bucketing_runs').select('*').eq('id', id).single();
        if (error || !run) return res.status(404).json({ error: error?.message || 'Run not found' });
        if (run.status === 'assigning') {
            return res.status(409).json({ error: 'Assignment already in progress' });
        }
        if (run.status !== 'taxonomy_ready' && run.status !== 'completed') {
            return res.status(400).json({ error: `Cannot assign from status: ${run.status}` });
        }

        // Flip status BEFORE returning so the client's next refresh sees
        // 'assigning' and renders the live progress panel. Without this,
        // runAssignment() flips status as its first step but that races
        // with the client's refresh after the 202 — UI stayed stuck on
        // the Review screen for the entire Phase 1b run.
        await supabase.from('bucketing_runs').update({
            status: 'assigning',
            cancel_requested: false,
            progress: {
                phase: 'phase1b',
                step: 'starting',
                note: 'Starting Phase 1b…',
                pct: null,
                eta_seconds: null,
                elapsed_seconds: 0,
                updated_at: new Date().toISOString()
            }
        }).eq('id', id);
        runAssignment(supabase, id, buildBucketingCtx(id)).catch(async (err: any) => {
            const cancelled = err instanceof BucketingCancelledError;
            console.error(`[Bucketing] Assignment ${cancelled ? 'cancelled' : 'failed'} for ${id}:`, err);
            await supabase.from('bucketing_runs').update({
                status: cancelled ? 'cancelled' : 'failed',
                cancel_requested: false,
                error_message: cancelled ? 'Cancelled by user' : (err.message?.slice(0, 1000) || String(err))
            }).eq('id', id);
        });

        res.status(202).json({ ok: true, id });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Stop / cancel a running bucketing job. Sets cancel_requested=true; the
// service polls this flag at every phase boundary and unwinds cleanly,
// landing the run at status='cancelled' with error_message='Cancelled by user'.
app.post('/api/bucketing/runs/:id/cancel', async (req, res) => {
    const id = req.params.id;
    try {
        const { data: run } = await supabase
            .from('bucketing_runs').select('status').eq('id', id).single();
        if (!run) return res.status(404).json({ error: 'Run not found' });
        if (run.status !== 'taxonomy_pending' && run.status !== 'assigning') {
            return res.status(400).json({ error: `Cannot cancel from status: ${run.status}` });
        }
        await supabase.from('bucketing_runs')
            .update({ cancel_requested: true })
            .eq('id', id);
        res.json({ ok: true });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Resume a cancelled run. If the cancel happened during Phase 1a, retrigger
// runTaxonomyProposal. If during Phase 1b, retrigger runAssignment. Both
// phases are idempotent at the phase level — they wipe and rebuild the
// per-phase output tables so the resume is clean.
app.post('/api/bucketing/runs/:id/resume', async (req, res) => {
    const id = req.params.id;
    try {
        const { data: run, error } = await supabase
            .from('bucketing_runs').select('*').eq('id', id).single();
        if (error || !run) return res.status(404).json({ error: error?.message || 'Run not found' });
        if (run.status !== 'cancelled') {
            return res.status(400).json({ error: `Cannot resume from status: ${run.status}` });
        }

        // Decide which phase to resume from. If a taxonomy_proposal already
        // exists, Phase 1a finished; resume Phase 1b. Otherwise restart 1a.
        const resumePhase: 'phase1a' | 'phase1b' = run.taxonomy_proposal ? 'phase1b' : 'phase1a';

        await supabase.from('bucketing_runs').update({
            cancel_requested: false,
            error_message: null,
            status: resumePhase === 'phase1a' ? 'taxonomy_pending' : 'assigning'
        }).eq('id', id);

        const ctx = buildBucketingCtx(id);
        const fn = resumePhase === 'phase1a'
            ? () => runTaxonomyProposal(supabase, id, ctx)
            : () => runAssignment(supabase, id, ctx);

        fn().catch(async (err: any) => {
            const cancelled = err instanceof BucketingCancelledError;
            console.error(`[Bucketing] Resumed ${resumePhase} ${cancelled ? 'cancelled' : 'failed'} for ${id}:`, err);
            await supabase.from('bucketing_runs').update({
                status: cancelled ? 'cancelled' : 'failed',
                cancel_requested: false,
                error_message: cancelled ? 'Cancelled by user' : (err.message?.slice(0, 1000) || String(err))
            }).eq('id', id);
        });

        res.status(202).json({ ok: true, resumed: resumePhase });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Live log stream for a run. UI passes ?since=<last_id> to get only new
// rows since the previous poll. Returns up to 500 entries per call.
app.get('/api/bucketing/runs/:id/logs', async (req, res) => {
    const id = req.params.id;
    const sinceParam = req.query.since ? Number(req.query.since) : 0;
    const since = Number.isFinite(sinceParam) ? sinceParam : 0;
    try {
        const { data, error } = await supabase
            .from('bucketing_run_logs')
            .select('id,timestamp,level,message')
            .eq('bucketing_run_id', id)
            .gt('id', since)
            .order('id', { ascending: true })
            .limit(500);
        if (error) return res.status(500).json({ error: error.message });
        res.json({ logs: data || [] });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Paginated assignments for a run, optionally filtered by bucket.
app.get('/api/bucketing/runs/:id/contacts', async (req, res) => {
    const id = req.params.id;
    const bucket = req.query.bucket ? String(req.query.bucket) : null;
    const page = Math.max(1, parseInt(String(req.query.page || '1'), 10));
    const pageSize = Math.min(500, Math.max(10, parseInt(String(req.query.pageSize || '100'), 10)));

    try {
        // No FK between bucket_assignments and contacts, so we can't use
        // PostgREST's embed syntax. Fetch the assignment page first, then
        // hydrate contact fields via a second IN-list query.
        let q: any = supabase.from('bucket_assignments')
            .select('contact_id,bucket_name,bucket_leaf,bucket_ancestor,bucket_root,primary_identity,functional_core,functional_specialization,sector_core,sector_focus,canonical_classification,bucket_reason,pre_rollup_bucket_name,rollup_level,general_reason,reasons,is_generic,is_disqualified,source,confidence,assigned_at', { count: 'estimated' })
            .eq('bucketing_run_id', id);
        if (bucket) q = q.eq('bucket_name', bucket);
        q = q.order('assigned_at', { ascending: true })
            .range((page - 1) * pageSize, page * pageSize - 1);
        const { data: assigns, count, error } = await q;
        if (error) return res.status(500).json({ error: error.message });

        const ids = (assigns || []).map((r: any) => r.contact_id);
        let contactMap = new Map<string, any>();
        if (ids.length > 0) {
            const { data: cs, error: cErr } = await supabase
                .from('contacts')
                .select('contact_id,email,first_name,last_name,company_name,company_website,industry,lead_list_name')
                .in('contact_id', ids);
            if (cErr) return res.status(500).json({ error: cErr.message });
            for (const c of (cs || []) as any[]) contactMap.set(c.contact_id, c);
        }

        const data = (assigns || []).map((r: any) => ({
            ...r,
            contacts: contactMap.get(r.contact_id) || null,
        }));
        res.json({ data, count: count || 0, page, pageSize });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Delete a bucketing run (cascades to map + assignments).
app.delete('/api/bucketing/runs/:id', async (req, res) => {
    const id = req.params.id;
    try {
        const { error } = await supabase.from('bucketing_runs').delete().eq('id', id);
        if (error) return res.status(500).json({ error: error.message });
        res.json({ ok: true });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// ============================================
// BUCKET LIBRARY — reusable buckets across runs
// ============================================

app.get('/api/bucketing/library', async (req, res) => {
    try {
        const includeArchived = String(req.query.include_archived || '').toLowerCase() === 'true';
        const buckets = await listLibrary(supabase, { includeArchived });
        res.json({ buckets });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/bucketing/library', async (req, res) => {
    try {
        const bucket = await upsertLibraryBucket(supabase, req.body || {});
        res.json(bucket);
    } catch (err: any) {
        res.status(400).json({ error: err.message });
    }
});

// Bulk import from a newline+pipe text blob:
//   "SEO Agency"
//   "SEO Agency | Agency"
//   "SEO Agency | Agency | Performance + content marketing for B2B"
app.post('/api/bucketing/library/bulk-import', async (req, res) => {
    try {
        const text = String((req.body || {}).text || '');
        if (!text.trim()) return res.status(400).json({ error: 'text is required' });
        const result = await bulkImportLibraryFromText(supabase, text);
        res.json(result);
    } catch (err: any) {
        res.status(400).json({ error: err.message });
    }
});

app.patch('/api/bucketing/library/:id/archive', async (req, res) => {
    try {
        const archived = !!(req.body || {}).archived;
        await archiveLibraryBucket(supabase, req.params.id, archived);
        res.json({ ok: true, archived });
    } catch (err: any) {
        res.status(400).json({ error: err.message });
    }
});

app.delete('/api/bucketing/library/:id', async (req, res) => {
    try {
        await deleteLibraryBucket(supabase, req.params.id);
        res.json({ ok: true });
    } catch (err: any) {
        res.status(400).json({ error: err.message });
    }
});

// Save selected buckets from a completed run into the library.
app.post('/api/bucketing/runs/:id/save-to-library', async (req, res) => {
    try {
        const bucketNames: string[] = Array.isArray((req.body || {}).bucket_names) ? req.body.bucket_names : [];
        if (bucketNames.length === 0) return res.status(400).json({ error: 'bucket_names array required' });
        const result = await saveRunBucketsToLibrary(supabase, req.params.id, bucketNames);
        res.json(result);
    } catch (err: any) {
        res.status(400).json({ error: err.message });
    }
});

// ============================================
// TAXONOMY LIBRARY — editable Identity / Characteristic / Sector lists
// ============================================
//
// The Phase 1a tagger reads these tables every run as the allowed set
// of tags. AI-proposed additions (created_by='ai') surface in the
// Review screen with Accept / Reject controls.

const TAXONOMY_TABLES = {
    identities: 'taxonomy_identities',
    characteristics: 'taxonomy_characteristics',
    sectors: 'taxonomy_sectors'
} as const;

type TaxonomyKind = keyof typeof TAXONOMY_TABLES;

function pickTaxonomyTable(kind: string): string | null {
    return (TAXONOMY_TABLES as any)[kind] || null;
}

app.get('/api/bucketing/taxonomy', async (_req, res) => {
    try {
        const [idRes, chRes, secRes] = await Promise.all([
            supabase.from('taxonomy_identities').select('*').order('sort_order').order('name'),
            supabase.from('taxonomy_characteristics').select('*').order('sort_order').order('name'),
            supabase.from('taxonomy_sectors').select('*').order('sort_order').order('name')
        ]);
        if (idRes.error) throw new Error(idRes.error.message);
        if (chRes.error) throw new Error(chRes.error.message);
        if (secRes.error) throw new Error(secRes.error.message);
        res.json({
            identities: idRes.data || [],
            characteristics: chRes.data || [],
            sectors: secRes.data || []
        });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/bucketing/taxonomy/:kind', async (req, res) => {
    try {
        const table = pickTaxonomyTable(req.params.kind);
        if (!table) return res.status(400).json({ error: 'unknown taxonomy kind' });
        const body = req.body || {};
        const name = String(body.name || '').trim();
        if (!name) return res.status(400).json({ error: 'name is required' });

        const row: any = {
            name,
            description: typeof body.description === 'string' ? body.description : null,
            created_by: body.created_by === 'ai' ? 'ai' : 'user',
            archived: false,
            updated_at: new Date().toISOString()
        };
        if (req.params.kind === 'identities') {
            row.is_disqualified = !!body.is_disqualified;
        }
        if (req.params.kind === 'characteristics') {
            const parent = String(body.parent_identity || '').trim();
            if (!parent) return res.status(400).json({ error: 'parent_identity is required for characteristics' });
            row.parent_identity = parent;
        }
        if (req.params.kind === 'sectors') {
            row.synonyms = typeof body.synonyms === 'string' ? body.synonyms : null;
        }
        const { data, error } = await supabase.from(table).upsert(row, { onConflict: 'name' }).select().single();
        if (error) return res.status(400).json({ error: error.message });
        res.json(data);
    } catch (err: any) {
        res.status(400).json({ error: err.message });
    }
});

app.patch('/api/bucketing/taxonomy/:kind/:id', async (req, res) => {
    try {
        const table = pickTaxonomyTable(req.params.kind);
        if (!table) return res.status(400).json({ error: 'unknown taxonomy kind' });
        const update: any = { updated_at: new Date().toISOString() };
        const body = req.body || {};
        if (typeof body.name === 'string' && body.name.trim()) update.name = body.name.trim();
        if (typeof body.description === 'string') update.description = body.description;
        if (typeof body.archived === 'boolean') update.archived = body.archived;
        if (typeof body.created_by === 'string') update.created_by = body.created_by;
        if (req.params.kind === 'identities' && typeof body.is_disqualified === 'boolean') {
            update.is_disqualified = body.is_disqualified;
        }
        if (req.params.kind === 'characteristics' && typeof body.parent_identity === 'string' && body.parent_identity.trim()) {
            update.parent_identity = body.parent_identity.trim();
        }
        if (req.params.kind === 'sectors' && typeof body.synonyms === 'string') {
            update.synonyms = body.synonyms;
        }
        const { data, error } = await supabase.from(table).update(update).eq('id', req.params.id).select().single();
        if (error) return res.status(400).json({ error: error.message });
        res.json(data);
    } catch (err: any) {
        res.status(400).json({ error: err.message });
    }
});

app.delete('/api/bucketing/taxonomy/:kind/:id', async (req, res) => {
    try {
        const table = pickTaxonomyTable(req.params.kind);
        if (!table) return res.status(400).json({ error: 'unknown taxonomy kind' });
        const { error } = await supabase.from(table).delete().eq('id', req.params.id);
        if (error) return res.status(400).json({ error: error.message });
        res.json({ ok: true });
    } catch (err: any) {
        res.status(400).json({ error: err.message });
    }
});

// AI-proposed additions surfaced from Phase 1a results — `is_new_*=true`
// rows in bucket_industry_map for a run. Returns a deduped list per kind.
app.get('/api/bucketing/runs/:id/proposed-tags', async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('bucket_industry_map')
            .select('identity,is_new_identity,characteristic,is_new_characteristic,sector,is_new_sector,industry_string,confidence,llm_reason')
            .eq('bucketing_run_id', req.params.id)
            .or('is_new_identity.eq.true,is_new_characteristic.eq.true,is_new_sector.eq.true');
        if (error) return res.status(500).json({ error: error.message });

        const ids = new Map<string, { name: string; samples: string[]; count: number }>();
        const chars = new Map<string, { name: string; parent: string | null; samples: string[]; count: number }>();
        const secs = new Map<string, { name: string; samples: string[]; count: number }>();
        for (const r of (data || []) as any[]) {
            if (r.is_new_identity && r.identity) {
                const e = ids.get(r.identity) || { name: r.identity, samples: [], count: 0 };
                e.count++;
                if (e.samples.length < 5) e.samples.push(r.industry_string);
                ids.set(r.identity, e);
            }
            if (r.is_new_characteristic && r.characteristic) {
                const e = chars.get(r.characteristic) || { name: r.characteristic, parent: r.identity || null, samples: [], count: 0 };
                e.count++;
                if (e.samples.length < 5) e.samples.push(r.industry_string);
                chars.set(r.characteristic, e);
            }
            if (r.is_new_sector && r.sector) {
                const e = secs.get(r.sector) || { name: r.sector, samples: [], count: 0 };
                e.count++;
                if (e.samples.length < 5) e.samples.push(r.industry_string);
                secs.set(r.sector, e);
            }
        }
        res.json({
            identities: Array.from(ids.values()).sort((a, b) => b.count - a.count),
            characteristics: Array.from(chars.values()).sort((a, b) => b.count - a.count),
            sectors: Array.from(secs.values()).sort((a, b) => b.count - a.count)
        });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Low-confidence (needs_qa=true) industries for a run, with sample contacts
// — drives the Phase 1a QA panel.
app.get('/api/bucketing/runs/:id/qa-queue', async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('bucket_industry_map')
            .select('industry_string,identity,characteristic,sector,confidence,llm_reason,is_disqualified,bucket_name')
            .eq('bucketing_run_id', req.params.id)
            .eq('needs_qa', true)
            .order('confidence', { ascending: true })
            .limit(500);
        if (error) return res.status(500).json({ error: error.message });
        res.json({ queue: data || [] });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// ============================================
// CONNECTORS — UI-managed runtime secrets
// ============================================
//
// Today: Anthropic API key for Phase 1a. Stored in app_settings, masked on
// reads, never echoed back in full. The bucketing service reads via the
// cached getSetting() helper.

const ANTHROPIC_KEY_NAME = 'ANTHROPIC_API_KEY';

app.get('/api/settings/anthropic-key', async (_req, res) => {
    try {
        const stored = await getSetting(supabase, ANTHROPIC_KEY_NAME);
        const envFallback = process.env.ANTHROPIC_API_KEY || process.env.VITE_ANTHROPIC_API_KEY || null;
        const value = stored || envFallback;
        res.json({
            configured: !!value,
            source: stored ? 'app_settings' : (envFallback ? 'env' : 'none'),
            masked: maskSecret(value)
        });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/settings/anthropic-key', async (req, res) => {
    try {
        const key = (req.body?.key || '').trim();
        if (!key) return res.status(400).json({ error: 'key is required' });
        if (!key.startsWith('sk-ant-')) {
            return res.status(400).json({ error: 'Anthropic keys start with "sk-ant-"' });
        }
        await setSetting(supabase, ANTHROPIC_KEY_NAME, key);
        res.json({ configured: true, source: 'app_settings', masked: maskSecret(key) });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

app.delete('/api/settings/anthropic-key', async (_req, res) => {
    try {
        await setSetting(supabase, ANTHROPIC_KEY_NAME, null);
        res.json({ configured: false });
    } catch (err: any) {
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
    await loadExportJobs();
    await pruneExpiredExports();
    setInterval(pruneExpiredExports, 60 * 60 * 1000);

    await loadDeleteJobs();
    await pruneExpiredDeleteJobs();
    setInterval(pruneExpiredDeleteJobs, 60 * 60 * 1000);

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
