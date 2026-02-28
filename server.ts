
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
    isProcessing: false
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
                isProcessing: data.is_processing || false
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

    // FETCH LIVE JOB STATS FROM DB
    let liveStats = { ...jobStats };
    if (jobStats.isProcessing) {
        const { data: activeJob } = await supabase
            .from('jobs')
            .select('completed_items, failed_items, total_items')
            .in('status', ['pending', 'processing'])
            .order('created_at', { ascending: false })
            .limit(1)
            .single();

        if (activeJob) {
            liveStats.completed = activeJob.completed_items || 0;
            liveStats.failed = activeJob.failed_items || 0;
            liveStats.total = activeJob.total_items || 0;

            // Sync memory
            jobStats.completed = liveStats.completed;
            jobStats.failed = liveStats.failed;
            jobStats.total = liveStats.total;
        } else {
            // Check if there are ANY jobs left to see if we're truly done
            const { count } = await supabase
                .from('jobs')
                .select('*', { count: 'exact', head: true })
                .in('status', ['pending', 'processing']);

            if (count === 0) {
                jobStats.isProcessing = false;
                liveStats.isProcessing = false;
                await supabase.from('pipeline_state').update({ is_processing: false, updated_at: new Date().toISOString() }).eq('id', 1).then();
            }
        }
    }

    res.json({
        logs: dbLogs || currentJobLogs,
        stats: liveStats
    });
});

/**
 * Endpoints
 */

app.post('/api/enrich', async (req, res) => {
    let { contactIds, filters, searchQuery } = req.body;

    // If frontend sends filters instead of raw IDs, resolve them on the backend securely
    if (filters) {
        try {
            const batch = await db.getAllFilteredContacts(filters, searchQuery);
            contactIds = batch.map((c: any) => c.contact_id);
        } catch (e: any) {
            return res.status(500).json({ error: 'Failed to resolve filtered contacts: ' + e.message });
        }
    }

    if (!contactIds || !Array.isArray(contactIds)) {
        return res.status(400).json({ error: 'Invalid contactIds or filters' });
    }

    if (jobStats.isProcessing) {
        return res.status(409).json({ error: 'A job is already in progress' });
    }

    addServerLog(`Queuing ${contactIds.length} records...`, 'Pipeline', 'phase');

    try {
        // 1. Create a new job
        const { data: jobRaw, error: errJob } = await supabase.from('jobs').insert({
            status: 'processing',
            total_items: contactIds.length,
            started_at: new Date().toISOString()
        }).select('id').single();

        if (errJob) throw errJob;
        const jobId = jobRaw.id;

        // 2. Insert items in chunks to avoid DB payload limit
        const ENQUEUE_CHUNK_SIZE = 5000;
        for (let i = 0; i < contactIds.length; i += ENQUEUE_CHUNK_SIZE) {
            const chunk = contactIds.slice(i, i + ENQUEUE_CHUNK_SIZE);
            const items = chunk.map(id => ({
                job_id: jobId,
                contact_id: id,
                status: 'pending'
            }));
            const { error: errItems } = await supabase.from('job_items').insert(items);
            if (errItems) throw errItems;

            addServerLog(`Enqueued ${Math.min(i + ENQUEUE_CHUNK_SIZE, contactIds.length)}/${contactIds.length}...`, 'Sync');
        }

        // 3. Initiate legacy pipeline state for backward compat
        jobStats = {
            total: contactIds.length,
            completed: 0,
            failed: 0,
            isProcessing: true
        };
        currentJobLogs = [];
        await persistPipelineState();

        // 4. Start the Background Processor immediately
        JobProcessor.start();

        // 5. Immediate response (202 Accepted)
        res.status(202).json({ message: 'Enrichment successfully queued in the background' });
    } catch (err: any) {
        console.error("Queueing error:", err);
        addServerLog(`Fatal Pipeline Enqueue Error: ${err.message}`, 'Pipeline', 'error');
        res.status(500).json({ error: 'Failed to queue job: ' + err.message });
    }
});

app.post('/api/stop', async (req, res) => {
    addServerLog(`⚠️ Stop command received. Halting background pipeline...`, 'Pipeline', 'warn');
    JobProcessor.stop();
    jobStats.isProcessing = false;
    await persistPipelineState();

    // Attempt to mark the currently active jobs as cancelled
    await supabase.from('jobs').update({ status: 'cancelled' }).eq('status', 'processing').then();

    res.json({ message: 'Pipeline stopping gracefully...' });
});

app.get('/api/stats/proxies', async (req, res) => {
    try {
        const { data, error } = await supabase.rpc('get_proxy_stats')

        if (error) {
            // If the RPC fails (maybe not created), fallback to grabbing all counts manually
            // This is slower but safer out of the box
            const { data: rawData, error: rawError } = await supabase
                .from('scraped_data')
                .select('proxy_used')
                .not('proxy_used', 'is', null);

            if (rawError) throw rawError;

            const stats = (rawData || []).reduce((acc: any, row) => {
                const proxy = row.proxy_used;
                acc[proxy] = (acc[proxy] || 0) + 1;
                return acc;
            }, {});

            const formattedStats = Object.keys(stats)
                .map(key => ({ proxy_used: key, success_count: stats[key] }))
                .sort((a, b) => b.success_count - a.success_count);

            return res.json(formattedStats);
        }
        res.json(data);
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
        const { data, error } = await supabase
            .from('contacts')
            .select(column)
            .not(column, 'is', null)
            .neq(column, '')
            .limit(10000);

        if (error) throw error;

        const unique = [...new Set((data || []).map((d: any) => d[column]))].sort();
        res.json(unique);
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/import', async (req, res) => {
    const { contacts } = req.body;

    if (!contacts || !Array.isArray(contacts) || contacts.length === 0) {
        return res.status(400).json({ error: 'No contacts provided' });
    }

    try {
        // Generate UUIDs for contacts that don't have a contact_id
        const prepared = contacts.map((c: any) => {
            const row: any = {};
            // Only include non-empty fields
            if (c.contact_id) row.contact_id = c.contact_id;
            else row.contact_id = crypto.randomUUID();

            if (c.email) row.email = c.email;
            if (c.first_name) row.first_name = c.first_name;
            if (c.last_name) row.last_name = c.last_name;
            if (c.company_website) row.company_website = c.company_website;
            if (c.company_name) row.company_name = c.company_name;
            if (c.industry) row.industry = c.industry;
            if (c.linkedin_url) row.linkedin_url = c.linkedin_url;
            if (c.title) row.title = c.title;
            if (c.lead_list_name) row.lead_list_name = c.lead_list_name;

            return row;
        });

        // Upsert in sub-chunks of 1000 to respect Supabase payload limits
        const CHUNK_SIZE = 1000;
        let inserted = 0;

        for (let i = 0; i < prepared.length; i += CHUNK_SIZE) {
            const chunk = prepared.slice(i, i + CHUNK_SIZE);
            const { error } = await supabase
                .from('contacts')
                .upsert(chunk, { onConflict: 'email' });

            if (error) {
                console.error(`Import chunk error at row ${i}:`, error);
                return res.status(500).json({
                    error: `Failed at row ${i}: ${error.message}`,
                    inserted
                });
            }
            inserted += chunk.length;
        }

        addServerLog(`📥 Imported ${inserted} contacts via CSV upload.`, 'Sync', 'info');
        res.json({ inserted });
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
});

// Graceful Shutdown
process.on('SIGTERM', () => {
    console.log('SIGTERM received. Shutting down gracefully...');
    JobProcessor.stop();
    setTimeout(() => process.exit(0), 5000);
});
