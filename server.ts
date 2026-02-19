
import './loadEnv';

import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import { fetchDigest } from './services/scraperService';
import { enrichBatch } from './services/enrichmentService';
import { MergedContact, Contact, Enrichment } from './types';

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
async function runBackgroundEnrichment(contactIds: string[]) {
    try {
        addServerLog(`Enqueuing ${contactIds.length} records in chunks...`, 'Pipeline', 'phase');
        const ENQUEUE_CHUNK_SIZE = 100;
        for (let i = 0; i < contactIds.length; i += ENQUEUE_CHUNK_SIZE) {
            const chunk = contactIds.slice(i, i + ENQUEUE_CHUNK_SIZE);
            const { error: enqueueError } = await supabase.from('enrichments').upsert(
                chunk.map(id => ({ contact_id: id, status: 'pending', processed_at: null, error_message: null })),
                { onConflict: 'contact_id' }
            );
            if (enqueueError) throw enqueueError;
            addServerLog(`Enqueued ${Math.min(i + ENQUEUE_CHUNK_SIZE, contactIds.length)}/${contactIds.length}...`, 'Sync');
        }
    } catch (e: any) {
        addServerLog(`Enqueuer Failure: ${e.message}`, 'Pipeline', 'error');
        jobStats.isProcessing = false;
        persistPipelineState();
        return;
    }

    // 1. Fetch contact data from Supabase in chunks
    const contacts: any[] = [];
    try {
        addServerLog(`Fetching data for ${contactIds.length} records...`, 'Pipeline', 'phase');
        const FETCH_CHUNK_SIZE = 100;
        for (let i = 0; i < contactIds.length; i += FETCH_CHUNK_SIZE) {
            const chunkIds = contactIds.slice(i, i + FETCH_CHUNK_SIZE);
            const { data: chunkData, error } = await supabase
                .from('contacts')
                .select('*, enrichments(*)')
                .in('contact_id', chunkIds);

            if (error) throw error;
            if (chunkData) contacts.push(...chunkData);
            addServerLog(`Fetched ${contacts.length}/${contactIds.length}...`, 'Sync');
        }
    } catch (e: any) {
        addServerLog(`Fetch Error: ${e.message}`, 'Pipeline', 'error');
        jobStats.isProcessing = false;
        persistPipelineState();
        return;
    }

    if (contacts.length === 0) {
        addServerLog(`No contact details found.`, 'Pipeline', 'warn');
        jobStats.isProcessing = false;
        persistPipelineState();
        return;
    }

    addServerLog(`Starting Enrichment Pipeline for ${contacts.length} records.`, 'Pipeline', 'phase');

    // Flatten contacts (MergedContact pattern)
    const batch: MergedContact[] = contacts.map((item: any) => {
        const enrichmentData = Array.isArray(item.enrichments) && item.enrichments.length > 0
            ? item.enrichments[0]
            : (item.enrichments || {});
        const { enrichments, ...contactData } = item;
        return { ...contactData, ...enrichmentData, status: enrichmentData.status || 'new' };
    });

    // --- ENRICHMENT CACHING (Scrape Once, Use Many) ---
    // 1. Identify all unique websites in the batch
    const uniqueWebsites = Array.from(new Set(batch.map(c => c.company_website).filter(Boolean)));
    const domainCache: Record<string, any> = {};

    if (uniqueWebsites.length > 0) {
        addServerLog(`Checking cache for ${uniqueWebsites.length} unique websites...`, 'Sync');
        const { data: cachedEnrichments } = await supabase
            .from('enrichments')
            .select(`
                classification,
                confidence,
                reasoning,
                contacts!inner(company_website)
            `)
            .eq('status', 'completed')
            .gte('confidence', 7)
            .in('contacts.company_website', uniqueWebsites);

        if (cachedEnrichments) {
            cachedEnrichments.forEach((row: any) => {
                const website = row.contacts?.company_website;
                if (website && !domainCache[website]) {
                    domainCache[website] = {
                        classification: row.classification,
                        confidence: row.confidence,
                        reasoning: row.reasoning
                    };
                }
            });
            const hitCount = Object.keys(domainCache).length;
            if (hitCount > 0) {
                addServerLog(`⚡ Cache Hit: Found high-confidence results for ${hitCount} domains.`, 'Sync');
            }
        }
    }

    const BATCH_SIZE = 10;
    let currentIndex = 0;

    while (currentIndex < batch.length) {
        const sprintItems = batch.slice(currentIndex, currentIndex + BATCH_SIZE);
        currentIndex += BATCH_SIZE;

        try {
            const sprintEnd = Math.min(currentIndex, batch.length);

            // Separate items: Cached vs Needs Scraping
            const itemsToScrape = sprintItems.filter(item => !domainCache[item.company_website]);
            const itemsFromCache = sprintItems.filter(item => domainCache[item.company_website]);

            if (itemsFromCache.length > 0) {
                addServerLog(`♻️ Reusing cache for ${itemsFromCache.length} items.`, 'Sync');
            }

            let scrapes: any[] = [];
            if (itemsToScrape.length > 0) {
                addServerLog(`Sprint [${currentIndex - BATCH_SIZE + 1}-${sprintEnd}/${batch.length}]: Scraping ${itemsToScrape.length} websites...`, 'Pipeline', 'phase');
                const scrapeResults = await Promise.all(itemsToScrape.map(async (c) => {
                    const domain = c.company_website || c.email.split('@')[1];
                    try {
                        const { digest, proxyName } = await fetchDigest(domain, msg => {
                            const level = msg.includes('failed') || msg.includes('FATAL') ? 'warn' : 'info';
                            addServerLog(msg, 'Scraper', level);
                        });
                        addServerLog(`${domain} success via ${proxyName}`, 'Scraper');
                        return { contact: c, digest, proxyName, success: true };
                    } catch (e: any) {
                        addServerLog(`${domain} FATAL: ${e.message}`, 'Scraper', 'error');
                        return { contact: c, digest: e.message, success: false };
                    }
                }));
                scrapes = scrapeResults;
            }

            // Results processing
            const validScrapes = scrapes.filter(s => s.success);
            const failedScrapes = scrapes.filter(s => !s.success);

            let aiResults: any[] = [];
            if (validScrapes.length > 0) {
                addServerLog(`Classifying ${validScrapes.length} items (Success: ${Math.round((validScrapes.length / sprintItems.length) * 100)}%)...`, 'OpenAI', 'phase');
                aiResults = await enrichBatch(validScrapes.map(s => ({
                    contact_id: s.contact.contact_id,
                    email: s.contact.email,
                    digest: s.digest
                })));
                const aiSuccessCount = aiResults.filter(r => r.status === 'completed').length;
                addServerLog(`Classification complete: ${aiSuccessCount}/${validScrapes.length} successful.`, 'OpenAI');
            }

            addServerLog(`Syncing results to Supabase...`, 'Sync');
            const enrichmentsToUpsert: Partial<Enrichment>[] = [];
            const contactsToUpdate: Partial<Contact>[] = [];

            // 1. Process Cached Items (Bypass AI entirely)
            itemsFromCache.forEach(item => {
                const cached = domainCache[item.company_website];
                if (cached) {
                    jobStats.completed++;
                    enrichmentsToUpsert.push({
                        contact_id: item.contact_id,
                        status: 'completed',
                        confidence: cached.confidence,
                        reasoning: `⚡ Reused high-confidence result (Score: ${cached.confidence}): ${cached.reasoning}`,
                        classification: cached.classification,
                        cost: 0, // Cache is free!
                        processed_at: new Date().toISOString()
                    });

                    contactsToUpdate.push({
                        id: item.id,
                        email: item.email,
                        industry: cached.classification,
                        lead_list_name: item.lead_list_name
                    } as any);
                }
            });

            // 2. Result processing for successful scrapes (AI Results)
            aiResults.forEach(res => {
                const original = validScrapes.find(s => s.contact.contact_id === res.contact_id);
                if (original) {
                    const isSuccess = res.status === 'completed' && res.classification !== 'ERROR';
                    enrichmentsToUpsert.push({
                        contact_id: res.contact_id,
                        status: isSuccess ? 'completed' : 'failed',
                        confidence: res.confidence,
                        reasoning: res.reasoning,
                        classification: isSuccess ? res.classification : undefined,
                        cost: res.cost,
                        processed_at: new Date().toISOString()
                    });

                    if (isSuccess) {
                        jobStats.completed++;
                        contactsToUpdate.push({
                            id: original.contact.id,
                            email: original.contact.email,
                            industry: res.classification,
                            lead_list_name: original.contact.lead_list_name
                        } as any);
                    } else {
                        jobStats.failed++;
                    }
                }
            });

            // ENRICHMENT GUARD: Process failed scrapes WITHOUT calling OpenAI
            failedScrapes.forEach(f => {
                jobStats.completed++; // Marked as completed to move through the queue
                enrichmentsToUpsert.push({
                    contact_id: f.contact.contact_id,
                    status: 'completed',
                    classification: 'Scrape Error',
                    confidence: 1,
                    error_message: f.digest,
                    processed_at: new Date().toISOString()
                });

                // Also update the industry in the contacts table so the user sees the error
                contactsToUpdate.push({
                    id: f.contact.id,
                    email: f.contact.email,
                    industry: 'Scrape Error',
                    lead_list_name: f.contact.lead_list_name
                } as any);
            });

            if (enrichmentsToUpsert.length > 0) {
                const { error: syncError } = await supabase.from('enrichments').upsert(enrichmentsToUpsert, { onConflict: 'contact_id' });
                if (syncError) addServerLog(`🛑 [Sync] Failed to save enrichments: ${syncError.message}`);
            }
            if (contactsToUpdate.length > 0) {
                const { error: contactError } = await supabase.from('contacts').upsert(contactsToUpdate, { onConflict: 'email' });
                if (contactError) addServerLog(`Failed to update contact records: ${contactError.message}`, 'Sync', 'error');
            }

            addServerLog(`Batch processed: ${jobStats.completed} done, ${jobStats.failed} failed`, 'Pipeline');
            await persistPipelineState();

        } catch (err: any) {
            addServerLog(`Batch Error: ${err.message}`, 'Pipeline', 'error');
            await persistPipelineState();
        }
    }

    jobStats.isProcessing = false;
    addServerLog("✨ Background task finalized.", 'Pipeline', 'phase');
    await persistPipelineState();
}

app.get('/api/status', async (req, res) => {
    // Fetch latest 200 logs from Supabase
    const { data: dbLogs } = await supabase
        .from('pipeline_logs')
        .select('*')
        .order('timestamp', { ascending: false })
        .limit(200);

    res.json({
        logs: dbLogs || currentJobLogs,
        stats: jobStats
    });
});

/**
 * Endpoints
 */

app.post('/api/enrich', (req, res) => {
    const { contactIds } = req.body;
    if (!contactIds || !Array.isArray(contactIds)) {
        return res.status(400).json({ error: 'Invalid contactIds' });
    }

    if (jobStats.isProcessing) {
        return res.status(409).json({ error: 'A job is already in progress' });
    }

    // 1. Initialize stats IMMEDIATELY so polling starts correctly
    jobStats.total = contactIds.length;
    jobStats.completed = 0;
    jobStats.failed = 0;
    jobStats.isProcessing = true;
    currentJobLogs = []; // Clear current session cache
    addServerLog(`Queuing ${contactIds.length} records...`, 'Pipeline', 'phase');
    persistPipelineState(); // Save initial state

    // 2. Fire-and-forget worker
    runBackgroundEnrichment(contactIds).catch(err => {
        console.error("Worker fatal error:", err);
        addServerLog(`Fatal Pipeline Error: ${err.message}`, 'Pipeline', 'error');
        jobStats.isProcessing = false;
        persistPipelineState();
    });

    // 3. Immediate response (202 Accepted)
    res.status(202).json({ message: 'Enrichment started in background' });
});

// All other GET requests serve React App
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

app.listen(PORT, async () => {
    console.log(`🟢 Monolithic Server running on port ${PORT}`);
    await restorePipelineState();
});
