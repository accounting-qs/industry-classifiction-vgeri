
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
let serverLogs: string[] = [];
let jobStats = {
    total: 0,
    completed: 0,
    failed: 0,
    isProcessing: false
};

const addServerLog = (msg: string) => {
    const time = new Date().toLocaleTimeString();
    const entry = `${time}: ${msg}`;
    serverLogs = [entry, ...serverLogs].slice(0, 100);
};

app.use(cors());
app.use(express.json({ limit: '50mb' }));

// Serve static files from Vite build
app.use(express.static(path.join(__dirname, 'dist')));

/**
 * Background Enrichment Logic (Ported from App.tsx)
 */
async function runBackgroundEnrichment(contactIds: string[]) {
    try {
        addServerLog(`📦 [Phase 1/3] Enqueuing ${contactIds.length} records in chunks...`);
        const ENQUEUE_CHUNK_SIZE = 100;
        for (let i = 0; i < contactIds.length; i += ENQUEUE_CHUNK_SIZE) {
            const chunk = contactIds.slice(i, i + ENQUEUE_CHUNK_SIZE);
            const { error: enqueueError } = await supabase.from('enrichments').upsert(
                chunk.map(id => ({ contact_id: id, status: 'pending', processed_at: null, error_message: null })),
                { onConflict: 'contact_id' }
            );
            if (enqueueError) throw enqueueError;
            addServerLog(`   - Enqueued ${Math.min(i + ENQUEUE_CHUNK_SIZE, contactIds.length)}/${contactIds.length}...`);
        }
    } catch (e: any) {
        addServerLog(`🛑 [Enqueuer] Critical failure: ${e.message}`);
        jobStats.isProcessing = false;
        return;
    }

    // 1. Fetch contact data from Supabase in chunks
    const contacts: any[] = [];
    try {
        addServerLog(`📥 [Phase 2/3] Fetching data for ${contactIds.length} records...`);
        const FETCH_CHUNK_SIZE = 100;
        for (let i = 0; i < contactIds.length; i += FETCH_CHUNK_SIZE) {
            const chunkIds = contactIds.slice(i, i + FETCH_CHUNK_SIZE);
            const { data: chunkData, error } = await supabase
                .from('contacts')
                .select('*, enrichments(*)')
                .in('contact_id', chunkIds);

            if (error) throw error;
            if (chunkData) contacts.push(...chunkData);
            addServerLog(`   - Fetched ${contacts.length}/${contactIds.length}...`);
        }
    } catch (e: any) {
        addServerLog(`❌ [Fetch] Error fetching contact details: ${e.message || 'Unknown error'}`);
        jobStats.isProcessing = false;
        return;
    }

    if (contacts.length === 0) {
        addServerLog(`❌ [Fetch] No contact details found.`);
        jobStats.isProcessing = false;
        return;
    }

    addServerLog(`🚀 [Phase 3/3] Starting Enrichment Pipeline for ${contacts.length} records.`);

    // Flatten contacts (MergedContact pattern)
    const batch: MergedContact[] = contacts.map((item: any) => {
        const enrichmentData = Array.isArray(item.enrichments) && item.enrichments.length > 0
            ? item.enrichments[0]
            : (item.enrichments || {});
        const { enrichments, ...contactData } = item;
        return { ...contactData, ...enrichmentData, status: enrichmentData.status || 'new' };
    });

    const BATCH_SIZE = 10;
    let currentIndex = 0;

    while (currentIndex < batch.length) {
        const sprintItems = batch.slice(currentIndex, currentIndex + BATCH_SIZE);
        currentIndex += BATCH_SIZE;

        try {
            const sprintEnd = Math.min(currentIndex, batch.length);
            addServerLog(`🔍 Sprint [${currentIndex - BATCH_SIZE + 1}-${sprintEnd}/${batch.length}]: Scraping websites...`);

            const scrapes = await Promise.all(sprintItems.map(async (c) => {
                const domain = c.company_website || c.email.split('@')[1];
                try {
                    const { digest, proxyName } = await fetchDigest(domain, msg => addServerLog(msg));
                    addServerLog(`✅ [Scraper] ${domain} success via ${proxyName}`);
                    return { contact: c, digest, proxyName, success: true };
                } catch (e: any) {
                    addServerLog(`⚠️ [Scraper] ${domain} FATAL: ${e.message}`);
                    return { contact: c, digest: e.message, success: false };
                }
            }));

            const validScrapes = scrapes.filter(s => s.success);
            const failedScrapes = scrapes.filter(s => !s.success);

            let aiResults: any[] = [];
            if (validScrapes.length > 0) {
                addServerLog(`🧠 [OpenAI] Classifying ${validScrapes.length} items (Sprint Success: ${Math.round((validScrapes.length / sprintItems.length) * 100)}%)...`);
                aiResults = await enrichBatch(validScrapes.map(s => ({
                    contact_id: s.contact.contact_id,
                    email: s.contact.email,
                    digest: s.digest
                })));
                const aiSuccessCount = aiResults.filter(r => r.status === 'completed').length;
                addServerLog(`✅ [OpenAI] Classification complete: ${aiSuccessCount}/${validScrapes.length} successful.`);
            }

            addServerLog(`💾 Syncing results to Supabase...`);
            const enrichmentsToUpsert: Partial<Enrichment>[] = [];
            const contactsToUpdate: Partial<Contact>[] = [];

            // Result processing for successful scrapes (AI Results)
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
                if (contactError) addServerLog(`🛑 [Sync] Failed to update contact records: ${contactError.message}`);
            }

            addServerLog(`✅ Batch of ${sprintItems.length} processed. (${jobStats.completed} done, ${jobStats.failed} failed)`);

        } catch (err: any) {
            addServerLog(`🛑 Batch Error: ${err.message}`);
        }
    }

    jobStats.isProcessing = false;
    addServerLog("✨ Background task finalized.");
}

app.get('/api/status', (req, res) => {
    res.json({
        logs: serverLogs,
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
    serverLogs = []; // Clear old logs for fresh start
    addServerLog(`🚀 Queuing ${contactIds.length} records...`);

    // 2. Fire-and-forget worker
    runBackgroundEnrichment(contactIds).catch(err => {
        console.error("Worker fatal error:", err);
        addServerLog(`🛑 Fatal Pipeline Error: ${err.message}`);
        jobStats.isProcessing = false;
    });

    // 3. Immediate response (202 Accepted)
    res.status(202).json({ message: 'Enrichment started in background' });
});

// All other GET requests serve React App
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

app.listen(PORT, () => {
    console.log(`🟢 Monolithic Server running on port ${PORT}`);
});
