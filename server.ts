
import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import { fetchDigest } from './services/scraperService';
import { enrichBatch } from './services/enrichmentService';
import { MergedContact, Contact, Enrichment } from './types';

dotenv.config({ path: '.env.local' });
dotenv.config(); // Fallback to .env for Render

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3001;

// Supabase Init (Using SERVICE_ROLE_KEY for background updates)
const SUPABASE_URL = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL || 'https://zxnaxtdeujunujnjaweo.supabase.co';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_SERVICE_KEY) {
    console.error("CRITICAL: SUPABASE_SERVICE_ROLE_KEY is missing!");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY || '');

app.use(cors());
app.use(express.json({ limit: '50mb' }));

// Serve static files from Vite build
app.use(express.static(path.join(__dirname, 'dist')));

/**
 * Background Enrichment Logic (Ported from App.tsx)
 */
async function runBackgroundEnrichment(contactIds: string[]) {
    console.log(`🚀 [Server Worker] Starting enrichment for ${contactIds.length} records.`);

    // 1. Fetch contact data from Supabase
    const { data: contacts, error } = await supabase
        .from('contacts')
        .select('*, enrichments(*)')
        .in('contact_id', contactIds);

    if (error || !contacts) {
        console.error("Error fetching contacts for enrichment:", error);
        return;
    }

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
            console.log(`🔍 [${currentIndex}/${batch.length}] Scraping phase...`);
            const scrapes = await Promise.all(sprintItems.map(async (c) => {
                const domain = c.company_website || c.email.split('@')[1];
                try {
                    const digest = await fetchDigest(domain);
                    return { contact: c, digest, success: true };
                } catch (e: any) {
                    return { contact: c, digest: e.message, success: false };
                }
            }));

            const validScrapes = scrapes.filter(s => s.success);
            const failedScrapes = scrapes.filter(s => !s.success);

            let aiResults: any[] = [];
            if (validScrapes.length > 0) {
                console.log(`🧠 OpenAI classification...`);
                aiResults = await enrichBatch(validScrapes.map(s => ({
                    contact_id: s.contact.contact_id,
                    email: s.contact.email,
                    digest: s.digest
                })));
            }

            console.log(`💾 Syncing results to Supabase...`);
            const enrichmentsToUpsert: Partial<Enrichment>[] = [];
            const contactsToUpdate: Partial<Contact>[] = [];

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
                        contactsToUpdate.push({
                            id: original.contact.id,
                            email: original.contact.email,
                            industry: res.classification,
                            lead_list_name: original.contact.lead_list_name
                        } as any);
                    }
                }
            });

            failedScrapes.forEach(f => {
                enrichmentsToUpsert.push({
                    contact_id: f.contact.contact_id,
                    status: 'failed',
                    error_message: f.digest,
                    processed_at: new Date().toISOString()
                });
            });

            if (enrichmentsToUpsert.length > 0) {
                await supabase.from('enrichments').upsert(enrichmentsToUpsert, { onConflict: 'contact_id' });
            }
            if (contactsToUpdate.length > 0) {
                await supabase.from('contacts').upsert(contactsToUpdate, { onConflict: 'email' });
            }

        } catch (err: any) {
            console.error(`🛑 Batch Error: ${err.message}`);
        }
    }

    console.log("✨ Background task finalized.");
}

/**
 * Endpoints
 */

app.post('/api/enrich', (req, res) => {
    const { contactIds } = req.body;
    if (!contactIds || !Array.isArray(contactIds)) {
        return res.status(400).json({ error: 'Invalid contactIds' });
    }

    // 1. Mark as pending immediately in DB (Atomic enqueue)
    supabase.from('enrichments').upsert(
        contactIds.map(id => ({ contact_id: id, status: 'pending', processed_at: null })),
        { onConflict: 'contact_id' }
    ).then(({ error }) => {
        if (error) console.error("Enqueue error:", error);
    });

    // 2. Immediate response (202 Accepted)
    res.status(202).json({ message: 'Enrichment started in background' });

    // 3. Fire-and-forget worker
    runBackgroundEnrichment(contactIds).catch(err => {
        console.error("Worker fatal error:", err);
    });
});

// All other GET requests serve React App
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

app.listen(PORT, () => {
    console.log(`🟢 Monolithic Server running on port ${PORT}`);
});
