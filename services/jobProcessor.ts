import { createClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import { fetchDigest } from './scraperService';
import { enrichSingle } from './enrichmentService';

// Supabase Init (Service Role Key preferred for background processor)
const SUPABASE_URL = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.VITE_SUPABASE_ANON_KEY || process.env.SUPABASE_ANON_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// Tuning Parameters from Environment (with aggressive default fallbacks for speed)
const QUEUE_FETCH_CHUNK_SIZE = parseInt(process.env.QUEUE_FETCH_CHUNK_SIZE || '200', 10);
const CONCURRENCY_SCRAPE = parseInt(process.env.CONCURRENCY_SCRAPE || '30', 10);
const CONCURRENCY_AI = parseInt(process.env.CONCURRENCY_AI || '15', 10);
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '2000', 10);

// Concurrency Limiters
const scrapeLimit = pLimit(CONCURRENCY_SCRAPE);
const aiLimit = pLimit(CONCURRENCY_AI);

export class JobProcessor {
    private static isRunning = false;
    private static shouldStop = false;

    /**
     * Starts the infinite background polling loop.
     */
    public static async start() {
        if (this.isRunning) return;
        this.isRunning = true;
        this.shouldStop = false;
        console.log(`🚀 JobProcessor started [Chunk: ${QUEUE_FETCH_CHUNK_SIZE}, Scrape: C${CONCURRENCY_SCRAPE}, AI: C${CONCURRENCY_AI}]`);

        this.pollLoop();
    }

    /**
   * Gracefully requests the background loop to stop after the current chunk.
   */
    public static stop() {
        console.log('🛑 Stop signal received. JobProcessor will halt after current chunk.');
        this.shouldStop = true;
    }

    /**
     * Resets jobs that were stuck in "processing" state from a previous server crash.
     */
    public static async recoverStaleJobs() {
        try {
            const { data, error } = await supabase
                .from('job_items')
                .update({ status: 'pending', locked_at: null })
                .eq('status', 'processing')
                .select('id');

            if (!error && data && data.length > 0) {
                console.log(`♻️ Recovered ${data.length} stale job items from previous crash.`);
            }

            // Auto-start if there are any jobs still incomplete
            const { count } = await supabase
                .from('jobs')
                .select('*', { count: 'exact', head: true })
                .in('status', ['pending', 'processing']);

            if (count && count > 0) {
                console.log(`▶️ Active jobs found (${count}). Auto-starting processor...`);
                this.start();
            }
        } catch (e) {
            console.error('⚠️ Recovery failed:', e);
        }
    }

    private static async pollLoop() {
        while (!this.shouldStop) {
            try {
                const processedCount = await this.processNextChunk();

                // If we processed items, the queue is active, so poll immediately again without waiting.
                // If 0 items were found, wait a few seconds before checking the DB again.
                if (processedCount === 0) {
                    await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
                } else {
                    // Micro-sleep to allow event loop to breathe
                    await new Promise(r => setImmediate(r));
                }

                // Force garbage collection if exposed, keeps Node.js heap extremely clean on long 100k runs
                if (global.gc) {
                    global.gc();
                }
            } catch (err: any) {
                console.error('❌ JobProcessor Loop Error:', err.message);
                await new Promise(r => setTimeout(r, 5000)); // Backoff on unhandled error
            }
        }
        this.isRunning = false;
        console.log('🛑 JobProcessor fully stopped.');
    }

    private static async processNextChunk(): Promise<number> {
        // 1. Claim N pending/retrying items
        const { data: itemIdsToClaim, error: fetchError } = await supabase
            .from('job_items')
            .select('id, job_id')
            .in('status', ['pending', 'retrying'])
            .or('next_retry_at.is.null,next_retry_at.lte.now()')
            .limit(QUEUE_FETCH_CHUNK_SIZE);

        if (fetchError || !itemIdsToClaim || itemIdsToClaim.length === 0) {
            if (fetchError) console.error('Fetch Claim Error:', fetchError.message);
            return 0; // Queue empty or error
        }

        const ids = itemIdsToClaim.map(row => row.id);
        const activeJobIds = [...new Set(itemIdsToClaim.map(row => row.job_id))];

        // Mark as processing
        const { data: claimedItems, error: lockError } = await supabase
            .from('job_items')
            .update({
                status: 'processing',
                locked_at: new Date().toISOString()
            })
            .in('id', ids)
            .select('*, contacts(*)');

        if (lockError || !claimedItems || claimedItems.length === 0) {
            return 0; // Lost the race or error
        }

        console.log(`📦 Claimed chunk of ${claimedItems.length} items from ${activeJobIds.length} job(s).`);

        // --- CHUNK LOCAL CACHE ---
        const uniqueDomains = [...new Set(claimedItems.map((item: any) => item.company_website).filter(Boolean))];
        const domainCache: Record<string, any> = {};
        const digestCache: Record<string, string> = {};

        if (uniqueDomains.length > 0) {
            // Fetch prior high-confidence classifications to reuse
            const { data: cachedEnrichments } = await supabase
                .from('enrichments')
                .select(`classification, confidence, reasoning, contacts!inner(company_website)`)
                .eq('status', 'completed')
                .gte('confidence', 7)
                .in('contacts.company_website', uniqueDomains);

            if (cachedEnrichments) {
                cachedEnrichments.forEach((row: any) => {
                    const w = row.contacts?.company_website;
                    if (w) domainCache[w] = row;
                });
            }

            // Fetch prior scraped digests
            const { data: cachedDigests } = await supabase
                .from('scraped_data')
                .select('domain, content')
                .in('domain', uniqueDomains);

            if (cachedDigests) {
                cachedDigests.forEach((d: any) => digestCache[d.domain] = d.content);
            }
        }

        // --- EXECUTION PIPELINE ---
        const results = await Promise.all(claimedItems.map(async (jobItem: any) => {
            const contact = jobItem.contacts;
            const domain = jobItem.company_website || (contact.email ? contact.email.split('@')[1] : null);

            if (!domain) {
                return this.createResult(jobItem, false, 'failed', 'No domain or email to extract domain from.');
            }

            // A. Check Domain Cache (High Confidence)
            if (domainCache[domain]) {
                return this.createResult(jobItem, true, 'completed', '⚡ Reused high-confidence classification', domainCache[domain]);
            }

            // B. Check Digest Cache OR Scrape Live
            let digest = digestCache[domain];
            let proxyUsed = 'Cache';

            if (!digest) {
                try {
                    // Bounded Scraping
                    const scrapeRes = await scrapeLimit(() => fetchDigest(domain, undefined));
                    digest = scrapeRes.digest; // Fix the return access pattern
                    proxyUsed = scrapeRes.proxyName; // Fix the return access pattern

                    // Fire-and-forget digest persist
                    supabase.from('scraped_data').upsert({ domain, content: digest, proxy_used: proxyUsed }, { onConflict: 'domain' }).then();
                    // Update local chunk cache so duplicate domains in the SAME chunk don't double scrape!
                    digestCache[domain] = digest;
                } catch (e: any) {
                    // Scrape failed. Determine if retryable.
                    const isNetworkError = e.message.includes('timeout') || e.message.includes('socket') || e.message.includes('ENOTFOUND');
                    // For simplicity, let's treat FastFail as terminal.
                    const isTerminal = e.message.includes('FastFail');

                    if (!isTerminal && jobItem.attempt_count < MAX_RETRIES) {
                        return this.createRetry(jobItem, `Scraper error: ${e.message}`);
                    } else {
                        return this.createResult(jobItem, false, 'failed', `Scrape terminal error: ${e.message}`);
                    }
                }
            }

            // C. Classify with AI
            try {
                // Bounded AI
                const aiOutput = await aiLimit(() => enrichSingle({
                    contact_id: contact.contact_id, // the UUID or int8 - enrichSingle takes contact_id
                    email: contact.email,
                    digest: digest
                }));

                if (aiOutput.status === 'completed' && aiOutput.classification !== 'ERROR') { // Success
                    // Cache it locally so subsequent duplicates in SAME chunk can skip AI
                    domainCache[domain] = aiOutput;
                    return this.createResult(jobItem, true, 'completed', aiOutput.reasoning, aiOutput, digest, aiOutput.cost);
                } else {
                    if (jobItem.attempt_count < MAX_RETRIES) {
                        return this.createRetry(jobItem, `AI error: ${aiOutput.reasoning}`);
                    }
                    return this.createResult(jobItem, false, 'failed', `AI terminal error: ${aiOutput.reasoning}`);
                }
            } catch (aiErr: any) {
                if (jobItem.attempt_count < MAX_RETRIES) {
                    return this.createRetry(jobItem, `AI exception: ${aiErr.message}`);
                }
                return this.createResult(jobItem, false, 'failed', `AI terminal exception: ${aiErr.message}`);
            }
        }));

        // --- BULK DATABASE UPDATE ---
        const jobsToUpdate: Record<string, { completed: number, failed: number }> = {};
        const itemsToUpsert: any[] = [];
        const enrichmentsToUpsert: any[] = [];
        const contactsToUpdate: any[] = [];

        results.forEach(res => {
            if (!jobsToUpdate[res.jobId]) jobsToUpdate[res.jobId] = { completed: 0, failed: 0 };
            if (res.newStatus === 'completed') jobsToUpdate[res.jobId].completed++;
            if (res.newStatus === 'failed') jobsToUpdate[res.jobId].failed++;

            itemsToUpsert.push(res.jobItemUpdate);

            if (res.enrichmentUpdate) {
                enrichmentsToUpsert.push(res.enrichmentUpdate);
            }
            if (res.contactUpdate) {
                contactsToUpdate.push(res.contactUpdate);
            }
        });

        // 1. Update Job Items
        if (itemsToUpsert.length > 0) {
            await supabase.from('job_items').upsert(itemsToUpsert, { onConflict: 'id' });
        }

        // 2. Update Contacts
        if (contactsToUpdate.length > 0) {
            await supabase.from('contacts').upsert(contactsToUpdate, { onConflict: 'id' });
        }

        // 3. Update Enrichments
        if (enrichmentsToUpsert.length > 0) {
            await supabase.from('enrichments').upsert(enrichmentsToUpsert, { onConflict: 'contact_id' }); // contact_id is UUID in enrichments
        }

        // 4. Increment Job Counters
        for (const [jid, tallies] of Object.entries(jobsToUpdate)) {
            if (tallies.completed > 0 || tallies.failed > 0) {
                const { data: currentJob } = await supabase.from('jobs').select('completed_items, failed_items').eq('id', jid).single();
                if (currentJob) {
                    await supabase.from('jobs').update({
                        completed_items: currentJob.completed_items + tallies.completed,
                        failed_items: currentJob.failed_items + tallies.failed
                    }).eq('id', jid);
                }
            }
        }

        // Update old pipeline state for backend UI compatibility
        await supabase.from('pipeline_state').update({
            is_processing: true,
            updated_at: new Date().toISOString()
        }).eq('id', 1).then();

        return claimedItems.length;
    }

    // Helpers
    private static createResult(jobItem: any, isSuccess: boolean, status: 'completed' | 'failed', errorOrReasoning: string, classificationData?: any, digest?: string, cost: number = 0) {
        const contact = jobItem.contacts;

        const enr = {
            contact_id: contact.contact_id, // The UUID for enrichments table
            status: status,
            confidence: classificationData?.confidence || 1,
            reasoning: errorOrReasoning,
            classification: classificationData?.classification || (isSuccess ? "Unknown" : "Scrape Error"),
            page_html: digest || undefined,
            cost: cost,
            processed_at: new Date().toISOString()
        };

        const cnt = {
            id: contact.id, // The int8 for contacts table primary key
            email: contact.email,
            industry: enr.classification,
            lead_list_name: contact.lead_list_name
        };

        return {
            jobId: jobItem.job_id,
            newStatus: status,
            jobItemUpdate: {
                id: jobItem.id,
                status: status,
                error_message: isSuccess ? null : errorOrReasoning,
                finished_at: new Date().toISOString(),
            },
            enrichmentUpdate: enr,
            contactUpdate: cnt
        };
    }

    private static createRetry(jobItem: any, errorMsg: string) {
        // Exponential backoff
        const nextRetryMs = Math.pow(2, jobItem.attempt_count) * 2000;

        return {
            jobId: jobItem.job_id,
            newStatus: 'retrying',
            jobItemUpdate: {
                id: jobItem.id,
                status: 'retrying',
                attempt_count: jobItem.attempt_count + 1,
                next_retry_at: new Date(Date.now() + nextRetryMs).toISOString(),
                error_message: errorMsg,
                finished_at: null,
                locked_at: null
            },
            enrichmentUpdate: null,
            contactUpdate: null
        };
    }
}
