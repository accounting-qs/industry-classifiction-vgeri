import { createClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import { fetchDigest, proxyStats } from './scraperService';
import { enrichSingle } from './enrichmentService';

// Supabase Init (Service Role Key preferred for background processor)
const SUPABASE_URL = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.VITE_SUPABASE_ANON_KEY || process.env.SUPABASE_ANON_KEY || '';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// Tuning Parameters from Environment (with aggressive default fallbacks for speed)
const QUEUE_FETCH_CHUNK_SIZE = parseInt(process.env.QUEUE_FETCH_CHUNK_SIZE || '200', 10);
const CONCURRENCY_SCRAPE = parseInt(process.env.CONCURRENCY_SCRAPE || '15', 10);  // Issue #6: Reduced from 30 to 15
const CONCURRENCY_AI = parseInt(process.env.CONCURRENCY_AI || '15', 10);
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);
// Transient AI errors (5xx / timeout) get many more attempts than terminal
// ones (4xx / parse). A 5-min OpenAI outage shouldn't permanently fail an item.
const MAX_RETRIES_TRANSIENT = parseInt(process.env.MAX_RETRIES_TRANSIENT || '10', 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '2000', 10);
const STALE_PROCESSING_MINUTES = 5; // Issue #7: Auto-reset items stuck processing longer than this

// Issue #9: Common email domains that should be skipped (scraping gmail.com is pointless)
const PERSONAL_EMAIL_DOMAINS = new Set([
    'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com', 'live.com', 'msn.com',
    'yahoo.com', 'yahoo.co.uk', 'yahoo.fr', 'yahoo.de', 'ymail.com',
    'aol.com', 'icloud.com', 'me.com', 'mac.com',
    'protonmail.com', 'proton.me', 'tutanota.com', 'zoho.com',
    'mail.com', 'gmx.com', 'gmx.net', 'web.de', 'yandex.com', 'yandex.ru',
    'qq.com', '163.com', '126.com', 'sina.com',
    'comcast.net', 'verizon.net', 'att.net', 'sbcglobal.net', 'cox.net',
    'earthlink.net', 'charter.net', 'optonline.net'
]);

// Concurrency Limiters
const scrapeLimit = pLimit(CONCURRENCY_SCRAPE);
const aiLimit = pLimit(CONCURRENCY_AI);

// Hard ceiling on how long a single chunk may take. A chunk that exceeds this
// is almost certainly waiting on a dead Supabase/OpenAI socket with no TCP
// reset — the Promise.all never resolves and the pollLoop goes silent for
// hours. 10 min is comfortably above a normal chunk (~40s) but well below
// the "overnight silence" threshold we've actually seen in prod.
const CHUNK_TIMEOUT_MS = parseInt(process.env.CHUNK_TIMEOUT_MS || '600000', 10);
// Heartbeat emits a log line at this interval regardless of pipeline state,
// so a silent pollLoop is immediately visible in Render logs.
const HEARTBEAT_MS = parseInt(process.env.HEARTBEAT_MS || '60000', 10);

export class JobProcessor {
    private static isRunning = false;
    private static shouldStop = false;
    // Fix #1: Batched log buffer — avoids fire-and-forget promises per log call
    private static pendingLogs: { timestamp: string; instance_id: string; module: string; message: string; level: string }[] = [];
    private static heartbeatTimer: NodeJS.Timeout | null = null;
    // Incremented each time the pollLoop makes progress (claims a chunk or
    // confirms the queue is empty). Heartbeat reads this to report whether
    // the loop is live and advancing, vs. merely idle.
    private static loopTick = 0;
    private static lastTickAt = Date.now();

    private static log(msg: string, level: 'info' | 'warn' | 'error' | 'phase' = 'info') {
        console.log(`[JobProcessor] ${msg}`);
        this.pendingLogs.push({
            timestamp: new Date().toISOString(),
            instance_id: 'worker',
            module: 'Pipeline',
            message: msg,
            level
        });
    }

    private static async flushLogs() {
        if (this.pendingLogs.length === 0) return;
        const batch = this.pendingLogs;
        this.pendingLogs = [];
        try {
            await supabase.from('pipeline_logs').insert(batch);
        } catch (e: any) {
            console.error('[JobProcessor] Log flush failed:', e.message);
        }
    }

    /**
     * Starts the infinite background polling loop.
     */
    public static async start() {
        if (this.isRunning) return;
        this.isRunning = true;
        this.shouldStop = false;
        this.log(`🚀 JobProcessor started [Chunk: ${QUEUE_FETCH_CHUNK_SIZE}, Scrape: C${CONCURRENCY_SCRAPE}, AI: C${CONCURRENCY_AI}, GC: ${typeof global.gc === 'function' ? 'ENABLED' : 'DISABLED'}]`, 'phase');

        this.startHeartbeat();

        // Issue #3: Watchdog wrapper — auto-restarts the loop if it dies unexpectedly
        this.runWithWatchdog();
    }

    private static startHeartbeat() {
        if (this.heartbeatTimer) return;
        this.lastTickAt = Date.now();
        this.heartbeatTimer = setInterval(() => {
            const silentSec = Math.round((Date.now() - this.lastTickAt) / 1000);
            // The whole point of this log: if we see 💓 lines in Render but
            // the "last tick" keeps growing, the HTTP server is alive but
            // the pollLoop is stuck.
            console.log(`[JobProcessor] 💓 heartbeat — tick=${this.loopTick}, last_progress=${silentSec}s ago, shouldStop=${this.shouldStop}`);
        }, HEARTBEAT_MS);
    }

    private static stopHeartbeat() {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    /**
     * Issue #3: Watchdog that auto-restarts pollLoop on unexpected death.
     */
    private static async runWithWatchdog() {
        const MAX_CONSECUTIVE_CRASHES = 5;
        let consecutiveCrashes = 0;

        while (!this.shouldStop) {
            try {
                await this.pollLoop();
                // pollLoop exited cleanly (shouldStop or queue drained)
                break;
            } catch (fatal: any) {
                consecutiveCrashes++;
                this.log(`💀 pollLoop crashed unexpectedly (${consecutiveCrashes}/${MAX_CONSECUTIVE_CRASHES}): ${fatal.message}`, 'error');

                if (consecutiveCrashes >= MAX_CONSECUTIVE_CRASHES) {
                    this.log(`🚨 Too many consecutive crashes. Giving up. Manual restart required.`, 'error');
                    break;
                }

                // Exponential backoff: 5s, 10s, 20s, 40s, 80s
                const backoffMs = Math.min(5000 * Math.pow(2, consecutiveCrashes - 1), 80000);
                this.log(`🔄 Restarting pollLoop in ${backoffMs / 1000}s...`, 'warn');
                await new Promise(r => setTimeout(r, backoffMs));
            }
        }

        this.isRunning = false;
        this.stopHeartbeat();
        this.log('🛑 JobProcessor fully stopped.', 'phase');
    }

    /**
   * Gracefully requests the background loop to stop after the current chunk.
   */
    public static stop() {
        this.log('🛑 Stop signal received. JobProcessor will halt after current chunk.', 'warn');
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
                this.log(`♻️ Recovered ${data.length} stale job items from previous crash.`, 'info');
            }

            // Auto-start if there are any jobs still incomplete
            const { count } = await supabase
                .from('jobs')
                .select('*', { count: 'exact', head: true })
                .in('status', ['pending', 'processing']);

            if (count && count > 0) {
                this.log(`▶️ Active jobs found (${count}). Auto-starting processor...`, 'info');
                this.start();
            }
        } catch (e: any) {
            this.log('⚠️ Recovery failed: ' + e.message, 'error');
        }
    }

    /**
     * Races an awaitable against a hard deadline. If the deadline fires first,
     * we throw — the watchdog catches the throw, logs it, and restarts the
     * loop. Without this, a hung Supabase socket (no TCP reset) stalls the
     * whole pipeline indefinitely with no signal in the logs.
     */
    private static withTimeout<T>(label: string, p: Promise<T>, ms: number): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            const timer = setTimeout(() => {
                reject(new Error(`Timeout after ${ms}ms: ${label}`));
            }, ms);
            p.then(v => { clearTimeout(timer); resolve(v); },
                   e => { clearTimeout(timer); reject(e); });
        });
    }

    private static async pollLoop() {
        while (!this.shouldStop) {
            try {
                // Issue #7: Auto-reset items stuck in 'processing' for too long
                await this.resetStaleProcessingItems();

                const processedCount = await this.withTimeout(
                    'processNextChunk',
                    this.processNextChunk(),
                    CHUNK_TIMEOUT_MS
                );
                this.loopTick++;
                this.lastTickAt = Date.now();

                // If we processed items, the queue is active, so poll immediately again without waiting.
                // If 0 items were found, check if the queue is truly empty.
                if (processedCount === 0) {
                    // Check if the queue is genuinely drained (no pending/retrying/processing items)
                    const { count } = await supabase
                        .from('job_items')
                        .select('*', { count: 'exact', head: true })
                        .in('status', ['pending', 'retrying', 'processing']);

                    if (!count || count === 0) {
                        // Queue fully drained — mark active jobs as completed and auto-stop
                        await supabase.from('jobs')
                            .update({ status: 'completed', finished_at: new Date().toISOString() })
                            .eq('status', 'processing');

                        await supabase.from('pipeline_state').update({
                            is_processing: false,
                            updated_at: new Date().toISOString()
                        }).eq('id', 1);

                        this.log('✅ All items processed. Queue empty — auto-stopping.', 'phase');
                        break;
                    }

                    await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
                } else {
                    // Micro-sleep to allow event loop to breathe
                    await new Promise(r => setImmediate(r));
                }

                // Flush accumulated logs before GC
                await this.flushLogs();

                // Force garbage collection if exposed, keeps Node.js heap extremely clean on long 100k runs
                if (typeof global.gc === 'function') {
                    global.gc();
                }
            } catch (err: any) {
                this.log('❌ JobProcessor Loop Error: ' + err.message, 'error');
                await new Promise(r => setTimeout(r, 5000)); // Backoff on unhandled error
            }
        }
    }

    /**
     * Issue #7: Reset items stuck in 'processing' state for more than STALE_PROCESSING_MINUTES.
     * This handles cases where a chunk failed mid-processing and left items locked.
     */
    private static async resetStaleProcessingItems() {
        try {
            const cutoff = new Date(Date.now() - STALE_PROCESSING_MINUTES * 60 * 1000).toISOString();
            const { data, error } = await supabase
                .from('job_items')
                .update({ status: 'pending', locked_at: null })
                .eq('status', 'processing')
                .lt('locked_at', cutoff)
                .select('id');

            if (!error && data && data.length > 0) {
                this.log(`♻️ Auto-recovered ${data.length} items stuck in processing for >${STALE_PROCESSING_MINUTES}min.`, 'warn');
            }
        } catch (e: any) {
            // Non-fatal — just log and continue
            console.warn('[JobProcessor] Stale item cleanup failed:', e.message);
        }
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
            if (fetchError) this.log('Fetch Claim Error: ' + fetchError.message, 'error');
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
            .select('*');

        if (lockError || !claimedItems || claimedItems.length === 0) {
            this.log('Lock Error: ' + (lockError?.message || 'Lost race'), 'warn');
            return 0; // Lost the race or error
        }

        // 1.5 Manually fetch contacts since we dropped the Foreign Key to fix the UUID mismatch
        const contactIds = [...new Set(claimedItems.map(item => item.contact_id))];
        // Fix #3: Only fetch the columns we actually use — reduces memory by ~60%
        const { data: contactData, error: contactErr } = await supabase
            .from('contacts')
            .select('contact_id, id, email, company_website, lead_list_name')
            .in('contact_id', contactIds);

        if (contactErr) {
            this.log('Contact Join Error: ' + contactErr.message, 'error');
            // Revert claims
            await supabase.from('job_items').update({ status: 'pending', locked_at: null }).in('id', ids);
            return 0;
        }

        // Create dictionary for fast mapping
        const contactMap: Record<string, any> = {};
        if (contactData) {
            contactData.forEach(c => contactMap[c.contact_id] = c);
        }

        // Attach contact data manually
        const enrichedItems = claimedItems.map(item => ({
            ...item,
            contacts: contactMap[item.contact_id] || { contact_id: item.contact_id, email: '', id: 0 }
        }));

        this.log(`📦 Claimed chunk of ${enrichedItems.length} items from ${activeJobIds.length} job(s).`, 'phase');

        // --- CHUNK LOCAL CACHE ---
        // Issue #5: Use contact.company_website, not jobItem.company_website (which doesn't exist on job_items table)
        const uniqueDomains = [...new Set(enrichedItems.map((item: any) => item.contacts?.company_website).filter(Boolean))];
        let domainCache: Record<string, any> = {};
        let digestCache: Record<string, string> = {};
        // Fix #5: Batch scraped_data upserts instead of fire-and-forget per scrape
        const pendingDigestUpserts: { domain: string; content: string; proxy_used: string }[] = [];

        // Issue #1: Wrap cache-prefetch in try/catch — a failed cache query should NOT kill the chunk
        if (uniqueDomains.length > 0) {
            try {
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
            } catch (cacheErr: any) {
                // Non-fatal: cache miss just means we scrape/classify fresh — no big deal
                this.log(`⚠️ Enrichment cache prefetch failed (non-fatal): ${cacheErr.message}`, 'warn');
            }

            try {
                // Fetch prior scraped digests
                const { data: cachedDigests } = await supabase
                    .from('scraped_data')
                    .select('domain, content')
                    .in('domain', uniqueDomains);

                if (cachedDigests) {
                    cachedDigests.forEach((d: any) => digestCache[d.domain] = d.content);
                }
            } catch (digestErr: any) {
                // Non-fatal: we'll just re-scrape
                this.log(`⚠️ Digest cache prefetch failed (non-fatal): ${digestErr.message}`, 'warn');
            }
        }

        // --- EXECUTION PIPELINE ---
        const results = await Promise.all(enrichedItems.map(async (jobItem: any) => {
            const contact = jobItem.contacts;
            // Issue #5: Use contact.company_website, not jobItem.company_website
            const domain = contact.company_website || (contact.email ? contact.email.split('@')[1] : null);

            if (!domain) {
                return this.createResult(jobItem, false, 'failed', 'No domain or email to extract domain from.', undefined, undefined, 0, 'error:no_domain');
            }

            // Issue #9: Skip personal email domains — scraping gmail.com is pointless
            if (PERSONAL_EMAIL_DOMAINS.has(domain.toLowerCase())) {
                return this.createResult(jobItem, false, 'failed', `Skipped: personal email domain (${domain})`, undefined, undefined, 0, 'error:personal_email');
            }

            // A. Check Domain Cache (High Confidence) — "Domain Intelligence" reuse
            if (domainCache[domain]) {
                return this.createResult(jobItem, true, 'completed', '⚡ Reused high-confidence classification', domainCache[domain], undefined, 0, 'domain_intelligence');
            }

            // B. Check Digest Cache OR Scrape Live
            let digest = digestCache[domain];
            let proxyUsed = 'Cache';

            if (digest) {
                proxyStats.recordCacheHit();
            }

            if (!digest) {
                try {
                    // Bounded Scraping
                    const scrapeRes = await scrapeLimit(() => fetchDigest(domain, undefined));
                    digest = scrapeRes.digest;
                    proxyUsed = scrapeRes.proxyName;

                    // Fix #5: Queue digest for batched upsert instead of fire-and-forget
                    pendingDigestUpserts.push({ domain, content: digest, proxy_used: proxyUsed });
                    // Update local chunk cache so duplicate domains in the SAME chunk don't double scrape!
                    digestCache[domain] = digest;
                } catch (e: any) {
                    // Scrape failed. Determine if retryable.
                    const isTerminal = e.message.includes('FastFail');

                    if (!isTerminal && jobItem.attempt_count < MAX_RETRIES) {
                        return this.createRetry(jobItem, `Scraper error: ${e.message}`);
                    } else {
                        return this.createResult(jobItem, false, 'failed', `Scrape terminal error: ${e.message}`, undefined, undefined, 0, 'error:scrape');
                    }
                }
            }

            // C. Classify with AI
            try {
                // Bounded AI
                const aiOutput = await aiLimit(() => enrichSingle({
                    contact_id: contact.contact_id,
                    email: contact.email,
                    digest: digest
                }));

                if (aiOutput.status === 'completed' && aiOutput.classification !== 'ERROR') { // Success
                    // Cache it locally so subsequent duplicates in SAME chunk can skip AI
                    domainCache[domain] = aiOutput;
                    // proxyUsed is 'Cache' when digest came from digestCache; otherwise it's the winning proxy name.
                    const source = proxyUsed === 'Cache' ? 'digest_cache' : proxyUsed;
                    return this.createResult(jobItem, true, 'completed', aiOutput.reasoning, aiOutput, digest, aiOutput.cost, source);
                } else {
                    // Treat OpenAI 5xx / timeout as transient — don't burn a retry slot as fast.
                    const isTransient = aiOutput.error_category === 'openai_5xx' || aiOutput.error_category === 'openai_timeout';
                    const cap = isTransient ? MAX_RETRIES_TRANSIENT : MAX_RETRIES;
                    if (jobItem.attempt_count < cap) {
                        return this.createRetry(jobItem, `AI error: ${aiOutput.reasoning}`, { transient: isTransient });
                    }
                    return this.createResult(jobItem, false, 'failed', `AI terminal error: ${aiOutput.reasoning}`, undefined, undefined, 0, 'error:ai');
                }
            } catch (aiErr: any) {
                // Uncaught throw from enrichSingle — treat as transient unless we can prove otherwise.
                const isTransient = /timeout|aborted|ECONNRESET|ENOTFOUND|EAI_AGAIN|fetch failed/i.test(aiErr?.message || '');
                const cap = isTransient ? MAX_RETRIES_TRANSIENT : MAX_RETRIES;
                if (jobItem.attempt_count < cap) {
                    return this.createRetry(jobItem, `AI exception: ${aiErr.message}`, { transient: isTransient });
                }
                return this.createResult(jobItem, false, 'failed', `AI terminal exception: ${aiErr.message}`, undefined, undefined, 0, 'error:ai');
            }
        }));

        // --- BULK DATABASE UPDATE ---
        let jobsToUpdate: Record<string, { completed: number, failed: number }> = {};
        let itemsToUpsert: any[] = [];
        let enrichmentsToUpsert: any[] = [];
        let contactsToUpdate: any[] = [];

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

        // Issue #4: All bulk writes now have error handling
        // Dedupe-by-conflict-key helper. A chunk can carry two entries that
        // share the same conflict target (e.g. two job_items pointing at the
        // same contact_id, or two enrichments for the same contact) — Postgres
        // rejects the whole batch with "ON CONFLICT DO UPDATE command cannot
        // affect row a second time" in that case. Map keeps the last entry per
        // key, which is the latest state we want to persist.
        const dedupeBy = <T>(rows: T[], key: keyof T): T[] =>
            Array.from(new Map(rows.map(r => [r[key] as unknown as string, r])).values());

        // 1. Update Job Items (most critical — tracks item state)
        if (itemsToUpsert.length > 0) {
            const deduped = dedupeBy(itemsToUpsert as any[], 'id');
            const { error: itemErr } = await supabase.from('job_items').upsert(deduped, { onConflict: 'id' });
            if (itemErr) this.log(`⚠️ job_items upsert error: ${itemErr.message}`, 'error');
        }

        // 2. Update Contacts
        if (contactsToUpdate.length > 0) {
            const deduped = dedupeBy(contactsToUpdate as any[], 'id');
            const { error: contactErr } = await supabase.from('contacts').upsert(deduped, { onConflict: 'id' });
            if (contactErr) this.log(`⚠️ contacts upsert error: ${contactErr.message}`, 'error');
        }

        // 3. Update Enrichments
        if (enrichmentsToUpsert.length > 0) {
            const deduped = dedupeBy(enrichmentsToUpsert as any[], 'contact_id');
            const { error: enrichErr } = await supabase.from('enrichments').upsert(deduped, { onConflict: 'contact_id' });
            if (enrichErr) this.log(`⚠️ enrichments upsert error: ${enrichErr.message}`, 'error');
        }

        // Fix #5: Flush batched scraped_data upserts.
        // Dedupe by domain first — two contacts in the same chunk can share a
        // company_website, and Postgres rejects the whole batch with
        // "ON CONFLICT DO UPDATE command cannot affect row a second time"
        // if the same conflict key appears twice.
        if (pendingDigestUpserts.length > 0) {
            const dedupedByDomain = Array.from(
                new Map(pendingDigestUpserts.map(u => [u.domain, u])).values()
            );
            const { error: digestErr } = await supabase.from('scraped_data').upsert(dedupedByDomain, { onConflict: 'domain' });
            if (digestErr) this.log(`⚠️ scraped_data batch upsert error: ${digestErr.message}`, 'error');
        }

        // Issue #2: Atomic job counter increments — avoids read-then-write race condition
        // Uses a single UPDATE with raw SQL increment instead of SELECT + UPDATE
        for (const [jid, tallies] of Object.entries(jobsToUpdate)) {
            if (tallies.completed > 0 || tallies.failed > 0) {
                // Use RPC for atomic increment, falling back to direct update if RPC doesn't exist
                try {
                    const { error: rpcError } = await supabase.rpc('increment_job_counters', {
                        job_id_input: jid,
                        completed_increment: tallies.completed,
                        failed_increment: tallies.failed
                    });

                    if (rpcError) {
                        // Fallback to read-then-write if RPC not available yet
                        const { data: currentJob } = await supabase.from('jobs').select('completed_items, failed_items').eq('id', jid).single();
                        if (currentJob) {
                            await supabase.from('jobs').update({
                                completed_items: (currentJob.completed_items || 0) + tallies.completed,
                                failed_items: (currentJob.failed_items || 0) + tallies.failed
                            }).eq('id', jid);
                        }
                    }
                } catch (counterErr: any) {
                    this.log(`⚠️ Job counter update failed for job ${jid}: ${counterErr.message}`, 'error');
                }
            }
        }

        // Update old pipeline state for backend UI compatibility
        await supabase.from('pipeline_state').update({
            is_processing: true,
            updated_at: new Date().toISOString()
        }).eq('id', 1);

        // One-line summary of which proxies won this chunk (and how many fell back to cache / all failed).
        this.log(`📊 [Scraper Stats] chunk: ${proxyStats.flushSummary()}`, 'info');

        // Flush logs for this chunk
        await this.flushLogs();

        // 🗑️ AGGRESSIVE MEMORY CLEANUP 🗑️
        // Fix #4: Use let bindings + null reassignment to fully release references
        const returnedLength = claimedItems.length;

        claimedItems.length = 0;
        enrichedItems.length = 0;
        results.length = 0;
        itemsToUpsert.length = 0;
        enrichmentsToUpsert.length = 0;
        contactsToUpdate.length = 0;
        pendingDigestUpserts.length = 0;
        // Null out object references so V8 can collect the backing stores
        domainCache = null as any;
        digestCache = null as any;
        jobsToUpdate = null as any;
        itemsToUpsert = null as any;
        enrichmentsToUpsert = null as any;
        contactsToUpdate = null as any;

        return returnedLength;
    }

    // Helpers
    //
    // `source` documents how this enrichment was obtained. See README
    // ("Enrichment `source` values") for the full taxonomy. The dashboard
    // aggregates on this column, so every result path must set it.
    private static createResult(jobItem: any, isSuccess: boolean, status: 'completed' | 'failed', errorOrReasoning: string, classificationData?: any, digest?: string, cost: number = 0, source?: string) {
        const contact = jobItem.contacts;

        const enr = {
            contact_id: contact.contact_id, // The UUID for enrichments table
            status: status,
            confidence: classificationData?.confidence || 1,
            reasoning: errorOrReasoning,
            classification: classificationData?.classification || (isSuccess ? "Unknown" : "Scrape Error"),
            // Fix #2: Removed page_html — already persisted to scraped_data table, storing again wastes ~6KB/item
            cost: cost,
            source: source,
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
                job_id: jobItem.job_id,
                contact_id: jobItem.contact_id,
                status: status,
                attempt_count: jobItem.attempt_count || 0,
                error_message: isSuccess ? null : errorOrReasoning,
                finished_at: new Date().toISOString(),
            },
            enrichmentUpdate: enr,
            contactUpdate: cnt
        };
    }

    private static createRetry(jobItem: any, errorMsg: string, opts?: { transient?: boolean }) {
        // Exponential backoff with jitter. For transient errors (OpenAI 5xx /
        // timeout) we start at a higher floor (30s) and cap at 5min so a short
        // outage doesn't produce a thundering retry storm. For normal errors
        // keep the previous aggressive schedule (2s, 4s, 8s…) but still cap.
        const transient = opts?.transient ?? false;
        const base = Math.pow(2, jobItem.attempt_count) * 2000;
        const floor = transient ? 30_000 : 0;
        const ceil = transient ? 300_000 : 60_000;
        const jitter = Math.floor(Math.random() * 1000);
        const nextRetryMs = Math.min(ceil, Math.max(floor, base)) + jitter;

        return {
            jobId: jobItem.job_id,
            newStatus: 'retrying',
            jobItemUpdate: {
                id: jobItem.id,
                job_id: jobItem.job_id,
                contact_id: jobItem.contact_id,
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
