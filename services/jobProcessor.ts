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

// Postgres text columns reject null bytes and lone UTF-16 surrogates with
// "unsupported Unicode escape sequence", which kills the whole batch upsert.
// Scraped HTML occasionally contains these bytes from binary-ish content,
// broken encodings, or embedded PDF blobs — strip them before persisting.
function sanitizeForPostgres(s: string): string {
    if (!s) return s;
    return s
        // Null bytes — Postgres can never store these in text/jsonb.
        .replace(/\u0000/g, '')
        // Lone high surrogate (no matching low surrogate following).
        .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/g, '')
        // Lone low surrogate (no matching high surrogate preceding).
        .replace(/(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '');
}

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
    // Postgres 57014 (statement_timeout) on the chunk-claim query keeps
    // hitting the same code path. Without this counter the loop's catch
    // would just sleep 5s and re-fire the same failing query forever, with
    // no escalation and no log signal beyond a flat repeating error line.
    // Once we hit the threshold we throw to the watchdog so its exponential
    // backoff (5/10/20/40/80s) kicks in instead.
    private static consecutiveStmtTimeouts = 0;

    // Last time we ran reap_orphan_job_items inside the poll loop. The
    // reaper is also called once on startup (recoverStaleJobs); this
    // periodic call is the defence-in-depth backstop against any path
    // that completes a job without going through reconcile_and_complete_job.
    // 60s grace inside the RPC means a fresh completion isn't racy.
    private static lastReaperAt = 0;
    private static readonly REAPER_INTERVAL_MS = 60_000;

    // Per-list progress tracker: keyed by lead_list_name. `baseline` is the
    // completed+failed count at the moment we first saw this list in the
    // current run (from the RPC), so the log's "1000/100000" reflects the
    // list's absolute state, not just the run's delta. `lastMilestone` is
    // the last 1000-multiple we already logged — prevents duplicate lines
    // when multiple chunks close within the same milestone bucket.
    private static listProgress: Map<string, {
        total: number;
        baselineProcessed: number;
        processedInRun: number;
        lastMilestone: number;
    }> = new Map();

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

    // Render → Supabase has intermittent transport blips — undici throws
    // "TypeError: fetch failed", or supabase-js bubbles back ECONNRESET /
    // ETIMEDOUT / EAI_AGAIN. The chunk-write upserts used to log + drop on
    // these, leaving items in `processing` until the 5-min watchdog reset.
    // This classifier matches the same set the import path retries on
    // (server.ts:isTransientImportError) so behaviour is consistent.
    private static isTransientSupabaseError(err: any): boolean {
        if (!err) return false;
        if (err.code === '57014' || err.code === '40001' || err.code === '40P01') return true;
        const msg = String(err?.message || err || '').toLowerCase();
        return /fetch failed|econnreset|etimedout|eai_again|enotfound|socket hang up|network|aborted|connection reset/.test(msg);
    }

    // Wrap a Supabase op with transport-level retry. Mirrors
    // importSupabaseRetry in server.ts. Catches BOTH thrown TypeErrors
    // (undici can throw directly out of await) AND tuple-shape errors
    // (the supabase-js usual path). Capped exponential backoff so a stuck
    // pooler doesn't park the worker indefinitely.
    private static async retrySupabaseOp<T>(
        label: string,
        fn: () => PromiseLike<{ data: T; error: any }>,
        maxRetries = 5
    ): Promise<{ data: T | null; error: any }> {
        let attempt = 0;
        while (true) {
            let result: { data: T | null; error: any };
            try {
                result = await fn();
            } catch (thrown: any) {
                result = { data: null, error: thrown };
            }
            if (!result.error || !this.isTransientSupabaseError(result.error) || attempt >= maxRetries) {
                return result;
            }
            attempt++;
            const backoff = Math.min(8000, 500 * Math.pow(2, attempt));
            this.log(`↻ ${label} transient (${result.error?.message || 'unknown'}); retry ${attempt}/${maxRetries} in ${backoff}ms`, 'warn');
            await new Promise(r => setTimeout(r, backoff));
        }
    }

    // First time we see a list in this run, force a targeted recompute of
    // its stats-cache row and use the returned counts as the baseline —
    // exact numbers AND a warm cache right as the list starts running.
    // The gated wrapper returns nothing when the snapshot was already
    // refreshed in the last 5s (or another refresh holds the lock); the
    // cache reader is current enough for a log baseline in that case.
    // Failing both falls through to a zero baseline + contact_count from
    // import_lists — the log is still informative, just less precise.
    private static async ensureListBaseline(listName: string): Promise<void> {
        if (this.listProgress.has(listName)) return;
        let total = 0;
        let baseline = 0;
        try {
            let row: any | undefined;
            const { data } = await supabase.rpc('request_list_stats_refresh', { p_lists: [listName] });
            if (Array.isArray(data)) row = (data as any[]).find(r => r.lead_list_name === listName);
            if (!row) {
                const { data: cached } = await supabase.rpc('get_list_enrichment_stats');
                if (Array.isArray(cached)) row = (cached as any[]).find(r => r.lead_list_name === listName);
            }
            if (row) {
                total = Number(row.total_count) || 0;
                baseline = (Number(row.completed_count) || 0) + (Number(row.failed_count) || 0);
            }
        } catch { /* fall through to the import_lists fallback */ }
        if (total === 0) {
            const { data: il } = await supabase.from('import_lists').select('contact_count').eq('name', listName).maybeSingle();
            total = il?.contact_count || 0;
        }
        // Starting milestone = floor(baseline / 1000) so the first log line
        // of the run reports the next clean 1000-crossing rather than an
        // arbitrary mid-milestone number.
        const lastMilestone = Math.floor(baseline / 1000) * 1000;
        this.listProgress.set(listName, { total, baselineProcessed: baseline, processedInRun: 0, lastMilestone });
        const pct = total > 0 ? Math.round((baseline / total) * 100) : 0;
        this.log(`🏁 Enriching list: "${listName}" — starting at ${baseline.toLocaleString()}/${total.toLocaleString()} (${pct}%)`, 'phase');
    }

    // Call after each chunk with the number of completed+failed items per
    // list from that chunk. Emits one log per list that crossed a new
    // 1000-processed milestone since the last call.
    private static logListProgress(perListDelta: Record<string, number>) {
        for (const [listName, delta] of Object.entries(perListDelta)) {
            const p = this.listProgress.get(listName);
            if (!p || delta <= 0) continue;
            p.processedInRun += delta;
            const liveProcessed = p.baselineProcessed + p.processedInRun;
            const nextMilestone = Math.floor(liveProcessed / 1000) * 1000;
            if (nextMilestone > p.lastMilestone) {
                p.lastMilestone = nextMilestone;
                const pct = p.total > 0 ? Math.min(100, Math.round((liveProcessed / p.total) * 100)) : 0;
                this.log(`📈 Progress: "${listName}": ${pct}% (${liveProcessed.toLocaleString()}/${p.total.toLocaleString()})`, 'info');
            }
        }
    }

    /**
     * Starts the infinite background polling loop.
     */
    public static async start() {
        if (this.isRunning) return;
        this.isRunning = true;
        this.shouldStop = false;
        // Fresh run → fresh per-list tallies. Baseline is queried lazily
        // the first time we see items from each list.
        this.listProgress.clear();
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
            // Recovery step 1: any items left in 'processing' belonging to
            // a STILL-LIVE parent (pending/processing) are the result of a
            // crash mid-claim — flip them back to 'pending' so the worker
            // re-claims after start(). Items whose parent is already
            // terminal are NOT resurrected here (they'd just zombie); the
            // orphan reaper below handles those.
            const { data: recovered, error: recoverErr } = await supabase
                .from('job_items')
                .update({ status: 'pending', locked_at: null })
                .eq('status', 'processing')
                .in('job_id',
                    (await supabase.from('jobs').select('id').in('status', ['pending', 'processing'])).data?.map(j => j.id) || []
                )
                .select('id');

            if (!recoverErr && recovered && recovered.length > 0) {
                this.log(`♻️ Recovered ${recovered.length} stale job items from previous crash.`, 'info');
            }

            // Recovery step 2: retire any leftover open items whose parent
            // job is already terminal (the FL Oct 14 shape — pre-fix
            // residue). Grace 0 because nothing legitimate should be in
            // this state at boot; the periodic reaper inside pollLoop uses
            // 60s to avoid racing fresh job completions.
            const { data: reapedCount, error: reapErr } = await supabase
                .rpc('reap_orphan_job_items', { p_grace_seconds: 0 });
            if (!reapErr && (reapedCount || 0) > 0) {
                this.log(`🧹 Reaped ${reapedCount} orphan items (parent job already terminal).`, 'warn');
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

                // Periodic orphan reaper. Cheap when there's nothing to do
                // (the RPC is one indexed UPDATE that matches zero rows).
                // Catches anything that bypasses reconcile_and_complete_job
                // — e.g. a direct DB UPDATE, a future code path that flips
                // jobs.status without the RPC, or a race we didn't foresee.
                const now = Date.now();
                if (now - this.lastReaperAt >= this.REAPER_INTERVAL_MS) {
                    this.lastReaperAt = now;
                    try {
                        const { data: reaped, error: reapErr } = await supabase
                            .rpc('reap_orphan_job_items', { p_grace_seconds: 60 });
                        if (reapErr) {
                            this.log(`⚠️ Periodic reaper failed: ${reapErr.message}`, 'warn');
                        } else if ((reaped || 0) > 0) {
                            this.log(`🧹 Periodic reaper retired ${reaped} orphan items.`, 'warn');
                        }
                    } catch (e: any) {
                        this.log(`⚠️ Periodic reaper threw: ${e?.message || e}`, 'warn');
                    }
                }

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
                    // Check if the queue is genuinely drained. Critically,
                    // distinguish "count query errored" from "count is 0" —
                    // the previous `if (!count || count === 0)` treated both
                    // as empty, which could auto-stop the processor the
                    // moment a sanity check timed out and leave thousands
                    // of pending items orphaned.
                    //
                    // We also scope the count to job_items whose PARENT job
                    // is still live (pending/processing). Without this join
                    // an orphan row (job_items.status='retrying' under a
                    // jobs.status='completed' parent) would keep the worker
                    // spinning forever even though no live job can ever
                    // pick it up. The orphan reaper in commit 2 prevents
                    // those rows from existing in the first place; this
                    // join is defence-in-depth.
                    const { count, error: countErr } = await supabase
                        .from('job_items')
                        .select('jobs!inner(status)', { count: 'exact', head: true })
                        .in('status', ['pending', 'retrying', 'processing'])
                        .in('jobs.status', ['pending', 'processing']);

                    if (countErr) {
                        this.log(`⚠️ Queue drain check failed (${countErr.message}); keep polling instead of auto-stopping.`, 'warn');
                        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
                    } else if (count === 0) {
                        // Confirmed zero pending/retrying/processing under
                        // any live parent — safe to auto-stop. Close each
                        // still-'processing' job atomically via the RPC so
                        // any straggler open item gets retired in the same
                        // transaction (defence-in-depth: openCount==0 above
                        // should mean none exist, but a race against a
                        // crashed claimChunkFromJob could leave one).
                        const { data: activeJobs } = await supabase
                            .from('jobs').select('id').eq('status', 'processing');
                        for (const j of (activeJobs || []) as Array<{ id: string }>) {
                            const { error: rpcErr } = await supabase.rpc(
                                'reconcile_and_complete_job', { p_job_id: j.id });
                            if (rpcErr) this.log(`⚠️ Auto-stop: reconcile RPC failed for ${j.id}: ${rpcErr.message}`, 'error');
                        }

                        await supabase.from('pipeline_state').update({
                            is_processing: false,
                            updated_at: new Date().toISOString()
                        }).eq('id', 1);

                        this.log('✅ All items processed. Queue empty — auto-stopping.', 'phase');
                        break;
                    } else {
                        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
                    }
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

                // Tick completed without throwing — reset the timeout counter.
                this.consecutiveStmtTimeouts = 0;
            } catch (err: any) {
                const isStmtTimeout = err?.code === '57014'
                    || /statement timeout|canceling statement/i.test(err?.message || '');

                if (isStmtTimeout) {
                    this.consecutiveStmtTimeouts++;
                    const consec = this.consecutiveStmtTimeouts;
                    this.log(`❌ JobProcessor Loop Error: ${err.message} (code 57014, consecutive=${consec})`, 'error');

                    if (consec >= 5) {
                        this.consecutiveStmtTimeouts = 0;
                        throw new Error(`Statement timeout repeated ${consec}× — escalating to watchdog for backoff.`);
                    }
                } else {
                    this.consecutiveStmtTimeouts = 0;
                    this.log('❌ JobProcessor Loop Error: ' + err.message, 'error');
                }

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
        // FIFO across jobs (real serial multi-list queueing):
        //   - Walk 'processing' jobs oldest-first.
        //   - For each, claim a chunk filtered to that job_id only.
        //   - If a job returns 0 items, its queue is drained — mark it
        //     'completed' and try the next-oldest in the SAME tick so the
        //     processor doesn't sleep on a dead job.
        //
        // History: ordering job_items by id ASC was wrong (uuid, not bigserial),
        // and a previous fix that just filtered to oldest 'processing' job
        // got stuck once that job drained because its status never flipped to
        // 'completed' (the global auto-complete only fires when the WHOLE queue
        // is empty). This loop handles per-job completion inline.
        const { data: candidates, error: candErr } = await supabase
            .from('jobs')
            .select('id')
            .eq('status', 'processing')
            .order('created_at', { ascending: true })
            .limit(20);

        if (candErr) throw candErr;

        if (candidates && candidates.length > 0) {
            for (const cand of candidates) {
                const claimed = await this.claimChunkFromJob(cand.id);
                if (claimed > 0) return claimed;

                // Zero items CLAIMABLE doesn't mean the job is drained — it
                // can also mean every remaining item is in 'retrying' with
                // next_retry_at still in the future. Earlier code marked
                // those jobs completed, which silently orphaned the retrying
                // items (FIFO only scans 'processing' jobs, so once a job
                // flips to 'completed' its retrying items are never picked
                // up again — even after next_retry_at passes). Re-checking
                // the job's open queue WITHOUT the next_retry_at filter
                // distinguishes the two cases: truly empty → mark drained
                // and advance; still has retrying items → keep job in
                // 'processing' and return 0 so the outer pollLoop sleeps
                // POLL_INTERVAL_MS and re-checks when the backoff expires.
                const { count: openCount, error: openErr } = await supabase
                    .from('job_items')
                    .select('id', { count: 'exact', head: true })
                    .eq('job_id', cand.id)
                    .in('status', ['pending', 'retrying', 'processing']);
                if (openErr) {
                    // Soft-fail to "drained" rather than throwing — a count
                    // error here would block FIFO for everyone. Erring
                    // toward "drained" matches the old behaviour for the
                    // happy path; the only regression risk is the bug this
                    // patch fixes, and that requires repeated count errors.
                    this.log(`⚠️ Drain check failed for job ${cand.id} (${openErr.message}); falling back to old drained-marking behaviour.`, 'warn');
                } else if ((openCount || 0) > 0) {
                    this.log(`⏸ Job ${cand.id} has ${openCount} item(s) backing off (retrying / pending) — keeping in 'processing', will recheck after next_retry_at.`);
                    return 0;
                }

                // Genuinely zero items remaining — flip the job atomically.
                // The RPC retires any straggler open items in the same
                // transaction (should be zero given the openCount probe
                // above, but the race window is non-zero and the RPC is
                // load-bearing for the no-orphans invariant).
                const { data: rpcResult, error: rpcErr } = await supabase
                    .rpc('reconcile_and_complete_job', { p_job_id: cand.id });
                if (rpcErr) {
                    this.log(`⚠️ reconcile_and_complete_job failed for ${cand.id}: ${rpcErr.message}`, 'error');
                } else {
                    const reaped = (rpcResult as Array<{ reaped: number }> | null)?.[0]?.reaped || 0;
                    if (reaped > 0) {
                        this.log(`⚠️ Job ${cand.id} drained with ${reaped} open items reaped — marking completed; advancing FIFO.`, 'warn');
                    } else {
                        this.log(`✅ Job ${cand.id} drained — marking completed; advancing FIFO.`, 'phase');
                    }
                    // Snap the stats cache to final counts for every list
                    // this run touched, so the import-history row flips to
                    // DONE immediately — even if no browser tab is open to
                    // trigger the server's stale-detection path. Fire and
                    // forget; the poll-driven refresh is the backstop.
                    const runLists = [...this.listProgress.keys()];
                    if (runLists.length > 0) {
                        // Gated wrapper, not the raw refresh RPC — works
                        // even when this process fell back to the anon key
                        // (no SUPABASE_SERVICE_ROLE_KEY in the deploy env).
                        Promise.resolve(supabase.rpc('request_list_stats_refresh', { p_lists: runLists }))
                            .then(({ error }: { error: any }) => {
                                if (error) this.log(`⚠️ Post-drain stats refresh failed: ${error.message}`, 'warn');
                            })
                            .catch(() => { /* poll-driven refresh covers it */ });
                    }
                }
            }
        }

        // Fallback: no live 'processing' job had items. Look for jobs in
        // any non-processing terminal-or-pre-live state that STILL have
        // unfinished rows — orphans from a previously-cancelled job, a
        // job that was created with status='pending' and never promoted,
        // or (defensively) a 'completed' job whose worker missed reaping
        // some retrying items. The list_jobs_with_open_items RPC uses
        // EXISTS so we don't scan millions of historical jobs.
        const { data: orphanJob, error: orphanErr } = await supabase
            .rpc('list_jobs_with_open_items', { p_limit: 50 });

        if (orphanErr) throw orphanErr;
        if (!orphanJob || orphanJob.length === 0) return 0;

        for (const cand of orphanJob as Array<{ id: string; status: string }>) {
            // Promote pending/cancelled jobs to processing so the FIFO
            // walker picks them up next tick. 'completed' jobs with open
            // items are a different beast — those items are orphans the
            // reaper should retire (commit 2). We do NOT resurrect a
            // 'completed' job here; just claim its items so they progress
            // through retry → terminal-fail and stop blocking the queue
            // count. The reaper will close them out atomically.
            if (cand.status === 'pending' || cand.status === 'cancelled') {
                await supabase.from('jobs')
                    .update({ status: 'processing' })
                    .eq('id', cand.id);
            }
            const claimed = await this.claimChunkFromJob(cand.id);
            if (claimed > 0) return claimed;
        }
        return 0;
    }

    /**
     * Claim a chunk of pending/retrying items belonging to a SPECIFIC job.
     * Extracted so the FIFO + orphan-fallback paths in processNextChunk
     * share the same locking + dispatch code.
     */
    private static async claimChunkFromJob(jobId: string): Promise<number> {
        const { data: itemIdsToClaim, error: fetchError } = await supabase
            .from('job_items')
            .select('id, job_id')
            .eq('job_id', jobId)
            .in('status', ['pending', 'retrying'])
            .or('next_retry_at.is.null,next_retry_at.lte.now()')
            .order('created_at', { ascending: true })
            .limit(QUEUE_FETCH_CHUNK_SIZE);

        if (fetchError) {
            // Transient DB error (57014 etc.) — bubble to the caller's
            // try/catch backoff, don't silently return 0 because returning
            // 0 would let the outer loop misread this as "queue empty"
            // and potentially auto-stop the processor mid-run.
            throw fetchError;
        }
        if (!itemIdsToClaim || itemIdsToClaim.length === 0) {
            return 0;
        }

        const ids = itemIdsToClaim.map(row => row.id);
        // Always 1 now (FIFO claim is per-job) but kept for the log line so
        // we'd notice immediately if someone re-broadened the claim later.
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

        if (lockError) {
            // Same reasoning as fetchError above — this is a DB error, not
            // an empty queue. Must bubble up so auto-stop can't fire.
            throw lockError;
        }
        if (!claimedItems || claimedItems.length === 0) {
            this.log('Lock Error: Lost race — another worker claimed these rows first', 'warn');
            return 0;
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

        // Per-list delta for this chunk — drives the progress log.
        const perListDelta: Record<string, number> = {};
        results.forEach(res => {
            if (!jobsToUpdate[res.jobId]) jobsToUpdate[res.jobId] = { completed: 0, failed: 0 };
            if (res.newStatus === 'completed') jobsToUpdate[res.jobId].completed++;
            if (res.newStatus === 'failed') jobsToUpdate[res.jobId].failed++;

            // Completed + failed both count as "processed" for progress
            // purposes — a failed scrape is still forward motion, not a
            // row we'll retry.
            const listName: string | undefined = (res as any).leadListName;
            if (listName && (res.newStatus === 'completed' || res.newStatus === 'failed')) {
                perListDelta[listName] = (perListDelta[listName] || 0) + 1;
            }

            itemsToUpsert.push(res.jobItemUpdate);

            if (res.enrichmentUpdate) {
                enrichmentsToUpsert.push(res.enrichmentUpdate);
            }
        });

        // Ensure we have a baseline for every list we touched in this
        // chunk (parallel — usually just 1–2 distinct lists), then emit
        // milestone logs.
        await Promise.all(Object.keys(perListDelta).map(l => this.ensureListBaseline(l)));
        this.logListProgress(perListDelta);

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
            const { error: itemErr } = await this.retrySupabaseOp(
                'job_items upsert',
                () => supabase.from('job_items').upsert(deduped, { onConflict: 'id' })
            );
            if (itemErr) this.log(`⚠️ job_items upsert error: ${itemErr.message}`, 'error');
        }

        // 2. Update Enrichments
        if (enrichmentsToUpsert.length > 0) {
            const deduped = dedupeBy(enrichmentsToUpsert as any[], 'contact_id');
            const { error: enrichErr } = await this.retrySupabaseOp(
                'enrichments upsert',
                () => supabase.from('enrichments').upsert(deduped, { onConflict: 'contact_id' })
            );
            if (enrichErr) this.log(`⚠️ enrichments upsert error: ${enrichErr.message}`, 'error');
        }

        // Fix #5: Flush batched scraped_data upserts.
        // Dedupe by domain first — two contacts in the same chunk can share a
        // company_website, and Postgres rejects the whole batch with
        // "ON CONFLICT DO UPDATE command cannot affect row a second time"
        // if the same conflict key appears twice.
        //
        // Also sanitize content: scraped HTML sometimes contains null bytes
        // (\u0000) or lone UTF-16 surrogates from corrupted-encoding pages or
        // embedded binary blobs. Postgres text columns reject those with
        // "unsupported Unicode escape sequence" and the whole batch fails.
        if (pendingDigestUpserts.length > 0) {
            const dedupedByDomain = Array.from(
                new Map(pendingDigestUpserts.map(u => [u.domain, {
                    ...u,
                    content: sanitizeForPostgres(u.content)
                }])).values()
            );
            const { error: digestErr } = await this.retrySupabaseOp(
                'scraped_data batch upsert',
                () => supabase.from('scraped_data').upsert(dedupedByDomain, { onConflict: 'domain' })
            );
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
        pendingDigestUpserts.length = 0;
        // Null out object references so V8 can collect the backing stores
        domainCache = null as any;
        digestCache = null as any;
        jobsToUpdate = null as any;
        itemsToUpsert = null as any;
        enrichmentsToUpsert = null as any;

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
            processed_at: new Date().toISOString(),
            // Denormalized from contacts.lead_list_name so the per-list
            // progress aggregate (get_list_enrichment_stats) can group
            // without joining contacts. See migration
            // 20260516_denormalize_enrichments_lead_list_name.sql.
            lead_list_name: contact.lead_list_name || null,
        };

        // The enrichment flow no longer writes back to contacts: the AI
        // classification lives only on enrichments.classification (read via
        // COALESCE(enrichments.classification, contacts.industry) everywhere it
        // is needed). contacts.industry stays the RAW imported value. The old
        // `cnt` object's only enrichment-derived field was `industry =
        // enr.classification`; the rest (email, lead_list_name) were unchanged,
        // so there is nothing to upsert here anymore.
        return {
            jobId: jobItem.job_id,
            newStatus: status,
            leadListName: contact.lead_list_name || null,
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
            contactUpdate: null
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
