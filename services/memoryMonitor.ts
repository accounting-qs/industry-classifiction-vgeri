import v8 from 'node:v8';
import fs from 'node:fs';
import path from 'node:path';

/**
 * Memory telemetry.
 *
 * The Render instance has been dying with "Instance failed" (Jul 30 ×2, Aug 3,
 * Aug 4, Aug 5) on a dead-flat linear climb — ~10% to ~85% of 4 GB over 13
 * hours, roughly 233 MB/h — then an OOM kill. A linear climb means memory is
 * genuinely *retained*: GC has nothing to collect, so lowering the heap ceiling
 * (commit d5f5644, 3600→3072 MB) could only change when the wall was hit, never
 * whether.
 *
 * Diagnosing it was impossible because the process emitted no memory signal at
 * all — the only evidence was Render's graph and a post-mortem "Instance
 * failed". This module fixes that: it records where memory actually sits, which
 * pool it sits in, and what the app was doing at the time.
 *
 * Deliberately NOT automatic heap snapshots. v8.writeHeapSnapshot() on a 3 GB
 * heap blocks the event loop for many seconds and writes a file that can exceed
 * the heap itself — on a box with ~2 GB of disk that turns one incident into
 * two. Snapshots are available on demand instead (writeHeapSnapshotTo), for when
 * someone is watching. The per-space and external/arrayBuffer breakdown below is
 * usually enough to localise a leak without one:
 *
 *   external / arrayBuffers climbing  → Buffers, undici sockets, streams
 *   used_heap_size climbing           → retained JS objects (closures, caches)
 *   number_of_detached_contexts > 0   → leaked contexts, almost always a bug
 *   malloced_memory climbing          → native allocations
 */

interface Sample {
    t: number;
    rss: number;
    heapUsed: number;
    external: number;
    arrayBuffers: number;
}

const MB = 1024 * 1024;
const toMb = (n: number) => Math.round(n / MB);

// 4 h of history at one sample a minute. Bounded so the leak monitor can
// never itself become the leak.
const MAX_SAMPLES = 240;
const history: Sample[] = [];

let started = false;
let lastWarnBand = 0;
let contextProvider: (() => Record<string, any>) | null = null;

/**
 * Lets the host describe what it was doing, so a memory line is correlatable
 * with activity rather than being a bare number. Called on every sample, so it
 * must be cheap and must not throw.
 */
export function setMemoryContextProvider(fn: () => Record<string, any>) {
    contextProvider = fn;
}

export function memorySnapshot() {
    const m = process.memoryUsage();
    const h = v8.getHeapStatistics();
    const trend = growthPerHour();
    return {
        rss_mb: toMb(m.rss),
        heap_used_mb: toMb(m.heapUsed),
        heap_total_mb: toMb(m.heapTotal),
        external_mb: toMb(m.external),
        array_buffers_mb: toMb(m.arrayBuffers),
        heap_limit_mb: toMb(h.heap_size_limit),
        malloced_mb: toMb(h.malloced_memory),
        peak_malloced_mb: toMb(h.peak_malloced_memory),
        // Non-zero here is a near-certain leak: a context that was discarded but
        // is still reachable, so everything it closed over is pinned too.
        detached_contexts: h.number_of_detached_contexts,
        native_contexts: h.number_of_native_contexts,
        rss_growth_mb_per_hour: trend.rssPerHour,
        heap_growth_mb_per_hour: trend.heapPerHour,
        samples: history.length,
        spaces: v8.getHeapSpaceStatistics().map(s => ({
            name: s.space_name,
            used_mb: toMb(s.space_used_size),
            size_mb: toMb(s.space_size),
        })).filter(s => s.size_mb > 0),
        context: safeContext(),
    };
}

function safeContext(): Record<string, any> {
    if (!contextProvider) return {};
    try { return contextProvider() || {}; } catch { return { context_error: true }; }
}

/**
 * Growth rate across the retained window. Uses first-vs-last rather than a
 * regression on purpose: the failure signature here is a straight line, and a
 * simple slope is easier to reason about in a log line than an r².
 */
function growthPerHour(): { rssPerHour: number; heapPerHour: number } {
    if (history.length < 2) return { rssPerHour: 0, heapPerHour: 0 };
    const first = history[0];
    const last = history[history.length - 1];
    const hours = (last.t - first.t) / 3_600_000;
    if (hours <= 0) return { rssPerHour: 0, heapPerHour: 0 };
    return {
        rssPerHour: Math.round((last.rss - first.rss) / MB / hours),
        heapPerHour: Math.round((last.heapUsed - first.heapUsed) / MB / hours),
    };
}

/**
 * Start sampling.
 *
 * Samples every `sampleMs` but only logs every `logEveryMs`, so the routine
 * signal stays readable while the trend is still computed from fine-grained
 * data. Threshold crossings log immediately regardless — by the time RSS is at
 * 85% on this box there are only ~40 minutes left, and that warning needs to be
 * in the log before the kill, not after it.
 *
 * `limitMb` is the container limit (4 GB on this Render plan), NOT the V8 heap
 * ceiling. The process is killed on RSS, and RSS includes native allocations
 * and buffers that never appear in heap statistics — which is precisely how a
 * process with a 3072 MB heap cap still crosses 4 GB.
 */
export function startMemoryMonitor(opts: {
    log: (msg: string, module?: string, level?: any) => void;
    sampleMs?: number;
    logEveryMs?: number;
    limitMb?: number;
} ) {
    if (started) return;
    started = true;

    const sampleMs = opts.sampleMs ?? 60_000;
    const logEveryMs = opts.logEveryMs ?? 5 * 60_000;
    const limitMb = opts.limitMb ?? Number(process.env.MEMORY_LIMIT_MB || 4096);
    let lastLoggedAt = 0;

    const tick = () => {
        try {
            const m = process.memoryUsage();
            history.push({
                t: Date.now(),
                rss: m.rss,
                heapUsed: m.heapUsed,
                external: m.external,
                arrayBuffers: m.arrayBuffers,
            });
            if (history.length > MAX_SAMPLES) history.shift();

            const snap = memorySnapshot();
            const pct = Math.round((snap.rss_mb / limitMb) * 100);

            // Bands rather than a single threshold so one climb produces a few
            // escalating lines instead of a warning every sample.
            const band = pct >= 90 ? 90 : pct >= 85 ? 85 : pct >= 70 ? 70 : 0;
            const crossedUp = band > lastWarnBand;
            if (band !== lastWarnBand) lastWarnBand = band;

            const due = Date.now() - lastLoggedAt >= logEveryMs;
            if (!due && !crossedUp) return;
            lastLoggedAt = Date.now();

            const line =
                `🧠 mem rss=${snap.rss_mb}MB (${pct}% of ${limitMb}MB) ` +
                `heap=${snap.heap_used_mb}/${snap.heap_total_mb}MB ` +
                `ext=${snap.external_mb}MB buf=${snap.array_buffers_mb}MB ` +
                `malloc=${snap.malloced_mb}MB detached_ctx=${snap.detached_contexts} ` +
                `trend=+${snap.rss_growth_mb_per_hour}MB/h (heap +${snap.heap_growth_mb_per_hour}MB/h) ` +
                `ctx=${JSON.stringify(snap.context)}`;

            if (crossedUp && band >= 85) {
                const hoursLeft = snap.rss_growth_mb_per_hour > 0
                    ? ((limitMb - snap.rss_mb) / snap.rss_growth_mb_per_hour).toFixed(1)
                    : '∞';
                opts.log(`${line} — ⚠️ ${hoursLeft}h to OOM at current rate`, 'Memory', 'error');
            } else if (crossedUp && band >= 70) {
                opts.log(line, 'Memory', 'warn');
            } else {
                opts.log(line, 'Memory', 'info');
            }
        } catch { /* telemetry must never take the process down */ }
    };

    tick();
    const timer = setInterval(tick, sampleMs);
    // Never hold the event loop open just to report memory.
    if (typeof timer.unref === 'function') timer.unref();
    opts.log(`🧠 memory monitor started — sampling ${sampleMs / 1000}s, limit ${limitMb}MB`, 'Memory', 'info');
}

/**
 * On-demand heap snapshot. Blocking and large — roughly the size of the live
 * heap — so this is never called automatically. Returns the path written.
 */
export function writeHeapSnapshotTo(dir: string): { file: string; bytes: number } {
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, `heap-${new Date().toISOString().replace(/[:.]/g, '-')}.heapsnapshot`);
    v8.writeHeapSnapshot(file);
    return { file, bytes: fs.statSync(file).size };
}

/** Raw sample history, for charting or an endpoint. */
export function memoryHistory() {
    return history.map(s => ({
        t: new Date(s.t).toISOString(),
        rss_mb: toMb(s.rss),
        heap_used_mb: toMb(s.heapUsed),
        external_mb: toMb(s.external),
        array_buffers_mb: toMb(s.arrayBuffers),
    }));
}
