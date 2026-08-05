import { Pool, type PoolClient } from 'pg';

/**
 * Shared direct-Postgres connection, and the escape hatch for database work
 * that is too slow to go through PostgREST.
 *
 * WHY THIS EXISTS
 * ---------------
 * Every `supabase.rpc(...)` call is an HTTP request through Supabase's
 * gateway, which cancels the request at ~60 s. The gateway gives up but
 * Postgres does not — the statement keeps running, still holding its locks,
 * while the caller believes it failed. That is exactly how a 109k-contact
 * bucketing run died: the per-contact explosion took 76 s, the gateway
 * returned "upstream request timeout" at 60 s, the code logged it as
 * non-fatal and moved on, and three minutes later the rollup's
 * `DELETE FROM bucket_contact_map` collided with the still-held locks and
 * was cancelled by `lock_timeout`.
 *
 * Measured at 109k contacts: explosion 76 s, rollup 39 s (95.9 s under load).
 * Both scale roughly linearly, so at the 700k target they are ~8 min and
 * ~10 min — far past the 60 s gateway limit. No amount of chunking fixes the
 * rollup, which is a single set-based statement by design.
 *
 * And the ceiling was lower than it looked: measured through the real REST
 * path, service_role sees `statement_timeout = 8s`, inherited from the
 * `authenticator` login role because service_role had no rolconfig of its own.
 * The heavy RPCs only survived past that because they SET it inside the
 * function body; plain table writes could not, which is what killed the
 * finalize upsert on a 421k run. The accompanying migration gives service_role
 * its own 120 s, but 120 s is still under what these operations need — hence
 * this module.
 *
 * A direct connection has neither ceiling: no HTTP gateway, and the session's
 * timeouts are ours to set. This is the same channel the CSV export/import
 * COPY streaming already uses.
 */

let poolSingleton: Pool | null = null;

export function getPgPool(): Pool {
    if (poolSingleton) return poolSingleton;
    const url = process.env.DATABASE_URL;
    if (!url) {
        throw new Error('DATABASE_URL is not set — required for direct-Postgres operations (CSV streaming, bucketing rollup/explosion)');
    }
    // DATABASE_URL must NOT include `?sslmode=require` — pg's connection-string
    // parser turns that into ssl:true, which overrides the ssl option below and
    // triggers a self-signed-cert error against the Supabase pooler CA (not in
    // Node's default trust). Leave sslmode off; the option enables encryption
    // without CA verification, matching Supabase's docs for direct pg.
    poolSingleton = new Pool({
        connectionString: url,
        // Raised from 2 now that bucketing shares this pool with CSV
        // export/import. Concurrency is still gated above (copySlot serialises
        // COPY, and one bucketing run executes at a time), so this is headroom
        // against a straggler holding a connection, not parallelism. The
        // instance shows 11/90 connections in use, so 6 is comfortable.
        max: 6,
        idleTimeoutMillis: 30_000,
        connectionTimeoutMillis: 10_000,
        ssl: { rejectUnauthorized: false }
    });
    // Loud startup log so a 6543-misconfig is obvious in Render logs.
    try {
        const parsed = new URL(url);
        console.log(`🔌 pg pool ready: host=${parsed.hostname} port=${parsed.port || '(default)'} max=6`);
        if (parsed.port === '6543') {
            console.warn('⚠️  DATABASE_URL is using port 6543 (transaction-mode pooler). COPY streaming and long RPCs require port 5432 (session-mode). They will fail mid-statement.');
        }
    } catch { /* not a URL — let the connection error surface */ }
    return poolSingleton;
}

/** Postgres SQLSTATEs that mean "someone else held the lock — try again". */
const RETRYABLE = new Set([
    '55P03', // lock_not_available
    '40001', // serialization_failure
    '40P01', // deadlock_detected
]);

// A cancelled statement (57014) is only retryable when it was a *lock* wait.
// Postgres reports both `statement_timeout` and `lock_timeout` cancellations
// under 57014, distinguishable only by message text.
function isRetryable(err: any): boolean {
    if (RETRYABLE.has(err?.code)) return true;
    return err?.code === '57014' && /lock timeout/i.test(String(err?.message || ''));
}

export interface HeavyRpcOptions {
    /** 0 disables the limit entirely. Default 15 min. */
    statementTimeoutMs?: number;
    /** How long a statement waits for a lock before giving up. Default 30 s. */
    lockTimeoutMs?: number;
    /** Attempts on a lock/serialization failure. Default 3. */
    retries?: number;
    /** Called before each attempt after the first, for progress logging. */
    onRetry?: (attempt: number, err: any) => void;
}

/**
 * Run a Postgres function over the direct connection instead of PostgREST.
 *
 * Bypasses the ~60 s gateway limit and the 120 s `statement_timeout` that
 * service_role inherits, so an operation that legitimately takes minutes at
 * volume can finish instead of being severed halfway.
 *
 * `lock_timeout` is deliberately still finite (30 s default): waiting forever
 * would turn a stuck peer into a hung run with no diagnostic. Instead we fail
 * fast and retry with backoff, which resolves the common case (a straggler
 * finishing its write) without hiding a genuine deadlock.
 *
 * Arguments are passed as bound parameters — never interpolated.
 */
export async function callHeavyRpc<T = any>(
    fn: string,
    args: any[] = [],
    opts: HeavyRpcOptions = {}
): Promise<T> {
    const rows = await runHeavy(fn, args, opts, false);
    return rows[0]?.result as T;
}

/**
 * Same as `callHeavyRpc`, for functions declared `RETURNS TABLE(...)`.
 *
 * A set-returning function has to be called as `SELECT * FROM fn(...)`.
 * `SELECT fn(...)` would collapse each row into a single composite column and
 * silently hand back malformed data, so the two cannot share a call shape.
 */
export async function callHeavyRpcRows<T = any>(
    fn: string,
    args: any[] = [],
    opts: HeavyRpcOptions = {}
): Promise<T[]> {
    return await runHeavy(fn, args, opts, true) as T[];
}

const IDENT = /^[a-z_][a-z0-9_]*$/i;

/**
 * Bulk INSERT ... ON CONFLICT DO UPDATE over the direct connection.
 *
 * The PostgREST equivalent (`supabase.from(t).upsert(rows)`) is capped by
 * whatever statement_timeout the request role carries — 8 s for service_role
 * before the accompanying migration, and still bounded by the ~60 s gateway
 * afterwards. This has neither limit, so a write that legitimately takes
 * minutes at volume completes instead of dying part-way through a chunk and
 * leaving the caller unsure how much landed.
 *
 * Chunk size is derived from Postgres' 65535 bind-parameter ceiling rather
 * than hardcoded: with 13 columns that is ~4600 rows per statement, versus
 * the 1000 the REST path used, so there are also far fewer round trips.
 *
 * Identifiers are validated, never interpolated blindly; values are always
 * bound parameters.
 */
export async function bulkUpsert(
    table: string,
    rows: Record<string, any>[],
    conflictColumns: string[],
    opts: HeavyRpcOptions & { updateColumns?: string[] } = {}
): Promise<number> {
    if (rows.length === 0) return 0;
    if (!IDENT.test(table)) throw new Error(`Unsafe table name: ${table}`);

    // Union of keys across all rows: a caller may omit a column on some rows,
    // and those must bind as NULL rather than shifting every later parameter.
    const columns = Array.from(rows.reduce<Set<string>>((set, r) => {
        Object.keys(r).forEach(k => set.add(k));
        return set;
    }, new Set<string>()));
    for (const c of [...columns, ...conflictColumns]) {
        if (!IDENT.test(c)) throw new Error(`Unsafe column name: ${c}`);
    }

    const updateCols = (opts.updateColumns ?? columns).filter(c => !conflictColumns.includes(c));
    const setClause = updateCols.length > 0
        ? `DO UPDATE SET ${updateCols.map(c => `${c} = EXCLUDED.${c}`).join(', ')}`
        : 'DO NOTHING';

    // Stay under the 65535 bind-parameter limit with headroom.
    const perChunk = Math.max(1, Math.floor(60000 / columns.length));
    let written = 0;

    for (let i = 0; i < rows.length; i += perChunk) {
        const chunk = rows.slice(i, i + perChunk);
        const values: any[] = [];
        const tuples = chunk.map(row => {
            const ph = columns.map(c => {
                values.push(row[c] ?? null);
                return `$${values.length}`;
            });
            return `(${ph.join(', ')})`;
        });
        const sql =
            `INSERT INTO ${table} (${columns.join(', ')}) VALUES ${tuples.join(', ')} ` +
            `ON CONFLICT (${conflictColumns.join(', ')}) ${setClause}`;
        const res = await runHeavySql(sql, values, opts);
        written += res.rowCount ?? chunk.length;
    }
    return written;
}

async function runHeavy(
    fn: string,
    args: any[],
    opts: HeavyRpcOptions,
    setReturning: boolean
): Promise<any[]> {
    // Function name is code-supplied, never user input, but it is concatenated
    // into SQL so assert the shape rather than trust the caller.
    if (!IDENT.test(fn)) throw new Error(`Unsafe function name: ${fn}`);
    const placeholders = args.map((_, i) => `$${i + 1}`).join(', ');
    const sql = setReturning
        ? `SELECT * FROM ${fn}(${placeholders})`
        : `SELECT ${fn}(${placeholders}) AS result`;
    const res = await runHeavySql(sql, args, opts);
    return res.rows;
}

/**
 * Execute one statement on the direct connection with our own timeouts and a
 * lock-contention retry. Shared by callHeavyRpc and bulkUpsert so both get
 * identical connection hygiene — there is exactly one place that decides how a
 * possibly-poisoned connection is returned to the pool.
 */
async function runHeavySql(sql: string, values: any[], opts: HeavyRpcOptions) {
    const {
        statementTimeoutMs = 15 * 60 * 1000,
        lockTimeoutMs = 30_000,
        retries = 3,
        onRetry,
    } = opts;

    const pool = getPgPool();
    let lastErr: any;

    for (let attempt = 1; attempt <= retries; attempt++) {
        let client: PoolClient | null = null;
        // Per-attempt, NOT lastErr: a retry that succeeds after an earlier
        // failure must still return a healthy connection to the pool.
        let attemptErr: any = null;
        try {
            client = await pool.connect();
            await client.query(`SET statement_timeout = ${Number(statementTimeoutMs) | 0}`);
            await client.query(`SET lock_timeout = ${Number(lockTimeoutMs) | 0}`);
            // Never leave a pooled session able to sit idle in a transaction:
            // a caller that dies mid-transaction would otherwise keep its locks
            // and wedge everything behind it — the failure mode that killed the
            // 109k run, where the server-wide setting is 0 (infinite).
            await client.query('SET idle_in_transaction_session_timeout = 60000');
            return await client.query(sql, values);
        } catch (err: any) {
            attemptErr = err;
            lastErr = err;
            if (attempt < retries && isRetryable(err)) {
                onRetry?.(attempt, err);
                // 2 s, 8 s — long enough for a straggling writer to commit.
                await new Promise(r => setTimeout(r, 2000 * attempt * attempt));
                continue;
            }
            throw err;
        } finally {
            if (client) {
                // Discard a connection whose state we are unsure of; returning
                // a session that may still be mid-transaction is how locks leak
                // across callers.
                try {
                    if (attemptErr) client.release(attemptErr);
                    else client.release();
                } catch { /* already released */ }
            }
        }
    }
    throw lastErr;
}
