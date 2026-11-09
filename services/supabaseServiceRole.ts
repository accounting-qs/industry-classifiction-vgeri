import { createClient, type SupabaseClient } from '@supabase/supabase-js';

/**
 * The server's Supabase client, always authenticated as service_role.
 *
 * The key comes from SUPABASE_SERVICE_ROLE_KEY and nowhere else. The server
 * used to fall back to the anon key when it was unset. Once
 * supabase/migrations/20261109_lock_down_anon_access.sql revokes anon, that
 * fallback would boot a server whose every query fails. So a missing key,
 * or a key that is not service_role (the anon key pasted into the wrong
 * variable, say), stops the process at startup with the reason. On Render
 * the failed deploy never takes traffic and the previous one keeps serving.
 */

export const SUPABASE_URL = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL || '';

// Which Postgres role a Supabase API key authenticates as.
export function supabaseKeyRole(key: string | undefined): string {
    if (!key) return 'missing';
    // Newer Supabase key format isn't a JWT.
    if (key.startsWith('sb_secret_')) return 'service_role';
    if (key.startsWith('sb_publishable_')) return 'anon';
    try {
        const claims = JSON.parse(Buffer.from(key.split('.')[1], 'base64').toString());
        return String(claims.role || 'unknown');
    } catch { return 'unrecognised'; }
}

export function createServiceRoleClient(): SupabaseClient {
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
    const role = supabaseKeyRole(key);
    if (!SUPABASE_URL) {
        throw new Error('SUPABASE_URL (or VITE_SUPABASE_URL) is not set. The server cannot start without it.');
    }
    if (role !== 'service_role') {
        throw new Error(
            `SUPABASE_SERVICE_ROLE_KEY ${role === 'missing' ? 'is not set' : `holds a key for role "${role}", not service_role`}. ` +
            `The server only talks to Supabase as service_role (the anon role has no access to the database), ` +
            `so it will not start without it. Set SUPABASE_SERVICE_ROLE_KEY to the project's service_role ` +
            `(or sb_secret_) key and redeploy.`
        );
    }
    return createClient(SUPABASE_URL, key!);
}
