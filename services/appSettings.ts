/**
 * App-wide settings backed by the `app_settings` table.
 *
 * Used for runtime-configurable secrets that we don't want in env vars —
 * e.g. the Anthropic API key entered via the Connectors UI. Reads are
 * cached in-process for 60s so hot paths (Phase 1a kickoff) don't hammer
 * the DB on every request.
 */

import type { SupabaseClient } from '@supabase/supabase-js';

const CACHE_TTL_MS = 60_000;

interface CachedValue {
    value: string | null;
    fetchedAt: number;
}

const cache = new Map<string, CachedValue>();

export async function getSetting(supabase: SupabaseClient, key: string): Promise<string | null> {
    const hit = cache.get(key);
    if (hit && (Date.now() - hit.fetchedAt) < CACHE_TTL_MS) return hit.value;

    const { data, error } = await supabase
        .from('app_settings').select('value').eq('key', key).maybeSingle();
    if (error) {
        console.warn(`[app_settings] read ${key} failed: ${error.message}`);
        // Cache the miss briefly so we don't loop on a failing DB.
        cache.set(key, { value: null, fetchedAt: Date.now() });
        return null;
    }
    const value = data?.value || null;
    cache.set(key, { value, fetchedAt: Date.now() });
    return value;
}

export async function setSetting(supabase: SupabaseClient, key: string, value: string | null): Promise<void> {
    if (value === null || value === '') {
        const { error } = await supabase.from('app_settings').delete().eq('key', key);
        if (error) throw new Error(`app_settings delete failed: ${error.message}`);
    } else {
        const { error } = await supabase.from('app_settings').upsert(
            { key, value, updated_at: new Date().toISOString() },
            { onConflict: 'key' }
        );
        if (error) throw new Error(`app_settings upsert failed: ${error.message}`);
    }
    cache.set(key, { value, fetchedAt: Date.now() });
}

export function invalidateSetting(key: string): void {
    cache.delete(key);
}

// Mask a secret to a small disclosure that proves it's configured without
// leaking the value. "sk-abcdef…wxyz".
export function maskSecret(value: string | null): string | null {
    if (!value) return null;
    if (value.length <= 8) return '*'.repeat(value.length);
    return `${value.slice(0, 6)}…${value.slice(-4)}`;
}
