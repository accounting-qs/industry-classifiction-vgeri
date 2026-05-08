/**
 * Bucket Library — reusable campaign-bucket definitions across runs.
 *
 * The library persists proven (primary_identity, bucket_name) pairs and
 * seeds future runs with them. Phase 1b matches contacts against the library
 * deterministically before LLM matching, so saved knowledge wins.
 *
 * v6: dropped the legacy `characteristic` alias on LibraryBucketInput —
 * bucket_library entries are CAMPAIGN BUCKETS, not Layer-2 sub-identities,
 * so the alias was confusing. Use bucket_name everywhere.
 */

import type { SupabaseClient } from '@supabase/supabase-js';

export interface LibraryBucket {
    id: string;
    bucket_name: string;
    primary_identity: string | null;
    description: string | null;
    direct_ancestor: string | null;              // legacy mirror of primary_identity
    root_category: string | null;                // legacy
    include_terms: string[];
    exclude_terms: string[];
    example_strings: string[];
    notes: string | null;
    times_used: number;
    last_used_at: string | null;
    archived: boolean;
    created_at: string;
    updated_at: string;
}

export interface LibraryBucketInput {
    primary_identity?: string;
    bucket_name?: string;
    direct_ancestor?: string;                    // legacy alias for primary_identity
    root_category?: string;                      // legacy
    description?: string;
    include_terms?: string[];
    exclude_terms?: string[];
    example_strings?: string[];
    notes?: string;
}

export async function listLibrary(
    supabase: SupabaseClient,
    opts: { includeArchived?: boolean } = {}
): Promise<LibraryBucket[]> {
    let q: any = supabase.from('bucket_library').select('*');
    if (!opts.includeArchived) q = q.eq('archived', false);
    const { data, error } = await q.order('last_used_at', { ascending: false, nullsFirst: false }).order('bucket_name');
    if (error) throw new Error(`library list failed: ${error.message}`);
    return (data || []) as LibraryBucket[];
}

export async function upsertLibraryBucket(
    supabase: SupabaseClient,
    input: LibraryBucketInput
): Promise<LibraryBucket> {
    const spec = (input.bucket_name || '').trim();
    const ident = (input.primary_identity || input.direct_ancestor || '').trim();
    if (!spec) throw new Error('bucket_name is required');

    const payload = {
        bucket_name: spec,
        primary_identity: ident || null,
        direct_ancestor: ident || null,          // legacy mirror, kept until v6
        root_category: input.root_category?.trim() || null,
        description: input.description?.trim() || null,
        include_terms: input.include_terms || [],
        exclude_terms: input.exclude_terms || [],
        example_strings: input.example_strings || [],
        notes: input.notes?.trim() || null,
        updated_at: new Date().toISOString()
    };

    const { data, error } = await supabase.from('bucket_library')
        .upsert(payload, { onConflict: 'bucket_name' })
        .select()
        .single();
    if (error) throw new Error(`library upsert failed: ${error.message}`);
    return data as LibraryBucket;
}

export async function archiveLibraryBucket(
    supabase: SupabaseClient,
    id: string,
    archived: boolean
): Promise<void> {
    const { error } = await supabase.from('bucket_library')
        .update({ archived, updated_at: new Date().toISOString() })
        .eq('id', id);
    if (error) throw new Error(`library archive failed: ${error.message}`);
}

export async function deleteLibraryBucket(
    supabase: SupabaseClient,
    id: string
): Promise<void> {
    const { error } = await supabase.from('bucket_library').delete().eq('id', id);
    if (error) throw new Error(`library delete failed: ${error.message}`);
}

// Bulk hard-delete. The FK on bucket_library_run_links cascades, so the
// per-run usage history clears automatically. Historical run snapshots
// (bucket_industry_map / bucket_assignments) are denormalized text and
// stay untouched — those are point-in-time records of past runs.
export async function bulkDeleteLibraryBuckets(
    supabase: SupabaseClient,
    ids: string[]
): Promise<{ deleted: number }> {
    const clean = (ids || []).filter(id => typeof id === 'string' && id.length > 0);
    if (clean.length === 0) return { deleted: 0 };
    const { error, count } = await supabase
        .from('bucket_library')
        .delete({ count: 'exact' })
        .in('id', clean);
    if (error) throw new Error(`library bulk delete failed: ${error.message}`);
    return { deleted: count || 0 };
}

// Custom error so the route handler can map duplicate-name to HTTP 409
// without inspecting the message string.
export class LibraryRenameConflictError extends Error {
    constructor(name: string) {
        super(`A library bucket named "${name}" already exists`);
        this.name = 'LibraryRenameConflictError';
    }
}

export async function renameLibraryBucket(
    supabase: SupabaseClient,
    id: string,
    newName: string
): Promise<LibraryBucket> {
    const trimmed = (newName || '').trim();
    if (!trimmed) throw new Error('new name is required');

    // Pre-check uniqueness so we can return a clean 409 instead of a raw
    // Postgres unique-violation. Excludes the row being renamed so a
    // no-op rename (same name) doesn't false-positive.
    const { data: clash } = await supabase
        .from('bucket_library')
        .select('id')
        .eq('bucket_name', trimmed)
        .neq('id', id)
        .maybeSingle();
    if (clash) throw new LibraryRenameConflictError(trimmed);

    const { data, error } = await supabase
        .from('bucket_library')
        .update({ bucket_name: trimmed, updated_at: new Date().toISOString() })
        .eq('id', id)
        .select()
        .single();
    if (error) throw new Error(`library rename failed: ${error.message}`);
    return data as LibraryBucket;
}

/**
 * Bulk-import library buckets from a flexible newline+pipe format. Each
 * non-empty line is one bucket. Pipe separators define optional fields:
 *
 *   "SEO Agency"
 *   "SEO Agency | Agency"
 *   "SEO Agency | Agency | Performance + content marketing for B2B"
 *
 * Empty lines, lines starting with '#' (comments), and duplicate names
 * are skipped. Returns counts and the list of names that were skipped.
 */
export async function bulkImportLibraryFromText(
    supabase: SupabaseClient,
    text: string
): Promise<{ saved: number; skipped: { name: string; reason: string }[] }> {
    const lines = (text || '').split(/\r?\n/);
    const seen = new Set<string>();
    const skipped: { name: string; reason: string }[] = [];
    let saved = 0;

    for (const raw of lines) {
        const line = raw.trim();
        if (!line || line.startsWith('#')) continue;

        const parts = line.split('|').map(p => p.trim());
        const spec = parts[0];
        const ident = parts[1] || '';
        const desc = parts[2] || '';

        if (!spec) continue;
        if (seen.has(spec.toLowerCase())) {
            skipped.push({ name: spec, reason: 'duplicate within import' });
            continue;
        }
        seen.add(spec.toLowerCase());

        try {
            await upsertLibraryBucket(supabase, {
                bucket_name: spec,
                primary_identity: ident,
                description: desc
            });
            saved++;
        } catch (e: any) {
            skipped.push({ name: spec, reason: e.message?.slice(0, 200) || 'upsert failed' });
        }
    }
    return { saved, skipped };
}

/**
 * Save selected sub-identities from a completed run into the library
 * as new bucket_library entries. The taxonomy proposal stores them in
 * BucketProposal.sub_identity (v6); older runs may still use
 * .bucket_name — both are accepted for forward-compat.
 */
export async function saveRunBucketsToLibrary(
    supabase: SupabaseClient,
    runId: string,
    specNames: string[]
): Promise<{ saved: number; skipped: string[] }> {
    const { data: run, error } = await supabase
        .from('bucketing_runs').select('taxonomy_final,taxonomy_proposal').eq('id', runId).single();
    if (error || !run) throw new Error(`run not found: ${error?.message}`);
    const final = (run.taxonomy_final || run.taxonomy_proposal) as any;
    if (!final?.buckets) throw new Error('no taxonomy on this run');

    const wanted = new Set(specNames.map(s => s.trim()));
    const selected = (final.buckets as any[]).filter(b => {
        const spec = b.sub_identity || b.bucket_name;
        return spec && wanted.has(spec);
    });
    const matched = new Set(selected.map(b => (b.sub_identity || b.bucket_name).trim()));
    const skipped = specNames.filter(n => !matched.has(n.trim()));

    let saved = 0;
    for (const b of selected) {
        await upsertLibraryBucket(supabase, {
            bucket_name: b.sub_identity || b.bucket_name,
            primary_identity: b.primary_identity || b.direct_ancestor,
            description: b.description,
            include_terms: b.include || b.include_terms || [],
            exclude_terms: b.exclude || b.exclude_terms || [],
            example_strings: b.example_strings || []
        });
        saved++;
    }
    return { saved, skipped };
}
