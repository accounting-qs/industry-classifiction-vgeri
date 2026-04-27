/**
 * Bucket Library — reusable bucket definitions across runs.
 *
 * The library lets a user persist proven bucket definitions and seed future
 * Phase 1a runs with them. The discovery LLM is asked to REUSE preferred
 * buckets verbatim when alignment is high (≥ 0.7), avoiding name drift
 * across campaigns and accumulating institutional taxonomy knowledge.
 */

import type { SupabaseClient } from '@supabase/supabase-js';

export interface LibraryBucket {
    id: string;
    bucket_name: string;
    description: string | null;
    direct_ancestor: string | null;
    root_category: string | null;
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
    bucket_name: string;
    description?: string;
    direct_ancestor?: string;
    root_category?: string;
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
    const name = (input.bucket_name || '').trim();
    if (!name) throw new Error('bucket_name required');

    const payload = {
        bucket_name: name,
        description: input.description?.trim() || null,
        direct_ancestor: input.direct_ancestor?.trim() || null,
        root_category: input.root_category?.trim() || null,
        include_terms: input.include_terms || [],
        exclude_terms: input.exclude_terms || [],
        example_strings: input.example_strings || [],
        notes: input.notes?.trim() || null,
        updated_at: new Date().toISOString()
    };

    // upsert by unique bucket_name
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

/**
 * Save selected buckets from a completed run into the library.
 * For each selected bucket, the run's taxonomy_final entry is the source of truth.
 */
export async function saveRunBucketsToLibrary(
    supabase: SupabaseClient,
    runId: string,
    bucketNames: string[]
): Promise<{ saved: number; skipped: string[] }> {
    const { data: run, error } = await supabase
        .from('bucketing_runs').select('taxonomy_final,taxonomy_proposal').eq('id', runId).single();
    if (error || !run) throw new Error(`run not found: ${error?.message}`);
    const final = (run.taxonomy_final || run.taxonomy_proposal) as any;
    if (!final?.buckets) throw new Error('no taxonomy on this run');

    const selected = (final.buckets as any[]).filter(b => bucketNames.includes(b.bucket_name));
    const skipped: string[] = bucketNames.filter(n => !selected.some(b => b.bucket_name === n));
    let saved = 0;
    for (const b of selected) {
        await upsertLibraryBucket(supabase, {
            bucket_name: b.bucket_name,
            description: b.description,
            direct_ancestor: b.direct_ancestor,
            root_category: b.root_category,
            include_terms: b.include || [],
            exclude_terms: b.exclude || [],
            example_strings: b.example_strings || []
        });
        saved++;
    }
    return { saved, skipped };
}
