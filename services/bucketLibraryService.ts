/**
 * Bucket Library — reusable characteristic definitions across runs.
 *
 * The library persists proven (primary_identity, characteristic) pairs and
 * seeds future runs with them. Phase 1b matches contacts against the library
 * deterministically before LLM matching, so saved knowledge wins.
 *
 * v5: bucket_library.functional_specialization was dropped — bucket_name now
 * holds the characteristic name. Inputs accept the new `characteristic` key
 * as well as the legacy `bucket_name` alias.
 */

import type { SupabaseClient } from '@supabase/supabase-js';

export interface LibraryBucket {
    id: string;
    bucket_name: string;                         // holds the characteristic name
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
    primary_identity?: string;                   // preferred
    characteristic?: string;                     // preferred (v5)
    bucket_name?: string;                        // legacy alias for characteristic
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
    const spec = (input.characteristic || input.bucket_name || '').trim();
    const ident = (input.primary_identity || input.direct_ancestor || '').trim();
    if (!spec) throw new Error('characteristic (or bucket_name) is required');

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
                characteristic: spec,
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
 * Save selected specializations from a completed run into the library.
 * Inputs are functional_specialization names (the new shape). Legacy
 * bucket_name lookup is also accepted for forward-compat.
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
        const spec = b.characteristic || b.bucket_name;
        return spec && wanted.has(spec);
    });
    const matched = new Set(selected.map(b => (b.characteristic || b.bucket_name).trim()));
    const skipped = specNames.filter(n => !matched.has(n.trim()));

    let saved = 0;
    for (const b of selected) {
        await upsertLibraryBucket(supabase, {
            characteristic: b.characteristic || b.bucket_name,
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
