/**
 * Pre-flight schema validator for the bucketing pipeline.
 *
 * Every column write inside Phase 1a / Phase 1b targets a specific
 * (table, column) pair. If a migration was skipped or partially applied,
 * the run only fails when we hit that column mid-flight — by which time
 * Sonnet has already been billed. This helper probes every required
 * (table, column) before any LLM call so the failure surfaces as one
 * clear message at run start.
 *
 * Update REQUIRED_SCHEMA + REQUIRED_RPCS whenever new columns or RPCs
 * are added to the bucketing pipeline. Keeping this list in sync is
 * cheap insurance — the alternative is debugging a half-completed run.
 */

import type { SupabaseClient } from '@supabase/supabase-js';

const REQUIRED_SCHEMA: Record<string, string[]> = {
    // Per-industry tagging output (Phase 1a writes here).
    bucket_industry_map: [
        'bucketing_run_id', 'industry_string', 'bucket_name', 'source', 'confidence',
        'identity', 'characteristic', 'sector',
        'functional_core', 'sector_core',
        'is_new_identity', 'is_new_characteristic', 'is_new_sector',
        'is_new_functional_core', 'is_new_sector_core',
        'is_disqualified', 'is_generic',
        'needs_qa', 'raw_industry', 'llm_reason',
        'canonical_classification',
        'identity_confidence', 'characteristic_confidence', 'sector_confidence',
        'primary_identity', 'functional_specialization', 'sector_focus',
    ],
    // Final per-contact assignments (Phase 1b writes here).
    bucket_assignments: [
        'bucketing_run_id', 'contact_id', 'bucket_name', 'source', 'confidence',
        'bucket_leaf', 'bucket_ancestor', 'bucket_root',
        'primary_identity', 'functional_specialization', 'sector_focus',
        'functional_core', 'sector_core',
        'pre_rollup_bucket_name', 'rollup_level',
        'general_reason', 'reasons',
        'is_generic', 'is_disqualified',
        'canonical_classification', 'bucket_reason',
        'identity_confidence', 'characteristic_confidence', 'sector_confidence',
        'assigned_at',
    ],
    // Per-contact pre-rollup decisions (Phase 1b writes here).
    bucket_contact_map: [
        'bucketing_run_id', 'contact_id', 'industry_string',
        'primary_identity', 'functional_specialization', 'sector_focus',
        'functional_core', 'sector_core',
        'pre_rollup_bucket_name', 'bucket_name', 'rollup_level',
        'source', 'confidence',
        'leaf_score', 'ancestor_score', 'root_score',
        'is_generic', 'is_disqualified',
        'general_reason', 'reasons',
        'canonical_classification', 'bucket_reason',
        'identity_confidence', 'characteristic_confidence', 'sector_confidence',
        'assigned_at',
    ],
    // Run-level metadata (read + updated by both phases).
    bucketing_runs: [
        'id', 'name', 'list_names', 'min_volume', 'bucket_budget', 'status',
        'taxonomy_proposal', 'taxonomy_final', 'taxonomy_model',
        'preferred_library_ids',
        'total_contacts', 'assigned_contacts', 'cost_usd',
        'created_at', 'taxonomy_completed_at', 'assignment_completed_at',
        'progress', 'cancel_requested', 'error_message',
        'taxonomy_snapshot', 'taxonomy_version',
        'quality_warnings', 'coverage_summary',
        'generic_audit',
        'apply_identity_dq_cascade',
    ],
    // Editable taxonomy library (read by Phase 1a).
    taxonomy_identities: ['id', 'name', 'description', 'is_disqualified', 'created_by', 'archived'],
    taxonomy_characteristics: ['id', 'name', 'parent_identity', 'description', 'created_by', 'archived', 'functional_core'],
    taxonomy_sectors: ['id', 'name', 'synonyms', 'description', 'created_by', 'archived', 'sector_core'],
    bucketing_run_logs: ['id', 'bucketing_run_id', 'timestamp', 'level', 'message'],
};

// RPC name + a representative arg payload that's syntactically valid for
// the function's signature. The probe doesn't care about the result —
// it only distinguishes "function not found" from "function ran".
const REQUIRED_RPCS: { name: string; args: Record<string, any> }[] = [
    { name: 'get_industry_vocabulary', args: { p_list_names: ['__probe__'], p_limit: 1 } },
    { name: 'get_bucket_assignment_counts', args: { p_run_id: '00000000-0000-0000-0000-000000000000' } },
    { name: 'get_bucket_sector_mix', args: { p_run_id: '00000000-0000-0000-0000-000000000000' } },
    { name: 'get_bucket_general_breakdown', args: { p_run_id: '00000000-0000-0000-0000-000000000000' } },
];

export interface SchemaGap {
    table?: string;
    columns?: string[];
    rpc?: string;
    note: string;
}

export interface SchemaCheckResult {
    ok: boolean;
    gaps: SchemaGap[];
    summary: string;
}

/**
 * Runs the full pre-flight check. Returns ok=true when every table,
 * column, and RPC the bucketing pipeline depends on is live in the
 * Supabase schema cache.
 *
 * Fast path: a single call to the bucketing_schema_gaps RPC returns
 * every missing (table, column) in one round-trip. Falls back to the
 * legacy per-column probe loop if the RPC itself is missing — handles
 * the bootstrap case where 20260505_bucketing_schema_gaps.sql hasn't
 * been applied yet.
 */
export async function checkBucketingSchema(supabase: SupabaseClient): Promise<SchemaCheckResult> {
    const gaps: SchemaGap[] = [];

    // Fast path: ask the DB directly.
    const { data: gapRows, error: gapErr } = await supabase.rpc('bucketing_schema_gaps');
    const fastPathAvailable = !gapErr || !/function .* does not exist|not find the function/i.test(gapErr.message || '');

    if (fastPathAvailable && !gapErr) {
        // Group missing columns by table so the error message stays short.
        const byTable = new Map<string, string[]>();
        for (const r of (gapRows || []) as { table_name: string; column_name: string }[]) {
            if (!byTable.has(r.table_name)) byTable.set(r.table_name, []);
            byTable.get(r.table_name)!.push(r.column_name);
        }
        for (const [table, columns] of byTable.entries()) {
            gaps.push({ table, columns, note: `Missing columns on ${table}` });
        }
    } else {
        // Slow fallback — replicates the old behaviour for environments
        // that haven't had 20260505 applied yet. Up to ~10s of round-trips.
        for (const [table, columns] of Object.entries(REQUIRED_SCHEMA)) {
            const { error: tableErr } = await supabase.from(table).select('*').limit(0);
            if (tableErr && /schema cache|does not exist/i.test(tableErr.message)) {
                gaps.push({ table, columns, note: `Table "${table}" not found in schema cache` });
                continue;
            }
            const missing: string[] = [];
            for (const col of columns) {
                const { error } = await supabase.from(table).select(col).limit(1);
                if (error && /column .* does not exist|not find the .* column/i.test(error.message)) {
                    missing.push(col);
                }
            }
            if (missing.length > 0) {
                gaps.push({ table, columns: missing, note: `Missing columns on ${table}` });
            }
        }
    }

    // RPC presence check — kept unconditional; cheap and verifies
    // bucketing_schema_gaps itself + the read-side RPCs.
    for (const rpc of REQUIRED_RPCS) {
        const { error } = await supabase.rpc(rpc.name, rpc.args);
        if (error && /function .* does not exist|not find the function/i.test(error.message)) {
            gaps.push({ rpc: rpc.name, note: `RPC "${rpc.name}" not found` });
        }
    }

    const ok = gaps.length === 0;
    const summary = ok
        ? 'Schema OK — all required tables, columns, and RPCs are present.'
        : buildGapSummary(gaps);
    return { ok, gaps, summary };
}

function buildGapSummary(gaps: SchemaGap[]): string {
    const lines: string[] = [
        'Bucketing schema is out of date. Run the latest migrations in Supabase before retrying.',
        '',
        'Missing pieces:'
    ];
    for (const g of gaps) {
        if (g.rpc) {
            lines.push(`  • RPC: ${g.rpc}`);
        } else if (g.table && g.columns && g.columns.length > 0) {
            lines.push(`  • ${g.table} — ${g.columns.join(', ')}`);
        } else if (g.table) {
            lines.push(`  • ${g.table} (table missing)`);
        }
    }
    lines.push('');
    lines.push('Catch up by re-running the unapplied migration in supabase/migrations/. The migrations are idempotent, so it is safe to run them again.');
    return lines.join('\n');
}
