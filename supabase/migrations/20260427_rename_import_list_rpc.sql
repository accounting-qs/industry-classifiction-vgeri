-- Renames an import_lists row and cascades the new name to every contact
-- that references it via the (un-FK'd) lead_list_name column.
--
-- Why an RPC: the cascading UPDATE on contacts sweeps every row matching
-- the old name (50k–200k+ in production lists), which blows past
-- PostgREST's default 8s statement_timeout. Doing the work in plpgsql
-- with its own statement_timeout keeps both updates inside one
-- transaction (so a partial rename can't leak) and lifts the cap to
-- something that fits a 250k-row update on the indexed lead_list_name
-- column.
--
-- Returns the row count of contacts updated so the API can surface it.
CREATE OR REPLACE FUNCTION public.rename_import_list(
    p_id UUID,
    p_new_name TEXT
)
RETURNS TABLE (
    list_id UUID,
    old_name TEXT,
    new_name TEXT,
    contacts_updated BIGINT
)
LANGUAGE plpgsql
SET statement_timeout TO '300s'
AS $$
DECLARE
    v_old_name TEXT;
    v_clash UUID;
    v_updated BIGINT;
BEGIN
    IF p_id IS NULL THEN
        RAISE EXCEPTION 'p_id is required';
    END IF;
    IF p_new_name IS NULL OR length(btrim(p_new_name)) = 0 THEN
        RAISE EXCEPTION 'p_new_name is required';
    END IF;

    SELECT name INTO v_old_name FROM import_lists WHERE id = p_id FOR UPDATE;
    IF v_old_name IS NULL THEN
        RAISE EXCEPTION 'list % not found', p_id;
    END IF;

    IF v_old_name = p_new_name THEN
        RETURN QUERY SELECT p_id, v_old_name, v_old_name, 0::BIGINT;
        RETURN;
    END IF;

    -- Reject collisions — list views/joins group by name, so duplicates
    -- would silently merge two lists in the UI.
    SELECT id INTO v_clash FROM import_lists WHERE name = p_new_name AND id <> p_id LIMIT 1;
    IF v_clash IS NOT NULL THEN
        RAISE EXCEPTION 'duplicate_list_name: a list named "%" already exists', p_new_name
            USING ERRCODE = 'unique_violation';
    END IF;

    UPDATE import_lists SET name = p_new_name WHERE id = p_id;

    UPDATE contacts SET lead_list_name = p_new_name WHERE lead_list_name = v_old_name;
    GET DIAGNOSTICS v_updated = ROW_COUNT;

    RETURN QUERY SELECT p_id, v_old_name, p_new_name, v_updated;
END;
$$;
