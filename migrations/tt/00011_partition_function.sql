-- +goose Up
-- A role per component (M18): the components log in as roles that own no
-- table, so they cannot change the schema, yet the persister and the
-- scheduler create partitions (a post older than the window, the next
-- month). Creating a partition needs the parent's owner, so it goes through
-- this function, which runs as its owner (the role that migrates) and
-- creates only a range partition of one of tt's partitioned tables, named
-- as internal/store names them. tksctl migrate up grants EXECUTE on it.
-- +goose StatementBegin
CREATE FUNCTION tt.ensure_partition(parent text, child text, lo timestamptz, hi timestamptz, lz4 text[])
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tt, pg_temp
AS $$
DECLARE
    col text;
BEGIN
    IF parent NOT IN ('posts', 'comments', 'account_snapshots', 'post_metrics') THEN
        RAISE EXCEPTION 'ensure_partition: % is not a partitioned table of tt', parent;
    END IF;
    IF child !~ ('^' || parent || '_(y[0-9]{4}m[0-9]{2}|d[0-9]{8})$') OR hi <= lo THEN
        RAISE EXCEPTION 'ensure_partition: bad partition % [%, %)', child, lo, hi;
    END IF;
    PERFORM pg_advisory_xact_lock(4400001);
    IF to_regclass('tt.' || child) IS NOT NULL THEN
        RETURN false;
    END IF;
    -- Creating a partition locks its parent: wait a bounded time behind
    -- long readers instead of queueing every other query behind it.
    PERFORM set_config('lock_timeout', '5s', true);
    EXECUTE format('CREATE TABLE tt.%I PARTITION OF tt.%I FOR VALUES FROM (%L) TO (%L)%s',
        child, parent, lo, hi,
        CASE WHEN parent IN ('posts', 'comments') THEN ' WITH (fillfactor = 85)' ELSE '' END);
    FOREACH col IN ARRAY coalesce(lz4, '{}') LOOP
        BEGIN
            EXECUTE format('ALTER TABLE tt.%I ALTER COLUMN %I SET COMPRESSION lz4', child, col);
        EXCEPTION WHEN OTHERS THEN
            -- lz4 needs a server built with it (official images are).
            EXIT;
        END;
    END LOOP;
    RETURN true;
END;
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION tt.ensure_partition(text, text, timestamptz, timestamptz, text[]) FROM PUBLIC;

-- +goose Down
DROP FUNCTION tt.ensure_partition(text, text, timestamptz, timestamptz, text[]);
