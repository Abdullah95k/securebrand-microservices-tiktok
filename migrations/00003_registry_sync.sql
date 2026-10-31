-- +goose Up

-- fbs-registry-sync checks registry entries with real requests on canary
-- pages. A candidate that fails retire_after checks in a row is retired.
ALTER TABLE fb_queries
    ADD COLUMN last_checked_at timestamptz,
    ADD COLUMN last_check      text,          -- ok | stale | drift | inconclusive: <reason>
    ADD COLUMN check_failures  int NOT NULL DEFAULT 0;

-- One row per sync run, for operators and the registry gauges.
CREATE TABLE registry_sync_runs (
    run_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL,
    status       text NOT NULL,     -- ok | degraded | error
    report       jsonb NOT NULL
);
CREATE INDEX registry_sync_runs_started_idx ON registry_sync_runs (started_at DESC);

-- +goose Down
DROP TABLE registry_sync_runs;
ALTER TABLE fb_queries
    DROP COLUMN check_failures,
    DROP COLUMN last_check,
    DROP COLUMN last_checked_at;
