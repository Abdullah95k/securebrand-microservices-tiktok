-- +goose Up
-- Erasure requests that hit a passing error (database, object store, Kafka)
-- are retried with backoff instead of failing for good.
ALTER TABLE erasure_requests
    ADD COLUMN attempts        integer NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at timestamptz;

-- +goose Down
ALTER TABLE erasure_requests
    DROP COLUMN next_attempt_at,
    DROP COLUMN attempts;
