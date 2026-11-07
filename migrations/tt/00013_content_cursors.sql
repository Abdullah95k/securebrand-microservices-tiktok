-- +goose Up
-- The shared contract's discovery.candidate.v1 (M15b): the scheduler's
-- candidate publisher reads discovery_candidates by updated_at (every write
-- to a candidate sets it) from its cursor, and passes each candidate
-- through content_state's gate, so an outcome goes out once.
CREATE INDEX discovery_candidates_updated_idx ON discovery_candidates (updated_at);

-- How far a publisher that scans a table has read: the last updated_at
-- it published ("discovery.candidate.v1"). It reads again from a little
-- before, for rows whose transaction committed late; the gate drops the
-- repeats.
CREATE TABLE content_cursors (
    name        text PRIMARY KEY,
    at          timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE content_cursors;
DROP INDEX discovery_candidates_updated_idx;
