-- +goose Up
-- tks keeps everything it collects (owner, 2026-11-01; docs/tiktok ADR 0007
-- §14): no erasure and no suppression, so their tables go (nothing ever
-- wrote to them outside tests), and a comment author's predicted_age_group,
-- TikTok's inference of their age band, is stored.
DROP TABLE erasure_requests;
DROP TABLE suppression;
ALTER TABLE people ADD COLUMN predicted_age_group text;

-- +goose Down
ALTER TABLE people DROP COLUMN predicted_age_group;
CREATE TABLE suppression (
    subject_type  text NOT NULL,   -- author | account
    subject_key   text NOT NULL,   -- hex author_key, or the account's user id
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_type, subject_key)
);
CREATE TABLE erasure_requests (
    id               uuid PRIMARY KEY,
    api_client_id    uuid REFERENCES api_clients (id),
    subject_type     text NOT NULL,   -- author | account
    author_key       bytea,
    user_id          bigint,
    input            jsonb NOT NULL,
    status           text NOT NULL,   -- pending | done | failed
    stats            jsonb,
    error            text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz,
    attempts         int NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz
);
CREATE INDEX erasure_pending_idx ON erasure_requests (created_at) WHERE status = 'pending';
