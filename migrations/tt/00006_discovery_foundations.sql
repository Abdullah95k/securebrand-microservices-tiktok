-- +goose Up
-- Discovery foundations and bulk import (M10a; docs/tiktok/discovery-plan.md
-- §5, §6; ADR 0007 §18). Nothing here reads TikTok: it records where each
-- account came from, the owner's labels and removals, paced admission of
-- new accounts, the lighter checks of accounts that went quiet, the keyword
-- list, and import batches.

-- The search keywords (fbs's discovery_keywords, migration 00008): seeded
-- from configs/tt/discovery/iraq.yaml by `tksctl discovery keywords seed`.
-- Searched by part C (M12); last_queued_at and last_searched_at are its.
CREATE TABLE discovery_keywords (
    id                bigserial PRIMARY KEY,
    term              text NOT NULL,
    -- The term folded for matching and de-duplication (discovery.Normalize).
    normalized        text NOT NULL,
    language          text NOT NULL CHECK (language IN ('en', 'ar', 'ckb', 'kmr')),
    grp               text NOT NULL DEFAULT '',
    enabled           boolean NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    last_queued_at    timestamptz,
    last_searched_at  timestamptz
);
CREATE UNIQUE INDEX discovery_keywords_term_idx ON discovery_keywords (normalized, language);

-- origin: owner (registered through the API or an import: never moved off
-- the schedule automatically) or discovery (added by a source; moves to the
-- dormant list when it goes quiet, M10c). An owner's registration makes a
-- discovered account the owner's.
-- admitted_at: when paced admission (tt.admission) let the account's first
-- read start; NULL while it waits in the queue, by admit_priority (the
-- owner's first, then discovered accounts by Iraq score) and age. Every
-- schedule that exists already was admitted when it was made.
-- inactive_since: when checks first found the account private, banned or
-- gone (NULL while active): tt.freshness.inactive spaces its checks by it.
ALTER TABLE account_schedule
    ADD COLUMN origin text NOT NULL DEFAULT 'owner' CHECK (origin IN ('owner', 'discovery')),
    ADD COLUMN admit_priority int NOT NULL DEFAULT 0,
    ADD COLUMN admitted_at timestamptz,
    ADD COLUMN paced boolean NOT NULL DEFAULT false,
    ADD COLUMN inactive_since timestamptz;
UPDATE account_schedule SET admitted_at = created_at;
CREATE INDEX account_schedule_admission_idx ON account_schedule (admit_priority DESC, created_at)
    WHERE admitted_at IS NULL AND enabled;

-- Where each account came from: one row per way (how) and what led to it
-- (via: an import batch, a keyword, an account's user id; '' for the API),
-- kept for good with the first and last time it was seen and the evidence
-- at that time. account_ref is the schedule row's when the account is
-- registered, else the account's id: reference; user_id once known.
CREATE TABLE account_sources (
    account_ref    text NOT NULL,
    how            text NOT NULL CHECK (how IN ('api', 'import', 'mention', 'reply', 'commenter', 'search', 'hashtag',
                                               'sound', 'feed', 'related', 'following', 'follower')),
    via            text NOT NULL DEFAULT '',
    user_id        bigint,
    api_client_id  uuid REFERENCES api_clients (id),
    first_at       timestamptz NOT NULL,
    last_at        timestamptz NOT NULL,
    seen           bigint NOT NULL DEFAULT 1,
    evidence       jsonb,
    score          double precision,
    PRIMARY KEY (account_ref, how, via)
);
CREATE INDEX account_sources_user_idx ON account_sources (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX account_sources_how_idx ON account_sources (how, first_at);

-- Free labels on a client's registration (list:banks, import:2026-11-05).
CREATE TABLE account_labels (
    api_client_id  uuid NOT NULL REFERENCES api_clients (id),
    account_ref    text NOT NULL REFERENCES account_schedule (account_ref) ON DELETE CASCADE,
    label          text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (api_client_id, account_ref, label)
);
CREATE INDEX account_labels_label_idx ON account_labels (api_client_id, label);

-- Accounts a client removed: discovery never adds them back (plan §5.5).
-- A registration by the same client lifts its own block.
CREATE TABLE account_blocks (
    api_client_id  uuid NOT NULL REFERENCES api_clients (id),
    account_ref    text NOT NULL,
    user_id        bigint,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (api_client_id, account_ref)
);
CREATE INDEX account_blocks_user_idx ON account_blocks (user_id) WHERE user_id IS NOT NULL;

-- Bulk imports (plan §6): one batch per run of `tksctl accounts import` or
-- call of POST /v1/accounts/import, and one row per line read.
CREATE TABLE import_batches (
    id             uuid PRIMARY KEY,
    api_client_id  uuid NOT NULL REFERENCES api_clients (id),
    labels         text[] NOT NULL DEFAULT '{}',
    dry_run        boolean NOT NULL,
    counts         jsonb NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz
);
CREATE TABLE import_lines (
    batch_id     uuid NOT NULL REFERENCES import_batches (id) ON DELETE CASCADE,
    line         int NOT NULL,
    input        text NOT NULL,
    account_ref  text,
    outcome      text NOT NULL,   -- added | joined | already | duplicate | rejected
    reason       text,
    PRIMARY KEY (batch_id, line)
);
CREATE INDEX import_lines_ref_idx ON import_lines (account_ref) WHERE account_ref IS NOT NULL;

-- +goose Down
DROP TABLE import_lines;
DROP TABLE import_batches;
DROP TABLE account_blocks;
DROP TABLE account_labels;
DROP TABLE account_sources;
DROP INDEX account_schedule_admission_idx;
ALTER TABLE account_schedule DROP COLUMN inactive_since, DROP COLUMN paced, DROP COLUMN admitted_at, DROP COLUMN admit_priority,
    DROP COLUMN origin;
DROP TABLE discovery_keywords;
