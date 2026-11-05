-- +goose Up
-- The entry check, the dormant list and the Iraq score (M10c;
-- docs/tiktok/discovery-plan.md §3, §4; ADR 0007 §19).
--
-- Every account a discovery source finds and tks does not monitor is a
-- candidate, one row per account (by user id when known, else by its
-- reference): it waits for its entry check (pending), is being read
-- (checking), passed and waits to be registered (qualified), was registered
-- (added), is public and active but not Iraqi (yet) (not_iraqi), is on the
-- dormant list (dormant), or was removed by a client and stays out
-- (blocked). Its sources and their evidence are in account_sources under
-- the same account_ref.
CREATE TABLE discovery_candidates (
    account_ref     text PRIMARY KEY,   -- sec:<secUid>, else u:<username>
    -- The order found: the API lists by it (newest first).
    id              bigserial UNIQUE,
    user_id         bigint,
    -- The username the last source gave (personal data, like accounts'):
    -- the entry check loads the profile page and sends the referer by it.
    username        text,
    state           text NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'checking', 'qualified', 'added', 'not_iraqi', 'dormant', 'blocked')),
    -- Why it is dormant: private, inactive (no post within
    -- tt.discovery.entry.active_for), never_posted, banned, gone, or
    -- unreadable (the entry check failed max_attempts times).
    reason          text CHECK (reason IN ('private', 'inactive', 'never_posted', 'banned', 'gone', 'unreadable')),
    -- The Iraq score and its evidence at the last decision (discovery.Score).
    score           double precision,
    evidence        jsonb,
    -- What the entry check read: the account's status, how many posts, the
    -- newest one's time.
    posts_read      int,
    newest_post_at  timestamptz,
    first_seen_at   timestamptz NOT NULL,
    last_seen_at    timestamptz NOT NULL,
    seen            bigint NOT NULL DEFAULT 1,
    -- The entry check's job while checking; attempts counts failed reads.
    job_id          uuid,
    queued_at       timestamptz,
    attempts        int NOT NULL DEFAULT 0,
    checked_at      timestamptz,
    -- Dormant: not read again before this (checked_at plus
    -- tt.discovery.entry.recheck_after), and after it only when a source
    -- finds the account again.
    next_check_at   timestamptz,
    -- Added: when, and the schedule it was registered under.
    added_at        timestamptz,
    schedule_ref    text,
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX discovery_candidates_user_idx ON discovery_candidates (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX discovery_candidates_state_idx ON discovery_candidates (state, score DESC NULLS LAST, first_seen_at);
CREATE INDEX discovery_candidates_job_idx ON discovery_candidates (job_id) WHERE job_id IS NOT NULL;

-- The dormant list (discovery-plan §4) is the candidates in that state.
CREATE VIEW dormant_accounts AS
    SELECT account_ref, user_id, reason, posts_read, newest_post_at, checked_at, next_check_at, last_seen_at, evidence
    FROM discovery_candidates WHERE state = 'dormant';

-- +goose Down
DROP VIEW dormant_accounts;
DROP TABLE discovery_candidates;
