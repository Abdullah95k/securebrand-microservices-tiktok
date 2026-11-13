-- +goose Up
-- Facebook discovery (owner D-065, 2026-11-13): facebook.com links found
-- in monitored accounts' bios and bio links, each sent once to the
-- Facebook collector's candidate endpoint (internal/tt/fbdisc). One row a
-- link: when it was found and on which account, when it was sent and what
-- the collector answered, and when a refused or failed send is tried again.
CREATE TABLE fb_candidates (
    ref          text PRIMARY KEY,
    user_id      bigint NOT NULL,
    found_at     timestamptz NOT NULL DEFAULT now(),
    sent_at      timestamptz,
    result       text,
    attempts     int NOT NULL DEFAULT 0,
    next_try_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX fb_candidates_unsent_idx ON fb_candidates (next_try_at) WHERE sent_at IS NULL;
CREATE INDEX fb_candidates_sent_idx ON fb_candidates (sent_at) WHERE sent_at IS NOT NULL;

-- +goose Down
DROP TABLE fb_candidates;
