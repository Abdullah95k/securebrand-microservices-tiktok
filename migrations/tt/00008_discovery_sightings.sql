-- +goose Up
-- Discovery part B: accounts found in data tks already collects (M11;
-- docs/tiktok/discovery-plan.md §7; ADR 0007 §21), ported from fbs's
-- mentions (fbs ADR 0004, migration 00009_mentions).
--
-- The persister appends one row for each person a monitored account's
-- post, bio or comments bring in, only for rows it writes for the first
-- time (a new post, a new comment, a bio that changed), in the same
-- transaction: that is the de-duplication, so a re-read, an edit or a
-- redelivered batch records nothing again. how: mention (named in a
-- caption, a bio or a comment), reply (a monitored account answered their
-- comment), commenter (commented on a monitored account's post). via_user_id
-- is the monitored account. A person carries a user id and secUid, except
-- a bio mention, which carries only a username.
--
-- The leader counts the rows it has not counted yet (counted_at NULL) into
-- account_sources and discovery_candidates (discovery.Discoverer.Observe),
-- which sends each new account to its entry check. Rows are kept for good
-- (D8): counting only stamps counted_at.
CREATE TABLE discovery_sightings (
    id            bigserial PRIMARY KEY,
    how           text NOT NULL CHECK (how IN ('mention', 'reply', 'commenter')),
    -- Where it was seen: caption, bio or comment.
    found_in      text NOT NULL CHECK (found_in IN ('caption', 'bio', 'comment')),
    via_user_id   bigint NOT NULL,
    user_id       bigint,
    sec_uid       text,
    username      text,
    post_id       bigint,
    comment_id    bigint,
    seen_at       timestamptz NOT NULL,
    counted_at    timestamptz,
    CHECK (user_id IS NOT NULL OR sec_uid IS NOT NULL OR username IS NOT NULL)
);
CREATE INDEX discovery_sightings_uncounted_idx ON discovery_sightings (id) WHERE counted_at IS NULL;
CREATE INDEX discovery_sightings_user_idx ON discovery_sightings (user_id) WHERE user_id IS NOT NULL;

-- +goose Down
DROP TABLE discovery_sightings;
