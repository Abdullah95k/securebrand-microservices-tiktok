-- +goose Up
-- Discovery part D: who follows whom (M13; docs/tiktok/discovery-plan.md §9;
-- ADR 0007 §23). Every monitored account's following list (unless its
-- profile hides it) and followers list are read; every person on them is
-- kept as a link, and the people new to a list go to discovery's entry
-- check through discovery_sightings, as part B's do.

-- followingVisibility from the profile page: 1 shows the following list,
-- 2 hides it (TikTok answers the list with 10222; C6 run 1). NULL: not
-- read yet. Part D does not read a hidden list.
ALTER TABLE accounts ADD COLUMN following_visibility smallint;

-- The links (discovery-plan §9: who, whom, first and last seen), kept for
-- good (D8). user_id is the monitored account whose list it is; direction
-- following: user_id follows the person; follower: the person follows
-- user_id. The person's name and avatar are in people (author_key); what
-- the list showed of them (private, counts) is kept here as last seen.
-- Billions of links at full scale: hash partitions by the account, so one
-- list's links stay together and a partition never grows by date alone.
CREATE TABLE follows (
    user_id           bigint NOT NULL,
    direction         text NOT NULL CHECK (direction IN ('following', 'follower')),
    person_user_id    bigint NOT NULL,
    author_key        bytea NOT NULL,
    person_private    boolean,
    person_videos     bigint,
    person_followers  bigint,
    person_following  bigint,
    first_seen_at     timestamptz NOT NULL,
    last_seen_at      timestamptz NOT NULL,
    first_job_id      uuid,
    last_job_id       uuid,
    PRIMARY KEY (user_id, direction, person_user_id)
) PARTITION BY HASH (user_id);
CREATE TABLE follows_p00 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE follows_p01 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE follows_p02 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE follows_p03 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE follows_p04 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE follows_p05 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE follows_p06 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE follows_p07 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE follows_p08 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE follows_p09 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE follows_p10 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE follows_p11 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE follows_p12 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE follows_p13 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE follows_p14 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE follows_p15 PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER 15);
-- Whose lists a person is on (the API, and "followed by monitored Iraqi
-- accounts" on the review page).
CREATE INDEX follows_person_idx ON follows (person_user_id, direction);

-- One row per monitored account and list: when it is read next, where the
-- deep walk stopped, and what TikTok answered. The leader claims due lists
-- (next_read_at), the crawler's follow stage writes the outcome of each
-- read. state: new (never read), ok, hidden (10222: the account hides its
-- following list), gone (the account is private, banned or missing), refused
-- (another status code, kept in status_code: a list TikTok will not serve
-- a logged-out visitor).
CREATE TABLE follow_lists (
    user_id        bigint NOT NULL,
    direction      text NOT NULL CHECK (direction IN ('following', 'follower')),
    state          text NOT NULL DEFAULT 'new' CHECK (state IN ('new', 'ok', 'hidden', 'gone', 'refused')),
    status_code    bigint,
    next_read_at   timestamptz NOT NULL DEFAULT now(),
    claimed_at     timestamptz,
    last_job_id    uuid,
    first_read_at  timestamptz,
    last_read_at   timestamptz,
    -- TikTok's cursor where the deep walk stopped; deep_done once it reached
    -- the list's end (later reads take only the newest people).
    deep_cursor    text,
    deep_done      boolean NOT NULL DEFAULT false,
    -- The list's size when TikTok says it, and what tks read of it.
    total          bigint,
    people_seen    bigint NOT NULL DEFAULT 0,
    pages_read     bigint NOT NULL DEFAULT 0,
    -- Pages read on day (UTC), for tt.discovery.follows.day_pages.
    day            date,
    day_pages      int NOT NULL DEFAULT 0,
    -- Why the last read stopped: end, known, cap, day_cap, stuck, hidden,
    -- gone, refused.
    last_stop      text,
    PRIMARY KEY (user_id, direction)
);
CREATE INDEX follow_lists_due_idx ON follow_lists (next_read_at);

-- Part D's sources in the sightings: following (a monitored account follows
-- the person) and follower (the person follows it), found in the account's
-- following or followers list, with what the list showed (private, the
-- post count), so that the cheap pre-checks need no read.
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_how_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_how_check
    CHECK (how IN ('mention', 'reply', 'commenter', 'following', 'follower'));
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_found_in_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_found_in_check
    CHECK (found_in IN ('caption', 'bio', 'comment', 'following', 'followers'));
ALTER TABLE discovery_sightings ADD COLUMN private boolean, ADD COLUMN videos bigint;

-- +goose Down
ALTER TABLE discovery_sightings DROP COLUMN videos, DROP COLUMN private;
DELETE FROM discovery_sightings WHERE how IN ('following', 'follower');
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_found_in_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_found_in_check
    CHECK (found_in IN ('caption', 'bio', 'comment'));
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_how_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_how_check
    CHECK (how IN ('mention', 'reply', 'commenter'));
DROP TABLE follow_lists;
DROP TABLE follows;
ALTER TABLE accounts DROP COLUMN following_visibility;
