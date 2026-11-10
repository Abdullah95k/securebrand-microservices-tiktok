-- +goose Up
-- Discovery's reposts source (M19; owner D-047, 2026-11-10; ADR 0007 §28):
-- the reposts tab of every monitored account (every
-- tt.discovery.reposts.every) and of every candidate once its entry check
-- has read it (once, posts of its own or not). It is a part C source: each
-- account read is a discovery target (source repost, key its user id), each
-- reposted post is stored as a found post (discovered_via repost, find via
-- the reposting account's id) and its author goes to the entry check
-- through discovery_sightings.

-- The reposting account's secUid (the repost list asks for it) and the
-- origin candidate (an account discovery is checking, read once).
ALTER TABLE discovery_targets ADD COLUMN sec_uid text;
ALTER TABLE discovery_targets DROP CONSTRAINT discovery_targets_source_check;
ALTER TABLE discovery_targets ADD CONSTRAINT discovery_targets_source_check
    CHECK (source IN ('search', 'hashtag', 'sound', 'related', 'feed', 'repost'));
ALTER TABLE discovery_targets DROP CONSTRAINT discovery_targets_origin_check;
ALTER TABLE discovery_targets ADD CONSTRAINT discovery_targets_origin_check
    CHECK (origin IN ('keyword', 'monitored', 'post', 'feed', 'candidate'));

-- Every account's repost history (D-047: the owner's legal review approved
-- keeping it in full), kept for good (D8): which account reposted which
-- post, by whom the post is, and when tks first and last saw it on the
-- account's reposts tab (TikTok shows no repost time). The post itself is
-- in posts. Hash partitions by the reposting account, as follows.
CREATE TABLE reposts (
    user_id         bigint NOT NULL,
    post_id         bigint NOT NULL,
    author_user_id  bigint NOT NULL,
    first_seen_at   timestamptz NOT NULL,
    last_seen_at    timestamptz NOT NULL,
    first_job_id    uuid,
    last_job_id     uuid,
    PRIMARY KEY (user_id, post_id)
) PARTITION BY HASH (user_id);
CREATE TABLE reposts_p00 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE reposts_p01 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE reposts_p02 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE reposts_p03 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE reposts_p04 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE reposts_p05 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE reposts_p06 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE reposts_p07 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE reposts_p08 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE reposts_p09 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE reposts_p10 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE reposts_p11 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE reposts_p12 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE reposts_p13 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE reposts_p14 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE reposts_p15 PARTITION OF reposts FOR VALUES WITH (MODULUS 16, REMAINDER 15);
-- Who reposted a post, and whose posts an account reposts.
CREATE INDEX reposts_post_idx ON reposts (post_id);
CREATE INDEX reposts_author_idx ON reposts (author_user_id);

-- The source in the sightings.
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_how_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_how_check
    CHECK (how IN ('mention', 'reply', 'commenter', 'following', 'follower', 'search', 'hashtag', 'sound', 'feed', 'related', 'repost'));
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_found_in_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_found_in_check
    CHECK (found_in IN ('caption', 'bio', 'comment', 'following', 'followers', 'search', 'hashtag', 'sound', 'feed', 'related', 'repost'));

-- +goose Down
DELETE FROM discovery_sightings WHERE how = 'repost';
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_found_in_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_found_in_check
    CHECK (found_in IN ('caption', 'bio', 'comment', 'following', 'followers', 'search', 'hashtag', 'sound', 'feed', 'related'));
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_how_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_how_check
    CHECK (how IN ('mention', 'reply', 'commenter', 'following', 'follower', 'search', 'hashtag', 'sound', 'feed', 'related'));
DROP TABLE reposts;
DELETE FROM discovery_targets WHERE source = 'repost' OR origin = 'candidate';
ALTER TABLE discovery_targets DROP CONSTRAINT discovery_targets_origin_check;
ALTER TABLE discovery_targets ADD CONSTRAINT discovery_targets_origin_check
    CHECK (origin IN ('keyword', 'monitored', 'post', 'feed'));
ALTER TABLE discovery_targets DROP CONSTRAINT discovery_targets_source_check;
ALTER TABLE discovery_targets ADD CONSTRAINT discovery_targets_source_check
    CHECK (source IN ('search', 'hashtag', 'sound', 'related', 'feed'));
ALTER TABLE discovery_targets DROP COLUMN sec_uid;
