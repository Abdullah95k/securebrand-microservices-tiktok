-- +goose Up
-- Discovery part C: TikTok's discovery pages (M12; docs/tiktok/
-- discovery-plan.md §8; ADR 0007 §27): keyword search, hashtags' and
-- sounds' posts, monitored posts' related posts and the For You feed. Each
-- thing read is a target the leader reads on a timetable; every post found
-- is stored (discovered_via search, hashtag, sound, feed or related) and
-- its author goes to discovery's entry check through discovery_sightings,
-- as parts B and D's people do.

-- One row per thing read: source and key (search: the keyword's id;
-- hashtag: its name, lower-cased, as TikTok spells it; sound: the sound's
-- id; related: the post's id; feed: "feed"). item_id is the hashtag's id
-- (its post list needs it: from the monitored posts that use it, else from
-- its detail call), the sound's or the post's.
-- name is the keyword or the hashtag's name; origin says what made it a
-- target (keyword: the Iraq list; monitored: the posts of monitored
-- accounts use it; post: a monitored post's related posts; feed). uses is
-- how many monitored accounts used a hashtag or sound when it was last
-- picked. State: new, ok, gone (TikTok says the hashtag or sound does not
-- exist), refused (a status code twice: a login wall, 2483 from US exits
-- for search, T37). Kept for good (D8).
CREATE TABLE discovery_targets (
    source         text NOT NULL CHECK (source IN ('search', 'hashtag', 'sound', 'related', 'feed')),
    key            text NOT NULL,
    name           text,
    item_id        bigint,
    origin         text NOT NULL CHECK (origin IN ('keyword', 'monitored', 'post', 'feed')),
    uses           int,
    -- The related posts' post: its author and time, for the read's page.
    post_user_id   bigint,
    enabled        boolean NOT NULL DEFAULT true,
    state          text NOT NULL DEFAULT 'new' CHECK (state IN ('new', 'ok', 'gone', 'refused')),
    status_code    bigint,
    next_read_at   timestamptz,
    claimed_at     timestamptz,
    last_job_id    uuid,
    first_read_at  timestamptz,
    last_read_at   timestamptz,
    reads          bigint NOT NULL DEFAULT 0,
    pages_read     bigint NOT NULL DEFAULT 0,
    posts_seen     bigint NOT NULL DEFAULT 0,
    -- Pages read on day (UTC), for tt.discovery.pages.day_pages.
    day            date,
    day_pages      int NOT NULL DEFAULT 0,
    -- Why the last read stopped: end, cap, stuck, empty, gone, refused.
    last_stop      text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, key)
);
-- A target read once (related) has no next read.
CREATE INDEX discovery_targets_due_idx ON discovery_targets (next_read_at) WHERE next_read_at IS NOT NULL AND enabled;

-- Part C's finds in the sightings: via is what was read (the target's
-- key), region the exit's region TikTok served (discovery-plan §8.3: "IQ"
-- makes a find count as shown to viewers in Iraq). A find is recorded once
-- per source, target, post and person: a re-read or a redelivery records
-- nothing again. via_user_id stays parts B and D's monitored account.
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_how_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_how_check
    CHECK (how IN ('mention', 'reply', 'commenter', 'following', 'follower', 'search', 'hashtag', 'sound', 'feed', 'related'));
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_found_in_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_found_in_check
    CHECK (found_in IN ('caption', 'bio', 'comment', 'following', 'followers', 'search', 'hashtag', 'sound', 'feed', 'related'));
ALTER TABLE discovery_sightings ALTER COLUMN via_user_id DROP NOT NULL;
ALTER TABLE discovery_sightings ADD COLUMN via text, ADD COLUMN region text;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_via_check CHECK (via_user_id IS NOT NULL OR via IS NOT NULL);
CREATE UNIQUE INDEX discovery_sightings_find_idx ON discovery_sightings (how, via, post_id, user_id) NULLS NOT DISTINCT
    WHERE via IS NOT NULL;

-- +goose Down
DROP INDEX discovery_sightings_find_idx;
DELETE FROM discovery_sightings WHERE via IS NOT NULL;
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_via_check;
ALTER TABLE discovery_sightings DROP COLUMN region, DROP COLUMN via;
ALTER TABLE discovery_sightings ALTER COLUMN via_user_id SET NOT NULL;
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_found_in_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_found_in_check
    CHECK (found_in IN ('caption', 'bio', 'comment', 'following', 'followers'));
ALTER TABLE discovery_sightings DROP CONSTRAINT discovery_sightings_how_check;
ALTER TABLE discovery_sightings ADD CONSTRAINT discovery_sightings_how_check
    CHECK (how IN ('mention', 'reply', 'commenter', 'following', 'follower'));
DROP TABLE discovery_targets;
