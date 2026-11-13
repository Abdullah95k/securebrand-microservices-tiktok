-- +goose Up
-- The reposts source in account_sources (00017 added it to the sightings
-- and targets only): counting a repost sighting inserts its source row
-- with how 'repost', which the check refused (SQLSTATE 23514), failing the
-- whole sightings batch, so nothing after it was counted.
ALTER TABLE account_sources DROP CONSTRAINT account_sources_how_check;
ALTER TABLE account_sources ADD CONSTRAINT account_sources_how_check
    CHECK (how IN ('api', 'import', 'mention', 'reply', 'commenter', 'search', 'hashtag',
                   'sound', 'feed', 'related', 'following', 'follower', 'repost'));

-- +goose Down
DELETE FROM account_sources WHERE how = 'repost';
ALTER TABLE account_sources DROP CONSTRAINT account_sources_how_check;
ALTER TABLE account_sources ADD CONSTRAINT account_sources_how_check
    CHECK (how IN ('api', 'import', 'mention', 'reply', 'commenter', 'search', 'hashtag',
                   'sound', 'feed', 'related', 'following', 'follower'));
