-- +goose Up
-- What M17 reads from a post that tks dropped before: a photo post's own
-- title (imagePost.title). The creator's AI text (AIGCDescription) goes in
-- labels, with the other labels.
ALTER TABLE posts ADD COLUMN title text;

-- Videos' subtitle tracks (video.subtitleInfos): TikTok's speech-to-text
-- (source_name ASR) and its machine translations (MT), one row per post,
-- language and source as TikTok serves them (ara-SA, ASR). body is the
-- WebVTT file: the account's own speech, served by the API without
-- pii:read like its caption (ADR 0007 §6), kept for good (D8). A file over
-- tt.subtitles.max_bytes is a row without a body (skipped = 'too_large'),
-- so it is not fetched again. Not partitioned: a few KB per track, about
-- one track per video.
CREATE TABLE post_subtitles (
    post_id          bigint NOT NULL,
    language         text NOT NULL,
    source_name      text NOT NULL,
    source           text NOT NULL CHECK (source IN ('asr', 'mt', 'unspecified')),
    user_id          bigint NOT NULL,
    post_created_at  timestamptz NOT NULL,
    language_id      text,
    version          text,
    format           text,
    size_bytes       bigint,
    body             text,
    skipped          text,
    content_hash     bytea NOT NULL,
    first_seen_at    timestamptz NOT NULL,
    last_seen_at     timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (post_id, language, source_name)
);
CREATE INDEX post_subtitles_user_idx ON post_subtitles (user_id, post_created_at DESC);

-- +goose Down
DROP TABLE post_subtitles;
ALTER TABLE posts DROP COLUMN title;
