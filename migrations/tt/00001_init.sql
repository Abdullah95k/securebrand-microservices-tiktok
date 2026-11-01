-- +goose Up
-- tks's schema tt (docs/tiktok ADR 0007 §4). Every statement runs with
-- search_path = tt (internal/tt/ttstore pins it), so the names below are
-- tt's own. Ids are TikTok's int64 ids; a person is keyed by author_key,
-- HMAC-SHA256 of their numeric user id, never by username or secUid.
-- Monthly partitions on the post's creation time (posts, comments) and the
-- capture time (account_snapshots), daily on post_metrics, created by
-- `tksctl migrate up` and the scheduler; retention is a DROP.

-- Accounts ------------------------------------------------------------------
CREATE TABLE accounts (
    user_id               bigint PRIMARY KEY,
    sec_uid               text NOT NULL,
    username              text,     -- personal data from here to avatar_media_id
    nickname              text,
    bio                   text,
    bio_link              text,
    bio_link_risk         int,
    avatar_url            text,
    avatar_media_id       uuid,
    verified              boolean,
    private               boolean,
    organization          boolean,
    language              text,
    created_at            timestamptz,  -- the account's creation (createTime)
    username_modified_at  timestamptz,  -- uniqueIdModifyTime
    nickname_modified_at  timestamptz,  -- nickNameModifyTime
    -- Exact counts (statsV2; list items and stats are rounded).
    follower_count        bigint,
    following_count       bigint,
    heart_count           bigint,
    video_count           bigint,
    likes_given_count     bigint,
    friend_count          bigint,
    author_key            bytea NOT NULL,
    status                text NOT NULL DEFAULT 'active',  -- active | private | unavailable | banned | suppressed
    content_hash          bytea,
    first_seen_at         timestamptz NOT NULL DEFAULT now(),
    last_seen_at          timestamptz NOT NULL DEFAULT now(),
    profile_refreshed_at  timestamptz,  -- the last full account read
    page_read_at          timestamptz   -- the last profile page (the bio link's only source)
);
CREATE UNIQUE INDEX accounts_sec_uid_idx ON accounts (sec_uid);
CREATE INDEX accounts_username_idx ON accounts (lower(username)) WHERE username IS NOT NULL;
CREATE INDEX accounts_author_key_idx ON accounts (author_key);

-- The username history; also resolves an old username to its account.
CREATE TABLE account_usernames (
    user_id        bigint NOT NULL,
    username       text NOT NULL,
    first_seen_at  timestamptz NOT NULL,
    last_seen_at   timestamptz NOT NULL,
    PRIMARY KEY (user_id, username)
);
CREATE INDEX account_usernames_name_idx ON account_usernames (lower(username), last_seen_at DESC);

-- The daily history of an account.
CREATE TABLE account_snapshots (
    user_id          bigint NOT NULL,
    captured_at      timestamptz NOT NULL,
    username         text,
    nickname         text,
    follower_count   bigint,
    following_count  bigint,
    heart_count      bigint,
    video_count      bigint,
    verified         boolean,
    private          boolean,
    PRIMARY KEY (user_id, captured_at)
) PARTITION BY RANGE (captured_at);

-- Posts: videos and photo posts, partitioned on their creation time so a
-- post and its comments share a partition. -----------------------------------
CREATE TABLE posts (
    created_at          timestamptz NOT NULL,
    post_id             bigint NOT NULL,
    user_id             bigint NOT NULL,
    kind                text NOT NULL,          -- video | photo
    caption             text,
    text_ranges         jsonb,
    hashtags            text[],
    hashtag_ids         bigint[],
    duration_ms         bigint,
    width               int,
    height              int,
    cover_media_id      uuid,
    photo_count         int,
    music_id            bigint,
    is_pinned           boolean,                -- known only from list reads
    is_ad               boolean,
    labels              jsonb,                  -- AI-generated, warnings, anchors, as served
    category            int,
    playlist_id         text,
    comments_disabled   boolean,
    region              text,
    text_language       text,
    discovered_via      text NOT NULL DEFAULT 'list',  -- list | search | hashtag | post_page
    -- The latest counts (exact, statsV2); their history is post_metrics.
    play_count          bigint,
    like_count          bigint,
    comment_count       bigint,
    share_count         bigint,
    save_count          bigint,
    repost_count        bigint,
    -- Comment coverage (ADR 0007 §5), filled by the job finalizer.
    comments_collected  bigint,
    replies_collected   bigint,
    completeness        real,
    comments_state      text NOT NULL DEFAULT 'none',  -- none | partial | complete | truncated
    comments_synced_at  timestamptz,
    comment_count_read  bigint,                 -- comment_count when comments were last read
    content_hash        bytea NOT NULL,
    status              text NOT NULL DEFAULT 'visible',  -- visible | unavailable | private
    first_seen_at       timestamptz NOT NULL,
    last_seen_at        timestamptz NOT NULL,
    PRIMARY KEY (created_at, post_id)
) PARTITION BY RANGE (created_at);
CREATE INDEX posts_user_idx ON posts (user_id, created_at DESC);
CREATE INDEX posts_hashtags_idx ON posts USING gin (hashtags);

-- post_id -> partition key, so lookups by post id prune to one partition.
CREATE TABLE post_index (
    post_id     bigint PRIMARY KEY,
    user_id     bigint NOT NULL,
    created_at  timestamptz NOT NULL,
    kind        text NOT NULL
);
CREATE INDEX post_index_user_idx ON post_index (user_id, created_at DESC);

-- People a caption mentions (personal data: the user id is NULL when
-- pseudonymized).
CREATE TABLE post_mentions (
    post_id         bigint NOT NULL,
    author_key      bytea NOT NULL,
    user_id         bigint,
    offsets         int[] NOT NULL DEFAULT '{}',
    first_seen_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (post_id, author_key)
);
CREATE INDEX post_mentions_author_idx ON post_mentions (author_key);

-- Engagement history: a row only when a count changed. Daily partitions.
CREATE TABLE post_metrics (
    post_id        bigint NOT NULL,
    captured_at    timestamptz NOT NULL,
    user_id        bigint NOT NULL,
    play_count     bigint,
    like_count     bigint,
    comment_count  bigint,
    share_count    bigint,
    save_count     bigint,
    repost_count   bigint,
    PRIMARY KEY (post_id, captured_at)
) PARTITION BY RANGE (captured_at);

-- Sounds, shared by many posts.
CREATE TABLE music (
    music_id        bigint PRIMARY KEY,
    title           text,
    author_name     text,     -- personal data for original sounds
    original        boolean,
    duration_ms     bigint,
    album           text,
    cover_media_id  uuid,
    copyrighted     boolean,
    commerce        boolean,
    content_hash    bytea,
    first_seen_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at    timestamptz NOT NULL DEFAULT now()
);

-- Comments and replies (depth 1), partitioned like their posts. -----------------
CREATE TABLE comments (
    post_created_at      timestamptz NOT NULL,
    post_id              bigint NOT NULL,
    comment_id           bigint NOT NULL,
    user_id              bigint NOT NULL,   -- the post's account
    parent_comment_id    bigint,            -- a reply's top-level comment (reply_id)
    reply_to_comment_id  bigint,            -- the reply a reply answers (reply_to_reply_id)
    depth                smallint NOT NULL,
    author_key           bytea,
    author_user_id       bigint,            -- NULL when pii_mode = pseudonymized or erased
    text                 text,
    text_ranges          jsonb,
    created_at           timestamptz,
    like_count           bigint,
    reply_count          bigint,
    liked_by_creator     boolean,           -- is_author_digged
    pinned_by_creator    boolean,           -- author_pin
    stick_position       int,
    has_images           boolean,
    image_media_ids      uuid[],
    labels               text[],
    language             text,
    status               text NOT NULL DEFAULT 'visible',  -- visible | deleted
    redacted             boolean NOT NULL DEFAULT false,
    content_hash         bytea NOT NULL,
    edit_count           int NOT NULL DEFAULT 0,
    edited_at            timestamptz,
    deleted_at           timestamptz,
    first_seen_at        timestamptz NOT NULL,
    last_seen_at         timestamptz NOT NULL,
    PRIMARY KEY (post_created_at, post_id, comment_id)
) PARTITION BY RANGE (post_created_at);
CREATE INDEX comments_thread_idx ON comments (post_id, created_at, comment_id);
CREATE INDEX comments_author_idx ON comments (author_key);
CREATE INDEX comments_parent_idx ON comments (post_id, parent_comment_id) WHERE parent_comment_id IS NOT NULL;

-- Comment authors and mentioned users (identifying columns NULL when
-- pseudonymized). TikTok's inferences about people (predicted_age_group)
-- are never stored (ADR 0007 §6).
CREATE TABLE people (
    author_key     bytea PRIMARY KEY,
    user_id        bigint,
    sec_uid        text,
    username       text,
    nickname       text,
    avatar_url     text,
    verified       boolean,
    first_seen_at  timestamptz NOT NULL,
    last_seen_at   timestamptz NOT NULL
);
CREATE INDEX people_user_idx ON people (user_id);
CREATE INDEX people_sec_uid_idx ON people (sec_uid);
CREATE INDEX people_username_idx ON people (lower(username));
CREATE INDEX people_seen_idx ON people (last_seen_at);

-- Media (metadata always; the lifecycle columns are fbs's, under the same
-- names, for downloads behind limits.include_media) -------------------------
CREATE TABLE media (
    media_id           uuid PRIMARY KEY,
    source_media_id    text NOT NULL,   -- emptied first when a tombstone is scrubbed
    kind               text NOT NULL,   -- cover | origin_cover | dynamic_cover | video | photo | avatar | music_cover | comment_image
    variant            text NOT NULL,   -- original | sd | hd | thumbnail
    user_id            bigint,
    post_id            bigint,
    music_id           bigint,
    comment_id         bigint,
    position           int,
    author_key         bytea,           -- a person's avatar or comment image
    source_url         text,
    source_expires_at  timestamptz,
    width              int,
    height             int,
    duration_ms        bigint,
    bitrate            bigint,
    mime               text,
    post_created_at    timestamptz,
    status             text NOT NULL DEFAULT 'discovered',
        -- discovered | stored | ready | deleted | failed | expired | source_expired | skipped
    s3_bucket          text,
    s3_key             text,
    size_bytes         bigint,
    sha256             bytea,
    attempts           int NOT NULL DEFAULT 0,
    retryable_failures int NOT NULL DEFAULT 0,
    last_error         text,
    delete_after       timestamptz,
    discovered_at      timestamptz NOT NULL DEFAULT now(),
    stored_at          timestamptz,
    ready_at           timestamptz,
    processed_at       timestamptz,
    deleted_at         timestamptz
);
CREATE INDEX media_post_idx ON media (post_id);
CREATE INDEX media_user_idx ON media (user_id);
CREATE INDEX media_ready_idx ON media (delete_after) WHERE status = 'ready';
CREATE INDEX media_comment_idx ON media (comment_id) WHERE comment_id IS NOT NULL;
CREATE INDEX media_author_idx ON media (author_key) WHERE author_key IS NOT NULL;
CREATE INDEX media_unscrubbed_idx ON media (discovered_at) WHERE status IN ('deleted', 'expired') AND source_media_id <> '';

CREATE TABLE media_acks (
    media_id  uuid NOT NULL REFERENCES media (media_id) ON DELETE CASCADE,
    consumer  text NOT NULL,
    status    text NOT NULL,
    error     text,
    acked_at  timestamptz NOT NULL,
    PRIMARY KEY (media_id, consumer)
);

-- Machinery: fbs's tables and columns, Facebook names renamed (ADR 0007 §4).
-- Crawl state -----------------------------------------------------------------
CREATE TABLE crawl_cursors (
    scope_type           text NOT NULL,   -- account_list
    scope_id             bigint NOT NULL,
    ordering             text NOT NULL DEFAULT 'default',
    cursor               text,
    newest_seen_at       timestamptz,
    oldest_seen_at       timestamptz,
    state                text NOT NULL,   -- complete | partial | truncated
    truncated_reason     text,
    last_full_at         timestamptz,
    last_incremental_at  timestamptz,
    updated_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope_type, scope_id, ordering)
);

CREATE TABLE api_clients (
    id               uuid PRIMARY KEY,
    name             text NOT NULL,
    key_prefix       text NOT NULL UNIQUE,
    key_hash         bytea NOT NULL,
    scopes           text[] NOT NULL,
    rate_per_minute  int,
    created_at       timestamptz NOT NULL DEFAULT now(),
    revoked_at       timestamptz
);

CREATE TABLE jobs (
    job_id           uuid PRIMARY KEY,
    api_client_id    uuid REFERENCES api_clients (id),
    kind             text NOT NULL,   -- on_demand | scheduled | backfill
    lane             smallint NOT NULL,
    mode             text NOT NULL,   -- full | incremental
    account_ref      text NOT NULL,   -- u:<username> | sec:<secUid> | id:<user id>
    user_id          bigint,
    params           jsonb NOT NULL,
    status           text NOT NULL,   -- queued | running | finalizing | completed | partial | failed | cancelled
    idempotency_key  text,
    counters         jsonb,
    completeness     jsonb,
    error            jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    dispatched_at    timestamptz,
    finished_at      timestamptz
);
CREATE UNIQUE INDEX jobs_idem_idx ON jobs (api_client_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_account_idx ON jobs (account_ref, created_at DESC);
CREATE INDEX jobs_status_idx ON jobs (status, created_at) WHERE status IN ('queued', 'running', 'finalizing');

-- Scheduled checks, one row per normalised account reference. The default
-- interval is tt.freshness.list_interval (2 h 45 min, build brief D2);
-- adaptive accounts move between scheduler tiers by posting rate.
CREATE TABLE account_schedule (
    account_ref           text PRIMARY KEY,
    user_id               bigint,
    enabled               boolean NOT NULL DEFAULT true,
    tier                  text NOT NULL DEFAULT 'T2',
    interval_seconds      int NOT NULL,
    next_run_at           timestamptz NOT NULL,
    options               jsonb NOT NULL DEFAULT '{}',
    max_age_days          int,
    max_posts             bigint,
    consecutive_failures  int NOT NULL DEFAULT 0,
    paused_until          timestamptz,
    last_job_id           uuid,
    last_run_at           timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    adaptive              boolean NOT NULL DEFAULT true,
    posts_per_day         double precision,
    last_post_at          timestamptz,
    tier_changed_at       timestamptz
);
CREATE INDEX account_schedule_due_idx ON account_schedule (next_run_at) WHERE enabled;
CREATE INDEX account_schedule_user_idx ON account_schedule (user_id);

CREATE TABLE account_registrations (
    api_client_id  uuid NOT NULL REFERENCES api_clients (id),
    account_ref    text NOT NULL REFERENCES account_schedule (account_ref) ON DELETE CASCADE,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (api_client_id, account_ref)
);

-- Comment re-reads of posts past the list's reach, by post age (ADR 0007
-- §4: only when the comment count went up).
CREATE TABLE post_refresh (
    post_id             bigint PRIMARY KEY,
    user_id             bigint NOT NULL,
    post_created_at     timestamptz NOT NULL,
    next_refresh_at     timestamptz NOT NULL,
    last_refresh_at     timestamptz,
    refreshes           int NOT NULL DEFAULT 0,
    unchanged           int NOT NULL DEFAULT 0,
    last_comment_count  bigint,
    recrawls            int NOT NULL DEFAULT 0
);
CREATE INDEX post_refresh_due_idx ON post_refresh (user_id, next_refresh_at);

-- GDPR ------------------------------------------------------------------------
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

CREATE TABLE suppression (
    subject_type  text NOT NULL,   -- author | account
    subject_key   text NOT NULL,   -- hex author_key, or the account's user id
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_type, subject_key)
);

-- The operation registry (fbs's fb_queries and fb_client_params under
-- tks's names; ADR 0006 §3): doc_id holds the variant label (creator, web,
-- web-signed). Seeded from configs/tt/operations.seed.yaml.
CREATE TABLE operations (
    name                text NOT NULL,
    doc_id              text NOT NULL,
    friendly_name       text NOT NULL,
    variables_template  jsonb NOT NULL,
    parser              text NOT NULL,
    parser_version      int NOT NULL,
    status              text NOT NULL,  -- candidate | active | fallback | retired
    priority            int NOT NULL DEFAULT 0,
    source              text NOT NULL,  -- seed | canary | manual
    method              text NOT NULL DEFAULT '',
    endpoint            text NOT NULL DEFAULT '',
    headers             jsonb NOT NULL DEFAULT '[]',
    discovered_at       timestamptz NOT NULL DEFAULT now(),
    promoted_at         timestamptz,
    last_ok_at          timestamptz,
    last_checked_at     timestamptz,
    last_check          text,           -- ok | stale | drift | inconclusive: <reason>
    check_failures      int NOT NULL DEFAULT 0,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, doc_id)
);

CREATE TABLE client_params (
    key         text PRIMARY KEY,
    value       text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- One row per canary run (tks-canary, M8), for operators and the gauges.
CREATE TABLE canary_runs (
    run_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL,
    status       text NOT NULL,     -- ok | degraded | error
    report       jsonb NOT NULL
);
CREATE INDEX canary_runs_started_idx ON canary_runs (started_at DESC);

-- +goose Down
DROP TABLE canary_runs;
DROP TABLE client_params;
DROP TABLE operations;
DROP TABLE suppression;
DROP TABLE erasure_requests;
DROP TABLE post_refresh;
DROP TABLE account_registrations;
DROP TABLE account_schedule;
DROP TABLE jobs;
DROP TABLE api_clients;
DROP TABLE crawl_cursors;
DROP TABLE media_acks;
DROP TABLE media;
DROP TABLE people;
DROP TABLE comments;
DROP TABLE music;
DROP TABLE post_metrics;
DROP TABLE post_mentions;
DROP TABLE post_index;
DROP TABLE posts;
DROP TABLE account_snapshots;
DROP TABLE account_usernames;
DROP TABLE accounts;
