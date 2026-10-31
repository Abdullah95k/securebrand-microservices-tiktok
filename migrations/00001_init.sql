-- +goose Up

-- Pages ---------------------------------------------------------------------
CREATE TABLE pages (
    page_id          bigint PRIMARY KEY,
    vanity           text,
    url              text,
    name             text,
    categories       text[],
    intro            text,
    followers_count  bigint,
    likes_count      bigint,
    verified         boolean,
    profile_pic_url  text,
    cover_url        text,
    contact          jsonb,
    extra            jsonb,
    status           text NOT NULL DEFAULT 'active',  -- active | unavailable | restricted | suppressed
    content_hash     bytea,
    first_seen_at    timestamptz NOT NULL DEFAULT now(),
    last_scraped_at  timestamptz
);
CREATE UNIQUE INDEX pages_vanity_idx ON pages (lower(vanity)) WHERE vanity IS NOT NULL;

-- Profile time series, monthly partitions.
CREATE TABLE page_snapshots (
    page_id          bigint NOT NULL,
    captured_at      timestamptz NOT NULL,
    name             text,
    followers_count  bigint,
    likes_count      bigint,
    verified         boolean,
    PRIMARY KEY (page_id, captured_at)
) PARTITION BY RANGE (captured_at);

-- Posts: monthly partitions on the post's creation time, so a post and its
-- comments share a partition and retention is a DROP. -----------------------
CREATE TABLE posts (
    post_created_at          timestamptz NOT NULL,
    post_id                  bigint NOT NULL,
    page_id                  bigint NOT NULL,
    story_id                 text,
    permalink                text,
    post_type                text NOT NULL,
    text                     text,
    text_ranges              jsonb,
    hashtags                 text[],
    links                    jsonb,
    shared_post_id           bigint,
    shared_post_url          text,
    is_pinned                boolean,
    is_paid_partnership      boolean,
    author_id                text,
    author_name              text,
    reaction_count           bigint,
    reactions                jsonb,
    reactions_complete       boolean,
    comment_count_total      bigint,
    share_count              bigint,
    view_count               bigint,
    feedback_id              text,
    media_ids                text[],
    extra                    jsonb,
    -- Coverage, filled by the job finalizer.
    comments_collected       bigint,
    replies_collected        bigint,
    completeness             real,
    comments_state           text NOT NULL DEFAULT 'none', -- none | partial | complete | truncated
    comments_synced_at       timestamptz,
    content_hash             bytea NOT NULL,
    status                   text NOT NULL DEFAULT 'visible',
    first_seen_at            timestamptz NOT NULL,
    last_seen_at             timestamptz NOT NULL,
    PRIMARY KEY (post_created_at, post_id)
) PARTITION BY RANGE (post_created_at);
CREATE INDEX posts_page_idx ON posts (page_id, post_created_at DESC);
CREATE INDEX posts_hashtags_idx ON posts USING gin (hashtags);

-- post_id -> partition key, so lookups by post id prune to one partition.
CREATE TABLE post_index (
    post_id          bigint PRIMARY KEY,
    page_id          bigint NOT NULL,
    post_created_at  timestamptz NOT NULL
);
CREATE INDEX post_index_page_idx ON post_index (page_id, post_created_at DESC);

-- Comments and replies (depth > 0), partitioned like posts. -------------------
CREATE TABLE comments (
    post_created_at    timestamptz NOT NULL,
    post_id            bigint NOT NULL,
    comment_id         bigint NOT NULL,
    page_id            bigint NOT NULL,
    graph_id           text,
    parent_comment_id  bigint,
    root_comment_id    bigint,
    depth              smallint NOT NULL,
    author_key         bytea,
    author_id          text,      -- NULL when pii_mode = pseudonymized or erased
    text               text,
    text_ranges        jsonb,
    attachments        jsonb,
    created_at         timestamptz,
    edited             boolean,
    is_op              boolean,
    reaction_count     bigint,
    reactions          jsonb,
    reply_count        bigint,
    feedback_id        text,
    url                text,
    extra              jsonb,
    status             text NOT NULL DEFAULT 'visible',
    redacted           boolean NOT NULL DEFAULT false,
    content_hash       bytea NOT NULL,
    first_seen_at      timestamptz NOT NULL,
    last_seen_at       timestamptz NOT NULL,
    PRIMARY KEY (post_created_at, post_id, comment_id)
) PARTITION BY RANGE (post_created_at);
CREATE INDEX comments_thread_idx ON comments (post_id, created_at, comment_id);
CREATE INDEX comments_author_idx ON comments (author_key);
CREATE INDEX comments_parent_idx ON comments (post_id, parent_comment_id) WHERE parent_comment_id IS NOT NULL;

CREATE TABLE comment_authors (
    author_key     bytea PRIMARY KEY,
    author_id      text,
    name           text,
    profile_url    text,
    avatar_url     text,
    author_type    text,
    verified       boolean,
    first_seen_at  timestamptz NOT NULL,
    last_seen_at   timestamptz NOT NULL
);
CREATE INDEX comment_authors_url_idx ON comment_authors (profile_url);
CREATE INDEX comment_authors_id_idx ON comment_authors (author_id);
CREATE INDEX comment_authors_seen_idx ON comment_authors (last_seen_at);

-- Engagement time series: a row only when values changed. Daily partitions.
CREATE TABLE post_metrics (
    post_id         bigint NOT NULL,
    captured_at     timestamptz NOT NULL,
    page_id         bigint NOT NULL,
    reaction_count  bigint,
    reactions       jsonb,
    comment_count   bigint,
    share_count     bigint,
    view_count      bigint,
    PRIMARY KEY (post_id, captured_at)
) PARTITION BY RANGE (captured_at);

-- Media (operational state of temporary downloads) -------------------------
CREATE TABLE media (
    media_id           uuid PRIMARY KEY,
    fb_media_id        text NOT NULL,
    variant            text NOT NULL,
    kind               text NOT NULL,
    owner_type         text NOT NULL,
    owner_id           bigint,
    post_id            bigint,
    page_id            bigint,
    comment_id         bigint,
    source_url         text,
    source_expires_at  timestamptz,
    width              int,
    height             int,
    duration_ms        bigint,
    mime               text,
    alt_text           text,
    status             text NOT NULL DEFAULT 'discovered',
        -- discovered | stored | ready | deleted | failed | expired | skipped
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
CREATE INDEX media_ready_idx ON media (delete_after) WHERE status = 'ready';
CREATE INDEX media_comment_idx ON media (comment_id) WHERE comment_id IS NOT NULL;

CREATE TABLE media_acks (
    media_id  uuid NOT NULL REFERENCES media (media_id) ON DELETE CASCADE,
    consumer  text NOT NULL,
    status    text NOT NULL,
    error     text,
    acked_at  timestamptz NOT NULL,
    PRIMARY KEY (media_id, consumer)
);

-- Crawl state ----------------------------------------------------------------
CREATE TABLE crawl_cursors (
    scope_type           text NOT NULL,   -- page_timeline
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
    id          uuid PRIMARY KEY,
    name        text NOT NULL,
    key_prefix  text NOT NULL UNIQUE,
    key_hash    bytea NOT NULL,
    scopes      text[] NOT NULL,
    rate_per_minute int,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);

CREATE TABLE jobs (
    job_id           uuid PRIMARY KEY,
    api_client_id    uuid REFERENCES api_clients (id),
    kind             text NOT NULL,   -- on_demand | scheduled | backfill
    lane             smallint NOT NULL,
    mode             text NOT NULL,   -- full | incremental
    page_ref         text NOT NULL,
    page_id          bigint,
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
CREATE INDEX jobs_page_idx ON jobs (page_ref, created_at DESC);
CREATE INDEX jobs_status_idx ON jobs (status, created_at) WHERE status IN ('queued', 'running', 'finalizing');

-- Scheduled re-scrapes, one row per normalised page reference.
CREATE TABLE page_schedule (
    page_ref              text PRIMARY KEY,
    page_id               bigint,
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
    created_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX page_schedule_due_idx ON page_schedule (next_run_at) WHERE enabled;

CREATE TABLE page_registrations (
    api_client_id  uuid NOT NULL REFERENCES api_clients (id),
    page_ref       text NOT NULL REFERENCES page_schedule (page_ref) ON DELETE CASCADE,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (api_client_id, page_ref)
);

-- GDPR ----------------------------------------------------------------------
CREATE TABLE erasure_requests (
    id            uuid PRIMARY KEY,
    api_client_id uuid REFERENCES api_clients (id),
    subject_type  text NOT NULL,   -- author | page
    author_key    bytea,
    page_id       bigint,
    input         jsonb NOT NULL,
    status        text NOT NULL,   -- pending | done | failed
    stats         jsonb,
    error         text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz
);
CREATE INDEX erasure_pending_idx ON erasure_requests (created_at) WHERE status = 'pending';

CREATE TABLE suppression (
    subject_type  text NOT NULL,   -- author | page
    subject_key   text NOT NULL,   -- hex author_key or page id
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_type, subject_key)
);

-- Facebook query registry (hot-reloaded by crawlers) -----------------------
CREATE TABLE fb_queries (
    name                text NOT NULL,
    doc_id              text NOT NULL,
    friendly_name       text NOT NULL,
    variables_template  jsonb NOT NULL,
    parser              text NOT NULL,
    parser_version      int NOT NULL,
    status              text NOT NULL,  -- candidate | active | fallback | retired
    priority            int NOT NULL DEFAULT 0,
    source              text NOT NULL,  -- seed | bundle | har | manual
    discovered_at       timestamptz NOT NULL DEFAULT now(),
    promoted_at         timestamptz,
    last_ok_at          timestamptz,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, doc_id)
);

CREATE TABLE fb_client_params (
    key         text PRIMARY KEY,
    value       text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE fb_client_params;
DROP TABLE fb_queries;
DROP TABLE suppression;
DROP TABLE erasure_requests;
DROP TABLE page_registrations;
DROP TABLE page_schedule;
DROP TABLE jobs;
DROP TABLE api_clients;
DROP TABLE crawl_cursors;
DROP TABLE media_acks;
DROP TABLE media;
DROP TABLE post_metrics;
DROP TABLE comment_authors;
DROP TABLE comments;
DROP TABLE post_index;
DROP TABLE posts;
DROP TABLE page_snapshots;
DROP TABLE pages;
