-- +goose Up

-- Adaptive page tiers: the scheduler moves each page between tiers by how
-- often it posts. Pages registered with an explicit interval keep it
-- (adaptive = false).
ALTER TABLE page_schedule
    ADD COLUMN adaptive       boolean NOT NULL DEFAULT true,
    ADD COLUMN posts_per_day  double precision,
    ADD COLUMN last_post_at   timestamptz,
    ADD COLUMN tier_changed_at timestamptz;

-- Comment refresh of posts older than the timeline overlap. Due refreshes
-- ride on the page's next scheduled job as extra post tasks.
CREATE TABLE post_refresh (
    post_id             bigint PRIMARY KEY,
    page_id             bigint NOT NULL,
    post_created_at     timestamptz NOT NULL,
    next_refresh_at     timestamptz NOT NULL,
    last_refresh_at     timestamptz,
    refreshes           int NOT NULL DEFAULT 0,
    unchanged           int NOT NULL DEFAULT 0,   -- consecutive refreshes that found nothing new
    last_comment_count  bigint,                   -- comments stored at the last refresh
    recrawls            int NOT NULL DEFAULT 0    -- full recrawls done (scheduler.post_refresh.recrawl_at)
);
CREATE INDEX post_refresh_due_idx ON post_refresh (page_id, next_refresh_at);

-- +goose Down
DROP TABLE post_refresh;
ALTER TABLE page_schedule
    DROP COLUMN tier_changed_at,
    DROP COLUMN last_post_at,
    DROP COLUMN posts_per_day,
    DROP COLUMN adaptive;
