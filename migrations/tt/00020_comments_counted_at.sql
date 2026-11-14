-- +goose Up
-- When the finalizer last counted a post's stored comments (coverage).
-- comments_synced_at stays the time of the last comment read; a post read
-- again within scheduler.coverage_recount_after keeps the counts it has
-- (2026-11-14: recounting every read post's comments on each check took
-- 40-65 s a statement). NULL: the last count was at comments_synced_at.
ALTER TABLE posts ADD COLUMN comments_counted_at timestamptz;

-- +goose Down
ALTER TABLE posts DROP COLUMN comments_counted_at;
