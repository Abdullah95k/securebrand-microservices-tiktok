-- +goose Up
-- The delay before a new post is stored (M6c; build brief §8 A2): when the
-- post's row was first written (queryable), and the mode of the read that
-- first found it (full for a backfill, whose posts are not new; incremental
-- for a scheduled check; refresh for a metrics walk standing in for one).
-- stored_at - created_at is the delay. Rows written before this migration
-- keep both NULL.
ALTER TABLE posts
    ADD COLUMN stored_at timestamptz,
    ADD COLUMN first_mode text;

-- +goose Down
ALTER TABLE posts DROP COLUMN first_mode, DROP COLUMN stored_at;
