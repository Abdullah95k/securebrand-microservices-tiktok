-- +goose Up
-- Edits and deletions of comments (status 'deleted' is set when a full
-- re-read of the post no longer returns a known comment).
ALTER TABLE comments
    ADD COLUMN edit_count integer NOT NULL DEFAULT 0,
    ADD COLUMN edited_at  timestamptz,
    ADD COLUMN deleted_at timestamptz;

-- +goose Down
ALTER TABLE comments
    DROP COLUMN deleted_at,
    DROP COLUMN edited_at,
    DROP COLUMN edit_count;
