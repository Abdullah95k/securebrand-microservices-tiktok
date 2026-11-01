-- +goose Up
-- The author of a comment's media, so an author erasure finds media whose
-- comment isn't stored yet; and the indexes erasures look media up by.
ALTER TABLE media ADD COLUMN author_key bytea;
CREATE INDEX media_author_idx ON media (author_key) WHERE author_key IS NOT NULL;
CREATE INDEX media_page_idx ON media (page_id);

-- +goose Down
DROP INDEX media_page_idx;
DROP INDEX media_author_idx;
ALTER TABLE media DROP COLUMN author_key;
