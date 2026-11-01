-- +goose Up
-- A media whose source URL expired before it was handed over is now
-- 'source_expired' (downloaded when a later scrape brings a fresh URL);
-- 'expired' is kept for media handed over whose TTL ran out. Rows marked
-- before the split were never ready (some were stored, then overwritten
-- by a failed duplicate download).
UPDATE media SET status = 'source_expired' WHERE status = 'expired' AND ready_at IS NULL;
-- Media handed over stays as a tombstone after media_rows_days, without
-- what identifies it (fb_media_id emptied first among them): the rows
-- retention still has to clear.
CREATE INDEX media_unscrubbed_idx ON media (discovered_at) WHERE status IN ('deleted', 'expired') AND fb_media_id <> '';

-- +goose Down
DROP INDEX media_unscrubbed_idx;
UPDATE media SET status = 'expired' WHERE status = 'source_expired';
