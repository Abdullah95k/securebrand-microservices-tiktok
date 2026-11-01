-- +goose Up
-- The scheduler (M6a; docs/tiktok ADR 0007 §12).
--
-- walks: when each metrics walk (tt.freshness.metrics_walks, keyed by its
-- depth in seconds) last ended well for the account; a backfill counts for
-- every walk. last_success_at: the last check that ended completed or
-- partial, for the oldest-check gauge (build brief §8 A1).
ALTER TABLE account_schedule
    ADD COLUMN walks jsonb NOT NULL DEFAULT '{}',
    ADD COLUMN last_success_at timestamptz;
CREATE INDEX account_schedule_success_idx ON account_schedule (last_success_at) WHERE enabled;

-- +goose Down
DROP INDEX account_schedule_success_idx;
ALTER TABLE account_schedule DROP COLUMN last_success_at, DROP COLUMN walks;
