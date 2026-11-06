-- +goose Up
-- The discovery review page (M14; docs/tiktok/discovery-plan.md §11, ADR
-- 0007 §22). Indexes only: the page reads what discovery already stores.

-- Accounts found by a source and what led to it: the review page's filter
-- by keyword (how = 'search', via = the keyword's id) and its hits per
-- keyword.
CREATE INDEX account_sources_via_idx ON account_sources (how, via);

-- Jobs by kind and day: the review page's requests and data per discovery
-- source and day, without reading monitoring's jobs (kept for good, D8).
CREATE INDEX jobs_kind_created_idx ON jobs (kind, created_at);

-- Found accounts added, by day: the review page's accounts added per source
-- and day.
CREATE INDEX discovery_candidates_added_idx ON discovery_candidates (added_at) WHERE added_at IS NOT NULL;

-- +goose Down
DROP INDEX discovery_candidates_added_idx;
DROP INDEX jobs_kind_created_idx;
DROP INDEX account_sources_via_idx;
