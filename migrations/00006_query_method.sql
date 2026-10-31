-- +goose Up

-- A query's HTTP method, for platforms whose web API is not a GraphQL POST
-- (TikTok's GET calls, docs/tiktok ADR 0006 §3). Empty means the request
-- builder's default (POST for GraphQL), so existing rows keep working
-- unchanged.
ALTER TABLE fb_queries
    ADD COLUMN method text NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE fb_queries
    DROP COLUMN method;
