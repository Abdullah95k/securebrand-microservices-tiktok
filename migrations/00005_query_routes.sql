-- +goose Up

-- A query's endpoint and extra request headers, for platforms with more than
-- one GraphQL endpoint (Instagram: /api/graphql and /graphql/query). Empty
-- means the client's default endpoint and headers, so existing rows keep
-- working unchanged.
ALTER TABLE fb_queries
    ADD COLUMN endpoint text NOT NULL DEFAULT '',
    ADD COLUMN headers  jsonb NOT NULL DEFAULT '[]';  -- [{"name": ..., "value": ...}], in send order

-- +goose Down
ALTER TABLE fb_queries
    DROP COLUMN headers,
    DROP COLUMN endpoint;
