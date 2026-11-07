-- +goose Up
-- Leases for leader election and the canary's single run (internal/platform/
-- leader). Production reaches PostgreSQL through PgBouncer in transaction
-- mode (the master, 2026-11-07), where a session advisory lock stays on a
-- pooled server session other clients then use: two leaders. A lease is one
-- row per name, taken or renewed by one statement when it is free, ours, or
-- expired (until in the past), and renewed every few seconds by its holder.
CREATE TABLE leases (
    name    text PRIMARY KEY,
    holder  text NOT NULL,
    until   timestamptz NOT NULL
);

-- +goose Down
DROP TABLE leases;
