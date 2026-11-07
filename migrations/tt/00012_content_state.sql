-- +goose Up
-- The shared contract's change gate (M15; docs/tiktok/contract-mapping.md
-- §6): one row per event stream the persister publishes on content.*, so
-- an event goes out only when the contract's content hash changed (and,
-- for counts, at least once a day), never for an older observation than
-- the last one sent. id is "<message kind>/<contract id>", for example
-- "post/tt:post:7410000000000000001" or "metrics/tt:comment:…". text_hash
-- is the SHA-256 of a post's caption or a comment's text, and edited_at
-- the first observation with a different text: the contract's
-- flags.edited and edited_at. Kept for good, like everything (D8); hash
-- partitions, as follows, since comments make it large.
CREATE TABLE content_state (
    id           text NOT NULL PRIMARY KEY,
    content_hash bytea NOT NULL,
    text_hash    bytea,
    observed_at  timestamptz NOT NULL,
    edited_at    timestamptz
) PARTITION BY HASH (id);
CREATE TABLE content_state_p00 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE content_state_p01 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE content_state_p02 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE content_state_p03 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE content_state_p04 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE content_state_p05 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE content_state_p06 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE content_state_p07 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE content_state_p08 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE content_state_p09 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE content_state_p10 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE content_state_p11 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE content_state_p12 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE content_state_p13 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE content_state_p14 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE content_state_p15 PARTITION OF content_state FOR VALUES WITH (MODULUS 16, REMAINDER 15);

-- +goose Down
DROP TABLE content_state;
