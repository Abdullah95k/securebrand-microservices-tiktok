-- +goose Up
-- Posts that are no longer public (M8b; docs/tiktok/discovery-plan.md §17):
-- a list read covering a post's date no longer returned it, and its page,
-- read once by plain HTTP, confirmed it. Its status becomes 'unavailable'
-- (never 'deleted': a logged-out reader cannot tell a deletion from a post
-- made private or removed), with the statusCode the page answered and the
-- region of the exit that read it. Nothing is deleted (D8): the row keeps
-- its content, and a post that comes back is 'visible' again.
ALTER TABLE posts
    ADD COLUMN unavailable_since timestamptz,
    ADD COLUMN status_code bigint,
    ADD COLUMN status_region text;

-- Every change of a post's status, oldest first: the history a post that
-- went and came back keeps.
CREATE TABLE post_status_changes (
    post_id      bigint NOT NULL,
    changed_at   timestamptz NOT NULL,   -- the observation that saw the new status
    status       text NOT NULL,          -- visible | unavailable | private
    previous     text NOT NULL,
    status_code  bigint,
    region       text,
    PRIMARY KEY (post_id, changed_at, status)
);

-- +goose Down
DROP TABLE post_status_changes;
ALTER TABLE posts DROP COLUMN status_region, DROP COLUMN status_code, DROP COLUMN unavailable_since;
