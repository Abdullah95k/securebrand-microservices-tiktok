# ADR 0005: igs data model and contracts

Status: proposed (M0; accepted when the owner merges it)

## Context
igs collects what logged-out Instagram serves: profiles, feed posts,
carousels, reels, post metrics, top-level comments, media metadata and
highlights (build brief D1). Replies, current stories, view and play counts,
and follower and liker lists need login and are out of scope, but the schema
must leave room for replies and tagged posts. Profiles and commenters are both
personal data (D8). The analysis service comes later (D10), so igs publishes
its own `ig.*.v1` topics and keeps everything queryable in Postgres. In
testing, igs shares fbs's Kafka, Postgres 18, Valkey and S3 (D6); production
may share them or not, so the model must work either way.

What the data looks like comes from the owner's recordings
([`har-findings-2026-10-30.md`](../instagram/har-findings-2026-10-30.md)):

* Every entity has a numeric `pk` (users, media, carousel children, comments).
  Media also has a `code`, the shortcode in `/p/<code>/` and `/reel/<code>/`
  URLs. Usernames can change (D8).
* Grid items carry only `pk`, `code`, type, caption and a thumbnail.
  `taken_at`, like and comment counts, renditions, carousel children,
  usertags, location, collab co-authors and music come from the post page.
* `like_count` can be hidden (`like_and_view_counts_disabled`). No view or
  play count is served.
* Comments are top level only (`parent_comment_id` always null,
  `child_comment_count` null), 10 per page, newest first. A read that ends
  with `has_next_page: false` has every top-level comment; `comment_count`
  also counts the hidden replies, so it is larger.

The fbs contract rules carry over unchanged
([kafka-schemas.md](../contracts/kafka-schemas.md)): add-only protobuf,
`buf breaking` in CI, Schema Registry subjects under BACKWARD_TRANSITIVE.

## Decision

### 1. Identifiers
| Entity | Key | Notes |
|---|---|---|
| Profile | user `pk` (int64) | Never the username. API and schedule refs are `u:<username>` or `id:<pk>`, resolved to the pk on first load; renames are tracked (§4) |
| Post (feed, carousel, reel) | media `pk` (int64) | `shortcode` is unique and indexed; the API accepts either |
| Carousel child | its own media `pk` | Stored as media rows with `parent_media_pk` and `position` |
| Comment | comment `pk` (int64) | |
| Highlight | highlight id (int64, from `highlight:<id>`) | Items keyed by their story media `pk` |
| Media file row | UUIDv5 of `<pk>|<variant>` in an igs namespace | Same scheme as fbs, different namespace UUID, so ids never collide |
| Person (author or profile) | `author_key = HMAC-SHA256(ig secret, "ig:user:" + pk)` | Domain-separated from fbs's `HMAC(secret, author id)`, and a separate secret (`privacy.author_hmac_key` in igs's config) |

Pks are stored as int64. Media pks are 19-digit numbers, below int64's
9.2×10¹⁸ limit (reference chapter 7 §4.1); M5 confirms the range against the
scrubbed HAR fixtures, and a larger pk would make the columns `numeric(20)`.
Every profile row also stores its `author_key`, so the
analysis can relate a profile to the comments its owner writes elsewhere
without names (D8).

### 2. Proto packages
* **`securebrand.common.v1`** (new, used by new code only): `Platform`
  (`FACEBOOK`, `INSTAGRAM`), `CrawlMode`, `Observation`, `ImageRef`,
  `S3Object`, the media handoff (`MediaReady`, `MediaProcessed`,
  `MediaDeleted` and their enums, with `platform` and neutral ids:
  `source_media_id`, `account_id`, `post_id`, `comment_id`), `JobEvent` and
  `Completeness` (with `subject_ref`/`subject_id`), and `ErasureEvent`
  (subjects `AUTHOR`, `ACCOUNT`). fbs's `securebrand.fb.v1` is not changed:
  it lives in fbs's repository, and moving a message out of it would break
  its contract. The copy of it in the igs repository goes with the copied
  Facebook code (ADR 0004 §2).
* **`securebrand.ig.v1`**: `Profile`, `Post` (with `children`, `usertags`,
  `coauthors`, `location`, `music`), `PostMetrics`, `Comment`, `Media`,
  `Highlight` (with items). Fields Instagram may stop serving are optional or
  nullable. Room for later, without a breaking change: `Comment` has
  `parent_comment_pk` and `depth` from the start; `Post` has
  `discovered_via` (`GRID` now; `TAGGED` and `REELS` reserved); further counts
  arrive as new fields.
* **`securebrand.igs.task.v1`** (internal, never public): igs's task envelope.
  It keeps fbs's generic fields with the same names and numbers (task and
  lineage ids, `seq`, `job_id`, `lane`, `kind`, `mode`, `cursor`, `limits`,
  `attempt`, `stalls`, …), since igs's worker is a copy of fbs's (ADR 0004).
  What is Instagram's own is disjoint from fbs's: kind values start at 100
  (`PROFILE`, `POST`, `MEDIA`; `REPLIES` reserved) and scope fields at
  number 20. A task that lands on the other service's topic then decodes with
  an unknown kind and no scope, and is dead-lettered instead of running as
  the wrong task. `PostScope` carries `items_seen` across continuations,
  which fixes fbs's per-segment `max_comments_per_post` for igs from the
  start.

### 3. Topics
Public contract (keys keep a post's records in order):

| Topic | Key | Value |
|---|---|---|
| `ig.profile.v1` | user pk | `securebrand.ig.v1.Profile` |
| `ig.post.v1` | media pk | `securebrand.ig.v1.Post` |
| `ig.post_metrics.v1` | media pk | `securebrand.ig.v1.PostMetrics` |
| `ig.comment.v1` | media pk | `securebrand.ig.v1.Comment` |
| `ig.media.v1` | media id | `securebrand.ig.v1.Media` |
| `ig.highlight.v1` | user pk | `securebrand.ig.v1.Highlight` |
| `ig.media.ready.v1`, `ig.media.processed.v1`, `ig.media.deleted.v1` | media id | `securebrand.common.v1.Media{Ready,Processed,Deleted}` |
| `ig.job.v1` | job id | `securebrand.common.v1.JobEvent` |
| `ig.erasure.v1` | erasure request id | `securebrand.common.v1.ErasureEvent` |

Internal: `igs.task.profile.{p0,p1,p2}`, `igs.task.post.{p0,p1,p2}`,
`igs.task.media`, `igs.task.retry.{30s,5m}`, `igs.task.dlq`, and the
persister's quarantine topic `igs.persist.dlq` (brief §5 item 2). The wire
format is fbs's: plain protobuf, the full message name in a
type header (`igs-type`), `content-type: application/x-protobuf`, subjects
`<topic>-value`. PII topics use delete retention, never compaction.

`ig.post.v1` is published only once a post's detail is read, so every post
record has `taken_at`. A grid item alone produces no record.

### 4. Postgres: schema `ig`
All igs tables live in a Postgres schema named `ig`. igs's connection pool
pins `search_path` to `ig` in code, and goose keeps its version table there
(`ig.goose_db_version`), so the two services migrate independently. This
works whether igs gets its own database or shares fbs's, which D6 leaves open.
igs uses its own database (`igs`) on fbs's Postgres server in testing and
staging, so nothing in fbs can see igs's tables (see Consequences).

Freshness (D2, and the owner's answer of 2026-10-31): each profile's newest
posts are checked every 3 hours; the profile itself (counts, bio, verified,
the highlights list) is re-read **daily**, so `profile_snapshots` is a daily
follower history; highlight items are fetched only when the highlights list
changed.

Content tables (monthly partitions where marked; every post and comment
primary key starts with the post, as in fbs):

| Table | Primary key | Partitioned by | Holds |
|---|---|---|---|
| `profiles` | `user_pk` | — | username, full name, biography, bio links, verified, private, memorialized, follower and following counts, profile picture, linked Facebook page, `author_key`, status (`active`, `private`, `unavailable`, `suppressed`), `content_hash` |
| `profile_snapshots` | `(user_pk, captured_at)` | `captured_at` | username, full name, counts, verified, private: follower history and username history |
| `posts` | `(taken_at, media_pk)` | `taken_at` | owner pk, shortcode, `media_type`, `product_type`, caption, edited flag, hashtags and mentions, accessibility caption, location, usertags, music, `like_count` (NULL when hidden), counts-hidden flag, `comment_count`, commenting disabled, pinned (nullable), child count, `discovered_via`, comment collection state (§5), `content_hash`, status |
| `post_index` | `media_pk` | — | owner pk, `taken_at`, unique `shortcode` |
| `post_owners` | `(media_pk, user_pk)` | — | `role`: `owner` or `coauthor` (collab posts appear on several grids) |
| `post_metrics` | `(media_pk, captured_at)` | `captured_at` | `like_count`, `comment_count`, written only on change |
| `comments` | `(taken_at, media_pk, comment_pk)` | `taken_at` | `parent_comment_pk` (NULL), `depth` (0), `author_key`, author pk (NULL when pseudonymized), text, `created_at`, like count, `is_op`, flags, edit and deletion columns as fbs's 00004, status, `redacted`, `content_hash` |
| `comment_authors` | `author_key` | — | author pk, username, verified, profile picture (all NULL when pseudonymized) |
| `media` | `media_id` | — | Instagram ids (`source_media_id`, `user_pk`, `media_pk`, `parent_media_pk`, `position`, `comment_pk`), kind, variant, URL and expiry, size, duration, alt text, plus fbs's lifecycle columns under the same names (status, `s3_*`, attempts, `delete_after`, timestamps) |
| `media_acks` | `(media_id, consumer)` | — | as fbs |
| `highlights` | `highlight_id` | — | `user_pk`, title, cover, item count, status |
| `highlight_items` | `(highlight_id, item_pk)` | — | `taken_at`, media type, media ids |

Machinery tables keep fbs's names and columns, so the copied code runs on
them unchanged (ADR 0004): `api_clients`, `jobs`, `crawl_cursors`, `post_refresh`,
`erasure_requests`, `suppression`, `registry_sync_runs`. The only renames are
Facebook-named columns: `page_ref`/`page_id` become `profile_ref`/`user_pk`
(in `jobs`) and `post_id`/`page_id` become `media_pk`/`user_pk` (in
`post_refresh`). The schedule tables are `profile_schedule` (default interval
3 hours, D2) and `profile_registrations`. The query registry is `queries` and
`client_params` (fbs: `fb_queries`, `fb_client_params`), seeded with the five
operations of the brief §3.

Retention drops whole partitions. igs's comment retention equals its maximum
backfill age (365 days), so the partition loop does not create and drop old
partitions every hour as fbs's defaults do.

### 5. Comment completeness and deletions
fbs's rule Σ(1 + reply_count) = count cannot hold logged out. For igs:
* `comments_state` is `complete` when a full read of the post ends with
  `has_next_page: false`, `partial` when it stops early (limit, yield cap,
  error), `truncated` when a wall cuts it, `none` when not read.
  `comments_collected` against `comment_count` is reported as information,
  never as a failure.
* Deletion detection (fbs chapter 4 §11) runs only on complete full reads: a
  stored top-level comment the read did not return is marked deleted.
* Incremental reads go newest first and stop at the first stored comment.
  Comments are re-read only when the post's comment count went up (D2).

### 6. Privacy
* Personal data: profile username, full name, biography, bio links, profile
  picture, linked page name; comment author pk, username and picture; comment
  text; usertags and co-authors (they name people). The API returns these
  only with the `pii:read` scope.
* `pseudonymized` mode stores the `author_key` instead of the author's pk,
  username and picture, as fbs does.
* Erasure subjects: `author` (by author key, pk or username) and `profile`
  (by pk or username). A suppression row (`author` with the hex author key,
  `profile` with the pk) stops re-collection; the crawler and the persister
  both check it (brief §5 item 4).

### 7. Media
`ig.media.v1` always carries metadata: image candidates mapped to `original`
and `thumbnail`, video renditions to `sd` and `hd` by size, each carousel
child as its own rows, profile pictures as `PROFILE_PIC`. Download tasks
exist only when `ig.limits.include_media` is true; it defaults to false until
the analysis service exists (D5). The downloader and janitor then follow the
fbs contract (TTL, acknowledgements, orphan sweep) with igs's own buckets and
`ig.media.*` topics.

## Consequences
* A consumer reading both platforms uses one handoff schema
  (`common.v1`) and tells platforms apart by `platform` and topic.
* Posts are stored only once their detail is read. Until the cheap
  post-detail question (U2) is answered, each new post costs one post page
  load; the model does not change with the answer.
* A shared schema name or `search_path` mistake could point igs at fbs's
  tables. The pool sets `search_path` in code, the binaries check it at start,
  and production grants give igs's role no rights on fbs's schema.
* **igs never creates tables in a database fbs maintains.** fbs's
  `store.DropPartitionsBefore` (`internal/store/partitions.go:146-151`)
  finds partitions by parent table name alone, so it would list
  `ig.comments`'s partitions as well as `public.comments`'s and then fail on
  the unqualified `DROP`. igs's copy gets a schema filter in M1, but fbs's
  code keeps the old lookup, so igs keeps its own database `igs`.
* Hypotheses the build must check (not facts yet): whether a profile whose
  username changed can be read by pk alone (U9, the probe in M4), and whether
  a media pk encodes its creation time (U10, against the HAR timestamps in
  M5). U10 would let the grid walk stop at the age window without reading
  post details.

## Alternatives considered
* **`ig_`-prefixed tables in fbs's schema:** every table name in shared SQL
  would need a parameter, and machinery tables would still collide.
* **A separate database only:** clean while the servers are shared, but it
  breaks if production ever puts both services in one database. A schema
  works in both cases.
* **One `jobs` table with a platform column:** couples the two services'
  migrations, retention and finalizers.
* **Re-using `fb.v1` media handoff messages:** their fields are named for
  Facebook (`fb_media_id`, `page_id`).
* **Keying profiles by username:** usernames change and can be re-used.
