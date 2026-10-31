# ADR 0007: tks data model and contracts

Status: proposed (kickoff session, 2026-10-31; accepted when the owner approves the plan). Field names below come from public sources and are hypotheses until the owner's recordings and the probe confirm them ([open-questions.md](../open-questions.md)). M3 finalises this ADR against that evidence; the shape does not depend on it.

## Context
tks monitors public TikTok accounts (build brief §2, D1–D2):
1. **Account:** stable user id, username (with its history), display name, bio, bio link, avatar, verified and private flags, follower, following, total-likes and video counts. Refreshed daily.
2. **Posts** (videos and photo posts): id, link, caption, hashtags, mentions, posting time, duration, cover, music, pinned flag, the sponsored or ad flag where shown, and the counts TikTok shows (plays, likes, comments, shares, saves). New accounts: back 365 days or to the first post. Known accounts: new posts stored within 3 hours; metrics refreshed.
3. **Comments:** top-level comments (text, author, time, likes, reply count), re-read only when a post's comment count went up. Replies too, if served without login.
4. **Media:** metadata always; downloads behind one switch, off by default.

The rules carried over from fbs and igs: an HMAC key on the stable user id, never the username; PII behind a scope; erasure with suppression; add-only protobuf with `buf breaking` and Schema Registry subjects under BACKWARD_TRANSITIVE ([igs ADR 0005](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/adr/0005-ig-data-model-and-contracts.md), [fbs kafka-schemas.md](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/contracts/kafka-schemas.md)). In testing, tks shares fbs's Kafka, Postgres 18, Valkey and S3 (D6).

What the data is expected to look like (public sources, [methods.md §2](../methods.md#2-tiktoks-website-logged-out)):
* A user has a numeric `id` (decimal string in JSON), a `secUid` (a long opaque string the web API takes as the account key), a `uniqueId` (the username, changeable) and a `nickname` (display name). T17 asks which of them every user object carries, and whether `secUid` survives a rename. No public source shows a bio-link field (T1); the column stays nullable.
* A post (TikTok's "item") has a numeric `id`, `desc` (caption), `createTime` (Unix seconds), `video` (duration, covers, play and download addresses, bitrate renditions) or `imagePost` (photos), `music`, `stats` (`playCount`, `diggCount`, `commentCount`, `shareCount`, `collectCount`) and often `statsV2` with the same counts as strings, `textExtra` (hashtags and mentions with offsets), `challenges` (hashtags with ids), and flags such as `isAd` and AI-generated labels; a pinned flag exists in the app but its name in the web data is unknown (T9, T10, T16, T19).
* A comment has `cid`, `text`, `create_time`, `digg_count`, `reply_comment_total`, `user` (`uid`, `sec_uid`, `unique_id`, `nickname`, avatar), and for replies `reply_id` and `reply_to_reply_id` (T12–T14).
* Post and comment ids are reported to carry their creation time in the top 32 bits (T8).

## Decision

### 1. Identifiers

| Entity | Key | Notes |
|---|---|---|
| Account | user `id` (int64) | Never the username. API and schedule refs are `u:<username>`, `sec:<secUid>` or `id:<user id>`, resolved to the id on first load. `sec_uid` is stored and unique; usernames are tracked in `account_usernames` (§4) |
| Post (video or photo) | post `id` (int64) | Links are built from the id and the current username (`/@<username>/video/<id>` or `/photo/<id>`), never stored as the key |
| Comment or reply | comment `cid` (int64) | Replies carry `parent_comment_id` |
| Music | music `id` (int64) | Shared by many posts |
| Media file row | UUIDv5 of `<owner id>|<kind>|<variant>` in a tks namespace | fbs's scheme with a tks namespace UUID, so ids never collide with fbs's or igs's |
| Person (account, comment author, mentioned user) | `author_key = HMAC-SHA256(tks secret, "tt:user:" + user id)` | Domain-separated from fbs's and igs's, with a separate secret (`privacy.author_hmac_key` in tks's config). Not on `secUid` or the username |

**Integer range.** If T8 holds, the top 32 bits of a post or comment id are Unix seconds, so ids stay below int64's limit until 2038-01-19 (2³¹ seconds). Accounts created before TikTok merged with musical.ly have smaller ids. tks stores ids as `bigint`; M4 checks every id in the fixtures, and the parsers reject (and count) an id that does not fit, rather than wrapping it. A later move to `numeric(20)` would be a migration, well before 2038.

**If a user object lacks the numeric id** (T17; for example a comment author served with `sec_uid` only), the author key falls back to `HMAC(secret, "tt:sec:" + secUid)`, flagged `key_kind = sec`, and is re-keyed to the id when the id is seen. M3 decides whether the fallback is needed from the recordings.

### 2. Proto packages
* **`securebrand.common.v1`**: igs's file, copied byte for byte once igs's M3 lands (ADR 0006 §1): `Platform` (tks adds `TIKTOK` only through igs, so the enum stays one definition), `CrawlMode`, `Observation`, `ImageRef`, `S3Object`, the media handoff (`MediaReady`, `MediaProcessed`, `MediaDeleted`), `JobEvent`, `Completeness`, `ErasureEvent`. If tks needs it before igs has it, tks writes it in igs's shape and lists it in `UPSTREAM.md` for igs.
* **`securebrand.tt.v1`**: `Account`, `Post` (with `kind`, `photos`, `hashtags`, `mentions`, `music_id`, `labels`), `PostMetrics`, `Comment` (with `parent_comment_id`, `reply_to_comment_id`, `depth`), `Music`, `Media`. Fields TikTok may stop serving are optional. Room for later without a breaking change: `Post.discovered_via` (`LIST` now; `SEARCH`, `HASHTAG` reserved); new counts arrive as new fields.
* **`securebrand.tks.task.v1`** (internal): igs's envelope fields with the same names and numbers (task and lineage ids, `seq`, `job_id`, `lane`, `kind`, `mode`, `cursor`, `limits`, `attempt`, `stalls`, …). TikTok's own parts are disjoint from fbs's and igs's: **kind values start at 200** (`ACCOUNT`, `POST`, `MEDIA`, `THREAD`) and **scope fields at number 40** (`AccountScope{user_id, sec_uid, username, watermark}`, `PostScope{post_id, created_at, comment_count_seen, items_seen}`, `ThreadScope{post_id, comment_id, items_seen}`). A task that lands on another service's topic decodes with an unknown kind and no scope and is dead-lettered.

### 3. Topics
Public contract (keys keep one post's records in order):

| Topic | Key | Value |
|---|---|---|
| `tt.account.v1` | user id | `securebrand.tt.v1.Account` |
| `tt.post.v1` | post id | `securebrand.tt.v1.Post` |
| `tt.post_metrics.v1` | post id | `securebrand.tt.v1.PostMetrics` |
| `tt.comment.v1` | post id | `securebrand.tt.v1.Comment` |
| `tt.music.v1` | music id | `securebrand.tt.v1.Music` |
| `tt.media.v1` | media id | `securebrand.tt.v1.Media` |
| `tt.media.ready.v1`, `tt.media.processed.v1`, `tt.media.deleted.v1` | media id | `securebrand.common.v1.Media{Ready,Processed,Deleted}` |
| `tt.job.v1` | job id | `securebrand.common.v1.JobEvent` |
| `tt.erasure.v1` | erasure request id | `securebrand.common.v1.ErasureEvent` |

Internal: `tks.task.account.{p0,p1,p2}`, `tks.task.post.{p0,p1,p2}`, `tks.task.media`, `tks.task.retry.{30s,5m}`, `tks.task.dlq`, and the persister's quarantine topic `tks.persist.dlq`. Wire format as fbs and igs: plain protobuf, the full message name in the `tks-type` header, subjects `<topic>-value`. PII topics use delete retention, never compaction.

`tt.post.v1` is published when a post is first read in full (from the list or its page), so every post record has `createTime`.

### 4. Postgres: database `tks`, schema `tt`
All tables live in schema `tt` of tks's own database `tks` on fbs's Postgres server in testing (igs's reasons: fbs's partition maintenance ignores the schema). The pool pins `search_path` to `tt` in code; goose keeps `tt.goose_db_version`.

**Freshness:**
* list check every **2 h 45 min** (the delay target in build brief §8), reading back to the newest stored post, pinned posts exempt from the watermark;
* account profile **daily** (`account_snapshots` is a daily history);
* metrics: every list check refreshes the posts on the pages it reads; a deeper refresh walks the list **daily back 30 days and weekly back 365 days** (proposed defaults; the owner sets the tiers after Phase 0, because they are the biggest cost lever after comments);
* comments: re-read when a post's `comment_count` went up since the last read (§5), at most every 6 h for posts under 2 days old, daily up to 7 days, weekly up to 30 days, and after that only when the weekly metrics walk sees the count grow (proposed defaults: a busy post's count goes up at almost every check, so without these tiers comments would be re-read up to nine times a day; the owner sets them after Phase 0).

Content tables (monthly partitions where marked; every post and comment key starts with the post):

| Table | Primary key | Partitioned by | Holds |
|---|---|---|---|
| `accounts` | `user_id` | — | `sec_uid` (unique), username, nickname, bio, bio link, avatar media id, verified, private, follower, following, total-likes (`heart_count`), video and likes-given counts, `author_key`, status (`active`, `private`, `unavailable`, `banned`, `suppressed`), `content_hash`, first and last seen, profile refreshed at |
| `account_usernames` | `(user_id, username)` | — | first and last seen: the username history; also resolves an old username |
| `account_snapshots` | `(user_id, captured_at)` | `captured_at` | username, nickname, counts, verified, private: the daily history |
| `posts` | `(created_at, post_id)` | `created_at` | author `user_id`, `kind` (`video`, `photo`), caption, hashtags (`text[]`, GIN) and hashtag ids, duration, cover media id, photo count, `music_id`, `is_pinned` (nullable: known only from lists), `is_ad` (nullable), `labels` (`jsonb`: branded content, AI-generated, warnings, as served), comments disabled, region of creation, language, status, `comments_state` and counters (§5), `content_hash`, first and last seen |
| `post_index` | `post_id` | — | author `user_id`, `created_at`, `kind` |
| `post_mentions` | `(post_id, author_key)` | — | mentioned user id (NULL when pseudonymized), offsets |
| `post_metrics` | `(post_id, captured_at)` | `captured_at` | plays, likes, comments, shares, saves: written only on change |
| `music` | `music_id` | — | title, author name, original-sound flag, duration, album, cover media id |
| `comments` | `(post_created_at, post_id, comment_id)` | `post_created_at` | `parent_comment_id` (NULL for top level), `reply_to_comment_id`, `depth` (0 or 1), `author_key`, author user id (NULL when pseudonymized), text, `created_at`, like count, reply count, liked by the creator, pinned by the creator, labels, language, status, `redacted`, `content_hash`, edit and deletion columns as fbs's `00004` |
| `people` | `author_key` | — | user id, `sec_uid`, username, nickname, avatar, verified (identifying columns NULL when pseudonymized): comment authors and mentioned users |
| `media` | `media_id` | — | owner ids (`user_id`, `post_id`, `music_id`), kind (`cover`, `origin_cover`, `dynamic_cover`, `video`, `photo`, `avatar`, `music_cover`), variant, URL and expiry, width, height, duration, size, plus fbs's lifecycle columns under the same names |
| `media_acks` | `(media_id, consumer)` | — | as fbs |

Machinery tables keep fbs's and igs's names and columns so the copied code runs on them (ADR 0006): `api_clients`, `jobs`, `crawl_cursors`, `post_refresh`, `erasure_requests`, `suppression`. Facebook-named columns are renamed as igs renames them (`account_ref`/`user_id` in `jobs`, `post_id`/`user_id` in `post_refresh`). The schedule is `account_schedule` and `account_registrations`. The operation registry is `operations` and `client_params` (ADR 0006 §3), seeded from `configs/tt/operations.seed.yaml`; `canary_runs` replaces `registry_sync_runs`.

Retention drops whole partitions; comment retention equals the maximum backfill age (365 days), as in igs.

**Size check, for Phase 0 to confirm.** At 300,000 accounts posting on average 1 post a day, a year holds about 110 million posts, about 1 billion metric rows written on change, and, at 200 stored comments per post, about 20 billion comment rows. Comments dominate storage, as in fbs. Phase 0 measures the real averages, and the comment cap per post (§5) is the lever.

### 5. Comments: completeness, refresh and deletions
* `comments_state` per post: `complete` when a full top-level read ends with `has_more` false (and, when replies are collected, every thread read to its end), `partial` when it stops early (cap, yield, error), `truncated` when a wall cuts it, `none` when not read.
* **Completeness check.** If TikTok serves replies logged out (T14), fbs's rule applies again: Σ(1 + reply_count) over top-level comments should equal `comment_count`. tks reports the ratio per post; whether a gap is normal (hidden, filtered or deleted comments) is T15, answered by the probe. It is information, never a failure.
* **Refresh when the count went up.** Two strategies, chosen by T13:
  * if the top-level list can be read **newest first** (by its default order or a sort parameter), read until the first stored comment, then re-read reply threads whose `reply_comment_total` grew: fbs's incremental pattern, one or two requests when little changed;
  * if it is ordered **by relevance**, re-read the top-level list to the cap (`limits.max_comments_per_post`, carried across continuations in `PostScope.items_seen`), upserting what is new and updating like and reply counts; deletion detection runs only on reads that reached `has_more` false.
* Deletion detection (fbs chapter 4 §11) runs only on complete full reads: a stored comment the read did not return is marked deleted.
* The cap per post, and whether backfilled posts get a full read or only their first page, are set after Phase 0 (they decide most of the backfill cost).

### 6. Privacy
* **Personal data** (behind the `pii:read` scope in the API): account username, nickname, bio, bio link and avatar; comment author ids, `sec_uid`, username, nickname and avatar; comment text; mentions; music author names (original sounds are named after their creator). Captions are the monitored account's own content, as in igs, and are not gated.
* **`pseudonymized` mode** stores the `author_key` instead of a person's id, `sec_uid`, username, nickname and avatar, as fbs does.
* **Erasure subjects:** `author` (by author key, user id, `sec_uid` or username) and `account` (by user id, `sec_uid` or username). A suppression row (author key, or the account's user id) stops re-collection; the crawler and the persister both check it (igs brief §5 item 4).
* The HMAC secret is permanent: rotating it orphans every author key.

### 7. Media
`tt.media.v1` always carries metadata: the cover (original and the dynamic cover), each photo of a photo post, video renditions mapped to `sd` and `hd` by bitrate, avatars as `avatar`, music covers. Download tasks exist only when `limits.include_media` is true; it defaults to false until the analysis service exists (D5). When on, the downloader follows fbs's contract (TTL, acknowledgements, orphan sweep) with tks's buckets and `tt.media.*` topics. TikTok's media links are signed and expire (T18), so downloads run within minutes of the read that produced the link, and an expired link is re-read from the post, not retried as is.

## Consequences
* One consumer reads all three platforms' media handoff and job events with `common.v1`, and tells them apart by `platform` and topic.
* Every new post costs nothing beyond the list check that found it, if T9 holds. If list items turn out to lack counts or music, each new post needs its page (about one page load per post, as igs) and the model is unchanged.
* The comment refresh strategy is one switch in the post stage, set from T13; the schema is the same either way.
* Storage is dominated by comments; the caps are config.
* A `search_path` mistake could point tks at another service's tables; the pool sets it in code, the binaries check it at start, and production grants give tks's role no rights elsewhere.

## Alternatives considered
* **Keying accounts by `secUid`:** it is what the web API takes, but it is an opaque string whose stability is unproven (T17); the numeric id is the documented stable id. Both are stored.
* **Keying people by username:** usernames change and can be re-used.
* **One `jobs` table shared with fbs or igs:** couples three services' migrations, retention and finalizers.
* **Storing each post's raw JSON:** easiest for future fields, but it repeats PII (author objects) and multiplies storage; raw answers are sampled to the raw bucket for drift debugging instead, as in fbs, and new fields become columns.
* **Re-using `ig.v1` messages:** their fields are named for Instagram (`pk`, `shortcode`, carousels).
