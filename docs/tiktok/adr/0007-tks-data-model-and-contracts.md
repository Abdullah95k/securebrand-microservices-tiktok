# ADR 0007: tks data model and contracts

Status: accepted (owner approved the plan, 2026-10-31); **finalised against the evidence in M3 (2026-11-01)**: the recordings ([har-findings-2026-10-31.md](../har-findings-2026-10-31.md)), the probe's test pass ([c2-findings-2026-10-31.md](../c2-findings-2026-10-31.md)) and full pass ([c3-findings-2026-11-01.md](../c3-findings-2026-11-01.md)). What M3 settled or changed is in [§8](#8-finalised-in-m3); the contracts themselves are the files it names. §9–§12 record what M5a, M5b, M5c and M6a settled.

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

**If a user object lacks the numeric id** (T17): **no fallback key** (decided in M3). The recordings carried `uid` on all 706 comment authors, and the `creator` list's items carry the author's `id`. A person without a numeric id is dropped by the parser and counted (M4), never keyed on `secUid` or the username; if the count is ever more than noise, a `sec`-keyed fallback is a new, add-only column.

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

Internal: `tks.task.account.{p0,p1,p2}`, `tks.task.post.{p0,p1,p2}`, `tks.task.thread.{p0,p1,p2}` (reply reads, signed one request each, scaled apart from the top-level reads; added in M3), `tks.task.media`, `tks.task.retry.{30s,5m,1h}` (the copied worker's three retry steps; `1h` added in M3), `tks.task.dlq`, and the persister's quarantine topic `tks.persist.dlq`. Wire format as fbs and igs: plain protobuf, the full message name in the `tks-type` header, subjects `<topic>-value`. PII topics use delete retention, never compaction.

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
| `accounts` | `user_id` | — | `sec_uid` (unique), username, nickname, bio, bio link (and TikTok's `risk` value for it), avatar media id, verified, private, organization flag, account created at, username and nickname last changed at (`uniqueIdModifyTime`, `nickNameModifyTime`), exact follower, following, total-likes (`heart_count`), video and likes-given counts (from `statsV2`; `stats` is rounded), `author_key`, status (`active`, `private`, `unavailable`, `banned`, `suppressed`), `content_hash`, first and last seen, profile refreshed at |
| `account_usernames` | `(user_id, username)` | — | first and last seen: the username history; also resolves an old username |
| `account_snapshots` | `(user_id, captured_at)` | `captured_at` | username, nickname, counts, verified, private: the daily history |
| `posts` | `(created_at, post_id)` | `created_at` | author `user_id`, `kind` (`video`, `photo`), caption, hashtags (`text[]`, GIN) and hashtag ids, duration, cover media id, photo count, `music_id`, `is_pinned` (nullable: known only from lists), `is_ad` (nullable), `labels` (`jsonb`: AI-generated (`ShowAIGC`, `aigcLabelType`), warnings, anchors, as served), TikTok's category, playlist id, comments disabled (`itemCommentStatus`), region of creation, text language, status, `comments_state` and counters (§5), `content_hash`, first and last seen |
| `post_index` | `post_id` | — | author `user_id`, `created_at`, `kind` |
| `post_mentions` | `(post_id, author_key)` | — | mentioned user id (NULL when pseudonymized), offsets |
| `post_metrics` | `(post_id, captured_at)` | `captured_at` | plays, likes, comments, shares, saves, reposts (exact, from `statsV2`): written only on change |
| `music` | `music_id` | — | title, author name, original-sound flag, duration, album, cover media id |
| `comments` | `(post_created_at, post_id, comment_id)` | `post_created_at` | `parent_comment_id` (NULL for top level; a reply's `reply_id`), `reply_to_comment_id` (a reply's `reply_to_reply_id`, when not 0), `depth` (0 or 1), `author_key`, author user id (NULL when pseudonymized), text, `created_at`, like count, reply count, liked by the creator (`is_author_digged`), pinned by the creator (`author_pin`, `stick_position`), has images, labels, language, status, `redacted`, `content_hash`, edit and deletion columns as fbs's `00004` |
| `people` | `author_key` | — | user id, `sec_uid`, username, nickname, avatar, verified (identifying columns NULL when pseudonymized): comment authors and mentioned users |
| `media` | `media_id` | — | owner ids (`user_id`, `post_id`, `music_id`), kind (`cover`, `origin_cover`, `dynamic_cover`, `video`, `photo`, `avatar`, `music_cover`), variant, URL and expiry, width, height, duration, size, plus fbs's lifecycle columns under the same names |
| `media_acks` | `(media_id, consumer)` | — | as fbs |

Machinery tables keep fbs's and igs's names and columns so the copied code runs on them (ADR 0006): `api_clients`, `jobs`, `crawl_cursors`, `post_refresh`, `erasure_requests`, `suppression`. Facebook-named columns are renamed as igs renames them (`account_ref`/`user_id` in `jobs`, `post_id`/`user_id` in `post_refresh`). The schedule is `account_schedule` and `account_registrations`. The operation registry is `operations` and `client_params` (ADR 0006 §3), seeded from `configs/tt/operations.seed.yaml`; `canary_runs` replaces `registry_sync_runs`.

Retention drops whole partitions; comment retention equals the maximum backfill age (365 days), as in igs.

**Size check, for Phase 0 to confirm.** At 300,000 accounts posting on average 1 post a day, a year holds about 110 million posts, about 1 billion metric rows written on change, and, at 200 stored comments per post, about 20 billion comment rows. Comments dominate storage, as in fbs. Phase 0 measures the real averages, and the comment cap per post (§5) is the lever.

### 5. Comments: completeness, refresh and deletions
* `comments_state` per post: `complete` when a full top-level read ends with `has_more` false (and, when replies are collected, every thread read to its end), `partial` when it stops early (cap, yield, error), `truncated` when a wall cuts it, `none` when not read.
* **Completeness check.** If TikTok serves replies logged out (T14), fbs's rule applies again: Σ(1 + reply_count) over top-level comments should equal `comment_count`. tks reports the ratio per post; whether a gap is normal (hidden, filtered or deleted comments) is T15, answered by the probe. It is information, never a failure.
* **Refresh when the count went up.** Two strategies, chosen by T13 (**confirmed 2026-10-31: TikTok's ranking order, so the second applies**; the first stays for a future sort parameter):
  * if the top-level list can be read **newest first** (by its default order or a sort parameter), read until the first stored comment, then re-read reply threads whose `reply_comment_total` grew: fbs's incremental pattern, one or two requests when little changed;
  * if it is ordered **by relevance**, re-read the top-level list to the cap (`limits.max_comments_per_post`, carried across continuations in `PostScope.items_seen`), upserting what is new and updating like and reply counts; deletion detection runs only on reads that reached `has_more` false.
* Deletion detection (fbs chapter 4 §11) runs only on complete full reads: a stored comment the read did not return is marked deleted.
* The cap per post, and whether backfilled posts get a full read or only their first page, are set after Phase 0 (they decide most of the backfill cost).

### 6. Privacy
* **Personal data** (behind the `pii:read` scope in the API): account username, nickname, bio, bio link and avatar; comment author ids, `sec_uid`, username, nickname and avatar; comment text; mentions; music author names (original sounds are named after their creator). Captions are the monitored account's own content, as in igs, and are not gated.
* **`pseudonymized` mode** stores the `author_key` instead of a person's id, `sec_uid`, username, nickname and avatar, as fbs does.
* **Erasure subjects:** `author` (by author key, user id, `sec_uid` or username) and `account` (by user id, `sec_uid` or username). A suppression row (author key, or the account's user id) stops re-collection; the crawler and the persister both check it (igs brief §5 item 4).
* The HMAC secret is permanent: rotating it orphans every author key.
* **Never stored:** TikTok's inferences about people that comment and user objects carry, such as `predicted_age_group` (seen on a third of comment authors in the recordings), and every empty profile field of comment authors. Parsers drop them before anything is emitted.

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

## 8. Finalised in M3

M3 (2026-11-01) wrote the contracts. igs's M3 had not landed (igs main `670a762`), so tks wrote `common.v1` itself, in the shape igs's ADR 0005 §2 plans, and listed it in `UPSTREAM.md` for igs.

**Where each part lives:**

| Part | Files |
|---|---|
| `securebrand.common.v1` | `api/proto/securebrand/common/v1/{common,media,job,erasure}.proto` |
| `securebrand.tt.v1` | `api/proto/securebrand/tt/v1/{common,account,post,comment,music,media}.proto` |
| `securebrand.tks.task.v1` | `api/proto/securebrand/tks/task/v1/tasks.proto` |
| Topics | `configs/tt/topics.yaml` (partitions, retention); `internal/tt/topics` (names, value schemas, the `tks-type` values, task routing) |
| Schema `tt` | `migrations/tt/00001_init.sql`; `internal/tt/ttstore` (a pool pinned to `search_path = tt`, goose's `tt.goose_db_version`, the start-up check, partitions, the registry's `operations` and `client_params`) |
| Config | the `tt:` section, `configs/tt/default.yaml` and `internal/config` (validation) |
| Operations | `tksctl migrate up|down|status`, `tksctl kafka apply`, `tksctl schemas apply|check` |

**Settled from the evidence:**
* **Keys** (§1): the numeric user id is on every user object seen, so there is no `secUid` fallback key.
* **Counts** come exact from `statsV2` (posts: plays, likes, comments, shares, saves and reposts; accounts: followers, following, hearts, videos, likes given, friends). `AccountCounts.exact` says whether a record's counts are exact; list items carry the author's counts rounded, so an account read from a list (`ACCOUNT_SOURCE_POST_LIST`) does not overwrite the page's exact counts or its bio link. **Corrected in M4 (T31):** a post's counts of 10,000 and more are rounded to 3 or 4 significant digits in `statsV2` as in `stats`, on lists and post pages alike; a list item's `authorStatsV2` is rounded too. So only the profile page's account counts are exact. `post_metrics` rows written on change therefore record changes at that granularity for large posts, and the comment refresh's "count went up" rule still works below 10,000 comments (in the recordings, 3 of 253 posts had more).
* **Pinned** is `isPinnedItem` from the `creator` list (T10); `posts.is_pinned` stays nullable for posts read elsewhere.
* **Comments** are in ranking order (T13), so `tt.comments.order` defaults to `ranked`; replies are served (T14), so `tks.task.v1` has `KIND_THREAD` and its topics.
* **Never stored:** `predicted_age_group` and other inferences have no field anywhere in `tt.v1`.

**Changed from the draft above:**
* **`common.v1`:** `Observation` and `CrawlMode` keep fbs's numbers and gain `platform`; the media handoff carries a neutral `MediaKind` and the platform's own kind in `platform_kind`; `JobEvent` names its account `subject_ref` and `subject_id`; `ErasureEvent` names it `account_id`.
* **`tks.task.v1`:** kinds `ACCOUNT = 200`, `POST = 201`, `MEDIA = 202`, `THREAD = 203`; the scope oneof at fields 40–43 (each scope's own fields number from 1). The generic fields keep fbs's names and numbers, `waits = 20` included; fields 10–13 (fbs's scopes) and 21–29 (igs's) are reserved. A test decodes each service's task as the other's: unknown kind, no scope.
* **`tt.v1`:** a `Person` message for comment authors and mentioned users (author key always set; the rest empty when pseudonymized), `TextRange` for hashtags and mentions in UTF-16 units, `PostLabels` (AI-generated, `aigcLabelType`, warnings, anchors), `Post.counts` (the counts at the read; the history is `tt.post_metrics.v1`), `DiscoveredVia.POST_PAGE` beside `LIST` (and the reserved `SEARCH`, `HASHTAG`).
* **Tables:** `people` holds comment authors and mentioned users (igs's `comment_authors`); `posts` also keeps the latest counts and `comment_count_read` (the count when comments were last read, for the "count went up" rule); `post_metrics` keeps fbs's **daily** partitions (the copied partition code's), the rest monthly; `media.source_media_id` is igs's name for fbs's `fb_media_id`; the registry tables carry registry-sync's check columns, so the canary (M8) records its checks the same way.
* **Topics:** `tks.task.thread.*` and `tks.task.retry.1h` (§3).
* **Media switch:** `limits.include_media` stays `true` in `configs/default.yaml` while the copied Facebook pipeline's tests and chart smoke run on it; tks's binaries default it to `false` (D5) when they start reading `limits` (M5).


## 9. Settled in M5a (the account stage)

M5a (2026-11-01) built the account stage (`internal/tt/stage`) against `fakett`. What it settled about how the data is read:
* **One list check per account per check, on the `creator` list.** Every post on every page read is published with its counts, music and media, also the posts past an incremental check's watermark on the last page: they came with the page, so their metrics refresh for free (§4: "every list check refreshes the posts on the pages it reads"). The watermark only ends the walk; the age window (`tt.freshness.backfill_days`, or a metrics walk's depth) is the only limit on what is kept.
* **Pinned posts need no watermark exemption on this list:** the `creator` list serves them at their own date with `isPinnedItem` (C2 run 2), so a post pinned long after it was posted is seen with its flag by the next metrics walk that reaches its date, not by the 2 h 45 min check.
* **The cursor's second is read once.** The list's next cursor keeps the oldest post's second (`client.NextCreatorCursor`), so the next page serves the posts of that second again; the stage saves the cursor as `<ms>~<id>,<id>` with the ids already read at that second and skips them. The saved cursor is opaque to everything else (checkpoints, continuations).
* **The account, two ways:** the first list page's author on every check (username, nickname, bio, avatar, flags; renames show within one check; counts rounded, so `AccountCounts.exact` is false), and the profile page when `tt.freshness.account_refresh` (24 h) or `page_refresh` has passed: the only source of exact counts (T31) and of the bio link. The scheduler (M6) sets `AccountScope.account_seen_at` and `page_seen_at` from what is stored.
* **References:** a `u:` reference needs nothing else; a `sec:` reference works once the identity has page values (the list's author then names the username); an `id:` reference alone cannot be read (TikTok's calls take the secUid, and its pages the username), so the API (M6) resolves it from the store first. A renamed account's old username loads a page that names nothing; with its secUid the check goes on, and the list carries the new name.
* **Statuses:** the list's 10222 is `PRIVATE`, 10221 `BANNED`, other unavailable answers `UNAVAILABLE`; the check ends with a status-only `Account` record.
* **Comment reads are decided per post on every page read** (§4, §5): a post never read gets a post task if it has comments; one read before only when its count went up, and its comment window's interval has passed since the last read (`checkpoint.PostCache.ReadAt`, set by the post stage); past the last window, only a metrics walk (`CRAWL_MODE_REFRESH`) reads it again.

## 10. Settled in M5b (comments, replies and the signer)

M5b (2026-11-01) built the post and thread stages (`internal/tt/stage/post.go`) and the browser signer (`internal/tt/sign`) against `fakett`. What it settled:
* **The signer is a seam in the client, per variant.** `graphql.Client.Signer` gives each built request its final URL just before it is sent, after the identity's pacing. tks's signer (`client.SignerFor`) signs only the variants whose registry label ends in `-signed` (`comments` and `replies` are `web-signed` in the seed); everything else goes out as built. `tt.signer.mode: none` signs nothing, so the comment calls are refused (an empty 200: `RegistryStale`) and their tasks end in the dead letter queue: comments need `browser`.
* **A page signs without sending** (T32). `browser.Session.Sign` makes the call with the page's `fetch`, holds the request once TikTok's script has added its parameters, fails it before it leaves and returns its URL; the identity that asked sends it by plain HTTP (T27). So a signature costs browser time, not a second request. The pool (`tt.signer.pages` per process) keeps a page per user agent (the signed URL names the browser's version, so the sender must present the same), opens it on the call's referer (the post's page, as the probe signed from), and reopens it after `page_max_signs` or `page_max_age`, or after a failure. A call that finds every page busy for `sign_timeout` is retried in 30 s as a wait (not held against the task, like a task without a free exit); `tks_signer_signs_total{result}` counts both.
* **Ranked comments are read whole.** Every read starts at the first page and goes to `has_more` false or to the cap per post (`limits.max_comments_per_post`, across continuations by `PostScope.items_seen`), upserting what is new and updating like and reply counts (§5). The replies TikTok quotes inline (`reply_comment`, one in the recordings) are published at depth 1 with their parent.
* **Threads only when needed.** A comment gets a thread task only when it has more replies than the page quoted, and not when its thread was read whole before with as many replies (`checkpoint` thread counts); replies are read 3 a page, one signature each, to the end. The post's incomplete mark (a thread lost since its last whole read) makes the next read re-read every thread and clears it once the post was read whole.
* **States:** a read that reached the end is `complete`; one stopped at the cap `partial`; one whose offset stopped moving `truncated`; a task dead-lettered marks the post incomplete (its next read is whole). `tks_comment_reads_total{kind,state}`.
* **Deletions:** a whole read of a post read before records the top-level comments it saw (`jobs` passes, fbs's), so the finalizer (M6) marks a stored comment the read did not return as deleted. A read stopped at the cap records nothing.
* **The "count went up" rule** now keeps the count each read was planned on: the account stage puts the post's current comment count in `PostScope.comment_count_seen`, the post stage stores it as the read's `Reported` with `ReadAt`, and the next check reads the post again only when the count goes past it, within its comment window (§9).
* **`PostScope` and `ThreadScope` carry the account's username and the post's kind** (add-only fields 7 and 8; `PostScope.incomplete_mark` 9): the post's page is the comment calls' referer and where an identity without page values bootstraps.
* **Media tasks** follow D5's switch (`tt.media.include`): off, no download task; on, a post's cover and one video rendition (the job's video policy), and comment images when `limits.include_comment_media` is also on.

## 11. Settled in M5c (the writer and the end-to-end test)

M5c (2026-11-01) wrote tks's writer for schema `tt` (`internal/tt/persist`), wired the crawler and the persister (`internal/tt/app`, `cmd/tks-crawler`, `cmd/tks-persister`) and tested them end to end against `fakett` (`internal/tt/e2e`). What it settled about how the data is stored:
* **One consumer, two schemas.** `internal/persist`'s consumer (batches, bisection of a batch Postgres refuses, the quarantine topic) takes a schema's `Sink`; tks's reads `tt.*.v1` with the group `tks-tt-persister` and sets aside to `tks.persist.dlq`. The crawler's groups are `tks-tt-crawler-<stage>-<lane>`, apart from the copied Facebook pipeline's.
* **Upserts by observation time.** A record older than the stored row changes nothing (a redelivery, a replay, a restarted crawler's second read), and an unchanged one is not rewritten (content hash) except to move `last_seen_at` once a day.
* **Accounts: every record of a batch, oldest first.** A check reads the profile page and then the list, so both reach the same batch on the account's partition; keeping only the newest would lose the page's exact counts and bio link. Exact counts (`AccountCounts.exact`) replace the stored ones; rounded ones only fill an account whose page was never read (T31). The bio link and its risk come only from the page; a list read without a bio keeps the stored one (a removed bio shows at the next page read). A status-only record (`private`, `unavailable`, `banned`) changes the status of a stored account, or adds a minimal row when it carries the secUid. Every username seen goes to `account_usernames`; a snapshot is written when the username, nickname, counts or flags changed.
* **Posts** keep the pinned and ad flags when a read does not carry them (the `creator` list does, a post page may not), the discovery way of the first read, the latest counts, `post_index` and `post_mentions`. **`post_metrics` gets a row only when a post's counts differ from its previous row**: a list check that refreshes every post on its pages writes rows only for the posts that changed.
* **Comments** keep fbs's rules: an edit is counted, a deletion is undone by a later sighting (with the replies deleted with it, by `parent_comment_id`), a redacted row is never rewritten. Authors go to `people`.
* **Erasure:** every write takes the erasure lock (`gdpr.GuardAccounts` with the subject type `account`) and drops what belongs to an erased account (account, posts, metrics, comments, media), stores an erased author's comments redacted without a `people` row, and clears erased people's mentions.
* **Deletions** need the finalizer (M6): the post stage records whole reads, and `persist.MarkDeleted` marks what a whole read no longer returned.

## 12. Settled in M6a (the scheduler)

M6a (2026-11-01) wrote tks's dispatcher (`internal/tt/dispatch`), scheduler (`internal/tt/scheduler`) and `cmd/tks-scheduler`, and tested them end to end against `fakett` (`internal/tt/e2e`, `TestScheduledChecks`). M6 is split like M5: **M6a** the scheduler, **M6b** the API and erasure, **M6c** media and the §8 delay and success metrics end to end. What M6a settled:
* **One job per account per check.** Every registered account (`account_schedule`, one row per normalised reference) is checked every `interval_seconds`, `tt.freshness.list_interval` (2 h 45 min) by default, with fbs's jitter (±10 %), its doubling per consecutive failure (at most 30 days) and the budget's stretch. A check whose previous job still runs is skipped. Suppressed accounts are skipped in the planner too.
* **What a check is.** An account never read gets a **backfill** (mode `full`, lane p2) to 365 days or its first post. Afterwards, when a metrics walk is due (`tt.freshness.metrics_walks`: daily back 30 days, weekly back 365 days), the **deepest due walk** (mode `refresh`, lane p2, `limits.max_age_seconds` = its depth, never past the account's own window) stands in for the check, since it reads the newest posts too; otherwise an **incremental check** (lane p1). Walks and backfills go to p2 so that they cannot hold up the checks that meet the delay target (build brief §8 A2).
* **Walks are recorded when they end well** (`account_schedule.walks`: depth in seconds → time; migration `00002`): a backfill stamps every walk, a walk the walks no deeper than itself. So a new account's first walk comes a day after its backfill.
* **The root task's scope comes from the store** (`dispatch.Resolve`): by user id, secUid, a current or former username (the latest holder first), or the schedule's user id; the current username, the secUid (a renamed account is still found) and, for an active account only, when the account and its page were last read (so the page is read when due; a private or gone account's page is read at each of its checks). `id:` of an account not stored is refused: TikTok's calls cannot be made from the id alone (§9).
* **Job status.** `completed`, or `partial` when a comment read was cut by an error or a wall, a task was dead-lettered or a list walk got stuck; `failed` only when TikTok does not serve the account at all and no post was read. A private or banned account is an answer (`completed`, its status stored), and a comment read stopped at the cap per post is the configured outcome (build brief §8 A4), counted apart (`posts_capped`). Each job ends with a `common.v1.JobEvent` on `tt.job.v1` (`platform: TIKTOK`, the account's reference and user id, counters and coverage), sent before the row turns terminal (a retry sends the same event id).
* **Comment coverage per post** (§5): for every post whose comments the job read, `comments_collected`, `replies_collected`, `completeness` (against `comment_count`), `comments_state` and `comments_synced_at`.
* **Deleted comments** (§5, §11): for each whole read of a post read before, a stored top-level comment the read did not return is published deleted on `tt.comment.v1` with its replies and marked deleted (`persist.MarkDeleted`), all at one time so a later sighting restores them together. TikTok's count includes comments it does not serve (T15), so a read is checked only when what it returned (each comment with the replies TikTok counts for it) is within `tt.comments.deletion_slack` (10 %, after Phase 0) of the post's count. Deletions are noticed only when a post's count still goes up (the read is triggered by a rise; a post that only loses comments is not re-read).
* **The schedule moves on in the finalizer**: the user id once known, the failure count (reset by `completed` or `partial`), `last_success_at` (the check's start), the walks, the posting rate (`posts_per_day` over 30 days, `last_post_at`). An account that is not active is checked again only after `tt.freshness.account_refresh` (a day), until a check finds it active again. The reaper closes stalled jobs as fbs's does and counts them as failed checks.
* **Retention** (`privacy.retention`): comment partitions after 365 days (the backfill's reach), `post_metrics` and `account_snapshots` partitions after `metrics_days`, finished jobs and canary runs after `jobs_days`, media rows never handed over after `media_rows_days`, people not seen for `authors_inactive_days`. Posts are kept.
* **Metrics:** `tks_checks_total{kind,result}` (check, walk, backfill), `tks_account_check_oldest_seconds` (A1: no active account more than 6 h without a good check), `tks_scheduled_accounts{status}`, and fbs's `tks_schedule_lag_seconds`, `tks_jobs_total`, `tks_comment_completeness_ratio`, `tks_deletion_checks_total`. The budget and AIMD controllers are the shared ones on `tt.aimd`.
* **Not built, on purpose:** posting-rate tiers (every account is checked every 2 h 45 min: the 99 % target counts posts of rare posters too; whether slower checks for rare posters are acceptable is for the owner after Phase 0, and `interval_seconds` per account is the lever), and fbs's `post_refresh` re-reads (TikTok's comment windows are applied by the account stage on every page it reads, and past the last window by the weekly walk).
