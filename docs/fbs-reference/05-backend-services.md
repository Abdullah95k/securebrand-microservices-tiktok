# 5. Backend services: API, database, persister, scheduler, media, GDPR, config

[← Index](README.md)

Repository: this repository at commit `265ae3a`. `path:line` references are relative to the repository root and point at that commit. Everything below was read from source. Anything marked **[suggestion]** is a proposal for the Instagram build, not something the code does.

Scope: `internal/api`, `api/openapi/fbs.v1.yaml`, `internal/persist`, `internal/scheduler`, `internal/platform/leader`, `internal/media`, `docs/contracts/media-handoff.md`, `internal/platform/s3x`, `internal/gdpr`, `internal/store`, `internal/replay`, `migrations/*.sql`, `internal/config`, `configs/default.yaml` (everything except `fb:`), `cmd/{fbs-api,fbs-persister,fbs-scheduler,fbs-media-downloader,fbs-media-janitor,fbctl}`, and `internal/platform/{pg,redisx,schemareg,metrics,httpserver,run,log,config}`. Where these services depend on out-of-scope code (`internal/dispatch`, `internal/crawl/jobs`, `internal/fb/ratelimit/budget.go`, `internal/platform/kafka`, `internal/crawl/emit`), I read just enough of it to explain how the services behave.

---

## 0. Orientation: how the backend pieces fit

```
                  ┌──────────────┐  creates job row + root tasks   ┌───────────────┐
 client ──REST──▶ │   fbs-api    │ ───────────(dispatch)─────────▶ │ fbs.task.*    │──▶ fbs-crawler (out of scope)
                  └──────────────┘                                  └───────────────┘        │
                         ▲ reads (optional replica)                                          │ publishes
                         │                                                                   ▼
 ┌────────────┐   COPY+upsert   ┌──────────────────┐   consumes fb.page/post/comment/media.v1 (Kafka)
 │ Postgres18 │ ◀────────────── │  fbs-persister   │ ◀────────────────────────────────────────────
 └────────────┘                 └──────────────────┘
       ▲  ▲
       │  │ leader-only loops: planner, finalizer, reaper, maintenance, erasure, gauges, budget, AIMD
       │  └──────────── fbs-scheduler (pg advisory-lock leader) ──▶ fb.job.v1, fb.erasure.v1, fb.comment.v1 (deletions)
       │
       │  fbs.task.media ─▶ fbs-media-downloader ─▶ S3 (media/..) ─▶ fb.media.ready.v1 ─▶ analysis service
       └─────────────────── fbs-media-janitor ◀─ fb.media.processed.v1 ◀────────────────────┘
                                   └─▶ S3 DeleteObject ─▶ fb.media.deleted.v1
```

Key architectural facts (ADR 0002, `docs/adr/0002-kafka-postgres-valkey.md`):
- **Kafka** carries both the internal work queues (`fbs.task.*`) and the public contract (`fb.*`). The crawlers publish entities directly. There is no outbox.
- **Postgres 18** holds the queryable view. Only the persister builds it from the public topics. The scheduler writes job, coverage and schedule state, and the GDPR worker writes erasures. **PG18 is a hard requirement**: the code uses `RETURNING old.*/new.*` in `internal/persist/writer.go:103-109,280-282,405-406` and `internal/api/pages.go:128-129`.
- **Valkey/Redis** holds short-lived coordination state: job task accounting, rate limits, budget state, cancel flags, comment "seen" lists. Losing it costs in-flight jobs, not data.
- A job becomes `completed` only after the persister's committed offsets have passed the end offsets snapshotted when the job's last task finished. So **"completed" means "queryable"** (`internal/scheduler/finalizer.go:30-32,57-120`).

Topic list (`internal/topics/topics.go`, `configs/topics.yaml`): public `fb.page.v1`, `fb.post.v1`, `fb.post_metrics.v1`, `fb.comment.v1`, `fb.media.v1`, `fb.media.ready.v1`, `fb.media.processed.v1`, `fb.media.deleted.v1`, `fb.job.v1`, `fb.erasure.v1`. Internal: `fbs.task.{page,post,thread}.{p0,p1,p2}`, `fbs.task.media`, `fbs.task.retry.{30s,5m,1h}`, `fbs.task.dlq`. Every name gets `kafka.topic_prefix` prepended. All topics use `cleanup.policy=delete` and are never compacted, so erased PII ages out. Public topics keep data for 168h, except `fb.erasure.v1`, which keeps it for 720h (`configs/topics.yaml:21-32`, `internal/platform/kafka/admin.go:228-234`).

---

## 1. REST API (`fbs-api`, `internal/api`)

### 1.1 Process and middleware
- `cmd/fbs-api/main.go:32-68` opens the write pool (`postgres.dsn`). It also opens a read pool when `postgres.read_dsn` is set (`ReadDB`, used for list and read queries), plus Redis (for rate limiting and live job counters) and a Kafka producer (for dispatching tasks). The API listens on `http.addr` (`:8080`). The ops server on `http.ops_addr` (`:9090`) has readiness checks for postgres, redis and kafka.
- The router (`internal/api/server.go:37-77`) is chi with `RequestID`, `Recoverer`, `requestLog` (`fbs_api_requests_total{route,code}`, plus an error log for 5xx) and `middleware.Timeout(60s)`. All `/v1` routes go through `auth.middleware`, and each route is wrapped in `require(scope, handler)`.
- Request bodies are decoded with `DisallowUnknownFields` and capped at 1 MiB (`server.go:127-131`), so unknown JSON fields return 400.
- Errors are RFC 9457 problem documents, `application/problem+json`, shaped `{"type":"about:blank","title","status","detail"}` (`server.go:111-116`). `pgx.ErrNoRows` maps to 404 `"not found"`. Anything else becomes a 500 `"internal error"` and is logged (`server.go:118-125`).
- `TestRoutesMatchOpenAPI` (`internal/api/openapi_test.go`) walks the chi router and fails if a route and an OpenAPI path disagree in either direction. Keep this test in the Instagram build.

### 1.2 Authentication: API keys
- **Format:** `Authorization: Bearer fbs_<prefix>_<secret>`. `GenerateKey` (`internal/api/auth.go:51-58`) draws 30 random bytes and base64url-encodes them into 40 chars. The first 8 chars, lowercased with `-`→`a` and `_`→`b` so the prefix never contains the `_` separator, become the **prefix**. The remaining 32 chars (~192 bits) are the **secret**.
- **Storage:** `api_clients.key_prefix` (UNIQUE) and `key_hash = SHA-256(secret)` (unsalted, which is fine for high-entropy secrets), together with `scopes text[]`, `rate_per_minute` (NULL means the default) and `revoked_at` (`migrations/00001_init.sql:213-222`). The full key is shown once, by `fbctl apikey create`.
- **Lookup** (`auth.go:83-114`): split on the first `_` after `fbs_`, load the client by prefix with `revoked_at IS NULL`, and compare `SHA-256(secret)` in constant time. A successful lookup is **cached per prefix for 1 minute** per API replica (`auth.go:92`). As a result, a revocation or scope change takes up to 60 s to apply on each replica. Failed lookups are not cached, so every bad key costs one DB query.
- **Scopes** (`auth.go:21-27`): `jobs:write`, `pages:write`, `data:read`, `pii:read`, `admin`. `Client.Has` treats `admin` as holding every scope (`auth.go:40-47`). A missing scope returns 403 `"this API key lacks the <scope> scope"` (`auth.go:143-151`).
- **Rate limiting** (`auth.go:131-138`): `go-redis/redis_rate`, a GCRA limiter keyed `rl:api:<client uuid>` with `PerMinute(rate)`. The rate is the client's `rate_per_minute`, or `api.default_rate_per_minute` (600). When the limit is exceeded the API returns 429 `"rate limited"` with `Retry-After: <ceil seconds>+1`. The limiter **fails open** on Redis errors, and it runs before the scope check.
- A missing or invalid key returns 401 `"unauthorized"`.

### 1.3 Pagination
- `limit` defaults to 50, is capped at 500, and a non-positive value falls back to 50 (`server.go:133-139`).
- `cursor` is opaque: base64url JSON `{"t": <time>, "i": <int64>}` (`server.go:141-165`). An undecodable cursor is silently ignored, so the request starts from the first page.
- `next_cursor` is only emitted when the page is full (`len == limit`).
- Keyset orders per endpoint:
  - jobs: `created_at DESC`, cursor holds `created_at` only (`jobs.go:149-178`, `dispatch.go:296-318`). There is no tie-breaker, so jobs sharing a timestamp can be skipped.
  - posts: `(post_created_at, post_id) DESC` (`data.go:73-78`).
  - comments: `(coalesce(created_at,'epoch'), comment_id) ASC` (`data.go:219-224`).
  - `GET /pages`, `/snapshots`, `/metrics` and `/media` take `limit` only, with **no cursor**. `GET /pages` is ordered by `page_ref` (`pages.go:183-204`), so a client cannot list more than 500 registered pages.

### 1.4 Endpoint reference
Job JSON is `dispatch.Job` (`internal/dispatch/dispatch.go:59-75`): `job_id, kind (on_demand|scheduled|backfill), lane (p0|p1|p2), mode (full|incremental), page_ref ("v:nike"|"id:123"), page_id?, status, params (incl. limits), counters?, completeness?, error?, created_at, finished_at?, pending_tasks?`.

| # | Method & path | Scope | Request | Success | Errors / notes |
|---|---|---|---|---|---|
| 1 | `POST /v1/jobs` | jobs:write | `{page (URL/vanity/id), mode: auto\|full\|incremental, ...CrawlOptions}`; header `Idempotency-Key` | **202** + `Location: /v1/jobs/{id}` + Job; **200** + existing Job when the key repeats | 400 invalid body/page/mode/options. **429 + `Retry-After: 30`** when the client already has `api.max_running_p0_jobs_per_client` (50) running jobs (`ErrTooManyJobs`), or the fleet has `api.max_running_p0_jobs` p0 jobs running (`ErrLaneFull`, 0 = no cap) (`jobs.go:78-125`) |
| 2 | `GET /v1/jobs` | data:read | `?page, status, limit, cursor` | `{jobs, next_cursor?}` | Non-admin clients see only their own jobs (`jobs.go:162-166`) |
| 3 | `GET /v1/jobs/{id}` | data:read | uuid | Job; live `counters` and `pending_tasks` come from Redis while the job is running, finalizing or cancelling (`dispatch.go:276-293`) | 400 bad uuid, 404 |
| 4 | `POST /v1/jobs/{id}/cancel` | jobs:write | – | **202** `{job_id, status:"cancelling"}` | 404 when the job is not `running`/`queued` (`dispatch.go:321-330`). Also sets the Redis cancel flag `{job:<id>}:cancel`, which workers check between pages |
| 5 | `GET /v1/jobs/{id}/results` | data:read | `?include=comments` | NDJSON (`application/x-ndjson`): one `{"type":"page","page":…}`, then `{"type":"post"}` per post, each followed by its `{"type":"comment"}` lines | 409 `"no data yet"` when `page_id` is unresolved (`jobs.go:192-239`). Comments go through the PII gate (§1.5). Flushes after each post |
| 6 | `POST /v1/pages` | pages:write | `{page, schedule?{enabled, interval_hours 1..720}, backfill?, ...CrawlOptions}` | **201** `{page_ref, created, schedule{enabled, interval_seconds, next_run_at, adaptive}, backfill_job?}` | 400 interval. Idempotent re-registration, see below (`pages.go:80-156`) |
| 7 | `GET /v1/pages` | data:read | `?limit` | `{pages:[scheduleView]}` (page_ref, page_id, enabled, tier, interval_seconds, next_run_at, last_run_at, options, consecutive_failures, adaptive, posts_per_day, last_post_at) | Non-admin clients see only pages they registered (join on `page_registrations`) |
| 8 | `GET /v1/pages/{page}` | data:read | `{page}` = id, vanity, or URL-escaped ref | `{page: Page, page_ref, schedule?}` | 400 invalid ref; 404 `"unknown page"` when the ref resolves to no page id (`pages.go:53-65`) |
| 9 | `PATCH /v1/pages/{page}` | pages:write | `{enabled?, interval_hours? (1..720), adaptive?}` | **204** | 404 `"not registered"`. Setting `interval_hours` without `adaptive` sets `adaptive=false` (`pages.go:225-268`). Not scoped to the caller's registrations |
| 10 | `DELETE /v1/pages/{page}` | pages:write | `?purge=true` | **204**, or **202** + Erasure on purge | Removes the caller's registration, and disables the schedule once no registrations remain. `purge` requires admin (403 otherwise, but the caller's registration has already been removed by then) and queues a page erasure (`pages.go:270-308`) |
| 11 | `GET /v1/pages/{page}/posts` | data:read | `?since (RFC3339), type, limit, cursor` | `{posts:[Post], next_cursor?}` | 400 invalid since. The `type` filter is applied **in Go after the LIMIT** (`data.go:110-118`), so filtered pages can be short and `next_cursor` disappears early: a pagination bug |
| 12 | `GET /v1/pages/{page}/snapshots` | data:read | `?limit` | `{snapshots:[{captured_at,name,followers_count,likes_count,verified}]}` newest first | |
| 13 | `GET /v1/posts/{post}` | data:read | numeric id | Post | 400 non-numeric; 404. Uses `post_index` to find the partition key, then `WHERE post_created_at=$1 AND post_id=$2` (`data.go:127-156`) |
| 14 | `GET /v1/posts/{post}/comments` | data:read | `?parent_id, depth (0 = top level), status (visible\|deleted), limit, cursor` | `{comments:[Comment], next_cursor?}` | 400 on a bad parameter |
| 15 | `GET /v1/posts/{post}/metrics` | data:read | `?limit` | `{metrics:[{captured_at, reaction_count, reactions, comment_count, share_count, view_count}]}` newest first | Has no `captured_at` bound, so the query probes every daily partition |
| 16 | `GET /v1/posts/{post}/media` | data:read | `?limit` | `{media:[Media]}` by `discovered_at` | |
| 17 | `GET /v1/media/{media}` | data:read | uuid | Media (incl. `status`, `s3_key`, `sha256` hex, `delete_after`, `ready_at`, `deleted_at`) | 400 when the id is not uuid-shaped |
| 18 | `POST /v1/erasure-requests` | admin | `{subject_type: author\|page, author_id?, profile_url?, page_id?}` | **202** Erasure `{id, subject_type, status:"pending", created_at}` | 400 `"unknown author"` / `"page_id is required"` / bad subject_type (`admin.go:117-161`) |
| 19 | `GET /v1/erasure-requests/{id}` | admin | uuid | Erasure `{…, stats, error, completed_at}` | 404 |
| 20 | `GET /v1/admin/queries` | admin | – | `{queries:[registry.Query]}` with `last_check`, `last_checked_at`, `last_ok_at` | |
| 21 | `PUT /v1/admin/queries/{name}` | admin | `{doc_id, friendly_name, parser, variables, parser_version?, status?, priority?}` | 200 with the saved entry (`source:"manual"`, `parser_version` defaults to 1). Crawlers reload within `fb.registry_refresh` (30s) | 400 when required fields are missing |
| 22 | `GET /v1/admin/registry-sync/runs` | admin | `?limit 1..200 (20)` | `{runs:[{id, started_at, finished_at, status, report}]}` | |

**CrawlOptions** (`jobs.go:17-69`, OpenAPI `CrawlOptions`) are `max_age_days` (1..`limits.max_age_cap_days`), `max_posts`, `max_comments_per_post`, `max_depth` (1..5), `include_comments`, `include_replies`, `include_media`, `include_comment_media`, and `video_policy` (`none|thumbnail|sd|hd`). They overlay `task.DefaultLimits(cfg.Limits, backfill)`. Backfills take `limits.backfill_video_policy` (`thumbnail`) instead of `video_policy` (`sd`) (`internal/crawl/task/task.go:130-145`).

**Job creation** (`internal/dispatch/dispatch.go:111-241`):
- **Idempotency:** a lookup by `(api_client_id, idempotency_key)` first. A unique index (`jobs_idem_idx`) backs up concurrent requests: when the insert fails, the existing row is returned.
- **mode=auto** becomes `incremental` when `crawl_cursors` already holds a `page_timeline` row for the resolved page, and `full` otherwise.
- **Caps** are enforced in one transaction under `pg_advisory_xact_lock(hashtextextended('fbs:lane:p0',0))` and then `…('fbs:client:<uuid>')`, by counting jobs with `status IN ('running','finalizing')`.
  - Caveat: the per-client count includes **all of the client's jobs, backfills too**. A client that bulk-registers pages can therefore get 429s on on-demand jobs until its backfills finish.
- Next, the root page task is built. When the planner attached post refreshes (§4.4), extra root post tasks are added and recorded in the job's Redis `posts` hash as `pending`.
- Every root task id is registered with `Jobs.Spawn` before `ProduceSync` to `fbs.task.<stage>.<lane>`. If any step fails, the job row is set to `failed` with the error.
- New jobs start in `status='running'`. The `queued` and `finalizing` statuses exist in the schema and in some queries, but nothing writes them.

**Page registration semantics** (`pages.go:80-156`):
- `page_schedule` gets an upsert keyed by the normalised `page_ref`:
  - `enabled = old OR new`.
  - `interval_seconds`: the old value is kept when the new request is adaptive, otherwise `LEAST(old,new)`. An explicit interval can only shorten the schedule.
  - `adaptive = old AND new`.
  - `options`, `max_age_days` and `max_posts` are only set on insert, so the first registration's crawl options stay.
- The first `next_run_at` is `now + interval + rand(0..interval/10)`.
- A `page_registrations(api_client_id, page_ref)` row is added.
- A **backfill job** (kind `backfill`, lane **p2**, mode `full`, backfill limits, no cap) starts when `backfill:true` is passed, or when `backfill` is omitted and `old.last_job_id IS NULL` (the page never ran). Its id is written to `page_schedule.last_job_id`.
- **Page refs** come from `task.NormalizeRef` (`internal/crawl/task/task.go:163-215`): `id:<n>` for numeric ids, `profile.php?id=` and `pages/<name>/<id>` URLs, and `v:<lowercase vanity>` otherwise. `ResolvePageID` looks up `pages.vanity` and then `page_schedule.page_id` (`dispatch.go:96-107`).

### 1.5 PII gating (`internal/api/data.go:158-246`)
- Comments always carry `author_key`, the hex of the HMAC.
- The full `author{id, name, profile_url, avatar_url, type, verified}` is returned **only with `pii:read`**, and only when id or name is present. Without the scope, only `author{type, verified}` is returned.
- The join to `comment_authors` is `AND NOT c.redacted`, so redacted comments never show an author.
- In `pii_mode: pseudonymized` the identity fields are already NULL at the source (§6), so `pii:read` adds nothing.
- Post `author_id`/`author_name` (the page) are stored but not exposed by the API.

### 1.6 API caveats found
- `GET/cancel/results` on `/jobs/{id}` do **not** check job ownership: any `data:read` or `jobs:write` key can read or cancel any job id it knows. Only `GET /jobs` filters by owner.
- `PATCH /pages/{page}` is not scoped to the caller's registrations.
- **Job results depend on `last_seen_at >= job.created_at - 1m`** (`jobs.go:221`). The persister only refreshes `last_seen_at` when the content hash changed or the stored value is more than a day old (§3). An unchanged post re-seen by an incremental job is therefore **missing from `/results`**.
- `middleware.Timeout(60s)` covers the NDJSON stream, so long result streams are cut off at 60 s.
- `http.read_timeout`/`write_timeout` are not used. `httpserver.Serve` only sets `ReadHeaderTimeout: 10s` (`internal/platform/httpserver/ops.go:64`).

---

## 2. Database schema (`migrations/*.sql`, goose, embedded via `migrations/embed.go`)

Migrations run through `pressly/goose` v3 over `stdlib.OpenDBFromPool` (`internal/store/migrate.go`). `fbctl migrate up` also seeds the query registry and creates partitions (§9).

### 2.1 Tables (00001_init.sql unless noted)

**`pages`** (`00001:4-23`)
- Columns: `page_id bigint PK`, `vanity`, `url`, `name`, `categories text[]`, `intro`, `followers_count`, `likes_count`, `verified`, `profile_pic_url`, `cover_url`, `contact jsonb`, `extra jsonb`, `status text NOT NULL DEFAULT 'active'`, `content_hash bytea`, `first_seen_at NOT NULL DEFAULT now()`, `last_scraped_at`.
- Status values: `active | unavailable | restricted | suppressed`.
- Index: `UNIQUE pages_vanity_idx ON lower(vanity) WHERE vanity IS NOT NULL`.

**`page_snapshots`**: profile time series (`00001:26-34`)
- Columns: `(page_id, captured_at) PK`, `name`, `followers_count`, `likes_count`, `verified`.
- **`PARTITION BY RANGE (captured_at)`, monthly.**
- Written only when the page is new or its followers, likes or name changed (§3.4).

**`posts`** (`00001:38-77`)
- **PK `(post_created_at, post_id)`, `PARTITION BY RANGE (post_created_at)` monthly.** A post and its comments share a partition month, so retention is a `DROP`. Every PK starts with the post (the ADR mentions Citus later).
- Columns: `page_id`, `story_id`, `permalink`, `post_type NOT NULL` (lower-cased enum: `status|photo|album|video|reel|link|share|event|live`), `text`, `text_ranges jsonb`, `hashtags text[]`, `links jsonb`, `shared_post_id`, `shared_post_url`, `is_pinned`, `is_paid_partnership`, `author_id`, `author_name`, `reaction_count`, `reactions jsonb` (by type), `reactions_complete`, `comment_count_total` (Facebook's reported total, incl. replies), `share_count`, `view_count`, `feedback_id`, `media_ids text[]`, `extra jsonb`.
- Coverage columns, written by the finalizer: `comments_collected`, `replies_collected`, `completeness real`, `comments_state NOT NULL DEFAULT 'none'` (`none|partial|complete|truncated`), `comments_synced_at`.
- Also `content_hash bytea NOT NULL`, `status NOT NULL DEFAULT 'visible'` (or `unavailable`), and `first_seen_at`, `last_seen_at NOT NULL`.
- Indexes: `posts_page_idx (page_id, post_created_at DESC)`, `posts_hashtags_idx GIN(hashtags)`.

**`post_index`** (`00001:80-85`)
- `post_id PK → (page_id, post_created_at)`, so lookups by post id prune to one partition.
- Index `(page_id, post_created_at DESC)`.
- Written with `ON CONFLICT DO NOTHING`, so the first value wins.

**`comments`** (`00001:88-120` + `00004`)
- **PK `(post_created_at, post_id, comment_id)`, partitioned monthly by the post's creation time.**
- Columns: `page_id`, `graph_id`, `parent_comment_id`, `root_comment_id`, `depth smallint NOT NULL` (0 = top-level, >0 = replies), `author_key bytea`, `author_id` (NULL under pseudonymization or erasure), `text`, `text_ranges`, `attachments jsonb`, `created_at`, `edited`, `is_op`, `reaction_count`, `reactions`, `reply_count`, `feedback_id`, `url`, `extra`, `status NOT NULL DEFAULT 'visible'` (`visible|deleted`), `redacted bool NOT NULL DEFAULT false`, `content_hash NOT NULL`, `first_seen_at`, `last_seen_at`.
- 00004 adds `edit_count int NOT NULL DEFAULT 0`, `edited_at`, `deleted_at`.
- Indexes: `comments_thread_idx (post_id, created_at, comment_id)`, `comments_author_idx (author_key)` (used by erasure), `comments_parent_idx (post_id, parent_comment_id) WHERE parent_comment_id IS NOT NULL`.

**`comment_authors`** (`00001:122-135`)
- Columns: `author_key bytea PK`, `author_id`, `name`, `profile_url`, `avatar_url`, `author_type` (`User|Page`), `verified`, `first_seen_at`, `last_seen_at`.
- Indexes: `profile_url`, `author_id`, `last_seen_at` (the last for retention).

**`post_metrics`**: engagement time series, **a row only when values changed** (`00001:138-148`)
- Columns: `(post_id, captured_at) PK`, `page_id`, `reaction_count`, `reactions`, `comment_count`, `share_count`, `view_count`.
- **Daily partitions** on `captured_at`.
- Built by the persister from the post upsert's `RETURNING` (§3.3), **not** from the `fb.post_metrics.v1` topic, which the persister does not consume (`internal/topics/topics.go:48-50`). That topic exists for downstream consumers only.

**`media`**: operational state of temporary downloads (`00001:151-186`)
- Columns:
  - Identity: `media_id uuid PK` (UUIDv5 of fb_media_id + variant), `fb_media_id`, `variant`, `kind`, `owner_type` (`post|comment|page`).
  - Owners: `owner_id`, `post_id`, `page_id`, `comment_id`.
  - Source and metadata: `source_url`, `source_expires_at`, `width`, `height`, `duration_ms`, `mime`, `alt_text`.
  - Download state: `status NOT NULL DEFAULT 'discovered'` (`discovered|stored|ready|deleted|failed|expired|skipped`; `stored` and `skipped` are never written), `s3_bucket`, `s3_key`, `size_bytes`, `sha256 bytea`, `attempts`, `retryable_failures`, `last_error`, `delete_after`.
  - Timestamps: `discovered_at`, `stored_at`, `ready_at`, `processed_at`, `deleted_at`.
- Indexes: `(post_id)`, `(delete_after) WHERE status='ready'` (TTL sweep), `(comment_id) WHERE comment_id IS NOT NULL`.

**`media_acks`** (`00001:188-195`)
- `(media_id FK→media ON DELETE CASCADE, consumer) PK`, `status` (`processed|failed_permanent|failed_retryable`), `error`, `acked_at`.

**`crawl_cursors`** (`00001:198-211`), written by the crawlers
- `(scope_type 'page_timeline', scope_id, ordering 'default') PK`, `cursor`, `newest_seen_at` (the incremental watermark), `oldest_seen_at` (the depth of known history, used for the posting rate), `state` (`complete|partial|truncated`), `truncated_reason`, `last_full_at`, `last_incremental_at`, `updated_at`.

**`api_clients`** (`00001:213-222`): see §1.2.

**`jobs`** (`00001:224-244`)
- Columns: `job_id uuid PK` (UUIDv7), `api_client_id FK`, `kind`, `lane smallint` (the proto enum: 1=p0, 2=p1, 3=p2), `mode`, `page_ref`, `page_id`, `params jsonb` (incl. `limits`), `status`, `idempotency_key`, `counters jsonb`, `completeness jsonb`, `error jsonb`, `created_at`, `dispatched_at`, `finished_at`.
- Indexes: `UNIQUE (api_client_id, idempotency_key) WHERE idempotency_key IS NOT NULL`, `(page_ref, created_at DESC)`, `(status, created_at) WHERE status IN ('queued','running','finalizing')`.

**`page_schedule`** (`00001:247-263` + `00002`)
- Columns: `page_ref PK`, `page_id`, `enabled`, `tier text DEFAULT 'T2'`, `interval_seconds`, `next_run_at`, `options jsonb`, `max_age_days`, `max_posts`, `consecutive_failures`, `paused_until`, `last_job_id`, `last_run_at`, `created_at`.
- `paused_until` is read by the planner but written by no code: it is an operator lever.
- 00002 adds `adaptive bool NOT NULL DEFAULT true`, `posts_per_day double`, `last_post_at`, `tier_changed_at`.
- Index: `page_schedule_due_idx (next_run_at) WHERE enabled`.

**`page_registrations`** (`00001:265-270`)
- `(api_client_id, page_ref FK→page_schedule ON DELETE CASCADE) PK`, `created_at`. This is the many-clients-to-one-schedule mapping.

**`erasure_requests`** (`00001:273-286`)
- Columns: `id uuid PK`, `api_client_id`, `subject_type` (`author|page`), `author_key`, `page_id`, `input jsonb`, `status` (`pending|done|failed`), `stats jsonb`, `error`, `created_at`, `completed_at`.
- Index `(created_at) WHERE status='pending'`.

**`suppression`** (`00001:288-293`)
- `(subject_type, subject_key) PK`, where `subject_key` is the hex author_key or the decimal page id, plus `created_at`.

**`fb_queries`** (`00001:296-311` + `00003`)
- The hot-reloaded Facebook GraphQL registry: `(name, doc_id) PK`, `friendly_name`, `variables_template jsonb`, `parser`, `parser_version`, `status` (`candidate|active|fallback|retired`), `priority`, `source` (`seed|bundle|har|manual`), `discovered_at`, `promoted_at`, `last_ok_at`, `updated_at`.
- 00003 adds `last_checked_at`, `last_check text` (`ok|stale|drift|inconclusive: <reason>`), `check_failures int`.

**`fb_client_params`** (`00001:313-317`): key/value client params.

**`registry_sync_runs`** (`00003:44-51`)
- `run_id bigint identity PK`, `started_at`, `finished_at`, `status` (`ok|degraded|error`), `report jsonb`. Index `(started_at DESC)`.

**`post_refresh`** (`00002:14-25`): comment refresh plan for posts older than the timeline overlap
- Columns: `post_id PK`, `page_id`, `post_created_at`, `next_refresh_at`, `last_refresh_at`, `refreshes`, `unchanged` (consecutive refreshes that found nothing new), `last_comment_count`, `recrawls`.
- Index: `(page_id, next_refresh_at)`.

### 2.2 Partitioning mechanics (`internal/store/partitions.go`)
- **Monthly tables:** `posts`, `comments` and `page_snapshots` (`:21`). Names are `<table>_yYYYYmMM` (`:191`).
- **Daily tables:** `post_metrics` (`:24`). Names are `<table>_dYYYYMMDD` (`:195`). Ranges are UTC.
- **There is no DEFAULT partition.** An insert into a missing partition fails with SQLSTATE `23514` and "no partition" (`IsNoPartition`, `:90-93`).
- **Creation** (`ensureRange`, `:95-139`) checks `to_regclass` first. Under `pg_advisory_xact_lock(4_200_001)`, which serialises DDL across processes, it re-checks and runs `CREATE TABLE … PARTITION OF … FOR VALUES FROM … TO …`.
  - `posts` and `comments` partitions get `WITH (fillfactor = 85)` for HOT updates (`:116`).
  - Then, best-effort, `ALTER COLUMN … SET COMPRESSION lz4` on `posts.{text,text_ranges,links,extra}` and `comments.{text,text_ranges,attachments,extra}` (`:27-30,129-137`).
- **Who creates partitions:**
  1. `fbctl migrate up`.
  2. The scheduler's hourly maintenance: `EnsurePartitions` for monthly tables from `max_age_cap_days/30+1` months back (default 37) to `partition_months_ahead` (3) ahead, and for daily tables from 1 day back to 14 days ahead (`internal/scheduler/loops.go:283-292`).
  3. The persister on demand when an insert hits a missing partition (§3.5).
- **Dropping** (`DropPartitionsBefore`, `:145-181`): lists children through `pg_inherits`, parses the name back into a range, and runs `DROP TABLE` when the partition's upper bound is ≤ the cutoff.

### 2.3 Retention (maintenance loop, hourly; `internal/scheduler/loops.go:294-338`)

| Data | Mechanism | Default |
|---|---|---|
| `comments` | DROP monthly partitions whose post-month ended before `now - comments_days` | 365 d |
| `post_metrics` | DROP daily partitions before `now - metrics_days` | 180 d |
| `jobs` (finished) | `DELETE … LIMIT 5000` per tick | 90 d (`jobs_days`) |
| `media` rows in `deleted/expired/failed/skipped` | `DELETE … LIMIT 5000` by `discovered_at` | 30 d |
| `comment_authors` inactive | `DELETE … LIMIT 5000` by `last_seen_at` | 365 d |
| `registry_sync_runs` | DELETE by `started_at` | `jobs_days` |
| `post_refresh` | rows older than the max refresh age + 24h, or for unscheduled pages, `LIMIT 20000` | – |
| `posts`, `post_index`, `page_snapshots`, `pages` | **never dropped** | – |

Observations:
- With the defaults (`comments_days 365` < `max_age_cap_days 1095`), every hourly tick **creates about 24 old `comments` partitions and then drops them** (create window: 37 months; drop cutoff: 12 months). On top of that, a deep backfill stores comments of posts older than a year, and they vanish within the hour.
- At 300k pages/day, the `jobs` delete cap of 5000/hour (120k/day) can fall behind the job creation rate.
- `privacy.retention.raw_payload_days` is declared but **not enforced anywhere**: no lifecycle rule on the raw bucket.

---

## 3. Persister (`fbs-persister`, `internal/persist`)

### 3.1 Consumption pattern (`internal/persist/consumer.go`)
- It uses its own franz-go client, not the generic `platform/kafka` consumer. Group `<prefix>fbs-persister` (`persist.Group`, which the finaliser watches) consumes `<prefix>{fb.page.v1, fb.post.v1, fb.comment.v1, fb.media.v1}`.
- Client options: `DisableAutoCommit`, `BlockRebalanceOnPoll`, the `CooperativeStickyBalancer`, reset to the earliest offset, `FetchMaxWait 250ms` (`:31-52`).
- **Batching** (`:56-82`): poll until `maxBatch` records (2000) or the `linger` deadline (250ms, set in `cmd/fbs-persister/main.go:104`), or until a poll returns nothing after some records arrived.
- **Commit after write:** `CommitRecords(recs...)` only after the whole batch was written. Then `AllowRebalance()` runs, so a rebalance never splits a batch.
- **Failure handling:** a failed batch is re-written every 2 s **forever**, and never committed past unwritten data (`:83-103`). A shutdown leaves the batch uncommitted, so it is redelivered.
- **Per-batch order** (`flush`, `:111-157`): decode by topic, then write **pages → posts → comments → media**. Posts go before comments so that `post_index` is ahead of API comment lookups. Entity rows have no FKs, so cross-topic arrival order does not matter: comments carry `post_created_at` and can land before their post.

### 3.2 Idempotency and dedupe
- Within a batch, `latest()` keeps the newest observation per entity key by `obs.observed_at` (`writer.go:26-41`).
- Across batches, the upserts are content-hash guarded: `obs.content_hash` is an xxh3-128 over the message without its observation, computed by the emitter (`internal/crawl/emit/emit.go:50-65`, `stamp`). Re-delivering a batch is a no-op.
- `first_seen_at` is never updated. `last_seen_at` moves only when content changed or the stored value is more than **1 day** old. This deliberately caps write amplification, and it is the reason behind the `/results` caveat in §1.6.

### 3.3 The COPY + no-op upsert pattern (posts, `writer.go:72-155`)
Each batch runs in one transaction:
```sql
CREATE TEMP TABLE tmp_posts (LIKE posts INCLUDING DEFAULTS) ON COMMIT DROP;
COPY tmp_posts (post_created_at, post_id, page_id, … , content_hash, status, first_seen_at, last_seen_at) FROM STDIN;  -- pgx CopyFrom

INSERT INTO posts AS t (<cols>) SELECT <cols> FROM tmp_posts
ON CONFLICT (post_created_at, post_id) DO UPDATE SET <every col except post_created_at, post_id, first_seen_at> = EXCLUDED.<col>
WHERE t.content_hash IS DISTINCT FROM EXCLUDED.content_hash          -- no-op when unchanged
   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'       -- daily "still alive" touch
RETURNING new.post_id, new.page_id, new.last_seen_at, new.reaction_count, new.reactions,
          new.comment_count_total, new.share_count, new.view_count,
          (old.post_id IS NULL OR old.reaction_count IS DISTINCT FROM new.reaction_count
           OR old.comment_count_total IS DISTINCT FROM new.comment_count_total
           OR old.share_count IS DISTINCT FROM new.share_count
           OR old.view_count IS DISTINCT FROM new.view_count) AS metrics_changed;   -- PG18 old/new

INSERT INTO post_index (post_id, page_id, post_created_at) SELECT … FROM tmp_posts ON CONFLICT (post_id) DO NOTHING;

-- only for rows with metrics_changed:
CREATE TEMP TABLE tmp_metrics (LIKE post_metrics) ON COMMIT DROP;  COPY …;
INSERT INTO post_metrics SELECT * FROM tmp_metrics ON CONFLICT DO NOTHING;   -- captured_at = observation time
```
- The rows returned are the rows actually written, which yields `fbs_persist_rows_total{entity="post",op="written"|"unchanged"}`.
- **Change-only `post_metrics`:** a sample is appended only when the post is new or one of its four counts changed (`writer.go:105-109,128-130`).
- A missing daily `post_metrics` partition is **silently ignored** (`writer.go:166-170`), so that sample is lost. The scheduler pre-creates partitions 14 days ahead.
- Unlike comments, posts have **no stale-observation guard** (`EXCLUDED.last_seen_at >= t.last_seen_at`). An older replayed observation with a different hash overwrites newer data.
- Row mapping: `rows.go:86-119`. Empty strings become NULL, empty JSON arrays and objects become NULL (`jsonOf`, `:17-26`), `post_type` is the lower-cased enum, and `status` is `visible` or `unavailable`.

### 3.4 Comments, authors, pages, media
- **Comments** (`writer.go:178-328`, all in one transaction):
  - Records with `status=DELETED` go to `markDeleted`. Other records are COPYed into `tmp_comments` and upserted with:
    ```sql
    ON CONFLICT (post_created_at, post_id, comment_id) DO UPDATE SET <cols> = EXCLUDED.<cols>,
      edit_count = t.edit_count + CASE WHEN (NOT EXCLUDED.redacted AND t.text IS DISTINCT FROM EXCLUDED.text) THEN 1 ELSE 0 END,
      edited_at  = CASE WHEN <edited> THEN EXCLUDED.last_seen_at ELSE t.edited_at END,
      status     = CASE WHEN (t.status='deleted' AND EXCLUDED.last_seen_at > t.deleted_at) THEN 'visible' ELSE t.status END,
      deleted_at = CASE WHEN <restored> THEN NULL ELSE t.deleted_at END
    WHERE NOT t.redacted                                   -- erased content is never re-written
      AND EXCLUDED.last_seen_at >= t.last_seen_at          -- stale (replayed) observation changes nothing
      AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash
           OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
           OR <restored>)
    RETURNING new.edit_count > coalesce(old.edit_count,0), coalesce(old.status='deleted' AND new.status='visible', false), …
    ```
  - **Edit tracking (migration 00004):** text that differs on a newer observation is an edit (`edit_count++`, `edited_at = observation time`). Redaction does not count as an edit.
  - **Restore:** a top-level comment seen after its deletion becomes visible again, and so do the replies deleted in the same check (same `root_comment_id` and identical `deleted_at`, `writer.go:317-327`). A reply deleted on its own stays deleted (tested in `writer_test.go:239-291`).
  - **`markDeleted`** (`writer.go:340-363`):
    ```sql
    UPDATE comments … SET status='deleted', deleted_at=d.at
    FROM unnest(…)
    WHERE status='visible' AND NOT redacted AND last_seen_at < d.at
    ```
    A replayed deletion older than a later sighting does nothing.
  - **Authors:** `comment_authors` is upserted from the batch only for non-redacted comments with a key. It updates when the name or id changed or `last_seen_at` is more than a day old, and `last_seen_at = GREATEST(...)` (`writer.go:218-236`).
- **Pages** (`writer.go:367-439`) use a transaction **per page**, not COPY, and the upsert has **no hash guard**: every observation rewrites the row and sets `status='active'`.
  - `RETURNING old/new` detects a new page or a change in followers, likes or name, which triggers a `page_snapshots` insert (`ON CONFLICT DO NOTHING`, a missing partition tolerated).
  - The page resolves references: `UPDATE page_schedule SET page_id` and `UPDATE jobs SET page_id … WHERE page_ref=requested_ref AND status IN ('running','finalizing')`.
  - An `UNAVAILABLE` page only sets `status='unavailable'`.
- **Media** (`writer.go:442-475`) upserts metadata only. It never touches download state, and only updates when `source_url` changed (a refreshed signed URL): it sets `source_url` and `source_expires_at` and fills in dims, duration and alt text.

### 3.5 Partitions and poison messages
- **Partitions:** `withPartitions` (`writer.go:44-60`) runs the batch. On "no partition" it creates the partitions for every post-month in the batch (for posts, the `comments` partition of the same month too) and retries once.
- **Poison messages** are where this design is weak:
  - On a protobuf decode error the log says "undecodable record skipped", but the code still appends the zero or partially decoded message (`consumer.go:122-140`).
  - The persister has no DLQ, and a batch that fails permanently is retried forever (`consumer.go:89-98`).
  - A single bad record therefore **stalls the whole persister and every job finalisation**: for example a Post without `obs`, which violates `content_hash NOT NULL`, or a Media with a non-uuid id. The finaliser then falls back to its 10-minute timeout.
  - **[suggestion]** In the Instagram build, quarantine bad records (bisect the batch, send offenders to a `*.dlq` topic) instead of retrying forever.

---

## 4. Scheduler (`fbs-scheduler`, `internal/scheduler`, `internal/platform/leader`)

### 4.1 Leader election (`internal/platform/leader/leader.go`)
- Election uses a **Postgres session-level advisory lock**, which behaves the same on Compose and Kubernetes. The process acquires a dedicated pool connection and calls `pg_try_advisory_lock(4_200_100)` (`cmd/fbs-scheduler/main.go:30`).
- If the lock is taken, the process retries every 5 s.
- The holder runs the loops under a cancellable context and **pings the lock connection every 5 s**. If the ping fails it cancels the loops, waits for them, and goes back to competing. On exit it calls `pg_advisory_unlock`.
- The ops server runs on every replica. Leader-only gauges are bracketed by `fbs_scheduler_leader` being 1 or 0, and `ConsumerLag.Reset()` on loss (`cmd/fbs-scheduler/main.go:108-113`).

### 4.2 Loops (each `scheduler.Every` runs immediately, then on a ticker; errors are logged; `loops.go:31-44`)

| Loop | Interval | What |
|---|---|---|
| planner | `scheduler.planner_interval` 30s | §4.3 |
| finalizer | `scheduler.finalizer_interval` 2s | §4.6 |
| reaper | 1m | §4.8 |
| maintenance | 1h | partitions and retention (§2.2–2.3) |
| erasure | 10s | GDPR (§6) |
| tier-gauges | 5m | `fbs_scheduled_pages{tier}`, `fbs_schedule_lag_seconds{tier}` |
| consumer-lag | 30s | `fbs_kafka_consumer_lag{group,topic}` for every group starting `<prefix>fbs-` (`lag.go`). Keeps the last values on transient errors and deletes series for groups that went away |
| budget | `scheduler.budget.interval` 1m | only if `budget.enabled` and `requests_per_day>0 \|\| bytes_per_day>0`. **Both default to 0, so it is off by default** (§4.9) |
| aimd | `fb.aimd.interval` 10s | fleet RPS ceiling (out of scope; runs here because it must be a singleton) |

### 4.3 Planner (`loops.go:69-148`)
A single statement claims due pages and pushes `next_run_at` forward:
```sql
WITH d AS (SELECT page_ref FROM page_schedule
           WHERE enabled AND next_run_at <= now() AND (paused_until IS NULL OR paused_until < now())
           ORDER BY next_run_at LIMIT $batch FOR UPDATE SKIP LOCKED)
UPDATE page_schedule s SET
  next_run_at = now() + make_interval(secs => s.interval_seconds
        * least(power(2, s.consecutive_failures), 2592000.0 / s.interval_seconds)   -- failure backoff, capped at 30 days
        * CASE WHEN s.tier = ANY($stretchTiers) THEN $stretch ELSE 1.0 END           -- budget stretch
        * (1 + (random()*2 - 1) * $jitter)),                                        -- ±10% jitter
  last_run_at = now()
FROM d WHERE s.page_ref = d.page_ref
RETURNING s.page_ref, s.page_id, s.tier, s.options, s.max_age_days, s.max_posts, s.last_job_id;
```
For each page it claims, the planner then:
- **Skips the slot** if `last_job_id` is still `running`, `finalizing` or `cancelling` (`loops.go:107-113`).
- Builds limits from config defaults, overlays the stored per-page `options` JSON (`applyOptions`, `:174-215`), then applies `max_age_days` and `max_posts`.
- Asks `Adaptive.DueRefreshes` for due post refreshes. This only happens when the page id is known and comments are included.
- Dispatches a **`scheduled`, lane p1, mode `incremental`** job with the refresh roots attached. After a successful dispatch it calls `commit()`, which records the refreshes, and sets `last_job_id`.
  - A failed dispatch leaves the refreshes due.
  - The page's `next_run_at` has already moved, so the page waits a full interval.
- Sets `fbs_schedule_lag_seconds{tier="all"}`, the age of the oldest due row.

`consecutive_failures` resets to 0 when a job finishes `completed` or `partial`, and increments otherwise (`finalizer.go:193-197`). Jobs closed by the reaper do not touch it.

### 4.4 Adaptive tiers (`adaptive.go`, `00002`)
`Adaptive.AfterJob(pageID)` runs from the finaliser before the job turns terminal, and only for enabled scheduled pages (`adaptive.go:81-132`):
1. **Posting rate:**
   - `recent` = the number of posts with `post_created_at > now - rate_window` (14 d).
   - `last` = `max(post_created_at)` over the past `max(rate_window, dormant_after)`.
   - `first` = `crawl_cursors.oldest_seen_at`, how deep the timeline was ever read.
   - `PostingRate = recent / span * 24`, where `span = rate_window` unless the known history is shorter. In that case `span = max(now - first, 24h)` (`:43-49`).
   - Examples (tests): 14 posts in 14 days gives 1/day. With only 7 days of history it gives 2/day. History shorter than a day counts as a day.
2. **TierFor** (`:25-38`): dormant comes first. A page that never posted, or has not posted for `dormant_after` (90 d), goes to **T4** with `dormant_interval` (168h). Otherwise the page gets the first tier (sorted by `min_posts_per_day` descending) whose minimum the rate reaches:

   | Tier | min posts/day | interval |
   |---|---|---|
   | T0 | ≥ 10 | 3h |
   | T1 | ≥ 1 | 12h |
   | T2 | ≥ 0.142857 (weekly) | 24h |
   | T3 | ≥ 0 | 72h |
   | T4 | dormant 90 d | 168h |

3. Update `page_schedule`:
   - Always set `posts_per_day`, `last_post_at` and `tier` (`tier_changed_at` only when the tier changed).
   - **Only for `adaptive` rows**, set `interval_seconds` to the tier's interval, and pull `next_run_at` in to `now + new interval` when the new interval is shorter.
   - Pages registered with an explicit `interval_hours` keep their interval but still get a tier label.
4. **Register posts for refresh:**
   - Delete `post_refresh` rows older than the max refresh age, i.e. the max of the windows' `max_age` and `recrawl_at` (30 d by default).
   - Insert every visible post newer than that with `next_refresh_at = post_created_at + incremental_overlap` (24h) and `last_comment_count = comments_collected`, `ON CONFLICT DO NOTHING`.
   - Posts younger than the overlap are re-read by every timeline scan anyway.

### 4.5 Post refresh by post age, and the 7/30-day recrawls (`adaptive.go:150-295`)
`DueRefreshes(page, tier)` selects up to `post_refresh.max_per_job` (50) due rows joined to `posts`. It also takes the newest stored comment's `created_at`. For each row, `plan()` decides:
- **Backoff:** when this is not the first refresh and `comments_collected == last_comment_count`, `unchanged++`. Otherwise `unchanged = 0`.
- **Recrawls:**
  - Every age in `recrawl_at` (168h, 720h) that the post has passed advances `recrawls`. Several ages passed at once count as one.
  - A **recrawl only happens when the post is "busy"**: `comments_collected ≥ recrawl_min_comments` (20). A quiet post still advances the counter without a recrawl.
- **Window:** the first entry of `windows` with `age < max_age` whose `tiers` is empty or contains the page's tier:
  - up to 7 d: every 24h;
  - 7–14 d: every 72h;
  - 14–30 d: every 168h, **T0/T1 pages only**.

  The next refresh is `now + interval × 2^min(unchanged, max_backoff=3)`.
- If a recrawl is still pending and the post is busy, `next = min(next, created + recrawl_at[recrawls])`.
- With no recrawl and no matching window, the row is postponed to `next`, or **dropped** when there is no next.
- **Mode of the post task:**
  - `RECRAWL`: a full comment pass, which feeds deletion detection.
  - `INCREMENTAL`: newest-first from `CommentsWatermark` = the newest stored comment, so an unchanged post costs one request.
  - `FULL`: when nothing is stored yet.
- The `PostScope` sets `ReportedCommentCount = CommentsSeen = posts.comment_count_total`, so the crawler does not mistake the gap between replies and the total for growth. `FeedbackId` is derived from the post id when it is missing (Facebook-specific).
- The planner calls `commit()` only after dispatch. It sends a `pgx.Batch` that deletes, postpones or records each refresh, and increments `fbs_post_refreshes_total{kind=refresh|recrawl}`.

Worked examples from `adaptive_test.go:74-162`:
- A 3-day-old post on T1 refreshes incrementally every 24h. Two unchanged refreshes push the interval to 4 days, capped at 8 days.
- At 7 days with 40 comments it gets a RECRAWL, then the 72h window.
- A 20-day-old post on a T2 page is postponed to its 30-day recrawl when busy, and dropped when quiet. On a T1 page it is refreshed weekly.
- At 30 days it gets the final recrawl, then the row is dropped.

### 4.6 Finaliser and completeness (`finalizer.go`)
**Ready set:**
- Workers call `Tracker.Done`, which decrements the job's Redis `pending` counter (`{job:<id>}:pending`, `pending = |spawned| − |done|`, kept in Lua).
- When pending reaches 0, the job id is `SADD`ed to `jobs:finalize` (`internal/crawl/jobs/jobs.go:86-111`).

**Tick** (`finalizer.go:60-120`):
1. Take up to 500 ids with `SRANDMEMBER`. Ids stay in the set until they are finalised, so a leader change loses nothing.
2. For new ids, snapshot the **end offsets of the four entity topics**.
3. A batch of jobs is finalised once `GroupCaughtUp(persister group, ends)` holds, meaning committed ≥ end on every partition (`internal/platform/kafka/admin.go:263-280`), **or once `finalize_timeout` (10m) passes**, in which case the finaliser logs a warning and finalises anyway.
4. After each job, `SREM` the id from the set.

**`finalize(job)`** (`:134-220`):
1. Load the job row. Skip it if it is already terminal.
2. Read `Status` (counters, pending) and `Posts`: the per-job hash of `post_id → created|reported|state`, with states `pending|complete|truncated|skipped|failed`.
3. **Deletion detection** (§4.7).
4. **Coverage** (`:360-435`), for every post whose state is complete, truncated, failed or pending (**skipped posts are excluded**):
   ```sql
   UPDATE posts p SET comments_collected = top, replies_collected = replies,
          completeness = CASE WHEN coalesce(comment_count_total,0)=0 THEN 1
                              ELSE least(1.0, (top+replies)::real / comment_count_total) END,
          comments_state = state ('pending'/'failed' → 'partial'), comments_synced_at = now()
   ```
   - `top` and `replies` count **all stored visible** comments (depth 0, and depth > 0) for the post, not only those seen in this job.
   - The job summary (`Completeness`, `:123-132`):
     - Post counts: `posts`, `posts_complete`, `posts_truncated`, `posts_failed` (pending plus failed).
     - `comments_reported = Σ comment_count_total`; `comments_collected = Σ (top + replies)`.
     - `min_ratio`, and `median_ratio = sorted[len/2]` (the upper median), both rounded to 3 decimals. Both are 1 when there are no posts.
   - Each ratio is observed in `fbs_comment_completeness_ratio{lane}`.
   - Note: `posts_failed` is stored in `jobs.completeness`, but **the `fb.job.v1` proto `Completeness` has no such field** (`api/proto/securebrand/fb/v1/job.proto`), so it is dropped from the event.
5. **Final status** (`:170-178`), first match wins:
   - `cancelled` if the job was `cancelling`.
   - Else `partial` if any post was truncated or failed, or the `tasks_failed` or `timelines_truncated` counters are above 0.
   - Else `failed` if `pages_unavailable > 0 && posts == 0`.
   - Else `completed`.
6. Unless the job was cancelled, `Adaptive.AfterJob(page)` runs **before** the job turns terminal ("a finished job means an updated schedule").
7. `UPDATE jobs SET status, counters, completeness, finished_at`. Update `page_schedule.consecutive_failures` (and `page_id` when null) `WHERE last_job_id = job`.
8. Publish **`fb.job.v1` `JobEvent`** with `event_id = <job>:<status>`, kind, page, counters and completeness.
9. Increment `fbs_jobs_total{status,lane}`, and shorten the Redis keys' TTL to 24h.

Job counters seen in the code and the OpenAPI spec: `posts, comments, replies, requests, media_queued, pages_fetched, threads_spawned, tasks_failed, timelines_truncated, pages_unavailable, pages_suppressed, comments_deleted`.

### 4.7 Deletion detection (`finalizer.go:237-356`; config `post_refresh.detect_deletions`, `deletion_count_slack`)
- A post task that reads a known post's comments from the start (a RECRAWL, or a FULL job on a post crawled before) records a **pass** in Redis (`internal/crawl/jobs/jobs.go:194-329`). The pass appends `(comment_id, reply_count)` for every top-level comment it read, packed into 12-byte entries, and marks the post when it reached the last page, together with Facebook's `reported` count.
- **Completeness gate:** a pass counts only if `covered = Σ(1 + replies)` is within `round(slack × reported)` of `reported`. The default slack is 0, which means an exact match. Otherwise the post is skipped (`fbs_deletion_checks_total{outcome="skipped_partial"}`).
- When the gate passes, every stored visible, non-redacted top-level comment with `first_seen_at < pass start` that was **not** seen is marked deleted, **together with its visible replies**.
- These deletions are published on `fb.comment.v1` as `Comment{status: DELETED}` with ids only. All of them share one timestamp, so a later re-sighting restores the comment and its replies together.
- `persist.MarkDeleted` is called directly, and `comments_deleted` is added to the counters before the pass records are cleared, which makes retries idempotent.
- A cancelled job only clears its passes.

### 4.8 Reaper (`loops.go:217-275`)
- Every minute it checks jobs in `running`/`cancelling` older than `reaper_after` (30m), up to 500 per tick.
- No Redis state (`Spawned==0`) gives **`failed`** with "job state lost". `Pending>0` with no activity for `reaper_after` gives **`partial`** with "stalled with pending tasks".
- The reaper writes the job row (with the guard `WHERE status IN ('running','cancelling')`), publishes a `JobEvent`, and increments `fbs_jobs_total{status, lane="unknown"}`.
- It does not compute coverage, update the schedule or run AfterJob.

### 4.9 Budget interplay (`internal/fb/ratelimit/budget.go`, config `scheduler.budget`)
- Crawlers add requests and wire bytes per lane to a per-day Redis hash `budget:d:YYYYMMDD` and to 5-minute slices `budget:s:<unix>`.
- Each `interval`, the leader projects the end of the day: `used_today + recent_rate × seconds_left`, where `recent_rate` covers the last `projection_span` (1h). It then publishes `BudgetState` to `budget:state`, with a TTL of `max(10×interval, 10m)`. When the controller stops, the state expires and everything falls back to neutral.
  - **Backfill:** pause p2 when the projection including p2 exceeds the budget. While paused, resume only once `(p0+p1 projection) + last known p2 rate × left` fits within 95% of the budget (hysteresis).
  - **Stretch:** when **p0+p1 alone** would exceed the budget, set `Stretch = budget.stretch` (2) for `stretch_tiers` [T4, T3, T2]. It is held until the projection drops under 95%.
- Effects:
  - The **planner** multiplies `next_run_at` intervals of the stretch tiers by the factor (§4.3).
  - **Crawlers** close the p2 lane: `BudgetGate` refreshes every 15 s and calls `SetMax(lane, 0)` (`internal/app/crawler.go:224-237`).
  - **p0 (on-demand) is never held back.**
- Metrics: `fbs_budget_used{kind,lane}`, `fbs_budget_projected{kind}`, `fbs_budget_limit{kind}`, `fbs_budget_backfill_paused`, `fbs_budget_stretch`.

### 4.10 Job state machine (as implemented)
```
running ──(cancel API)──▶ cancelling ──finalizer──▶ cancelled
   │                                       └─reaper─▶ failed | partial
   ├──finalizer──▶ completed | partial | failed
   ├──reaper─────▶ failed (state lost) | partial (stalled)
   └──dispatch error──▶ failed
(queued, finalizing: declared in schema/queries, never written)
```

---

## 5. Media lifecycle (`internal/media`, `cmd/fbs-media-*`, `docs/contracts/media-handoff.md`)

ADR 0003: fbcdn URLs are signed and expire (`oe`), and SecureBrand wants **no long-term media archive**. Media is downloaded promptly over **direct datacenter egress (never residential proxies)**, handed to the analysis service, and deleted once that service acknowledges it.

### 5.1 What gets queued for download (crawl stage, `internal/crawl/stage/deps.go:173-221`)
- Every discovered media item is published on `fb.media.v1` and persisted as metadata.
- A download task (`fbs.task.media`, key = media_id) is queued only if `include_media` is set (or `include_comment_media` for comments) **and** `wanted(media, video_policy)`:
  - `VIDEO`/`REEL`/`COMMENT_VIDEO`:
    - `hd` takes the hd variant, or sd when no hd exists.
    - `sd` and the default take sd only.
    - `thumbnail` and `none` take no video file.
  - `THUMBNAIL`: downloaded unless the policy is `none`.
  - Everything else is always downloaded: photo, comment photo, GIF, sticker, cover, profile pic.
- Defaults are `video_policy: sd`, and `backfill_video_policy: thumbnail` for registration backfills.
- Media tasks are **not part of job accounting**: a job completes when its data is stored, not when its media is (`internal/crawl/task/task.go:119-128`).
- `media_id` is a UUIDv5 of `(fb_media_id, variant)`, so the same file seen again keeps the same id.

### 5.2 Downloader (`internal/media/downloader.go`, `cmd/fbs-media-downloader`)
- It uses the generic consumer: group `fbs-media-downloader`, topic `fbs.task.media`, `MaxInflight = media.concurrency` (50), drain 60s.
- At startup it ensures the bucket exists and installs the lifecycle rule (§5.4).
- `Handle` (`:75-159`):
  1. Skip when the media row is already `ready`, `deleted` or `expired`. Media already handed off is never re-downloaded.
  2. Mark `expired` when `source_expires_at` is in the past.
  3. Run `GET source_url` with a Chrome UA, using `NewHTTPClient` with `Proxy: nil` (it ignores `HTTPS_PROXY`), a 5-minute timeout and 64 idle conns per host:
     - 403 or 410 marks the media `expired`.
     - 404 is permanent and marks it `failed`.
     - Any other ≥400 or a network error is returned as an error, and the consumer **retries with backoff (200ms…30s) indefinitely**. Retries end on success or once the URL expires into a 403.
     - `Content-Length > max_bytes` (500 MiB) is permanent. The body is capped by a `LimitReader`; when the stored size turns out larger, the object is removed and the error is permanent.
  4. Stream to S3 while hashing SHA-256 (TeeReader), using `PutObject` with an 8 MiB part size and user metadata `media-id` and `fb-media-id`.
     - **Key layout** (`ObjectKey`, `:59-70`): `media/<first 2 chars of media_id>/<media_id><ext>`. The extension comes from the content type (`image/jpeg` gives `.jpg`), else from the URL path, else `.bin`.
  5. Upsert the row with `status='ready'`, bucket, key, size, sha256, mime, `attempts+1`, `delete_after = now + media.ttl` (72h), and `stored_at = ready_at = now`.
  6. Publish **`fb.media.ready.v1`** (key media_id) with `MediaReady{event_id "<media_id>:ready:1", media_id, fb_media_id, kind, variant, page/post/comment ids, object{bucket,key,size,sha256,content_type}, width, height, duration_ms, alt_text, discovered_at, stored_at, delete_after, attempt:1, job_ids, presigned_url?}`. The presigned URL is only present with `media.presign: true` and a `presign_ttl` of 24h.
- Metrics: `fbs_media_download_total{kind,outcome=ready|skipped|expired|failed|retry}` and `fbs_media_discovery_to_stored_seconds{kind}`.
- `markFailed` never downgrades a row that is `ready` or `deleted` (`:212-223`).

### 5.3 Consumer acks and the janitor (`internal/media/janitor.go`, `cmd/fbs-media-janitor`)
- The analysis service produces **`fb.media.processed.v1`** with `MediaProcessed{event_id, media_id, consumer, status: PROCESSED|FAILED_PERMANENT|FAILED_RETRYABLE, processed_at, error}`, keyed by media_id.
- The janitor consumes it with group `fbs-media-janitor`, 32 in flight (`Handle`, `:37-65`):
  - The ack is upserted into `media_acks` **only if the media row still exists**. Acks for unknown or purged media are ignored, so replays are safe.
  - `FAILED_RETRYABLE`: increment `retryable_failures` (only while the media is `ready`). Above `media.max_retryable_failures` (3) the object is deleted with reason **`ACKED`**; there is no separate reason. Otherwise `MediaReady` is re-published with `attempt = failures+1` and `event_id "<id>:ready:<n>"`, rebuilt from the row (`fillReady`). `delete_after` is **not** extended.
  - Otherwise, the object is deleted with reason `ACKED` once `count(DISTINCT consumer)` over `media.required_consumers` (`["analysis"]`) in status `processed`/`failed_permanent` equals the number of required consumers.
- **`delete`** (`:132-162`), which only acts on `ready` rows:
  1. `RemoveObject`.
  2. Set `status='deleted'`, or `'expired'` for a TTL deletion, plus `deleted_at`, and `processed_at` for acked deletions.
  3. Increment `fbs_media_deleted_total{reason}`.
  4. Publish **`fb.media.deleted.v1`** `MediaDeleted{event_id "<id>:<reason>", media_id, object, reason, deleted_at}`.
- **TTL sweep** (`Sweep`, every `media.sweep_interval` 1h): `ready` rows with `delete_after < now`, in batches of 500, are deleted with reason `TTL_EXPIRED`.
- **Orphans** (`Orphans`, every `media.orphan_interval` 24h): lists `media/` objects older than TTL+1h, parses the uuid from the file name, and removes the object if no `ready` row exists (`fbs_media_deleted_total{reason="orphan"}`). **No `fb.media.deleted.v1` event is published for orphans**, although the contract lists an `ORPHAN` reason.

### 5.4 Safety nets
- **Bucket lifecycle rule** `fbs-media-ttl`: prefix `media/`, expiry `int(ttl/24h)+1` days (4 days for 72h), installed by the downloader at startup. Stores that do not support it log a warning (`janitor.go:236-242`, `lifecycle.go:5-12`).
- Bucket versioning must stay **off** so that deletes free storage (contract).
- `s3x.Open` (`internal/platform/s3x/s3.go`) wraps minio-go with static V4 credentials. It works with any S3-compatible store and creates missing buckets.

### 5.5 The handoff contract (summary of `docs/contracts/media-handoff.md`)
- Delivery is at least once: deduplicate on `event_id`.
- Process before `delete_after`, and watch `fbs_media_deleted_total{reason="ttl_expired"}`.
- Ack with exactly the configured consumer name.
- Do not archive media without your own privacy review.
- Honour `fb.erasure.v1` by deleting everything derived from the author key or page id.
- `fbctl media ack --consumer analysis` simulates acks in dev.
- Sizing (ADR 0003): storage is about throughput × TTL, 5–12 TB at target volume. If the analysis service is down longer than the TTL, the media is lost to it, and `TTL_EXPIRED` events say why.

---

## 6. GDPR (`internal/gdpr`)

### 6.1 Pseudonymous author keys and pii_mode
- `AuthorKey = HMAC-SHA256(privacy.author_hmac_key, author_id)` (`suppression.go:20-27`). The crawler's emitter computes it on every comment (`internal/crawl/emit/emit.go:106-125`). The key is the stable join key for `comment_authors`, `comments.author_key`, suppression and the API `author_key`.
- **`privacy.pii_mode: full | pseudonymized`** (validated):
  - `pseudonymized`: the emitter blanks `Author.{Id, Name, Url, AvatarUrl}` before publishing, and keeps the `author_key`, `type` and `verified`. Kafka, the DB and the API then never see author identity.
  - `full`: identity is stored, and the API gates it behind `pii:read`.
- Validation rejects `service.env=prod` with the dev key `dev-only-change-me` (`internal/config/config.go:407-409`).
- **Rotating `author_hmac_key` would orphan every existing key**: suppression entries, joins and erasure-by-id. Treat it as permanent.
- `privacy.download_author_avatars` is declared but not used.

### 6.2 Erasure workflow (`erasure.go`; API §1.4 #18/#19; `DELETE /pages/{p}?purge=true`)
- **Request intake** (API):
  - Authors are given by `author_id`, which is HMACed locally, or by `profile_url`, which is resolved through `comment_authors` (first to an `author_id`, else directly to the stored `author_key`).
  - Under pseudonymization profile URLs are not stored, so `author_id` must be supplied.
  - No Facebook request is ever made. The request row is inserted as `pending`.
- **Worker:** `Eraser.Tick`, on the scheduler leader every 10 s, takes up to 10 pending requests in FIFO order. Errors set `status='failed'` with the error, **with no automatic retry**: such a request must be resubmitted.
- **Author erasure (redaction)**, `:73-105`:
  1. In one transaction:
     - `INSERT suppression('author', hex(key))`.
     - `UPDATE comments SET text=NULL, text_ranges=NULL, attachments=NULL, author_id=NULL, url=NULL, extra=NULL, redacted=true WHERE author_key=$1`. This covers all partitions via `comments_author_idx`. The comment rows remain, so thread structure, counts and reactions survive.
     - `DELETE FROM comment_authors WHERE author_key=$1`.
  2. Then delete the media of the redacted comment ids (§6.4).
  3. Stats: `comments_redacted`, `authors_deleted`, `media_deleted`.
- **Page erasure (deletion)**, `:106-144`:
  1. `INSERT suppression('page', id)`.
  2. Delete the page's media.
  3. In one transaction: DELETE `comments`, `post_metrics`, `posts`, `post_index`, `page_snapshots` and `crawl_cursors` for the page; disable `page_schedule`; and **tombstone `pages`** with `status='suppressed'` and every descriptive column NULLed. Stats are rows per table.
- Both kinds then publish **`fb.erasure.v1`** `ErasureEvent{event_id=request_id=id, subject_type AUTHOR|PAGE, author_key | page_id, requested_at, completed_at}` with key = request id. Every downstream store must apply it.

### 6.3 Suppression: re-scrapes never reintroduce data
- `gdpr.Suppression` is an in-memory set of author hex keys and page ids, loaded from `suppression` at startup and every `crawler.suppression_refresh` (60s) in each crawler (`suppression.go:62-123`, `internal/app/crawler.go:119,144`).
- **Pages:** the page stage checks `PageSuppressed` after parsing the page HTML and before emitting anything, and finishes with counter `pages_suppressed` (`internal/crawl/stage/page.go:100-120`). The schedule is also disabled.
- **Authors:** `Emitter.Privatize` redacts a suppressed author's comment at the source. It replaces the author with `{author_key}`, clears text, ranges, attachments and url, sets `redacted=true`, and returns `keep=false` so the comment's media is not downloaded.
- **Database guards:**
  - The comment upsert's `WHERE NOT t.redacted` means a redacted row is never overwritten.
  - `markDeleted` skips redacted rows.
  - The persister never writes `comment_authors` for redacted comments.
- **Gap:** suppression is enforced at the **producer**, not in the persister. Records already in Kafka when the erasure runs can still land afterwards if the persister lags:
  - posts and comments of an erased page (and its page row, re-activated by the unguarded page upsert);
  - new comments and `comment_authors` rows of an erased author. Rows that already existed are protected by `NOT t.redacted`, but fresh rows are not.

  There is also a window of up to 60 s before the crawlers reload the set. **[suggestion]** Check suppression in the persister as well.

### 6.4 Media on erasure (`deleteMedia`, `erasure.go:158-191`)
- For matching rows with `s3_key` set in status `stored` or `ready`: `RemoveObject`, set `status='deleted'`, and publish `fb.media.deleted.v1` with reason **`ERASURE`** (`fbs_media_deleted_total{reason="erasure"}`).
- Then every other matching row gets `status='deleted'` and `source_url=NULL`.
- Kafka retention is `delete`, never compaction (`configs/topics.yaml:21-22`), so erased data in topics ages out after 168h. `fb.erasure.v1` itself keeps 720h.

---

## 7. Replay (`internal/replay`, `fbctl replay`)

- **Capture side** (out of scope, for context): the crawler's `rawsink` batches raw Facebook GraphQL responses into zstd JSONL objects in `s3.raw_bucket` (`fbs-raw`).
  - Keys: `raw-failures/<query>/<YYYY/MM/DD/HH>/<pod>-<seq>.jsonl.zst` for non-OK classes, and `raw/<query>/…` for everything when `fb.capture_raw: all` (the default is `failures`).
  - Each line is `{at, query, class, vars, body|text}` (`internal/crawl/rawsink`).
- **`replay.Run`** (`replay.go:87-131`):
  - Selects queries: given ones or all, where a `shadow-<query>` prefix maps to that query's parser.
  - Lists objects under `<prefix>/<query>/`, filters by `Since`, sorts by key (hour, pod, sequence) and keeps the newest `MaxObjects`.
  - Decompresses each object and **re-parses each OK or SCHEMA_DRIFT line with the current parser** (`timeline`, `comments` or `replies`), using the request vars as parse context: page id, and post or parent comment id derived from `feedback_id` (`:205-229`).
- Per query it reports `lines`, `ok`, `drift`, `skipped` (rate limits, walls and transport errors), `classes`, `missing` (required fields still missing) and fixtures. **It never contacts Facebook.**
- **Fixture generation** (`:182-203`): up to `Samples` still-drifting responses per query are written as **scrubbed** parser fixtures to `<dir>/<parser>/<date>_<name>-<query>-<n>/response.ndjson` plus `meta.json` (page_id, post_id, parent_comment_id, query, class, missing, replayed_from, vars). Existing fixtures are never overwritten.
- **CLI:**
  ```
  fbctl replay [--prefix raw-failures|raw] [--query Q,..] [--since 168h] [--max-objects 500]
               [--fixtures-dir DIR --name N --samples 3]
  ```
  - Parsers come from the live registry, falling back to the embedded seed when the DB is unreachable (`cmd/fbctl/replay.go:63-87`).
  - It prints JSON and **exits 3 while anything still drifts**, so it can serve as a CI or deploy gate after a parser fix.
- Test (`replay_test.go`): a drifted timeline still drifts and yields one fixture with `meta.page_id`. A comments response captured as drift but parsing now counts as OK. Rate-limited lines are skipped, and a second run writes new fixture names.

---

## 8. Configuration system

### 8.1 Layering (`internal/platform/config/load.go`, `internal/config/config.go:366-391`)
Layers are applied with koanf, later layers winning:
1. The **embedded `configs/default.yaml`** (`configs/embed.go`). Every binary starts complete, even without a mounted file.
2. **Files** from `--config` or `FBS_CONFIG`, **comma separated, later files win**. The intended use is a ConfigMap followed by a Secret. A missing file is an error.
3. **Environment `FBS_<SECTION>__<KEY>`**: the prefix is stripped, the name lowercased, and `__` becomes `.` (so nested keys work, e.g. `FBS_KAFKA__SASL__PASSWORD`).
   - **Any value containing a comma becomes a list**, with items trimmed. Watch out for DSNs or passwords that contain commas.
   - Lists of objects (proxy tiers, scheduler tiers) need a file.

Then `Validate()` runs, and every binary exits with status 2 on an error (`MustLoadFromFlags`). `TestLoadLayers` covers files, environment overrides and surviving defaults.

### 8.2 Validation rules (`config.go:394-448`)
- `crawler.yield_pages > 0`.
- `limits.max_age_days ≤ limits.max_age_cap_days`.
- `privacy.pii_mode ∈ {full, pseudonymized}`.
- In prod (`service.env=prod`), `privacy.author_hmac_key` must not be the dev default.
- `media.ttl > 0` and `scheduler.default_interval > 0`.
- `scheduler.dormant_interval > 0` when `dormant_after` is set.
- Every scheduler tier needs a name and an interval > 0, and tiers must be ordered by `min_posts_per_day`, highest first.
- `post_refresh.windows` need intervals > 0 and strictly ascending `max_age`.
- `kafka.sasl.mechanism ∈ {"", plain, scram-sha-256, scram-sha-512}`, and SASL needs a username.
- `post_refresh.deletion_count_slack ∈ [0, 1)`.
- `registry_sync.interval > 0` and `registry_sync.timeout > 0`.
- TLS blocks are validated lazily: `cert_file` and `key_file` go together, and `ca_file` must contain PEM (`internal/config/tls.go`).

### 8.3 Every knob outside `fb:` (default → meaning)
**service:** `name: fbs`; `env: dev` (`prod` enables the HMAC key check).

**log:** `level: info` (`debug|info|warn|error`); `format: json` (`json|text`). fbctl always uses text.

**http:**
- `addr: ":8080"`: API listener.
- `ops_addr: ":9090"`: `/metrics`, `/healthz` and `/readyz` on every binary.
- `read_timeout: 15s`, `write_timeout: 60s`: **unused**.

**postgres:** `dsn` (local default); `read_dsn: ""` (optional replica for API reads; the API only); `max_conns: 20` (pool size per process).

**redis:** `addr: localhost:6379`, `password`, `db: 0`, `tls{enabled, ca_file, cert_file, key_file, server_name}`.

**kafka:**
- `brokers: [localhost:19092]`; `client_id: fbs` (with a per-binary suffix: `-api`, `-persister`, `-scheduler`, `-media`, `-janitor`, `-fbctl`).
- `topic_prefix: ""`: prepended to all topics, consumer groups and schema subjects.
- `produce_linger: 5ms`.
- `compression: zstd`: **unused**. The producer is hard-coded to zstd, then lz4, then none.
- `tls{…}`; `sasl{mechanism, username, password}`.
- `schema_registry{url, username, password}`: used by `fbctl schemas`.

**s3:** `endpoint: localhost:9000`, `access_key`, `secret_key`, `use_ssl: false`, `region: us-east-1`, `media_bucket: fbs-media`, `raw_bucket: fbs-raw`.

**crawler** (consumed by the crawler, listed for completeness):
- `stages: [page, post, thread]`.
- `slots: 240`: in-flight tasks per pod and stage, shared by priority.
- `lanes{p0:240, p1:240, p2:96}`: per-lane caps (backfill at most 40%).
- `lane_min: 12`: slots a waiting lane always gets.
- `max_inflight_per_partition: 64`.
- `yield_pages: 20`, `yield_after: 60s`: a task yields a continuation after this many pages or this long.
- `timeline_page_size: 10`, `comments_page_size: 50`.
- `consecutive_old_stop: 3`: the incremental stop rule.
- `max_attempts: 5` (retries without progress) and `max_total_attempts: 50`; exhausted tasks go to the DLQ.
- `lease_ttl: 120s`: stream lease.
- `freshness_p0: 15m`, `freshness_p1: 6h`: **unused**.
- `suppression_refresh: 60s`.

**limits** (job defaults):
- `max_age_days: 365`; `max_age_cap_days: 1095` (the API cap and the partition look-back).
- `max_posts: 0` (unlimited); `max_depth: 3`.
- `include_comments`, `include_replies`, `include_media`, `include_comment_media`: all true.
- `video_policy: sd`; `backfill_video_policy: thumbnail`.

**scheduler:**
- `planner_interval: 30s`, `planner_batch: 500`.
- `default_interval: 24h`: the first interval of a registration without `interval_hours`, before the first re-tier.
- `jitter: 0.1`: ±10% on `next_run_at`.
- `finalizer_interval: 2s`; `finalize_timeout: 10m` (finalise anyway when the persister lags).
- `reaper_after: 30m`.
- `partition_months_ahead: 3`.
- `incremental_overlap: 24h`: posts younger than this are re-read by every scan, and their first refresh is due at `created + overlap`.
- `rate_window: 336h`.
- `tiers`: T0/T1/T2/T3 (§4.4); `dormant_after: 2160h`, `dormant_interval: 168h`.
- `post_refresh`:
  - `enabled: true`, `max_per_job: 50`, `max_backoff: 3`.
  - `windows`: `[{168h,24h},{336h,72h},{720h,168h,tiers:[T0,T1]}]`.
  - `recrawl_at: [168h, 720h]`, `recrawl_min_comments: 20`.
  - `detect_deletions: true`, `deletion_count_slack: 0`.
- `budget`:
  - `enabled: true` (inert while both budgets are 0).
  - `requests_per_day: 0`, `bytes_per_day: 0`.
  - `interval: 1m`, `stretch: 2`, `stretch_tiers: [T4,T3,T2]`, `projection_span: 1h`.

**media:**
- `concurrency: 50` (downloader in-flight); `ttl: 72h`.
- `required_consumers: [analysis]`; `max_retryable_failures: 3`.
- `sweep_interval: 1h`, `orphan_interval: 24h`.
- `presign: false`, `presign_ttl: 24h`.
- `max_bytes: 524288000` (500 MiB).

**api:**
- `max_running_p0_jobs_per_client: 50` (counts all of the client's running jobs).
- `max_running_p0_jobs: 0` (fleet cap; 0 = none).
- `default_rate_per_minute: 600`.

**privacy:**
- `pii_mode: full`; `author_hmac_key: dev-only-change-me`.
- `download_author_avatars: false`: **unused**.
- `retention{comments_days 365, authors_inactive_days 365, metrics_days 180, raw_payload_days 7 (unused), media_rows_days 30, jobs_days 90}`.

**registry_sync** (Facebook-specific):
- `interval: 30m`.
- `canary_pages: []` (set at least three in prod).
- `tier: ""`.
- `max_bundles: 60`, `bundles_via_proxy: false`.
- `promote: true` (false means a dry run).
- `keep_fallbacks: 2`, `retire_after: 3`, `recheck_retired: 24h`.
- `timeout: 10m`.

---

## 9. `fbctl` (`cmd/fbctl`)
`fbctl [--config f] <cmd> [sub] [flags]`. The context timeout is 30 min, except for `pages import`, which runs until Ctrl-C.

| Command | What it does |
|---|---|
| `migrate up` | goose up. Then **seeds the query registry** from the embedded `fb.registry_seed` (`queries.seed.yaml`), then `EnsurePartitions` (same window as maintenance) and prints counts (`commands.go:30-68`) |
| `migrate down` / `status` | goose down one migration / print `version state path` |
| `kafka apply [--partitions-cap N] [--replicas N]` | creates topics from the embedded `configs/topics.yaml` (default 3 replicas). Grows partitions and updates retention, never shrinks or deletes. Sets `cleanup.policy=delete` and `compression.type=producer` (`internal/platform/kafka/admin.go:212-259`) |
| `schemas apply` / `schemas check` | registers the `fb.v1` protos with the Schema Registry, or only checks compatibility (§11) |
| `apikey create --name N [--scopes jobs:write,pages:write,data:read] [--rate N]` | prints `{client_id, name, api_key, note}`. The key is shown once |
| `apikey revoke --prefix P` | sets `revoked_at` (effective within 60s due to the API cache) |
| `registry list` / `registry set --file q.json` | dumps the registry or upserts an entry (Facebook-specific) |
| `dlq replay [--reason R] [--max 1000] [--idle 10s]` | consumes `fbs.task.dlq` with group `fbctl-dlq-replay`, optionally filters on header `x-reason`, re-produces each record to its `x-orig-topic`, and commits (`commands.go:187-241`) |
| `replay …` | §7 (exit code 3 while drifting) |
| `fixtures scrub --dir DIR [--keep-ids …]` | scrubs PII from a fixture directory (`internal/fixtures`) |
| `media ack --consumer NAME [--status processed\|failed_permanent\|failed_retryable] [--max 10000]` | dev or e2e only: produces `MediaProcessed` for every `ready` row |
| `pages import --api URL --key KEY [--interval-hours N] [--rps 5 (≤1000)] [--backfill=false] FILE` | registers pages through `POST /v1/pages`. The file holds one ref per line, or is a CSV with a `page` column. Paced by `rps`. Retries 429, 5xx and network errors up to 5 times with exponential backoff, honouring `Retry-After`. Prints `{created, existing, failed[]}` and is safe to re-run (`pages.go`, tested in `pages_test.go`) |

---

## 10. Binaries in scope (quick reference)

| Binary | Deps | Components (`run.Group`) | Readiness checks |
|---|---|---|---|
| `fbs-api` | PG (+ replica), Redis, Kafka producer | ops server, API server | postgres, redis, kafka |
| `fbs-persister` | PG, Kafka consumer | ops, persist consumer | postgres |
| `fbs-scheduler` | PG, Redis, Kafka producer, S3 (for erasure) | ops, leader election → loops (§4.2) | postgres |
| `fbs-media-downloader` | PG, S3, Kafka | ops, consumer on `fbs.task.media` | postgres |
| `fbs-media-janitor` | PG, S3, Kafka | ops, consumer on `fb.media.processed.v1`, sweep, orphans | postgres |

Each binary: `config.MustLoadFromFlags`, then `plog.Setup(name, level, format)`, opens its dependencies (ping on open), then `run.Group(ctx, ops.Run, …)`.

---

## 11. Platform libraries
- **`pg.Open(ctx, dsn, maxConns)`**: `pgxpool` with an optional `MaxConns`, pinged on open (`internal/platform/pg/pg.go`).
- **`redisx.Open(ctx, addr, password, db, tls)`**: go-redis v9 client with optional TLS, pinged on open. `app.OpenRedis` builds the TLS config from `redis.tls`.
- **`schemareg`**: Confluent-compatible REST client (`/subjects`, `/config`, `/compatibility`).
  - `Apply` plans the subjects dependencies-first: every imported proto becomes subject `<prefix><import path>` (e.g. `securebrand/fb/v1/common.proto`), and each topic gets `<prefix><topic>-value` holding the whole proto file that declares its value message.
  - **It checks every changed schema first and registers nothing unless all pass.** A changed import is staged under a temporary `fbs-check-<ts>.` subject so that dependents are checked against it; temporary subjects are deleted afterwards.
  - Subjects without their own compatibility level get `BACKWARD_TRANSITIVE`; levels an operator set are kept. Applying is idempotent.
  - The client retries once on EOF, ECONNRESET or a closed idle connection, and sends an `Idempotency-Key` header so net/http may replay POSTs.
  - The wire format stays plain protobuf with an `fbs-type` header (no Confluent magic byte). The `ValueSchemas` topic→file map lives in `internal/topics/schemas.go`.
- **`metrics`**: one Prometheus registry plus Go and process collectors, with every metric under the `fbs_` namespace (§12). `monitoring_test.go` checks that every `fbs_*` metric and `fbs:` recording rule used in the Helm alert rules and Grafana dashboards exists, that alerts have severity, `for`, summary, description and a runbook URL whose anchor exists, and that dashboards use the `${datasource}` variable.
- **`httpserver`**: the ops server serves `/metrics` (promhttp), `/healthz` (always 200, liveness) and `/readyz` (runs every registered check with a 3 s timeout; the first failure returns 503 `"<name>: <err>"`). `Serve` uses `ReadHeaderTimeout 10s` and a graceful `Shutdown` with 15 s.
- **`run.Group(ctx, fns...)`**: runs components concurrently under a SIGINT/SIGTERM-aware context. When any component returns, the rest are cancelled and awaited, and the first non-cancellation error is returned.
- **`log.Setup(service, level, format)`**: a slog JSON or text handler on stderr with `service=<name>` on every record, installed as the default logger.
- **`platform/config.Load`**: the generic layered loader (§8.1).
- **Related, but not on the scope list: `platform/kafka`.**
  - The producer requires all-ISR acks, uses zstd and a 1 MiB batch, allows auto topic creation, and adds `fbs-type` and `content-type` headers.
  - `Batch` produces asynchronously, and `Wait` collects the errors.
  - The consumer processes records **concurrently within a partition**, but **commits only the highest contiguous completed offset** (the tracker). A handler error is retried with backoff 200ms→30s **forever**, so handlers must route poison records themselves.
  - Drain on shutdown with `DrainTimeout`. On revoke, the consumer waits up to 15 s for in-flight work before committing.
  - Admin helpers: `EnsureTopics`, `GroupCaughtUp`, `EndOffsets`.

---

## 12. Metrics emitted by these services
All metrics are prefixed `fbs_` (`internal/platform/metrics/metrics.go`).

| Service | Metric | Labels |
|---|---|---|
| API | `api_requests_total` | route, code |
| persister | `persist_rows_total` | entity (page\|post\|comment\|media), op (written\|unchanged) |
| persister | `persist_batch_seconds` (hist) | entity |
| persister / finaliser | `comment_changes_total` | kind (edited\|restored\|deleted) |
| finaliser | `comment_completeness_ratio` (hist, buckets 0.1…1.01) | lane |
| finaliser / reaper | `jobs_total` | status, lane (reaper uses `unknown`) |
| finaliser | `deletion_checks_total` | outcome (checked\|skipped_partial) |
| planner | `schedule_lag_seconds` | tier (incl. `all`) |
| planner | `scheduled_pages` | tier |
| planner | `post_refreshes_total` | kind (refresh\|recrawl) |
| scheduler | `scheduler_leader` | – |
| scheduler | `kafka_consumer_lag` | group, topic (prefix stripped) |
| budget | `budget_used` | kind, lane |
| budget | `budget_projected`, `budget_limit` | kind |
| budget | `budget_backfill_paused`, `budget_stretch` | – |
| scheduler (AIMD) | `aimd_ceiling_rps`, `aimd_adjustments_total{direction}`, `fleet_rps`, `fleet_block_rate` | |
| downloader | `media_download_total` | kind, outcome (ready\|skipped\|expired\|failed\|retry) |
| downloader | `media_discovery_to_stored_seconds` (hist) | kind |
| janitor / eraser | `media_deleted_total` | reason (acked\|ttl_expired\|erasure\|orphan) |
| all producers | `kafka_produce_total` | topic, outcome |
| generic consumers | `kafka_commits_total` | group, outcome (the persister commits directly, so it is **not** counted here) |
| all | Go runtime and process collectors | |

---

## 13. Findings worth knowing (bugs, gaps, sharp edges)
1. **Persister poison records stall everything** (§3.5). Decode errors are logged as skipped, but the message is still written, and permanently failing batches are retried forever with no DLQ.
2. **Suppression is not enforced in the persister** (§6.3). In-flight records can re-insert erased page data and re-activate the page row, because the page upsert sets `status='active'`.
3. **Job `/results` misses unchanged posts** because of the daily `last_seen_at` throttle (§1.6).
4. **No job ownership check** on `GET/cancel/results /jobs/{id}`. `PATCH /pages` is not scoped to the caller's registrations.
5. **`type` filter breaks pagination** on `/pages/{p}/posts`. `GET /pages` has no cursor (at most 500 rows).
6. Posts have **no stale-observation guard**, unlike comments.
7. The **per-client on-demand cap counts backfills**, so bulk registration can cause 429s on `POST /jobs`.
8. **Orphan deletions publish no `fb.media.deleted.v1`**, contrary to the contract. Exhausted `FAILED_RETRYABLE` deletes with reason `ACKED`.
9. **`posts_failed`** is missing from the `fb.job.v1` Completeness proto.
10. Retention churn: about 24 `comments` partitions are created and dropped every hour with the defaults. `posts` and `page_snapshots` are never dropped. The jobs delete cap of 5000/h may lag at scale.
11. Unused knobs: `http.read_timeout`/`write_timeout`, `kafka.compression`, `crawler.freshness_p0/p1`, `privacy.download_author_avatars`, `privacy.retention.raw_payload_days`. `jobs.status` values `queued`/`finalizing` and `media.status` values `stored`/`skipped` are never written.
12. The API key cache delays revocation by up to 60 s per replica. The limiter fails open when Redis errors.
13. `middleware.Timeout(60s)` cuts long NDJSON result streams.
14. PG18 is required (`RETURNING old/new`). On older Postgres, emulate it with a CTE that captures the old row, or the `xmax = 0` trick.

---

## 14. Instagram adaptation notes

### 14.1 Classification
**(a) Generic, reusable as-is** (rename the `fbs_` namespace, binary names and the env prefix):
- **Platform libs:** `pg`, `redisx`, `httpserver`, `run`, `log`, `platform/config` (koanf layering), `metrics` (registry pattern and the alert/dashboard consistency test), `schemareg` (check-then-register, BACKWARD_TRANSITIVE), `platform/kafka` (producer, concurrent-in-partition consumer with contiguous commits, admin), `s3x`, and `leader` (advisory lock; choose new lock keys).
- **The persistence technique:** temp table + COPY + `ON CONFLICT … WHERE content_hash IS DISTINCT FROM … OR last_seen_at stale`, plus `RETURNING old/new` to derive change-only time series. Also `withPartitions` on-demand creation and `store/partitions.go` (make the table lists a parameter).
- **Job machinery:** Redis Lua task accounting (`pending = |spawned|-|done|`), the dispatcher (idempotency key, advisory-lock caps), the finaliser's "wait until the persister's committed offsets pass the snapshotted end offsets" rule, the reaper, and the job state machine.
- **Scheduler core:** the `FOR UPDATE SKIP LOCKED` planner with failure backoff, jitter and stretch; adaptive tiers driven by posting rate; post refresh by age with backoff and age-based recrawls; the budget controller and gate; lag and tier gauges.
- **Media pipeline end to end:** downloader, janitor, ack table, TTL sweep, lifecycle rule, orphan reconciliation and the handoff contract. Instagram media is served from the same Meta CDN family (`scontent-*.cdninstagram.com`/`fbcdn`), with signed URLs whose `oe` expiry param likely works the same; verify this.
- **GDPR building blocks:** HMAC author keys, the suppression table and set, redact-vs-delete erasure, the erasure topic, and `pii_mode`.
- **API scaffolding:** API keys (`<prefix>_…`), scopes, GCRA rate limit, problem+json, keyset cursors, NDJSON results, and the OpenAPI ⇄ router parity test.
- **fbctl:** `migrate`, `kafka apply`, `schemas`, `apikey`, `dlq replay`, `media ack`, `pages import` (renamed `profiles import`), and `replay` (the framework).

**(b) Reusable with renamed tables, topics and fields:**
- `pages` → `ig_profiles`; `page_snapshots` → `ig_profile_snapshots`; `page_schedule` / `page_registrations` → `profile_schedule` / `profile_registrations`; `page_ref` (`v:`/`id:`) → `profile_ref` (`u:<username>` / `id:<user pk>`).
- `posts`, `post_index`, `comments`, `comment_authors`, `post_metrics` and `post_refresh` keep their shape but need different engagement columns (see below).
- `crawl_cursors` with `scope_type='profile_timeline'`.
- Topics `fb.*.v1` → `ig.*.v1`; task topics `igs.task.{profile,post,thread}.{p0,p1,p2}`; proto package `securebrand.ig.v1`.
- API paths `/pages` → `/profiles`; `/posts/{post}` should also accept a **shortcode**.
- Completeness math, deletion detection, tiers and refresh windows are unchanged in logic, but need retuning for Instagram (§14.3).

**(c) Facebook-specific (replace):**
- **The query registry content and machinery:** `fb_queries`, `fb_client_params`, `registry_sync_runs`, `/admin/queries*`, `registry_sync` config and `fbctl registry`. The *concept* of a hot-reloaded doc_id registry with canary validation is reusable for Instagram web GraphQL doc_ids, but the entries, parsers and canary logic are new.
- **Facebook identifiers:** `feedback_id` (and `parse.FeedbackID` used by `plan()`), `story_id`, `graph_id`, and `NormalizeRef`'s facebook.com URL forms.
- **Facebook concepts:** reactions-by-type plus `reactions_complete`, `share_count`, `shared_post_id`, `likes_count` on pages, page `categories`/`intro`/`contact`/`cover_url`, and the `User|Page` author typename.
- **Media variants and kinds:** `link_preview`, `STICKER`, `COMMENT_GIF`, `COVER`.
- **Replay parsers** (`timeline`, `comments`, `replies`) and their parse-context derivation.

### 14.2 Proposed Instagram schema changes **[suggestion]**
- **`ig_profiles`**:
  - `user_id bigint PK` (Instagram pk), `username` (with a unique index on `lower(username)`), `full_name`, `biography`, `bio_links jsonb`, `external_url`, `category`.
  - Flags: `is_verified`, `is_private`, `is_business`, `is_professional`.
  - Counts and media: `followers_count`, `following_count`, `media_count`, `profile_pic_url`.
  - `status` (`active|private|unavailable|suppressed`), `content_hash`, `first_seen_at`, `last_scraped_at`.
  - A **private** profile is a terminal "cannot crawl" state, not a failure. Do not count it toward `consecutive_failures`.
- **`ig_profile_snapshots`** (monthly): `followers_count`, `following_count`, `media_count`, **`username`** (to track renames, which are common on Instagram; resolve schedules by `user_id`, not by username), `full_name`, `is_verified`.
- **`ig_posts`** (monthly by `taken_at`), PK `(taken_at, media_pk)`. Columns:
  - Identity: `shortcode`, `owner_user_id`.
  - Type: `media_type` (`photo|video|carousel`) and `product_type` (`feed|clips|carousel_container|igtv`), or a combined `post_type` (`photo|video|carousel|reel`).
  - Text: `caption`, `caption_ranges`, `hashtags text[]`, `mentions jsonb`.
  - Tags and place: `usertags jsonb`, `location jsonb` (id, name, lat, lng).
  - Engagement: `like_count`, plus **`like_and_view_counts_disabled bool`**. Store NULL, not 0, when counts are hidden, so time series do not show fake drops. Also `comment_count`, `comments_disabled`, **`play_count`** (reels), `view_count` (legacy video views), and `video_duration_ms`.
  - Flags: `is_pinned`, `is_paid_partnership`, `sponsor_tags jsonb`.
  - Reels audio: `audio jsonb` (id, title, artist, `is_original`).
  - Media: `accessibility_caption` (alt text), `carousel_count`, `media_ids text[]`.
  - The same coverage columns as Facebook, plus `content_hash`, `status`, `first_seen_at` and `last_seen_at`.
- **`ig_post_index`**: `media_pk PK → (owner_user_id, taken_at)` plus a **unique `shortcode` index**. Instagram URLs use shortcodes (`/p/<code>/`, `/reel/<code>/`).
- **`ig_post_owners`** (`media_pk`, `user_id`, `role`: `owner|coauthor`): **collab posts** appear on several profiles, but the Facebook schema has a single `page_id`. Profile post listings and posting-rate counts should join this table.
- **Carousel children:**
  - Either a `carousel_children` table (`taken_at`, `parent_media_pk`, `position`, `child_media_pk`, `media_type`, `width`, `height`, `usertags`, `accessibility_caption`),
  - or `parent_media_pk` and `position` columns on the `media` table.

  `media_ids` on the post stays the ordered list.
- **`ig_post_metrics`** (daily, change-only, same technique): `like_count`, `comment_count`, `play_count`, `view_count`, and reshare or save counts if they are ever exposed.
- **`ig_comments`**: the same partitioning and change tracking.
  - **Threading is two-level on Instagram**: top-level comments plus child comments, where replies to replies are flattened into the parent thread with an @mention. So `depth ∈ {0,1}`, `root = parent`, and `max_depth` is fixed at 1. Optionally record `reply_to_username` parsed from the leading mention.
  - Replace reactions with `like_count`. Add `child_comment_count`, `is_pinned` (creators can pin comments) and `liked_by_author`.
  - Instagram comments cannot be edited, so `edit_count` will normally stay 0. Keep the columns anyway.
  - Deletion detection carries over unchanged.
- **`ig_comment_authors`**: `author_key = HMAC(user_pk)`, never the username, which is mutable. Store `username` and `profile_pic_url` as PII. Derive `profile_url` from the username.
- **`media`**:
  - Kinds `photo`, `video`, `reel`, `carousel_photo`, `carousel_video`, `thumbnail` (cover frame), `profile_pic`.
  - Instagram returns **several renditions** (`image_versions2.candidates`, `video_versions`), so map `video_policy` to "pick a rendition by width or bitrate", and keep `variant` as `original|hd|sd|thumbnail`.
  - The UUIDv5 media id stays (`ig_media_id + variant`).
- **Keep as-is:** `jobs`, `api_clients`, `profile_schedule` (same columns, keyed by `profile_ref`, resolved to `user_id`), `profile_registrations`, `post_refresh`, `erasure_requests`, `suppression`, `media_acks`, `crawl_cursors`.

### 14.3 Other adaptation suggestions **[suggestion]**
- **Data subjects:**
  - On Facebook the scraped *pages* are treated as non-personal (brands), and only comment authors are PII.
  - On Instagram many tracked **profiles are natural persons** (creators). Treat profile fields (`full_name`, `biography`, `username`) as PII gated by `pii:read`.
  - Make profile erasure the equivalent of today's page erasure (delete plus suppress), and apply `pii_mode: pseudonymized` to profiles that are individuals.
- **Completeness:** Instagram's `comment_count` includes comments the web UI hides (filtered or "hidden comments", restricted accounts), and logged-out comment access is much shallower than on Facebook. Expect systematically low ratios. Use a non-zero `deletion_count_slack`, consider a "comments_accessible" flag, and do not treat a low ratio alone as a failure.
- **Tiers:** Instagram posting rates are lower than Facebook pages. Reuse the T0–T4 shape but retune `min_posts_per_day`, for example T0 ≥ 3/day.
- **Pinned posts:** up to 3 pinned grid posts appear out of chronological order. The incremental stop rule and posting-rate count must ignore them; they are already filtered by window in `AfterJob`.
- **Fix the Facebook gaps from §13 in the new build:**
  - a persister DLQ or quarantine;
  - a suppression check in the persister;
  - a stale-observation guard on posts;
  - job ownership checks;
  - a results query that does not depend on `last_seen_at`. Use a per-job post list instead: the job tracker already has one in the Redis `posts` hash.
  - publish orphan deletion events;
  - add `posts_failed` to the proto;
  - align `comments_days` with `max_age_cap_days` to avoid the partition churn.
- **Shared media handoff:** if one analysis service consumes both platforms, either keep separate `ig.media.*` topics with an identical schema in a shared proto package, or add a `platform` field to `MediaReady`, `MediaProcessed` and `MediaDeleted`, and keep `required_consumers` per platform.
- **Lock keys and names:** choose new advisory-lock keys (Facebook uses `4_200_001` for partitions and `4_200_100` for the leader) and a new consumer-group prefix, so both stacks can share a Postgres or Kafka cluster (or use `kafka.topic_prefix`).
