# 4. Crawl pipeline: tasks, stages, workers, checkpoints, job accounting

[← Index](README.md)

Repository: this repository at commit `265ae3a`. `path:line` references are relative to the repository root and point at that commit.

Scope: `internal/crawl/**` (task, stage, checkpoint, jobs, lanes, emit, rawsink, shadow), `internal/app/crawler.go`, `cmd/fbs-crawler`, `internal/dispatch`, `api/proto/securebrand/fbs/task/v1/tasks.proto`, `internal/platform/kafka`, `configs/topics.yaml`, the `crawler`/`limits`/`kafka` config sections, and the e2e tests. Where behaviour depends on code outside this scope (finalizer, persister, scheduler, GraphQL client), I describe only the interface the crawl layer relies on and cite the file.

---

## 0. Summary

- **Shape.** Cursor pagination runs **inside a task**. Every 1-to-N fan-out goes through a **Kafka topic**:
  - page timeline → one post (comments) task per post;
  - post comments → one thread (replies) task per reply thread;
  - each discovered media item → a media download task.

  A task that has read `yield_pages` (20) pages or run for `yield_after` (60s) stops and produces a **continuation** (same lineage, `seq+1`). Tasks therefore stay short, and lanes can take capacity back (`docs/adr/0002-kafka-postgres-valkey.md:6-13`).
- **Delivery is at-least-once everywhere, and idempotent at every layer:**
  - deterministic task ids;
  - `done:<task_id>` markers;
  - per-scope stream **leases**;
  - per-segment **checkpoints** in Valkey (Redis), which also carry the counters;
  - set-based job accounting in Lua;
  - content-hash event ids on the `fb.*` topics;
  - deterministic media ids (UUIDv5).
- **Kafka consumer.** It runs many records of one partition concurrently and commits only the highest **contiguous** finished offset. A handler error is never skipped: it is retried in-process with backoff. Giving up on a task means routing it explicitly to a retry topic or the DLQ.
- **Retries.** Failed tasks are parked on fixed-delay retry topics (`30s`, `5m`; `1h` exists but is never used). A relay re-produces them when due.
  - Retries are bounded by **stalls**, the retries since the checkpoint last advanced (`max_attempts` 5), and by total attempts (`max_total_attempts` 50).
  - The attempt number escalates the proxy tier.
- **Job completion.** A job is done when `pending = |spawned| − |done|` hits 0 in Valkey. Children are registered before their parent is marked done, so pending can never reach 0 early. The job id then lands in the `jobs:finalize` set. The finalizer waits until the persister has caught up on the entity topics, so "completed" means "queryable".
- **Incremental re-scrapes:**
  - The timeline stops at the Postgres watermark minus `incremental_overlap` (24h), but always re-reads the last 72h.
  - Posts whose comment count did not grow are skipped, using a 45-day per-post cache.
  - Comment reads go newest-first and stop after 5 consecutive comments older than the stored newest.
  - If the count grew more than was found, a full **RECRAWL** pass is spawned.
- **Deletion detection** happens only on **complete full reads**. Every page read appends the top-level ids and reply counts seen to a packed list. The finalizer marks a stored top-level comment deleted, with its replies, only when Σ(1 + reply_count) of what was seen equals Facebook's total count (± slack). **Edits** are counted by the persister when a newer observation carries different text.
- **For Instagram**, almost all of this layer is reusable (Kafka platform, lanes, worker lifecycle, checkpoints and leases, job accounting, retry/DLQ, raw sink, emitter skeleton). The stage bodies, the envelope's scope messages, ref normalisation, the GraphQL registry and bootstrap are Facebook-specific. See §17.
- **Gaps found (details in §16):**
  - `fbctl dlq replay` looks like a no-op for 7 days, because dead-lettered tasks keep their `done:` marker.
  - `max_comments_per_post` is enforced per segment, not per post.
  - Several configured knobs and proto fields are unused: `comments_page_size`, `freshness_p0/p1`, `kafka.compression`, `Task.not_before`/`affinity`/`trace`, `CRAWL_MODE_REFRESH`, and the 1h retry topic.

---

## 1. Architecture overview

```
              API (on-demand, p0)   Scheduler planner (p1)   API page registration (backfill, p2)
                         \                 |                         /
                          +-------- internal/dispatch.Create --------+
                                  | INSERT jobs row (Postgres)
                                  | Jobs.Spawn(root ids)  (Valkey)
                                  | ProduceSync root PageTask (+ PostTasks for post refreshes)
                                  v
  fbs.task.page.{p0,p1,p2} --> [PageStage] --emit--> fb.page.v1, fb.post.v1, fb.post_metrics.v1, fb.media.v1
                                  |  spawn PostTask per post needing comments      \--> fbs.task.media (download)
                                  v
  fbs.task.post.{p0,p1,p2} --> [PostStage] --emit--> fb.comment.v1 (top-level + inline replies), fb.media.v1
                                  |  spawn ThreadTask per reply thread needing its own pagination
                                  v
  fbs.task.thread.{p0,p1,p2} -> [ThreadStage] -emit-> fb.comment.v1, fb.media.v1 (+ nested ThreadTasks up to max_depth)

  any stage: Retry  -> fbs.task.retry.{30s,5m,1h} --[RetryRelay waits x-not-before]--> original topic
             Fail / exhausted -> fbs.task.dlq  (fbctl dlq replay re-produces to x-orig-topic)
             Done / Continue  -> done:<task> marker + Jobs.Done; pending==0 -> SADD jobs:finalize
                                                                              |
                         scheduler Finalizer (leader) <---------------------- +
                         waits for persister offsets >= snapshot -> jobs row terminal, fb.job.v1
```

**Process layout.** `cmd/fbs-crawler/main.go:27-75` takes `--stages page,post,thread,retry` (default `crawler.stages`, which is `[page, post, thread]`). Run one Deployment per stage, or all stages in one process. Each binary exposes `/metrics`, `/healthz` and `/readyz`, with Kafka, Redis and Postgres checks (`main.go:70-74`).

**Wiring.** `internal/app/crawler.go:71-240` builds the following:
- the registry and fingerprints;
- the proxy pool and identity pool (`MaxPerTier = max(16, slots × #stages × 1.3)`, line 103);
- the GraphQL client, with the per-page/global Redis GCRA limiter and the query breakers;
- the Loader, with an optional browser;
- `stage.Deps` and `stage.Worker`;
- one Kafka consumer per (stage, lane), plus the retry relay;
- the AIMD reporter and ceiling;
- the raw sink;
- the shadow set;
- the budget gate that pauses p2.

---

## 2. The task envelope (`api/proto/securebrand/fbs/task/v1/tasks.proto`)

The internal work topics carry protobuf `Task` messages: plain proto bytes with headers `fbs-type: securebrand.fbs.task.v1.Task` and `content-type: application/x-protobuf` (`internal/platform/kafka/producer.go:23-32,97-109`).

### 2.1 Enums

| Enum | Values | Notes |
|---|---|---|
| `Lane` (`:11-19`) | `P0`=1 on-demand API jobs, `P1`=2 scheduled re-scrapes, `P2`=3 backfill | `LANE_UNSPECIFIED` maps to `p1` in `topics.LaneName` (`internal/topics/topics.go:56-65`) |
| `Kind` (`:21-27`) | `PAGE`, `POST` (comments of a post), `THREAD` (replies of a comment), `MEDIA` | `MEDIA` is not handled by the crawler worker; the media downloader consumes it |
| `VideoPolicy` (`:29-35`) | `NONE`, `THUMBNAIL`, `SD`, `HD` | `UNSPECIFIED` behaves as SD (`deps.go:212`) |
| `fb.v1.CrawlMode` (`api/proto/securebrand/fb/v1/common.proto:10-20`) | `FULL`, `INCREMENTAL`, `REFRESH`, `RECRAWL` | `REFRESH` ("counts-only refresh") is **never used** by the crawl code |

### 2.2 `Limits` (`:37-47`)

The job's limits are copied into every child task (`task.NewPost`/`NewThread` copy `parent.Limits`).

| Field | Meaning / where enforced |
|---|---|
| `max_age_seconds` | Timeline cutoff (`task.MaxAgeCutoff`, `task.go:232-237`; page.go:123,207) |
| `max_posts` | Posts emitted per page job; carried across segments in `PageScope.posts_seen` (page.go:214-218) |
| `max_comments_per_post` | Post stage stops when `ItemsSeen >= max`. Only settable via the API, with no config default. **Enforced per segment** (see §16) |
| `max_depth` | Reply-thread depth cap (`post.go:308-316`). 0 means 3. The API accepts 1..5 (`internal/api/jobs.go:46-50`) |
| `include_comments` | When false, no post tasks are created (post state `skipped`) |
| `include_replies` | When false, no thread tasks are created (`post.go:305-307`) |
| `include_media` / `include_comment_media` | Whether **download** tasks are produced. Media **metadata** is always emitted (`deps.go:174-204`) |
| `video_policy` | Picks the video variant to download (`deps.go:206-222`) |

### 2.3 Scope messages (the `oneof scope`)

**`PageScope`** (`:49-58`):
- `page_ref` is the normalised `id:<n>` or `v:<vanity>`;
- `page_id` is 0 until resolved from the page HTML;
- `url` is an optional explicit URL;
- `watermark` is the INCREMENTAL stop time, filled on the first segment from Postgres;
- `posts_seen`, `consecutive_old` and `newest_seen_at` are carried across continuations, so limits and stop conditions span segments.

**`PostScope`** (`:60-70`):
- `post_id`, `page_id` and `post_created_at` (used as the partition key downstream);
- `feedback_id` is Facebook's comment-connection id. It falls back to `base64("feedback:<post_id>")` (`internal/fb/parse/fields.go:129-131`);
- `reported_comment_count` is the count shown on the timeline;
- `permalink` is used as the Referer;
- `comments_watermark` is the newest stored comment time, for an incremental pass;
- `comments_seen` is the count already collected (the "previous reported").

**`ThreadScope`** (`:72-83`):
- `post_id`, `page_id`, `post_created_at`;
- `comment_id` is the parent;
- `root_comment_id`;
- `depth` is the depth of the replies being fetched (1 = direct replies);
- `feedback_id` and `expansion_token` are the reply connection handles;
- `reported_reply_count`.

**`MediaScope`** (`:85-87`) wraps a `fb.v1.Media`.

### 2.4 `Task` fields (`:96-123`)

| # | Field | Semantics |
|---|---|---|
| 1 | `task_id` | Deterministic: `hashID(job, kind, scopeKey, mode, lineage, seq)`. `hashID` is the lower-case unpadded base32 of the first 20 bytes of SHA-256 over the `|`-joined parts (`internal/crawl/task/task.go:22-25,50`) |
| 2 | `lineage_id` | Shared by every continuation of one pagination stream: `hashID("lineage", job, kind, scopeKey, mode)` (`task.go:47-49`). The lineage includes the **job** and **mode**, so two jobs, or a FULL and a RECRAWL pass on the same post, are different streams |
| 3 | `seq` | Segment number within the lineage. Bumped by `Continuation` |
| 4 | `job_id` | UUIDv7 of the job row |
| 5 | `lane` | Inherited by every child |
| 6 | `kind` | |
| 7 | `mode` | FULL/INCREMENTAL/RECRAWL. A page task can downgrade INCREMENTAL to FULL when no watermark exists |
| 8 | `cursor` | Resume point at segment start. Empty means the start of the stream. The Redis checkpoint takes precedence when present |
| 9 | `limits` | §2.2 |
| 10-13 | `scope` | §2.3 |
| 14 | `affinity` (`SessionSnapshot`) | Meant for cursors bound to a session ("decided in Phase 0"). **Never set**; only copied |
| 15 | `attempt` | Every retry increments it. It picks the proxy tier: `identity(class, attempt)` starts at `tiers[min(attempt, len-1)]` and skips tiers whose exits are all behind open breakers (`stage/deps.go:98-109`) |
| 16 | `not_before` | **Unused**. The retry delay travels in the `x-not-before` header instead |
| 17 | `enqueued_at` | Set by `Finalize` when nil |
| 18 | `trace` | Map copied to children. **Never populated** |
| 19 | `stalls` | Retries since the task last advanced its checkpoint. `crawler.max_attempts` applies to these (`worker.go:103-116`) |

**Constructors** (`internal/crawl/task/task.go`):
- `NewPage` (`:97-102`) builds the root of a job.
- `NewPost(parent, mode, scope)` (`:105-110`) and `NewThread` (`:113-118`) inherit job, lane, limits and trace.
- `NewMedia(jobID, m)` (`:122-127`) is always `LANE_P1`. Media is **not part of job accounting**: "a job completes when its data is stored, not when media is".
- `Continuation(t, cursor)` (`:58-65`) clones the task with `seq+1`, the new cursor, `attempt` and `stalls` reset to 0 and `enqueued_at` cleared, then re-finalises it, which gives a new `task_id` and the same lineage.
- `Retry(t, cursor)` (`:69-76`) keeps the same `task_id`, sets `attempt+1`, and replaces the cursor only if non-empty. The worker sets `stalls`.
- `cloneShallow` (`:78-94`) deep-copies the page, post and thread scopes. It does not copy `not_before` or `enqueued_at`.

**ScopeKey** (`task.go:28-43`) identifies the stream for leases and Kafka keys:
- `page:<id>`, or `pageref:<ref>` while the id is unknown;
- `post:<post_id>`;
- `thread:<comment_id>`;
- `media:<media_id>`.

A vanity-only root therefore uses `pageref:v:nike` for seq 0, and its continuations `page:<id>`, because the continuation carries the resolved `page_id` (`page.go:281`).

**Ref normalisation** (`task.NormalizeRef`, `task.go:163-214`; tests `task_test.go:10-31`) accepts:
- facebook.com URLs (any subdomain);
- `profile.php?id=`;
- `pages/<name>/<id>`;
- bare numeric ids;
- vanities made of `[A-Za-z0-9._-]`.

The output is `id:<n>` or `v:<lowercase>`. `RefPath` (`:221-229`) maps it back to `/<id or vanity>`.

**Default limits** (`task.DefaultLimits`, `task.go:130-145`): taken from config. Backfill jobs use `backfill_video_policy` (`thumbnail`).

### 2.5 Topics, keys and partitioning

From `configs/topics.yaml` and `internal/topics/topics.go`. Every name gets `kafka.topic_prefix` prepended (`producer.go:84`, `consumer.go:110-113`). Replicas: 3.

| Topic | Partitions | Retention | Key | Consumer group |
|---|---|---|---|---|
| `fbs.task.page.p0/p1/p2` | 24 each | 72h | `ScopeKey` | `fbs-crawler-page-<lane>` |
| `fbs.task.post.p0/p1/p2` | 48 each | 72h | `ScopeKey` (`post:<id>`) | `fbs-crawler-post-<lane>` |
| `fbs.task.thread.p0/p1/p2` | 48 each | 72h | `ScopeKey` (`thread:<comment_id>`) | `fbs-crawler-thread-<lane>` |
| `fbs.task.media` (no lanes) | 24 | 72h | `media_id` | `fbs-media-downloader` (out of scope) |
| `fbs.task.retry.30s` / `.5m` / `.1h` | 12 each | 48h | the task's `ScopeKey` | `fbs-retry-relay` |
| `fbs.task.dlq` | 6 | 336h (14d) | `ScopeKey` (or the original key for undecodable records) | none (`fbctl dlq replay`) |
| `fb.page.v1` | 12 | 168h | page id | persister |
| `fb.post.v1` | 48 | 168h | post id | persister |
| `fb.post_metrics.v1` | 24 | 168h | post id | persister |
| `fb.comment.v1` | 96 | 168h | **post id**, so a post's comments stay ordered | persister |
| `fb.media.v1` | 24 | 168h | media id | persister |
| `fb.job.v1` | 12 | 168h | job id | downstream |

The topic for a task is `fbs.task.<page|post|thread>.<p0|p1|p2>` (`topics.Task`, `topics.go:37-53`). Task topics are keyed by scope, so every segment and retry of one stream, and every job's task on the same page or post, land on the same partition. Ordering is **not** relied on: up to 64 records per partition run concurrently.

Record headers (`producer.go:23-32`):
- `fbs-type` and `content-type` on every record;
- retry records add `x-orig-topic` (the prefix-less base name of the original task topic) and `x-not-before` (unix milliseconds);
- DLQ records add `x-orig-topic`, `x-reason` and `x-error` (truncated to 1000 characters, `worker.go:183`).

---

## 3. dispatch: how jobs become root tasks (`internal/dispatch/dispatch.go`)

`Dispatcher.Create(ctx, Request, maxRunning)` (`:111-241`) is used by the API (on-demand p0 and backfill p2) and the scheduler planner (scheduled p1).

1. **Idempotency** (`:112-116`). If the request has an `Idempotency-Key` and a client, an existing job with that key is returned (`created=false`).
2. **Page id resolution** (`ResolvePageID`, `:96-107`):
   - an `id:` ref is parsed;
   - otherwise `pages.vanity` is looked up, then `page_schedule.page_id`;
   - an unknown page gets `page_id = 0`, and the page stage resolves it from the HTML.
3. **Mode** (`:119-134`). `auto` or empty becomes `full`, unless the page id is known **and** `crawl_cursors` has a `page_timeline` row for it. In that case it becomes `incremental`. The scheduler always asks for `incremental` (`internal/scheduler/loops.go:130-131`), and backfill asks for `full` (`internal/api/pages.go:145-147`).
4. **Insert the job row** (`jobs`, status `running`), with `params.limits` stored as JSON (`:139-160`).
   - With a client cap (`api.max_running_p0_jobs_per_client`, 50) or a lane cap (`api.max_running_p0_jobs`, 0 = none), counting and inserting happen in one transaction under advisory locks: lane first, then client (`:161-197`).
   - The functions return `ErrLaneFull` or `ErrTooManyJobs`, which the API maps to 429 with `Retry-After: 30`.
5. **Root tasks** (`:209-220`):
   - `task.NewPage(jobID, lane, mode, ref, pageID, "", limits)`;
   - plus one `task.NewPost(root, mode, scope)` per `Request.Posts` entry. These are the scheduler's **post refreshes/recrawls** of older posts that "ride on the page's job" (`internal/scheduler/adaptive.go:150-224,227-295`). Each refreshed post is recorded `pending` in the job's post states, so its coverage is reported with the job.
6. **Register, then produce** (`:221-240`). `Jobs.Spawn(jobID, root ids...)` runs **before** `ProduceSync`. Any failure marks the job row `failed` (`fail`, `:243-246`).

**Lanes per caller:**

| Caller | Lane |
|---|---|
| API `/v1/jobs` | p0 |
| Scheduler planner | p1 |
| Page registration backfill (`/v1/pages` with backfill, or a page that never ran) | p2, with `backfill_video_policy` |

**Dedup of concurrent jobs for the same page.** There is **no dispatch-level per-page dedup**: two API jobs for the same page are both created unless they share an idempotency key. Dedup happens elsewhere:
- **Scheduler:** a due page is skipped when its `last_job_id` is still `running`/`finalizing`/`cancelling` (`internal/scheduler/loops.go:106-112`).
- **Registration:** registering a page twice does not start a second backfill (`internal/api/pages.go:144`; e2e `schedule_test.go:52-59,163-190`).
- **Crawl layer:**
  - The stream lease (`lease:<scopeKey>`, owner = lineage) means two jobs never paginate the same page, post or thread **at the same time**. They interleave segment by segment (§7.3).
  - Per-post and per-thread caches (`pc:`, `tc:`) let the second incremental job skip unchanged posts and threads.
  - Within a job, `Jobs.SetPost` returns `collected=true` for a post the job already finished, so the timeline does not spawn it again (`page.go:234-239`, `jobs.go:124-148`).

`Cancel` (`:321-330`) sets the row to `cancelling` and the Redis `{job:<id>}:cancel` flag. Workers check the flag at task start and between pages.

---

## 4. The worker (`internal/crawl/stage/worker.go`)

`Worker` owns the generic task lifecycle: duplicate suppression, cancellation, stream leases, retries with escalating delay, the DLQ and job accounting (`:22-36`). One `Worker` instance handles every kind. `Stages` maps `KIND_PAGE/POST/THREAD` to the stage implementations (`app/crawler.go:134-140`).

### 4.1 `Handle` and `Run`, step by step (`:43-126`)

1. **Decode.** An undecodable record goes to `deadLetterRaw` (DLQ with the original key and value, reason `undecodable`; `:44-47,200-208`).
2. **Stage lookup.** An unknown kind is dead-lettered with reason `no_stage` (`:56-59`).
3. **Cancelled job.** `Jobs.Cancelled(job)` leads to `finish(t, {"cancelled":1})`: the `done:` marker is set and `Jobs.Done` is called. The task is accounted and nothing is produced (`:60-63`).
4. **Already done.** `Ckpt.IsDone(task_id)` means a duplicate or a redelivery. `Jobs.Done(job, task, nil)` is called again to **repair** a crash that happened between `MarkDone` and `Jobs.Done`, and the worker returns (`:64-69`).
5. **Lease.** `Ckpt.Acquire(scopeKey, owner=lineage, lease_ttl)`. If another lineage owns it, the record is counted as `lease_busy` and the **unchanged** task (no attempt or stall increment) goes to the 30s retry topic (`:70-78`). Lease-busy retries are therefore unbounded, by design.
6. **Snapshot.** The checkpoint `before` is loaded, so the worker can later tell whether a failing run still moved its stream (`:80-82`).
7. **Run.** `st.Run(ctx, t)` is timed into `fbs_task_seconds{stage}`.
8. **Release the lease before producing follow-on work**, so the continuation or follow-up stream can claim it immediately (`:86-88`).
9. **Act on the `Outcome`** (`stage/deps.go:72-82`):
   - **Done.** Spawn `FollowUps` (for example the RECRAWL pass after an incremental one), then `finish` (`:91-96`).
   - **Continue.** Spawn `Next` (the continuation) and the `FollowUps`, then `finish` (`:97-102`).
   - **Retry** (`:103-121`):
     - `next = out.Next` (built by `task.Retry`, so `attempt+1`);
     - `next.Stalls = t.Stalls+1`;
     - the checkpoint is reloaded, and **if it advanced** (`after.Cursor != before.Cursor || after.Pages > before.Pages`, `:131-133`), `Stalls` resets to 0;
     - `exhausted(next)` (`:137-149`) returns `max_attempts` when `stalls >= MaxAttempts` and `max_total_attempts` when `attempt >= MaxTotalAttempts` (default `10 × MaxAttempts` if 0). Exhausted tasks are dead-lettered;
     - otherwise the task goes to `toRetry(next, out.Delay)`.
   - **Fail.** The task is dead-lettered with reason `permanent`.
10. **`finish`** (`:151-157`): `Ckpt.MarkDone(task_id)` (TTL 7d), then `Jobs.Done(job, task, counters)`. An error from `Jobs.Done` (for example "never spawned" for a replay of an old job) is **logged and swallowed**, so it cannot block the consumer (`ignoreUnspawned`, `:210-217`).
11. **`toRetry`** (`:160-176`) maps the delay to a topic: `"30s"` → `Retry30s`, `"5m"` → `Retry5m`, `"1h"` → `Retry1h`. It sets `x-orig-topic = topics.Task(kind, lane)` and `x-not-before = now + wait`, keeps the key `ScopeKey`, and produces synchronously.
12. **`deadLetter`** (`:178-198`):
    - produces to the DLQ with `x-orig-topic`, `x-reason` and `x-error`;
    - adds `tasks_failed` to the counters;
    - for a POST task, sets the job's post state to `failed`;
    - then `finish`. The task counts as done for the job, which then ends `partial`.

**Errors returned from `Run`** (Redis down, a produce failure in `spawn`, `toRetry` or `deadLetter`) are **not** dead-lettered. The Kafka consumer re-invokes the handler with exponential backoff (200ms, doubling up to ≥30s, plus jitter) until it succeeds or shutdown begins (`consumer.go:248-270`). The offset is never committed past an unprocessed record (`consumer.go:18-23`).

### 4.2 Retry relay (`worker.go:226-260`; wiring `app/crawler.go:178-187`)

- Consumer group `fbs-retry-relay` on the three retry topics, with `MaxInflight 64`, **`MaxInflightPerPartition 1`** and `DrainTimeout 5s`.
- Each retry topic has a fixed delay, so the head of a partition is always the next one due. The relay sleeps until `x-not-before`, then re-produces key and value to the original topic, copying only the `fbs-type`, `content-type` and `traceparent` headers.
- A record without `x-orig-topic` is dropped (returns nil).
- On shutdown the sleep returns `ctx.Err()`. The record is then abandoned and not committed, so it is redelivered.

### 4.3 How stages map response classes to outcomes (`stage/deps.go:136-150`)

| Class (`internal/fb/detect`) | Outcome |
|---|---|
| `RateLimited`, `Transport`, `Checkpoint`, `LoginWall` | Retry, 30s |
| `SoftError` | Retry, 30s, reason prefixed `soft:` |
| `RegistryStale`, `SchemaDrift` | Retry, **5m** (waits for a registry or parser update) |
| anything else | Retry, 30s |

Other outcomes:
- `ContentUnavailable` on a page HTML load ends the task as **Done** with `pages_unavailable`.
- Stage-level permanent failures (**Fail**): a missing scope, or "page id unresolved".
- Nothing currently returns `"1h"`.

The **login-wall truncation rule** (`wallTruncates`, `page.go:333-341`) applies to page, post and thread stages:
- a login wall at the **same cursor** is counted in the checkpoint (`Walls`, `WallCursor`, saved before the retry);
- each retry uses a fresh identity;
- on the 2nd wall at that cursor the stream is declared **truncated** instead of retried (`timelines_truncated`, `posts_truncated`, `threads_truncated`; the post state becomes `truncated`);
- any successful page resets the counter (`page.go:256`, `post.go:158`, `post.go:418`).

---

## 5. The stages

Every stage follows the same skeleton:
- acquire an identity for a routing class, escalating tiers with `attempt`;
- load the checkpoint `ckpt:<lineage>:<seq>`;
- add the saved counters to this run's counters (`st.Resume(c)`), so a retried or redelivered segment still reports the pages it read before;
- fall back to the task's cursor when there is no checkpoint;
- loop over pages until finished, or until yield, drain or cancel;
- per page: request, classify, parse, run the shadow comparison, emit asynchronously into a `kafka.Batch`, `Batch.Wait` (all acks), spawn children (synchronously, after registering them with the job), `Pages++`, save the checkpoint.

**Yield conditions** (checked before each page):
- `kafka.IsDraining(ctx)` (shutdown);
- `st.Pages >= crawler.yield_pages` (20);
- `time.Since(started) > crawler.yield_after` (60s).

On yield, the stage returns `Continue` with `task.Continuation(t, st.Cursor)`.

### 5.1 PageStage (`internal/crawl/stage/page.go`)

Purpose: load the page profile and paginate the timeline. It emits posts, post metrics and media, and spawns one comments task per post whose comments need (re)collecting (`:26-29`).

1. **Identity** class `"timeline"` (`:42-47`). Without an identity: Retry 30s.
2. **Checkpoint** load and resume (`:48-60`). With no saved cursor, the stage seeds `Cursor`, `PostsSeen`, `ConsecutiveOld` and `NewestSeenUnix` from the task and its `PageScope`.
3. **Page HTML** (`:62-114`), loaded on `seq == 0`, or on any segment whose identity is not bootstrapped yet:
   - URL: `sc.url`, otherwise `RefPath(id:<page_id> or page_ref)` on the identity's base URL.
   - `Loader.Load`. The load is reported to AIMD and the budget (`observeLoad`, `deps.go:124-134`) and counts as a request.
   - `ContentUnavailable`: on seq 0, emit `Page{status: UNAVAILABLE}` (`emitUnavailable`, `:321-331`), then Done with `pages_unavailable: 1`.
   - Other non-OK classes go through `classOutcome`.
   - On seq 0 only:
     - `parse.Page(html)`. Missing required fields count as `SchemaDrift` and retry in 5m;
     - a GDPR page suppression check (Done with `pages_suppressed`);
     - set `sc.PageId` and `page.RequestedRef`;
     - emit `fb.page.v1` and wait for acks; `pages++`.
4. If `page_id` is still 0, the task **Fails** ("page id unresolved"). The suppression check is repeated (`:115-120`).
5. **Window** (`:122-146`):
   - `cutoff = now − max_age_seconds`.
   - In INCREMENTAL mode, the watermark comes from `sc.watermark`, or on the first segment from Postgres: `crawl_cursors.newest_seen_at − scheduler.incremental_overlap` (24h; `deps.go:225-239`).
   - **No row: the mode switches to FULL** ("never crawled: full pass").
   - `stopBefore = min(watermark, now − 72h)`. `refreshHorizon = 72h` (`:31-33`): an incremental pass always re-reads at least the last 72h of posts, so their counts stay fresh.
6. **Pagination loop** (`:155-260`):
   - Cancellation is checked before each page. Cancelled means Done `{"cancelled":1}`. This segment's other counters are dropped.
   - GraphQL `timeline` with `{page_id, count: crawler.timeline_page_size (10), cursor}`:
     - `Fast: lane == p0` uses the p0 per-page RPS (6 vs 3);
     - `Lane` feeds the traffic budget;
     - `Referer` is the base URL.
   - Login wall: the truncation rule (§4.3). Other non-OK classes go through `classOutcome` (retry from `st.Cursor`).
   - `parse.Timeline`, then `GQL.Parsed(resp, ok)`, which feeds the query breaker and captures OK-but-drifted responses to the raw sink. Shadow comparison when the parse is OK. Missing optional fields go to metrics. Missing required fields mean SchemaDrift, 5m.
   - `NodeNull` on the very first page (no pages, no cursor): the page is unavailable.
   - **For each post** (`:203-243`):
     - If `created < cutoff` or `created < stopBefore`: skip it, and increment `ConsecutiveOld` **unless it is pinned**. Old pinned posts at the top therefore don't stop the scan.
     - Otherwise reset `ConsecutiveOld`.
     - `max_posts` reached: `finished`.
     - Update `PostsSeen`, `NewestSeenUnix` and the segment's `oldest`.
     - `Emit.Post` publishes `fb.post.v1` plus an `fb.post_metrics.v1` sample, with key post id.
     - `d.media(...)` emits `fb.media.v1` for each media and queues download tasks per policy.
     - Counters `posts` and `media_queued`.
     - `commentsTask` (`:291-319`) decides whether a child is needed:
       - `include_comments == false` gives state `skipped`;
       - a count **known** to be 0 gives state `complete`, with no task;
       - otherwise build a `PostScope`, using the `feedback_id` fallback when missing;
       - in INCREMENTAL mode with a post cache entry `pc:<post_id>` that says `reported <= prev.Reported && prev.ThreadsComplete`: state `skipped` (unchanged);
       - if the cache exists but the count grew: child mode INCREMENTAL, `comments_watermark = prev.NewestComment`, `comments_seen = prev.Reported`;
       - otherwise child mode FULL.
     - If a child is needed: `Jobs.SetPost(..., pending)`, and spawn **unless** the job already collected this post (a refresh root of the same job finished first). Otherwise `SetPost` records the decided state.
   - `Batch.Wait`, then `spawn(children)` (§7.5), then `Pages++`.
   - **Stop** when `ConsecutiveOld >= crawler.consecutive_old_stop` (3), or there is no next page or end cursor. Otherwise advance the cursor.
   - Reset the wall counters and **save the checkpoint**. A failed save is only logged.
7. **After the loop** (`:261-287`):
   - `pages_fetched += Pages`.
   - If finished or truncated: write the durable **timeline watermark** (`savePageWatermark`, `deps.go:242-266`). This is an upsert into Postgres `crawl_cursors(scope_type='page_timeline', scope_id=page_id, ordering='default')` with:
     - `newest_seen_at = GREATEST(old, new)`;
     - `oldest_seen_at = LEAST`;
     - `state` complete or truncated, and `truncated_reason`;
     - `last_full_at` or `last_incremental_at` set to now, depending on mode.

     A failed write means Retry 30s. Then the task is Done.
   - Otherwise yield: the continuation carries `PageId`, `PostsSeen`, `ConsecutiveOld`, `Watermark` and `NewestSeenAt`, and `next.Mode = mode` (which can now be FULL).

### 5.2 PostStage (`internal/crawl/stage/post.go:32-209`)

Purpose: paginate a post's top-level comments, with any inline replies, and spawn a thread task per reply thread that needs its own pagination.

1. **Identity** class `"comments"`. `ensureBootstrapped` loads `/<page_id>` HTML on a fresh identity to get tokens (`deps.go:112-120`). A classified failure goes through `classOutcome`; otherwise Retry 30s.
2. **Checkpoint** load and resume. The cursor falls back to `t.Cursor`.
3. **Query choice** (`:64-69`):
   - `comments` uses Facebook's "RANKED_UNFILTERED_CHRONOLOGICAL" intent, an unfiltered chronological list (`configs/queries.seed.yaml:48-65`);
   - in INCREMENTAL mode with a `comments_watermark`, `comments_newest` uses the `RECENT_ACTIVITY_INTENT_V1` newest-first intent (`:67-84`), with `watermark = comments_watermark − 1 minute`.
4. **`fullPass`** (`:222-238`). Is this lineage a full pass for deletion detection? It needs:
   - `scheduler.post_refresh.detect_deletions`;
   - a job id;
   - mode FULL or RECRAWL;
   - on seq 0 with no checkpoint progress, either RECRAWL mode or an existing `pc:` cache entry (a post crawled before). If so, `Jobs.BeginPass(job, lineage, post, now)`.

   Later segments and retries ask `Jobs.InPass(job, lineage)`.
5. **Loop** (`:84-173`). Each iteration checks yield and cancellation, then:
   - GraphQL with `{feedback_id, cursor}` and Referer = permalink. There is no page-size variable: `commentsAfterCount: -1` means the server default.
   - Login wall truncation, `classOutcome`, parse (`parse.Comments` with `CommentContext{PostID, PageID, PostCreatedAt, PageAuthorID}`), shadow, drift.
   - `NodeNull` on the first page: post state `complete`, Done `posts_unavailable`.
   - **`emitComments`** (`:258-293`), for every comment, top-level and inline replies:
     - track `NewestSeenUnix`;
     - count comments older than the watermark;
     - `Emit.Comment` applies privacy and publishes `fb.comment.v1` keyed by post id;
     - `ItemsSeen++`;
     - counters `comments` (depth 0) and `replies` (depth > 0);
     - a redacted comment counts as `comments_redacted` and its media is **not** queued;
     - otherwise media metadata, and download tasks per `include_comment_media`.
   - Incremental stop accounting (`:134-140`):
     - if every comment on the page was older, `olderRun += older`;
     - otherwise `olderRun = older`.
   - `threads := threadTasks(...)` (§5.3), `Batch.Wait`, `spawn(threads)`.
   - `reported = res.TotalCount` when present.
   - If `pass`: `Jobs.AddSeen(job, post, topLevelSeen(res))`, the depth-0 ids with their reply counts.
   - `threads_spawned`, `Pages++`, reset walls.
   - **Stop** (`:159-169`):
     - no next page or end cursor: `finished` and `exhausted`;
     - `max_comments_per_post` reached;
     - incremental with `olderRun >= 5` (`incrementalOlderStop`, `:28-30`).

     Otherwise advance the cursor. Save the checkpoint.
6. **Finish** (`:176-205`):
   - If `pass && exhausted && !truncated`: `Jobs.EndPass(job, lineage, post, post_created_at, reported)`. Only a read that reached the last page can feed deletion detection.
   - Job post state `complete` or `truncated`.
   - Update the cross-job **post cache** `pc:<post_id>` (45d): `{Reported: sc.ReportedCommentCount, NewestComment: max(prev, st.NewestSeenUnix), ThreadsComplete: !truncated}`.
   - **Recrawl follow-up**: in INCREMENTAL mode, when not truncated, if `grew = reported_comment_count − comments_seen > 0` and `st.ItemsSeen < grew`, add `FollowUps = [NewPost(t, RECRAWL, scope without watermark)]` and count `recrawls_spawned`. A newest-first pass cannot see replies added to older comments, so a full pass is done. The follow-up has a new lineage (different mode) and starts after this task released the lease.
7. Otherwise yield: `Continuation(t, st.Cursor)`.

### 5.3 ThreadStage and `threadTasks` (`post.go:295-439`)

**`threadTasks`** (`:303-329`) turns `parse.ThreadRef`s into thread tasks. The parser emits a ThreadRef when a comment's `reply_count > inline replies` returned with it (`internal/fb/parse/comments.go:122-143`). The function:
- returns nothing when `include_replies` is false;
- uses `maxDepth = limits.max_depth`, or 3 when it is 0;
- **skips a ref when `ref.Depth + 1 >= maxDepth`**, or when it has no `feedback_id`. With `max_depth = 3`, depths 0, 1 and 2 are collected;
- in INCREMENTAL mode (of the parent task), skips a thread whose `reply_count <= tc:<comment_id>` (the reply count stored when that thread was last fully read);
- creates the child with **mode FULL always**, `depth = ref.Depth + 1`, `root_comment_id`, `feedback_id`, `expansion_token` and `reported_reply_count`.

Inline replies deeper than `max_depth` that come back inside a response are still emitted. The cap only limits spawning.

**ThreadStage.Run** (`:334-439`) works like PostStage, with these differences:
- identity class `"replies"`;
- query `replies` with `{feedback_id, expansion_token, cursor}`;
- parse with `CommentContext{BaseDepth: depth, ParentCommentID, RootCommentID, Replies: true}`;
- no watermark;
- emits the replies and spawns **nested** thread tasks via `threadTasks`;
- stops only at the end of the list.

On finish:
- if truncated: `threads_truncated` and post state `truncated`;
- otherwise `Ckpt.SetThread(comment_id, reported_reply_count)` (45d cache).

### 5.4 Media

There is no crawler media stage. `d.media` (`deps.go:174-204`) emits metadata for **all** media. It queues a `MediaTask` (`fbs.task.media`, key `media_id`, lane p1, outside job accounting) only when `include_media` (or `include_comment_media` for comment media) is set **and** `wanted(media, policy)` holds (`:206-222`):

| Media | Rule |
|---|---|
| Video, reel, comment video | HD policy: `hd`, or `sd` when no `hd` variant exists. SD or unspecified: `sd`. Otherwise none |
| Thumbnail | Downloaded unless the policy is `none` |
| Everything else (photos, stickers, GIFs, covers…) | Always |

Media ids are UUIDv5 of `fb_media_id + variant`, stable across scrapes (`api/proto/securebrand/fb/v1/media.proto:27-29`).

---

## 6. Concurrency, lanes and backpressure

### 6.1 Consumers per stage and lane (`app/crawler.go:176-222`)

For each enabled stage (page, post, thread) and each lane with `crawler.lanes[lane] > 0`, the crawler builds **one consumer group** `fbs-crawler-<stage>-<lane>` on `fbs.task.<stage>.<lane>`, with:
- `MaxInflight = lanes[lane]` (that lane's cap);
- `MaxInflightPerPartition = crawler.max_inflight_per_partition` (64);
- `DrainTimeout` 75s by default;
- `Slots` = this stage's shared **lane scheduler** (when `crawler.slots > 0`).

`retry` adds the relay consumer.

**Slots 0** means fixed slots per lane. p2 still gets a one-lane scheduler of its own, so the budget can pause it (`:205-211`).

### 6.2 The lane scheduler (`internal/crawl/lanes/lanes.go`)

- `New(total, lanes...)` (`:44-54`) keeps lanes in priority order: p0, then p1, then p2. `Max` is clamped to `[1, total]`; minimums are trimmed so their sum stays ≤ total.
- `Acquire(ctx, lane)` (`:58-81`) queues a waiter and runs `dispatch()`. It returns false when ctx is done, handing back a slot granted while it was giving up.
- **`pick()`** (`:113-125`):
  1. first, the highest-priority waiting lane with `used < Min` (its guaranteed share);
  2. otherwise the highest-priority waiting lane with `used < Max`.
- **Capacity is lent, never pre-empted.** Idle p0 slots go to p1 and p2. They come back as those tasks finish, and tasks yield within 60s or 20 pages.
- `SetMax(lane, 0)` **pauses** a lane; its minimum is ignored while paused (`:133-146`). The traffic-budget gate uses this to pause backfill (`app/crawler.go:224-238`).
- `OnChange` feeds `fbs_lane_slots{stage,lane,state=used|waiting}` (`app/crawler.go:253-256`).

**Defaults** (per pod, per stage):
- `slots: 240`;
- `lanes: {p0: 240, p1: 240, p2: 96}`. p2 is capped at **40%**;
- `lane_min: 12` (5%), so no lane starves however busy the others are (`configs/default.yaml:160-169`).

The unit tests prove lending, priority on release, minimum share beating priority, cancellation, clamping, a 48-goroutine stress run never exceeding capacity or the p2 cap, and pause/resume (`lanes_test.go`).

### 6.3 Consumer semantics (`internal/platform/kafka/consumer.go`, `tracker.go`)

**Client setup** (franz-go, `:118-127`): consumer group with `prefix+group`, autocommit **disabled**, `CooperativeStickyBalancer`, reset to **earliest** when the group has no committed offset (no task is ever skipped), `FetchMaxWait 500ms`, and revoke and lost callbacks.

**Poll loop** (`:161-212`): `PollRecords(MaxInflight)`. For each record, in fetch order:
1. `tracker.add` registers the offset;
2. acquire the global semaphore (`MaxInflight`);
3. acquire the **per-partition** semaphore (`MaxInflightPerPartition`);
4. acquire the **shared lane slot** last ("a record waiting for its partition must not hold capacity other lanes could use");
5. run the handler in a goroutine. The slot and both semaphores are released when it returns.

**Backpressure.** The loop blocks in these acquires, so a full lane stops polling. Kafka lag grows and KEDA scales on it (runbook `docs/runbooks/operations.md:143-151`).

**Commit** (`:272-298`, tracker `:68-107`). Every `CommitInterval` (2s default), the consumer commits, per partition, the **first offset that is not done yet**, or `next` if everything is done. This is the highest contiguous completed point (`tracker_test.go` covers out-of-order completion).

**At-least-once.** A crash redelivers at most the in-flight window plus anything after the first unfinished record.

**Handler errors** (`process`, `:248-270`) are retried with backoff, 200ms doubling up to ≥30s, plus jitter, until success or cancellation. A record abandoned at shutdown is **not** marked done.

**Graceful shutdown** (`:214-234`):
1. Stop polling, then close `drainCh`. `kafka.IsDraining(ctx)` becomes true, and stages break at the next page boundary, returning a continuation.
2. Wait up to `DrainTimeout` for every in-flight record, by filling the global semaphore.
3. On timeout, set `abandoned`, cancel the handler contexts, and leave the abandoned offsets uncommitted. "A crawler that hit its drain timeout committed the offsets of the tasks it abandoned" was a real bug that is now fixed (`docs/runbooks/load-testing.md:117`).
4. Final commit, then close and leave the group.

**Rebalance** (`onRevoked`, `:300-309`): wait up to 15s for the revoked partitions' in-flight records, commit what is done, then drop the tracking. Records still running finish on the old consumer while their offsets are redelivered to the new owner. The duplicate is absorbed by idempotency (§8). `onLost` just drops the tracking.

### 6.4 Producer semantics (`producer.go`)

- `RequiredAcks(AllISR)`, zstd with lz4 and none as fallbacks, linger `kafka.produce_linger` (5ms), batch max 1 MiB, auto topic creation allowed. franz-go's idempotent producer is on by default.
- `ProduceSync` is used for **task records** (children, retries, DLQ).
- Entity events go through **`Batch`**: asynchronous `Produce` per record, then `Wait` at the page boundary before the checkpoint is saved (`:127-179`).
- The `kafka.compression` config key is **not read**: compression is hard-coded.

---

## 7. Checkpoints and coordination state

### 7.1 Valkey keys (`internal/crawl/checkpoint/checkpoint.go`, `internal/crawl/jobs/jobs.go`)

| Key | Value | TTL | Written by | Read by |
|---|---|---|---|---|
| `ckpt:<lineage>:<seq>` | JSON `State`: `cursor`, `pages`, `posts_seen`, `consecutive_old`, `newest_seen`, `items_seen`, `walls`, `wall_cursor`, `counters{}` (`checkpoint.go:17-30`) | 24h (`:67-70`) | stages after every page, and before a login-wall retry | stages at start; worker before and after `Run` (progress) |
| `done:<task_id>` | `"1"` | 7d (`:79-81`) | `Worker.finish` | `Worker.Run` |
| `lease:<scopeKey>` | owner = lineage | `crawler.lease_ttl` (120s), renewed every ttl/3 by a goroutine with a Lua compare-and-`PEXPIRE` (`:83-139`) | `Acquire` (`SETNX`; the same owner may re-acquire after a crash) | `Release` is a Lua compare-and-`DEL` (`:142-146`) |
| `pc:<post_id>` | JSON `{reported, newest_comment, threads_complete}` | 45d (`:148-172`) | PostStage finish | PageStage `commentsTask`; PostStage `fullPass` ("known post") |
| `tc:<comment_id>` | reply count last fully read | 45d (`:174-185`) | ThreadStage finish (not truncated) | `threadTasks` in INCREMENTAL mode |
| `{job:<id>}:*` | job accounting (§9) | 7d, shortened to 24h after finalisation | | |

`Load` treats an unparsable checkpoint as absent (`:59-62`). `State.Resume` adds the saved counters to this run's counters (`:33-37`); `saveCkpt` stores a clone of the current counters (`post.go:212-215`).

### 7.2 Postgres

- **`crawl_cursors`** (`migrations/00001_init.sql:198-211`) holds the **durable** per-page timeline watermark: `newest_seen_at`, `oldest_seen_at`, `state`, `truncated_reason`, `last_full_at`, `last_incremental_at`. It has a `cursor` column that is **never written**, so in-stream resume points are Redis-only. Dispatch reads the table for `auto` mode, and PageStage reads it for the incremental boundary.
- **Also relevant downstream**: `comments.first_seen_at` (deletion detection), `post_refresh` (the scheduler's per-post refresh plan) and `jobs`.

**Consequence** (ADR 0002): "losing Valkey costs at most re-crawling recent pages":
- tasks restart their segment from the envelope's cursor;
- `done:` markers vanish, so duplicates are re-run idempotently;
- caches vanish, so posts get FULL comment reads;
- job accounting vanishes, so the reaper marks running jobs `failed` with "job state lost" (`scheduler/loops.go:251-255`).

### 7.3 How retries resume

- A retry keeps the **same task id, lineage and seq**. It reloads the same `ckpt:<lineage>:<seq>` and resumes at the saved cursor, with the saved page count and counters.
- `task.Retry` also writes the stage's current cursor into the retried envelope, so even a lost checkpoint resumes close to where the task stopped.
- A continuation starts a new `seq` with an empty checkpoint. The cursor comes from the envelope, and the page-level running state (`posts_seen`, `consecutive_old`, `newest_seen_at`, `watermark`) comes from `PageScope`.
- **Stall counting**: "dead-letter on retries without checkpoint progress". After a Retry outcome, the worker compares the checkpoint before and after the run. Any saved page (cursor changed, or pages increased) resets `stalls` to 0. A stream that moves forward between rate limits therefore never hits `max_attempts`. Only one that keeps failing at the same place does, after 5 consecutive non-advancing retries. `max_total_attempts` (50) is an absolute cap (`worker.go:103-116,128-149`; tests `worker_test.go`).
- **Attempts** count every retry. They are never reset by progress, only by a continuation, and they choose the proxy tier.

**Lease behaviour across jobs.** Lease owner = lineage, and lineage includes the job id.
- A second job's task on the same scope gets `lease_busy` and comes back every 30s.
- The lease is released at the end of **each segment**, so two jobs' streams on one page interleave segment by segment but never overlap.
- The same lineage can re-acquire its own lease. Two copies of one task (for example a rebalance redelivery while the old consumer still runs it) can therefore run at the same time on the same checkpoint key. This is safe because everything downstream is idempotent, but it wastes requests.

### 7.4 Order of operations within one page (why at-least-once is safe)

1. emit entities (async batch);
2. `Batch.Wait` (all acked);
3. `spawn(children)`: `Jobs.Spawn` (ids registered), then `ProduceSync` of the child task records;
4. `Pages++`;
5. `saveCkpt`.

A crash anywhere before step 5 re-reads that page. It re-emits the same entities (same event ids) and re-spawns the same children (same task ids, so no double accounting, and the duplicate records are skipped by the `done:` marker or the lease).

### 7.5 `spawn` (`deps.go:153-171`)

Builds the records (topic `topics.Task(kind, lane)`, key `ScopeKey`), then **`Jobs.Spawn(job, ids...)` before `ProduceSync`**. Registering first guarantees that a child that finishes extremely fast cannot decrement `pending` below the true value.

---

## 8. At-least-once delivery vs idempotency: every mechanism

| Layer | Duplicate source | What makes it harmless |
|---|---|---|
| Kafka consumer | crash or rebalance redelivers uncommitted offsets | contiguous commit; everything below is idempotent |
| Task identity | re-produced child, retry, redelivery | deterministic `task_id`; `done:<task_id>` skip; `Jobs.Done` repair path (`worker.go:64-69`) |
| Concurrent pagination | two jobs on one page/post/thread | `lease:<scope>` per lineage |
| Partial segment | retry or redelivery mid-segment | `ckpt:<lineage>:<seq>` resume, with counters |
| Job accounting | double spawn or double done | Lua `SADD`-gated `INCRBY`/`DECR`; counters applied only on the first done (`jobs.go:35-65`; `jobs_test.go:12-63`) |
| Entity events | same page emitted twice | `Observation.event_id = xxh3_128(entityKey | content_hash)`. The persister upserts only on a changed `content_hash`, and ignores observations older than the stored one (`internal/persist/writer.go:174-178,257-283`) |
| Media | same media seen twice | UUIDv5 `media_id`; the downloader dedups |
| Deletions | finalizer retried | deletion event id includes the job id; `persist.MarkDeleted` is idempotent; `comments_deleted` is added **before** `ClearPasses` (`scheduler/finalizer.go:273-286`) |
| Post states | refresh root and timeline in the same job | `setPostScript` keeps a terminal state (`jobs.go:124-148`; `jobs_test.go:65-93`) |

Counters are therefore **"never under, possibly over"**. A page re-read after a crash may count twice (stated explicitly in `chaos_test.go:101-112`).

---

## 9. Job accounting and how the finalizer is fed

### 9.1 Keys

All under the hash tag `{job:<id>}`, so every key of a job is in one cluster slot and the Lua scripts work (`jobs.go:33`). TTL 7d.

| Key | Type | Content |
|---|---|---|
| `spawned` | SET | every task id ever registered |
| `done` | SET | every task id finished (done, continued, cancelled, dead-lettered) |
| `pending` | STRING int | `|spawned| − |done|`, maintained atomically |
| `counters` | HASH | summed task counters: `posts`, `comments`, `replies`, `requests`, `pages`, `pages_fetched`, `comment_pages`, `media_queued`, `threads_spawned`, `tasks_failed`, `timelines_truncated`, `posts_truncated`, `threads_truncated`, `pages_unavailable`, `posts_unavailable`, `pages_suppressed`, `comments_redacted`, `recrawls_spawned`, `cancelled`, `comments_deleted` (added by the finalizer) |
| `last` | STRING | unix time of the last spawn or done, used by the reaper |
| `posts` | HASH | `post_id → "<created_unix>|<reported>|<state>"`, state in pending, complete, truncated, skipped, failed |
| `cancel` | STRING | cancellation flag |
| `passes` | HASH | `lineage → "<post_id>|<start_ms>"` (full passes begun) |
| `passed` | HASH | `post_id → "<created>|<start_ms>|<reported>"` (full passes that reached the last page) |
| `seen:<post_id>` | STRING | packed 12-byte entries (int64 comment id, uint32 reply count), appended per page read |

### 9.2 Scripts

- `Spawn` (`:35-47,75-84`): `SADD` each id; `INCRBY pending` by the number actually added; refresh the TTLs and `last`.
- `Done` (`:49-65,88-110`):
  - if the task is not in `spawned`, return −1 (an error, swallowed by the worker);
  - if `SADD done` adds it: `DECR pending`, and `HINCRBY` each counter;
  - refresh the TTLs and `last`;
  - **if the pending count returned is 0, `SADD jobs:finalize <job>`**.

### 9.3 Why it cannot finish early

- The root is registered by dispatch before it is produced.
- Every child is registered by the parent's `spawn` **before** the parent's own `Done`. For `Continue`, the next segment is spawned before `finish`; for `Done`, the `FollowUps` are spawned before `finish`.
- Retries and lease-busy tasks never call `Done`.
- Cancelled and dead-lettered tasks do call `Done`, so the job always converges.
- Media tasks are excluded, so a job's completion does not wait for downloads.

### 9.4 The finalizer (`internal/scheduler/finalizer.go`; leader-only, `scheduler.finalizer_interval` 2s)

1. `ReadyToFinalize(500)` (`SRANDMEMBER`). Jobs stay in the set until `Finalized`, so a leader change loses nothing (`jobs.go:374-384`). Losing them was a real bug before (`load-testing.md:118`).
2. For newly seen jobs, snapshot the **end offsets** of the entity topics `fb.page.v1`, `fb.post.v1`, `fb.comment.v1` and `fb.media.v1` (`topics.Entity`). Wait until the persister group has committed past them (`kafka.GroupCaughtUp`, `internal/platform/kafka/admin.go:74-92`), or until `scheduler.finalize_timeout` (10m) passes, in which case it finalises anyway with a warning.
3. `finalize`:
   - load the job row (skip it if already terminal);
   - read `Jobs.Status` and `Jobs.Posts`;
   - run **deletion detection** (§11);
   - compute **coverage**. Posts in states complete, truncated, failed or pending are included; pending and failed are written as `partial`. A single SQL statement updates `posts.comments_collected`, `replies_collected`, `completeness = min(1, collected / comment_count_total)`, `comments_state` and `comments_synced_at`, and returns per-post ratios for the job's median and min plus the `comment_completeness_ratio` histogram (`:360-435`);
   - decide the status (`:170-178`):
     - `cancelled` if the row was `cancelling`;
     - `partial` if any post is truncated or failed, or `tasks_failed > 0`, or `timelines_truncated > 0`;
     - `failed` if `pages_unavailable > 0` and `posts == 0`;
     - else `completed`;
   - adaptive re-tiering and post-refresh registration (out of scope);
   - update the `jobs` row (counters, completeness, finished_at) and `page_schedule.consecutive_failures`;
   - emit `fb.job.v1` (key job id, `event_id = "<job>:<status>"`);
   - shorten the Redis TTLs to 24h; `SREM` from the finalize set.
4. **Reaper** (`scheduler/loops.go:226-275`, `reaper_after` 30m). Jobs `running` or `cancelling` for longer than 30m become:
   - `failed` ("job state lost") when `Spawned == 0`;
   - `partial` ("stalled with pending tasks") when `pending > 0` and there has been no activity for 30m.

**Completeness inputs**: the job's `posts` hash (states and the reported counts), Postgres comment counts per post (visible only) against `posts.comment_count_total`, and the task counters.

**Live status**: `Dispatcher.Get` merges `Jobs.Status` (counters and `pending_tasks`) into the API view while the job runs (`dispatch.go:276-293`).

---

## 10. Incremental re-scrape logic (end to end)

1. **Dispatch** chooses `incremental` when `crawl_cursors` has a row for the page. The scheduler always asks for it.
2. **Timeline** (PageStage):
   - boundary = `min(newest_seen_at − 24h, now − 72h)`;
   - posts older than the boundary are skipped, and 3 consecutive old non-pinned posts stop the scan;
   - the watermark is resolved once and carried in `PageScope.watermark` across segments;
   - a page with no watermark row is read FULL.
3. **Per post** (`commentsTask`):
   - skip if the `pc:` cache says it is unchanged (`reported <= prev.Reported`) and its own pagination completed last time;
   - with a cache entry and a grown count: an INCREMENTAL comments task with `comments_watermark` and `comments_seen`;
   - otherwise FULL.
4. **Comments** (PostStage INCREMENTAL): the `comments_newest` ordering, stopping after 5 comments older than `watermark − 1min` (accumulated across pages that were entirely old). Threads are spawned only if their reply count exceeds `tc:`.
5. **Recrawl fallback**: if the count grew by more than the items found (new replies on old comments), the stage spawns a RECRAWL pass of the post: a full chronological read, which is also a deletion-detection pass.
6. **Older posts** (beyond the timeline window) are refreshed by the scheduler as extra **post roots** on the page's scheduled job (`dispatch.Request.Posts`). `adaptive.plan` (`scheduler/adaptive.go:227-295`) decides per post:
   - INCREMENTAL from the stored newest comment, with `CommentsSeen = reported`, so no recrawl follow-up is triggered. "Replies added to old threads are caught by the recrawls";
   - FULL if nothing is stored yet;
   - RECRAWL at the `recrawl_at` ages (7d, 30d) for posts with ≥ `recrawl_min_comments` (20).

   The windows are 24h up to 7d old, 72h for 7–14d, and 168h for 14–30d (T0/T1 pages only), with exponential backoff (up to 2^3) when nothing changed.

The e2e test proves an incremental job collects new posts and comments with **far fewer requests** than the full job (`e2e_test.go:230-252`). It also proves a refresh of a 2–6-day-old post collects comments the timeline scan does not reach (`schedule_test.go:31-144`).

---

## 11. Comment edits and deletions

### 11.1 Edits

Edits are detected by the **persister**, not the crawler. On upsert, a newer observation (`last_seen_at >=` the stored one) of a non-redacted comment whose `text IS DISTINCT FROM` the stored text increments `edit_count` and sets `edited_at` (`internal/persist/writer.go:250-283`; metric `fbs_comment_changes_total{kind="edited"}`).

The upsert also only fires when `content_hash` differs, or the stored row is more than a day old. An edit is therefore seen whenever any read (full, recrawl, or an incremental read that overlaps the comment) re-emits the comment. An edit made before the comment was first stored is not counted (`soak_test.go:287-291`).

### 11.2 Deletions

Deletions are detected only after a **complete full read**:

1. **Pass bookkeeping in the crawler.** PostStage `fullPass` → `BeginPass`. Each page read calls `AddSeen` with the top-level ids and their reply counts. `EndPass` runs only if the read reached the last page and was not truncated, recording Facebook's `total_count` from the last page.
2. **Finalizer `deletions`/`missing`** (`scheduler/finalizer.go:242-356`), for each post with a `passed` entry:
   - `covered = Σ over seen top-level (1 + reply_count)`;
   - if `reported <= 0` or `|covered − reported| > round(deletion_count_slack × reported)`, the read may have been partial (Facebook withholding comments). The post is **skipped** (`fbs_deletion_checks_total{outcome="skipped_partial"}`);
   - otherwise (`checked`), select the stored top-level comments of the post that are `visible`, not redacted, and **`first_seen_at < pass start`**, and are **not in the seen list**. These are deleted, together with all their stored replies (`root_comment_id = ANY(...)`);
   - publish one `fb.comment.v1` record per deleted comment via `Emitter.CommentDeleted` (`emit.go:96-104`):
     - only the keys are set, with `status = DELETED`;
     - the entity key includes the job id, so the event id is unique per job;
     - one shared timestamp is used, so a later re-sighting restores the comment and its replies together;
   - `persist.MarkDeleted`, `AddCounters({"comments_deleted": n})`, then `ClearPasses` (which deletes the seen lists and the passes/passed hashes).
3. **Cancelled jobs** only clear their pass records.
4. **Re-sightings.** A later ordinary record of a deleted comment with `last_seen_at > deleted_at` makes it visible again, together with the replies deleted with it (`writer.go:253-254,266-272`, metric `kind="restored"`).
5. **Limits.** Reply-level deletions are not detected, only top-level comments, with their replies cascaded. "Deleted" means no longer publicly visible (deleted, hidden or filtered) (`docs/contracts/kafka-schemas.md:21-30`).

Deletion detection requires `scheduler.post_refresh.detect_deletions: true` and `deletion_count_slack: 0` (exact match). Validation keeps the slack in [0, 1) (`config.go:441-443`).

---

## 12. Emitter, raw sink and shadow mode

### 12.1 Emitter (`internal/crawl/emit/emit.go`, `normalize.go`)

- `Source{JobID, Query, DocID, ParserVersion, Mode}` describes how the entities were obtained.
- **`stamp`** (`emit.go:52-65`):
  - clear `obs`;
  - `content_hash = xxh3_128(deterministic proto marshal of hashable(msg))`;
  - `event_id = hex(xxh3_128(entityKey + "|" + content_hash))`;
  - set `Observation{event_id, observed_at, job_ids: [job], query_name, doc_id, parser_version, content_hash, mode}`.
- **`hashable`** (`normalize.go:14-59`) clones the message and strips the query string of any string or repeated string that carries signed-CDN parameters (`oe=` or `_nc_`). Re-signed URLs don't change the hash, but the published message keeps the real URL (`emit_test.go:11-33`).
- **Entities and keys:**

  | Method | Topic | Entity key | Kafka key |
  |---|---|---|---|
  | `Page` | `fb.page.v1` | `page:<id>` | page id |
  | `Post` | `fb.post.v1` | `post:<id>` | post id |
  | `Post` (metrics sample) | `fb.post_metrics.v1` | `post_metrics:<id>` | post id |
  | `Comment` | `fb.comment.v1` | `comment:<id>` | **post id** |
  | `CommentDeleted` | `fb.comment.v1` | `comment-deleted:<id>:<job>` | post id |
  | `Media` | `fb.media.v1` | `media:<id>` | media id |

  The `Post` method also emits a `PostMetrics` sample (reactions, comments, shares, views, `captured_at`).
- **Privacy** (`Privatize`, `:108-123`):
  - `author_key = HMAC(privacy.author_hmac_key, author.id)`;
  - a **suppressed author** (GDPR erasure) gets text, text ranges, attachments and URL cleared, `redacted = true`, and the comment's media is not downloaded;
  - in `pii_mode: pseudonymized`, the author id, name, URL and avatar are stripped.

  Pages are suppressed at the page stage (`Suppression.PageSuppressed`). The suppression set refreshes every `crawler.suppression_refresh` (60s).
- **Dedup downstream** is by `event_id`, among records of the same status (`kafka-schemas.md:30`).

### 12.2 Raw sink (`internal/crawl/rawsink/rawsink.go`; wiring `app/crawler.go:157-161`, `cmd/fbs-crawler/main.go:60-65`)

- **What is captured.** The GraphQL client calls `Raw.Capture` for:
  - every response when `fb.capture_raw: all`;
  - otherwise (`failures`, the default) only non-OK classes (`internal/fb/graphql/client.go:204-206`), plus responses classified OK whose parse drifted (`Parsed`, `client.go:86-102`), filed as `SchemaDrift`;
  - nothing when `none`.

  HTML page loads are **not** captured.
- **Record format.** One JSONL line per response: `{at, query, class, vars, body (if valid JSON) | text}`. The request's template vars (`page_id`, `feedback_id`, `cursor`…) are kept so the response can be re-parsed with its context.
- **Buffering.** One buffer per `<prefix>/<query>`, where the prefix is `raw/` for OK and `raw-failures/` otherwise. A buffer is flushed when it reaches **8 MiB**, or every **minute** (`sink.Run(ctx, time.Minute)`), and on shutdown.
- **Objects.** Written as **zstd** to `s3.raw_bucket` (`fbs-raw`) under `<prefix>/<query>/YYYY/MM/DD/HH/<pod>-<seq>.jsonl.zst` (`:105-127`).
- **Replay.** `fbctl replay [--prefix raw-failures|raw] [--query …] [--since …] [--fixtures-dir …]` re-parses the captures and writes scrubbed fixtures (`cmd/fbctl/replay.go`, `internal/replay`).
- **Retention.** `privacy.retention.raw_payload_days: 7` exists in config but is **not enforced in code**. The go-live runbook asks operators to put a 7-day lifecycle rule on the raw bucket (`docs/runbooks/go-live.md:48`).
- **Sampling.** There is no sampling knob for raw capture. It is all, failures or none.

### 12.3 Shadow mode (`internal/crawl/shadow/shadow.go`)

Purpose: prove a **candidate parser version on live traffic** before it becomes active.

- A candidate registers itself in `shadow.Candidates.Timeline` or `.Comments` from its file's `init`.
- `shadow.Configured(fb.shadow_rate, raw)` (default 0.05) returns nil when no candidate is compiled in or the rate is 0 (`:173-178`).
- On a sampled response whose **active** parse was OK, the candidate parses it as well, under `recover`, so a panic only counts as `error`.
- The comparison (`:123-161`, outcome recording `:107-120`):
  - for posts: the deterministic proto bytes of each post plus its media, keyed by post id;
  - for comments: each comment's bytes keyed by id, plus a sorted string of the thread refs.
- Outcomes are `same`, `differs`, `drift` (the candidate misses required fields) and `error`, counted in `fbs_parse_shadow_total{query,outcome}`.
- Any non-`same` response is captured to `raw-failures/shadow-<query>/` with `shadow` and `shadow_missing` vars, for `fbctl replay --query shadow-<query>`.
- **Only the active result is emitted.** The workflow is in `docs/runbooks/operations.md` ("Proving a new parser version"). Tests: `shadow_test.go`.

---

## 13. Configuration knobs and constants

### 13.1 `crawler` (`configs/default.yaml:158-185`; struct `internal/config/config.go:217-237`)

| Key | Default | Effect |
|---|---|---|
| `stages` | `[page, post, thread]` | Stages run by this process. `retry` adds the relay. Override with `--stages` |
| `slots` | 240 | Shared in-flight tasks per pod **per stage**. 0 gives fixed per-lane slots |
| `lanes.p0` / `p1` / `p2` | 240 / 240 / 96 | Per-lane cap. Also each lane consumer's `MaxInflight`. p2 is 40% |
| `lane_min` | 12 | Guaranteed slots for a waiting lane (5%) |
| `max_inflight_per_partition` | 64 | Concurrent records per partition |
| `yield_pages` | 20 | Pages per segment before yielding (must be > 0) |
| `yield_after` | 60s | Time per segment before yielding |
| `timeline_page_size` | 10 | `count` variable of the timeline query |
| `comments_page_size` | 50 | **Unused** (the comments query uses `commentsAfterCount: -1`) |
| `consecutive_old_stop` | 3 | Old non-pinned posts in a row that end the timeline |
| `max_attempts` | 5 | Retries without checkpoint progress, then DLQ `max_attempts` |
| `max_total_attempts` | 50 | Retries in all, then DLQ `max_total_attempts` (0 means 10 × max_attempts) |
| `lease_ttl` | 120s | Stream lease TTL. Heartbeat every 40s |
| `freshness_p0` / `freshness_p1` | 15m / 6h | **Unused** in code |
| `suppression_refresh` | 60s | GDPR suppression set reload |

### 13.2 `limits` (`default.yaml:187-197`)

| Key | Default |
|---|---|
| `max_age_days` | 365 (validated ≤ `max_age_cap_days`) |
| `max_age_cap_days` | 1095 (API cap; also sizes the partitions) |
| `max_posts` | 0 (unlimited) |
| `max_depth` | 3 |
| `include_comments` / `include_replies` / `include_media` / `include_comment_media` | true |
| `video_policy` | `sd` |
| `backfill_video_policy` | `thumbnail` |

The API also accepts `max_comments_per_post`, which has no config default.

### 13.3 `kafka` (`default.yaml:29-55`)

| Key | Default | Notes |
|---|---|---|
| `brokers` | `["localhost:19092"]` | |
| `client_id` | `fbs` | Suffixed `-crawler` |
| `topic_prefix` | `""` | Applied to topics **and** group names |
| `produce_linger` | 5ms | |
| `compression` | `zstd` | **Unused**: always zstd > lz4 > none |
| `tls.*`, `sasl.*` | | `plain`, `scram-sha-256`, `scram-sha-512` |
| `schema_registry.*` | | For `fbctl schemas apply` |

Overrides come from env vars `FBS_<SECTION>__<KEY>`.

### 13.4 Other knobs the pipeline reads

| Key | Default | Use |
|---|---|---|
| `scheduler.incremental_overlap` | 24h | Timeline watermark overlap |
| `scheduler.finalizer_interval` | 2s | |
| `scheduler.finalize_timeout` | 10m | |
| `scheduler.reaper_after` | 30m | |
| `scheduler.post_refresh.detect_deletions` | true | |
| `scheduler.post_refresh.deletion_count_slack` | 0 | |
| `scheduler.post_refresh.*` | | Refresh windows and recrawls |
| `scheduler.budget.*` | | Pauses p2 |
| `fb.capture_raw` | `failures` | |
| `fb.shadow_rate` | 0.05 | |
| `fb.page_rps` / `page_rps_p0` | 3 / 6 | Per page, fleet-wide |
| `fb.global_rps` | 50 | AIMD start |
| `fb.identity_rps` | 0.33 | |
| `fb.proxy.routing` | | Tiers per class: `timeline`, `comments`, `replies`, `default` |
| `api.max_running_p0_jobs_per_client` | 50 | |
| `api.max_running_p0_jobs` | 0 | |
| `privacy.*` | | HMAC key, PII mode |

### 13.5 Hard-coded constants

| Constant | Value | Where |
|---|---|---|
| Incremental refresh horizon | 72h | `page.go:33` |
| Login-wall truncation | 2 walls at one cursor | `page.go:340` |
| Incremental older-stop | 5 comments | `post.go:30` |
| Comment watermark slack | 1 minute | `post.go:68` |
| Default `max_depth` when 0 | 3 | `post.go:308-311` |
| Checkpoint TTL | 24h | `checkpoint.go:69` |
| `done:` TTL | 7d | `checkpoint.go:80` |
| `pc:` / `tc:` cache TTL | 45d | `checkpoint.go:155` |
| Job key TTL | 7d; 24h after finalisation | `jobs.go:22`; `finalizer.go:216` |
| Seen-entry size | 12 bytes | `jobs.go:228` |
| Finalize set | `jobs:finalize`, 500 per tick | `jobs.go:20`; `finalizer.go:61` |
| Consumer defaults | MaxInflight 64, commit every 2s, drain 60s | `consumer.go:91-102` |
| Crawler drain | 75s | `app/crawler.go:171-174` |
| Relay drain / inflight / per partition | 5s / 64 / 1 | `app/crawler.go:181` |
| Handler retry backoff | 200ms ×2, up to ≥30s, plus jitter | `consumer.go:249-268` |
| Revoke grace | 15s | `consumer.go:303` |
| `FetchMaxWait` | 500ms | `consumer.go:124` |
| Producer acks / batch | all ISR / 1 MiB | `producer.go:66-69` |
| Raw buffer flush | 8 MiB; every 1 min | `rawsink.go:36`; `crawler.go:160` |
| DLQ `x-error` truncation | 1000 characters | `worker.go:183` |
| Identity pool per tier | `max(16, slots × stages × 1.3)` | `app/crawler.go:103` |

---

## 14. Metrics emitted by this layer (namespace `fbs_`, `internal/platform/metrics/metrics.go`)

| Metric | Labels | Emitted at |
|---|---|---|
| `fbs_tasks_total` | stage, lane, outcome (`done`, `yield`, `retry`, `failed`, `cancelled`, `lease_busy`) | `worker.go` |
| `fbs_task_seconds` | stage | `worker.go:85` |
| `fbs_task_yields_total` | stage | page, post, thread |
| `fbs_retries_total` | stage, tier (the delay `30s`/`5m`) | `worker.go:118` |
| `fbs_dlq_total` | stage, reason (`undecodable`, `no_stage`, `max_attempts`, `max_total_attempts`, `permanent`) | `worker.go:179,201` |
| `fbs_lane_slots` | stage, lane, state (`used`, `waiting`) | lane scheduler |
| `fbs_parse_total` | query, parser_version, outcome (`ok`, `drift`) | stages. The version label is hard-coded `"1"` |
| `fbs_parse_missing_field_total` | query, field | stages. Not emitted for `replies` |
| `fbs_parse_shadow_total` | query, outcome | shadow |
| `fbs_timeline_truncated_total` | reason | `page.go:175` |
| `fbs_kafka_produce_total` | topic, outcome | producer |
| `fbs_kafka_commits_total` | group, outcome | consumer |
| `fbs_fb_requests_total` | query, class, tier | GraphQL client |
| `fbs_breaker_state` | name (`query:<name|doc_id>`) | query breakers |
| `fbs_jobs_total` | status, lane | finalizer and reaper (lane `unknown` from the reaper) |
| `fbs_comment_completeness_ratio` | lane | finalizer |
| `fbs_deletion_checks_total` | outcome (`checked`, `skipped_partial`) | finalizer |
| `fbs_comment_changes_total` | kind (`edited`, `restored`) | persister |
| `fbs_post_refreshes_total` | kind (`refresh`, `recrawl`) | scheduler |

Runbook targets: retries and DLQ "low, flat" (`docs/runbooks/operations.md:21`).

---

## 15. What the tests prove

### 15.1 Harness (`internal/e2e/stack.go`)

The whole pipeline runs **in-process** against real Postgres, Redpanda and Valkey (via `testenv`), an in-memory S3 (gofakes3) and **fakefb** (a synthetic Facebook with ground truth, faults, inline replies, mutations, withholding and hiding). Settings: `yield_pages=2` (to exercise continuations), `timeline_page_size=3`, `slots=24`, lanes 24/16/8, `lane_min=2`, topics capped at 3 partitions, `capture_raw=none`.

The harness can restart three component groups independently:
- **crawler**: a pod replacement, with a configurable drain. A short drain is a crash that abandons in-flight tasks;
- **persister**;
- **finalizer**: a leader change, which drops in-memory state.

### 15.2 End-to-end tests

**`TestEndToEnd`** (`e2e_test.go:92-294`):
- A full API job (`auto` → full) stores **exactly** fakefb's posts and comments, including depth-2 replies, with completeness 1.0 (median and min) and `fb.job.v1` COMPLETED with the right count.
- API reads hide author identity without the `pii:read` scope.
- Media is downloaded, then deleted on acknowledgement or TTL.
- After `World.Mutate`, an **`auto` job runs incremental**, collects the new posts and comments exactly, and uses **fewer requests than the full job**.
- An erased author stays redacted after a full re-scrape.
- No identity exceeded its pacing.

**`TestBlockedIdentitiesRetryAndTruncation`** (`:296-349`): every 4th GraphQL request is rate-limited. Tasks retry on other identities through the retry topics and still collect everything. The job ends `completed`, and the AIMD windows saw every response and every block.

**`TestCommentEditsAndDeletions`** (`changes_test.go:27-282`). Setup: post A loses 2 top-level comments (one with replies) and has one edited; B loses 80% of its comments; C has 80% withheld and D 100%. A second full job then shows:
- `comments_deleted` equals exactly the deleted comments plus their replies, in the store, the API (`?status=deleted`) and `fb.comment.v1` (deletion records keyed by post with the job id, and no text);
- C and D are untouched (two `skipped_partial` checks);
- exactly one edit is counted;
- coverage counts only visible comments.

Further rounds:
- a third full job deletes nothing;
- a hidden comment with replies is marked deleted as a whole thread, then **restored** with its replies when shown again (`kind="restored"` metric).

**`TestChaosRestartsAndFaults`** (`chaos_test.go:25-152`). Setup: 3 pages, rate limits on every 11th request, 20ms of latency, and ≥3 crawler restarts with a **20ms drain** (abandoning in-flight work), plus persister and finalizer restarts (up to 12 in all). Results:
- every job `completed`;
- counters are ≥ the truth ("never zero times");
- the store holds **exactly** the world's posts and comments, with no missing, extra or duplicate rows;
- no comment is marked deleted or edited, and every post has completeness 1;
- a second round under chaos changes nothing and deletes nothing.

**`TestSoak`** (`soak_test.go`, opt-in `FBS_SOAK=1`, 20m by default). The scheduled pipeline runs with post refreshes and recrawls, continuous world mutations (new posts and comments, comments on old posts, edits, deletions) and a random restart every 90s. It then checks:
- no job is failed, partial or stuck for more than 10 minutes;
- after a final full job per page, every live comment is stored, visible and carries its **current text**, and every deleted one is marked deleted;
- edits are counted;
- no jobs are left in `jobs:finalize` and no `seen:` lists are left behind;
- goroutines are bounded.

A 90-minute CI run (2,870 jobs, 44 restarts, 19,683 comments) passed exactly (`docs/runbooks/load-testing.md:100-106`).

**`TestAdaptiveScheduleAndPostRefresh`** and others (`schedule_test.go`):
- a backfill leads to tiering and post_refresh rows;
- new comments on a 2–6-day-old post are collected by the next scheduled job's refresh root, although the timeline does not reach the post;
- budget stretch works;
- the p0 fleet cap returns 429;
- re-registering a page does not re-backfill.

**Load tests** (`load_test.go`, opt-in):
- 200 pages × 30 posts: about 422 Facebook requests per second and 2,900 comments per second in one 4-CPU process, about 1.9 ms of CPU per request;
- **the thread stage holds almost all the backlog**, so give it the most replicas (`load-testing.md:20-60`).

The registry and browser e2e tests are out of scope.

### 15.3 Unit tests

- `task_test.go`: deterministic ids; retries keep the id; continuations get a new id, the same lineage and reset counters.
- `worker_test.go`: `advanced` and `exhausted`.
- `jobs_test.go`: accounting under duplicates; terminal post states kept; passes and seen lists, including retried pages and multiple lineages.
- `lanes_test.go`.
- `tracker_test.go`: contiguous commits.
- `emit_test.go`: hash stability, privacy.
- `shadow_test.go`.

---

## 16. Observations, gaps and probable bugs

Each item below is verified against the code. Severity is my judgement.

1. **`fbctl dlq replay` is very likely a no-op for 7 days.** Medium.
   - `Worker.deadLetter` calls `finish`, which runs `Ckpt.MarkDone(task_id)` with a 7d TTL (`worker.go:197,152`).
   - `fbctl dlq replay` re-produces the **same value**, and so the same `task_id` (`cmd/fbctl/commands.go:187-238`).
   - `Worker.Run` then sees `IsDone`, calls `Jobs.Done`, and returns without running the task (`worker.go:64-69`). The runbook's "ship the fix, then `fbctl dlq replay --reason max_attempts`" (`docs/runbooks/operations.md:96-97,132`) would only work after the marker expires. DLQ retention is 14d.
   - Even when it runs, the job has usually been finalised already: `Jobs.Done` errors with "never spawned" once the keys expire, and that error is swallowed.
   - Fix ideas: delete `done:<id>` during replay, or replay as a new task id (for example a bumped `seq`/attempt namespace), and re-open accounting or attach the replay to a new job.
2. **`max_comments_per_post` is enforced per segment.** Low. `ItemsSeen` lives in the per-segment checkpoint and is not carried in `PostScope` by `Continuation` (`post.go:163`, `task.go:58-65`). A post longer than one segment (20 pages) can exceed the limit. The same per-segment scope affects `st.ItemsSeen < grew` (the recrawl decision) and `NewestSeenUnix`, but in the safe direction (more reads).
3. **`ThreadsComplete` in the post cache ignores the thread tasks.** Low/medium. It is set from the post's own truncation only (`post.go:193`). A reply thread that was dead-lettered or truncated does not stop a later incremental pass from skipping the post as "unchanged" (`page.go:308`). Those replies only come back with a recrawl or a full job.
4. **Unused configuration and envelope fields.** `crawler.comments_page_size`, `crawler.freshness_p0/p1`, `kafka.compression`, `privacy.retention.raw_payload_days` (bucket lifecycle only), `Task.not_before`, `Task.affinity`, `Task.trace` (and no `traceparent` is ever set), `CRAWL_MODE_REFRESH`, the `fbs.task.retry.1h` topic (no stage returns `"1h"`), and `crawl_cursors.cursor`.
5. **Cancelling mid-segment** returns `done({"cancelled":1})` and drops that segment's other counters (`page.go:159-161`, `post.go:88-90`, `post.go:372-374`).
6. **`fbs_parse_total`'s `parser_version` label is hard-coded `"1"`** instead of `parse.*Version`. The thread stage does not record `ParseMissingField` for `replies`.
7. **Lease-busy tasks bounce through `retry.30s` without bounds.** By design, but a very long stream held by another job costs a relay hop every 30s. The same lineage can re-acquire its own lease, so a redelivered duplicate can run concurrently with the original. That is safe but wasteful.
8. **The page `oldest` is per segment.** `oldest_seen_at` uses `LEAST` in SQL, so this is harmless.
9. **No per-page dedup at the API.** Two concurrent on-demand jobs on one page both crawl it, serialised per segment by the lease. Consider "attach to the running job" semantics for Instagram.

---

## 17. Instagram adaptation notes

### 17.1 Classification of each piece

**(a) Generic, reusable as-is.**
- **`internal/platform/kafka`**: producer, `Batch`, the consumer with in-partition concurrency, contiguous commits, drain/abandon semantics, `Slots`, `IsDraining`, the tracker, `EnsureTopics`, `GroupCaughtUp`.
- **`internal/crawl/lanes`**: the priority-lane slot scheduler, `SetMax` pause.
- **`internal/crawl/checkpoint`**: segment checkpoints, `done:` markers, leases with heartbeat, and the generic per-entity count caches (`pc:`/`tc:` renamed).
- **`internal/crawl/jobs`**: Lua accounting, the finalize set, cancellation, post states, full passes and seen lists. The "post" naming should be generalised, but the logic is platform-neutral.
- **`stage.Worker` and `RetryRelay`**: lease, done-skip, stall/attempt logic, retry topics, DLQ, `finish`.
- **`rawsink`**: capture, buffering, zstd JSONL to S3, replay format.
- **`dispatch` skeleton**: job row, advisory-lock caps, idempotency key, root registration before produce.
- **`task` mechanics**: `hashID`, `Finalize`, `Continuation`, `Retry`, `ScopeKey` pattern.
- **Finalizer and reaper flow**: offset-snapshot "completed means queryable", coverage, status rules.
- **Emitter mechanics**: `stamp`/`hashable` content-hash event ids, HMAC author keys, suppression and redaction, the URL-signature stripping. Instagram CDN URLs carry the same `oe=`/`_nc_*` parameters on Meta's CDN; **verify**.

**(b) Reusable with Instagram parameters.**
- **Task proto**: new `Kind`s and scope messages (§17.3), and an `ig.task.*` topic family (or a platform segment in the names). `Lane`, `Limits` (add `include_reels`, `include_carousel_children`), `VideoPolicy`, `attempt` and `stalls` stay as they are. **`affinity` becomes useful** if Instagram cursors turn out to be bound to the session or CSRF token.
- **`topics`**: `ig.profile.v1`, `ig.post.v1`, `ig.post_metrics.v1`, `ig.comment.v1` (keyed by media id), `ig.media.v1`, `ig.job.v1`. Keep the same key strategy.
- **`NormalizeRef`/`RefPath`**: `instagram.com/<username>`, the `@username` form, and numeric user ids. Instagram usernames are `[a-z0-9._]`, ≤ 30 characters, case-insensitive.
- **`classOutcome` mapping**: Instagram-specific `detect` classes, for example HTTP 429 or a "Please wait a few minutes" JSON for rate limits, a redirect to `/accounts/login/` for the login wall, and the `checkpoint_required` challenge. The 30s/5m policy and the 2-wall truncation rule carry over.
- **Identity routing classes** (`timeline`/`comments`/`replies`, renamed). Expect much tighter per-identity and per-profile rates. Logged-out Instagram is stricter than Facebook (**verify in a Phase-0 spike**).
- **Incremental logic**:
  - Postgres watermark per profile timeline, overlap and refresh horizon;
  - `consecutive_old_stop` with pinned-post exemption (Instagram has up to 3 pinned grid posts);
  - the per-post count cache;
  - newest-first comment reads with the older-stop, only if Instagram exposes a newest-first comment ordering logged-out (**verify**). Otherwise incremental comment reads degrade to "re-read until N already-stored ids are seen" in the default ordering, or to count-gated full reads;
  - the recrawl follow-up.
- **Deletion detection.** The algorithm is generic, but the **completeness formula depends on count semantics**: Facebook's total equals Σ(1 + reply_count). If Instagram's `comment_count` excludes child comments, the formula becomes Σ1 = count (**verify**). The slack knob already exists.
- **Video policy `wanted()`**: map Instagram `video_versions` (several widths and types) to sd and hd variants; `display_resources` and `image_versions2` candidates to photo variants.
- **Shadow**: the parser function types must be Instagram signatures. The rest of the set is generic.

**(c) Facebook-specific; replace.**
- Page HTML **bootstrap and token extraction** (`Loader`, `ensureBootstrapped` loading `/<page_id>`), LSD/fb_dtsg-style tokens, the browser fallback triggers.
- The **GraphQL doc_id registry** and query templates (`timeline`, `comments`, `comments_newest`, `replies`), `feedback_id` (including the `base64("feedback:<id>")` fallback), `expansion_token`, and Facebook's `NodeNull` unavailability signal.
- Facebook's **multi-level reply threading** (`max_depth` 3, nested ThreadTasks, `BaseDepth`) and inline-reply detection.
- `PageStatus` and the `page_html` parser, the `id:`/`v:` ref forms, `profile.php`, and `pages/<name>/<id>`.
- `CommentsListComponentsPaginationQuery` intents (RANKED_UNFILTERED_CHRONOLOGICAL for completeness).

### 17.2 How Instagram's model differs (things the stage graph must absorb)

The items below are things to **verify in a Phase-0 spike**, like `docs/runbooks/phase0-spike.md` did for Facebook.

- **Comment threading depth is 1.** Replies to a reply are attached to the same top-level parent, usually with an @mention.
  - The thread stage never spawns nested threads. `max_depth` is effectively fixed at 2 levels (0 and 1); drop the depth check or validate `max_depth ∈ {1, 2}`.
  - Responses typically include a few **preview child comments** inline with a `child_comment_count`. Keep the FB rule: spawn a replies task only when `child_comment_count > inline previews` (cf. `parse/comments.go:136`).
- **Carousels (sidecars)** are one post with N child media items (each with its own id, type and dimensions), up to 10–20.
  - Emit one `Media` per child, with an index or ordinal field (add one to the media proto).
  - Apply the video policy per child.
  - The post's `content_hash` should include the child list so reorders and removals register.
- **Reels (clips)** have a play or view count and may be absent from the grid ("also share to feed" off). Suggestion: a separate **reels stream** per profile (its own lineage and lease scope `reels:<user_id>`, same stage code with a different query) when the grid feed does not include all reels. Put the play count in `PostMetrics.view_count`.
- **Collab posts** appear on several profiles. Deduplicate by media pk: the lease scope `media:<pk>` and per-job `SetPost` already prevent double comment reads in one job. Record co-authors rather than assuming one owner `page_id`.
- **Captions can be edited.** Comment editing is limited or absent on Instagram (**verify**). Edit tracking is more valuable at **post** level, which is persister work outside this layer: the content hash already changes on a caption edit, so only an edit counter is missing.
- **Comments may be disabled, or hidden by filters** ("hidden comments", restricted accounts, offensive-word filters).
  - Treat `comments_disabled` like a known zero, giving post state `skipped`.
  - Treat a filter-hidden shortfall as **withheld**, which the partial-read rule already skips for deletion checks.
- **Like counts may be hidden** ("hide like counts"). Make metrics optional, as the FB proto already does with `optional` counts.
- **IDs.** Media `pk` values are ~19-digit integers (fit in int64). There is also a `shortcode`, and composite ids `<pk>_<owner_id>`. Comment and user `pk`s are int64. Keep `shortcode` in the scope for URL building and the Referer.
- **Pagination tokens** differ by endpoint (`end_cursor`/`has_next_page`, `next_max_id`, `next_min_id`, `min_id`/`max_id` for comments). The checkpoint `Cursor` string handles any of them. If a cursor is bound to a session or CSRF token, persist the identity snapshot in `Task.affinity` and route retries to that identity, or restart the stream on a 400.
- **Page sizes.** The grid pages are typically 12 items. Make the size per query in the registry (unlike FB's unused `comments_page_size`).

### 17.3 Proposed Instagram stage graph (suggestion)

```
dispatch (API p0 / scheduler p1 / backfill p2)
   └─ ProfileTask (ig.task.profile.<lane>)               scope key  profile:<user_id> | uref:<username>
        seq 0: load profile (resolve user_id, bio, counts, is_private/unavailable) -> ig.profile.v1
        then paginate the profile's media feed (grid): for each item -> ig.post.v1 + ig.post_metrics.v1
             + ig.media.v1 per carousel child / video variant / thumbnail  -> ig.task.media (per policy)
             + CommentsTask if comments enabled, count > 0 and (FULL or count grew vs cache)
        stop: max_age window, N consecutive old non-pinned items, max_posts, end of feed
   └─ (optional) ReelsTask (ig.task.reels.<lane>)        scope key  reels:<user_id>
        same as above over the clips feed, only items not already seen in the grid (job post-state dedup)
   └─ CommentsTask (ig.task.comments.<lane>)             scope key  ig_post:<media_pk>
        paginate top-level comments (+ inline preview replies) -> ig.comment.v1 (key media pk)
        full pass bookkeeping (BeginPass/AddSeen/EndPass) for deletion detection
        incremental: newest-first + older-stop if available; else count-gated
        spawn RepliesTask when child_comment_count > previews (and > tc: cache in incremental)
   └─ RepliesTask (ig.task.replies.<lane>)               scope key  ig_comment:<comment_pk>
        paginate child comments (depth 1 only); no nested spawns
   └─ MediaTask (ig.task.media)  -> media downloader (outside job accounting)
   retry: ig.task.retry.{30s,5m}; DLQ: ig.task.dlq   (reuse Worker / RetryRelay unchanged)
```

Suggested envelope (illustrative; reuse `tasks.proto` with new scope messages):

```proto
enum Kind { KIND_UNSPECIFIED = 0; KIND_PROFILE = 1; KIND_COMMENTS = 2; KIND_REPLIES = 3; KIND_MEDIA = 4; KIND_REELS = 5; }
message ProfileScope { string username = 1; int64 user_id = 2; google.protobuf.Timestamp watermark = 3;
  int64 posts_seen = 4; int32 consecutive_old = 5; google.protobuf.Timestamp newest_seen_at = 6; }
message PostScope { int64 media_pk = 1; string shortcode = 2; int64 owner_id = 3; google.protobuf.Timestamp taken_at = 4;
  int64 reported_comment_count = 5; google.protobuf.Timestamp comments_watermark = 6; int64 comments_seen = 7;
  int64 items_seen = 8; /* carry across continuations: fixes §16.2 */ string product_type = 9; }
message RepliesScope { int64 media_pk = 1; int64 owner_id = 2; google.protobuf.Timestamp taken_at = 3;
  int64 parent_comment_id = 4; int64 reported_child_count = 5; }
```

Keep `lineage_id`, `seq`, `attempt`, `stalls`, `cursor`, `limits`, `lane`, `mode` and `affinity` unchanged.

### 17.4 Things to change when porting (suggestions)

1. Fix the DLQ replay versus `done:` interaction (§16.1) before relying on DLQ replays.
2. Carry `items_seen` (and the newest comment seen) in the scope across continuations (§16.2).
3. Make `ThreadsComplete` reflect the reply tasks: for example, have the replies stage clear a per-post "complete" bit on truncation or DLQ (§16.3).
4. Remove or wire the dead knobs (§16.4). Put page sizes in the query registry.
5. Consider per-profile dedup of concurrent on-demand jobs (return the running job), since Instagram budgets per profile will be tight.
6. Keep the e2e harness approach: a **fake Instagram** with ground truth, faults (429, login redirects, challenges), mutations (new posts, comments, edits, deletions, hidden comments, carousel edits) and the chaos and soak tests that compare the store exactly against the truth. They are what found the real bugs listed in `docs/runbooks/load-testing.md:115-120`: lost quoted posts, offsets committed for abandoned tasks, finalizer losing jobs, undercounted counters, false partial jobs.
