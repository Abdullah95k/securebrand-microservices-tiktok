# fbs technical reference (base for the Instagram scraper)

This folder describes how `fbs`, the Facebook public-page scraper in this repository, works: every component, protocol detail, data structure, configuration default and operational procedure. It was written so that a team can build an equivalent Instagram scraper by reusing what is generic and rewriting only what is Facebook-specific.

- **Source of truth:** the code at commit `265ae3a` (main, October 2026). Every chapter was written by reading the code, tests included. `path:line` references point at that commit and will drift as the code changes.
- **Facts vs suggestions:** statements about fbs come from the code. Anything about Instagram is a hypothesis or suggestion; the chapters mark these `[SUGGESTION]`, `[IG-VERIFY]` or "suggestion". Nothing in this repository talks to Instagram. Verify every Instagram detail with a live capture (Phase 0) before building on it.
- **Observations about defects** are marked `[OBS]` or listed under "gaps". The most important ones were re-checked against the code; see [Known issues](#known-issues-in-fbs-do-not-port-them).

## Chapters

| # | Chapter | What it covers |
|---|---|---|
| 1 | [Client layer](01-client-layer.md) | TLS/HTTP2 fingerprinting, proxies and exit buckets, identities and the pool, HTML bootstrap and tokens, the GraphQL request format, response classification, GCRA/AIMD/budget rate control, circuit breakers |
| 2 | [Query registry, doc_ids, browser](02-query-registry-docids-browser.md) | The hot-reloaded doc_id registry, every seed query, `fbs-registry-sync` (bundle scanning, canary probes, promotion), the headless-Chromium fallback over raw CDP |
| 3 | [Parsing and data model](03-parsing-data-model-fakefb.md) | `__typename` tree-walking parsers, drift reports, id decoding, every proto field and where it comes from, golden fixtures, the PII scrubber, `fakefb`, the Kafka contract |
| 4 | [Crawl pipeline](04-crawl-pipeline.md) | The task envelope, topics and lanes, page/post/thread stages, the worker, checkpoints and leases, job accounting in Lua, incremental re-scrapes, edit and deletion detection, emitter, raw sink, shadow parsers |
| 5 | [Backend services](05-backend-services.md) | REST API and auth, Postgres schema and partitioning, the persister (COPY + no-op upsert), scheduler loops and adaptive tiers, the media lifecycle, GDPR erasure, config system, `fbctl`, platform libraries |
| 6 | [Phase 0, deploy, CI, tests](06-phase0-deploy-ci-tests.md) | The Phase 0 measurement kit (`fbs-spike`), go/no-go and cost formulas, Docker/Compose/Helm, alerts and dashboards, CI workflows, the test pyramid, load and soak results, dependencies |
| 7 | [Instagram build plan](07-instagram-build-plan.md) | What to reuse, what to rewrite, the proposed Instagram data model and stage graph, `fakeig`, the Instagram Phase 0 plan, build order, open questions |

To start the Instagram project, use the [kickoff brief](../instagram/kickoff-brief.md): what the owner must decide and provide, the brief for a planning session, and the prompt that starts it.

Existing project docs complement this reference: the [README](../../README.md), ADRs [0001](../adr/0001-logged-out-graphql-scraping.md), [0002](../adr/0002-kafka-postgres-valkey.md) and [0003](../adr/0003-temporary-media.md), the contracts ([Kafka schemas](../contracts/kafka-schemas.md), [media handoff](../contracts/media-handoff.md)) and the runbooks ([operations](../runbooks/operations.md), [Phase 0](../runbooks/phase0-spike.md), [go-live](../runbooks/go-live.md), [load testing](../runbooks/load-testing.md), [cost tuning](../runbooks/cost-tuning.md)).

---

## fbs in one page

**What it does.** It scrapes public Facebook pages, **logged out only** (no accounts of any kind), and collects:
- the page profile, with a follower history;
- every post in a configurable age window (default 365 days, capped at 1095);
- every comment and reply Facebook serves logged out, nested to `max_depth` (3), with edits counted and deletions detected on full re-reads;
- all media with their metadata. Media files are downloaded temporarily, handed to an analysis service, and deleted on acknowledgement.

Coverage is measured, not assumed: every post and job reports `completeness` (comments reported by Facebook vs collected).

**Scale target** (README sizing; estimates until Phase 0 measures): 300k pages/day incremental plus 2k pages/day backfilled to 365 days. That is about 16M requests/day (about 190 rps on average, 500 at peak), 10–24 crawler pods, about 570 GB/day of proxy traffic, about 1.7 TB/day of media over direct egress, and about 100M Postgres row writes/day.

**Status:** the whole pipeline is verified end to end against `fakefb`, a synthetic Facebook, with real Postgres 18, Redpanda and Valkey. It is **not yet verified against real Facebook**: the seed doc_ids are placeholders and the response shapes are modelled on public observations. Phase 0 is the step that closes that gap.

### How Facebook is scraped (ADR 0001)

The crawler behaves like the logged-out Facebook web client:
1. It loads the page HTML with a Chrome-identical TLS/HTTP2 fingerprint (`bogdanfinn/tls-client`, profile `chrome_146`). It extracts the tokens the web client would use: `lsd`, `__spin_r/b/t`, `hsi`, `server_revision`, `haste_session`, `_js_datr`, `__comet_req`, and `fb_dtsg` when present. It accepts the EU consent wall when one is shown.
2. It calls `POST /api/graphql/` with a persisted-query `doc_id`, a `variables` JSON and a fixed 24-field form, plus `x-fb-lsd`, `x-fb-friendly-name` and `x-asbd-id` headers. Responses are streamed multi-part JSON (`@stream`/`@defer`), sometimes labelled `text/html`.
3. Doc_ids, friendly names and variable templates are **data**: they live in a Postgres registry that crawlers reload every 30s. `fbs-registry-sync` follows Facebook's rotations by scanning JS bundles and validating candidates with real requests on canary pages.
4. Parsers find entities by `__typename` anywhere in the JSON tree, read each field from a list of candidate paths, and report missing required fields as **schema drift** instead of silently dropping data.
5. A headless Chromium (driven over raw CDP) is only a **fallback**: for page HTML that carries no tokens (capped at 2% of loads and 2 browsers per pod), and for doc_id discovery. It is never used per page.

### Architecture

```
clients ─▶ fbs-api ─▶ jobs (Postgres) + root PageTask ─▶ fbs.task.page.{p0,p1,p2}
fbs-scheduler (leader): planner, finalizer, reaper, partitions, retention, erasure, AIMD, budget, lag gauges
fbs-crawler --stages page ─PostTask─▶ post ─ThreadTask─▶ thread   (one topic per stage and lane)
   │  identity = proxy exit + fingerprint + cookies + tokens; GCRA per identity/page/global; breakers
   ├─▶ fb.page.v1 / fb.post.v1 / fb.post_metrics.v1 / fb.comment.v1 / fb.media.v1 ─▶ fbs-persister ─▶ Postgres
   │                                                                              └─▶ analysis service
   └─▶ fbs.task.media ─▶ fbs-media-downloader ─▶ S3 ─▶ fb.media.ready.v1 ─▶ analysis service
                          fb.media.processed.v1 ─▶ fbs-media-janitor ─▶ delete ─▶ fb.media.deleted.v1
fbs-registry-sync: canary pages ─▶ bundle scan / render ─▶ candidate doc_ids ─▶ probe ─▶ promote
```

| Binary | Role | Chapter |
|---|---|---|
| `fbs-api` | REST `/v1`: jobs, page registration, data reads, erasure, registry admin. API keys with scopes; per-client GCRA rate limit | 5 |
| `fbs-scheduler` | Leader-elected through a Postgres advisory lock. Runs the planner, adaptive tiers and post refresh, the finalizer (completeness, `fb.job.v1`), reaper, partitions, retention, erasure, AIMD ceiling, budget, lag gauges | 5, 1 |
| `fbs-crawler --stages page,post,thread,retry` | Fetch, parse, emit. One Deployment per stage; one consumer per (stage, lane) plus the retry relay | 4, 1, 3 |
| `fbs-persister` | Kafka `fb.*` → partitioned Postgres via batched COPY + no-op upserts | 5 |
| `fbs-media-downloader` / `fbs-media-janitor` | Temporary media: download over direct egress, announce, delete on ack or TTL | 5 |
| `fbs-registry-sync` | Follows doc_id rotations: bundle scan, render fallback, canary probes, promotion | 2 |
| `fbctl` | Migrations, topics, schemas, API keys, registry, DLQ replay, raw replay against current parsers, fixture scrub, page import, dev media acks | 5 |
| `fbs-spike` | Phase 0 measurement kit against real Facebook, needing no infrastructure | 6 |
| `fakefb`, `devs3` | Synthetic Facebook with ground truth and fault injection; in-memory S3 | 3, 6 |

### Storage roles (ADR 0002)

| Store | Holds | Why |
|---|---|---|
| **Kafka** (Redpanda in dev) | Work queues `fbs.task.{page,post,thread}.{p0,p1,p2}`, `fbs.task.media`, `fbs.task.retry.{30s,5m,1h}`, `fbs.task.dlq`; public contract `fb.*.v1` (protobuf with an `fbs-type` header, schemas in a Schema Registry under BACKWARD_TRANSITIVE) | A topic at every 1-to-N fan-out; replay; decoupled consumers. Delete retention only (never compaction), so erased PII ages out |
| **Postgres 18** | The queryable view (pages, posts, comments, authors, metrics, media), jobs, schedules, registry, erasure and suppression. Posts and comments are partitioned monthly by post creation time | Retention = `DROP PARTITION`; every PK starts with the post (Citus-ready). PG18 is required for `RETURNING old/new` |
| **Valkey** | GCRA limits, AIMD windows, budget, stream leases, checkpoints, `done:` markers, job accounting (`pending = |spawned| − |done|` in Lua), comment "seen" lists | Sub-millisecond shared state. Losing it costs in-flight work, not data |
| **S3** | Temporary media (`media/`), raw response captures (`raw-failures/`, `raw/`) | Bounded by throughput × TTL |

### A crawl, end to end

1. **Dispatch.** `POST /v1/jobs` (lane p0), the scheduler planner (p1) or a page registration's backfill (p2) inserts a `jobs` row. It registers the root task ids in Valkey **before** producing the root `PageTask`. `mode=auto` becomes `incremental` when the page has a timeline watermark.
2. **Page stage.** A worker takes a lane slot, skips the task if a `done:` marker exists, and acquires a lease on the page's stream. It leases an identity, bootstraps it from the page HTML if needed, and emits `fb.page.v1`. It then paginates the timeline (`ProfileCometTimelineFeedRefetchQuery`, 10 posts per page), emitting `fb.post.v1`, `fb.post_metrics.v1` and `fb.media.v1`, and queues media downloads per `video_policy`. For each post needing comments it spawns a `PostTask`. It stops at the age window (3 consecutive old non-pinned posts), at `max_posts`, at the end of the feed, or, for incremental runs, at the watermark minus 24h. After 20 pages or 60s it yields a continuation (same lineage, `seq+1`).
3. **Post stage.** It paginates comments with the "All comments" ordering (`RANKED_UNFILTERED_CHRONOLOGICAL_REPLIES_INTENT_V1`), or newest-first with an early stop for incremental runs. It emits `fb.comment.v1` (keyed by post), records the top-level ids seen for deletion detection, and spawns a `ThreadTask` for each reply thread not fully inlined.
4. **Thread stage.** It paginates replies (`Depth1CommentsListPaginationQuery` with an expansion token), nesting to `max_depth`.
5. **Outcomes.** Every response is classified (`ok`, `rate_limited`, `login_wall`, `checkpoint`, `soft_error`, `registry_stale`, `schema_drift`, `content_unavailable`, `transport`). The class feeds identity health, the exit-bucket breaker, the fleet AIMD and the budget. Failures park the task on a retry topic: 30s for blocks, 5m for stale registry or drift. Retrying escalates to the next proxy tier. A task is dead-lettered after 5 retries without checkpoint progress, or 50 in total.
6. **Accounting.** Children are registered before the parent is marked done, so pending can never hit 0 early. When pending reaches 0 the job enters `jobs:finalize`.
7. **Persist.** The persister COPYs batches into temp tables and upserts with `WHERE content_hash IS DISTINCT FROM …`, so unchanged content is a no-op. It derives change-only `post_metrics` rows and comment edit counts from `RETURNING old/new`.
8. **Finalize.** The leader snapshots end offsets once a job's last task finishes, and marks the job `completed`/`partial` only after the persister's committed offsets pass that snapshot (**"completed" means "queryable"**). It computes completeness, runs deletion detection for complete full reads, and publishes `fb.job.v1`.
9. **Media.** The downloader stores files in S3 and publishes `fb.media.ready.v1`. The analysis service acks on `fb.media.processed.v1`, then the janitor deletes the file and publishes `fb.media.deleted.v1`. A 72h TTL sweep, a bucket lifecycle rule and orphan reconciliation are the safety nets.

### Design decisions worth keeping for Instagram

- **Logged out, measured coverage.** Report truncation (`completeness`, `comments_state`, `timelines_truncated`) instead of hiding it.
- **Protocol facts are data.** doc_ids, variables and client params live in a hot-reloaded registry with canary-validated promotion and fallbacks.
- **Shape-tolerant parsing with explicit drift.** A typename walker, candidate paths, required/optional field reports, a per-(query, doc_id) breaker, shadow parsers and raw-failure capture with replay.
- **One outcome funnel, three feedback loops.** Identity health → exit-bucket breaker → fleet AIMD, with a Redis GCRA under all of them. Adding pods never adds load.
- **Short, idempotent tasks.** Pagination stays inside a task with yielded continuations. Idempotency comes from deterministic task ids, `done:` markers, leases, checkpoints, content-hash event ids and UUIDv5 media ids. The consumer commits only contiguous offsets.
- **Priority lanes.** On-demand > scheduled > backfill share slots; backfill is capped at 40% and each lane keeps a minimum.
- **"Completed means queryable"** via offset snapshots.
- **Change-only writes.** COPY + no-op upsert, and metrics time series written only on change.
- **Adaptive scheduling and budgets.** Tiers by posting rate, comment refresh by post age with backoff, recrawls at 7 and 30 days, and a daily request/byte budget that pauses backfill first and never holds on-demand work.
- **Temporary media** with an ack contract.
- **Privacy by construction.** HMAC author keys, `pseudonymized` mode, PII behind the `pii:read` scope, erasure with a suppression set that crawlers apply before publishing, retention by partition drop.
- **A fake platform as the test foundation.** `fakefb` with ground truth and faults underpins unit, e2e, chaos, load and soak tests and the Phase 0 dry run.
- **Phase 0 before scale.** A one-command, resumable measurement with a go/no-go verdict per proxy tier and a cost projection.

### Technology

Go 1.27 · `bogdanfinn/tls-client` (fingerprinted TLS/HTTP2) · `twmb/franz-go` (Kafka) · `jackc/pgx/v5` + `goose` (Postgres 18) · `redis/go-redis/v9` + `redis_rate/v10` (Valkey, GCRA) · `tidwall/gjson` (JSON walking) · protobuf + buf · `minio-go` (S3) · `klauspost/compress` (zstd) · `zeebo/xxh3` (content hashes) · `go-chi` (API) · `knadh/koanf` (config: embedded `default.yaml` → `--config` files → `FBS_<SECTION>__<KEY>` env) · Prometheus · a hand-written CDP client for Chromium (no chromedp or rod). Deployed with Docker Compose or Helm (KEDA lag autoscaling, PDBs, hook Jobs, PrometheusRule, Grafana dashboard). Full list with versions: [chapter 6 §5](06-phase0-deploy-ci-tests.md#5-key-third-party-dependencies-gomod).

### Numbers that shape behaviour (defaults)

| Area | Default |
|---|---|
| Pacing | 0.33 rps per identity (burst 2); 3 rps per page fleet-wide (6 for p0); 50 rps global start; identity retired after 150–400 requests |
| AIMD | every 10s; ×0.7 above 6% blocks; +2% below 2% blocks while utilisation ≥ 80%; 5–500 rps; at least 100 samples |
| Breakers | exit bucket: opens at 15% over 1m (≥ 20 events), cooldown 2m doubling to 30m, 3 probe successes. Query: opens at 50% over 5m |
| Crawler | 240 slots per pod and stage; p2 ≤ 96; lane minimum 12; yield after 20 pages or 60s; timeline page size 10; stop after 3 consecutive old posts; lease 120s |
| Retries | 30s / 5m retry topics; DLQ after 5 stalled retries or 50 in total |
| Limits | 365 days (cap 1095); `max_depth` 3; video `sd`, backfills `thumbnail` |
| Scheduling | tiers T0 ≥ 10 posts/day → 3h, T1 ≥ 1 → 12h, T2 weekly+ → 24h, T3 → 72h, T4 (dormant 90 days) → 7d; incremental overlap 24h; comment refresh 24h/72h/7d by post age, at most 50 per job; recrawls at 7 and 30 days |
| Media | 72h TTL; 500 MiB per object; 50 concurrent downloads |
| Registry sync | every 30m; 2 fallbacks kept; retire a candidate after 3 failed checks |
| Browser fallback | off; at most 2% of HTML loads; 2 browsers per pod; 45s timeout |

The full config reference is in chapters 1 (`fb:`), 2 (`registry_sync`, `fb.browser`), 4 (`crawler`, `limits`, `kafka`) and 5 (everything else).

### Observability

Every binary serves `/metrics`, `/healthz` and `/readyz` on `:9090`; metric names start with `fbs_`. The main signals are request outcomes by class and tier, fleet block rate, the AIMD ceiling, breaker states, lane slots, schedule lag, budget, parser drift (required and optional fields, shadow diffs), registry health, comment completeness, retries and DLQ, identity retirements by reason, proxy bytes, media outcomes and consumer lag. The chart ships 25 alerts and 14 recording rules, each linked to a runbook section, plus a 43-panel Grafana dashboard. Leader-only gauges are read through `fbs:leader:*` recording rules. A test checks that every alert and dashboard references only metrics and runbook anchors that exist. Details: chapters 1, 4, 5 and 6.

---

## Known issues in fbs (do not port them)

The deep-dives found defects and gaps worth fixing in fbs and avoiding in the Instagram build. **Checked** means it was re-verified against the code while assembling this reference; the others are as reported in the chapter and should be confirmed before acting on them.

| Issue | Where | Status |
|---|---|---|
| `fbctl dlq replay` is a no-op for 7 days: dead-lettering marks the task `done:` (7-day TTL), and the replay re-produces the same task id, which the worker then skips. The runbook's "ship the fix, then replay the DLQ" step does nothing until the marker expires | `internal/crawl/stage/worker.go:64-69,152,178-197`, `cmd/fbctl/commands.go:187-238` | Checked |
| One undecodable or unwritable record stalls the persister indefinitely: a batch that fails is retried forever with no quarantine or DLQ, and job finalisation then waits for its 10-minute timeout | `internal/persist/consumer.go:83-102,122-140` | Checked |
| `GET` and `cancel` on `/v1/jobs/{id}` (and `/results`) do not check that the caller owns the job; only the list endpoint filters by owner | `internal/api/jobs.go:136-147,180-200` | Checked |
| Hashtag ranges are sliced in runes; Facebook's text-range offsets are almost certainly UTF-16 code units (JavaScript string offsets), so an emoji before a hashtag yields a shifted, wrong extra tag | `internal/fb/parse/fields.go:200-220` | Checked (the rune slicing); the UTF-16 behaviour is an assumption about Facebook |
| Orphaned media deletions publish no `fb.media.deleted.v1` event, although the contract lists the `ORPHAN` reason | `internal/media/janitor.go:190-209` | Checked |
| doc_id discovery matches only `__d("<Name>_facebookRelayOperation"…)` modules and `"<Name>Query",{queryID:…}` pairs, not Relay `params:{id,…,name}` declarations; render capture reads only POST bodies | `internal/fb/docids/docids.go:19-21`, `internal/fb/browser` | Checked (regexes) |
| `fb.bootstrap_attempts` is declared but never read | `internal/config/config.go:150` | Checked |
| Suppression is enforced by the crawlers, not the persister: records already in Kafka when an erasure runs can re-insert erased data | [Chapter 5 §6.3](05-backend-services.md) | Reported |
| `/jobs/{id}/results` omits posts that were unchanged during the job (it relies on `last_seen_at`, throttled daily) | Chapter 5 §1.6 | Reported |
| `max_comments_per_post` is enforced per task segment, not per post; reply-thread failures do not stop later incremental runs from treating a post as complete | Chapter 4 §16 | Reported |
| With default retention (`comments_days` 365 < `max_age_cap_days` 1095), the maintenance loop creates and drops about 24 old `comments` partitions every hour | Chapter 5 §2.3 | Reported |
| Detection rules are hard-coded although documented as configurable; HTTP 400/401/403 are not classified; the rate-limit cooldown is effectively a constant 60s; every identity in a pod uses the same fingerprint profile | Chapter 1 §15 | Reported |
| Unused knobs and fields: `crawler.comments_page_size`, `freshness_p0/p1`, `kafka.compression`, the `retry.1h` topic, `Task.not_before/affinity/trace`, `CRAWL_MODE_REFRESH`, several proto fields that are never filled | Chapters 3 §12, 4 §16, 5 §13 | Reported |
| Phase 0 report: latency is recorded but never aggregated; the depth check passes timelines cut by the page cap | Chapter 6 §0 | Reported |

Chapter 7 lists these again as the "fix before porting" list.
