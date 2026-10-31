# Operations runbook

Every binary exposes `/metrics`, `/healthz` and `/readyz` on `:9090`.
All metric names start with `fbs_`.

## Key signals

| Signal | Metric | Healthy |
|---|---|---|
| Request outcomes | `igs_ig_requests_total{query,class,tier}` | `ok` ≥ 95% |
| Block rate | classes `rate_limited`, `login_wall`, `checkpoint`; fleet-wide `igs_fleet_block_rate` | < 5% |
| Fleet ceiling | `igs_aimd_ceiling_rps`, `igs_fleet_rps`, `igs_aimd_adjustments_total{direction}` | ceiling stable or rising; `fleet_rps` close to it when there is backlog |
| Breakers | `igs_breaker_state{name="proxy:..."}`, `{name="query:..."}` (2 = open), `igs_ig_query_short_circuit_total` | all 0 |
| Lanes | `igs_lane_slots{stage,lane,state}` | `waiting` on p0 ≈ 0 |
| Schedule | `igs_scheduled_pages{tier}`, `igs_schedule_lag_seconds{tier}`, `igs_post_refreshes_total{kind}` | lag < tier interval |
| Budget | `igs_budget_used{kind,lane}`, `igs_budget_projected{kind}`, `igs_budget_limit{kind}`, `igs_budget_backfill_paused`, `igs_budget_stretch` | projected < limit |
| Parser health | `igs_parse_total{outcome="drift"}`, `igs:parse_missing_per_response:rate1h` (vs 7d ago), `igs_parse_shadow_total` | drift ≈ 0; optional fields as last week |
| Registry | `igs_registry_query_healthy{query}`, `igs_registry_sync_last_success_timestamp_seconds`, `igs_registry_promotions_total{query}`, `igs_registry_checks_total{query,status,verdict}` | all 1; last success < 1h ago |
| Coverage | `igs_comment_completeness_ratio` | median at the Phase 0 baseline |
| Truncation | `igs_timeline_truncated_total` | flat |
| Retries / DLQ | `igs_retries_total`, `igs_dlq_total{reason}` | low, flat |
| Identity churn | `igs_identity_retired_total{reason}` | mostly `max_requests`/`sticky_expiry` |
| Proxy bandwidth | `igs_proxy_bytes_total{tier}` | within budget |
| Jobs | `igs_jobs_total{status}` | `completed` dominates |
| Schedule lag | `igs_schedule_lag_seconds` | < planner interval |
| Media | `igs_media_download_total{outcome}`, `igs_media_deleted_total{reason}` | `ttl_expired` ≈ 0 |
| Persister | `igs_persist_batch_seconds`, `igs_kafka_consumer_lag{group="igs-persister"}` | lag < 1 min |
| Consumer lag | `igs_kafka_consumer_lag{group,topic}` (leader scheduler) | p0 groups ≈ 0 |

## Alerts and dashboards

The chart ships the alerting and recording rules in `deploy/helm/fbs/files/alerts.yaml`; set `alerts.enabled` to install them as a PrometheusRule. The Grafana dashboard **fbs / Overview** is in `deploy/helm/fbs/files/dashboards/`; set `dashboards.enabled` to install it for the Grafana sidecar. Every alert links to the section of this runbook that handles it. The thresholds are starting points: tune them once Phase 0 has a baseline.

Only the leader scheduler publishes AIMD, budget, schedule-lag and consumer-lag values. Every fbs binary exports those gauges, though, at 0, and a former leader keeps its last values. So the rules read them through `igs:leader:*` recording rules, which join on `igs_scheduler_leader == 1`. Do the same in ad-hoc queries.

`igs_kafka_consumer_lag{group,topic}` is published by the leader scheduler every 30s, so lag alerts need no separate Kafka exporter.

## Block storm (block rate > 10%)

The fleet reacts on its own, at three levels:

- **Identity:** a rate-limited identity cools down; one that keeps being blocked, walled or checkpointed is retired, and so is one whose exit stops answering (3 transport failures in a row: `reason="transport"`).
- **Exit bucket:** a static endpoint's /24, or a gateway and country, opens its breaker at `fb.proxy.breaker.open_rate` (15%) over `window` (1m). It gets no new identities, its idle identities are retired, and tasks move to the next tier in `fb.proxy.routing`. After `cooldown` (2m, doubling up to `max_cooldown`) one probe identity tests it.
- **Fleet:** AIMD, on the leader scheduler, reads the block rate of the whole fleet every 10s. Above `fb.aimd.high_block_rate` (6%) the ceiling becomes 70% of the rate actually sent. Below `low_block_rate` (2%), while the fleet uses most of the ceiling, the ceiling grows by 2% (at least 1 rps) per window. Recovery from the floor is deliberately slow.

What to check:

1. `igs_fleet_block_rate` and `igs_aimd_ceiling_rps`. The ceiling should be falling while blocks are high.
2. `igs_breaker_state{name=~"proxy:.*"}` and `igs_ig_requests_total` by tier. If one provider or tier is affected, change `fb.proxy.routing` to prefer another.
3. `igs_identity_retired_total` by reason.
4. If login walls appear at the same depth everywhere, it is coverage, not blocking: jobs report `timelines_truncated` / `posts_truncated`.

To pin the ceiling by hand, for example during an incident, set the key the crawlers read (they pick it up within 5s):

```bash
redis-cli SET "${KAFKA_TOPIC_PREFIX}aimd:global_rps" 20
```

AIMD keeps adjusting from that value. To stop it, set `fb.aimd.enabled: false`. Crawlers then use the fixed `fb.global_rps`.

Adding pods never adds load: every pod paces against the same fleet ceiling.

## Registry stale (doc_id rotated)

Symptom: `class="registry_stale"` rising, tasks retrying with delay 5m.

The `query:timeline|<doc_id>` breaker (`fb.query_breaker`) may open. While it is open, requests for that query are not sent (`igs_ig_query_short_circuit_total`), the fallback doc_id is tried if there is one, and tasks wait in the 5m retry topic.

Usually `fbs-registry-sync` heals this on its own within `registry_sync.interval` (30m). Each run:

1. **Loads** the canary pages (`registry_sync.canary_pages`) and scans them and their JS bundles for Relay operation ids, the same scan as `fbs-spike docids`. Bundles are content-addressed and fetched once each.
2. **Records** each doc_id it finds for a query's friendly name, if the query does not know it yet, as a `candidate` with the same variables and parser.
3. **Checks** the active entry and the candidates with a real request on a canary page: the timeline first, then comments on a busy post it returned (or on a stored post when the timeline fails), then a reply thread. Verdicts:
   - `ok`: the response parsed and had data;
   - `stale`: Facebook rejects the doc_id;
   - `drift`: Facebook accepts it, but the parser misses required fields;
   - `inconclusive`: blocked, no data, or a transport error; it says nothing about the entry.
4. **Promotes** a candidate that is `ok` and declared by the current bundles. The old active entry becomes a `fallback`, which the client tries on a stale doc_id. When the active entry fails and no current candidate passes, a passing older candidate or fallback is promoted. Only `registry_sync.keep_fallbacks` (2) fallbacks are kept. A candidate or fallback that fails `retire_after` (3) checks in a row is retired.

Crawlers reload the registry within `fb.registry_refresh` (30s), and parked tasks come back from the 5m retry topic. The query breaker is keyed by doc_id, so the new one starts closed.

When it does not heal:

- `igs_registry_query_healthy{query} == 0`: the active doc_id is stale or drifted and nothing passed.
  - If the run report lists the query's friendly name under `not_found`, Facebook renamed the operation. Capture a HAR (phase0 runbook), then set the new entry with `PUT /v1/admin/queries/{name}`.
  - If the checks say `drift`, it is a parser change: see Schema drift below.
- `igs_registry_sync_last_success_timestamp_seconds` is old: no recent run gave every query a verdict. Typically no canary page loads (blocked tier, or a page gone), every probe is blocked, or the canary pages have no posts with comments or reply threads. See `GET /v1/admin/registry-sync/runs` (`unchecked`) and the service logs. Replace dead or quiet canary pages.
- To check without promoting, set `registry_sync.promote: false`. The runs then list what they would promote under `proposed`.
- `fbs-registry-sync --once` runs once and prints the report. It exits 3 when the run is degraded.

The manual path is still there:

1. `fbs-spike docids --page <public page>` (see phase0 runbook).
2. `PUT /v1/admin/queries/{name}` with the new doc_id (and variables if they
   changed). Crawlers reload within 30s; parked tasks come back from the 5m
   retry topic. Tasks that exhausted retries are in the DLQ:
   `fbctl dlq replay --reason max_attempts`. That reason is
   `crawler.max_attempts` retries in a row without the stream moving forward.
   `max_total_attempts` is the cap on all retries of one task.

`GET /v1/admin/queries` shows each entry's `last_check`, `last_checked_at` and `last_ok_at`. The sync's own requests are counted under `query="registry_sync:<name>"` in `igs_ig_requests_total`, apart from crawl traffic.

## Browser fallback

`fb.browser.enabled` turns on the headless-Chromium fallback (internal/fb/browser). It needs the image built with `docker build --target browser`; set `image.browserTag` in the chart so the crawlers and registry-sync use it.

- **Bootstraps:** when a page's HTML carries no tokens (it needs JavaScript first), the identity is bootstrapped in a fresh Chromium.
  - The browser presents the identity: its own exit (proxy credentials are answered on the challenge; SOCKS exits with credentials are not supported by Chromium), its user agent and matching client hints, and its cookies.
  - Images, media and fonts are refused, and Chromium's own background traffic is off, to save proxy bandwidth. The render's wire bytes count in `igs_proxy_bytes_total`.
  - The rendered page is classified like an HTTP load: a login or checkpoint wall, or a block, is recorded on the identity, not taken for a bootstrap.
  - On success, the cookies and tokens are copied into the identity and the plain HTTP client continues as usual.
- **Limits:** at most `max_share` (2%) of HTML loads and `max_concurrent` (2) browsers per pod. A capped bootstrap fails at once (`igs_bootstrap_total{method="browser",outcome="capped"|"busy"}`) and the task retries. It never holds a crawl slot waiting for a browser.
- **Doc_id discovery:** when the bundles declare no doc_id for a registry query, fbs-registry-sync renders a canary page and reads the doc_id from the GraphQL requests the page makes itself. These show under `rendered` in the run report.
- **Sandbox and processes:** pods drop every capability, which leaves Chromium's sandbox unusable, so the chart sets `fb.browser.no_sandbox: true`. Each render runs in its own process group and is killed as a whole. Pods using the browser image share their process namespace, so the pause process reaps what is left. With `docker run`, the image's `tini` does that.

Signals:

- `igs_bootstrap_total{method="browser"}` by outcome;
- `IgsBrowserFallbackSaturated`: more pages need JavaScript than the cap allows. Raise `max_share` and `max_concurrent`, and give the crawler pods the CPU and memory for that many browsers. A render of a fakefb page took about a second in development; size real pages under load first;
- `IgsBrowserFallbackFailing`: Chromium fails to render, or the rendered pages carry no tokens either.

## Schema drift

Symptom: `igs_parse_total{outcome="drift"}` (alert `IgsSchemaDrift`), or a field going missing more often than a week ago (`IgsOptionalFieldDrift`). When half the responses of a query fail to parse (`fb.query_breaker`), its breaker opens and the query stops being sent until a probe parses again. This saves the requests that would be wasted until the fix ships.

**Required fields** (a post or comment cannot be built):

1. The drifted responses are in the raw bucket under `raw-failures/<query>/` (`fb.capture_raw: failures`). They are captured with the request's variables (page, feedback id), so they can be parsed again with their context.
2. See what fails, and turn some of them into scrubbed fixtures:
   `fbctl replay --query timeline --since 24h --fixtures-dir testdata/fb --name drift`
3. Fix the candidate paths in `internal/fb/parse`, `make golden`, and check the fix against the production responses before shipping: `fbctl replay --query timeline --since 24h`. It exits 3 while some still drift.
4. Ship, then replay the DLQ: `fbctl dlq replay --reason max_attempts`.

**Optional fields** (text, reactions, media and the like) never fail a parse, so their loss is silent. `igs:parse_missing_per_response:rate1h` is how often each field is missing per response. `IgsOptionalFieldDrift` fires when that is more than twice last week's rate plus 5 points, or above 25% with no history. The "Optional fields missing per response" panel shows both curves. To look at the responses, set `fb.capture_raw: all` for a while and run `fbctl replay --prefix raw --query <q>`.

**Proving a new parser version on live traffic:** register it as a candidate in `internal/crawl/shadow` (`shadow.Candidates.Timeline` or `.Comments`, from its file's `init`). A share of responses (`fb.shadow_rate`, 5%) is then parsed by both versions; only the active result is emitted.
- `igs_parse_shadow_total{query,outcome}` counts `same`, `differs` and `drift`.
- Responses the two parse differently are captured under `raw-failures/shadow-<query>/`, apart from real drift. Replay them with `fbctl replay --query shadow-<query>`. A candidate that panics only counts as `error`.
- `IgsShadowParserDiffers` (info) fires above 1%.
- Promote the candidate once it stays `same`.

## Kafka lag

* Crawler lag: KEDA scales stage deployments. If they are at max replicas and lag still grows, the fleet ceiling (not pods) is the bottleneck.
  - Compare `igs_fleet_rps` with `igs_aimd_ceiling_rps`.
  - AIMD raises the ceiling only while blocks stay low, up to `fb.aimd.max_rps`. Raise that cap only if the block rate allows.
* Lanes share each crawler's `crawler.slots` per stage in priority order: on-demand (p0), scheduled (p1), backfill (p2).
  - Backfill is capped by `crawler.lanes.p2` (40%). Every lane keeps `crawler.lane_min` slots when it has work.
  - `igs_lane_slots{state="waiting",lane="p0"}` above 0 for long means on-demand work is queuing behind tasks that are still finishing. Tasks yield after `crawler.yield_after`, so it clears within a minute; if it persists, add pods.
* Persister lag delays job completion (the finalizer waits up to
  `scheduler.finalize_timeout`). Scale the persister; check Postgres I/O.

## Media backlog / TTL expiries

* `igs_media_deleted_total{reason="ttl_expired"}` rising: the analysis service
  is not acking in time. Fix the consumer or raise `media.ttl`.
* `igs_media_download_total{outcome="expired"}` rising: downloads lag behind
  discovery; scale `media-downloader`.

## Erasure requests

`POST /v1/erasure-requests` (admin) queues the request; the scheduler leader
processes it within ~10s: suppression entry, comment redaction (rows stay as
tombstones so coverage numbers stay correct), author row deletion, media
deletion and an `fb.erasure.v1` event downstream must honour. Crawlers refresh
the suppression set every `crawler.suppression_refresh` (60s) and redact
before publishing, so re-scrapes never reintroduce the data. Kafka topics and
raw payloads age out by retention (7 days).

## Stuck jobs

The reaper marks jobs `partial` after `scheduler.reaper_after` (30m) without
activity, or `failed` when their accounting state is gone. Inspect the DLQ
and `GET /v1/jobs/{id}` counters.

## Schedule and tiers

**Registering pages in bulk:** `fbctl pages import --api URL --key KEY FILE`, with a file of page URLs, vanity names or ids (one per line, or CSV with a `page` column).
- It registers 5 pages a second by default (`--rps`) and retries rate limits and server errors.
- Each new page gets its backfill in the low-priority lane.
- Running it again is safe: pages already registered keep their schedule and options and are not backfilled again. A page that has never had a job (its backfill failed to start) is backfilled then.
- It prints `{"created", "existing", "failed"}`; the failed pages and their errors are listed even when it is interrupted.

Pages registered without `schedule.interval_hours` are scheduled adaptively. Every tier crawls for new posts **every 3 hours**: a run that finds nothing new costs a request or two. The tier still decides which posts' comments are refreshed (`scheduler.post_refresh` windows can be limited to some tiers) and which pages a daily budget slows down first.

- **When the tier is set:** after every job, the page's posting rate over `scheduler.rate_window` (14 days) picks its tier. If the stored history is shorter, the rate uses that span, with a 1-day floor.

| Tier | Posts per day | Interval |
|---|---|---|
| T0 | ≥ 10 | 3h |
| T1 | ≥ 1 | 3h |
| T2 | weekly or more | 3h |
| T3 | less than weekly | 3h |
| T4 | no post for 90 days | 3h |

- **Longer intervals for quiet pages,** if cost ever matters more than freshness: raise the T1–T4 `interval`s (and `dormant_interval`) in the config. A page's next run moves after its next job.

- **What a tier change does:** a faster tier pulls the next run forward; a slower one takes effect after the run already planned.
- **Failing pages** still back off exponentially, up to 30 days.
- **Turning it off:** a page registered or patched with `interval_hours` keeps that interval (`adaptive: false`). `PATCH /v1/pages/{page}` with `{"adaptive": true}` hands it back to the tiers.
- **Inspect:** `GET /v1/pages/{page}` shows `tier`, `posts_per_day`, `last_post_at` and `adaptive`.

## Comment refresh of older posts

The timeline scan re-reads posts younger than `scheduler.incremental_overlap` (24h). Older posts are registered in `post_refresh` after each job and ride on the page's next scheduled job as extra post tasks, at most `post_refresh.max_per_job` (50) per job.

**What a refresh reads:** comments newest-first, stopping at the first one already stored, so an unchanged post costs one request.

**How often:**

| Post age | Interval |
|---|---|
| up to 7 days | 24h |
| 7–14 days | 72h |
| 14–30 days | 7d, T0/T1 pages only |

- **Backoff:** each refresh that finds nothing new doubles the post's interval, up to 2^`max_backoff`.
- **Recrawls:** posts with at least `recrawl_min_comments` (20) comments get a full recrawl at 7 and 30 days, to pick up edits and deletions (next section).
- **Signal:** `igs_post_refreshes_total{kind="refresh"|"recrawl"}`.
- **Cleanup:** rows of posts past every window, and of pages no longer scheduled, are deleted hourly.

## Comment edits and deletions

**Edits:** every scrape publishes each comment again. When a newer observation has other text, the persister counts an edit (`edit_count`, `edited_at`). An observation older than the stored one, such as a replayed batch, changes nothing.

**Deletions** are found by full reads of known posts: recrawls, and `full` jobs on posts crawled before.
1. The read records the top-level comments it sees, with the reply count Facebook gives for each.
2. When it reaches the last page, the job finalizer compares them with the post's stored visible comments from before the read began.
3. Each unseen comment, with its replies, is marked `deleted` (`deleted_at`) in the store, published on `fb.comment.v1` with `status: COMMENT_STATUS_DELETED`, and counted in the job's `comments_deleted`.

"Deleted" means no longer publicly visible: deleted by its author, removed or hidden by the page, or filtered by Facebook. The content is kept. Replies are only marked through their top-level comment.

**Only complete reads count.** Facebook sometimes serves a logged-out visitor fewer comments than it counts. A read that ends early (login wall, `max_comments_per_post`) never marks anything. A read that reached the last page counts as complete when it accounts for the post's comment count from that same read: the top-level comments it returned, plus the reply count Facebook gave for each, must add up to the count, within `scheduler.post_refresh.deletion_count_slack` (a share; 0 = exactly). Any other read may be a partial answer, and its post is skipped (`skipped_partial`). A skipped post is checked again at its next full read. Busy posts that change while being read can miss the exact count; that also skips, and only delays detection.

**A comment seen again** after being marked deleted is `visible` again, with the replies that were deleted with it (same `deleted_at`). That happens, for example, when a page hides a comment and shows it again later.

**Signals:**
- `igs_comment_changes_total{kind="edited"|"deleted"|"restored"}`;
- `igs_deletion_checks_total{outcome="checked"|"skipped_partial"}`;
- the "Comment edits and deletions" and "Deletion checks" panels.

**`IgsDeletionChecksSkipped`:** more than a quarter of the checks were skipped over 6h.
1. Facebook is likely serving partial comment lists, or its counts include comments it does not show. Expect `IgsCommentCompletenessLow` too.
2. Check `igs_ig_requests_total` for login walls on `comments`.
3. Compare the orderings with the Phase 0 harness (`fbs-spike run`).
4. If the lists are complete and only the counts are a little off, raise `deletion_count_slack` (for example to 0.02). A partial answer within the slack can then mark live comments deleted until they are seen again. `detect_deletions: false` turns detection off.

**Cost:** the seen lists live in Redis, 12 bytes per top-level comment, until the job finalizes. A post whose comments were all deleted (Facebook counts none) is not read, so it is not checked.

## Daily budget

`scheduler.budget.requests_per_day` and `bytes_per_day` bound Facebook traffic fleet-wide per UTC day. 0 means unlimited, the default; set them from the Phase 0 cost projection.

**Every minute the leader:**
1. Adds today's use (`igs_budget_used`) to the last hour's rate extrapolated to midnight (`igs_budget_projected`).
2. If that exceeds the budget, backfill (p2) pauses (`igs_budget_backfill_paused` = 1). Crawlers close the p2 lane within 15s; queued backfill waits in Kafka.
3. If on-demand plus scheduled traffic alone would exceed it, the `stretch_tiers` (T4, T3, T2) intervals are multiplied by `stretch` (2) for the runs planned next (`igs_budget_stretch`).
4. On-demand (p0) traffic is never held back.

Backfill resumes, and the stretch lifts, only when the projection is 5% under the budget. Each new day starts from zero.

## On-demand admission

`POST /v1/jobs` answers `429` with `Retry-After` when:
- the client already runs `api.max_running_p0_jobs_per_client` (50) on-demand jobs; or
- the fleet runs `api.max_running_p0_jobs`. That cap is 0 (off) by default; set it to what the crawler fleet can serve within the p0 SLO.

