# tks: deploying and the alerts

How tks is built, installed and watched: the images, the Helm chart (`deploy/helm/tks`), the local Compose stack, and what to do when an alert in the chart's rules fires. The other runbooks cover the rest: [`tks-operations.md`](tks-operations.md) (restarts, identities), [`tks-objectives.md`](tks-objectives.md) (A1, A2, cost and budget), [`tks-canary.md`](tks-canary.md) (operations and the signer), [`tks-import.md`](tks-import.md) (accounts), [`tks-discovery.md`](tks-discovery.md).

**No production traffic to TikTok before C5**: legal signed off (D15, 2026-11-05); staging and the canary of 50 to 100 real accounts come first ([build brief §6](../tiktok/build-brief.md#6-milestones)).

## Images

One Dockerfile (`deploy/docker/Dockerfile`), two images, every binary in both:

| Image | Build | Used by |
|---|---|---|
| default (distroless, static binaries) | `docker build -f deploy/docker/Dockerfile -t tks:dev .` | `tks-api`, `tks-scheduler`, `tks-crawler --stages account` and `retry`, `tks-persister`, `tks-media-downloader`, the hooks (`tksctl`), `fakett`, `devs3` |
| browser (Debian with Chromium) | `docker build -f deploy/docker/Dockerfile --target browser -t tks:dev-browser .` | `tks-crawler --stages post` and `thread` (the signer's pages sign comments and replies: D11) and `tks-canary` |

`make docker` builds both (`IMAGE`, `BROWSER_IMAGE`). The release publishes both tags of one repository; the chart's `image.tag` and `image.browserTag` pick them.

## The Helm chart

`deploy/helm/tks`: one Deployment per component, pre-install and pre-upgrade hooks (`tksctl migrate up`, `tksctl kafka apply`, optionally `tksctl schemas apply`), KEDA scalers on consumer lag, PodDisruptionBudgets, and optionally the PrometheusRule (`alerts.enabled`), the dashboard (`dashboards.enabled`), a PodMonitor, an Ingress and a NetworkPolicy.

| Component | Command | Image | Notes |
|---|---|---|---|
| `api` | `tks-api` | default | Service on port 80 |
| `scheduler` | `tks-scheduler` | default | 2 replicas, one leader |
| `crawler-account` | `tks-crawler --stages account` | default | Post lists, metrics, account refresh (plain HTTP) |
| `crawler-post` | `tks-crawler --stages post` | browser | Comments, signed |
| `crawler-thread` | `tks-crawler --stages thread` | browser | Replies, signed |
| `crawler-retry` | `tks-crawler --stages retry` | default | The retry relays |
| `persister` | `tks-persister` | default | |
| `media-downloader` | `tks-media-downloader` | default | Idle while `tt.media.include` is false (D5); no janitor: another service deletes files |
| `canary` | `tks-canary` | browser | Needs `TKS_TT__CANARY__ACCOUNTS` |

What the release namespace needs before `helm install`:

1. A Secret `tks-env` (`secretEnv.existingSecret`) with `TKS_POSTGRES__DSN` (database `tks`, D6), `TKS_REDIS__PASSWORD` if any, `TKS_S3__ACCESS_KEY`, `TKS_S3__SECRET_KEY`, `TKS_PRIVACY__AUTHOR_HMAC_KEY` (`openssl rand -hex 32`; never change it: ADR 0007 §6) and `TKS_TT__CANARY__ACCOUNTS` (comma separated public accounts with comments and replies; never in git).
2. A Secret `tks-proxies` (`secretFiles.existingSecret`) whose `proxies.yaml` defines the tier the values route through (`isp`, D12):

   ```yaml
   tt:
     proxy:
       tiers:
         - name: isp
           kind: static_list
           endpoints: ["http://USER:PASS@IP1:PORT", "http://USER:PASS@IP2:PORT"]
   ```
3. Kafka, Valkey and S3 reachable as `config.kafka.brokers`, `config.redis.addr` and `config.s3.endpoint` say (on the fbs infrastructure with tks's own names: D6).

Then:

```bash
helm upgrade --install tks deploy/helm/tks --set image.tag=<version> --set image.browserTag=<version>-browser
```

and an API key (printed once):

```bash
kubectl exec deploy/tks-api -- /usr/local/bin/tksctl apikey create --name <client> --scopes jobs:write,accounts:write,data:read
```

Settings the chart's values fix for tks (checked by `internal/config/deploy_test.go`): TikTok through the ISP tier, the browser signer on with Chromium's sandbox off (pods drop every capability), media downloads off (D5), and **`scheduler.budget.stretch_tiers: []`**: over a budget only backfills pause, checks keep their 2 h 45 min (owner, 2026-11-05). The dollar budget itself is `tt.cost.budget_per_account_usd` ([`tks-objectives.md`](tks-objectives.md#a5-cost-per-account-per-day)).

Sizes are C4's: 2 IPs and 2 signer pages served 1,000 accounts. Before a larger list, raise the IPs in `tks-proxies` and the post and thread crawlers' replicas (each runs `tt.signer.pages` pages); `GET /v1/admission` says how many a queue needs.

**Checks before a release:** `make chart-check` (lint, and every render validated with kubeconform) and `make chart-smoke` (below). CI runs both.

## The smoke run and the local stack

`make infra-up && make docker && make chart-smoke` renders the chart with `examples/fakett.yaml` and runs its hooks and Deployments in Docker exactly as rendered (user, read-only root, dropped capabilities, memory limits), against `fakett` with signatures required for comments and replies. It imports three accounts through the API, waits for their backfills, and fails unless every backfill completes with posts, comments, replies (signed by the crawlers' Chromium pages) and media stored, and no component logs an error. It uses its own database (`tks_smoke`), Redis DB 15 and topic prefix, next to the tests.

`make compose-up` starts the whole stack locally against `fakett` (`deploy/compose/docker-compose.yml`, Compose project `tks`, ports on 127.0.0.1 only); `make compose-down` removes it. Its pacing (`deploy/compose/local.yaml`) is for `fakett` only: never point it at TikTok.

## Key signals

The dashboard `tks / Overview` (`files/dashboards/tks-overview.json`): traffic and blocks, AIMD, breakers, operations and parsers, the pipeline, jobs and coverage, the budget, media and the API. The objectives (A1, A2, A5) have their own alerts and runbook ([`tks-objectives.md`](tks-objectives.md)); the canary and the signer theirs ([`tks-canary.md`](tks-canary.md)).

`TksAPIErrors`: read the API's logs and the database's. `TksCommentCompletenessLow` and `TksDeletionChecksSkipped`: comment reads end short of TikTok's comment count; look first at refused signed calls (`TksSignedCallsRefused`, the signer's pages) and at drift.

## Block storm

`TksBlockRateHigh` and `TksBlockRateCritical`: rate limits, captchas, login walls and checkpoints above 10 % (25 %) of answers. AIMD lowers the fleet's ceiling by itself. Look at `Block ratio by tier` and the open exit buckets (`TksProxyBucketOpen`): one provider's IPs, or all? If all, lower `tt.identity_rps` or pin the ceiling (`tt.aimd.max_rps`) and tell the owner; T20 (an IP's tolerance) was measured at 4 requests/s without a block (C4).

## Kafka lag

`TksCrawlersIdleWithBacklog`, `TksOnDemandLag`, `TksPersisterLag`, `TksHandlerPanics`. The leader publishes every `tks-*` group's lag (`tks_kafka_consumer_lag`). Crawlers idle with work queued: the crawler pods, the AIMD ceiling, the breakers and the budget. A stage behind: KEDA's `max` and the lane slots. The persister behind: `tks_persist_batch_seconds` and the database. A handler panicking: its log line names the topic, partition and offset; the record goes through once the bug is fixed.

## Browser fallback

`TksBrowserFallbackSaturated` and `TksBrowserFallbackFailing`: profile pages that carry no data (shell pages, T1) are rendered in Chromium (`tt.browser`, off by default). Saturated: raise `tt.browser.max_concurrent` or `max_share`; failing: Chromium does not start in the image (the browser image's `chromium --version`), or the rendered pages carry no data either.

## Schema drift

`TksSchemaDrift`, `TksOptionalFieldDrift`, `TksShadowParserDiffers`: TikTok changed an answer's shape. The failing answers are in the raw bucket (`tt.capture_raw: failures`); turn `all` on for a while to compare. Add the field's new path to the parser (`internal/tt/parse`), with a fixture scrubbed by `tksctl fixtures add`. `TksQueryBreakerOpen`, `TksRegistryStale` and `TksRegistryQueryFailing` are operation variants TikTok refuses: [`tks-canary.md`](tks-canary.md#an-operation-is-failing).

## Persister quarantine

`TksPersisterQuarantine`: records the persister could not write are in `tks.persist.dlq` (kept 7 days). Its log lines give each record's SQLSTATE; fix the cause (a migration, a constraint), then send them back with `tksctl dlq replay --topic tks.persist.dlq` ([`tks-operations.md`](tks-operations.md#replaying-dead-letters)); otherwise the next check of the account reads it again.

## Stuck jobs

`TksSchedulerNoLeader`: no `tks-scheduler` holds the leader lock, so nothing plans checks, admits accounts, finalises jobs or prices the day; look at the scheduler pods and their Postgres connection. `TksJobsFailing`: read the failed jobs' `error` (`GET /v1/jobs/{id}`) and the request classes. `TksDLQGrowing`: tasks that used up their attempts are in `tks.task.dlq` (kept 14 days); fix the cause and deploy, then `tksctl dlq replay` ([`tks-operations.md`](tks-operations.md#replaying-dead-letters)); otherwise the next scheduled check reads the account again.

## Schedule lag

`TksScheduleLag`: scheduled checks are more than 6 hours overdue, so the crawl capacity does not keep up: more IPs, signer pages and crawler replicas, or a look at the budget (backfills pause first; with `stretch_tiers: []` checks are never stretched). `TksBudgetOverrun` and `TksBackfillPausedLong` are the request and byte budget's; the dollar budget's are in [`tks-objectives.md`](tks-objectives.md#a5-cost-per-account-per-day).
