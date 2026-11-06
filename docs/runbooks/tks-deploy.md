# tks: deploying and the alerts

How tks is built, installed and watched: the images, the Helm chart (`deploy/helm/tks`), the local Compose stack, and what to do when an alert in the chart's rules fires. The other runbooks cover the rest: [`tks-operations.md`](tks-operations.md) (restarts, identities), [`tks-objectives.md`](tks-objectives.md) (A1, A2, cost and budget), [`tks-canary.md`](tks-canary.md) (operations and the signer), [`tks-import.md`](tks-import.md) (accounts), [`tks-discovery.md`](tks-discovery.md).

**No production traffic to TikTok before C5**: legal signed off (D15, 2026-11-05); staging and the canary of 50 to 100 real accounts come first ([build brief §6](../tiktok/build-brief.md#6-milestones)).

## Images

One Dockerfile (`deploy/docker/Dockerfile`), two images, every binary in both:

| Image | Build | Used by |
|---|---|---|
| default (distroless, static binaries) | `docker build -f deploy/docker/Dockerfile -t tks:dev .` | `tks-api`, `tks-scheduler`, every `tks-crawler` stage, `tks-persister`, `tks-media-downloader`, `tks-canary`, the hooks (`tksctl`), `fakett`, `devs3` |
| browser (Debian with Chromium) | `docker build -f deploy/docker/Dockerfile --target browser -t tks:dev-browser .` | `tks-signer` only: its pages sign the comment and reply calls (D11, M9d) |

`make docker` builds both (`IMAGE`, `BROWSER_IMAGE`). The release publishes both tags of one repository; the chart's `image.tag` and `image.browserTag` pick them.

## The Helm chart

`deploy/helm/tks`: one Deployment per component, pre-install and pre-upgrade hooks (`tksctl migrate up`, `tksctl kafka apply`, optionally `tksctl schemas apply`), KEDA scalers on consumer lag, PodDisruptionBudgets, and optionally the PrometheusRule (`alerts.enabled`), the dashboard (`dashboards.enabled`), a PodMonitor, an Ingress and a NetworkPolicy.

| Component | Command | Image | Notes |
|---|---|---|---|
| `api` | `tks-api` | default | Service on port 80 |
| `scheduler` | `tks-scheduler` | default | 2 replicas, one leader |
| `crawler-account` | `tks-crawler --stages account` | default | Post lists, metrics, account refresh (plain HTTP) |
| `crawler-post` | `tks-crawler --stages post` | default | Comments, signed by tks-signer |
| `crawler-thread` | `tks-crawler --stages thread` | default | Replies, signed by tks-signer |
| `crawler-retry` | `tks-crawler --stages retry` | default | The retry relays |
| `persister` | `tks-persister` | default | |
| `media-downloader` | `tks-media-downloader` | default | Idle while `tt.media.include` is false (D5); no janitor: another service deletes files |
| `canary` | `tks-canary` | default | Needs `tt.canary.accounts` (production: `values-production.yaml`) or `TKS_TT__CANARY__ACCOUNTS`; signs through tks-signer |
| `signer` | `tks-signer` | browser | The signer's pages in their own capped pod; Service on port 8080 for the crawlers and the canary only ([below](#the-signer-service)) |

What the release namespace needs before `helm install`:

1. A Secret `tks-env` (`secretEnv.existingSecret`) with `TKS_POSTGRES__DSN` (database `tks`, D6), `TKS_REDIS__PASSWORD` if any, `TKS_S3__ACCESS_KEY`, `TKS_S3__SECRET_KEY`, `TKS_PRIVACY__AUTHOR_HMAC_KEY` (`openssl rand -hex 32`; never change it: ADR 0007 §6) and, unless the values set `config.tt.canary.accounts` (production's do), `TKS_TT__CANARY__ACCOUNTS` (comma separated public accounts with comments and replies).
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

Settings the chart's values fix for tks (checked by `internal/config/deploy_test.go`): TikTok through the ISP tier, the browser signer on in tks-signer (`tt.signer.mode: remote`) with Chromium's sandbox off (pods drop every capability), media downloads off (D5), and **`scheduler.budget.stretch_tiers: []`**: over a budget only backfills pause, checks keep their 2 h 45 min (owner, 2026-11-05). The dollar budget itself is `tt.cost.budget_per_account_usd` ([`tks-objectives.md`](tks-objectives.md#a5-cost-per-account-per-day)).

Sizes are C4's: 2 IPs and 2 signer pages served 1,000 accounts. Before a larger list, raise the IPs in `tks-proxies` and tks-signer's replicas (`components.signer.replicas`, each with `tt.signer.pages` pages in 2 GiB); `GET /v1/admission` says how many a queue needs.

## The signer service

`tks-signer` (M9d) runs the browser signer's Chromium pages in a pod of their own, so a page that runs away can only take down that pod, never a crawler or a node. The crawlers and the canary run no browser (`tt.signer.mode: remote`, `tt.signer.remote_url` set by the chart to `http://<release>-signer:8080`) and ask it over HTTP:

1. The crawler builds the call as before and sends `POST /v1/sign` with the URL, the page it is made from, the identity's browser fingerprint (user agent and client hints) and its exit **by name** (`tier` and `exit` for a static exit, `session` for a gateway). tks-signer finds that exit in its own copy of the proxy file (the same `tks-proxies` Secret), so **no proxy credential crosses the network**, and an exit it does not have is refused.
2. A page with that user agent signs the call, unsent, with TikTok's own script (as before: a page opened on the asking identity's exit is reused for every identity with its user agent, T27 and C4). tks-signer answers the signed URL; the crawler sends it by plain HTTP.
3. The crawler reports TikTok's answer (`POST /v1/outcome`) to the replica that signed (its pod IP, `$POD_IP`). A refused call names the page, and the crawler's retry has tks-signer sign on another one (S4); a page refused `max_refusals` times in a row is replaced.

Limits: memory request = limit (2 GiB for 2 pages), one CPU at most. Within the pod, a page whose Chrome passes `tt.signer.max_page_mb` (800) is reopened after its call or when idle, and one past **`tt.signer.kill_page_mb` (1,200) is closed even while it signs**, found within 10 s (`tks_signer_page_closes_total{reason="killed"}`); its call fails and is signed again on a fresh page. With `networkPolicy.enabled` only the post and thread crawlers and the canary reach its port; `networkPolicy.signerEgress` restricts its egress to the proxies, Valkey and DNS.

Its metrics are the signer's (`tks_signer_*`, `tks_signed_calls_total`, `tks_signer_page_seconds_total` for the cost); the callers count their calls to it in `tks_signer_remote_calls_total{call, result}`. `tt.signer.mode: browser` keeps pages in the process itself (the probes, Phase 0, `tks-spike`); `none` signs nothing.

## tks-signer unreachable

`TksSignerUnreachable`: more than 20 % of the crawlers' sign calls got no answer for 5 minutes. No comment or reply is read meanwhile; the reads wait in the 30 s retry topic (as without a page), nothing is sent unsigned and nothing is lost.
1. `kubectl get pods -l app.kubernetes.io/component=signer`: not running or restarting? An OOM kill: see [tks-signer memory](#tks-signer-memory). Not starting: its log says why (Chromium missing in the image, `image.browserTag` unset, or the proxy file unreadable).
2. Running but unreachable: the Service `<release>-signer` and, with `networkPolicy.enabled`, the `<release>-signer` NetworkPolicy (only `crawler-post`, `crawler-thread` and `canary` may connect).
3. `tks_signer_remote_calls_total{result="refused"}` instead: tks-signer refuses the requests; its answer names the exit it lacks when its proxy file differs from the crawlers' (both mount `secretFiles`).

## tks-signer memory

`TksSignerMemoryNearLimit` (over 90 % of its limit for 10 minutes) and `TksSignerOOMKilled` (the pod restarted after an OOM kill). Only the signer's calls fail meanwhile; crawlers and nodes are unaffected, which is what the cap is for.
1. `tks_signer_page_closes_total` by reason: `killed` and `memory` rising mean pages grow; check `tt.signer.freeze` and `open_on: profile` are on (S2: the runaway is the video page's).
2. Two pages near `max_page_mb` together, plus Chrome's shared processes, fill 2 GiB: lower `tt.signer.kill_page_mb` or `max_page_mb`, or set `tt.signer.pages: 1` and add a replica.
3. Never raise the limit without lowering the pages per pod: the point is that one pod's pages can never take more than the pod.

**Checks before a release:** `make chart-check` (lint, and every render validated with kubeconform) and `make chart-smoke` (below). CI runs both.

## The smoke run and the local stack

`make infra-up && make docker && make chart-smoke` renders the chart with `examples/fakett.yaml` and runs its hooks and Deployments in Docker exactly as rendered (user, read-only root, dropped capabilities, memory limits), against `fakett` with signatures required for comments and replies. It imports three accounts through the API, waits for their backfills, and fails unless every backfill completes with posts, comments, replies (signed by tks-signer's Chromium pages, which the crawlers reach by its name as by its Service) and media stored, and no component logs an error. It uses its own database (`tks_smoke`), Redis DB 15 and topic prefix, next to the tests.

`make compose-up` starts the whole stack locally against `fakett` (`deploy/compose/docker-compose.yml`, Compose project `tks`, ports on 127.0.0.1 only); `make compose-down` removes it. Its pacing (`deploy/compose/local.yaml`) is for `fakett` only: never point it at TikTok.

## Usable exits running low

`TksUsableExitsLow` (fewer than half a tier's exit groups can start identities, for 15 minutes) and `TksNoUsableExits` (none, for 5 minutes). An exit group is a static exit's /24 or a gateway's country; its breaker opens after blocks and lets one probe through after a cooldown that doubles while probes fail (`tt.proxy.breaker`). `tks_proxy_exits{tier, state}` counts them per pod (`usable`, `cooling`).
1. The block rate by tier on the dashboard: one provider's IPs, or all? Blocks across every tier point at tks's pacing: lower `tt.identity_rps` or pin the AIMD ceiling (`tt.aimd.max_rps`).
2. One tier only: the provider (expired IPs, a changed gateway, credentials). Route around it in `tt.proxy.routing` or ask the owner for new IPs (the Secret `proxies`).
3. None usable and blocks still high: `tksctl stop --reason "no usable exits"` ([`tks-production.md`](tks-production.md#5-stop-rules)) until the cause is fixed; nothing is lost.

## Memory near the limit

`TksMemoryNearLimit` (a container over 90 % of its memory limit for 15 minutes) and `TksOOMKilled` (it restarted after an OOM kill; a stop rule of the canary). The signer has its own: [tks-signer memory](#tks-signer-memory).
1. Which component: `kubectl -n tks top pods`. Its heap (`go_memstats_heap_inuse_bytes`) growing without bound is a leak: save the component's log and its memory graph and tell the build session.
2. Load (more accounts, a larger persister batch): raise `components.<name>.resources.limits.memory` (and the request with it) in the GitOps values, and sync.

## Scratch volume near its limit

`TksScratchNearLimit` (a pod's `/tmp` over 80 % of `tmpSizeLimitMB` for 15 minutes) and `TksPodEvicted`. Every pod's root filesystem is read-only; `/tmp` is an `emptyDir` capped at `tmpSizeLimitMB` (1 GiB; the signer 4 GiB, `components.signer.tmpSizeLimitMB`), past which the kubelet evicts the pod. `tks_tmp_bytes` and `tks_tmp_limit_bytes` measure it.
1. The signer: its Chromium profiles. Pages are replaced after `tt.signer.page_max_signs` or `page_max_age`, which removes their profile; lower either, or raise the limit.
2. Another component: nothing else writes there but the Postgres CA (a few KB). Look inside (`kubectl -n tks exec POD -- ls -la /tmp` does not work in the distroless image; use `kubectl debug`) and tell the build session.
3. Evicted for the node's disk or memory instead (`kubectl describe pod`): the platform's.

## Egress policy

With `networkPolicy.enabled` and `networkPolicy.egress.enabled` (M16), three NetworkPolicies limit where tks's pods may connect: every pod and the backup job to DNS, tks's own pods and `networkPolicy.egress.platform` (Postgres, Kafka, Valkey, the object store, the schema registry); the crawlers, the canary and the signer also to `networkPolicy.egress.proxies` (the proxies' exits); the media downloader also to `networkPolicy.egress.media` (TikTok's CDN on 443, from its own IP). The hooks are not limited. The chart refuses egress without proxies. Values: [`values.yaml`](../../deploy/helm/tks/values.yaml); an example: [`examples/egress.yaml`](../../deploy/helm/tks/examples/egress.yaml).

Symptoms of a missing address: connection timeouts (not refusals) to that service in the component's log, and `TksUsableExitsLow` with every exit cooling when the proxies are missing. Add the address, sync, and the pods connect again without a restart.

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
