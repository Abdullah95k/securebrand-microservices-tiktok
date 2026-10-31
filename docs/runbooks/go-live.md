# Going live

What has to happen before fbs scrapes real Facebook in production, in order. Items 1 and 2 can start at once; item 3 can run in parallel with item 2.

## 1. Legal and policy sign-off (blocks go-live)

Commenter data is personal data.
- **Reviews:** a Facebook terms-of-service review, a legitimate-interest assessment and a DPIA.
- **`privacy.pii_mode`:** `full` keeps author ids and names, readable only with the `pii:read` scope. `pseudonymized` keeps only the HMAC `author_key`.
- **Retention**, under `privacy.retention`. The defaults:

  | Data | Kept for |
  |---|---|
  | comments | 365 days |
  | inactive authors | 365 days |
  | post metrics | 180 days |
  | raw payloads | 7 days |
  | media rows | 30 days |
  | jobs | 90 days |

- **Erasure:** an owner for erasure requests (`POST /v1/erasure-requests`, scope `admin`). An erased author or page is also suppressed, so later scrapes never bring the data back.

## 2. Proxies and Phase 0 (blocks real data)

The query ids in `configs/queries.seed.yaml` are placeholders, and the parsers were built against fakefb. Phase 0 measures real, logged-out Facebook through your proxies ([runbook](phase0-spike.md)).

1. **Buy two proxy tiers** in the countries you need:
   - static ISP addresses;
   - a rotating residential gateway with sticky sessions.
2. **Rehearse:** `make phase0-dryrun` takes about a minute and needs no proxies. It must end with `OVERALL: GO`.
3. **Fill in the two files:**
   - `phase0/proxies.yaml` (then `chmod 600 phase0/proxies.yaml`);
   - `phase0/pages.csv`: about 150 real public pages, mixed by size, category, region and language. The example file shows the strata.
4. **Run** `make phase0 PHASE0_TIERS=isp,residential`. It reports, per tier: bootstrap success, block rate, comment completeness, backfill depth and a monthly cost projection.
5. **Send `phase0/out/phase0-bundle.tar.gz` privately**: attach it in a Claude Code session on this repository. Never commit it; the repository is public. With it:
   - the registry gets the real query ids;
   - the parsers are fixed against the real responses;
   - the pacing, proxy routing and daily budget defaults are set from the measurements.

## 3. Infrastructure

| What | Notes |
|---|---|
| Kubernetes | KEDA, for the lag-based autoscaling; the Prometheus Operator and Grafana, recommended: the chart ships alerts, recording rules and a dashboard; an ingress controller with TLS for the API. |
| Kafka | 3 brokers: in-cluster (Strimzi) or managed. TLS and SASL (`PLAIN`, `SCRAM-SHA-256/512`) are supported: `kafka.tls`, `kafka.sasl`. AWS MSK IAM auth is not. A Schema Registry is optional (`kafka.schema_registry.url`, Helm `hooks.schemas`). |
| Postgres 18 | Managed, with point-in-time backups. At full scale about 100M row writes a day; comments and posts are partitioned by month. An optional read replica takes the API's reads (`postgres.read_dsn`). |
| Valkey or Redis | Highly available, with persistence. It holds rate limits, job accounting, checkpoints and leases. TLS: `redis.tls`. |
| S3 | A media bucket: temporary; add a lifecycle rule expiring objects after about 4 days, as a safety net beyond `media.ttl`. A raw bucket: drift captures; a 7-day lifecycle. |
| Egress | Crawler and registry-sync pods to your proxy endpoints; the media downloader to Facebook's CDN directly (no proxy). |
| Images | Tag a release (`git tag v0.2.0 && git push origin v0.2.0`). The release workflow publishes `ghcr.io/abdullah95k/fbs:<version>` and `<version>-browser`. Make the package public, or give the cluster a pull secret (`imagePullSecrets`). |

## 4. Secrets and values

**Secrets.** `secretEnv.existingSecret` (`fbs-env`) holds these environment variables:

| Variable | Notes |
|---|---|
| `IGS_POSTGRES__DSN` | `sslmode=require` or stricter |
| `IGS_POSTGRES__READ_DSN` | Optional read replica |
| `IGS_REDIS__PASSWORD` | |
| `IGS_KAFKA__SASL__USERNAME`, `IGS_KAFKA__SASL__PASSWORD` | When `kafka.sasl.mechanism` is set |
| `IGS_S3__ACCESS_KEY`, `IGS_S3__SECRET_KEY` | |
| `IGS_PRIVACY__AUTHOR_HMAC_KEY` | 32 or more random bytes, for example `openssl rand -hex 32`. Changing it later changes every author's pseudonymous key. |

`secretFiles.existingSecret` (`fbs-proxies`) holds the key `proxies.yaml`: the `fb.proxy` tiers from your Phase 0 file.

**Values** (`helm install … -f prod.yaml`):

| Setting | Notes |
|---|---|
| `image.tag` | The released version. Also `image.browserTag` with `config.fb.browser.enabled: true` if Phase 0 showed pages that need the browser fallback. |
| `config.kafka.brokers`, `keda.kafkaBootstrap` | When the brokers need SASL or TLS: `config.kafka.sasl.mechanism` and `config.kafka.tls`, plus `keda.authSecret`: a Secret with KEDA's Kafka parameters. `keda.authParams` must list exactly the keys the Secret holds: `sasl`, `username`, `password` by default; add `tls` (`enable`) and `ca` for TLS brokers. |
| `config.redis.addr`, `config.redis.tls` | |
| `tlsFiles.existingSecret` | A Secret with the CA, and any client certificate and key, mounted at `/etc/fbs-tls`. Point `config.kafka.tls.ca_file` and `config.redis.tls.ca_file` at it, for example `/etc/fbs-tls/ca.pem`. |
| `config.s3` | Endpoint, region, buckets |
| `config.registry_sync.canary_pages` | At least three public pages that post daily and get reply threads |
| `config.media.required_consumers` | The analysis service's name in its `fb.media.processed.v1` acks |
| `config.scheduler.budget` | Requests and/or bytes per day, from the Phase 0 cost projection |
| `api.ingress` | Host and TLS |
| `alerts.enabled`, `dashboards.enabled`, `serviceMonitor.enabled` | `true` |
| `hooks.topicReplicas` | 3 |
| `networkPolicy.enabled` | `true`, plus your egress rules |

## 5. Analysis service integration

| Topic | Notes |
|---|---|
| `fb.post.v1`, `fb.post_metrics.v1` | Posts, and engagement samples over time |
| `fb.comment.v1` | Every observation of a comment. Edits arrive as new versions; deletions as `COMMENT_STATUS_DELETED` records ([contract](../contracts/kafka-schemas.md)). |
| `fb.media.ready.v1` | Read the object from the media bucket with read-only credentials, then acknowledge it on `fb.media.processed.v1` ([contract](../contracts/media-handoff.md)). Media nobody acknowledges is deleted after `media.ttl` (72h) anyway. |
| `fb.job.v1` | One event per finished job, with coverage |

The REST API serves the same data (`/v1/pages/{page}/posts`, `/v1/posts/{post}/comments`, …); see `api/openapi/fbs.v1.yaml`.

## 6. Deploy and ramp up

1. **Staging.**
   - Install the chart against fakefb with the [staging values](../../deploy/helm/fbs/examples/fakefb.yaml). The file lists the Services and the Secret it expects.
   - Register a few fakefb pages (`brand0`, `brand1`, …) with `fbctl pages import` and check that their backfills complete.
   - `make chart-smoke` rehearses exactly this without a cluster. It runs the chart's hooks and Deployments in Docker, with the rendered config, user, read-only root filesystem and dropped capabilities, and CI runs it on every change.
2. **Production install.** The hooks run `fbctl migrate up` and `fbctl kafka apply`, and `fbctl schemas apply` when enabled.
3. **API keys.**

   ```bash
   kubectl exec deploy/fbs-api -- fbctl apikey create --name analysis --scopes data:read
   ```

   `jobs:write`, `pages:write` and `pii:read` are the other scopes; `admin` covers queries, erasure and registry-sync runs.
4. **Canary.**
   - Register 50–100 real pages:

     ```bash
     fbctl pages import --api https://fbs.example.com --key "$KEY" canary.csv
     ```

     A file of page URLs or vanity names, one per line or as CSV with a `page` column.
   - Watch the "fbs / Overview" dashboard for 2–3 days: block rate, comment completeness, proxy GB per day, schedule lag and deletion checks.
5. **Ramp.**
   - Go from 1k to 10k to 100k to all pages, with `scheduler.budget` set.
   - New pages backfill in the low-priority lane, which the budget pauses first.
   - Raise `fb.aimd.max_rps` only while the block rate stays low.
6. **On-call.**
   - Route the PrometheusRule alerts (Alertmanager to Slack or PagerDuty).
   - Every alert links to its section in [operations](operations.md).
   - Review [cost tuning](cost-tuning.md) after the first week.
