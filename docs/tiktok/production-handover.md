# tks: hand-over to the master (production)

What the platform needs to deploy tks (M18, decision D-026: tks's first real run is in production). Written for the master session; the owner's part is in [`../runbooks/tks-production.md`](../runbooks/tks-production.md). Synthetic examples only.

## 1. The commit to pin

**`3bca5c0b379c62904f64ad03dac736856cc80c98`**: the merge commit of M16's pull request ([#61](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/61)) on `main` (production hardening; it adds the backup image and the backup job, so it supersedes M18's `a7a5dfab763d2170d47b155487950d4c6eb0dc15`). Pin this full SHA, never a branch. A later tks commit to deploy is announced the same way, in [`build-status.md`](build-status.md).

## 2. Images (the platform builds all three)

| Image | Dockerfile | Target | Build context | Used by |
|---|---|---|---|---|
| `registry.digitalocean.com/securebrand/tks` | `deploy/docker/Dockerfile` | `runtime` (the last stage; also the default) | the repository root (`.`) | every component except the signer, and the migrate hook |
| `registry.digitalocean.com/securebrand/tks-browser` | `deploy/docker/Dockerfile` | `browser` (Debian with Chromium) | the repository root (`.`) | `tks-signer` only |
| `registry.digitalocean.com/securebrand/tks-backup` | `deploy/docker/Dockerfile` | `backup` (`postgres:18-alpine` with `tksctl`) | the repository root (`.`) | the daily backup CronJob (M16) |

The first two hold every tks binary (`/usr/local/bin/tks-*`, `tksctl`); the backup image holds `tksctl` with PostgreSQL 18's `pg_dump` and `pg_restore`. Tag all three with the pinned commit's SHA. CI builds and scans all three on every change (Trivy: a critical vulnerability with a fix fails the build).

## 3. Chart

- **Chart path:** `deploy/helm/tks`
- **Values file:** `deploy/helm/tks/values-production.yaml` (layered over the chart's `values.yaml`)
- **Namespace:** `tks`
- **Helm keys for the images:**

  | Key | Value |
  |---|---|
  | `image.repository` | `registry.digitalocean.com/securebrand/tks` (already in the values file) |
  | `image.tag` | the commit SHA |
  | `image.browserRepository` | `registry.digitalocean.com/securebrand/tks-browser` (already in the values file) |
  | `image.browserTag` | the commit SHA |
  | `image.backupRepository` | `registry.digitalocean.com/securebrand/tks-backup` (already in the values file) |
  | `image.backupTag` | the commit SHA |

  `imagePullSecrets` if the cluster needs one to pull from the registry.

Rendered and checked in CI on every pull request (`make chart-check`: `helm lint`, `kubeconform`, and `scripts/check-production-render.py`, which checks the conventions below).

## 4. Secrets in namespace `tks`

| Secret | Keys | From |
|---|---|---|
| `platform-credentials` | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_SSLMODE`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_CA_CERT`; `KAFKA_BROKERS`, `KAFKA_USER`, `KAFKA_PASSWORD`, `KAFKA_SASL_MECHANISM`, `KAFKA_TLS`, `KAFKA_CA_CERT`; `VALKEY_HOST`, `VALKEY_PORT`, `VALKEY_PASSWORD`, `VALKEY_TLS`, `VALKEY_CA_CERT`; `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET_RAW`, `S3_BUCKET_MEDIA`, **`S3_BUCKET_BACKUPS`**; `SCHEMA_REGISTRY_URL`; **and the roles below** | the platform |
| `proxies` | `proxies.yaml`: `tt.proxy.tiers` in tks's format, each tier with `country` | the platform, from the owner |
| `tks-env` | `TKS_PRIVACY__AUTHOR_HMAC_KEY` (a secret of 64 hex characters: `openssl rand -hex 32`, made once and **never changed**), `TKS_TT__CANARY__ACCOUNTS` (3 or more of the owner's public accounts, comma separated) | made once for tks; not in git |

Notes on the mapping (`deploy/helm/tks/templates/_helpers.tpl`, `tks.platformEnv`):
- CA certificates are read as values (PEM); tks writes Postgres's to a file itself. `KAFKA_SASL_MECHANISM` may be `SCRAM-SHA-256` in any case. `KAFKA_TLS` and `VALKEY_TLS` are `true` or `false`. `S3_ENDPOINT` may be a URL (`https://…`) or a host.
- Optional keys (a missing one is left unset): the three CAs, `KAFKA_TLS`, `VALKEY_TLS`, `VALKEY_PASSWORD`, `POSTGRES_PORT` (5432), `POSTGRES_SSLMODE`, `S3_REGION`, `SCHEMA_REGISTRY_URL`.
- `S3_BUCKET_BACKUPS` is read by the backup job only (M16); without it the job fails and `TksBackupMissing` fires. Not read yet: `CONTRACT_VERSION` (the shared contract, M15).
- KEDA reads the brokers from the scaled container (`bootstrapServersFromEnv`), and `KAFKA_USER`, `KAFKA_PASSWORD`, `KAFKA_CA_CERT` through a `TriggerAuthentication`, with SASL SCRAM-SHA-256 over TLS.

## 5. PostgreSQL roles

Besides the owner (`POSTGRES_USER`, which owns the database and runs the migrations), please create these **login roles** and deliver each as `POSTGRES_<ROLE>_USER` and `POSTGRES_<ROLE>_PASSWORD` in `platform-credentials`:

| Role (suggested name) | Keys | Used by | Rights tks grants |
|---|---|---|---|
| `tks_api` | `POSTGRES_API_USER`, `POSTGRES_API_PASSWORD` | `tks-api` (it registers accounts, jobs, keys, imports and review decisions, so it writes) | read and write rows |
| `tks_reader` | `POSTGRES_READER_USER`, `POSTGRES_READER_PASSWORD` | the API's read pool; people reading the data | **read only** |
| `tks_persister` | `POSTGRES_PERSISTER_USER`, `POSTGRES_PERSISTER_PASSWORD` | `tks-persister` | read and write rows |
| `tks_scheduler` | `POSTGRES_SCHEDULER_USER`, `POSTGRES_SCHEDULER_PASSWORD` | `tks-scheduler` | read and write rows |
| `tks_crawler` | `POSTGRES_CRAWLER_USER`, `POSTGRES_CRAWLER_PASSWORD` | the crawlers, `tks-canary`, `tks-media-downloader` | read and write rows |

`tks-signer` gets no database credentials. The names are free: tks uses whatever the keys hold.

The roles need only `LOGIN` (no `CREATEDB`, no `CREATEROLE`, nothing owned). **The migrate hook grants their rights** after every migration (`tksctl migrate up`, `ttstore.Grant`): `CONNECT` (and `TEMPORARY` for writers) on the database, `USAGE` on schema `tt`, row rights on every table and sequence (also those later migrations add), and `EXECUTE` on `tt.ensure_partition`, a `SECURITY DEFINER` function that creates monthly and daily partitions as the owner. The writers can neither change nor drop the schema, truncate a table, nor rewrite the migrations' record; the end-to-end suite runs every component as such a role. A role missing when the hook runs fails the hook (and the sync), by design.

The rights are by kind (read, or read and write rows), not table by table: the components share code paths (the API registers accounts through the same code discovery uses), so a per-table list could not be proven complete before production. Narrowing it is M16's, from what the canary shows.

## 6. Topics

Every topic tks produces or consumes is in the platform's list (`configs/tt/topics.yaml` against build-status's "Production platform" table): the account, post and thread lanes, `tks.task.media`, the retries (`tks.task.retry.30s`, `.5m`, `.1h`), `tks.task.dlq`, `tks.persist.dlq` and the `tt.*.v1` topics, **including `tt.follow.v1`** (already added; 168 h). **No other topic is missing.** The chart's topic hook is off (`hooks.topics: false`), and no schema is registered (`hooks.schemas: false`).

**The shared contract's topics (M15a).** tks's persister can publish the contract's events on `content.account.v1`, `content.account_snapshot.v1`, `content.post.v1`, `content.comment.v1`, `content.metrics.v1`, `content.media.v1` and `content.audio.v1` (user `tks-kafka` is already allowed `content.*`). They are shared with the other collectors, so tks does not create them and they are not in its list. It is **off** (`tt.content.enabled: false` in `values-production.yaml`): once the platform has made these topics, set it to `true` (or `TKS_TT__CONTENT__ENABLED=true`) and restart the persister. With it on and a topic missing, the persister stops writing until the topic exists (nothing is lost; it retries).

**The rest of the contract (M15b)** goes on with the same switch, so the platform makes these **before** turning it on:
- `content.media.ready.v1` (tks-media-downloader writes), `content.media.processed.v1` and `content.media.deleted.v1` (its ledger reads them, group `tks-tt-media-ledger`);
- **`collection.coverage.v1`** (tks-scheduler: one per finished job) and **`discovery.candidate.v1`** (tks-scheduler: found accounts and their outcomes). These two are **not under `content.*`**: user `tks-kafka` needs write on them too.

With the switch on and `collection.coverage.v1` missing or refused, the finalizer cannot send a job's events and finalises nothing until it can (the job's `tt.job.v1` goes in the same batch; nothing is lost: the jobs wait to be finalised, and the finalizer logs `job failed to finalise`). The media deadline is `tt.content.media.delete_after` (72 h by default, at least 24 h) and the consumers that must acknowledge a file `tt.content.media.required_consumers` (`[sb-analysis]`); both are the owner's to change.

Pod maximums are at most the partitions of the topics each component reads: 3 for the crawlers (account, post, thread), the persister and the media downloader. When the platform adds partitions, raise `components.<name>.autoscale.max` with them.

## 7. Storage

- Raw answers in `S3_BUCKET_RAW`: failures and a 1 % sample of good answers under **`tks/keep/`** (kept for good); with `tt.capture_raw: all` the other answers under **`tks/30d/`** (production uses `sample`, so `tks/30d/` stays empty until someone switches it). tks never deletes raw files.
- Media files in `S3_BUCKET_MEDIA` under **`tks/`** (downloads are off: `tt.media.include: false`).
- tks makes no bucket and sets no lifecycle rule (`s3.create_buckets: false`).

## 8. Monitoring

The first-run report (`tksctl report`) reads Prometheus for requests, signatures, blocks, proxy data and containers. Please give the operator the Prometheus URL that scrapes namespace `tks`. If the cluster runs the Prometheus Operator, set `serviceMonitor.enabled`, `alerts.enabled` and `dashboards.enabled` to `true` in the GitOps values.

## 9. Backups

A CronJob, `tks-backup`, dumps schema `tt` every day at 02:17 Iraq time (23:17 UTC) into `S3_BUCKET_BACKUPS` under **`tks/postgres/`**, as the read-only role `tks_reader`, streamed (no disk). tks never deletes a dump: **please add the bucket's expiry rule for `tks/postgres/`** (30 days is a sensible start). Restore and failures: [`../runbooks/tks-backup.md`](../runbooks/tks-backup.md). Rollbacks need no down migration ([`../runbooks/tks-rollback.md`](../runbooks/tks-rollback.md)).

## 10. Egress policy (optional, recommended)

The chart can restrict every tks pod's egress (`networkPolicy.enabled` and `networkPolicy.egress.enabled`; off in `values-production.yaml` because NetworkPolicy takes addresses, which only the platform knows). To turn it on, set in the GitOps values:

- `networkPolicy.egress.platform`: the addresses and ports of Postgres, Kafka, Valkey, the schema registry (the VPC's range and their ports) and the object store's endpoint (443);
- `networkPolicy.egress.proxies`: the proxies' exit addresses and ports (from the Secret `proxies`);
- DNS is allowed to `kube-dns` by default (`networkPolicy.egress.dns`).

The crawlers, the canary and the signer may then reach only the proxies (and the platform); the API, the scheduler, the persister and the backup job only the platform; the media downloader also TikTok's CDN on 443 (downloads are off). The cluster's CNI must enforce NetworkPolicy. An example with synthetic addresses: `deploy/helm/tks/examples/egress.yaml`.

## 11. The stop switch

`tksctl stop --reason "…"` (from any tks pod) stops work without touching the deployment: no task starts and no request goes to TikTok until `tksctl start`. It lives in Valkey, so Argo CD's self-heal does not undo it. Use it rather than scaling to zero.
