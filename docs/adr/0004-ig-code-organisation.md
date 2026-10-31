# ADR 0004: igs code organisation

Status: proposed (M0; accepted when the owner merges it). Revised on
2026-10-31: the owner chose a separate repository for igs, so that igs work
never touches fbs.

## Context
igs, the logged-out Instagram scraper, is built on fbs's design and code. The
build brief first said to share that code inside this repository (D7). On
2026-10-31 the owner set a stronger rule: **igs must not impact fbs work in
any way**, while still working fully. In testing igs still runs on fbs's
Kafka, Postgres 18, Valkey and S3 with separate topics, tables, lock keys,
consumer groups and metric prefixes (D6).

Facts that shape the choice (evidence in
[`docs/instagram/reuse-matrix.md`](../instagram/reuse-matrix.md)):

* **Go cannot import another module's `internal/` packages.** All of fbs's
  reusable code lives under `internal/`. Sharing it across repositories would
  mean moving it to public packages in fbs's repository, which touches fbs.
* **The client layer is mostly platform-neutral.** Of `internal/fb/*` (about
  5,700 lines without tests), the transport, proxies, rate limits, identity
  pool, registry, CDP browser engine, GraphQL pacing and the JSON walker
  carry no Facebook logic beyond names. The Facebook parts are tokens,
  request format, endpoint, detect rules, walls and consent, bundle patterns
  and entity parsers.
* **fbs's code assumes it is the only tenant** of its infrastructure: `fbs_`
  metrics registered at `init`, unprefixed Valkey keys, fixed advisory-lock
  keys (the scheduler leader `4_200_100`), `fbs-*` consumer groups, the
  `fbs-type` header, a media janitor whose sweep and TTL rule cover the whole
  bucket, and partition lookups that ignore the schema.
* **fbs changes fast:** about 75 commits reached main on 28–31 October.

## Decision

### 1. igs has its own repository, which owns a copy of fbs's code
* **Repository:** `Abdullah95k/securebrand-microservices-igs`, private. Go module
  `github.com/abdullah95k/securebrand-microservices-igs`.
* **Seed:** this repository's history up to main `f1377a9`, so `git blame`
  and cherry-picks keep working, plus one seed commit that marks the
  repository as igs and removes `.github/workflows/release.yml` (on a `v*`
  tag it would publish `ghcr.io/<owner>/fbs` and fbs's chart). Tags are not
  copied.
* **fbs is never touched.** igs sessions never push to
  `securebrand-microservices`; fbs's repository, CI, images and charts stay
  exactly as they are.
* **Upstream record.** `UPSTREAM.md` in the igs repository names the fbs
  commit the copy starts from and lists every fbs change ported since, and
  every igs fix fbs might want. Porting is by `git cherry-pick` from an
  `upstream` remote while paths still match, otherwise by hand.

### 2. Layout of the igs repository (after M1)
Neutral code and Instagram code stay apart, so a shared library remains
possible later:

| Path | Holds | From the copied fbs code |
|---|---|---|
| `internal/scrape/transport` | fingerprinted TLS/HTTP2 client | `internal/fb/transport` |
| `internal/scrape/detect` | response classes and the classifier; rules are data | the engine of `internal/fb/detect` |
| `internal/scrape/proxy` | tiers, exit buckets, breakers | `internal/fb/proxy` |
| `internal/scrape/ratelimit` | GCRA, AIMD ceiling, daily budget | `internal/fb/ratelimit` |
| `internal/scrape/session` | identities, pool, fingerprints, HTML loader with hooks | `internal/fb/session` |
| `internal/scrape/htmlx` | `JSONBlobs`, ordered pattern extraction | the neutral half of `internal/fb/htmlx` |
| `internal/scrape/registry` | query registry with per-query endpoint and headers | `internal/fb/registry` |
| `internal/scrape/graphql` | persisted-query client with a request builder (the package keeps its name, so callers need no edits) | the core of `internal/fb/graphql` |
| `internal/scrape/docids` | HTML and bundle scanner with patterns and hosts | `internal/fb/docids` |
| `internal/scrape/browser` | CDP engine, render, capture, bootstrapper | `internal/fb/browser` |
| `internal/scrape/walk` | JSON walker with a kind predicate | `internal/fb/parse/walk.go` |
| `internal/crawl/*`, `internal/platform/*`, `store`, `persist`, `scheduler`, `api`, `media`, `gdpr`, `dispatch` | the pipeline and services | same paths |
| `internal/ig/*` | `htmlx`, `detect`, `client`, `parse`, `stage`, `config`, `app`, `igtest` | new |
| `internal/fakeig`, `cmd/igs-*`, `cmd/igsctl` | synthetic Instagram, binaries, ops CLI | new |

A test enforces that `internal/scrape/...` and `internal/crawl/...` import
nothing under `internal/ig/...` or `internal/fb/...`.

**The copied Facebook code stays until igs's own tests replace it.** fbs's
unit, `fakefb` end-to-end, chaos and Phase 0 dry-run tests are the only tests
of the copied pipeline. They remain in the igs repository, under the copied
`internal/fb/*`, `internal/fakefb` and `internal/e2e`, as the regression net
for M1 and M2, and are removed once the `fakeig` suites of M6 to M8 cover the
same code. Facebook code is never run against Facebook from the igs
repository.

### 3. The crawl framework
igs's copy of the worker works on igs's own envelope
(`securebrand.igs.task.v1`, ADR 0005), so it needs no cross-platform
interface. Three safeguards carry over from the audit, because both services
share Kafka:
* the worker checks each record's type header (`igs-type`) against its
  envelope's message name;
* igs's kind values and scope field numbers differ from fbs's (ADR 0005 §2);
* a task of unknown kind is dead-lettered. In fbs's code it crashes the
  crawler (`deadLetter` calls `topics.Task`, which panics,
  `internal/topics/topics.go:50`); igs fixes its copy in M2.

The client layer is reshaped in igs's copy along the seams the reuse matrix
names: the loader takes token, wall and after-load hooks; the GraphQL client
takes a request builder that picks `/api/graphql` or `/graphql/query`; detect
rules gain paths, markers and status classes and load from config; the pool
picks a fingerprint per identity.

### 4. Isolation on shared infrastructure (D6)
fbs keeps its values because its code is untouched. igs changes every one of
them in its copy:

| Resource | fbs | igs |
|---|---|---|
| Kafka topics | `fb.*.v1`, `fbs.task.*` | `ig.*.v1`, `igs.task.*` |
| Consumer groups | `fbs-*` | `igs-*` |
| Record type header | `fbs-type` | `igs-type` |
| Postgres | its database, default schema | its own database `igs`, tables in schema `ig` (ADR 0005 §4) |
| Advisory locks | `4_200_100`, `4_200_001`, `fbs:registry-sync`, `fbs:lane:*`, `fbs:client:*` | `4_300_100`, `4_300_001`, `igs:registry-sync`, `igs:lane:*`, `igs:client:*` |
| Valkey | unprefixed keys | every key starts with `igs:` (checkpoints, leases, `jobs:finalize`, `rl:`, the API's `rl:api:`, AIMD and budget) |
| S3 | fbs's buckets | igs's own buckets: fbs's janitor deletes every object under `media/` its table lacks, and sets its TTL rule on the whole bucket |
| Metrics | `fbs_*` | `igs_*` (§5) |
| Config and env | `fb:`, `FBS_`, `FBS_CONFIG` | `ig:`, `IGS_`, `IGS_CONFIG` |
| Browser env | `FBS_BROWSER_*` | `IGS_BROWSER_*` |
| API keys | `fbs_<prefix>_<secret>` | `igs_<prefix>_<secret>` |

igs never relies on a value being different by chance. Because fbs's
partition maintenance ignores the schema, igs never creates tables in a
database fbs maintains.

### 5. Metrics
igs's copy registers its metrics under the `igs` namespace and renames the
four `fb_` series to `ig_` (`igs_ig_requests_total`), so fbs's alerts and
dashboards port by substitution. This is not cosmetic: fbs's alert rules
select by metric name only, and `fbs_scheduler_leader` is always exported, so
an igs pod exporting `fbs_*` would mask "fbs has no scheduler leader". Each
igs binary has a test that its `/metrics` output contains no `fbs_` line, and
the copied monitoring consistency test checks igs's chart and prefix.

### 6. Configuration
igs's root `Config` has the same sections as fbs's, with `ig:` in place of
`fb:` (its type renamed `Client`), embedded defaults in
`configs/default.yaml`, env prefix `IGS_` and `--config` or `IGS_CONFIG`.
The media switch the brief calls `ig.limits.include_media` is
`limits.include_media` (`IGS_LIMITS__INCLUDE_MEDIA`), default `false`.

### 7. How M1 lands in the igs repository
1. **Seed** (§1), pushed as the repository's `main`.
2. **Rename** the module path and the fbs names that would collide on shared
   infrastructure (§4, §5), with the copied fbs tests green.
3. **Move** the neutral code to `internal/scrape/*`, bottom up (detect and
   transport; proxy and ratelimit; session, htmlx, registry, docids; GraphQL
   core and browser; the walker), each step a scripted `git mv` plus import
   rewrites, tests green after each.
4. **Seams** for Instagram (loader hooks, request builder, detect rules,
   registry endpoints, fingerprint per identity), each with fbs's defaults
   reproducing today's behaviour so the copied fbs tests still pass.

Every PR runs `make test`, `make test-integration`, `make e2e` and CI in the
igs repository, and says so.

## Consequences
* fbs is untouched: no coordination, no frozen branches, no shared CI.
* A fix to shared code lands only where it is made. The brief's §5 defects
  are fixed in igs's copy; fbs keeps them unless its own session ports them,
  and `UPSTREAM.md` lists the fixes worth porting.
* fbs's later improvements reach igs only when ported.
* Shared infrastructure still needs every isolation rule in §4.
* If both services later want one library, `internal/scrape/*` is its
  starting point.

## Alternatives considered
* **Share code inside this repository** (this ADR's first version): fixes
  land once, but fbs's code and import paths change and fbs work must pause
  for the moves. Rejected by the owner.
* **igs in this repository, new folders only:** no fbs file changes, but
  `go.mod`, CI and the PR queue stay shared, so an igs change could still
  block an fbs merge.
* **A third repository with a shared library now:** fbs would have to switch
  to it to benefit, which touches fbs.
* **A copy without history:** simpler, but loses `git blame` and
  cherry-picks of fbs fixes.
* **One binary per role with a `--platform` switch:** a config mistake would
  run igs logic against fbs's topics and tables.
