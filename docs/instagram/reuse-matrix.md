# igs reuse matrix: what igs takes from fbs

Every fbs package classified for igs, with its Facebook coupling and the seam that makes it shareable. It was checked against the code at `df76ce1` (main `f1377a9` plus the docs of PR #22) on 2026-10-31. `path:line` references are at that commit. Regenerate the import graph with `scripts/import-graph.sh`. The decisions it supports are in [ADR 0004](../adr/0004-ig-code-organisation.md) (code) and [ADR 0005](../adr/0005-ig-data-model-and-contracts.md) (data).

> **Reading this after the separate-repository decision (2026-10-31, ADR 0004 §1).** igs owns a copy of this code in `Abdullah95k/securebrand-microservices-igs`, so every change named here is made in igs's copy and fbs's repository never changes. The classification and the evidence stand as written. "Seam" now describes the change in igs's copy, where keeping fbs's default matters only while the copied fbs tests are igs's regression net. §2 still applies in full: fbs's code keeps every single-tenant assumption, so on shared infrastructure igs must avoid each of fbs's names.

**Classes:**
- **reuse:** moves to a neutral path unchanged; only its import path changes.
- **parameterise:** shared after a small seam. fbs passes values that reproduce today's behaviour exactly; igs passes its own.
- **rewrite:** igs writes its own, copying the named structure. fbs keeps its package untouched.
- **split:** parts of the package fall in different classes; the row says which.

**Lines** are Go source / test lines, without generated code.

---

## 1. Summary

| Area | reuse | parameterise | rewrite | Biggest seam |
|---|---|---|---|---|
| Client layer (`internal/fb/*`, `internal/fixtures`) | transport, proxy | ratelimit, session, registry, docids, browser, detect, fixtures | fbtest | the request builder in `graphql` and the loader hooks in `session` |
| Crawl framework (`internal/crawl/*`) | lanes | checkpoint, jobs, rawsink, shadow | — | the worker over an envelope interface (`stage`, `task`) |
| Backend (`dispatch`, `api`, `persist`, `scheduler`, `media`, `gdpr`, `store`, `replay`) | — | dispatch, media, store, replay | `cmd/fbctl` (as `igsctl`) | the finalizer hook and the media handoff adapter |
| Platform and config | breaker, config loader, leader, log, pg, redisx, run, s3x, schemareg | metrics, httpserver, kafka, `internal/config` | — | the metric namespace |
| Tools, tests, deploy | devs3 | spike orchestration, testenv, compose, CI | registrysync, fakefb, e2e stack, pilot, the `cmd/fbs-*` mains, the Helm chart | the Phase 0 `Measurer` (M8) |

Split packages: `htmlx`, `graphql`, `parse`, `crawl/task`, `crawl/stage`, `crawl/emit`, `app`, `api`, `persist`, `scheduler`, `gdpr`, `spike`.

**What no Instagram code can avoid:** tokens and their patterns, the request form and headers, the two endpoints, detect rules and wall paths, entity parsers, stage bodies, the data model, `fakeig`, and the Phase 0 measurement flow. Everything else is shared.

---

## 2. Single-tenant assumptions

These would make fbs and igs interfere on shared infrastructure (D6). Each must be parameterised before igs runs next to fbs. fbs keeps today's values.

| What | Where | What goes wrong if shared | Seam |
|---|---|---|---|
| Metric namespace `fbs`, set at `init` | `internal/platform/metrics/metrics.go:21,27,33,39` | every igs binary exports `fbs_*` series; fbs's alerts and dashboards read igs traffic | gatherer that renames at scrape time (ADR 0004 §5) |
| Valkey keys without a prefix: `ckpt:`, `done:`, `lease:`, `pc:`, `tc:` | `internal/crawl/checkpoint/checkpoint.go:47,74,80,108,158-184` | `lease:post:<n>`, `pc:<n>` and `tc:<n>` are bare numeric ids, so igs and fbs would block or corrupt each other's streams | key prefix option |
| `jobs:finalize` set | `internal/crawl/jobs/jobs.go:20,378` | each finalizer samples the other's job ids, logs a load error and never removes them | key prefix option |
| GCRA key `rl:<key>`; API key `rl:api:<client>` | `internal/fb/ratelimit/ratelimit.go:33`; `internal/api/auth.go:132` | igs would spend fbs's global request rate (`rl:global`) | key prefix option |
| AIMD and budget keys namespaced by `kafka.topic_prefix` (empty) | `internal/fb/ratelimit/aimd.go:34,37`, `budget.go:27-34`; `internal/app/crawler.go:148,153,226` | the two fleets would share one ceiling (`aimd:global_rps`) and one budget (`budget:state`) | igs passes its own non-empty namespace |
| Scheduler leader lock `4_200_100` | `cmd/fbs-scheduler/main.go:30,107` | on one database, only one of the two schedulers would ever lead | key from the binary |
| Partition DDL lock `4_200_001` | `internal/store/partitions.go:18` | the two maintenance loops serialise (harmless, but coupled) | key from the binary |
| Advisory locks `fbs:lane:*`, `fbs:client:*`, `fbs:registry-sync` | `internal/dispatch/dispatch.go:166-181`; `internal/registrysync/sync.go:183` | one service's job caps and registry runs would block the other's | prefix from the binary |
| Partition lookup by parent name only | `internal/store/partitions.go:146-151` | with igs's schema in fbs's database, fbs's hourly maintenance lists `ig.comments`'s partitions and fails on the unqualified `DROP` | schema filter (must land before igs shares a database) |
| goose version table `goose_db_version`, one migration set | `internal/store/migrate.go:16` | the two services could not migrate independently | `goose.WithTableName` and a migration FS parameter |
| Consumer groups `fbs-*` | `internal/app/crawler.go:180,213`; `internal/persist/consumer.go:20`; `cmd/fbs-media-*/main.go:56`; lag filter `internal/scheduler/lag.go:30` | — (igs simply uses `igs-*`) | names from the platform |
| Record type header `fbs-type` | `internal/platform/kafka/producer.go:24` | — | producer option (`igs-type`) |
| Topic names as constants; `Task()` panics on an unknown kind | `internal/topics/topics.go:11-52` | — | a topic set per platform |
| Media bucket: orphan sweep over `media/`; lifecycle rule `fbs-media-ttl` on the bucket | `internal/media/janitor.go:190-205`; `internal/media/lifecycle.go:5-11` | each janitor deletes the other's files; each overwrites the other's TTL rule | separate buckets (config) |
| Raw capture keys `<prefix>/<query>/…` | `internal/crawl/rawsink/rawsink.go:51-62,122` | captures of same-named queries (`comments`) mix | separate raw bucket (config) |
| Env names `FBS_*`, `FBS_CONFIG`, `FBS_BROWSER_*` | `internal/config/config.go:17,313`; `internal/fb/browser/render.go:26,39`, `cdp.go:237` | — | igs's own config root and options |
| `internal/app` as the composition root | `internal/app/crawler.go` | every fbs binary links the whole crawler stack, so "running fbs's binary with igs config" is not possible | igs's own root, `internal/ig/app` |

---

## 3. Client layer

| Package | Lines | Facebook coupling | Class | Seam: fbs → igs | Target |
|---|---|---|---|---|---|
| `fb/transport` | 279 / 0 | none (comments mention `chrome_146` and `.facebook.com`, `transport.go:51,173-181`) | reuse | — | `scrape/transport` |
| `fb/proxy` | 258 / 123 | `config.Proxy` types (`proxy.go:71,208`); `detect.Class` in `Observe` (`:155-160`) | reuse | after `detect.Class` moves | `scrape/proxy` |
| `fb/ratelimit` | 645 / 304 | unprefixed `rl:` (`ratelimit.go:33`); namespaced AIMD and budget keys; `config.AIMD`, `config.Budget`; `detect.Class` (`aimd.go:72`) | parameterise | `NewRedis(c, prefix)`: fbs `""`, igs `igs:`; igs passes its own `ns` | `scrape/ratelimit` |
| `fb/session` | 633 / 124 | Identity holds `htmlx.Tokens`, "bootstrapped" = an LSD token (`identity.go:34,65,72,83,189`); loader extracts Facebook tokens (`bootstrap.go:116`), plants `datr` from `_js_datr` (`:117-119`), accepts the consent wall with a POST to `/cookie/consent/` (`:120-125,167-192`), treats only `/login` and `/checkpoint` as walls (`:201-203`); one fingerprint profile per pod (`pool.go:150`) | parameterise | tokens become a map keyed by form field; `Loader{Extract, IsWall, AfterLoad}` (fbs: today's functions; igs: Instagram tokens, `/accounts/login`, `/challenge`, no after-load); `PoolConfig.Profiles []string` | `scrape/session`; fbs's hooks stay in `internal/fb` |
| `fb/htmlx` | 190 / 89 | `Tokens` has DTSG, `_js_datr` and consent but no `csrf_token`, app id or rollout hash (`tokens.go:15-27`); Facebook patterns (`:54-75`); `PageID` from `fb://` URLs (`:99-152`) | split | `JSONBlobs` and the pattern engine: reuse. Tokens, patterns, `PageID`: stay in fbs; igs writes its own pattern table | `scrape/htmlx`, `fb/htmlx`, `ig/htmlx` |
| `fb/detect` | 239 / 42 | Facebook error codes and markers (`detect.go:72-86`); `/checkpoint` and `/login` redirects (`:108-111`); HTML checkpoint marker (`:165`); `for (;;);` (`:188`); 400/401/403 fall through to OK (`:113-126`) | split, then parameterise | class enum, `Input` and engine move; `Rules` gains login and checkpoint paths, HTML markers and status classes, loaded from config. fbs's defaults equal today's rules | `scrape/detect`; rules in `fb/detect`, `ig/detect` |
| `fb/registry` | 418 / 94 | tables `fb_queries`, `fb_client_params` (`registry.go:261,308-373`); placeholder doc_id regex (`:111`); no per-query endpoint or headers (`:32-46`); variable keys re-ordered alphabetically (`:82,93,383-392`, and the `jsonb` column) | parameterise | `Tables{Queries, Params}`; `Query.Endpoint`, `Query.Headers` (empty = today) | `scrape/registry` |
| `fb/docids` | 142 / 86 | bundle URLs must contain `rsrc.php` (`docids.go:19`); two Facebook declaration regexes (`:20-21`); hosts limited to `fbcdn.net` (`:48`) | parameterise | `Scanner.Patterns`, `Scanner.AllowHost`, a script-URL pattern | `scrape/docids` |
| `fb/browser` | 1,013 / 407 | env `FBS_BROWSER_BINARY`, `FBS_BROWSER_NO_SANDBOX`; captures only `/api/graphql` POST bodies (`render.go:148-159,464-480`); the bootstrapper hard-codes `detect.DefaultRules()`, `session.IsWall`, Facebook tokens and `datr` (`bootstrap.go:61,120-138`) | parameterise | capture filter, GET query strings, friendly name from a form key or the `x-fb-friendly-name` header; the loader's hooks; injected rules; env names as options | `scrape/browser` |
| `fb/graphql` | 392 / 20 | fixed endpoint `/api/graphql/` (`client.go:150`); fixed header list (`:155-173`) without `x-csrftoken` or `x-ig-app-id`; fixed form order without `__d` or the client-state fields (`:247-278`); `Request.PageID` becomes the limiter key `page:<id>` (`:34,226`) | split | `parts.go` (`SplitParts`, which already reads NDJSON without a `for (;;);` prefix): reuse. Client: a `RequestBuilder` returning endpoint, ordered headers and body | `scrape/graphql`; fbs's builder in `fb/graphql` |
| `fb/parse` | 1,436 / 334 | the walker keys on `__typename` (`walk.go:72,124,166,233`); Facebook graph ids (`fields.go:88-151`); reactions; typenames; `l.facebook.com` (`timeline.go:306,314`) | split | `walk.go` and the neutral helpers (`First`, `Report`, `Int64`, `Unix`, `ParseCount`, `MediaID`, `URLExpiry`): parameterise with `CollectBy(kindOf)`, since Instagram objects often lack `__typename`. Entity parsers: rewrite | `scrape/walk`; `fb/parse`, `ig/parse` |
| `fb/fbtest` | 104 / 359 | fakefb, the Facebook seed, `chrome_146`, `DefaultRules` | rewrite | igs copies `Env`/`Options`/`New` against `fakeig` | `ig/igtest` |
| `fixtures` | 599 / 304 | key sets, `for (;;);`, Facebook id and profile-URL patterns, `fbcdn` signed URLs, person detection by typename (`scrub.go:46,78,114-153,255-262,319,330,371`) | parameterise | a `Scrubber` holding the detectors; igs adds `username` + `pk` and pseudonymises usernames consistently. A change to fbs's output bumps `scrubVersion` (`:64`) | stays |
| `configs/fingerprints.yaml` | — | neutral format; profiles `chrome_146`, `chrome_144`, `chrome_133` only. The pinned tls-client (v1.17.1) also ships `chrome_150` and `chrome_152` | reuse | add `chrome_152` (M2) | stays |
| `configs/queries.seed.yaml` | — | Facebook queries and client params | rewrite (content) | igs's seed holds the five operations of the brief §3 and an endpoint per query | `configs/ig/` |

---

## 4. Crawl framework

| Package | Lines | Facebook or single-tenant coupling | Class | Seam | Target |
|---|---|---|---|---|---|
| `crawl/task` | 250 / 73 | scope keys `page:`, `pageref:`, `post:`, `thread:`, `media:` (`task.go:28-43`); `NormalizeRef` parses facebook.com URLs (`:163-213`); `RefPath`; `DefaultLimits(config.Limits)`; `taskv1` and `fbv1` in every signature | split | `hashID`, `Finalize`, `Continuation`, `Retry`: generic over the envelope, **with byte-identical hash inputs** (`:48,50`), or in-flight ids, `done:` markers and checkpoints are orphaned. `ScopeKey`, `New*`, `NormalizeRef`, `RefPath`, `DefaultLimits`: platform code | `crawl/task` + `fb/stage` |
| `crawl/stage` | 1,369 / 77 | worker: `Stages map[taskv1.Kind]` (`worker.go:27`), stage names (`:38-40`), `topics.*` (`:55,196-216,237`), a dead-lettered `KIND_POST` marks its post failed (`:229-231`); deps: Facebook clients (`deps.go:34-49`), bootstrap from `/<pageID>` (`:98-120`), `crawl_cursors` with `'page_timeline'` (`:243,266-277`); page and post stages are Facebook end to end | split | worker, `RetryRelay` (header-only, `:261-295`), `spawn`, `saveCkpt`: parameterise (envelope, platform, topic set, an `OnDeadLetter` hook replacing the `KIND_POST` case). Page, post and thread stages: move to `fb/stage`; igs copies the loop skeleton (checkpoint resume, yield, wall truncation, spawn after the batch) | `crawl/worker` + `fb/stage` |
| `crawl/checkpoint` | 185 / 0 | unprefixed keys (§2); the post and thread caches hold Facebook's comment state | parameterise | key prefix | stays |
| `crawl/jobs` | 394 / 171 | `jobs:finalize` (§2); per-post pass and "seen" records hold (id, reply count) for Σ(1 + reply_count) (`jobs.go:124-328`) | split | accounting: key prefix. Post state, passes and seen lists: fbs-only, or a neutral "sub-entity state" igs re-uses for deletion detection | stays |
| `crawl/lanes` | 172 / 202 | none | reuse | — | stays |
| `crawl/emit` | 188 / 51 | `topics.*` (`emit.go:70-128`); `fbv1` types; Facebook author privatisation (`:108-123`) | split | `stamp` (content hash and event id, `:52-56`) and `normalize.go` (stable signed URLs; `oe=` and `_nc_` are Meta CDN parameters): generic over `proto.Message`, hashes byte-identical. Entity emitters: rewrite | stays (core) |
| `crawl/rawsink` | 127 / 0 | `detect.Class` (`rawsink.go:19,51-54`); keys `<prefix>/<query>/…` | parameterise | neutral class; a separate raw bucket | stays |
| `crawl/shadow` | 178 / 83 | typed on Facebook parse results (`shadow.go:26-30,122-149`) | parameterise | `Compare[R any]` with a keys function per platform | stays |

**Envelope fields the shared code touches** (for the interface in ADR 0004 §3): `task_id`, `lineage_id`, `seq`, `job_id`, `lane`, `kind`, `cursor` and `attempt` are read; `stalls` is read and written; the scope is read only through `ScopeKey` and the dead-letter hook; `mode`, `limits` and the scope are stage business. `enqueued_at` is written but never read; `affinity` and `trace` are copied but never set; `not_before` is unused (the retry relay uses the `x-not-before` header). The retry relay and the DLQ path never decode the envelope; `jobs.Tracker` only sees id strings.

---

## 5. Backend services

| Package | Lines | Facebook or single-tenant coupling | Class | Seam | Target |
|---|---|---|---|---|---|
| `dispatch` | 329 / 0 | SQL on `pages`, `page_schedule`, `crawl_cursors` and `jobs.page_ref/page_id` (`dispatch.go:101-321`); advisory locks `fbs:lane:*`, `fbs:client:*` (`:166-181`); `task.NewPage`/`NewPost`; `BaseURL` set but never read (`:81`) | parameterise | the platform builds roots and resolves the target; lock prefix; tables through `search_path`; subject column names | stays |
| `app` | 378 / 169 | fbs's composition root (§2): seed, fingerprints, Facebook clients, groups `fbs-retry-relay`, `fbs-crawler-<stage>-<lane>` | split | `clients.go`: reuse. The per-stage × lane consumer loop: extract. The rest stays fbs's | `internal/ig/app` for igs |
| `topics` | 96 / 0 | §2 | parameterise | a `TopicSet`; a neutral lane type | stays |
| `api` | 1,493 / 60 | keys `fbs_<prefix>_<secret>` (`auth.go:57`); `rl:api:` (`:132`); `task.NormalizeRef`; Facebook data endpoints (`data.go`, `pages.go`); registry admin | split | auth, problem+json, cursors, limits: parameterise (key prefix; tables through `search_path`). Jobs: parameterise (ref normaliser, limits builder) and add owner checks (M2). Data, pages, admin: rewrite | stays + igs routes |
| `persist` | 813 / 291 | group `fbs-persister` (`consumer.go:20`); `topics.Entity` with Facebook decoders (`:32-35,118-140`); Facebook tables; comment edit, restore and deletion rules | split | consumer loop: parameterise (group, topic → decoder, flush). `copyTemp`, `latest`, partition-on-demand: reuse. Writer: rewrite | stays + igs writer |
| `scheduler` | 1,162 / 160 | `topics.Job`, `topics.Entity`, `persist.Group` (`finalizer.go:75-90`); `fbs-` lag filter (`lag.go:30`); Facebook retention tables (`loops.go:298-337`); Σ(1 + reply_count) deletion check (`finalizer.go:295-356`); `parse.FeedbackID` (`adaptive.go:283`) | split | `Every`, the finalizer core (ready set → end offsets → caught up → terminal row → event), the reaper, lag gauges: parameterise (a platform finalize hook, an event builder, the group prefix). Planner, adaptive tiers, retention: rewrite | stays + igs loops |
| `media` | 484 / 0 | decodes `taskv1.Task` → `fbv1.Media` (`downloader.go:76-81`); Facebook columns of `media`; `fb.media.*` topics; object metadata `fb-media-id` (`:200`); §2 bucket issues | parameterise | a handoff adapter (task decode, ready and deleted messages, platform columns), topic set, group names, lifecycle rule id, buckets | stays; thin `cmd/igs-media-*` |
| `gdpr` | 315 / 0 | `suppression`, `erasure_requests`; Facebook entity deletes (`erasure.go:208-311`); `topics.Erasure` | split | `AuthorKey` and `suppression.go`: reuse (tables through `search_path`). Erasure: rewrite, copying the request loop and media deletion | stays + igs eraser |
| `store` | 266 / 0 | §2 (goose table, lock key, schema-blind partition lookup); fixed partition table lists (`partitions.go:21-30,115`) | parameterise | `Migrations{FS, Table}`; `PartitionSet{Monthly, Daily, LZ4, FillFactor, LockKey}`; schema filter | stays |
| `replay` | 248 / 123 | Facebook parser switch, base `https://www.facebook.com` (`replay.go:218`) | parameterise | `Options.Parse` | stays |
| `cmd/fbctl` | 752 / 62 | Facebook seed and topics; DLQ replay group `fbctl-dlq-replay` | rewrite | `igsctl` copies `dlq replay`, `kafka apply`, `media ack`, `migrate` once the topic set and store options exist | `cmd/igsctl` |

**SQL:** no code sets `search_path` or qualifies a table name, so every statement in `persist`, `scheduler`, `store`, `gdpr`, `api`, `dispatch` and `stage/deps` resolves through the connection's `search_path`. igs's pool pins it to `ig` (ADR 0005 §4); `pg.Open` would also honour `?search_path=ig` in the DSN (`internal/platform/pg/pg.go:13`), but igs sets it in code so a DSN mistake cannot point it at fbs's tables. Advisory locks are per database, so `search_path` does not separate them (§2).

---

## 6. Platform, config, tools and deploy

| Package or area | Lines | Facebook or fbs-name coupling | Class | Seam or what igs writes |
|---|---|---|---|---|
| `platform/metrics` | 111 / 264 | namespace `fbs` in all four constructors (`metrics.go:21-39`); 51 metrics registered at `init` by any binary that imports `httpserver` or `kafka`; 4 names embed `fb_` (`:46-48,87`); 8 plain gauges always exported, `fbs_scheduler_leader` among them (`:88`); fbs's alerts select by name only (`alerts.yaml:35-39`) | parameterise | `UseNamespace(ns, renames)` at gather time (ADR 0004 §5); a per-binary "no `fbs_` line" test; `monitoring_test.go` (fixed to `deploy/helm/fbs/files` and `\bfbs_`, `:18-23`) made table-driven |
| `platform/httpserver` | 79 / 0 | serves `metrics.Registry` (`ops.go:39`) | parameterise | serve `metrics.Gatherer()` |
| `platform/kafka` | 749 / 40 | type header `fbs-type` (`producer.go:24`; copied by the retry relay, `worker.go:287`); `topic_prefix` is prepended to topics and groups (`consumer.go:110-119`) | parameterise | header name as a producer option (igs `igs-type`); fbs keeps `fbs-type` |
| `platform/schemareg` | 397 / 245 | temporary subjects `fbs-check-…` (`schemareg.go:333`, cosmetic) | reuse | igs passes its own file map |
| `platform/{breaker,config,leader,log,pg,redisx,run,s3x}` | 524 / 141 | none; keys, names and prefixes come from callers | reuse | — |
| `config` | 501 / 66 | `EnvPrefix "FBS_"`, `FBS_CONFIG` (`config.go:17,369,380`); embeds fbs's defaults; neutral types (`Proxy`, `AIMD`, `Browser`, `breaker.Config`) nested under `FB` (`:135-215`) | parameterise | shared sections to `internal/config/shared` with aliases; igs's root in `internal/ig/config` (ADR 0004 §6) |
| `registrysync` | 985 / 401 | lock `fbs:registry-sync` (`sync.go:183`); `fb_queries`, `registry_sync_runs`, `posts`; probes built on `fb/parse` | rewrite | `igs-registry-sync` (M9) copies the skeleton: lock and skipped status, report persistence, candidate lifecycle, metrics. Parameterising in place would tangle fbs's probes |
| `spike` | 3,665 / 1,595 | orchestration is generic in shape (steps, resume, `StepError`, smoke and cost estimate, report, verdict, bundle, secret scan, `Redact`, the strata CSV); the measurement is Facebook's (`spike.go`, `capture.go`, `har.go`, `trim.go`, `preflight.go`, `fixturecheck.go`) | split | M8: a `Measurer` interface the kit drives (fbs passes today's runner, igs its own), so the spike gaps below are fixed once. The probe (M4) is igs-only |
| `pilot` | 219 / 50 | sums `jobs.counters` by `page_schedule.tier` (`report.go:95-142`) | rewrite | igs's own once igs has a schedule (after M7) |
| `fakefb` (+ `cmd/fakefb`) | 1,749 / 20 | Facebook's wire format throughout; cookie `fbs_consent` | rewrite | `fakeig` copies the skeleton: seeded world and truth, faults, per-identity tracking, latency, admin endpoint, doc_id rotation, fixture writer |
| `e2e` | 343 / 1,953 | fbs stack and groups; `FBS_E2E_DEBUG`, `FBS_LOADTEST*`, `FBS_SOAK*`; asserts `fbs_*` metrics | rewrite | igs's stack copies `Start`/`Stack` and the restart helpers |
| `testenv` | 91 / 0 | creates `fbs_test_<rand>` and runs fbs's migrations (`testenv.go:34,49`); a random Valkey DB 1–14 with `FLUSHDB` (`:72-74`) | parameterise | `PostgresWith(t, migrate)`; igs's tests use key prefixes, because parallel runs on one Valkey can pick the same DB and flush each other |
| `cmd/fbs-*` | 1,203 / 0 | thin mains on `config.MustLoadFromFlags`, `plog.Setup("fbs-…")`, fixed groups and the leader key | rewrite | thin `cmd/igs-*` mains |
| `cmd/devs3` | 25 / 0 | none | reuse | — |
| `deploy/helm/fbs` | — | chart name and `app.kubernetes.io/name: fbs`; `FBS_CONFIG`, `/etc/fbs*`; image, buckets, secrets, commands and KEDA groups in `values.yaml`; alerts (42 `fbs_` references, `fbs:` rules) and dashboard (`fbs-overview`) included verbatim; hooks run `fbctl` | rewrite (M10) | a copied `deploy/helm/igs` keeps fbs's renders untouched; extract a library chart later. Object names come from the release name, so the two releases do not clash |
| `deploy/compose`, `deploy/docker` | — | Compose project `fbs` (container names used by `Makefile:41`, `chart-smoke.py`, `soak.yml`); `FBS_*` env; the Dockerfile builds all of `./cmd/...` (`:11`) | parameterise | igs's binaries ship in the same image automatically; an igs Compose file or profile (M10) |
| CI, Makefile, scripts, `phase0/` | — | `FBS_TEST_*` env; explicit integration package list (`ci.yml:82`, `Makefile:48`); chart-check and chart-smoke on the fbs chart; `phase0-dryrun` builds `fakefb` and `fbs-spike` (not in CI; `TestPhase0AgainstFakeFacebook` is) | parameterise | igs packages join the lists; igs chart and dryrun targets (M8, M10) |

**fbs Phase 0 kit gaps, confirmed at HEAD** (brief M8 fixes them in the shared kit):
- Latency is recorded (`spike.go:178,203,475`) but never aggregated; the report and the run estimate ignore `MS`.
- The depth check passes whenever no timeline ended at a wall (`report.go:423-427`), so timelines cut by `max_pages`, errors, blocks or drift all pass.
- The portability check's extra bootstrap (`spike.go:358`) emits no record, so its HTML load is missing from the report's cost (`report.go:293,307,388-397`), while the smoke estimate adds a flat 2 requests per page (`phase0.go:458`).

---

## 7. Contracts and data files

| Asset | Today | igs |
|---|---|---|
| `api/proto/securebrand/fb/v1` | public contract | untouched; igs adds `ig/v1` and `common/v1` (ADR 0005 §2) |
| `api/proto/securebrand/fbs/task/v1` | internal envelope | untouched; igs adds `igs/task/v1` |
| `migrations/` | one goose set, default schema | `migrations/ig/`, schema `ig`, `ig.goose_db_version` |
| `configs/default.yaml`, `topics.yaml`, `queries.seed.yaml` | fbs | `configs/ig/` with its own embed |
| `configs/fingerprints.yaml` | neutral | shared; gains `chrome_152` |

---

## 8. Known issues at HEAD

The brief's §5 list and the reference's known-issues list, re-checked against the code. All are **confirmed**.

| # | Issue | Evidence at HEAD | Milestone |
|---|---|---|---|
| 1 | `fbctl dlq replay` does nothing for 7 days: dead-lettering ends in `finish` → `MarkDone` (7-day TTL), and replay re-produces the same task id, which the worker skips. After 7 days the replayed task still carries its stall count, so its first failure dead-letters it again | `internal/crawl/stage/worker.go:64-68,105,178,187,232`; `checkpoint.go:80`; `cmd/fbctl/commands.go:229` | M2 §5.1 |
| 2 | The persister retries a failed batch every 2 s forever; an "undecodable record skipped" is in fact appended half-decoded before the error check; an empty media id blocks it the same way | `internal/persist/consumer.go:89-98,118-140` | M2 §5.2 |
| 3 | `GET`, `cancel` and `results` of `/v1/jobs/{id}` do not check the owner; only the list does | `internal/api/jobs.go:141,163-166,185,200`; `dispatch.go:275,320` | M2 §5.3 |
| 4 | Suppression is enforced only by the crawlers (a 60 s cache); the persister never reads it, and its page upsert sets `status = 'active'`, undoing the eraser's `suppressed` | `internal/persist/writer.go:403`; `internal/gdpr/erasure.go:254`, `suppression.go:108-123` | M2 §5.4 |
| 5 | `max_comments_per_post` counts per 20-page segment: `items_seen` lives in the segment's checkpoint and is not carried by `Continuation` or `PostScope` | `internal/crawl/stage/post.go:55,165,278`; `checkpoint.go:47`; `task.go:58-65` | M2 §5.5 |
| 6a | Detect rules are hard-coded although the code calls them config; every caller uses `DefaultRules()` | `internal/fb/detect/detect.go:59-61`; `internal/app/crawler.go:108` and four more | M2 §5.6 |
| 6b | HTTP 400, 401 and 403 classify as OK | `detect.go:113-126,160-177` | M2 §5.6 |
| 6c | The rate-limit cooldown is always 60 s: a second consecutive block retires the identity before the cooldown branch runs | `internal/fb/session/identity.go:139-155` | M2 §5.6 |
| 6d | One fingerprint profile per pod; no Chrome 152 profile | `pool.go:150`; `configs/fingerprints.yaml:7-17` | M2 §5.6 |
| — | Reply-thread failures do not stop later incremental runs from treating a post as complete (`ThreadsComplete` is set before the threads run) | `post.go:195,319`; `worker.go:229-231` | fbs only (igs has no replies) |
| — | `/jobs/{id}/results` misses posts unchanged during the job (`last_seen_at` is touched at most daily) | `internal/api/jobs.go:221`; `writer.go:101-102` | igs avoids it in M7 |
| — | Orphan media deletions publish no `media.deleted` event | `internal/media/janitor.go:204-208` | shared fix, M7 |
| — | With default retention, maintenance creates and drops about 24 old `comments` partitions every hour | `internal/scheduler/loops.go:285-303`; `configs/default.yaml:189,282` | igs avoids it by config (ADR 0005 §4) |
| — | doc_id discovery misses Relay `params:{id,…,name}`; render capture reads only POST bodies | `internal/fb/docids/docids.go:20-21`; `browser/render.go:464-480` | M9 |
| — | Hashtag ranges are sliced in runes while the scrubber keeps UTF-16 lengths | `internal/fb/parse/fields.go:211-213`; `fixtures/scrub.go:398-405` | fbs only (Instagram captions carry no ranges) |
| — | Variables templates lose their key order (map, then `jsonb`) | `internal/fb/registry/registry.go:82,93,383-392` | igs keeps template text if needed (M4) |
| — | `fb.bootstrap_attempts` is never read | `internal/config/config.go:150` | M2 (remove or wire) |
| — | Unused: `crawler.comments_page_size`, `freshness_p0/p1`, `kafka.compression` (zstd is fixed), the `retry.1h` topic (consumed, never produced), `Task.not_before`, `affinity`, `trace`, `CRAWL_MODE_REFRESH` | `config.go:103,230,235-236`; `producer.go:67`; `deps.go:154-161` | igs does not copy them |

## 9. New findings (not in the reference)

| Finding | Evidence | Where it goes |
|---|---|---|
| **A task of unknown kind crashes the crawler.** `Run` dead-letters it as `no_stage`, `deadLetter` calls `topics.Task`, which panics, and the consumer does not recover | `internal/crawl/stage/worker.go:56-58,213-216`; `internal/topics/topics.go:50` | M2 (shared defect); ADR 0005 makes igs's kinds disjoint so a misrouted task meets this path |
| `deadLetter` writes the unprefixed origin topic, `deadLetterRaw` the prefixed one | `worker.go:216,237` | M2, with the DLQ replay fix (`fbctl` copes with both, `commands.go:229`) |
| The HTML loader follows a same-site redirect to `/accounts/login` or `/challenge`, and page-load classification has no login marker, so an Instagram wall page could count as a successful load | `internal/fb/session/bootstrap.go:98-103,201-203`; `detect.go:160-173` | M4 (igs's wall paths and HTML markers) |
| GraphQL redirects to `/challenge` classify as a soft error, not a checkpoint (`/accounts/login` already matches `/login`) | `detect.go:108-125` | M4 |
| `SplitParts` already reads Instagram's NDJSON shape, whatever the content type | `internal/fb/graphql/parts.go:22-44`; `detect.go:183-190` | reuse as is |

## 10. Drift since the reference (`265ae3a`)

Eight fbs commits landed on main after the reference was written: `internal/fb/{session,htmlx,parse,browser,graphql}`, `internal/crawl/{stage,task}`, `internal/dispatch`, `internal/fakefb`, `internal/registrysync`, `internal/spike` (a new response-trimming experiment, `trim.go`) and a new `internal/pilot` (steady-state pilot report). The reference's chapters 1, 3, 4 and 6 have drifted there; every line in this matrix is at `df76ce1`.
