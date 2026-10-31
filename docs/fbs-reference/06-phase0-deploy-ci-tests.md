# 6. Phase 0 spike kit, deployment, CI and test strategy

[← Index](README.md)

**Purpose of this note.** This documents how the `fbs` repository (a self-hosted, logged-out scraper of public Facebook pages) measures Facebook before scaling up (Phase 0). It also covers how fbs is packaged and deployed, how CI works and how it is tested. It is the technical base for building a similar scraper for **Instagram**. Everything here was read from the source. File references use the form `path:line`. Anything marked **Suggestion** is a recommendation, not something the repo does.

Scope:
- `internal/spike/**`, `cmd/fbs-spike`, `phase0/**`, `docs/runbooks/phase0-spike.md`;
- `deploy/**` and `scripts/chart-smoke.py`;
- `.github/workflows/*`, `Makefile`, `.golangci.yml`, `go.mod`, `buf*.yaml`, `.dockerignore`, `.gitignore`;
- `internal/e2e/**`, `internal/testenv`;
- the go-live, load-testing and cost-tuning runbooks.

Client internals, parsers, crawl stages, and the API, persister and scheduler internals are covered in the other chapters of this reference. They appear here only where this scope touches them.

---

## 0. Key findings (TL;DR)

1. **Phase 0 runs as one CLI command on one machine, with no infrastructure.** `fbs-spike phase0` runs seven steps in order: preflight → docids → capture → smoke → run → report → bundle (`internal/spike/phase0.go:33-44`).
   - It writes one JSONL record per measurement event.
   - The output is a per-proxy-tier GO/NO-GO verdict with a monthly cost projection.
   - It also produces a scrubbed, credential-checked tarball to send back so doc_ids and parsers can be fixed.
2. **Go/no-go has four checks.** Default thresholds (`internal/spike/report.go:22`):
   - median comment completeness ≥ 0.70 (best ordering);
   - HTTP bootstrap success ≥ 0.90;
   - block rate < 5%;
   - median timeline depth ≥ `--min-days` (365), *or* no timeline hit a login wall.

   A tier is GO when all four pass, and Phase 0 is GO when any tier passes. The recommended tier is the passing tier with the lowest projected monthly cost (`report.go:193-217`).
3. **The cost projection is linear in measured bytes per proxied request.**
   - Formula: `$/month = (wire bytes / (GraphQL requests + page loads)) × req_per_day (16M default) × 30 / 1e9 × cost_per_gb_usd` (`report.go:387-397`).
   - Media downloads go over direct egress and are excluded.
4. **The run is resumable.** A page counts as done once its `page_done` record is in `run-<tier>.jsonl`; a rerun skips it (`pages.go:87-128`, `phase0.go:529-563`). Tiers are measured **one after another**, not in parallel, so two tiers take twice the wall time. The default is about 7 h per tier for 150 pages.
5. **One image serves every component.**
   - Distroless static binaries by default, plus a Debian+Chromium `browser` target (`deploy/docker/Dockerfile`).
   - Helm chart: one Deployment per component, KEDA ScaledObjects on Kafka consumer lag, PDBs, pre-install/upgrade hook Jobs (migrate, topics, schemas), an optional NetworkPolicy, PodMonitor, PrometheusRule (25 alerts, 14 recording rules) and a Grafana dashboard (43 panels in 6 rows).
6. **CI has three jobs.**
   - `test`: build, vet, protobuf regeneration and drift check, buf breaking check on PRs, a check that fixtures hold no real signed URLs, unit tests with `-race`, golangci-lint.
   - `integration`: real Postgres 18, Valkey 8 and Redpanda.
   - `deploy`: `make chart-check` with helm lint and kubeconform, Docker builds of both targets, **chart-smoke** (the chart's rendered workloads run in Docker against fakefb), and compose config validation.

   Releases on `v*` tags push two images and an OCI chart to GHCR. Soak runs by manual dispatch only; there is no cron schedule.
7. **The test pyramid rests on fakefb, a synthetic Facebook.**
   - The unit layer includes the whole Phase 0 kit against fakefb.
   - The e2e layer runs the whole pipeline in-process against real PG, Kafka and Valkey.
   - Chaos tests restart components at random. Opt-in load and soak tests compare the store exactly against fakefb's ground truth.
   - **For Instagram, the biggest single piece of new work is a `fakeig` with the same features:** ground truth, faults and rotated doc_ids.
8. **Gaps in the spike that an Instagram version should close:**
   - per-request latency (`ms`) is recorded but never aggregated in the report;
   - the portability check's second bootstrap is not counted in traffic or cost;
   - the spike's block-rate denominator includes `transport` errors, while the production recording rule excludes them;
   - the depth check passes timelines cut by `--timeline-pages` (`max_pages`), because only login walls fail it.

---

## 1. Phase 0 (the spike kit)

### 1.1 Goal

`docs/runbooks/phase0-spike.md:3-18` lists four questions, to be answered with real data before money goes into scale:
1. Does a plain HTTP client get a working logged-out session (bootstrap)?
2. How many comments does logged-out Facebook actually serve, per ordering?
3. Which proxy tier stays under a 5% block rate at production pacing, and what would it cost per month?
4. How far back do logged-out timelines go?

Phase 0 has two side goals:
- replace the **placeholder doc_ids** in `configs/queries.seed.yaml`, which match `^9000000000000\d{3}$` (`internal/fb/registry/registry.go:111`);
- capture **real response fixtures**, so the parsers can be checked against Facebook's current shapes.

Legal sign-off (ToS review, LIA, DPIA) is required *before* Phase 0, because it collects public comments by real people (`phase0-spike.md:20-22`).

Package doc: `internal/spike/spike.go:1-6`. It needs no Kafka, Postgres or Redis (`cmd/fbs-spike/main.go:1-3`).

### 1.2 Architecture of the kit

| Piece | Where | Role |
|---|---|---|
| `fbs-spike` CLI | `cmd/fbs-spike/main.go` | Flag parsing and subcommand dispatch; builds the real client `Stack`. |
| `spike.Stack` | `internal/spike/phase0.go:78-88` | Holds `BaseURL`, the proxy pool, the session pool, the HTML `Loader`, a direct-egress `HTTP` client (JS bundles, media), a `NewGQL(seed)` factory for GraphQL clients, and an optional `EchoClient` for tests. |
| `spike.Kit` | `phase0.go:91-125` | The `phase0` orchestrator: runs steps from `KitConfig.From` to the end. |
| `spike.Runner` | `spike.go:104-119` | Measures pages and writes `Record`s as JSONL. Also implements `Capture` and `DocIDs`. |
| `spike.Record` | `spike.go:56-101` | One JSONL line. `type` ∈ `run`, `page`, `request`, `bootstrap`, `timeline`, `comments`, `portability`, `media`, `page_done`. |
| Report | `report.go` | Aggregates records per tier: summary, strata, verdict, cost. |
| Preflight | `preflight.go` | Config, exit-IP and first-page checks. |
| HAR import | `har.go` | doc_ids, variable diffs and fixtures from a browser recording. |
| Fixture check | `fixturecheck.go` | Runs the production parsers over captured fixtures. |
| Registry helpers | `registry.go` | Merges discovered doc_ids, writes and reads registry YAML, refuses placeholders against real FB. |

How the real stack is wired (`cmd/fbs-spike/main.go:413-442`):
- **Fingerprints:** `configs/fingerprints.yaml`, loaded through `session.LoadFingerprints`.
- **Proxy pool:** `proxy.New(cfg.FB.Proxy)`. A `direct` tier always exists (`internal/fb/proxy/proxy.go:70-72`).
- **Session pool:**
  - `IdentityRPS: --rps` and `IdentityBurst: 1`;
  - `MaxRequestsMin/Max: 100000/100001`, so identities are effectively never retired by request count during a spike;
  - `MaxPerTier: concurrency*2+2`: each page holds one identity, plus briefly a second one for the portability check.
- **Loader:** `session.Loader` with `detect.DefaultRules()`.
- **HTTP:** `media.NewHTTPClient()`, direct egress.
- **GraphQL:** `graphql.Client` over a static registry built from the seed.

### 1.3 Input files

**`phase0/proxies.yaml`** (template: `phase0/proxies.example.yaml`).
- Handling:
  - git-ignored (`.gitignore:11-16`) and docker-ignored (`.dockerignore:7-10`);
  - preflight warns unless it is `chmod 600`;
  - never bundled.
- Structure: `fb.proxy.tiers[]`. Fields come from `internal/config/config.go:204-215`:
  - `name`;
  - `kind`: `static_list` | `gateway` | `direct`;
  - `endpoints[]` (static list: one exit per URL, credentials in the URL, for example `http://USER:PASS@203.0.113.10:8000`);
  - `gateway`, `username`, `password`;
  - `username_template`, with the placeholders `{user} {country} {session} {ttl_min}`, for example `{user}-country-{country}-session-{session}-sesstime-{ttl_min}`;
  - `sticky_ttl`, for example `10m`;
  - `country`;
  - `cost_per_gb_usd`: 0 for per-IP plans; for example 4.0 for residential.
- Optional overrides under `fb`: `fingerprint_profile: chrome_146`, `accept_language`, `locale` (`proxies.example.yaml:40-44`).
- `--config` accepts a comma-separated list of files, layered.

**`phase0/pages.csv`** (template: `phase0/pages.example.csv`; parser: `internal/spike/pages.go:31-70`). Two formats are accepted:
1. One page per line: a URL, a vanity name or `profile.php?id=`. Extra fields are ignored.
2. CSV whose header row starts with `page`, for example `page,size,category,region,language`. Every other column is a **label**: values are lower-cased, and the report breaks every metric down by each label column.

Lines starting with `#` and blank lines are skipped.

Recommended sample: about 150 pages, stratified as in `pages.example.csv:10-15`:
- size: small 40, medium 50, large 40, mega 20;
- category: brand 60, news 30, creator 30, government 15, other 15;
- region: US 60, EU 50 (cookie consent wall), APAC 40.

**Order matters.** The first rows feed doc_id discovery (3 pages), capture (2) and smoke (3), so put busy pages first (`phase0-spike.md:70`).

### 1.4 `fbs-spike` CLI

Usage header: `cmd/fbs-spike/main.go:5-16`. Subcommands: `phase0`, `preflight`, `docids`, `har`, `render`, `capture`, `check-fixtures`, `run`, `report`, `version`.

**Exit codes** (`main.go:53-59`): 0 OK/GO, 1 error, 2 usage, 3 NO-GO, 4 needs action (missing doc_ids, parser drift, smoke findings), 130 interrupted.

**Shared flags** (`main.go:84-113`):

| Flag | Default | Used by |
|---|---|---|
| `--config` | `$FBS_CONFIG` | all (comma-separated config files) |
| `--registry` | embedded seed | registry YAML to use instead of `configs/queries.seed.yaml` |
| `--tier` / `--tiers` | `direct` / — | single tier (run, capture, docids, render) / comma list (phase0, preflight) |
| `--pages` | — | pages file |
| `--out` / `--out-dir` | `spike-results.jsonl` / `phase0/out` | run / phase0 |
| `--in` | `spike-results.jsonl` | report (comma list) or HAR file (har) |
| `--json` | false | report, preflight, har, check-fixtures |
| `--page`, `--name`, `--dir`, `--fixtures-dir`, `--write-registry` | — / `sample` / `testdata/fb` / — / — | docids, capture, har, render |
| `--from` | `preflight` | phase0 first step |
| `--ip-echo-url` | `https://api.ipify.org?format=json` | preflight; empty skips the exit checks |
| `--resume` | true | run |
| `--rps` | 0.3 | requests/s per identity (the kit warns above 0.33: `phase0.go:192-194`) |
| `--req-per-day` | 16e6 | cost projection (`report.go:26`) |
| `--min-days` | 365 | depth threshold |
| `--timeline-pages` | 60 | about 600 posts (10 per request) |
| `--posts-per-page` | 3 | posts sampled for completeness |
| `--max-post-comments` | 300 | prefer posts at or under this many comments |
| `--comment-pages` | 40 | per post and ordering |
| `--threads-per-post` | 100 | reply threads followed |
| `--orderings` | `comments,comments_newest,comments_relevant` | the first is measured on every sampled post |
| `--compare-posts` | 1 | posts per page that also get the other orderings (0 = all) |
| `--media-samples` | 3 | CDN downloads per page |
| `--concurrency` | 4 | pages in parallel |
| `--max-bundles` | 150 | JS bundles scanned (docids) |

`PageTimeout` is fixed at 30 min per page (`main.go:248`).

**Subcommands.** `report`, `check-fixtures` and `har` run without building the network stack (`main.go:124-131`).

| Subcommand | Inputs | Outputs | Notes |
|---|---|---|---|
| `phase0` | `--config --pages --tiers [--out-dir --from --registry ...]` | `phase0/out/*` (see 1.5) | Builds a `Kit` (`main.go:257-287`). Exit codes: NeedsAction → 4, NO-GO → 3. Error text is passed through `Redact`. |
| `preflight` | `--tiers --page/--pages` | table or JSON | Exits 1 when any check FAILs (`main.go:149-160`). |
| `docids` | `--tier --page a,b` or the first 3 of `--pages`; `--max-bundles` | YAML summary (`registry_queries_found`, `missing`, `all_operations`); optional `--write-registry` | Exits 4 if a registry friendly name is missing (`main.go:193-224`). |
| `har` | `--in session.har [--registry] [--fixtures-dir --name] [--write-registry]` | Per query: FOUND/MISSING/KEPT lines, variables added, removed, set and not set, comment orderings | Exits 1 on a fixture write error, 4 if queries are missing (`main.go:331-411`). |
| `render` | `--tier --page [--write-registry]` | The GraphQL operations a headless-Chromium page load made (friendly name, doc_id, status, variable keys) | Needs Chromium (`fb.browser.binary`, `$FBS_BROWSER_BINARY` or PATH). A page load only requests the first timeline page, so comment and reply queries usually still need the HAR. Exits 4 when some are missing (`main.go:516-567`). |
| `capture` | `--tier --registry --page --name --dir` | Scrubbed fixtures under `dir/<query>/<YYYYMMDD>_<name>/` | Refuses placeholder doc_ids against real FB (`main.go:227-238`). |
| `check-fixtures` | `--dir` | PASS/FAIL per fixture | Exits 4 on any FAIL (`main.go:313-329`). |
| `run` | `--tier --registry --pages --out [--resume]` | Appends JSONL to `--out` | Resumes by default (`main.go:161-192`). Refuses placeholders. Takes the tier's `cost_per_gb_usd` from config. Exits 130 on interrupt. |
| `report` | `--in a.jsonl,b.jsonl [--json] [--min-days --req-per-day]` | Tier comparison and details | Exits 3 on NO-GO (`main.go:289-311`). |
| `version` | — | The build's `main.version` (ldflags) | |

### 1.5 The `phase0` orchestrator, step by step

`Kit.Run` (`phase0.go:159-229`):
- fills defaults: DocIDPages 3, CapturePages 2, SmokePages 3, MaxBundles 150, From `preflight`;
- validates `--from`, and requires tiers and pages;
- creates the output directory and runs `Steps[start:]`.

A step error of type `*StepError` with `NeedsAction` still writes the bundle before returning (`phase0.go:218-224`), so findings can always be sent back. Interrupting (Ctrl-C) is safe; resume with `--from run`.

Output files (`phase0.go:47-62`): `preflight.json`, `docids.json`, `registry.yaml`, `fixtures/`, `fixtures.json`, `failures/`, `smoke-<tier>.jsonl`, `run-<tier>.jsonl`, `report.txt`, `report.json`, `meta.json`, `phase0-bundle.tar.gz`.

| Step | Code | What it does | Stops (StepError) when |
|---|---|---|---|
| **preflight** | `phase0.go:251-271` → `preflight.go:119-171` | See 1.5.1. Uses `--registry` if given, otherwise the embedded seed, and the first page. | Any FAIL (not NeedsAction: exit 1). |
| **docids** | `phase0.go:273-309` | If `--registry` is given, skips discovery and only validates the registry. Otherwise loads the first 3 pages through `Tiers[0]` and scans the HTML and up to 150 JS bundles over **direct egress**. The scan is `internal/fb/docids`, with regexes for `__d("<Name>_facebookRelayOperation"... exports="<id>")` and `"<Name>Query",{queryID/id:"<id>"}`. Found ids are merged into the seed **by friendly name**; every query sharing a friendly name gets the same id (`registry.go:24-47`). Writes `docids.json` and `registry.yaml`. | Any registry friendly name is missing (NeedsAction). The message tells the user to record a HAR and rerun with `--registry ... --from capture`. |
| **capture** | `phase0.go:322-383` → `capture.go:27-101` | For the first 2 pages on `Tiers[0]`: bootstrap; write `page_html`; first `timeline` page (count 10); `comments` (ordering "comments") of the post with the most comments; `replies` of its first thread. Each response is scrubbed and written as a fixture with `meta.json` (page_id, post_id, parent_comment_id, source=capture). A response that comes back but is not OK is kept under `failures/<query>/<case>/`, with HTTP status, content type, class, reason, size, which tokens were found (booleans) and the **cookie names** the identity held (`capture.go:129-146`). `failures/` is cleared at the start of each capture (`phase0.go:330-334`). Then `CheckFixtures` runs the production parsers over everything under `fixtures/`. | A fixture does not parse (missing required fields, or 0 items) → "shapes differ, send the bundle". Or no timeline fixture at all (NeedsAction). |
| **smoke** | `phase0.go:477-527` | Per tier, sequentially: the first 3 pages at reduced limits (`smokeOptions`, `phase0.go:385-395`: timeline ≤ 10 pages, 1 post per page, first ordering only, ≤ 1 media sample, concurrency ≤ pages). Writes `smoke-<tier>.jsonl`, summarises it and prints an estimate of the full run (1.9). | Per tier: bootstrap success 0; block rate ≥ 50%; any timeline ended in `drift`; median posts 0. Problems from all tiers are joined into one NeedsAction error. |
| **run** | `phase0.go:529-563` | Per tier, **sequentially**: reads `run-<tier>.jsonl`, skips pages with a `page_done` record, appends the rest. Progress goes to the log. | Only on interrupt or ctx cancel. |
| **report** | `phase0.go:565-588` | `ReportAll` over every `run-<tier>.jsonl`; writes `report.txt` and `report.json`. | Never (NO-GO is a result, not an error). |
| **bundle** | `phase0.go:640-716` | See 1.11. | A secret, or a URL with credentials, found in a bundled file. |

Placeholder protection: `registry()` (`phase0.go:232-249`) and `CheckRegistry` (`registry.go:86-91`) refuse placeholder doc_ids when the base URL's host is `facebook.com` or `*.facebook.com` (`IsFacebook`, `registry.go:72-79`). No proxy traffic is spent on requests that cannot succeed. Against fakefb, placeholders are fine.

#### 1.5.1 Preflight checks (`preflight.go`)

| Check | Status logic |
|---|---|
| `registry` | WARN if the seed has placeholder doc_ids *and* the target is real FB; otherwise PASS with the query count (`:129-133`). |
| `config` | For each `--config` file: FAIL if stat fails; WARN if group or others can read it (`perm & 077`), with "chmod 600"; else PASS (`:134-146`). |
| `tier` | FAIL for an unknown tier; WARN for `direct` against real FB ("measures this machine's IP"); else PASS with the redacted endpoint (`:148-158`). |
| `exits` | Samples `Exits=3` endpoints of the tier. Each gets a fresh `http.Transport` with keep-alives disabled and **two** IP-echo requests (`:173-216`). FAIL if the two IPs differ (not sticky within a session); WARN if gateway sessions share an IP (`{session}` missing from `username_template`); PASS otherwise. SKIP when `--ip-echo-url` is empty. |
| `facebook` | Acquires an identity, `Loader.Load`s the first page, extracts tokens (`:218-259`). FAIL on a non-OK class or when the identity did not bootstrap ("no tokens"); WARN if any of `lsd`, `spin_r`, `hsi` is missing; PASS shows class, status, ms, wire size and whether the consent wall was shown. |

Every detail string goes through `Redact`, which replaces `scheme://user:pass@` with `scheme://***@` (`pages.go:130-134`). Tests check that no credential appears (`preflight_test.go:58-86`).

### 1.6 What the measurement does per page (`Runner.page`, `spike.go:184-311`)

`Runner.Run` (`spike.go:142-168`) emits a `run` header record (rps, concurrency, cost_per_gb_usd, version). It then measures pages with a semaphore of `Concurrency`, each page under `PageTimeout`.

Per page:
1. **`page` record** with the labels.
2. **Acquire an identity** on the tier, then **bootstrap**: `Loader.Load(page URL)`.
   - The `bootstrap` record carries `ok = class==OK && id.Bootstrapped()`, class, reason, status, decoded bytes, **wire bytes**, ms and the consent-wall flag.
   - It also carries `tokens_found`: booleans for `lsd`, `js_datr`, `spin_r`, `hsi`, `rev`, `haste_session`, `dtsg` (`spike.go:134-137`).
   - The page stops here if the bootstrap was not OK.
3. **Page id** from the HTML (`parse.Page`). If there is none: a `timeline` record with `end_reason: no_page_id`.
4. **Timeline depth:** pages of `timeline` (count 10, cursor) up to `MaxTimelinePages`.
   - Every GraphQL call emits a `request` record (`spike.go:170-182`) with query, class, reason, status, decoded bytes, wire bytes (in+out), ms and identity id.
   - End reasons: `end` (no next page), `wall` (LoginWall class), `blocked:<class>`, `drift` (parser report not OK), `error` (transport), `max_pages`.
   - `days_covered` is now minus the oldest post's time.
5. **Cursor portability** (`spike.go:313-337`): the middle cursor of the timeline is replayed on a **fresh identity**, after a bootstrap. OK means the class was OK and at least one post came back.
   - This measures whether cursors are tied to a session; if they are, tasks need session affinity (`SessionSnapshot`).
   - Note: this second bootstrap emits no `bootstrap` record, so its bytes are not counted.
6. **Comment completeness:** `SamplePosts` (`spike.go:455-474`) picks posts with comments.
   - Order: the busiest at or under `MaxPostComments` first, then the smallest of the bigger ones.
   - The first ordering runs on every sampled post. The other orderings run only on the first `ComparePosts` posts.
   - For each (post, ordering), `comments()` (`spike.go:339-427`) pages through comments up to `MaxCommentPages`. It then follows reply threads breadth-first, nested ones included, up to `ThreadsPerPost`, each up to `MaxCommentPages` pages. It de-duplicates by comment id and counts top-level and replies.
   - `reported` is the post's `comment_count` from the timeline.
   - `capped=true` when the spike's own limits cut the measurement, not Facebook.
7. **Media/CDN:** up to `MediaSamples` media URLs of the timeline's posts are downloaded **directly**, without a proxy (`spike.go:429-449`). The record holds status, bytes (capped at 50 MB), ms, OK (HTTP 200) and `expires_in_h`, the time until the signed URL's expiry.
8. **`page_done`**, emitted in a defer *unless* the context was cancelled (`spike.go:190-196`): a page interrupted by shutdown is measured again on resume, while one that hit PageTimeout counts as measured.

### 1.7 What the report computes (`summarize`, `report.go:233-431`)

| Metric | Definition |
|---|---|
| `pages` | Distinct pages with records. |
| `bootstrap_success` | OK bootstrap records ÷ bootstrap records. `tokens_found_rate` per token. `consent_walls` counts bootstraps with the consent flag. |
| `class_rate` | Share of `request` records per class. Classes are from `internal/fb/detect/detect.go:14-36`: `ok`, `rate_limited`, `login_wall`, `checkpoint`, `soft_error`, `registry_stale`, `schema_drift`, `content_unavailable`, `transport`. |
| `block_rate` | `rate_limited + login_wall + checkpoint` shares (`report.go:356`). The denominator is every GraphQL request **including transport errors**; page loads are not counted. |
| `timeline_end`, `median_posts`, `median_days` | Distribution of end reasons; medians over pages. |
| `completeness[ordering]` | Ratio = `min(1, collected/reported)`, or 1 if reported is 0. `median` and `p10` exclude capped measurements, unless every measurement was capped; `median_all` includes them (a lower bound); also `capped` and `walls` counts (`report.go:358-365`). `best_ordering` is the ordering with the highest median, ties broken by name. |
| `bytes_by_query` / `wire_bytes_by_query` | n, mean, p50 and p95 of decoded and on-the-wire sizes. Page loads count under `page_html`. |
| `cursor_portability` | OK portability records ÷ portability records. |
| `media_success`, `median_media_expiry_hours` | Direct CDN downloads. |
| Traffic | `wire_bytes` (bootstraps + GraphQL), `wire_bytes_per_request` = wire ÷ (requests + bootstraps), `requests_per_page`, `wire_bytes_per_page`, `run_cost_usd`, `projected_monthly_usd`. |
| `strata[dim]` | Per label value: pages, bootstrap success, block rate, median days, median completeness (the tier's best ordering) (`report.go:433-476`). |

**Latency gap:** request and bootstrap records carry `ms` (`spike.go:178`, `:205`), but `summarize` never reads `MS`, so the report has no latency figure. To get latency, post-process the JSONL. **Suggestion:** add p50/p95 latency per query and per tier in the Instagram spike.

Human format: `Format` (`report.go:501-574`) and `FormatAll` (`:577-607`). FormatAll starts with a tier table (`tier pages bootstrap blocks comments days $/month verdict`), then each tier's details, then `OVERALL: GO (recommended tier: X)`, `OVERALL: NO-GO` or `OVERALL: NO DATA`. Each failed check prints its advice line (`report.go:46-51`).

### 1.8 Verdict logic and thresholds

`report.go:399-427`:

| Check key | Pass condition | Default | Advice when it fails |
|---|---|---|---|
| `completeness` | best ordering's median ≥ `MinMedianCompleteness` | 0.70 | Compare orderings and walls. If even "All comments" is low, logged-out access hides comments: plan the browser fallback or another surface. |
| `bootstrap` | `bootstrap_success ≥ MinBootstrapSuccess` | 0.90 | Region table (EU consent wall), tokens found; try another fingerprint profile or tier. |
| `block_rate` | `block_rate < MaxBlockRate` | 0.05 | Lower `--rps`, add exits, or change tier. Mixing tiers via `fb.proxy.routing` is possible. |
| `depth` | `median_days ≥ MinMedianDays` **or** `timeline_end["wall"] == 0` | 365 (`--min-days`) | Set `limits.max_age_days` to the measured median, or accept partial backfills. |

- A tier's `Go` requires all four checks.
- `Result.Go` is true when any tier is Go.
- **Recommended tier** (`report.go:193-217`): if *every* passing tier has a `cost_per_gb_usd`, the passing tier with the lowest `ProjectedMonthlyUSD`; otherwise the passing tier with the lowest block rate.
- Tests confirm that a cheaper tier with poor completeness never wins (`report_test.go:52-100`).

Subtlety: the depth check passes when no timeline hit a wall, *even if timelines stopped at `max_pages`* (60 pages, about 600 posts). That is fine for depth-by-wall, but a high-volume page can show fewer than 365 days and still pass.

### 1.9 Cost and time formulas

**Report projection** (`report.go:387-397`):
```
proxied             = GraphQL request records + bootstrap records
wire_per_request    = total wire bytes (bootstrap + GraphQL, in+out) / proxied
run_cost_usd        = wire_bytes / 1e9 × cost_per_gb_usd
projected_monthly   = wire_per_request × requests_per_day × 30 / 1e9 × cost_per_gb_usd
                      (requests_per_day = --req-per-day, default 16e6 = README sizing)
```
Test example (`report_test.go:83-89`): 20 kB per request × 1M requests/day × 30 days = 600 GB; at $5/GB that is **$3000/month**.

**Smoke estimate of the full run** (`EstimateRun`, `phase0.go:409-466`):
```
t   = timeline requests per timeline in the smoke run (or full --timeline-pages if any smoke timeline hit max_pages)
pm  = (comment + reply requests) / comment measurements
compare = PostsPerPage if ComparePosts == 0 else min(ComparePosts, PostsPerPage)
measurements_per_page = PostsPerPage + (len(Orderings) − 1) × compare
requests_per_page = 1 (bootstrap) + t + 2 (portability: bootstrap + 1 timeline) + measurements_per_page × pm
hours     = pages × requests_per_page / (RPS × Concurrency) / 3600
wire      = pages × requests_per_page × (smoke wire bytes / smoke proxied requests)
cost_usd  = wire / 1e9 × cost_per_gb
```
The runbook's planning numbers (`phase0-spike.md:31-32`):
- about 200 requests per page, at 4 pages × 0.3 rps: 150 × 200 / 1.2 / 3600 ≈ **7 h per tier**;
- about 35 KB per request: 150 × 200 × 35 KB ≈ **1 GB per tier**, single-digit dollars at $4/GB.

The hours estimate ignores latency and bootstrap time, so it is a lower bound.

**Sanity check on production numbers.** README sizing (`README.md:203-211`): 300k pages/day incremental + 2k/day backfilled to 365 days gives ≈16M requests/day (≈190/s average, about 500 peak), 10–24 crawler pods, **≈570 GB/day proxy**, ≈1.7 TB/day media over direct egress and ~100M Postgres row writes/day.
- At about 35 KB per request and $4/GB, 570 GB/day comes to about **$68k/month**.
- The runbook's *illustrative* report line shows residential at $19,800/month, which implies about 10 KB per request. Both are placeholders until Phase 0 measures real bytes. Proxy bytes per request is the most important number Phase 0 produces.

### 1.10 Resumability

- `Done` (`pages.go:87-101`) scans a results file for `page_done` records and collects their normalised refs.
- `Remaining` (`pages.go:117-128`) drops pages whose normalised ref is done.
- Malformed lines are ignored.
- The `run` step and the standalone `run --resume` both append (`O_APPEND`) to the per-tier file.
- `TestPhase0AgainstFakeFacebook` reruns `--from run` and checks the file is byte-identical, meaning nothing was measured twice (`phase0_test.go:165-175`).

### 1.11 The bundle

`Kit.bundle` (`phase0.go:640-716`):
1. Writes `meta.json`: version, go, os, arch, start and end timestamps, base_url, tiers, page count, full Options, cost per GB, thresholds, and whether `--registry` was used.
2. **Re-scrubs fixtures:** every case under `fixtures/` and `failures/` that an older scrubber version left (current `scrubVersion = 3`, `internal/fixtures/scrub.go:64`), or that is raw, is scrubbed again (`scrubFixtures`, `phase0.go:608-634`).
3. **Allow-list** of bundled files (`bundleFile`, `phase0.go:593-602`): `preflight.json`, `docids.json`, `registry.yaml`, `fixtures.json`, `report.txt`, `report.json`, `meta.json`, `fixtures/**`, `failures/**`, `smoke-*.jsonl`, `run-*.jsonl`. Anything else, including a stray `proxies.yaml`, is left out.
4. **Secret scan:** every configured proxy username and password, including credentials embedded in endpoint or gateway URLs (`cmd/fbs-spike/main.go:464-476`), with length ≥ 6, must not appear in any bundled file. A URL with userinfo (`scheme://user:pass@`) must not appear in the kit's own outputs; Facebook content under `fixtures/` and the response bodies under `failures/` are exempt, but `failures/**/meta.json` is still checked. Either finding aborts with `ErrSecretInBundle`.
5. Writes `phase0-bundle.tar.gz`, with entries under `phase0/`.

What the scrubber removes (`phase0-spike.md:239-247`):
- people's names and ids;
- profile and `/people/` links;
- signed media URLs;
- the text people wrote. Letters and digits become `x`, and the length is kept so mention offsets stay valid.

The page's own name, links and text stay, as does structure: ids, permalinks, timestamps and counts. The scrubber descends into JSON nested inside strings. Fixture writing scrubs *before* the body reaches disk (`internal/fixtures/write.go`).

The bundle goes back privately ("never commit it; the repo is public"). A maintainer, or a Claude Code session, then fixes parsers, updates `configs/queries.seed.yaml`, sets pacing and routing defaults and regenerates goldens (`phase0-spike.md:249-255`). In a running deployment, doc_ids can be hot-updated with `PUT /v1/admin/queries/{name}`; crawlers reload within `fb.registry_refresh` (30 s) (`phase0-spike.md:257-265`).

### 1.12 HAR import (`har.go`)

The fallback when the bundle scan misses lazily loaded queries.

**Recording procedure** (`phase0-spike.md:131-170`):
1. Use a private window, logged out, with DevTools Network open, "Preserve log" ticked and the filter set to `graphql`.
2. Open a busy page and scroll **slowly**. The first real recording got one page of posts, then "Rate limit exceeded" (code 1675004, sent **with HTTP 200**) about 6 times a second.
3. Open a post with many comments; switch the filter to "All comments", then "Newest"; expand more comments and replies.
4. Save all as HAR. Recent Chrome versions save it sanitized, without cookies.

**Parsing** (`ReadHAR`, `har.go:78-108`):
- `POST .../api/graphql` entries with postData: the form fields `fb_api_req_friendly_name`, `doc_id`, `variables` (JSON), the response status and the body. Base64 bodies are decoded. When only `params` are present, values are percent-decoded.
- `GET` 200 HTML responses on facebook.com become pages.
- Headers and cookies are never read.

**Merging** (`ImportHAR`, `har.go:182-247`):
- **doc_id choice** per friendly name: the most frequent doc_id among requests.
- **Missing** means "not in the HAR *and* the registry has only a placeholder". A second HAR merged with `--registry` keeps earlier finds; those show as `KEPT` (`har.go:204-223`).

**Variable diff** per registry query (`compareQuery`, `har.go:270-347`):
- Requests are filtered to the query's `commentsIntentToken` ordering when the template has one. If no request used that ordering, all requests are used and `IntentMissing` is set; fixed values are then left alone.
- `Added`: observed variables not in the template.
- `Removed`: template variables not observed.
- `Set`: written into the template. A variable qualifies when every request sent it with the same value and the value is a "setting" (`setting`, `har.go:376-386`): null, a bool, a number with |n| < 1e6, or a string shorter than 40 characters containing no run of 6 or more digits, and not equal to a caller value (page id, feedback id, cursor).
- `Unset`: values that varied, conditional variables, ids, long strings and objects.
- Caller placeholders (`{{cursor}}`, `{{page_id}}`, ...) and the ordering token are never changed.
- This is how required flags such as `__relay_internal__pv__*` get into the registry.

**Fixtures** (`harFixtures`, `har.go:420-482`):
- `page_html` is the first HTML that `parse.Page` recognises.
- Per parser, the **largest successful response that has data** (`largest`, `har.go:487-495`): HTTP-200 rate-limit errors and empty last pages are rejected.
- Post and comment ids are decoded from the base64 `feedback:<post>_<comment>` id variable (`parse.FeedbackTarget`).

### 1.13 Dry run against fakefb

`make phase0-dryrun` runs `phase0/dryrun.sh`:
1. Builds `bin/fakefb` and `bin/fbs-spike`.
2. Starts `fakefb --addr 127.0.0.1:8090 --docids rotated --pages 5`.
3. Waits for the TCP port.
4. Runs:
   ```
   fbs-spike phase0 --config phase0/dryrun.yaml --pages phase0/dryrun-pages.csv --tiers direct \
     --out-dir phase0/out-dryrun --rps 20 --ip-echo-url ""
   ```

Details:
- `dryrun.yaml` sets `fb.base_url: http://127.0.0.1:8090` and `request_timeout: 10s`.
- `dryrun-pages.csv` holds `brand0..brand4` with size, category, region (us, eu, apac) and language labels.
- `--docids rotated` makes fakefb accept only ids rewritten as `"7316" + id[4:]` (`internal/fakefb/server.go:712-726`). The run **only passes if doc_id discovery works**: the seed placeholders are rejected.
- It must end `OVERALL: GO` in about a minute.
- It uses `--rps 20`, which triggers the >0.33 warning; that is fine against fakefb.

fakefb CLI flags (`cmd/fakefb/main.go:18-27`): `--seed`, `--pages`, `--posts`, `--max-comments`, `--rate-limit-every`, `--login-wall-after`, `--latency`, `--consent-wall`, `--docids seed|rotated`. Programmatic faults also include `Drift`, `DriftRequired`, `JSOnlyTokens`, `MediaTTL` and `FieldErrors` (`internal/fakefb/server.go:23-39`).

### 1.14 Spike tests (unit level, fakefb in-process via `internal/fb/fbtest`)

| Test | Covers |
|---|---|
| `TestPhase0AgainstFakeFacebook` (`phase0_test.go:115-176`) | Every step against rotated ids: GO on `direct`; registry has no placeholders and every id starts `7316`; bundle has all expected files and ≥ 8 fixture files; no credential in the bundle; resume is idempotent. |
| `TestPhase0StopsWhenDocIDsAreMissing` | An extra seed query `NoSuchQuery` gives a docids NeedsAction error, and the bundle still carries `docids.json`. |
| `TestPhase0RefusesPlaceholdersAgainstFacebook` | `ErrPlaceholderDocIDs` when the base URL is facebook.com. |
| `TestPhase0PreflightFailureStops` | Unknown tier gives a preflight error (not NeedsAction), and `preflight.json` is written. |
| `TestPhase0SmokeStopsWhenBlocked` | `RateLimitEvery: 1` gives a smoke error that mentions "blocked". |
| `TestPhase0CaptureKeepsRefusedResponses` | Refused responses are kept under `failures/` with status 200, `text/html`, class `rate_limited`, the `lsd` token flag, the `datr` cookie name and `__scrubbed`. The console shows `rate_limited fb error 1675004; http 200, text/html`. A later good capture clears `failures/`. |
| `TestBundleRefusesSecrets`, `TestBundleScrubsRawFixtures` | Secret, URL-credential, failures-meta and unknown-file rules; raw fixtures are re-scrubbed. |
| `TestEstimateRun` | Request-per-page arithmetic, including the capped timeline and compare-posts cases. |
| `TestSpikeAgainstFakeFacebook`, `TestSpikeReportsLoginWalls`, `TestSamplePosts` | Runner metrics; "relevant" ordering < 1; login walls fail the verdict; post sampling order. |
| `TestReportAllPerTier`, `...NoGo`, `TestRecommendedWithoutPricesUsesBlockRate`, `TestCappedMeasurementsAreLeftOut` | Verdict, recommended tier, strata, cost math, capped handling. |
| `TestHARImport`, `TestHARMergeSecondRecording`, `TestLargestSkipsErrors`, `TestFeedbackIDs` | HAR parsing (text and params, base64), variable set/unset rules, ordering fallback, merging, the fixture choice, no cookie leakage. |
| `TestPreflightExits`, `TestPreflightConfigChecks`, `TestPreflightFacebookLoad` | Sticky / not sticky / shared-exit cases; config permissions; placeholders allowed against fakefb; a login wall fails. |
| `TestReadPages`, `TestDoneAndRemaining`, `TestRedact`, `TestCheckFixtures`, `TestMergeDocIDs`, `TestScrubbedFixturesStillParse` | Input parsing, resume, redaction, and that the scrubber keeps fixtures parseable. |

---

## 2. Deployment

### 2.1 Docker images (`deploy/docker/Dockerfile`)

A multi-stage build:
- **`build`**: `golang:1.27-alpine` with `CGO_ENABLED=0 GOFLAGS=-trimpath GOTOOLCHAIN=local`. BuildKit cache mounts for the module and build caches. `go build -ldflags="-s -w -X main.version=${VERSION}" -o /out/ ./cmd/...` builds **every binary** in `cmd/`: `devs3`, `fakefb`, `fbctl`, `fbs-api`, `fbs-crawler`, `fbs-media-downloader`, `fbs-media-janitor`, `fbs-persister`, `fbs-registry-sync`, `fbs-scheduler`, `fbs-spike`.
- **`browser`** target (`:15-26`): `debian:12-slim` with `chromium`, `ca-certificates`, `fonts-liberation` and `tini`.
  - Runs as user 65532 (`nonroot`) with `FBS_BROWSER_BINARY=/usr/bin/chromium` and `HOME=/tmp`.
  - Entrypoint `tini -- fbs-crawler`: tini reaps Chromium's orphan processes under `docker run`. In Kubernetes the chart uses `shareProcessNamespace` instead.
- **Default (last) stage**: `gcr.io/distroless/static-debian12:nonroot`; entrypoint `fbs-api`; exposes 8080 (API) and 9090 (metrics and health).
- **`Dockerfile.prebuilt`**: packages host-built `bin/` (`make docker-prebuilt`) when the Go builder image cannot be pulled. Its own `.dockerignore` admits only `bin/`.
- **`.dockerignore`** excludes `.git`, `bin/`, test binaries, `deploy/helm`, `docs/` and the Phase 0 credential and result files.

### 2.2 Compose stack (`deploy/compose/docker-compose.yml`)

Project name `fbs`, so containers are named `fbs-<svc>-1` and the network `fbs_default`; chart-smoke and the Makefile rely on these names.

| Service | Image | Ports | Notes |
|---|---|---|---|
| postgres | `postgres:18-alpine` | 5432 | user, password and db all `fbs`; healthcheck `pg_isready`; volume `pgdata`. |
| redpanda | `redpandadata/redpanda:v24.2.7` | 19092 (external Kafka), 18081→8081 (Schema Registry) | dev-container mode, 1 core, 1 GB; internal listener `redpanda:9092`; healthcheck `rpk cluster health`. |
| valkey | `valkey/valkey:8-alpine` | 6379 | healthcheck `valkey-cli ping`. |
| s3 | the fbs image, `devs3 --addr :9000` | 9000 | In-memory S3 (gofakes3). Swap for MinIO, SeaweedFS or AWS via `FBS_S3__ENDPOINT`. |
| fakefb | the fbs image, `fakefb --addr :8090 --pages 5 --posts 30` | 8090 | Synthetic Facebook. |
| init | `fbctl migrate up` | — | Depends on postgres and redpanda being healthy. |
| topics | `fbctl kafka apply --partitions-cap 3 --replicas 1` | — | Depends on redpanda. |
| schemas | `fbctl schemas apply` | — | Depends on redpanda; not a dependency of the services. |
| api | `fbs-api` | 8080 | |
| scheduler, crawler (`--stages page,post,thread,retry`), persister, media-downloader, media-janitor, registry-sync | the fbs image | — | |

All app services share the `x-fbs` anchor:
- `restart: unless-stopped`;
- the config file `local.yaml` mounted at `/etc/fbs/local.yaml`;
- environment overrides `FBS_POSTGRES__DSN`, `FBS_REDIS__ADDR=valkey:6379`, `FBS_KAFKA__BROKERS=redpanda:9092`, `FBS_KAFKA__SCHEMA_REGISTRY__URL`, `FBS_S3__ENDPOINT=s3:9000`, `FBS_FB__BASE_URL=${FBS_FB_BASE_URL:-http://fakefb:8090}`;
- start only after `init` and `topics` complete successfully and valkey is healthy.

Config is koanf: YAML files, then environment variables `FBS_SECTION__KEY` (`__` becomes `.`) (`internal/platform/config`).

`local.yaml` uses fast pacing for fakefb only ("NEVER use these rates against real Facebook"):
- `fb.identity_rps: 20`, `identity_burst 5`, `page_rps 50`, `global_rps 200`, `capture_raw: failures`;
- `crawler.slots 48` with lanes p0 48, p1 48, p2 20 and `lane_min 4`;
- `registry_sync.interval 5m` with canary pages `brand0` and `brand1`.

### 2.3 Helm chart (`deploy/helm/fbs`)

`Chart.yaml`: `fbs`, version and appVersion 0.1.0. The release workflow overrides both with the tag.

**Values knobs and defaults** (`values.yaml`):

| Key | Default | Meaning |
|---|---|---|
| `image.repository` / `tag` / `browserTag` / `pullPolicy` | `ghcr.io/abdullah95k/fbs` / "" (= appVersion) / "" / IfNotPresent | `browserTag` switches the `browser: true` components to the Chromium image. |
| `imagePullSecrets` | [] | |
| `config` | see below | Rendered verbatim into the ConfigMap `config.yaml`. Any key of `configs/default.yaml` can be set. |
| `config.service.env`, `log` | prod; info/json | |
| `config.kafka.brokers` | `kafka-bootstrap.kafka:9092` | |
| `config.redis.addr` | `valkey-master.fbs:6379` | |
| `config.s3` | `s3.amazonaws.com`, ssl, eu-west-1, buckets `securebrand-fbs-media` / `securebrand-fbs-raw` | |
| `config.fb` | `base_url https://www.facebook.com`, `identity_rps 0.33`, `page_rps 3`, `global_rps 200` (AIMD's starting ceiling; `fb.aimd.max_rps` 500 caps it), `capture_raw: failures`, `proxy.routing: {default: [isp, residential], bootstrap_html: [residential]}`, `browser: {enabled: false, no_sandbox: true}` | Pods drop all capabilities, which leaves Chromium's sandbox unusable; hence `no_sandbox`. |
| `config.media` | `ttl 72h`, `required_consumers [analysis]` | |
| `config.registry_sync.canary_pages` | [] | At least 3 daily-posting pages with reply threads. |
| `secretEnv.existingSecret` | `fbs-env` | `envFrom` with DSNs, Redis password, S3 keys, HMAC key, Kafka SASL credentials. |
| `secretFiles.existingSecret` / `key` | `fbs-proxies` / `proxies.yaml` | Mounted at `/etc/fbs-secret/proxies.yaml` and layered over the ConfigMap via `FBS_CONFIG=/etc/fbs/config.yaml,/etc/fbs-secret/proxies.yaml` (`_helpers.tpl:19-31`). |
| `tlsFiles.existingSecret` / `mountPath` | "" / `/etc/fbs-tls` | CA and client certificates for Kafka and Redis TLS. |
| `podSecurityContext` | runAsNonRoot, uid 65532, fsGroup 65532 | |
| `securityContext` | no privilege escalation, `readOnlyRootFilesystem`, drop ALL | `/tmp` is an emptyDir. |
| `serviceMonitor.enabled` / `interval` | false / 30s | Renders a **PodMonitor**. |
| `alerts.enabled` / `labels` | false / {} | PrometheusRule. |
| `dashboards.enabled` / `labels` / `annotations` | false / `grafana_dashboard: "1"` / {} | Grafana sidecar ConfigMaps. |
| `keda.enabled` | **true** | |
| `keda.kafkaBootstrap`, `pollingInterval`, `cooldownPeriod` | `kafka-bootstrap.kafka:9092`, 15 s, 120 s | |
| `keda.authSecret` / `authParams` | "" / `[sasl, username, password]` | TriggerAuthentication for SASL/TLS. |
| `hooks.migrate` / `topics` / `topicReplicas` / `schemas` | true / true / 3 / false | |
| `api.service` | ClusterIP, port 80 | |
| `api.ingress` | disabled, host `fbs.example.com`, path `/v1` | |
| `networkPolicy.enabled` / `metricsFromNamespace` | false / `monitoring` | |

**Components** (`values.yaml:138-239`). Each enabled one becomes a Deployment.

| Component | Command | Replicas | Requests / limits | PDB | KEDA min–max | Lag triggers (group → topic: threshold) |
|---|---|---|---|---|---|---|
| api | `fbs-api` (port 8080) | 2 | 250m/256Mi; 512Mi | minAvailable 1 | — | — |
| scheduler | `fbs-scheduler` | 2 (leader-elected) | 100m/128Mi; 256Mi | minAvailable 1 | — | — |
| crawler-page | `fbs-crawler --stages page` (browser) | 2 | 1 CPU/1Gi; 2Gi | maxUnavailable 1 | 2–8 | p0 → `fbs.task.page.p0`: 20; p1: 200; p2: 500 |
| crawler-post | `--stages post` (browser) | 3 | 1/1Gi; 2Gi | maxUnavailable 1 | 3–12 | p0: 50; p1: 500; p2: 1000 |
| crawler-thread | `--stages thread` (browser) | 4 | 1/1Gi; 2Gi | maxUnavailable 1 | 4–16 | p0: 100; p1: 1000; p2: 2000 |
| crawler-retry | `--stages retry` | 1 | 100m/128Mi; 256Mi | — | — | — |
| persister | `fbs-persister` | 2 | 500m/512Mi; 1Gi | maxUnavailable 1 | 2–6 | `fbs-persister` → `fb.comment.v1`: 20000; `fb.post.v1`: 5000 |
| media-downloader | `fbs-media-downloader` | 4 | 500m/512Mi; 1Gi | maxUnavailable 1 | 4–12 | → `fbs.task.media`: 1000 |
| media-janitor | `fbs-media-janitor` | 1 | 100m/128Mi; 256Mi | — | — | — |
| registry-sync | `fbs-registry-sync` (browser) | 1 | 100m/256Mi; 1Gi | — | — | — |

Crawler pods get `terminationGracePeriodSeconds: 90`; everything else uses the default of 30.

**Templates:**
- **`deployments.yaml`** loops over components (`:1-104`).
  - `replicas` is omitted when KEDA owns scaling (`:13-15`); `revisionHistoryLimit: 5`.
  - Pod annotation `checksum/config` (sha256 of `.Values.config`), so a config change rolls the pods. Also `prometheus.io/scrape` with port 9090.
  - `shareProcessNamespace: true` for browser components when `browserTag` is set: the pause container reaps Chromium's orphans.
  - The image tag is `browserTag` for browser components, else `tag` or appVersion.
  - Ports: `metrics: 9090`, plus `http` when `port` is set.
  - Probes: readiness `GET /readyz` on metrics (10 s period, failureThreshold 3); liveness `GET /healthz` (20 s, 3). Both are served by `internal/platform/httpserver` on 9090.
  - Then: a PodDisruptionBudget when `pdb` is set, and a KEDA `ScaledObject` (`keda.sh/v1alpha1`) when `keda.enabled` and `autoscale` are set. The ScaledObject has one `kafka` trigger per lag entry, with `consumerGroup` and `topic` prefixed by `config.kafka.topic_prefix`, `lagThreshold`, `offsetResetPolicy: earliest` and an optional `authenticationRef` (`:74-102`).
- **`keda-auth.yaml`**: a `TriggerAuthentication` with one `secretTargetRef` per `authParams` key from `keda.authSecret`.
- **`hooks.yaml`**: Jobs `migrate` (`fbctl migrate up`), `topics` (`fbctl kafka apply --replicas N`) and `schemas` (`fbctl schemas apply`, BACKWARD_TRANSITIVE: an incompatible proto change fails the upgrade).
  - Annotations: `helm.sh/hook: pre-install,pre-upgrade`, weight -5, delete policy `before-hook-creation,hook-succeeded`, `backoffLimit: 3`, `restartPolicy: OnFailure`.
  - They mount a **separate hook ConfigMap** `-hook-config` (hook weight -10, `configmap.yaml:9-23`), because hooks run before the release's regular resources exist.
  - Hooks always use the non-browser image.
- **`configmap.yaml`**: `<release>-config` and `<release>-hook-config`, both holding `toYaml .Values.config`.
- **`service.yaml`**: Service `<release>-api` (http on 80 → container http, metrics on 9090), plus an optional Ingress on prefix `/v1`.
- **`networkpolicy.yaml`**: Ingress only.
  - Port 9090 from namespace `metricsFromNamespace`.
  - Port 8080 from any pod in any namespace.
  - Egress is deliberately left to operators: proxies for crawlers, CDN for the downloader.
- **`servicemonitor.yaml`**: actually a `PodMonitor` on port `metrics`.
- **`prometheusrule.yaml`**: embeds `files/alerts.yaml` **verbatim**, not rendered, because it contains Prometheus `{{ $value }}` templates.
- **`dashboards.yaml`**: one ConfigMap per `files/dashboards/*.json`.
- **`_helpers.tpl`**: fullname (release name truncated to 50), standard labels, a selector with `app.kubernetes.io/component`, config file list, env and envFrom, volumes and mounts.
- **`NOTES.txt`**: how to create an API key with `fbctl apikey create`, and a reminder to run Phase 0 and update the registry.

**Secrets layout:**
- `fbs-env` (env vars): `FBS_POSTGRES__DSN`, `FBS_POSTGRES__READ_DSN`, `FBS_REDIS__PASSWORD`, `FBS_KAFKA__SASL__USERNAME/PASSWORD`, `FBS_S3__ACCESS_KEY/SECRET_KEY`, `FBS_PRIVACY__AUTHOR_HMAC_KEY` (32+ random bytes);
- `fbs-proxies` (file `proxies.yaml`, the `fb.proxy.tiers` from Phase 0);
- optional `tlsFiles` and a KEDA auth Secret.

`go-live.md:52-82` documents all of them.

**Staging example** (`examples/fakefb.yaml`):
- the whole chart against fakefb with single-instance infrastructure: `redpanda:9092`, `valkey:6379`, `s3:9000`, `fakefb:8090`;
- `proxy.tiers: [{name: direct, kind: direct}]`; `secretFiles` disabled; `keda.enabled: false`; `topicReplicas: 1`;
- tiny resources, no PDBs, `registry-sync` disabled.

### 2.4 Alerts and recording rules (`files/alerts.yaml`)

Thresholds are "starting points: tune them against the Phase 0 baseline" (`alerts.yaml:3-4`). `internal/platform/metrics/monitoring_test.go` checks:
- every metric an alert or panel references exists in the registry;
- every recording rule is named `fbs:*`;
- every alert has a severity of critical, warning or info, a `for` duration, and summary, description and runbook_url annotations;
- every runbook anchor matches a `## ` heading in `docs/runbooks/operations.md`;
- there are at least 10 alerts;
- dashboard uids and titles are present.

**Recording rules** (group `fbs.recording`, every 30 s):
- `fbs:fb_requests:rate5m` (by query, class, tier);
- `fbs:block_ratio:rate5m`: blocked classes ÷ non-transport requests;
- `fbs:parse_drift_ratio:rate15m`;
- `fbs:parse_missing_per_response:rate1h`;
- `fbs:registry_stale_ratio:rate10m` (excluding `registry_sync:*` queries);
- the `fbs:leader:*` family: `aimd_ceiling_rps`, `fleet_rps`, `fleet_block_rate`, `schedule_lag_seconds`, `budget_projected`, `budget_limit`, `budget_backfill_paused`, `kafka_consumer_lag`. Each multiplies the gauge by `fbs_scheduler_leader == 1`, so a former leader's stale values do not count.
- `fbs:comment_completeness:p50_1h`.

**Alerts:**

| Group | Alert | Expression (abridged) | For | Severity | Meaning |
|---|---|---|---|---|---|
| facebook | FbsBlockRateHigh | `fbs:block_ratio:rate5m > 0.10` | 10m | warning | Blocks above 10% fleet-wide; AIMD should be lowering the ceiling. |
| | FbsBlockRateCritical | `> 0.25` | 10m | critical | A quarter blocked despite AIMD; pin the ceiling and reroute tiers. |
| | FbsCrawlersIdleWithBacklog | `sum(rate(fbs_tasks_total[10m])) == 0 and sum(lag of fbs-crawler-*) > 0` | 15m | critical | No tasks finished while tasks wait (pinned ceiling, breakers, budget). |
| | FbsQueryBreakerOpen | `max by(name)(fbs_breaker_state{name=~"query:.*"}) == 2` | 15m | warning | A (query, doc_id) keeps failing, so it is not sent. |
| | FbsProxyBucketOpen | `fbs_breaker_state{name=~"proxy:.*"} == 2` | 30m | warning | An exit bucket keeps failing its probes. |
| | FbsBrowserFallbackSaturated | `rate(fbs_bootstrap_total{method="browser",outcome=~"capped\|busy"}[15m]) > 0.05` | 30m | warning | More token-less pages than `max_share`/`max_concurrent` allow. |
| | FbsBrowserFallbackFailing | (error + no_tokens) ÷ (ok + error + no_tokens) for browser > 0.5 | 30m | warning | Chromium fails, or rendered pages carry no tokens either. |
| registry | FbsRegistryStale | `fbs:registry_stale_ratio:rate10m > 0.2` | 45m | warning | Active doc_id rejected and not replaced. |
| | FbsRegistryQueryFailing | `min by(query)(fbs_registry_query_healthy) == 0` | 5m | critical | Fails on the canaries with no replacement (renamed operation, or drift). |
| | FbsRegistrySyncNotRunning | `time() - max(last_success) > 3h and count(runs_total) > 0` | 10m | warning | Rotations are no longer followed. |
| | FbsSchemaDrift | `fbs:parse_drift_ratio:rate15m > 0.05` | 15m | critical | Required fields missing; raw failures in `raw-failures/<query>/`. |
| | FbsOptionalFieldDrift | missing-rate > 2× the rate a week ago + 0.05, or > 0.25 when never missing a week ago; volume > 0.05/s | 2h | warning | An optional field was silently renamed or dropped. |
| | FbsShadowParserDiffers | shadow outcome != same > 1% | 1h | info | The candidate parser disagrees. |
| pipeline | FbsSchedulerNoLeader | `max(fbs_scheduler_leader) < 1 or absent(...)` | 5m | critical | Nothing plans, finalises or runs AIMD and the budget. |
| | FbsOnDemandLag | p0 crawler group lag > 500 | 10m | warning | On-demand work waits. |
| | FbsPersisterLag | persister lag > 50000 | 15m | warning | Results reach Postgres late. |
| | FbsDLQGrowing | `increase(fbs_dlq_total[1h]) > 100` by stage and reason | 15m | warning | Replay with `fbctl dlq replay`. |
| | FbsJobsFailing | failed jobs ÷ all jobs (30m) > 10% | 30m | warning | |
| | FbsAPIErrors | 5xx ÷ all (5m) > 5% | 10m | warning | |
| coverage | FbsScheduleLag | `fbs:leader:schedule_lag_seconds > 6h` by tier | 30m | warning | Capacity or budget does not keep up. |
| | FbsCommentCompletenessLow | `fbs:comment_completeness:p50_1h < 0.8` | 2h | warning | Facebook serves fewer comments than it reports. |
| | FbsDeletionChecksSkipped | skipped_* ÷ all (6h) > 25% and > 20 checks | 1h | warning | Partial comment lists, so deletions go undetected. |
| | FbsBudgetOverrun | projected > 1.1 × limit (limit > 0) | 30m | warning | Even with backfill paused. |
| | FbsBackfillPausedLong | `fbs:leader:budget_backfill_paused == 1` | 12h | info | |
| | FbsMediaTTLExpiries | `increase(fbs_media_deleted_total{reason="ttl_expired"}[1h]) > 0` | 1h | warning | The analysis service is not acknowledging media. |

Every alert links to a section of `docs/runbooks/operations.md`, for example "Block storm", "Registry stale", "Browser fallback", "Schema drift", "Kafka lag", "Stuck jobs", "Schedule and tiers" and "Daily budget".

### 2.5 Dashboard (`files/dashboards/fbs-overview.json`)

"fbs / Overview": uid `fbs-overview`, 30 s refresh, last 6 h, one `datasource` variable. It has 43 panels in 6 rows (49 panel entries counting the rows):

1. **Facebook traffic**
   - Stats: Block ratio (5m; thresholds 5% orange, 10% red), AIMD ceiling, Fleet rps, Open breakers, Failing registry queries, Backfill paused.
   - Time series: Requests by class, Block ratio by tier (plus the fleet AIMD window), AIMD ceiling and fleet rate with adjustments, Requests by query, Request latency p95 by query (`fbs_fb_request_seconds`), Proxy bandwidth by tier and direction (`fbs_proxy_bytes_total`).
2. **Identities, breakers, registry and parsers**: Identities retired by reason, Live identities by tier, Bootstraps by method and outcome, Breakers, Short-circuited requests, Parse drift ratio, Registry query health, Registry checks, Promotions and sync runs, Optional fields missing per response (with the 7-days-ago comparison), Shadow parser outcomes.
3. **Pipeline**: Tasks by stage and outcome, Lane slots, Consumer lag, Retries and DLQ, Task duration p95, Persisted rows with batch p95.
4. **Jobs, schedule and coverage**: Jobs finished, Comment completeness p50/p10, Timelines truncated, Scheduled pages by tier, Schedule lag by tier, Post refreshes, Comment edits and deletions, Deletion checks.
5. **Daily budget**: Requests today (used, projected, limit), Bytes today, Backfill pause and stretch.
6. **Media and API**: Media downloads, Media deleted, API requests by code.

### 2.6 Release process (`.github/workflows/release.yml`)

Pushing a tag `vX.Y.Z` triggers the release, with permissions `contents: read` and `packages: write`. It:
1. Lower-cases the owner and computes version `X.Y.Z`.
2. Sets up buildx and logs in to GHCR with `GITHUB_TOKEN`.
3. `docker/build-push-action@v6` builds and pushes `ghcr.io/<owner>/fbs:X.Y.Z` (default target) and `...:X.Y.Z-browser` (`target: browser`), both with `VERSION` passed as a build arg.
4. `helm package deploy/helm/fbs --version X.Y.Z --app-version X.Y.Z`, then `helm push` to `oci://ghcr.io/<owner>/charts`. The chart's image tag then defaults to its appVersion.

go-live notes that the GHCR package must be made public, or the cluster needs `imagePullSecrets` (`go-live.md:50`).

### 2.7 `scripts/chart-smoke.py` (the chart's workloads in Docker)

Purpose: check what a cluster would run, without a cluster. Run as `make chart-smoke IMAGE=...` after `make infra-up`.
1. Waits for Redpanda health. Cleans up previous `fbs-smoke-*` containers, the `fbs_smoke` DB, `chartsmoke.*` topics and Redis DB 15. Warms Redpanda with an idempotent produce, because a fresh broker returns NOT_COORDINATOR for a while.
2. Runs `helm template` with the values file (default `examples/fakefb.yaml`) and overrides `image.repository/tag`, `pullPolicy=Never`, `config.kafka.topic_prefix=chartsmoke.` and `config.redis.db=15`.
3. Materialises each ConfigMap as a directory. Writes the `fbs-env` env-file: DSN to `fbs_smoke`, S3 keys and a random HMAC key. Starts `devs3` and `fakefb` (3 pages, 10 posts) on the compose network.
4. Converts each rendered pod spec into `docker run` (`docker_args`, `:64-97`): user and fsGroup, `--read-only`, `--cap-drop`, `no-new-privileges`, memory limit, env, ConfigMap mounts read-only, emptyDir as tmpfs, published ports. Any other volume type aborts.
5. Runs **hook Jobs** in order, with topics last, and each must exit 0. Then starts every Deployment and polls `/readyz` on 9090 until all are ready (120 s).
6. Creates an API key with `fbctl apikey create` and imports 3 pages with `fbctl pages import`. The import must report `created == 3`; a second import must report all as `existing`.
7. Waits up to `--timeout` (300 s) for jobs, then asserts:
   - all jobs `completed`;
   - posts > 0 and comments > 0;
   - media `ready` or `deleted`;
   - `GET /v1/pages` returns 200;
   - **no container logged `"level":"ERROR"`**.

   Prints PASS or FAIL.

It does not exercise Services, probes as Kubernetes runs them, KEDA or PDBs; kubeconform validates those objects' schemas.

---

## 3. CI

### 3.1 `.github/workflows/ci.yml`

Runs on push to `main` and on pull requests, with `contents: read`.

**Job `test`** (ubuntu-latest):
1. Checkout with full history (`fetch-depth: 0`); setup-go from `go.mod` (Go 1.27.2).
2. `go build ./...`, then `go vet ./...`.
3. **Generated code is up to date:** installs `protoc-gen-go@v1.36.5` and `buf@v1.47.2`, runs `buf lint` and `buf generate`, then `git diff --exit-code -- gen/`.
4. **Proto breaking changes** (PRs only): `buf breaking --against .git#branch=origin/<base>` when the base branch has a `buf.yaml`.
5. **Fixtures contain no real signed URLs:** `grep -rlE 'oh=00_|_nc_cat=' testdata/` must find nothing.
6. **Unit tests:** `go test -race -count=1 ./...` with `FBS_BROWSER_NO_SANDBOX=1`. Integration tests skip themselves without the `FBS_TEST_*` variables.
7. `golangci/golangci-lint-action@v8`, linter v2.15.0.

**Job `integration`**:
1. Service containers `postgres:18-alpine` (health-checked) and `valkey/valkey:8-alpine`. Redpanda v24.2.7 is started with `docker run` (ports 19092 and 18081), and the job waits for `rpk cluster health`.
2. Runs:
   ```
   go test -count=1 -timeout 20m ./internal/persist/... ./internal/e2e/... ./internal/spike/... ./internal/registrysync/... ./internal/platform/schemareg/...
   ```
   with `FBS_TEST_PG_DSN`, `FBS_TEST_REDIS_ADDR`, `FBS_TEST_KAFKA_BROKERS`, `FBS_TEST_SCHEMA_REGISTRY` and `FBS_BROWSER_NO_SANDBOX`. This includes the e2e tests and the chaos test; load and soak tests skip without `FBS_LOADTEST` and `FBS_SOAK`.

**Job `deploy`**:
1. setup-helm; install `kubeconform@v0.6.7`; `make chart-check`.
2. Two extra renders:
   - keda off, ingress and NetworkPolicy on;
   - alerts, dashboards and serviceMonitor on, asserting that `kind: PrometheusRule` and `fbs-overview.json` are present.
3. **Docker build** of the default target and of `--target browser`, then `chromium --version` inside the browser image.
4. **Chart smoke:** install python3-yaml if missing, `make infra-up`, then `make chart-smoke IMAGE=fbs:ci`.
5. **Compose config is valid:** `docker compose -f deploy/compose/docker-compose.yml config -q`.

`make chart-check` (`Makefile:80-86`):
- `helm lint` with the defaults, and with the fakefb example values;
- `kubeconform -strict -summary -kubernetes-version 1.30.0`, with schema locations from the defaults and from the datreeio CRDs-catalog (for KEDA, PodMonitor and PrometheusRule), over three renders: defaults; every optional object on (`keda.authSecret`, `tlsFiles`, alerts, dashboards, serviceMonitor, ingress, networkPolicy, `hooks.schemas`); and the fakefb example.

### 3.2 `release.yml`

See 2.6.

### 3.3 `soak.yml`

- Trigger: **`workflow_dispatch` only**, with no cron. Inputs:
  - `duration`, default 90m (`FBS_SOAK_DURATION`);
  - `pages`, default 20;
  - `restart_every`, default 2m.
- Timeout 360 minutes.
- Steps:
  1. `make infra-up` and wait for Redpanda.
  2. `make soak > soak.log` with `FBS_SOAK_OUT=soak.md` and `FBS_SOAK_HEAPPROFILE=heap`.
  3. Always: append `soak.md` to the step summary, show the last 60 non-INFO log lines, and run `go tool pprof -top -diff_base heap-base.pprof heap-end.pprof`.
  4. Upload the `soak.md`, `soak.log` and `heap-*.pprof` artifacts.

### 3.4 Makefile targets (`Makefile`)

| Target | What it does |
|---|---|
| `build`, `test`, `vet`, `lint` | `lint` runs vet, golangci-lint and buf lint when installed. |
| `generate` | `buf generate`. |
| `golden` | `go test ./internal/fb/parse -run TestGolden -update`. |
| `fixtures` | `go test -tags fixturegen ./internal/fakefb -run TestWriteFixtures`. |
| `infra-up` / `infra-down` | Compose postgres, redpanda and valkey; waits for `pg_isready`. `infra-down` runs `down -v`. |
| `test-integration` | `TEST_ENV` (`FBS_TEST_PG_DSN`, `REDIS_ADDR=localhost:6379`, `KAFKA_BROKERS=localhost:19092`, `SCHEMA_REGISTRY=http://localhost:18081`) plus `go test -count=1` on the integration packages. |
| `loadtest` | `FBS_LOADTEST=1 go test -v -timeout 45m -run TestLoad ./internal/e2e/`. |
| `soak` | `FBS_SOAK=1 go test -v -timeout 6h -run TestSoak ./internal/e2e/`. |
| `e2e` | `go test -v ./internal/e2e/...` with `TEST_ENV`. |
| `docker`, `docker-prebuilt` | Build the image. |
| `chart-check`, `chart-smoke` | See above. |
| `compose-up`, `compose-down` | |
| `spike-report` | `fbs-spike report --in spike-results.jsonl`. |
| `phase0` | Checks that `PHASE0_CONFIG` (default `phase0/proxies.yaml`) and `PHASE0_PAGES` (default `phase0/pages.csv`) exist, then `go run ./cmd/fbs-spike phase0 --config --pages --tiers $(PHASE0_TIERS) --out-dir $(PHASE0_OUT) $(PHASE0_ARGS)`. Defaults: tiers `isp,residential`, out `phase0/out`. |
| `phase0-dryrun` | `phase0/dryrun.sh`. |
| `spike-bin` | Static `fbs-spike` binaries for linux/amd64, linux/arm64, darwin/arm64 and darwin/amd64, with the version from `git describe`. |
| `clean` | `rm -rf bin/`. |

### 3.5 Lint, proto and ignore configuration

- **`.golangci.yml`** (v2):
  - `default: standard`, plus `bodyclose`, `errorlint`, `gosec`, `misspell`, `noctx` and `unconvert`;
  - gosec excludes G115 (integer conversion) and G404 (math/rand for jitter);
  - test files are exempt from gosec, noctx, bodyclose and errcheck;
  - `internal/(fakefb|e2e|testenv)/` is exempt from gosec;
  - the CLI, fixtures and spike paths are exempt from G301/302/304/306/703, because file paths are operator-supplied and fixtures are committed world-readable.
- **`buf.yaml`**: module `api/proto`; lint `STANDARD` except `PACKAGE_DIRECTORY_MATCH`; breaking rule `FILE`.
- **`buf.gen.yaml`**: local `protoc-gen-go` writing to `gen/go` with `paths=source_relative`.
- **`.gitignore`**: `/bin/`, `*.test`, `*.out`, `spike-results.jsonl`, coverage and `.env`. Everything in `/phase0/*` is ignored **except** the kit's own five files, so editor swap files or backups of `proxies.yaml` cannot be committed.

---

## 4. Test strategy

### 4.1 Layers

| Layer | How it runs | Infrastructure | What it proves |
|---|---|---|---|
| Unit | `make test` / `go test ./...` (CI: with `-race`) | none; fakefb in-process via `internal/fb/fbtest`; miniredis for rate-limit tests | Parsers (golden `expected.json` per fixture under `testdata/fb/<query>/<case>/`); the client layer (sessions, GraphQL, detect) against fakefb; the whole Phase 0 kit against rotated fakefb; scrubber; monitoring rules and dashboards referencing real metrics. |
| Integration | `make infra-up && make test-integration` | Postgres, Redpanda, Valkey | Persister, registry sync, Schema Registry, the spike, e2e. |
| End-to-end | `make e2e` (in `internal/e2e`) | Same, plus in-process gofakes3 and fakefb | The whole pipeline (API → dispatch → crawler stages → Kafka → persister → finalizer → API reads, media, erasure, AIMD). Exact comparison against fakefb's `TruthFor`. |
| Chaos | `TestChaosRestartsAndFaults` (part of integration) | Same | Random crawler, persister and finalizer restarts under rate limits and latency lose and duplicate nothing. |
| Load | `make loadtest` (opt-in, `FBS_LOADTEST=1`) | Same | The pipeline's own throughput ceilings. |
| Soak | `make soak` / `soak.yml` (opt-in, `FBS_SOAK=1`) | Same | Long scheduled runs with a mutating world and restarts; no leaks; exact final state. |
| Deployment | `make chart-check`, `make chart-smoke`, compose config | Docker + infrastructure | The chart renders, validates and actually runs as rendered. |
| Real world | Phase 0 (`make phase0`) | Proxies + real FB | Not in CI; the human-run go/no-go. |

### 4.2 `internal/testenv` (`testenv.go`)

- `Postgres(t)`: requires `FBS_TEST_PG_DSN` (an admin DSN), otherwise `t.Skip`. Creates a fresh database `fbs_test_<rand>`, runs `store.MigrateUp`, and on cleanup drops it `WITH (FORCE)`.
- `Redis(t)`: requires `FBS_TEST_REDIS_ADDR`. Picks a random DB from 1 to 14 and flushes it.
- `Kafka(t)`: requires `FBS_TEST_KAFKA_BROKERS`; returns the broker list.
- `TopicPrefix()`: a random `t<n>.` so tests sharing a broker never see each other's records. The prefix also namespaces consumer groups.

### 4.3 `internal/e2e/stack.go`: the in-process pipeline

`Start(t, Options{Spec, Faults, Config, CrawlerDrain})` (`stack.go:88-221`):
1. **Config** (`config.Load("")` defaults), with test overrides:
   - broker list and prefix, linger 1 ms;
   - identity 200 rps (burst 20), page and global rps 2000/2000/5000, timeout 10 s;
   - `YieldPages 2` (to exercise continuations), timeline page size 3;
   - slots 24 with lanes p0 24, p1 16, p2 8 and lane_min 2;
   - AIMD MinSamples 10, `capture_raw none`, finalize timeout 30 s, media TTL 72 h;
   - then `Options.Config`.
2. **fakefb** on `httptest` (default spec unless given). It accepts the seed doc_ids, with inline replies and faults.
3. **In-memory S3** (gofakes3 `s3mem`) on httptest; `s3x.Open` creates the buckets.
4. Applies the registry seed to Postgres. `store.EnsurePartitions` covers 14 months back and 1 ahead, plus daily partitions. Creates the Kafka topics from `configs/topics.yaml`, capped at 3 partitions with replication 1, and deletes them on cleanup.
5. Builds the components:
   - `app.NewCrawler` with stages page, post, thread and retry;
   - `dispatch.Dispatcher`, `scheduler.Adaptive`, `scheduler.Planner` (**not auto-ticked**: tests call `Planner.Tick`), `gdpr.Eraser`;
   - the media `Downloader` as a consumer of `fbs.task.media`, and the `Janitor` as a consumer of `fb.media.processed.v1`;
   - the API on httptest;
   - the AIMD controller ticking every second.
6. Components run in named **groups** (`start` and `stop`, `stack.go:223-246`), so tests can restart one part:
   - `RestartCrawler`: the drain timeout `CrawlerDrain` simulates a killed pod when it is short;
   - `RestartPersister`: a batch of 2000 rows, 100 ms flush;
   - `RestartFinalizer`: simulates a scheduler leader change; ticks every 200 ms.
7. Helpers: `Key(t, scopes...)` creates an API client key; `Do(t, key, method, path, body)` sends an HTTP request. `util.go` holds a nil-safe body reader.

### 4.4 What each e2e test exercises

| Test | File | Exercises |
|---|---|---|
| `TestEndToEnd` | `e2e_test.go:92-294` | Seven steps: (1) a full crawl via the API gives `completed`/`full`, with exact post and comment counts, completeness 1.0, the page profile stored and depth-2 replies; (2) the `fb.job.v1` event with completeness; (3) API reads, with author names only under `pii:read` and 401 for a bad key; (4) media is downloaded to S3, the TTL sweep expires an object without an ack, and acked media is deleted; (5) after a world mutation, an incremental rescrape picks up new posts and comments with far fewer requests; (6) author erasure redacts comments and survives a full rescrape (suppression); (7) politeness: no identity was paced faster than its configured rps allows. |
| `TestBlockedIdentitiesRetryAndTruncation` | `e2e_test.go:296-349` | `RateLimitEvery: 4`: tasks retry on other identities and collect everything. The AIMD Redis windows (`aimd:w:*`) hold every response fakefb served, and every rate limit. |
| `TestChaosRestartsAndFaults` | `chaos_test.go:25-152` | Setup: 3 pages, `RateLimitEvery 11`, 20 ms latency, page rps 5, crawler drain 20 ms. Up to 12 random restarts (crawler ≤ 6, persister ≤ 3, finalizer ≤ 3); the test fails if fewer than 3 crawler restarts happened. Checks: all jobs complete; counters never undercount; the store equals the truth as sets (no extras, no duplicates); nothing deleted or edited; completeness 1. A second round finds no deletions. |
| `TestCommentEditsAndDeletions` | `changes_test.go:27+` | Deletions (with replies) and edits are detected on a full rescrape and propagate to the store, the API and `fb.comment.v1`. A post whose comments Facebook *withholds* (a partial or empty list under the full count) is not marked deleted. |
| `TestRegistrySyncHealsRotation` | `registry_test.go:28-95` | Every doc_id is rotated in fakefb; `registrysync.Run` on a canary page promotes all of them; the admin endpoints show the run and checks; after the crawler's registry reload, the page is collected fully. |
| `TestConsumerLagGauges` | `registry_test.go:98-127` | The lag gauge reports the persister and `fbs-crawler-page-p0` groups, with the topic prefix stripped from labels. |
| `TestBrowserBootstrapCrawl` | `browser_test.go` | Skips without Chromium. With `JSOnlyTokens`, HTTP bootstraps fail and the browser fallback bootstraps; the page is collected fully, with at least one browser bootstrap counted. |
| `TestAdaptiveScheduleAndPostRefresh` | `schedule_test.go:37-144` | Registering a page without an interval: after the backfill the page is tiered by posting rate (8 posts 20 h apart gives T1 = 12 h, adaptive). Re-registering starts no second backfill. `post_refresh` rows exist for every post. New comments on a 2–6 day old post are collected by the next scheduled job via the post refresh. A budget stretch of 1.5 on T1 puts the next run about 18 h out (±11%). |
| `TestOnDemandAdmission` | `schedule_test.go:147-161` | With `MaxRunningP0Jobs = 1` and one job already running, a new `POST /v1/jobs` gets 429 with `Retry-After`. |
| `TestRegisterBackfillsPagesNeverRun` | `schedule_test.go:166-190` | A page registered with `backfill:false` gets its backfill on the next registration, and only then. |
| `TestLoadPipeline` | `load_test.go:135-246` | Opt-in. Submits a full job for every fakefb page at once (pages and posts from `FBS_LOADTEST_PAGES` and `_POSTS`). Uses 96 slots, AIMD off and huge rps. Samples each stage's consumer backlog every 2 s with `kadm.Lag`, with an optional CPU profile. Reports: jobs, wall time, fakefb requests/s, posts and comments stored/s, job duration p50/p95/max, persister batch mean, backlog peak and mean per stage. Fails unless everything is stored and all jobs complete. |
| `TestLoadPersister` | `load_test.go:315-384` | Opt-in. Produces `FBS_LOADTEST_ROWS` (200k) synthetic comments in batches of 5000 (200 per post), waits until `fbs_persist_rows_total` reaches them, verifies the count, then produces the same rows again (the no-op upsert path). Reports rows/s and batch statistics. |
| `TestSoak` | `soak_test.go:32-316` | Opt-in; detailed in 4.6. |

### 4.5 Load test results (`docs/runbooks/load-testing.md`)

Measured on a development container with 4 CPUs, every component in one process. These are a floor, not a target.

| Run | Wall | FB requests | Comments + replies | Job p50 / max |
|---|---|---|---|---|
| 30 pages × 25 posts, 0 latency | 35 s | 15,791 (456/s) | 109,205 (3,150/s) | 27 s / 32 s |
| 60 × 20, 40 ms | 51 s | 25,475 (499/s) | 176,870 (3,460/s) | 40 s / 45 s |
| 100 × 30, 100 ms | 2m30s | 64,600 (431/s) | 449,888 (3,000/s) | 145 s / 149 s |
| 200 × 30, 0 latency | 5m05s | 128,550 (422/s) | 894,080 (2,935/s) | 265 s / 280 s |

| Persister | Rows/s | Batches |
|---|---|---|
| 200k new (COPY into a temp table, then insert) | 15,400 | ~1,980 rows each, 67 ms mean |
| 200k again (no-op upsert) | 28,700 | |
| 1M new | 16,300 | 1,988 rows, 87 ms mean |
| 1M again | 25,700 | |

Backlog in the 200-page run (peak / mean records): page 46 / 2; post 2,313 / 302; thread **67,709 / 43,704**; persister 161,514 / 37,988.

CPU profile of the 200-page run: 573 CPU-s over 305 s.
- Crawler stages 43%, about **1.9 ms CPU per Facebook request**. The thread stage alone is 28%; comment parsing is 12.5%.
- fakefb 16%; GC 7%; persister 2.4% (about 15 µs per comment).

Conclusions (`load-testing.md:49-57`):
- The stages are CPU-bound in-process: latency did not lower the request rate.
- **The thread stage saturates first**, so give `crawler-thread` the most replicas; KEDA scales it on its lag.
- Crawler CPU does not limit production: 16M requests/day is about 185/s, which needs about 0.4 core fleet-wide. Size crawler pods for memory and slots instead.
- Persister bursts drain within seconds. About 100M rows/day is about 1,200 rows/s on average; one pod does 16k/s, about 13× headroom, so Postgres IOPS will be the limit first.
- "Completed" means queryable: finalisation waits for the persister's committed offsets.

### 4.6 Soak and chaos (`soak_test.go`, `load-testing.md:64-120`)

`TestSoak`:
- **Setup.** Pages (`FBS_SOAK_PAGES`, default 6) are registered with a 1 h schedule, then forced to 30 s intervals with `adaptive=false`. Faults are `RateLimitEvery 23` with 10 ms latency, and crawler drain is 50 ms. Short refresh windows (90 s) and recrawl ages make post refreshes and recrawls run.
- **Loop**, for `FBS_SOAK_DURATION` (default 20m):
  - ticks the Planner;
  - every 1–4 s mutates the world: 30% new posts and comments, 30% comments on older posts, 20% edits, 20% deletions (keeping at least 2 comments);
  - every `FBS_SOAK_RESTART_EVERY` (default 90s) restarts the crawler (50%), persister (25%) or finalizer (25%);
  - every 30 s samples goroutines, heap, Redis keys and job counts.
- **Fails** on any failed, partial or cancelled job, or a job running longer than 10 minutes.
- **End checks:**
  1. Disable schedules and drain.
  2. Run one full job per page.
  3. Every live comment is stored, visible and has its current text. Every comment the test deleted (and its replies) is `deleted`. Nothing else is missing.
  4. At least one edit is counted.
  5. The finalize set is empty and no `{job:*}:seen:*` keys remain.
  6. Goroutines are at most 1.5× the 2-minute baseline + 50.
- Heap profiles at 2 minutes and at the end when `FBS_SOAK_HEAPPROFILE` is set; a Markdown report via `FBS_SOAK_OUT`.

Recorded runs:
- **6 min** locally: 48 jobs; 133 changes, including 27 deletions and 25 edits; 8 restarts; goroutines 190–324; heap ≤ 66 MB.
- **90 min on GitHub** (run 40882655887, 20 pages, a restart every 2 min, 1h56m in total), passed:
  - 2,870 jobs, all completed; 2,024 world changes; 44 restarts;
  - 19,683 comments stored: all 18,910 live comments exact, and 773 marked deleted;
  - goroutines 300 → max 495 → 279 at the end.
- **30-minute memory run:** the live heap grew 64 → 114 MB, of which 47.6 MB was the in-memory S3 holding unacknowledged media. Nothing in fbs's own code grew.

Bugs these tests found, all fixed (`load-testing.md:115-120`):
- posts lost when a timeline held both a post and a share quoting it;
- abandoned tasks' offsets committed on drain timeout;
- the finalizer dropping jobs from Redis before finalising them;
- counters lost across retries;
- false `partial` jobs from a post refresh being reached again by the same job's timeline scan.

### 4.7 Go-live checklist (`docs/runbooks/go-live.md`)

1. **Legal and policy** (blocks go-live):
   - ToS review, LIA, DPIA;
   - `privacy.pii_mode`: `full`, readable only with `pii:read`, or `pseudonymized` (HMAC `author_key` only);
   - retention defaults: comments 365 d, inactive authors 365 d, post metrics 180 d, raw payloads 7 d, media rows 30 d, jobs 90 d;
   - an owner for erasure requests. Erasure also suppresses the subject in later scrapes.
2. **Proxies and Phase 0** (blocks real data):
   - buy static ISP and rotating residential (sticky) tiers;
   - `make phase0-dryrun` must end in GO;
   - fill in `proxies.yaml` (chmod 600) and about 150 stratified pages;
   - `make phase0 PHASE0_TIERS=isp,residential`;
   - send the bundle privately; it is used to set real doc_ids, fix parsers and set pacing, routing and budget defaults.
3. **Infrastructure:**
   - Kubernetes with KEDA, the Prometheus Operator, Grafana and an ingress with TLS;
   - Kafka with 3 brokers (Strimzi or managed; TLS and SASL PLAIN or SCRAM supported; MSK IAM is not; an optional Schema Registry);
   - managed Postgres 18 with PITR (about 100M row writes/day; monthly partitions; an optional read replica);
   - HA Valkey or Redis with persistence;
   - S3: a media bucket with a lifecycle rule at about 4 days, and a raw bucket at 7 days;
   - egress: crawlers and registry-sync to the proxies, the downloader directly to the CDN;
   - images: tag a release.
4. **Secrets and values:** `fbs-env` and `fbs-proxies`. Prod values: image tag (and browserTag), Kafka and KEDA auth, Redis TLS, `tlsFiles`, S3, `canary_pages` (≥ 3), `required_consumers`, `scheduler.budget` from the Phase 0 projection, ingress, alerts, dashboards and serviceMonitor on, `topicReplicas` 3, NetworkPolicy plus egress rules.
5. **Analysis service integration:** topics `fb.post.v1`, `fb.post_metrics.v1`, `fb.comment.v1` (edits as new versions, deletions as `COMMENT_STATUS_DELETED`), `fb.media.ready.v1` → acknowledge on `fb.media.processed.v1`, and `fb.job.v1`. The REST API serves the same data (`api/openapi/fbs.v1.yaml`).
6. **Deploy and ramp:**
   - staging on fakefb (or `make chart-smoke`), then the production install (the hooks migrate and create topics);
   - API keys;
   - a canary of 50–100 real pages, watching the dashboard for 2–3 days (block rate, completeness, proxy GB/day, schedule lag, deletion checks);
   - ramp 1k → 10k → 100k → all pages with `scheduler.budget` set. New pages backfill in the low-priority lane. Raise `fb.aimd.max_rps` only while the block rate stays low;
   - on-call via Alertmanager; review cost tuning after a week.

### 4.8 Cost tuning (`docs/runbooks/cost-tuning.md`)

Almost all running cost is proxy bandwidth, billed per GB on the wire. Media goes over direct egress; Postgres, Kafka and compute are comparatively flat.

**Measure:**
- GB per day per tier: `sum by (tier)(increase(fbs_proxy_bytes_total[1d]))/1e9`;
- bytes per request per query: `fbs_fb_response_bytes_sum / _count`, decoded;
- requests per day per query;
- the block ratio (blocked requests are paid for too);
- `fbs_budget_used` per lane.

**Levers, roughly in order of effect:**

| Setting | Default | Effect | Trade-off |
|---|---|---|---|
| `fb.proxy.routing` | default `[isp, residential]` | Send traffic to the cheapest tier that works; keep `bootstrap_html` on the reliable tier. | More blocks and retries. |
| `scheduler.tiers` | T0 3h … T3 72h, T4 7d | Rescrape frequency; most pages are T2/T3. | Freshness. |
| `scheduler.post_refresh` | 24h / 72h / 7d by age, `max_per_job 50`, `max_backoff 3` | Refreshes of older posts; an unchanged post costs 1 request and its interval doubles. | New comments on old posts arrive later. |
| `recrawl_at`, `recrawl_min_comments` | 7d, 30d; 20 comments | Full rereads of busy posts. | Late replies and deletions missed. |
| `limits.max_age_days` | 365 | Backfill depth: the biggest one-off cost per page. | Less history. |
| `limits.include_replies`, `max_depth` | on, 3 | Each reply thread costs requests. | Missing replies. |
| `crawler.timeline_page_size` / `comments_page_size` | 10 / 50 | Fewer requests for the same data. | Larger responses; possibly more blocks. |
| `fb.identity_max_requests_min/max` | 150 / 400 | Amortises the bootstrap (a page load of hundreds of KB). | Identities are easier to rate-limit. |
| `scheduler.budget` | unlimited | A hard daily cap in requests and/or bytes: pauses backfill, then stretches T4, T3, T2. On-demand work is never held. | Backfills wait. |
| `fb.browser.max_share` | 2% | Browser bootstraps load whole pages. | JS-only pages fail more. |
| `fb.capture_raw` | `failures` | `all` writes every response to S3 (storage cost, not proxy). | — |
| `limits.video_policy` | `sd` (`thumbnail` for backfills) | Media egress and storage. | Media quality. |

**Tuning loop:**
1. Find the biggest consumer: by tier, then query, then lane.
2. Pick the matching lever.
3. Change one setting at a time and compare day over day, keeping completeness and schedule lag in view.
4. Then set `scheduler.budget.bytes_per_day` slightly above the steady state.

**Sizing** (README, estimates until Phase 0): ≈16M requests/day (≈190/s average, ~500 peak), 10–24 crawler pods, ≈570 GB/day proxy, ≈1.7 TB/day media, ~100M PG row writes/day.

---

## 5. Key third-party dependencies (`go.mod`)

| Module | Version | Role in fbs |
|---|---|---|
| `github.com/bogdanfinn/tls-client` (+ `fhttp`, and indirectly `utls`, `quic-go-utls`) | v1.17.1 / v0.7.2 | **Browser TLS/HTTP2 fingerprinting.** `internal/fb/transport` uses `profiles.MappedTLSClients[profile]` (for example `chrome_146`, from `fb.fingerprint_profile` and `configs/fingerprints.yaml`) and sets the header order via `fhttp.HeaderOrderKey`. It also meters bytes for the cost metrics. |
| `github.com/twmb/franz-go` (+ `kadm`, `kmsg`) | v1.22.2 | **Kafka client**: the producer and consumer (`internal/platform/kafka`), topic admin and consumer lag (`kadm`: `fbctl kafka apply`, lag gauges, test backlog sampling). |
| `github.com/jackc/pgx/v5` | v5.11.0 | **Postgres** driver and pool (store, API, scheduler, persister COPY, testenv). |
| `github.com/pressly/goose/v3` | v3.29.0 | Migrations (`internal/store`, `fbctl migrate`). |
| `github.com/redis/go-redis/v9` | v9.23.0 | **Valkey/Redis** client: rate limits, AIMD windows, job accounting, checkpoints, leases. |
| `github.com/go-redis/redis_rate/v10` | v10.0.1 | GCRA rate limiting on Redis (API, `internal/fb/ratelimit`). |
| `golang.org/x/time` | v0.16.0 | `rate.Limiter`: per-identity pacing in `internal/fb/session`. |
| `google.golang.org/protobuf` | v1.36.12 | Kafka record contracts (`api/proto` → `gen/go/securebrand/fb/v1`), generated with buf. |
| `github.com/tidwall/gjson` | v1.20.0 | Fast JSON path extraction in the parsers, GraphQL splitting and token extraction. |
| `golang.org/x/net` | v0.62.0 | `websocket`, for a **hand-written minimal CDP client** (`internal/fb/browser/cdp.go`) that drives headless Chromium. There is **no** chromedp or rod dependency. |
| `github.com/minio/minio-go/v7` | v7.4.1 | S3 client (media, raw sink, replay, GDPR erasure). |
| `github.com/johannesboyne/gofakes3` | v1.2.0 | In-memory S3: `cmd/devs3` and e2e. |
| `github.com/klauspost/compress` | v1.20.3 | zstd for raw-response capture to S3 (`internal/crawl/rawsink`, `internal/replay`). |
| `github.com/zeebo/xxh3` | v1.1.0 | 128-bit content and event hashes for change detection and dedup (`internal/crawl/emit`). |
| `github.com/go-chi/chi/v5` | v5.5.0 | HTTP router for the API. |
| `github.com/google/uuid` | v1.6.0 | Job, erasure and media ids. |
| `github.com/knadh/koanf/v2` (+ yaml parser, file, env, rawbytes providers) | v2.4.1 | Layered config: embedded defaults → YAML files → `FBS_SECTION__KEY` env vars. |
| `github.com/prometheus/client_golang` / `client_model` | v1.25.0 / v0.6.3 | Metrics; `client_model` is used in tests to read the registry. |
| `github.com/alicebob/miniredis/v2` | v2.40.0 | Test-only Redis (AIMD and budget unit tests). |
| `gopkg.in/yaml.v3` | v3.0.1 | Registry, fingerprints, topics and the spike's registry writer. |

The toolchain is Go 1.27.2; the Docker builder is `golang:1.27-alpine`.

---

## 6. Instagram adaptation notes

> Everything in this section is a **suggestion**. Instagram specifics (endpoints, headers, error shapes, limits) are stated as hypotheses to verify in Instagram's own Phase 0, not as facts from this repo.

### 6.1 What can be cloned nearly as-is

- **Phase 0 orchestration pattern.** Copy `internal/spike/phase0.go` almost verbatim:
  - the step machine (`Steps`, `--from`, `StepError{NeedsAction}`, "bundle even on action needed");
  - per-tier JSONL files with `page_done` resume;
  - the smoke-then-estimate step;
  - the bundle with an allow-list, a secret scan and a re-scrub;
  - the exit-code contract (0/1/2/3/4/130).
- The **report engine** (`report.go`) is mostly platform-neutral:
  - thresholds and advice, `Summary`, strata by CSV labels, quantiles;
  - the cost projection and the recommended-tier logic.

  Rename the queries and orderings.
- **Preflight** exit checks (stickiness, distinct exits, config permissions, placeholder refusal) carry over unchanged. Only the "first page load" check needs Instagram token rules.
- **Proxy tier model and `proxies.yaml`**: `static_list`, `gateway` with `username_template`, `direct`, `cost_per_gb_usd`. The tier and identity abstractions are platform-agnostic.
- **The HAR import algorithm**: most-frequent doc_id per friendly name; the add/remove/set/unset variable diff; the "setting" heuristic; the largest-data fixture choice; merging a second HAR with `--registry`. Only the request filter and the form-field names change (see 6.2).
- **Deployment**:
  - the single multi-binary image plus a `browser` target;
  - the compose stack (Postgres 18, Redpanda, Valkey, devs3, fake server);
  - the Helm chart structure: components map, KEDA lag triggers, PDBs, hook Jobs with a hook ConfigMap, `secretEnv`/`secretFiles` layering, the NetworkPolicy, PodMonitor, PrometheusRule from a verbatim file and dashboard ConfigMaps;
  - `chart-smoke.py`, which is generic except for page names, the Postgres tables it queries and the fbctl commands;
  - the release workflow.

  Rename `fbs` to, for example, `igs`.
- **CI**: all three jobs. Keep in particular the protobuf drift check, buf breaking, the "fixtures contain no real signed URLs" grep (Instagram's CDN uses the same Meta `oh=`/`oe=`/`_nc_*` signature parameters), chart-check with kubeconform, and chart-smoke.
- **Test harness pattern**:
  - `testenv` (per-test DB, Redis DB, topic prefix; skip without env);
  - the e2e `Stack` with restartable component groups;
  - truth-based exact assertions;
  - chaos, load and soak tests with Markdown reports and heap profiles;
  - `monitoring_test.go`, which checks that alerts and dashboards reference only existing metrics and runbook anchors.
- **Alerts and dashboard**: most rules are pipeline-generic (lag, DLQ, leader, budget, drift, breakers, registry stale). Re-prefix the metrics and recalibrate the thresholds from Instagram's Phase 0.

### 6.2 What must be redone for Instagram

1. **A fake Instagram server (`fakeig`).** This is the biggest prerequisite, because every test layer depends on fakefb. It needs:
   - a seeded **world** of profiles, posts (image, carousel, reel/video), comments and replies (Instagram comments are a single reply level, unlike Facebook's depth up to 3), with edit, delete and mutate methods and `TruthFor()` ground truth;
   - HTML profile and post pages carrying the tokens and config a logged-out web client needs;
   - JS bundles under a static path declaring relay operations (for discovery);
   - GraphQL endpoint(s) that accept a set of doc_ids, plus `RotatedDocs` and `RotateDoc` to simulate rotation;
   - any REST endpoints the web client uses;
   - **faults** mirroring `fakefb.Faults`: `RateLimitEvery`, `LoginWallAfter`, `Latency`, `ConsentWall`, `Drift`/`DriftRequired`, `JSOnlyTokens`, `MediaTTL`, and Instagram-specific ones such as a login redirect or an HTTP 429 shape;
   - `Stats()`: per-identity minimum interval and per-query counts, for politeness assertions;
   - a CLI with the same flags, including `--docids rotated`.
2. **Doc_id discovery.**
   - `internal/fb/docids` hard-codes the `_facebookRelayOperation` module suffix and `rsrc.php` bundle URLs.
   - **Hypothesis to verify:** Instagram's web bundles declare `<Name>_instagramRelayOperation` modules (for example `Polaris*Query`) on `static.cdninstagram.com`.
   - Parameterise the regexes and the CDN host allow-list.
   - The `render` (Chromium) path and the HAR fallback stay. The HAR filter must accept Instagram's GraphQL POST path(s) (to verify: `/graphql/query` and/or `/api/graphql`) and, if the web client uses REST (`/api/v1/...`) for some data, capture those as "operations" too.
   - `IsFacebook` becomes `IsInstagram` (`registry.go:72-79`), so placeholder refusal and the HAR page filter keep working.
3. **Bootstrap and token rules.**
   - The spike reports `lsd`, `js_datr`, `spin_r`, `hsi`, `rev`, `haste_session` and `dtsg` (`spike.go:134-137`), and preflight requires `lsd`, `spin_r` and `hsi` (`preflight.go:240`).
   - Instagram needs its own token set. **To verify:** the `csrftoken` cookie, the web app id header value, `lsd`, `X-ASBD-ID`, the `mid` and `ig_did` cookies, and whether any are only set after JS runs (which would make the browser fallback mandatory).
4. **Block signals (`detect.Rules`).** Facebook sends rate limits as code 1675004 **with HTTP 200**. Candidate Instagram signals to verify and encode:
   - HTTP 429;
   - HTTP 401/403 JSON with `require_login: true` or a "Please wait a few minutes" message;
   - 302 redirects to `/accounts/login/`;
   - `checkpoint_required` / `challenge_required`;
   - `feedback_required` / `spam: true`;
   - an HTML login wall served in place of JSON.

   Map them onto the same classes (`rate_limited`, `login_wall`, `checkpoint`, …) so the report, AIMD and alerts keep working.
5. **Measurement flow (`Runner.page`).** Replace the Facebook sequence (page id → timeline → comments per ordering → replies) with Instagram's:
   - profile load gives the user id and post count;
   - profile posts pagination;
   - per sampled post: comments pagination, then child comments (replies);
   - media CDN sampling.

   The "orderings" dimension may not exist logged-out on Instagram. Keep the abstraction (a list of query variants), but expect one. `reported` = the post's comment count.
6. **Parsers, fixture checks and scrubber.**
   - `fixturecheck.go` dispatches on `page_html`, `timeline`, `comments` and `replies`; new keys are needed (for example `profile_html`, `profile_info`, `profile_posts`, `post_comments`, `child_comments`).
   - The scrubber (`internal/fixtures/scrub.go`) must learn Instagram's person fields: `username`, `full_name`, `profile_pic_url`, `pk`/`id` of commenters, comment `text`, captions of other users, tagged users. The profile's own identity must be kept.
7. **Pages file → profiles file.** Suggested header: `profile,size,category,region,language,account_type`, where `profile` is a username, `@username` or `https://www.instagram.com/<username>/`, and `size` is by followers. The `ReadPages` CSV code works as-is if the first column is renamed (it checks the header with `strings.EqualFold(rec[0], "page")`, `pages.go:49`). `task.NormalizeRef` and `RefPath` need an Instagram version.
8. **Cost constants.** `DefaultRequestsPerDay = 16e6` is Facebook's README sizing. Instagram needs its own sizing from profiles × posts × comments per day and the measured requests per profile.
9. **Fixes to carry over** (from section 0):
   - aggregate latency;
   - record and count the portability bootstrap;
   - decide whether transport errors belong in the block-rate denominator, consistently with the production recording rule;
   - make the depth check report `max_pages`-limited pages separately.

### 6.3 Suggested Phase 0 plan for Instagram

1. **Week 0: legal.** ToS review for Instagram, LIA and DPIA, as in `go-live.md` §1. The data is commenters' personal data.
2. **Build `fakeig` and an `igs-spike` dry run**, a copy of `phase0/dryrun.sh`. It must reach `OVERALL: GO` with rotated doc_ids before any proxy money is spent, and becomes a CI unit test (like `TestPhase0AgainstFakeFacebook`).
3. **Manual reconnaissance in one session.** Record two or three HARs in a logged-out private window:
   - one profile scrolled slowly;
   - one post with many comments, with replies expanded.

   Run `igs-spike har` to produce the first registry and fixtures and to learn the variable shapes and required provider flags. Also note at which scroll depth Instagram shows the login wall: this calibrates `--timeline-pages` and the depth threshold.
4. **Inputs:**
   - `proxies.yaml` with at least two tiers (ISP and sticky residential), with prices;
   - `profiles.csv` with about 150 public profiles stratified by size (nano, micro, mid, macro, mega), category (brand, news, creator, government) and region (US, EU consent, APAC).

   Put busy profiles first, because the first rows drive discovery, capture and smoke.
5. **Run the steps:**
   - **preflight**: exits plus a profile page load with Instagram token rules;
   - **docids**: bundle scan, then `render`, then HAR as needed;
   - **capture**: profile HTML, profile posts page, comments page, child-comments page, plus `failures/` with status, content type, class and cookie names;
   - **smoke**: 3 profiles per tier, with the estimate;
   - **run**: resumable, per tier;
   - **report**;
   - **bundle**.
6. **Measure, per tier and stratum:**
   - bootstrap success and tokens found;
   - block rate split by signal (429, login redirect, `require_login`, checkpoint);
   - **posts reachable logged-out** before a wall (depth in posts and days);
   - comment completeness (collected ÷ reported) and child-comment coverage;
   - bytes per request on the wire per query, and the projected $/month;
   - **latency p50/p95** (new);
   - cursor portability (do cursors survive a fresh identity?);
   - CDN success and the signed URL's expiry (sets media downloader urgency);
   - EU consent-wall incidence.
7. **Pacing sweep** (suggestion). Instagram is widely reported to throttle logged-out traffic per IP aggressively. Run the same tier at two or three paces (for example 0.1, 0.2 and 0.3 rps) by declaring the tier under several names (`residential_p01`, …) or by adding a `--rps` per tier. Use the curve to pick `identity_rps`.
8. **Go/no-go thresholds.** Keep the four-check structure, but set the thresholds *after* step 3's reconnaissance:
   - if logged-out Instagram shows only a handful of comments or posts before a wall, a 0.70 completeness or 365-day depth bar will fail by design;
   - in that case, re-scope (shorter backfill, fewer comments per post, or another public surface measured as its own "query") instead of spending more proxy traffic. NO-GO is a valid and useful result.
9. **Hand-back**: the bundle goes privately to whoever maintains the parsers. Then:
   - update the seed registry and parsers;
   - set `identity_rps`, routing, `max_age_days` and the budget from the measurements;
   - write the README sizing and the alert thresholds from the measured baseline;
   - only then deploy the chart against `fakeig` (staging), then a canary of 50–100 real profiles, and ramp as in `go-live.md` §6.

### 6.4 Open questions to answer early

- Does logged-out Instagram serve **any** comment pagination, or only a preview? This decides whether completeness can be a go/no-go criterion at all.
- Are doc_ids for profile posts and comments discoverable in static bundles, or only at runtime (render or HAR)? This decides whether a `registry-sync`-style service can follow rotations automatically, as fbs does with canary pages.
- Are cursors identity-bound? If they are, crawl tasks need session affinity (fbs's `SessionSnapshot` path).
- Bytes per request versus Facebook. Instagram JSON often carries many image candidate URLs per media, so proxy cost may be higher per request; only Phase 0 can tell.
