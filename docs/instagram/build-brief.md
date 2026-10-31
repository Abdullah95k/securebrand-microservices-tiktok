# igs build brief: building the Instagram scraper microservice

This brief drives the sessions that **build** igs, the logged-out Instagram public-profile scraper, in its own repository, `Abdullah95k/securebrand-microservices-igs` (D7). igs reuses the design and a copy of much of the code of fbs, the Facebook page scraper, whose repository igs work never changes.

**How to use it:**
- Every build session starts by reading this brief and [`build-status.md`](build-status.md). It then does the next unfinished milestone (§6), opens one draft PR for that milestone, and updates `build-status.md`.
- The same prompt ([end of this file](#prompt-for-a-build-session)) starts every build session. It always picks up where the last one stopped.
- Sessions stop at **owner checkpoints**, which only the owner can do: live tests against Instagram, proxies, legal. They say exactly what is needed.

---

## 1. Sources of truth (read in this order)

1. **This brief**: the decisions in §2 are made and no longer open.
2. [`har-findings-2026-10-30.md`](har-findings-2026-10-30.md): what logged-out Instagram actually serves, from the owner's 9 recordings. It covers the operations, doc_ids, tokens, data availability and comment coverage. **This is evidence. Where it conflicts with a hypothesis anywhere else, it wins.**
3. The owner's answers doc: [Instagram scraper (igs): owner questions](https://claude.ai/artifact/C7CX2otkiE7Gtb1ugaE2RB). Read it with the Claude Docs tools: the decisions, inputs status, recording log and page tests. §2 summarises it as of 2026-10-31. If the doc changed since, the doc wins: note the change in `build-status.md`.
4. [`../fbs-reference/README.md`](../fbs-reference/README.md) and chapters 1–6: how fbs works, with `path:line` references at commit `265ae3a`. **Main has moved since, so re-verify any line you rely on.** Chapter 7 (Instagram build plan) and [`kickoff-brief.md`](kickoff-brief.md) are earlier drafts. Where they differ from this brief, this brief wins.
5. The code itself. fbs tests are the regression net for every shared change.

---

## 2. Decisions (made: do not reopen without the owner)

| # | Decision | Detail |
|---|---|---|
| D1 | **Scope** | Collect what logged-out Instagram serves: profiles, feed posts, carousels (every child), reels, post metrics, top-level comments, media metadata (and files, see D5), highlights. **Out of scope, because they need login** (owner's tests plus the HARs): replies, current stories, reel view or play counts, follower and liker lists. Not tested yet: tagged posts. Keep schema room (nullable columns, enum values) so replies or tagged posts can be added later without a breaking change |
| D2 | **Volume and freshness** | Hundreds of thousands of profiles. New profiles: backfill as far back as possible, up to 365 days (subject to the grid-depth test, U1). Already-scraped profiles: **every 3 hours**, reading the timeline back to the newest stored post. Comments: fetched only when a post's comment count went up |
| D3 | **Access** | Logged out only. No accounts of any kind |
| D4 | **Sources** | Our own tool only. **No official Instagram API, no third-party scrapers or data providers** (Apify, ScrapeCreators and the like), not even as a fallback. If logged-out access is too limited, the scope narrows |
| D5 | **Media** | The same contract as fbs: temporary files, acknowledgement by consumers, a TTL, video `sd`, thumbnails only for backfills. **The analysis service doesn't exist yet (D10).** Until it does, collect media **metadata** always and keep file downloads **switched off by default** (`ig.limits.include_media: false`). Otherwise every file would expire unread at the TTL. Make it one config switch |
| D6 | **Infrastructure** | Testing and staging use the **fbs infrastructure** (the same Kafka, Postgres 18, Valkey, S3, cluster), with separate topics, tables, lock keys, consumer groups and metric prefixes. Production may be shared or separate later, so nothing may assume either |
| D7 | **Code** | **A separate repository, `Abdullah95k/securebrand-microservices-igs`, that owns a copy of fbs's code** (owner, 2026-10-31; this replaced "share code with fbs in this repository"). igs work must not impact fbs in any way: igs sessions never push to `securebrand-microservices`. The copy starts from fbs's history at `f1377a9`; igs fixes the shared defects (§5) in its own copy, and `UPSTREAM.md` records what is ported in either direction. See [ADR 0004](../adr/0004-ig-code-organisation.md) |
| D8 | **Privacy** | Record both profiles and commenters, because the analysis will relate them later. Analysis output will not show names or accounts. Apply fbs's protections to both: HMAC `author_key` on the Instagram **user pk** (never the username, which is mutable), PII behind the `pii:read` scope, `pseudonymized` mode, erasure with suppression, retention by partition drop |
| D9 | **Budget** | Set after the Phase 0 measurement. Build the daily request and byte budget in from the start (fbs's `scheduler.budget`) |
| D10 | **Downstream** | The analysis service will be built later. For now, igs publishes `ig.*.v1` topics and stores everything queryable in Postgres |
| — | **Proxies** | The owner will test **rotating static (ISP) IPs** first, because they are much cheaper than residential. Phase 0 must compare them against residential. The code must support both (fbs's `static_list` and `gateway` tiers) |
| — | **Legal** | The legal team is working on the review. **No production traffic until it signs off** |

---

## 3. What logged-out Instagram serves (summary of the HAR findings)

- **Bootstrap.** A plain HTTP GET of any profile, post or reel page returns HTML (about 750 KB uncompressed) with every token the client needs:
  - `LSD`, `csrf_token`;
  - app id `936619743392459`;
  - `server_revision`/`__spin_r`, `haste_session`, `hsi`, `rollout_hash`;
  - `__comet_req=7`, `jazoest`.

  There is no DTSG. The browser fallback should rarely be needed.
- **Requests:** POST persisted queries, with the same form family as fbs plus `__d=www`, `av=0`, `__user=0`. Headers: `x-fb-lsd`, `x-csrftoken`, `x-ig-app-id`, `x-asbd-id: 359341`, `x-fb-friendly-name`. Responses are NDJSON with `data`/`extensions`.
- **Operations and doc_ids, valid on 2026-10-30.** They rotate, so they seed the registry:

| Logical name | Friendly name | Endpoint | doc_id | Page size |
|---|---|---|---|---|
| `profile` | `PolarisLoggedOutDesktopWWWProfileRootContentQuery` | `/api/graphql` | `27981003384861049` | — |
| `grid_first` | `PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery` | `/api/graphql` | `29483846714553007` | 12 |
| `grid` | `PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery_connection` | `/api/graphql` | `28904005159232689` | 12 |
| `comments` | `PolarisLoggedOutDesktopWWWPostCommentsPaginationQuery` | `/api/graphql` | `27837656105907150` | 10, newest first |
| `highlights` | `PolarisStoriesV3HighlightsLoggedOutPageQuery` | `/graphql/query` | `39683663864580775` | — |

- **Post detail** (`taken_at`, like and comment counts, media renditions, carousel children, usertags, location, collab co-authors, music) came **only from the post or reel page HTML** in the recordings. Grid items carry just pk, shortcode, type, caption and a thumbnail.
- **Top-level comments are complete:** reads that reached the end ended cleanly with `has_next_page: false`. The gap to Instagram's comment count matches the hidden replies. 36 comment pages in 2 minutes drew no wall.
- **The UI stopped the grid at 24 posts** (12 in the HTML plus one page), although `has_next_page` was still true. **Whether a crawler can keep paging is the first thing to test (U1).**
- The recordings came from a Chrome 152 user agent, while fbs pins the TLS profile `chrome_146`. The fingerprint must match a current Chrome.

### Open questions the build must resolve (each tied to a milestone)

| # | Question | Why it matters | Resolved by |
|---|---|---|---|
| U1 | Does `grid` keep paging past 24 posts for a crawler? How deep, before a wall? | Whether a 365-day backfill is possible | M4 probe, run by the owner |
| U2 | Is there a **cheap post-detail query** (the request a grid click makes inside the single-page app)? Otherwise each post detail costs a ~750 KB HTML page | Cost at 2.4M checks a day; how comment-count changes are detected | Owner HAR, checkpoint C1 |
| U3 | Do requests work without the browser's client-state fields (`__dyn`, `__csr`, `__hblp`, `__hsdp`, `__sjsp`)? | Request builder complexity | M4 probe |
| U4 | Per-IP tolerance for each proxy tier (rotating ISP vs residential), and wire bytes per request | `identity_rps`, pool sizes, cost, D9 | M8 Phase 0 |
| U5 | Are cursors bound to the identity or session? | Task affinity | M4 probe |
| U6 | Are doc_ids declared in static bundles on `static.cdninstagram.com`? | Automatic rotation following (registry sync) | M9 |
| U7 | Tagged posts reachable logged out? | Scope | Owner HAR, C1 |
| U8 | Cookies Instagram sets and expects (the HAR export stripped them) | Bootstrap | M4 probe |

### Scale and cost: design for it from the start

- 300k profiles every 3 hours is **2.4M profile checks a day**. The cheapest check is one `grid_first` call (about 15 KB) from an identity that is already bootstrapped. **Never load profile HTML per check**: bootstrap each identity once from one page, then reuse its tokens for its 10–40 requests.
- New posts need detail (U2). Comment refreshes can use the comments query newest first, stopping at the first stored comment: one request of about 10 KB when nothing changed. That is fbs's incremental pattern.
- Phase 0 measures the real wire bytes. Keep the request budget (D9) and the adaptive-tier code: the owner asked for every 3 hours, but the cost data may justify tiers later, and that is the owner's call.

---

## 4. Architecture of igs

The same shape as fbs (see `../fbs-reference/README.md`), with separate binaries:

```
clients ─▶ igs-api ─▶ ig jobs + root ProfileTask ─▶ igs.task.profile.{p0,p1,p2}
igs-scheduler (leader): 3-hourly planner, finalizer, reaper, partitions, retention, erasure, AIMD, budget
igs-crawler --stages profile ─PostTask─▶ post (detail + comments) ─▶ ig.* topics ─▶ igs-persister ─▶ Postgres
   └─▶ igs.task.media (only if D5 switch on) ─▶ media downloader/janitor (shared binaries, ig config)
igs-registry-sync: canary profiles ─▶ bundle scan / render ─▶ candidate doc_ids ─▶ probe ─▶ promote
igs-spike: Phase 0 kit and the probe the owner runs     fakeig: synthetic Instagram for all tests
```

**Code layout, in the igs repository (decided in [ADR 0004](../adr/0004-ig-code-organisation.md)):**
- **Neutral, moved out of the copied `internal/fb`:**
  - `internal/scrape/{transport,detect,proxy,ratelimit,session,htmlx,registry,graphql,docids,browser,walk}`;
  - `internal/crawl/*`, working on igs's own task envelope.
- **The copied Facebook code and its tests** stay as the regression net for the copied pipeline until igs's own `fakeig` suites cover it (M6–M8).
- **Instagram-only:**
  - `internal/ig/{htmlx,detect,client,parse,stage}`;
  - `internal/fakeig`;
  - `cmd/igs-*`;
  - `api/proto/securebrand/ig/v1`, with platform-neutral messages factored into `securebrand.common.v1` for new code only (`fb.v1` stays untouched);
  - `migrations/ig` with its own goose version table, so the two services migrate independently;
  - config section `ig:` and env prefix `IGS_`;
  - metric namespace `igs_`;
  - topics `ig.*.v1` and `igs.task.*`;
  - new advisory-lock keys and consumer-group prefix.

**Data model** (draft, finalised in ADR 0005, M0):
- `ig_profiles` and `ig_profile_snapshots`, including username history.
- `ig_posts` and `ig_comments`, partitioned monthly by the post's `taken_at`. Every primary key starts with the post.
- `ig_post_index`, which maps the media pk to the owner and `taken_at`, with a unique shortcode.
- `ig_post_owners` for collab posts.
- Carousel children as media rows with `parent_media_pk` and `position`.
- `ig_post_metrics`, written only on change: `like_count` (NULL when hidden), `comment_count`.
- `ig_comment_authors`, keyed by `author_key`.
- `ig_media` and `ig_media_acks`.
- `ig_highlights`.
- The `jobs`, schedule and registry tables follow the fbs patterns, separately or with a platform column (ADR).
- Comments are top-level only today. Keep a nullable `parent_comment_id` and a depth column.

**Comment completeness and deletion detection:** fbs's rule Σ(1 + reply_count) = count can never hold logged out, because reply counts are hidden. For igs, a read that ends with `has_next_page: false` is **complete for top-level comments**. Report `comments_collected` against `comment_count` as information, not as a failure. Deletion detection (fbs chapter 4 §11) runs only on complete reads.

---

## 5. fbs defects to fix first, in igs's copy

Each fix needs a regression test, and the copied fbs test suites must stay green in the igs repository. fbs's own repository is not changed; `UPSTREAM.md` lists the fixes fbs may want to port. Details are in `../fbs-reference/README.md` § Known issues and [`reuse-matrix.md`](reuse-matrix.md) §8–9, which adds a seventh: a task of unknown kind crashes the crawler.

1. **DLQ replay does nothing for 7 days.** Dead-lettering marks the task `done:`, and the replay re-sends the same task id. Fix it by clearing the marker on replay, or by replaying under a new id, and re-open job accounting.
2. **The persister retries a bad batch forever.** Quarantine bad records (bisect the batch, send them to a DLQ topic, add a metric and an alert), and actually skip undecodable records.
3. **Jobs: no owner check.** `GET`, `cancel` and `results` on `/v1/jobs/{id}` must check that the caller owns the job (404 for others; admin sees all).
4. **Suppression is enforced only by the crawler.** Check suppression in the persister as well.
5. **`max_comments_per_post` is enforced per segment.** Carry `items_seen` across continuations.
6. **Detect rules are hard-coded.** Make them loadable from config, and classify HTTP 400/401/403. Let the rate-limit cooldown actually grow. Rotate fingerprint profiles per identity, and add a current Chrome profile (152).

---

## 6. Milestones

One draft PR per milestone (a large one may split into several). A milestone is done only when CI is green, its acceptance criteria are met, and `build-status.md` is updated. **C-items are owner checkpoints**: stop there, and tell the owner exactly what to do.

| # | Milestone | Deliverables | Acceptance criteria |
|---|---|---|---|
| **M0** | Verify and decide | `docs/adr/0004-ig-code-organisation.md`, `docs/adr/0005-ig-data-model-and-contracts.md`, and the reuse matrix verified at HEAD (`go list -deps`, import graph) in `docs/instagram/reuse-matrix.md` | Every package classified as reuse, parameterise or rewrite, with its Facebook coupling named. Both ADRs accepted by the owner (ask with AskUserQuestion only for real trade-offs) |
| **M1** | Seed and neutralise (igs repository) | Seed `securebrand-microservices-igs` from fbs's history (ADR 0004 §1); rename the module and every name that would collide on shared infrastructure (ADR 0004 §4–5); move neutral code to `internal/scrape/*`; add the Instagram seams with fbs's defaults | The copied suites green after each step: `make test`, `make test-integration`, `make e2e` and CI in the igs repository. Nothing pushed to fbs's repository |
| **M2** | Fixes in igs's copy | §5 items 1–6 and the unknown-kind crash | A regression test for each. The copied suites green. Each fix listed in `UPSTREAM.md` |
| **C1** | *Owner:* 2 more HARs | Record in the same way as before: (a) open a post **by clicking it in a profile grid** (single-page navigation), for U2; (b) open a profile's **reels** tab and **tagged** tab, for U7 and the reels question. Re-share the Drive folder by link, as before | The session downloads the HARs into a git-ignored folder, analyses them, and appends the findings to `har-findings-*.md` |
| **M3** | Contracts | `securebrand.ig.v1` protos (+ `common.v1`), `buf` setup, topics in `configs/` (`ig.*.v1`, `igs.task.*`), `migrations/ig`, `ig:` config section with defaults and validation, metrics `igs_` | `buf breaking` clean, migrations up and down on Postgres 18, config tests, Schema Registry apply works |
| **M4** | Instagram client + **probe** | `internal/ig/{htmlx,detect,client}`:<ul><li>tokens from HTML;</li><li>the form and header builder exactly as in the HARs (header order, client hints);</li><li>both endpoints;</li><li>NDJSON reader;</li><li>detect rules for 200-with-login-wall, 401/403/429, redirects to `/accounts/login` and `/challenge`, persisted-query errors;</li><li>registry seed with the 5 operations above.</li></ul>`fakeig` first slice (bootstrap page + the 5 operations + faults). **`igs-spike probe`**: a single-binary command the owner runs locally | Client tests against fakeig over both transports, as fbs's `fbtest` does. The probe runs end to end against fakeig. It answers U1, U3, U5 and U8 when the owner runs it |
| **C2** | *Owner:* run the probe | `igs-spike probe --profiles canary.txt` from the owner's machine: once direct, once per proxy tier. It writes a JSON report with no PII | U1, U3, U5 and U8 answered in `har-findings` or `build-status`. If U1 shows the grid is walled after 24 posts, the session proposes backfill alternatives and waits for the owner |
| **M5** | Parsers and fixtures | `internal/ig/parse`:<ul><li>profile;</li><li>grid page;</li><li>post detail from HTML (and the U2 query if it exists);</li><li>comments page;</li><li>highlights.</li></ul>Shape-tolerant, as fbs's walker: typename or predicate collection, candidate paths, `Report{Missing,Optional}`. An Instagram scrubber (port `internal/fixtures`: usernames pseudonymised consistently, text blanked keeping UTF-16 length, signed URLs stripped). Golden fixtures in `testdata/ig/`, scrubbed from the owner's HARs | Goldens pass. The scrubber has tests proving no username, name, text or signed URL survives. Fixtures hold no real PII |
| **M6** | Crawl stages | `profile` stage: profile, then grid pages, then post detail, then spawn comments. `post` stage: comment pages. Media tasks only with the D5 switch on. Plus:<ul><li>incremental mode: every 3 h, stop at the watermark, comments only when the count grew, newest-first early stop;</li><li>backfill to 365 days or the wall;</li><li>pinned-post exemption;</li><li>carousel children;</li><li>collab co-authors.</li></ul>`fakeig` completed: world with ground truth, mutations, faults, `--docids rotated` | In-process e2e against fakeig with real PG, Redpanda and Valkey. The store equals fakeig's ground truth for profiles, posts, metrics and top-level comments. Completeness is reported. Chaos test with restarts passes |
| **M7** | Persister, API, scheduler, media, GDPR | `igs-persister` (COPY + no-op upsert + change-only metrics + quarantine). `igs-api`: `/v1` profiles, jobs, posts, comments, metrics, erasure, shortcode lookup, with scopes and owner checks. `igs-scheduler`: 3-hourly planner, finalizer ("completed means queryable"), reaper, partitions, retention, budget, AIMD. Media via the shared binaries with ig config. Erasure for profiles and authors | OpenAPI ⇄ router parity test. e2e covers API reads, scheduled incremental runs, erasure that survives re-scrapes, budget pause |
| **M8** | Phase 0 kit | `igs-spike phase0` (port of `fbs-spike`): preflight, docids, capture, smoke, run, report, bundle, with fbs's spike gaps fixed (latency reported, every bootstrap counted, depth limited by the page cap reported separately). Thresholds set from C2's results. A dry run against fakeig in CI | `make igs-phase0-dryrun` reaches GO against fakeig with rotated doc_ids |
| **C3** | *Owner:* Phase 0 | Run per tier (rotating ISP vs residential), about 150 profiles, and send back the bundle | GO or NO-GO per tier. Config, sizing, budget and alert thresholds set from the measurements |
| **M9** | Registry sync and browser fallback | `igs-registry-sync`:<ul><li>bundle scan on the Instagram CDN, with regexes for the declaration shapes found;</li><li>render capture that reads GET query strings too;</li><li>canary probe chain profile → grid → post → comments.</li></ul>Browser bootstrap with Instagram wall paths | Against fakeig's rotated doc_ids, a stale query heals itself. Alerts and runbook updated |
| **M10** | Deploy and operations | Helm (an `igs` chart or a platform values file), compose, Dockerfile targets, CI jobs, alerts and dashboard (`igs_` metrics, runbook anchors), load and soak tests, runbooks (`docs/runbooks/igs-*.md`) | `chart-check` and `chart-smoke` green. Load-test numbers recorded. The monitoring consistency test passes |
| **C4** | *Owner:* go-live | Legal sign-off, staging on the fbs infrastructure, a canary of 50–100 real profiles, then the ramp (fbs `go-live.md`) | — |

**What can run in parallel:** M1 → M2 must be sequential. M3 and M4 can start once M1 has landed. C1 can happen at any time, the earlier the better.

---

## 7. Rules for every build session

- **No live traffic to Instagram from the cloud session.** Build tools that the owner runs (probe, Phase 0). Never fabricate captures or results.
- **Never commit:** raw HARs, unscrubbed responses, proxy credentials, keys, or any real username, name or comment text. HARs go only into a git-ignored folder, such as `.local/har/` (add it to `.gitignore`).
- **Keep fbs safe:** never push to `securebrand-microservices`; all igs work goes to `securebrand-microservices-igs`. Every change to copied code keeps the copied unit, integration and e2e suites green; say so in the PR with the commands run. On shared infrastructure, never use one of fbs's topic, group, key, lock, bucket or metric names (ADR 0004 §4).
- **Facts vs hypotheses:** Instagram behaviour comes only from the HAR findings or the owner's probe and Phase 0 results. Anything else is labelled a hypothesis with its U-number.
- **Match the repo's style:** Go idioms, comment density, naming, the config layering, the metric conventions, and `docs/` tone.
- **Ask the owner** (AskUserQuestion) only for decisions that are theirs. §2 is settled. Give a recommendation with every question.
- **At the end of each session,** update `build-status.md` (milestone states, PR links, findings, what's next, what the owner must do) and reply with a short summary.

---

## Prompt for a build session

Use the same prompt for every build session, started with the **`Abdullah95k/securebrand-microservices-igs`** repository selected. Since 2026-10-31 igs has its own repository (D7); the first sessions ran in `securebrand-microservices`.

```text
You are a build session for "igs", the logged-out Instagram public-profile scraper, built in its own repository (Abdullah95k/securebrand-microservices-igs) from a copy of "fbs", the Facebook page scraper (Abdullah95k/securebrand-microservices). Never push to the fbs repository.

1. Read docs/instagram/build-brief.md completely, then docs/instagram/build-status.md. Follow the brief's reading order (har-findings, the owner's decisions doc via your Claude Docs tools: https://claude.ai/artifact/C7CX2otkiE7Gtb1ugaE2RB, then docs/fbs-reference/, ADRs 0004 and 0005, reuse-matrix.md). If they are missing on your checkout, the M0 pull request is not merged yet: fetch and check out origin/claude/brave-pascal-nxycds.
2. Find the next unfinished milestone in build-status.md and do it: think through the design first, verify every fbs claim against the current code, then implement with tests, matching the repo's style. Open one draft PR per milestone and keep CI green; for copied code, run the unit, integration and e2e suites and show they pass.
3. Continue to the following milestone in the same session only if the current one is merged-ready and context allows; otherwise stop cleanly.
4. Stop at owner checkpoints (C1–C4) and tell me exactly what to do, in simple numbered steps.
5. Rules: no live requests to Instagram from this session; never commit HARs, credentials or any real usernames, names or comment text; decisions in §2 of the brief are settled, so ask me (AskUserQuestion, with a recommendation) only about genuinely open trade-offs.
6. Before finishing, update docs/instagram/build-status.md (milestone states, PR links, findings, next step, what I need to do) and give me a short summary.
```
