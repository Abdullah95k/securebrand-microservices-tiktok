# 7. Instagram build plan

[← Index](README.md)

This chapter turns the fbs reference (chapters 1–6) into a plan for an Instagram scraper ("igs" below) built on the same design. It brings together the "Instagram adaptation notes" at the end of each chapter, which have the details.

> **Read this first.** Everything here about Instagram is a **suggestion or hypothesis**. The repository contains no Instagram code or captures. Endpoint paths, headers, tokens, field names, error shapes and limits below come from general knowledge of Instagram's web app and change often. Verify each one with an Instagram Phase 0 (§8) before writing code against it. Statements about fbs are facts from the code at commit `265ae3a`.

---

## 1. Scope and ground rules to carry over

- **Logged out only**, as ADR 0001 decided for Facebook: no accounts, no warmed or fake accounts, no ban evasion. "All comments" means everything Instagram serves to logged-out clients, and coverage is measured and reported, never assumed.
- **Legal before production.** The fbs go-live runbook requires a ToS review, a legitimate-interest assessment and a DPIA. Instagram adds a privacy issue that Facebook pages mostly did not have: **many tracked profiles are natural persons** (creators), so profile fields (`username`, `full_name`, `biography`, profile picture) are personal data too. They need the same PII gating, erasure and suppression as comment authors.
- **Expect less logged-out access than Facebook.** Logged-out Instagram is widely reported to wall profiles and comments after a few views per IP. Phase 0 may well show that some goals (365-day backfills, complete comment threads) are not reachable logged out. A NO-GO, or a narrower scope, is a valid outcome to plan for.

## 2. What to reuse, what to adapt, what to rewrite

Classes: **(a)** generic, reuse as-is (rename only); **(b)** reuse with Instagram parameters or small extensions; **(c)** Facebook-specific, rewrite. The chapter column points to the details.

| Package / asset | Class | Notes | Ch. |
|---|---|---|---|
| `internal/platform/*` (kafka, pg, redisx, leader, s3x, schemareg, metrics, httpserver, run, log, config, breaker) | a | New advisory-lock keys, consumer-group prefix and env prefix (`IGS_`) | 5, 4 |
| `internal/fb/transport` | a | Fingerprinted TLS/HTTP2 with header order; `cookieDomain` is already host-generic. Pin the `cookie` header position | 1 |
| `configs/fingerprints.yaml`, `session.Fingerprint` | a | Same Chrome profiles (same Meta edge). Rotate profiles per identity, not one per pod | 1 |
| `internal/fb/proxy` | a (config b) | Tiers, exit buckets, sticky gateway sessions, routing by request class. Expect residential/mobile exits to be needed sooner | 1 |
| `internal/fb/ratelimit` (GCRA, AIMD, budget) | a | Rename `page:` keys to `profile:`, namespace the GCRA keys, retune (§7) | 1 |
| `internal/fb/session` pool and identity lifecycle | b | New token struct (`csrftoken`, `lsd`, app id, `www-claim`, rollout hash), lower request quotas, fix the cooldown backoff | 1 |
| `internal/fb/session` loader (bootstrap) | b/c | Keep navigation headers and the redirect loop; rewrite wall paths (`/accounts/login`, `/challenge`), the cookie bootstrap and the consent flow | 1 |
| `internal/fb/htmlx` | c (structure a) | Keep "ordered regex candidates per token" and `JSONBlobs`; new patterns and user-id extraction | 1, 3 |
| `internal/fb/graphql` | b | Keep pacing, breakers, fallback, outcome funnel, raw capture and `SplitParts`. New endpoint, headers and form; **add a REST (`/api/v1/...`) request kind** | 1 |
| `internal/fb/detect` | b | Keep the 9 classes and evaluation order; new rules; **classify 400/401/403**; make the rules configurable | 1 |
| `internal/fb/registry` (+ `fb_queries`, `fb_client_params`) | a | Rename tables. Enforce one active row per name; order fallbacks by last success | 2 |
| `internal/fb/docids` | b | Parameterise the bundle host allow-list, path and module-suffix regexes; add the Relay `params:{id,…,name}` shape | 2 |
| `internal/registrysync` | a framework, c probes | Keep the advisory lock, candidates, verdicts (`ok/stale/drift/inconclusive`), promotion, fallbacks, retirement, dry run, reports. Rewrite the probe chain (profile → posts → comments → child comments) and canary loading | 2 |
| `internal/fb/browser` | a (params b) | Raw CDP render engine, proxy auth, media blocking, caps. Parameterise the GraphQL capture predicate, read GET query strings too, and add scripted actions (open post, expand replies) for discovery | 2 |
| `internal/fb/parse` walker (`Collect`, `First`, `Find*`, `FindPageInfo`, `Report`) | a/b | Generalise `Collect` to a predicate: v1 REST objects have no `__typename`. Add a pluggable pagination extractor (`end_cursor`, `next_max_id`, `next_min_id`) | 3 |
| `internal/fb/parse` entity parsers | c | Profile, media (image, video, carousel, reel), comment, child comment | 3 |
| Golden fixtures framework, `check-fixtures` | a | `testdata/ig/<query>/<case>/{response.*, meta.json, expected.json}` | 3 |
| `internal/fixtures` scrubber | b | New person detectors (`username`+`pk`, `full_name`, `profile_pic_url`, captions, comment text). **Pseudonymise usernames consistently**, since mentions and tags reference them | 3 |
| `internal/fakefb` | c (pattern a) | Build `fakeig` with the same skeleton (§6). The largest single piece of new work | 3, 6 |
| `internal/crawl/{lanes,checkpoint,jobs,rawsink,shadow}`, task mechanics, `stage.Worker`, `RetryRelay` | a | Leases, `done:` markers, stalls/attempts, retry topics, DLQ, Lua accounting, finalize set. Fix the DLQ replay bug first (§9) | 4 |
| `internal/crawl/emit` | a | Content-hash event ids, HMAC author keys (on the **user pk**, never the username), suppression, signed-URL stripping | 4 |
| `internal/crawl/stage` page/post/thread bodies, scopes, `NormalizeRef` | c | New stage graph (§5); Instagram refs (`username`, `@username`, profile URL, numeric id; shortcode for posts) | 4 |
| `internal/dispatch` | a/b | Idempotency, advisory-lock caps, roots registered before produce. Consider attaching to a running job for the same profile | 4, 5 |
| `internal/api` | a scaffolding, b endpoints | API keys, scopes, GCRA, problem+json, keyset cursors, NDJSON, OpenAPI parity test. `/pages` → `/profiles`; accept shortcodes on `/posts/{post}`. Add ownership checks | 5 |
| `internal/persist` | a technique, b tables | Temp-table COPY + no-op upsert + `RETURNING old/new` change detection. Add a quarantine/DLQ and a suppression check | 5 |
| `internal/scheduler` | a (retune) | Planner, adaptive tiers, post refresh, recrawls, finalizer, reaper, budget, lag. Lower tier thresholds | 5 |
| `internal/media` + media handoff contract | a | Same Meta CDN family with `oe=` expiry (verify). Map renditions to `video_policy`; carousel children are separate media | 5 |
| `internal/gdpr` | a | Add profile erasure for individuals; apply `pii_mode` to profiles | 5 |
| `internal/replay`, `fbctl` | a framework | New parser dispatch; `pages import` → `profiles import` | 5 |
| `internal/spike` (Phase 0) | a orchestration, c measurement | Keep steps, resume, report, verdict, cost, bundle, secret scan, HAR import; rewrite `Runner.page` for Instagram | 6 |
| `deploy/**`, CI workflows, Makefile, `chart-smoke.py`, alerts, dashboard | a | Rename `fbs` → `igs`; recalibrate thresholds from Instagram's Phase 0 | 6 |
| `configs/queries.seed.yaml` | c | New Instagram queries and client params (§4.4) | 2 |

### Recommended code organisation (suggestion)

Build igs in this repository and **share code rather than fork it**:
1. Move the platform-neutral parts of `internal/fb/*` into a neutral tree, for example `internal/scrape/{transport,proxy,ratelimit,session,registry,docids,browser,walk}`. Keep Facebook-only code (`htmlx` patterns, `detect` rules, `parse` entities, seed queries) under `internal/fb`. The existing fbs tests (unit, fakefb e2e, chaos) are the regression net for this refactor.
2. Generalise the crawl framework (`internal/crawl/*`) over a small platform interface: stage bodies, scope messages, ref normalisation, a parser set and a detect rule set.
3. Add `internal/ig/*` (client parameters, `htmlx` patterns, detect rules, parsers), `internal/fakeig`, `cmd/igs-*` and an `igs` Helm chart (or a platform value in the existing chart).
4. Keep separate topics, tables, metric prefixes and lock keys, so both stacks can share Kafka, Postgres and Valkey clusters.

A straight copy is faster to start, but every fix in this reference's issue list would then have to land twice.

## 3. The Instagram client (hypotheses to verify)

These details come from chapter 1 §16 and chapter 2 §9. Every row is **[IG-VERIFY]**.

### 3.1 Surfaces

1. **HTML bootstrap:** `GET https://www.instagram.com/<username>/` with Chrome navigation headers. Expected cookies: `csrftoken`, `mid`, `ig_did`, `datr`. Tokens in `ScheduledServerJS`-style JSON (`LSD`, `SiteData` with `server_revision`, `haste_session`, `hsi`, `__spin_*`), plus the web app id and a rollout hash. Frequent redirects to `/accounts/login/?next=…` after a few views from one IP.
2. **Web GraphQL (Relay persisted queries):** `POST https://www.instagram.com/graphql/query` (some captures show `/api/graphql`). Form-encoded `doc_id`, `variables`, `fb_api_req_friendly_name`, and the same `__a/__req/__hs/__rev/__s/__hsi/__comet_req/lsd/jazoest/__spin_*` family, plus `__d=www`. Friendly names in the `Polaris…Query` family, for example `PolarisProfilePostsQuery`. Recent responses root at `data.xdt_api__v1__feed__user_timeline_graphql_connection{edges,page_info}`.
3. **REST web endpoints:** for example `GET /api/v1/users/web_profile_info/?username=<u>` (profile plus first posts) with `x-ig-app-id`. Heavily throttled logged out. The legacy `?__a=1` and `query_hash` paths are deprecated.

### 3.2 Tokens and headers, Facebook → Instagram

| fbs concept | Instagram equivalent |
|---|---|
| `lsd` + `x-fb-lsd` | probably the same |
| `fb_dtsg` | absent logged out (as on Facebook) |
| `_js_datr` → `datr` cookie planting | probably unnecessary: `datr`, `ig_did` and `mid` arrive through `Set-Cookie` |
| — | **`csrftoken` cookie → `x-csrftoken` header** on every POST |
| — | **`x-ig-app-id`** on every XHR (the web app id has long been `936619743392459`) |
| — | `x-ig-www-claim` (starts at `0`; echo the server's `x-ig-set-www-claim`), `x-instagram-ajax` (rollout hash), `x-requested-with: XMLHttpRequest` |
| `x-asbd-id` | same header, different value; keep it in registry `client_params` |
| `__s`, `__spin_*`, `__hsi`, `__rev`, `__hs`, `jazoest` | same names and probably the same derivations |

### 3.3 Response classes

Keep fbs's class set and `Blocking()` semantics. Map the signals:

| Class | Candidate Instagram signals |
|---|---|
| `login_wall` | redirect to `/accounts/login`; 401 with `"require_login":true`; an HTML login form where JSON was expected |
| `checkpoint` | redirect to `/challenge`, `/accounts/suspended`; `checkpoint_required` / `challenge_required` |
| `rate_limited` | HTTP 429; 401/403 with "Please wait a few minutes"; `feedback_required`, `"spam":true`; Meta GraphQL rate-limit codes |
| `soft_error` | 5xx (except gateway errors → `transport`), `"status":"fail"` with an unknown message, critical GraphQL errors |
| `registry_stale` | persisted-query-not-found and unknown-operation errors |
| `content_unavailable` | 404, `"user": null`, "Sorry, this page isn't available." |

Treat a login wall on the **first** page of a profile as an exit-quality signal, not as coverage truncation. Keep fbs's "two walls at the same cursor = truncated" rule for deeper cursors only.

### 3.4 Registry seed (shape only)

`client_params`: `__a`, `__d: www`, Instagram's `__comet_req`, `dpr`, `__ccg`, `fb_api_caller_class: RelayModern`, `server_timestamps`, `x_asbd_id`, `x_ig_app_id`.

Logical queries (real friendly names, doc_ids and variables come from the Phase 0 HAR):

| Logical name | Purpose | Key variables |
|---|---|---|
| `profile_info` | username → user id, bio, counts, privacy | `username` |
| `profile_posts` | the profile's media grid | `username`/`user_id`, `first`/`count` (12), `after` cursor |
| `profile_reels` | the clips tab, if the grid misses reels | `user_id`, cursor |
| `post` | single post by shortcode | `shortcode` |
| `comments` | top-level comments, most complete ordering | media id, `first`, `after`, ordering flag |
| `comments_newest` | newest-first, for incremental runs, if logged-out Instagram offers it | as above |
| `child_comments` | replies to one comment | parent comment id, cursor |

Expect Relay provider flags (`__relay_internal__pv__…`) as fixed variables. Make `variables` key order follow the template (fbs alphabetises them).

## 4. Data model

The full field-by-field mapping is in chapter 3 §13.3, and the table design in chapter 5 §14.2. The essentials:

### 4.1 Entities

| fbs | igs | Main differences |
|---|---|---|
| Page (`fb.page.v1`, `pages`) | **Profile** (`ig.profile.v1`, `ig_profiles`) | Key `user_id` (pk), `username` is mutable (track renames in snapshots), `full_name`, `biography`, `bio_links`, `category`, `is_private`, `is_business`/`is_professional`, `followers_count`, `following_count`, `media_count`. A private profile is a terminal "cannot crawl" state, not a failure. No likes count or cover |
| Post | **Post** (feed post, carousel, reel, IGTV) | Key media `pk` (~19 digits; fits int64, or use strings) plus `shortcode` (base64-like encoding of the pk, used in `/p/` and `/reel/` URLs). Type from `media_type` (1 image, 2 video, 8 carousel) + `product_type` (`feed`, `clips`, `carousel_container`, `igtv`). Caption without entity ranges, so extract `#tags` and `@mentions` by regex. `like_count` (nullable when hidden: `like_and_view_counts_disabled`), `comment_count`, `comments_disabled`, `play_count` and `view_count` kept separate, `is_pinned` (up to 3), `is_paid_partnership`/sponsor tags, `usertags`, `location`, reel audio, `accessibility_caption` |
| (single `page_id` owner) | **`ig_post_owners`** | Collab posts appear on several profiles: model owner and co-authors explicitly, and dedupe crawls by media pk |
| PostMetrics | PostMetrics | `like_count`, `comment_count`, `play_count`, `view_count`; change-only as in fbs |
| Comment / reply (depth ≤ 3) | Comment / child comment (**depth 0 or 1**) | Replies to replies are flattened into the parent thread with an @mention. `comment_like_count` replaces reactions; `child_comment_count`; `is_op` = author pk equals media owner pk; `is_pinned`. Comments are not editable (verify), so `edit_count` stays 0, but keep the columns |
| Media (original/sd/hd/thumbnail/link preview) | Media | `image_versions2.candidates[]` and `video_versions[]` renditions: map by size to `original`/`hd`/`sd`/`thumbnail`. **Carousel children** are separate media, with `parent_media_pk` and `position`. `video_duration` (seconds) → `duration_ms`. `oe=` expiry parsing unchanged (verify). Separate UUIDv5 namespace |
| Job, Erasure, media handoff | unchanged | Either `ig.media.*` topics with the same schema, or a `platform` field on the shared handoff messages if one analysis service consumes both |

### 4.2 Contracts and schema (suggestions)

- Proto package `securebrand.ig.v1`; topics `ig.profile.v1`, `ig.post.v1`, `ig.post_metrics.v1`, `ig.comment.v1` (keyed by media pk), `ig.media.v1`, `ig.job.v1`; task topics `igs.task.{profile,comments,replies}.{p0,p1,p2}`, `igs.task.media`, `igs.task.retry.{30s,5m}`, `igs.task.dlq`. Same wire format (`fbs-type`-style header), Schema Registry, BACKWARD_TRANSITIVE and `buf breaking`.
- Factor platform-neutral messages (`Observation`, `TextRange`, `ImageRef`, `CrawlMode`, `S3Object`, media handoff, `JobEvent`, `ErasureEvent`) into a shared `securebrand.common.v1` **for new code**. Do not move them out of `fb.v1`: by the contract's own rules that is a breaking change.
- Tables: `ig_profiles`, `ig_profile_snapshots` (monthly), `ig_posts` and `ig_comments` (monthly by `taken_at`, PK starting with the post), `ig_post_index` (+ a unique `shortcode` index), `ig_post_owners`, carousel children (table or media columns), `ig_post_metrics`, `ig_comment_authors` (`author_key = HMAC(user pk)`). Keep `jobs`, `api_clients`, schedules (keyed by `profile_ref` = `u:<username>` / `id:<pk>`, resolved to `user_id`), registrations, `post_refresh`, erasure, suppression, `media_acks`, `crawl_cursors`.
- Store NULL, not 0, when counts are hidden, so metric series do not show fake drops.

## 5. Stage graph

The worker, lanes, checkpoints, leases, accounting, retry and DLQ machinery are reused unchanged (chapter 4 §17). Only the stage bodies and scopes are new:

```
dispatch (API p0 / scheduler p1 / backfill p2)
 └─ ProfileTask   igs.task.profile.<lane>    lease profile:<user_id> | uref:<username>
      load profile (resolve user_id, counts, private/unavailable) -> ig.profile.v1
      paginate the grid -> ig.post.v1 + ig.post_metrics.v1 + ig.media.v1 (carousel children, renditions)
                        -> igs.task.media per video_policy
                        -> CommentsTask when comments are enabled, count > 0, and FULL or the count grew
      stop: age window, N consecutive old non-pinned posts, max_posts, end of feed; yield after 20 pages/60s
 └─ (optional) ReelsTask  lease reels:<user_id>   clips tab, only items not already seen in the job
 └─ CommentsTask  igs.task.comments.<lane>   lease ig_post:<media_pk>
      top-level comments (+ inline preview replies) -> ig.comment.v1; deletion-detection bookkeeping
      spawn RepliesTask when child_comment_count > previews (and > cache, for incremental runs)
 └─ RepliesTask   igs.task.replies.<lane>    lease ig_comment:<comment_pk>
      child comments, depth 1 only, no nested spawns
 └─ MediaTask     igs.task.media             downloader (outside job accounting)
```

Envelope changes: new `Kind`s (`PROFILE`, `COMMENTS`, `REPLIES`, `MEDIA`, optional `REELS`) and scopes:
- `ProfileScope`: `username`, `user_id`, `watermark`, `posts_seen`, `consecutive_old`, `newest_seen_at`.
- `PostScope`: `media_pk`, `shortcode`, `owner_id`, `taken_at`, `reported_comment_count`, `comments_watermark`, `comments_seen`, plus **`items_seen` carried across continuations**, which fixes fbs's per-segment `max_comments_per_post`.
- `RepliesScope`: parent comment and `reported_child_count`.

Keep `lineage_id`, `seq`, `attempt`, `stalls`, `cursor`, `limits` (add `include_reels`, `include_carousel_children`), `lane` and `mode`. `affinity` (a serialised identity) becomes useful if Instagram cursors turn out to be bound to the session or CSRF token.

Things the logic must absorb:
- **Incremental runs:** keep the watermark minus overlap and the 72h re-read. Ignore the up to 3 pinned posts in the old-post stop. Use the per-post comment-count cache. Read comments newest-first with an older-stop only if logged-out Instagram offers that ordering; otherwise gate full reads on count changes.
- **Deletion detection:** the algorithm ports, but the completeness formula depends on count semantics. Facebook's total equals Σ(1 + reply_count). If Instagram's `comment_count` excludes child comments, the formula becomes Σ1 = count. Hidden or filtered comments make counts larger than what is visible, so plan a non-zero `deletion_count_slack`.
- **Comments disabled** is a known zero (post state `skipped`); filter-hidden shortfalls count as withheld.

## 6. `fakeig`: the test foundation

Every fbs test layer (client tests over both transports, golden fixtures, e2e, chaos, load, soak and the Phase 0 dry run) depends on `fakefb`. `fakeig` needs the same parts:
- **World:** seeded public and private profiles; posts with a mix of image, video, carousel and reel; play counts; hidden like counts; comments disabled; pinned posts; collab co-authors; comments with depth-1 child comments; withheld/filtered comments; mutations (new posts and comments, deletions, caption edits); ground truth through `TruthFor`.
- **Endpoints:** a profile HTML page with tokens and cookies; the profile-info JSON; `/graphql/query` that validates doc_ids, friendly names, `x-csrftoken` against the cookie, `x-ig-app-id` and `lsd`; the comments and child-comments operations; JS bundles declaring operations (for discovery); a signed CDN host with `oe`/`oh` and expiry; admin endpoints `/__fakeig/{profiles,truth,mutate,faults,rotate,stats}`.
- **Renderers** for both the GraphQL (`xdt_*`, `edge_*`) and the v1 REST shapes, with A/B variants per item (fakefb alternates story shapes the same way).
- **Faults:** 429 / "Please wait a few minutes" every N requests, login redirect or `require_login` after N, `checkpoint_required`, doc_id rotation (`--docids rotated`), renamed optional and required fields, latency, JS-only tokens, consent wall, media TTL.
- **Stats** for politeness assertions (minimum interval per identity, counts per query), `WriteFixtures` for synthetic goldens, and the same CLI flags as `fakefb`.

## 7. Starting configuration (suggestions; replace with Phase 0 measurements)

| Knob | fbs default | igs starting point |
|---|---|---|
| `identity_rps` / burst | 0.33 / 2 | 0.1–0.2 / 1 |
| requests per identity | 150–400 | 10–40 |
| per-profile rps | 3 (p0 6) | 1 |
| global rps (AIMD start) | 50 | 5–10; `aimd.min_samples` 20–30 |
| exit breaker | 15% over 1m, ≥ 20 events | 15–25%, ≥ 10 events |
| gateway `sticky_ttl` | 10m | 5–10m |
| timeline page size | 10 | 12 (Instagram grid page); make page size per query in the registry |
| `max_depth` | 3 | fixed at 1 |
| tiers | T0 ≥ 10 posts/day … | lower thresholds, e.g. T0 ≥ 3/day |
| browser `max_share` | 2% | size from Phase 0; may need to be higher if tokens need JavaScript |
| budget `requests_per_day` | 0 (off) | set from the Phase 0 cost projection |

## 8. Instagram Phase 0 plan

Reuse the `fbs-spike` kit's orchestration, resume, report, verdict, cost projection, bundle and secret scan (chapter 6 §1). Rewrite the measurement flow.

1. **Legal sign-off** (ToS, LIA, DPIA).
2. **`fakeig` and an `igs-spike` dry run** that reaches GO with rotated doc_ids, and runs as a CI unit test (like `TestPhase0AgainstFakeFacebook`).
3. **Manual reconnaissance.** Two or three logged-out HARs: a profile scrolled slowly, and a post with many comments with replies expanded. Note the scroll depth where the login wall appears. `igs-spike har` turns them into the first registry, the variable shapes and fixtures.
4. **Inputs:** `proxies.yaml` with at least two priced tiers (ISP and sticky residential, maybe mobile); `profiles.csv` with about 150 public profiles stratified by follower size, category (brand, news, creator, government) and region (US, EU consent, APAC), busiest first. Suggested header: `profile,size,category,region,language,account_type`.
5. **Steps:** preflight (exits, profile load with Instagram token rules) → docids (bundle scan, render, HAR) → capture (profile HTML, posts page, comments page, child-comments page, failures) → smoke → run (resumable, per tier) → report → bundle.
6. **Measure per tier and stratum:** bootstrap success and tokens found; block rate by signal (429, login redirect, `require_login`, checkpoint); **posts reachable before a wall** (in posts and days); comment and child-comment completeness; bytes per request per query and the projected $/month; latency p50/p95 (fbs records latency but never reports it); cursor portability across identities; CDN success and signed-URL lifetime; EU consent incidence.
7. **Pacing sweep:** the same tier at 0.1, 0.2 and 0.3 rps, to choose `identity_rps`.
8. **Verdict.** Keep the four checks (completeness, bootstrap, block rate, depth), but set the thresholds after reconnaissance. If logged-out Instagram shows only a preview of comments, a 0.70 completeness bar fails by design: re-scope instead of buying more proxy traffic.
9. **Hand-back:** update the seed registry and parsers; set `identity_rps`, routing, `max_age_days` and the budget from the measurements; write the sizing and alert thresholds; then deploy against `fakeig` in staging, run a canary of 50–100 real profiles, and ramp as in `go-live.md`.

## 9. Fix before porting

These issues exist in fbs today (see the [index](README.md#known-issues-in-fbs-do-not-port-them)). Fix them in the shared code, or at least in igs:

1. DLQ replay vs `done:` markers: clear the marker on replay, or replay under a new task id, and re-open job accounting.
2. Persister poison records: bisect failing batches and send offenders to a DLQ instead of retrying forever.
3. Check suppression in the persister as well as in the crawlers.
4. Check job ownership on `GET`, `cancel` and `results`; scope `PATCH /pages` to the caller's registrations.
5. Make `/jobs/{id}/results` use the job's own post list (the tracker already keeps one in Redis), not `last_seen_at`.
6. Carry `items_seen` across continuations; let reply-thread failures clear a post's "complete" bit.
7. Text offsets: compute in UTF-16 code units wherever ranges are produced or consumed.
8. Publish `media.deleted` events for orphans; add `posts_failed` to the job completeness message.
9. Align comment retention with the maximum backfill age (no hourly partition churn).
10. Detection: classify 400/401/403; make rules configurable; let the rate-limit cooldown actually grow; rotate fingerprint profiles per identity.
11. Registry: one active row per name; fallbacks ordered by last success; record render-discovered candidates with their own source; diff captured variables against the template.
12. Phase 0: report latency; count the portability bootstrap in cost; report depth limited by the page cap separately.
13. Remove or wire up the unused knobs and fields.

## 10. Suggested build order

1. **Phase 0 reconnaissance (manual HARs)** first, before writing code: it decides whether the project is viable and which surfaces to use.
2. Extract the shared packages out of `internal/fb` (§2) with fbs's tests green; land the fixes in §9 that touch shared code.
3. `fakeig` (world, endpoints, faults) and the Instagram client: tokens, headers, detect rules, GraphQL and REST request kinds, with a client test suite against `fakeig`.
4. Parsers with golden fixtures from the reconnaissance HARs (scrubbed), and the scrubber's Instagram detectors.
5. `igs-spike` and its dry run; then the real Phase 0 measurement and the go/no-go.
6. Protos, topics and migrations; the stages on the reused crawl framework; the e2e test against `fakeig`.
7. Persister tables, API endpoints, scheduler retuning, media mapping, GDPR for profiles.
8. Registry sync for Instagram (bundle regexes, probes, canary profiles) and the browser fallback parameters.
9. Helm, CI, alerts and dashboard; chaos, load and soak tests; staging against `fakeig`; canary; ramp.

## 11. Open questions for Phase 0

- Does logged-out Instagram serve **any** comment pagination, or only a preview? This decides whether completeness can be a go/no-go criterion.
- How far does the profile grid paginate logged out before a wall? This decides the backfill window.
- Are doc_ids for posts and comments declared in static bundles (so registry-sync can follow rotations automatically), or only visible at runtime (render or HAR)?
- Are cursors bound to the identity or CSRF token? If yes, tasks need session affinity.
- Which tokens and cookies does a plain HTTP profile load yield? Is JavaScript needed (browser fallback share)?
- Does `comment_count` include child comments, and hidden or filtered ones?
- Bytes per request compared with Facebook: Instagram JSON carries many rendition URLs, so proxy cost per request may be higher.
- Does the grid include all reels, or is a separate clips stream needed?
- How often do Instagram doc_ids rotate (sets `registry_sync.interval`)?
