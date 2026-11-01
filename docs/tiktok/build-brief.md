# tks build brief: building the TikTok account scraper

This brief drives the sessions that **build** tks, the logged-out scraper of public TikTok accounts, in its own repository, `Abdullah95k/securebrand-microservices-tiktok` (the owner calls the service and the repository "tks"). tks starts as a copy of igs, the Instagram scraper, which started as a copy of fbs, the Facebook scraper ([ADR 0006](adr/0006-tks-code-organisation.md)). tks sessions read igs and fbs and never push to them.

**How to use it:**
- Every build session starts by reading this brief and [`build-status.md`](build-status.md). It then does the next unfinished milestone (§6), opens one draft PR for it, merges it once CI is green, and updates `build-status.md`.
- The same prompt ([end of this file](#prompt-for-a-build-session)) starts every build session. It always picks up where the last one stopped. **Each session starts the next one itself** when its milestone is merged and the next needs nothing from the owner (§7); the owner starts one only after a checkpoint or decision.
- Sessions stop at **owner checkpoints**, which only the owner can do: recordings, probe runs, proxies, legal. They say exactly what is needed, in numbered steps.

---

## 1. Sources of truth (read in this order)

1. **This brief.** The decisions in §2 are made; reopen one only with evidence, and let the owner decide.
2. **Evidence about TikTok**, newest first: the probe results recorded in `build-status.md` and [`har-findings-2026-10-31.md`](har-findings-2026-10-31.md) (the owner's recordings). **Evidence wins over every hypothesis.**
3. [`open-questions.md`](open-questions.md): every hypothesis about TikTok (T1, T2, …), why it matters and what answers it. Until answered, anything about TikTok's behaviour is labelled with its T-number.
4. [`methods.md`](methods.md): why tks uses the logged-out website, the fallback, and the other methods compared, with sources and the notes for the legal team.
5. [ADR 0006](adr/0006-tks-code-organisation.md) (code), [ADR 0007](adr/0007-tks-data-model-and-contracts.md) (data and contracts), [`reuse-plan.md`](reuse-plan.md) (what tks takes from igs), [`probe-spec.md`](probe-spec.md) (the first milestone), [`recording-guide.md`](recording-guide.md).
6. igs's docs, at the commit tks was copied from: its [build brief](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/instagram/build-brief.md), [HAR findings](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/instagram/har-findings-2026-10-30.md), [reuse matrix](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/instagram/reuse-matrix.md) and `docs/fbs-reference/` (how fbs works). Re-verify any `path:line` you rely on: the code has moved.
7. The code itself. The copied fbs and igs tests are the regression net for every shared change.

---

## 2. Decisions

Made by the owner or carried over from fbs and igs. D2's delay target, D7 and D11 were proposed in the kickoff session and approved by the owner on 2026-10-31 (`build-status.md`).

| # | Decision | Detail |
|---|---|---|
| D1 | **Scope** | For each monitored account: the account (§ objective 1), its videos and photo posts with their metrics (objective 2), top-level comments and, if TikTok serves them logged out (T14), replies (objective 3), media metadata, and media files behind a switch (D5). **Not in scope:** follower and following lists, liked videos, stories and lives, search, sound and hashtag pages. Keep schema room (nullable columns, reserved enum values) for later additions |
| D2 | **Volume and freshness** | Design for **hundreds of thousands of accounts**; the first real list is a few hundred Iraqi accounts. New accounts: every post back **365 days** or to the first post. Known accounts: **new posts stored within 3 hours** of posting: **at least 99 % within 3 h**, so the list is checked every 2 h 45 min (owner, 2026-10-31; §8 A2). Account details **daily**. Metrics refreshed (proposed tiers in ADR 0007 §4; the owner sets them after Phase 0). Comments re-read **only when a post's comment count went up**, at most as often as the post's age allows (proposed tiers in ADR 0007 §4) |
| D3 | **Access** | **Logged out only.** No TikTok accounts of any kind, no OAuth logins |
| D4 | **Sources** | **Our own tool by default.** Official APIs and third-party providers are compared honestly in `methods.md`; using one is the owner's decision. None is used today |
| D5 | **Media** | fbs's contract (temporary files, consumer acknowledgements, a TTL). Until the analysis service exists: metadata always, downloads **off by default** (`limits.include_media: false`), one config switch |
| D6 | **Infrastructure** | Testing and staging on the **fbs infrastructure** (Kafka, Postgres 18, Valkey, S3) with tks's own names: topics `tt.*.v1` and `tks.task.*`, groups `tks-*`, database `tks` with schema `tt`, locks `4_400_*` and `tks:*`, Valkey prefix `tks:`, buckets `tks-*`, metrics `tks_` (ADR 0006 §5). Production may be shared or not; nothing may assume either |
| D7 | **Code** (owner, 2026-10-31) | Own repository; **starts as a copy of igs with its history**, keeps igs's paths for shared code, removes Instagram-only code, keeps the copied Facebook code as the regression net until tks's own suites replace it, and ports igs's later fixes by cherry-pick (ADR 0006) |
| D8 | **Privacy and keeping data** (owner, 2026-11-01) | Accounts and commenters are both personal data. HMAC `author_key` on the **TikTok user id** (never the username or `secUid`), PII behind the `pii:read` scope, `pseudonymized` mode (ADR 0007 §6). **Nothing is deleted and everything is collected** (owner, 2026-11-01; ADR 0007 §14): no erasure and no suppression (every account and commenter is always collected), no retention (`tt.retention` all 0: comments, metrics, snapshots, people, jobs and raw samples kept for good; downloaded media files kept too, so M6c sets no TTL), no comment cap per post (`tt.comments.max_per_post: 0`), and commenters' `predicted_age_group` stored. Recorded for the legal review (`methods.md` §5), which must see it before production (D15) |
| D9 | **Budget** | Set after Phase 0. The daily request and byte budget (fbs's `scheduler.budget`) is built in from the start, and cost per account per day is a reported metric (§8) |
| D10 | **Downstream** | The analysis service comes later. tks publishes `tt.*.v1` topics, stores everything queryable in Postgres, and serves the HTTP API, like fbs and igs |
| D11 | **Method** (owner, 2026-10-31) | **The logged-out website**: plain HTTP with a current Chrome TLS fingerprint for pages and TikTok's own web JSON calls, unsigned wherever TikTok accepts it; calls that need a signature run inside a TikTok page in a small, capped pool of headless browsers, so TikTok's own script signs them. tks never re-implements TikTok's signature algorithms. **Fallback:** the headless browser for everything. Evidence and alternatives in `methods.md` §7. **Confirmed at G1 (owner, 2026-11-01), from C2 and C3:** plain HTTP for accounts, posts, metrics, pinned flags and the backfill (the unsigned `creator` list); a few browser pages (Chrome without its automation flag) sign each comment and reply request, which plain-HTTP workers send from any identity (a signature covers its query, so one per request); video files by plain HTTP with the `tt_chain_token` cookie, off by default (D5) |
| D12 | **Proxies** | The owner's **rotating ISP proxies** first. **Residential only if the owner agrees.** The code supports both (`static_list`, `gateway`) |
| D13 | **Probes** | Run by the owner on the **Phase 0 droplet** (never the owner's Mac, never the cloud session), paced like a person. Their output holds everything they read (names, links, pictures, videos, comments), is written 0600 under `phase0/`, and stays off git |
| D14 | **Pull requests** | One draft PR per milestone (a large one may split). The session merges its own PR once CI is green |
| D15 | **Legal** | **No production traffic until the legal team signs off.** `methods.md` §5 is the briefing for it |

---

## 3. What logged-out TikTok is expected to serve (hypotheses until evidence)

Everything in this section came from public sources (`methods.md` §2). The owner's recordings of 2026-10-31 then confirmed, for a logged-out browser: the profile page's data (with the bio link), the post list's items (counts, pinned flag, ad flag, photo posts), ids carrying time, comments in ranking order, and replies ([har-findings-2026-10-31.md](har-findings-2026-10-31.md)). The rest is a hypothesis with its T-number.

- **Pages carry data.** A post page, and a server-rendered profile page, embed one JSON document (`<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">`) with the post (caption, time, video or photos, music, counts, hashtags, mentions) or the account (id, `secUid`, username, nickname, bio, avatars, verified, private, counts), plus client values (T1, T2). Some profile loads return a client-rendered shell with no data (T1). Pages are large (~400–620 KB), so tks reads them to bootstrap and as a fallback, not per check.
- **The web app calls JSON endpoints with GET**, with the client's standard parameters (`aid=1988`, `app_name=tiktok_web`, `device_platform=web_pc`, a random 19-digit `device_id`, browser fields) and, on some calls, signatures (`X-Gnarly`, `X-Bogus`, `X-Dynosaur`) and `msToken` from TikTok's script (T2, T4). **The post list through `/api/creator/item_list/` works unsigned** for yt-dlp and gallery-dl today; `/api/post/item_list/` needs `X-Gnarly`; the account call (`/api/user/detail/`) may need one; comments (`/api/comment/list/`) and replies (`/api/comment/list/reply/`) are unknown (T4, T24). A refused call answers **HTTP 200 with an empty body**, so an empty answer always counts as a failure.
- **List items are complete**: each carries its counts (plays, likes, comments, shares, saves), music, hashtags, mentions, duration, covers and flags such as `isAd` (T9, T16). The `creator` list pages by time (next cursor = the last post's `createTime` × 1000), about 15 posts a page (T11). Whether and how pinned posts are flagged is unknown (T10).
- **Ids carry time**: a post id's top 32 bits are its Unix time (T8), so a backfill can stop at 365 days without reading every post.
- **Comments are the biggest risk**: public reports conflict on whether guests get them at all (T12). If served, about 20 per page with `total`, probably in TikTok's ranking order rather than by time (T13); replies through their own call (T14).
- **Blocking**: TikTok checks TLS and header fingerprints (a missing `Referer` was enough to be blocked), shows a JavaScript proof-of-work page to some requests (region-dependent), scores IPs (datacenter worst) and shows slide, rotate or 3D captchas under suspicion; burnt identities and device ids get empty answers (T5, T6, T28, T29). Per-IP tolerance on the ISP tier is unknown (T20) and is the main cost driver.

### Open questions (summary; full table in [`open-questions.md`](open-questions.md))

| Group | Questions | Answered by |
|---|---|---|
| Access and bootstrap | T1–T7, T24, T27, T28 | HARs (C1), probe test pass (C2) |
| Data | T8–T19, T26 | HARs, probe (C2, C3) |
| Scale and cost | T20–T23, T25, T29 | probe full pass (C3), Phase 0 (C4), canary (M8) |

---

## 4. Architecture of tks

The same shape as fbs and igs, with separate binaries:

```
clients ─▶ tks-api ─▶ jobs + root AccountTask ─▶ tks.task.account.{p0,p1,p2}
tks-scheduler (leader): list planner (2 h 45 min), daily account refresh, metrics tiers,
                        finalizer, reaper, partitions, AIMD, budget (no erasure, no retention: D8)
tks-crawler --stages account ─PostTask─▶ post (comments, replies) ─▶ tt.* topics ─▶ tks-persister ─▶ Postgres (tks.tt)
   ├── signer pool: a few headless Chromium pages, only for calls that need a signature (D11)
   └─▶ tks.task.media (only with the D5 switch on) ─▶ media downloader and janitor (shared binaries, tks config)
tks-canary: canary accounts ─▶ every operation variant ─▶ promote or demote variants, alert on drift
tks-spike: the probe (M1) and the Phase 0 kit (M7)          fakett: synthetic TikTok for every test
```

**The account stage** (one task per account per check):
1. Take a bootstrapped identity (one page load per identity, reused for its tens of calls; its own random `device_id`).
2. Read the first list page (the unsigned `creator` variant first, if T4 holds). Every item yields the post, its metrics, music and media metadata. New posts go to `tt.post.v1`; changed counts to `tt.post_metrics.v1`.
3. Continue to the next page while posts are newer than the watermark (newest stored post; pinned posts exempt), or, in a backfill, until 365 days or the first post (by the id's time, T8).
4. For each post whose comment count went up, spawn a post task.
5. Once a day, refresh the account itself (user detail call if it works unsigned, T24; else the profile page).

**The post stage** reads comments with the strategy T13 selects (ADR 0007 §5), then replies per thread if T14 holds.

**The signer** is a seam (`tt.signer.mode`, per-operation override in the registry): `none` for calls that work unsigned, `browser` for the rest. Signed calls cost browser time; the pool is capped and its share of calls is a metric with an alert.

**Code layout, data model and contracts:** ADR 0006 and ADR 0007. In short: `internal/scrape/*` (igs's neutral layer, three seams added), `internal/tt/*`, `internal/fakett`, `cmd/tks-*`; protos `securebrand.tt.v1`, `securebrand.tks.task.v1`, `securebrand.common.v1` (igs's); topics `tt.*.v1`; schema `tt`.

---

## 5. Inherited defects

tks's copy carries igs's open defects at `9463b91` (igs brief §5 and the unknown-kind crash; [reuse-plan.md §7](reuse-plan.md#7-what-tks-inherits-that-igs-has-not-fixed-yet)). M2 ports igs's fixes if igs has merged them, or makes them in igs's shape, each with a regression test and an `UPSTREAM.md` line. They do not affect the probe.

---

## 6. Milestones

A milestone is done only when CI is green, its acceptance criteria are met, its PR is merged, and `build-status.md` is updated. **C-items are owner checkpoints**: stop there and tell the owner exactly what to do, in numbered steps with one command per block. **G-items are owner decisions.**

| # | Milestone | Deliverables | Acceptance criteria |
|---|---|---|---|
| **M0** | Plan (kickoff session) | `docs/tiktok/`: methods, open questions, recording guide, probe spec, reuse plan, this brief, status, ADRs 0006 and 0007 | The owner approves the plan (**done**, 2026-10-31) |
| **C1** | *Owner:* recordings | Record R1–R7 of the [recording guide](recording-guide.md) and share the Drive folder by link | The session downloads the HARs into `.local/har/`, analyses them, and writes `docs/tiktok/har-findings-<date>.md` (shapes, field names and counts only), updating the T-table. **Done 2026-10-31** (5 HARs) |
| **M1** | **The probe** (first build milestone) | **M1a** seed: igs's history merged into `main` (ADR 0006 §1), module renamed, Instagram-only code removed, `UPSTREAM.md`. **M1b** names: every shared-infrastructure name to tks's (ADR 0006 §5), igs's guard tests retargeted. **M1c** seams: GET and decoder in `graphql`, method and variants in `registry`, `browser.Session`, the boundary test extended. **M1d** TikTok client: `internal/tt/{htmlx,detect,client,sign}`, operation seed, `fakett` first slice, `tttest`. **M1e** probe: `internal/tt/probe`, `cmd/tks-spike probe`, Makefile targets, runbook `docs/runbooks/tks-probe.md` | [probe-spec.md §5](probe-spec.md#5-acceptance-criteria-for-m1). The copied fbs and igs suites green after each step (`make test`, `make test-integration`, `make e2e`, CI) |
| **C2** | *Owner:* probe test pass | `make tks-probe` on the droplet, 3 to 5 accounts, ISP tier; send the bundle | The session records the answers (T1–T7, T9, T10, T12–T19, T22, T24, T26–T28) in `build-status.md` and the HAR findings, fixes the probe if a step misread, and asks for a rerun if needed. **If comments are not served logged out (T12), it asks the owner how to treat objective §3 before M5** |
| **C3** | *Owner:* probe full pass | `make tks-probe-all`, the whole list, back 365 days; send the bundle | T8, T11, T20 (first look), T21, T23, T29 recorded; depth per account; captchas and challenges per thousand requests |
| **G1** | *Owner decision:* method | The session proposes, from C2 and C3: unsigned website, website plus browser signer, browser for everything, or (owner's call) a provider; with a cost estimate per account per day | The owner chooses (AskUserQuestion, with a recommendation). D11 updated |
| **M2** | Inherited fixes | igs's M2 fixes ported (or made): DLQ replay, persister quarantine, job owner checks, suppression in the persister, `items_seen` across continuations, detect classes for 400/401/403, growing cooldown; the unknown-kind crash | A regression test each; `UPSTREAM.md` updated; copied suites green |
| **M2b** | fbs's review fixes (owner, 2026-11-01) | The fixes fbs merged after igs copied it (`f1377a9..b50b4a7`) in code tks reuses as it is, listed in `UPSTREAM.md` (§ Ported from igs or fbs, M2b): crawl worker and leases, retry relay, Kafka consumer, persister, erasure and media, scheduler, API, leader. Facebook-only fixes are not ported | A regression test each (fbs's, adapted, where it has one); `UPSTREAM.md` updated; copied suites green |
| **M3** | Contracts | `securebrand.tt.v1`, `securebrand.tks.task.v1`, `common.v1` (igs's file), `buf` setup, topics in `configs/tt/`, `migrations/tt` (schema `tt`, own goose table), the `tt:` config section with defaults and validation, metrics `tks_`. ADR 0007 finalised against the evidence | `buf breaking` clean; migrations up, down and up on Postgres 18; config tests; Schema Registry apply works |
| **M4** | Parsers and fixtures | `internal/tt/parse`: account (page and call), post list, post page, comments, replies; shape-tolerant (candidate paths, `Report{Missing,Optional}`). A TikTok scrubber (usernames, nicknames and ids pseudonymised consistently, text blanked keeping UTF-16 length, signed media links stripped). Golden fixtures in `testdata/tt/`, scrubbed from the HARs and the probe's `raw/` | Goldens pass; the scrubber has tests proving no username, name, id, text or signed link survives; ids checked against int64 (ADR 0007 §1) |
| **M5** | Crawl stages (split in M5a: worker and account stage; M5b: post and thread stages, signer seam; M5c: `tt` writer, e2e and chaos) | Account stage (list check, watermark with pinned exemption, backfill to 365 days or the first post by id time, metrics, daily account refresh, spawn post tasks); post stage (comments by the T13 strategy, replies if T14, completeness and deletions); media tasks only with D5 on; `fakett` completed: world with ground truth, mutations (new posts, count changes, renames, deletions, private accounts), faults, rotated variants | In-process e2e against `fakett` with real Postgres, Redpanda and Valkey: **the store equals `fakett`'s ground truth** (§8, A6). Chaos test with restarts passes |
| **M6** | Persister, API, scheduler, media (split in M6a: scheduler and dispatcher; M6b: API, its erasure removed by D8 afterwards; M6c: media kept for good, the §8 delay and success metrics, budget pause) | `tks-persister` (COPY, no-op upsert, change-only metrics, quarantine); `tks-api` `/v1` (accounts by id, username or `secUid`, username history, posts, metrics, comments, jobs, erasure; scopes and owner checks); `tks-scheduler` (2 h 45 min planner, daily account refresh, metrics tiers, finalizer, reaper, partitions, retention, budget, AIMD); media via the shared binaries; erasure for accounts and authors | OpenAPI ⇄ router parity test; e2e covers API reads, scheduled incremental runs, the delay and success metrics of §8, erasure that survives re-scrapes, budget pause |
| **M7** | Phase 0 kit | `tks-spike phase0` (igs's M8 `Measurer` ported, or added): preflight, smoke, run, report, bundle; per-IP tolerance, wire bytes per call, captcha rate, signer share and browser time; cost per account per day projected to 1k, 10k, 100k and 300k accounts | `make tks-phase0-dryrun` reaches GO against `fakett` with rotated variants |
| **C4** | *Owner:* Phase 0 | Run on the ISP tier (residential only if the owner agrees), about 150 accounts, send the bundle | GO or NO-GO; config, sizing, metrics tiers, comment caps, **budget** and alert thresholds set from the measurements |
| **M8** | Canary and signer operations | `tks-canary` (each operation variant on canary accounts; promote or demote; alert on drift, empty answers and signer failures); signer pool health, caps and metrics | Against `fakett`'s rotated variants and a signing change, a stale operation heals itself; alerts and runbook updated |
| **M9** | Deploy and operations | Helm (igs's chart if landed, else fbs's copied), compose, Dockerfile targets (Chromium in the crawler image only if the signer is used), CI, alerts and dashboard (`tks_`), load and soak tests, runbooks `docs/runbooks/tks-*.md`; the copied Facebook code removed once `fakett` suites cover the pipeline | `chart-check` and `chart-smoke` green; load-test numbers recorded; the monitoring consistency test passes |
| **C5** | *Owner:* go-live | Legal sign-off, staging on the fbs infrastructure, a canary of 50–100 real accounts, then the ramp (fbs `go-live.md`) | §8 met on the canary for 7 days |

**What can run in parallel:** C1 at any time, the earlier the better. M1a → M1b → M1c are sequential; M1d needs M1c (and C1's findings if possible). M2 and M3 can start once M1 has landed, in parallel with C2 and C3; M2b comes after M2 and before M3 (owner, 2026-11-01), while tks's shared code still has fbs's names. M4 needs C1 or C2 data. G1 comes before M5.

---

## 7. Rules for every build session

- **No live traffic to TikTok from the cloud session**, including TikTok's developer pages. Build tools the owner runs (probe, Phase 0). Never fabricate captures or results.
- **No paid services:** no provider calls, no credits spent, no sign-ups.
- **Never commit:** HARs, raw or unscrubbed responses, probe output, proxy credentials, keys, or any real username, name, id or comment text. HARs go only into `.local/har/`; probe output only under `phase0/`. Both are git-ignored, and CI fails on a committed `.har`.
- **Keep fbs and igs safe:** never push to `securebrand-microservices` or `securebrand-microservices-igs`. Every change to copied code keeps the copied unit, integration and e2e suites green; say so in the PR with the commands run. On shared infrastructure, never use one of fbs's or igs's topic, group, key, lock, bucket or metric names (ADR 0006 §5).
- **Facts vs hypotheses:** TikTok behaviour comes only from the HAR findings and the owner's probe and Phase 0 results. Anything else is labelled with its T-number. A new hypothesis gets the next T-number in `open-questions.md`.
- **No signature algorithms:** tks never re-implements or copies an implementation of TikTok's request signatures (ADR 0006 §4); signed calls go through TikTok's own script in a browser page.
- **Licences:** never copy code from a project whose licence we cannot use; read public projects for behaviour only, and cite them.
- **Match the repo's style:** Go idioms, comment density, naming, the config layering, the metric conventions, and the `docs/` tone.
- **Owner steps:** numbered, one command per block; files reach the droplet by `scp` from the Mac and come back by `scp` run on the Mac; no editing files on the droplet. **Always give all the steps, every time** (the owner's rule, 2026-11-01): from logging in (`ssh root@<droplet>`) and `cd` to the folder, through every command, to downloading the result on the Mac and uploading it, with what each step should show and what to do if it does not, even when earlier messages gave some of them. Never say "as before" or "steps 2 to 5 from last time".
- **Ask the owner** (AskUserQuestion, with a recommendation) only for decisions that are theirs. §2 is settled once approved.
- **At the end of each session,** update `build-status.md` (milestone states, PR links, findings, next step, what the owner must do) and reply with a short summary.
- **Start the next session yourself** (the owner's rule, 2026-11-01). When your milestone's PR is merged and the next milestone needs nothing from the owner first, start the next build session before you finish: `create_session` with the prompt at the end of this file, in this session's environment, on this repository, with this session's model and permission mode (never `plan`, which would wait for an approval no one gives). First check with `list_sessions` that no other tks build session is running; start exactly one; give its link in your summary and in the Log of `build-status.md`. **Do not start one** when you stop at an owner checkpoint (C-items) or decision (G-items, or a question to the owner still unanswered), when your PR is not merged, or when your milestone is unfinished: then the owner starts the next one. Sessions can start each other only to a limited depth (8 on the platform today); if `create_session` refuses, say so, and the owner starts the next one by hand, which starts the count again.

---

## 8. Acceptance criteria for the finished service

Measured in staging on the canary (C5) for 7 consecutive days, and in the end-to-end tests where marked. Each has a metric and an alert from M6 on. Numbers marked *after Phase 0* are provisional targets that C4's measurements confirm or change, with the owner's agreement.

| # | Criterion | Target | Measured as |
|---|---|---|---|
| A1 | **Scheduled checks that succeed** | ≥ **99.0 %** of scheduled list checks succeed within their 2 h 45 min window (retries included), per day; ≥ **99.5 %** of daily account refreshes succeed within 24 h; no account goes **more than 6 h** without a successful list check unless it is private, deleted or suppressed | `tks_checks_total{result}` per window; the oldest successful check per account |
| A2 | **Delay before a new post is stored** | ≥ **99 %** of new public posts queryable in Postgres within **3 h** of their `createTime`; median ≤ **1.5 h**; ≥ 99.9 % within **6 h** | `stored_at − createTime` for posts first seen by an incremental check (backfilled posts excluded) |
| A3 | **Backfill depth** | For ≥ **98 %** of new accounts, the backfill reaches **365 days or the account's first post** (list ended with `hasMore` false) within **48 h** of registration (*after Phase 0*: the time depends on the fleet's size). Every shortfall is reported with its reason (wall, cap, private, error) | per account: oldest stored `createTime`, stop reason |
| A4 | **Comment completeness** | ≥ **98 %** of comment reads end `complete` or at the configured cap (not `partial` or `truncated` by errors); for complete reads, collected top-level comments plus replies ≥ **90 %** of the post's `comment_count` at the median (*after Phase 0*, from T15's ratio); 100 % of comments reachable in `fakett` | `comments_state` counts; the collected ÷ count ratio |
| A5 | **Cost per account per day** | ≤ the **budget** the owner sets after Phase 0 (D9), reported daily as proxy cost + browser time + compute + storage ÷ active accounts, with an alert at 90 % of budget | `tks_cost_per_account_usd` from the budget module's counters |
| A6 | **Store equals the fake TikTok's ground truth** | In the end-to-end suite against `fakett` (with rate limits, captchas, signature-required calls, rotated variants, restarts): **100 %** of accounts, username histories, posts, latest metrics, top-level comments and replies match the ground truth; **0** duplicates; nothing collected is lost or deleted (D8); metric rows only on change | e2e assertions (M5, M6) |

---

## Prompt for a build session

Use the same prompt for every build session, started with the **`Abdullah95k/securebrand-microservices-tiktok`** repository selected.

```text
You are a build session for "tks", the logged-out scraper of public TikTok accounts, built in its own repository (Abdullah95k/securebrand-microservices-tiktok). tks starts as a copy of "igs", the Instagram scraper (Abdullah95k/securebrand-microservices-igs), which is a copy of "fbs", the Facebook scraper (Abdullah95k/securebrand-microservices). Read igs and fbs for reference (attach igs with add_repo, read access); never push to either.

1. Read docs/tiktok/build-brief.md completely, then docs/tiktok/build-status.md. Follow the brief's reading order (the HAR findings and probe results first, open-questions.md, methods.md, ADRs 0006 and 0007 in docs/tiktok/adr/, reuse-plan.md, probe-spec.md).
2. Find the next unfinished milestone in build-status.md and do it: think through the design first, verify every igs or fbs claim against the current code, then implement with tests, matching the repo's style. Open one draft PR per milestone, keep CI green, and merge it once CI is green; for copied code, run the unit, integration and e2e suites and show they pass.
3. Continue to the following milestone in the same session only if the current one is merged and context allows; otherwise stop cleanly.
4. Stop at owner checkpoints and decisions (C1–C5, G1) and tell me exactly what to do, in simple numbered steps with one command per block, always all the steps from logging in to sending the result back; I copy files to the droplet with scp from my Mac and do not edit files there.
5. Rules: no live requests to TikTok from this session; no paid services; never commit HARs, probe output, credentials or any real usernames, names, ids or comment text; never re-implement TikTok's request signatures; decisions in §2 of the brief are settled, so ask me (AskUserQuestion, with a recommendation) only about genuinely open trade-offs.
6. Before finishing, update docs/tiktok/build-status.md (milestone states, PR links, findings, next step, what I need to do) and give me a short summary.
7. If your milestone is merged and the next one needs nothing from me first, start the next build session yourself with this same prompt (brief §7: check no other tks build session is running, start one only, never at an owner checkpoint or decision) and give me its link.
```
