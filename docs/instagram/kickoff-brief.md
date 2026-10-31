# Instagram scraper (igs): kickoff brief

This brief starts the work on **igs**, a self-hosted scraper of public Instagram profiles built on the same design as **fbs**, the Facebook page scraper in this repository. It has two parts:

- **[Part 1](#part-1--what-the-owner-needs-to-decide-and-provide), for the project owner:** the decisions, data, tools and resources you need to provide. Fill in the "Your answer" columns before starting the planning session; the session will ask about anything left blank.
- **[Part 2](#part-2--brief-for-the-planning-session), for the planning session:** what is known, what is not, how to analyse it, and what to produce. That session thinks, analyses and plans. It does not build production code.

The prompt that starts the planning session is at the end: [Prompt](#prompt-to-start-the-planning-session).

Background reading: [`docs/fbs-reference/`](../fbs-reference/README.md), a full technical reference of fbs written from the code, with an Instagram build plan in [chapter 7](../fbs-reference/07-instagram-build-plan.md).

---

## TL;DR

- **Goal.** Collect, for a list of public Instagram profiles: the profile, every post (feed posts, carousels, reels) in an age window, their engagement over time, every comment and reply Instagram serves logged out, and the media files (temporarily, for the analysis service). The same scale, privacy and operational standards as fbs apply.
- **Approach.** Reuse most of fbs: the pipeline, Kafka, Postgres, Valkey, scheduling, rate control, media lifecycle, GDPR, deployment and tests. Rewrite the Instagram-specific parts: the client protocol, block detection, parsers, data model, stage bodies, a fake Instagram for tests, and Phase 0.
- **Biggest unknown.** How much logged-out Instagram serves before a login wall, especially comments. It decides whether the project is viable as scoped. Nobody should assume an answer: it is measured.
- **Order of work:**
  1. decisions and legal;
  2. logged-out reconnaissance HARs;
  3. the plan;
  4. shared-code extraction, `fakeig` and the Instagram client;
  5. the Phase 0 measurement;
  6. go/no-go;
  7. build;
  8. staging, canary, ramp.

---

## Part 1 — What the owner needs to decide and provide

### 1.1 Decisions only you can make

| # | Decision | Options | Suggested default | Why it matters | Your answer |
|---|---|---|---|---|---|
| D1 | **Data scope** | profile · feed posts · carousels · reels · comments · replies · media files · stories · highlights · tagged posts · follower or liker lists | Profile, feed posts, carousels, reels, comments, replies, media. **Not:** stories, highlights, follower or liker lists (mostly need login, and more personal data) | Drives queries, stages, schema and cost | |
| D2 | **Volume** | profiles tracked; new profiles/day to backfill; backfill window (days); re-scrape frequency; on-demand latency target | Start from your real list. fbs targets 300k pages/day + 2k backfills/day to 365 days | Sizes proxies, pods, Kafka partitions, budget | |
| D3 | **Access rule** | logged out only (as fbs, ADR 0001) · something else | Logged out only, no accounts | A legal and ethical line; changes the whole design | |
| D4 | **Fallback if Phase 0 says NO-GO** | narrow the scope · official API (Instagram Graph API Business Discovery: business/creator accounts only, needs your own professional account and a Meta app, no comment threads of other accounts; verify current capabilities) · paid data provider · stop | Decide now which ones are acceptable | The plan must branch on this | |
| D5 | **Media** | which files (images, video sd/hd, thumbnails, carousel children, reel covers); same analysis service; same 72h TTL and ack contract | Same contract as fbs; video `sd`, backfill `thumbnail` | Storage, egress, downstream contract | |
| D6 | **Infrastructure** | share the fbs Kafka/Postgres/Valkey/S3/cluster · separate | Share the clusters with separate topics, tables, lock keys and metric prefixes | Cost and blast radius | |
| D7 | **Code organisation** | share code with fbs (extract the generic parts) · fork a copy | Share (chapter 7 §2): fixes then land once | Speed now vs maintenance later | |
| D8 | **Privacy** | treat profiles of individuals (creators) as personal data · only commenters | Treat both as personal data: `pii:read` gating, erasure and suppression for profiles too | DPIA scope, API design | |
| D9 | **Budget** | monthly proxy ceiling; acceptable cost per profile per month | Set after the Phase 0 cost projection, with a provisional cap now | Pacing, tier choice, daily budget | |
| D10 | **Downstream contract** | separate `ig.*.v1` topics · a unified cross-platform schema · a `platform` field on the shared media handoff | Separate `ig.*` topics; shared media handoff with a `platform` field (ask the analysis team) | Consumers need to plan too | |

### 1.2 Legal and compliance (before production; ideally before Phase 0 at volume)

- [ ] Review of Instagram's / Meta's terms for automated logged-out collection.
- [ ] Legitimate-interest assessment (LIA) covering commenters **and** profiles that are individuals.
- [ ] DPIA, data-processing records, retention periods, erasure process.
- [ ] A named person who signs off (fbs's go-live runbook makes the same demand: `docs/runbooks/go-live.md`).

### 1.3 Data you need to provide

| Item | Format | How much | When | Notes |
|---|---|---|---|---|
| **Phase 0 profile list** | CSV, header `profile,size,category,region,language,account_type` (`profile` = username, `@username` or profile URL) | about 150 public profiles | before Phase 0 | Stratify by followers (nano, micro, mid, macro, mega), category (brand, news, creator, government) and region (US, EU, APAC). Put the busiest first: the first rows drive discovery and smoke tests |
| **Canary profiles** | list of usernames | at least 3, better 5 | before registry sync is built | Public, active, stable (unlikely to go private), with posts that have many comments **and** reply threads |
| **Reconnaissance HARs** | `.har` files ("Save all as HAR with content") | 2–3 sessions | **first, before planning is final** | See [how to record](#how-to-record-the-reconnaissance-hars). They decide most open questions |
| **Ground-truth samples** (optional, valuable) | table: post URL, comments shown logged in, comments shown logged out, replies | 10–20 posts | before Phase 0 | Calibrates "completeness". A paid data API can also provide reference counts |
| **Production profile list or source** | file, API or table | all | before go-live | Where profiles come from, and how they are added or removed |
| **Consumer needs** | the fields and events downstream services use | — | during planning | Contact the analysis service owner |

#### How to record the reconnaissance HARs

1. Use desktop Chrome in a **new private/incognito window, not logged in**, from a normal connection. Note the country. If possible, repeat once through a proxy exit you plan to use.
2. Open DevTools → Network. Tick **Preserve log** and **Disable cache**.
3. Visit `https://www.instagram.com/<public profile>/`. Scroll the grid slowly until it stops or a login wall appears. **Write down how far it got** (number of posts, date of the oldest post).
4. Open a post with many comments. Scroll its comments, click "View replies" on a few threads, and open a reel.
5. Right-click in the Network panel → **Save all as HAR with content**.
6. Treat the files as **sensitive**: they contain other people's data and session tokens. Keep them private and **never commit them**. Fixtures are produced from them by the scrubbing tools (`fbs-spike har` has an Instagram counterpart to be built).

### 1.4 Tools, accounts and infrastructure

| Item | Needed for | Notes |
|---|---|---|
| **Proxy providers**: at least 2 tiers. ISP/static (endpoint list) and rotating residential with sticky sessions (gateway URL, username template with country and session, sticky TTL); optionally mobile | Phase 0 and production | Note the **price per GB** of each tier. Credentials stay in a local `proxies.yaml` (`chmod 600`) or a Kubernetes secret, **never in chat or git**. Expect Instagram to need residential or mobile exits sooner than Facebook |
| **A machine to run Phase 0**: Linux or macOS, Go 1.27, Docker optional, Chromium for the browser fallback | Phase 0 | About 7 h per proxy tier at fbs's pace; a few GB of proxy traffic per tier. **The Claude cloud session cannot and should not reach Instagram through your proxies.** You run the measurement and hand back the scrubbed bundle |
| **Staging/production infrastructure**: Kubernetes (KEDA; Prometheus Operator optional) or a Docker host; Kafka/Redpanda with a Schema Registry; **Postgres 18** (required); Valkey; S3-compatible storage for media and raw captures; a container registry (GHCR); Prometheus and Grafana | build and go-live | Can be shared with fbs (D6) |
| **Secrets to generate**: `privacy.author_hmac_key` (permanent: rotating it orphans every author key), database and S3 credentials, API keys | go-live | Kept in your secret store |
| **GitHub**: repository access, CI with Docker services (the existing workflows use them) | build | Already in place for fbs |
| **Optional: a paid Instagram data API** (your Claude account has a ScrapeCreators connector) | Phase 0 calibration; D4 fallback | Reference counts for completeness, and an option if logged-out access fails. Its output shapes are not Instagram's web responses, so it cannot replace the HARs as parser fixtures |

### 1.5 People

- **Analysis service owner:** the media handoff and `ig.*` topic consumers.
- **Legal / DPO:** section 1.2.
- **Phase 0 operator:** runs the measurement and sends back the bundle (probably you).

### 1.6 Owner checklist

- [ ] D1–D10 answered (table above)
- [ ] Legal review started (1.2)
- [ ] 2–3 reconnaissance HARs recorded and stored privately, with the wall depth noted
- [ ] Phase 0 profile CSV (about 150) and canary profiles (3–5)
- [ ] Proxy tiers bought or trialled, with prices per GB
- [ ] Phase 0 machine ready
- [ ] Analysis service owner contacted
- [ ] PR #22 (`docs/fbs-reference`) merged, or its branch available to the planning session

---

## Part 2 — Brief for the planning session

### 2.1 Your role and what you produce

You are the **planning session** for igs. Your job is to **think, analyse and plan**: establish what can be reused from fbs, what must be built, in what order, what is uncertain and how to resolve it, and what the owner must decide or provide. The result is a decision-ready plan that implementation sessions can execute milestone by milestone.

- **Do not write production code in this session.** You may run commands that verify claims: `go build ./...`, `go test` on a package, `go list -deps`, `grep`.
- **Produce these files** on your branch and open a draft PR:
  - `docs/instagram/plan.md`: the implementation plan (template in §2.8);
  - `docs/instagram/open-questions.md`: questions for the owner, each with options, your recommendation and what it blocks;
  - `docs/adr/0004-instagram-code-organisation.md`: the share-or-fork decision. Add more ADRs for other decisions of that weight (surfaces, data model, ids). Match the format of the existing ADRs.
- **End by** summarising the plan and listing the open questions in your reply.

### 2.2 Read first, in this order

1. This brief, and Part 1's answers (the owner may have filled them in).
2. [`docs/fbs-reference/README.md`](../fbs-reference/README.md): the system in one page, and the known fbs defects.
3. [`docs/fbs-reference/07-instagram-build-plan.md`](../fbs-reference/07-instagram-build-plan.md): the reuse matrix, data model, stage graph, `fakeig`, Phase 0 plan and build order. Treat it as a **first draft to challenge**, not as a decision.
4. The chapters as needed: [1 client](../fbs-reference/01-client-layer.md), [2 registry/doc_ids/browser](../fbs-reference/02-query-registry-docids-browser.md), [3 parsing/model/fakefb](../fbs-reference/03-parsing-data-model-fakefb.md), [4 crawl pipeline](../fbs-reference/04-crawl-pipeline.md), [5 backend](../fbs-reference/05-backend-services.md), [6 Phase 0/deploy/CI/tests](../fbs-reference/06-phase0-deploy-ci-tests.md). Each ends with "Instagram adaptation notes".
5. ADRs 0001–0003 and the runbooks (`phase0-spike.md`, `operations.md`, `go-live.md`).

**The reference was written at commit `265ae3a`. Main has moved since.** For example, `1d0bc81` changed timeline parsing, the spike and Chromium start-up. Re-check every `path:line` you rely on against HEAD, and note where the code differs from the reference.

### 2.3 What is established (facts about fbs; do not re-derive)

- **Access and protocol.** fbs works logged out. A Chrome-identical TLS/HTTP2 client bootstraps tokens from page HTML. It calls the web GraphQL endpoint with persisted-query doc_ids kept in a hot-reloaded registry, which `fbs-registry-sync` keeps current. A headless-Chromium fallback is capped at 2% of loads.
- **Parsing.** Parsers walk the JSON by `__typename` with candidate paths. They report missing required fields as drift, which feeds a per-(query, doc_id) breaker, shadow parsers and raw-failure replay.
- **Pacing.** Every response is classified into 9 classes. The class feeds identity health, an exit-bucket breaker and a fleet AIMD ceiling, on top of a Redis GCRA.
- **Pipeline.** Kafka work queues have one topic per stage and lane (p0 on-demand > p1 scheduled > p2 backfill). Tasks are short and idempotent, with continuations, leases, checkpoints and `done:` markers. Jobs are accounted in Lua, and "completed" means "queryable".
- **Storage.** Postgres 18 is partitioned monthly by post time. The persister uses COPY + no-op upserts with `RETURNING old/new` change detection. Media is temporary, with an ack contract. GDPR covers HMAC author keys, erasure and suppression.
- **Operations and tests.** Adaptive scheduling tiers, comment refresh by post age, and a daily budget. Phase 0 is a one-command, resumable measurement with a go/no-go verdict and a cost projection. The test pyramid rests on `fakefb`. Helm, CI, 25 alerts and a dashboard are in place.
- **Known defects** (fbs reference index): DLQ replay skipped by `done:` markers; the persister retrying a bad batch forever; no job ownership checks; rune-vs-UTF-16 text offsets; and others. Decide which to fix in shared code before igs depends on it.

### 2.4 What is unknown (Instagram); nothing below is a fact until verified

| # | Question | Why it matters | How to resolve | Blocks |
|---|---|---|---|---|
| U1 | How far does a profile grid paginate logged out before a wall (posts, days)? | Backfill window, viability | HARs (owner's notes), then Phase 0 | scope, D2 |
| U2 | Is any comment pagination served logged out, or only a preview? Child comments? | Viability of "all comments", completeness metric | HARs, then Phase 0 | scope, comment stages |
| U3 | Which surfaces carry the data: web GraphQL (`/graphql/query` or `/api/graphql`), REST `/api/v1/...`, embedded HTML JSON? | Client design (one or two request kinds) | HARs | client |
| U4 | Required tokens, cookies and headers (`csrftoken`→`x-csrftoken`, `x-ig-app-id`, `lsd`, `x-asbd-id`, `x-ig-www-claim`, …), and where each comes from | Bootstrap, identity model | HARs | client |
| U5 | Does a plain HTTP profile load yield the tokens, or is JavaScript needed? | Browser fallback share, cost | HARs + a test load | client, cost |
| U6 | Are doc_ids declared in static bundles (which shape? which CDN host?), or only visible at runtime? | Whether registry sync can follow rotations automatically | HARs + bundle inspection | registry sync |
| U7 | Error and block signals (429, 401 `require_login`, redirects to `/accounts/login`, `checkpoint_required`, persisted-query errors) | Detection rules, AIMD, alerts | HARs, Phase 0 failures | client |
| U8 | Are cursors bound to the session or CSRF token? | Task affinity design | Phase 0 portability check | crawl stages |
| U9 | Count semantics: does `comment_count` include child comments, and hidden or filtered ones? | Completeness and deletion detection formulas | HARs + ground truth | finalizer |
| U10 | Does the grid include all reels, or is a separate clips stream needed? | Stage graph | HARs | stages |
| U11 | Bytes per request and requests per profile | Cost per profile, budget, tier choice | Phase 0 | D9, sizing |
| U12 | Per-IP tolerance (pace vs block rate), per tier | `identity_rps`, pool sizes, proxy spend | Phase 0 pacing sweep | config |
| U13 | Signed CDN URL lifetime (`oe=`) | Media downloader urgency | Phase 0 | media |
| U14 | doc_id rotation frequency | `registry_sync.interval` | Phase 0 + a week of monitoring | registry sync |

If the owner's HARs are available, analyse them first and answer whatever they settle, citing the request in the HAR. **Never commit raw HARs.**

**Read [`har-findings-2026-10-30.md`](har-findings-2026-10-30.md) first.** It analyses the owner's 9 logged-out recordings and settles U2–U5, U9 and parts of U1 and U7: the operations and doc_ids, tokens, data availability, comment coverage, and the 24-post grid limit in the UI. Treat it as evidence that outranks the hypotheses below.

**Owner constraints (October 2026):** logged out only; **no official Instagram API and no third-party scrapers** (Apify, ScrapeCreators and the like). Only our own tool. If logged-out access is too limited, the scope narrows (D4). The owner's answers and recording notes are kept in a Claude doc linked from the planning prompt; read it with the docs tools.

**Web research notes (October 2026, unverified; mostly scraper vendors' reports, so treat them as leads):**
- Logged-out profile and single-post data now come mainly from **JSON embedded in the page HTML**. A profile's post feed still pages through `graphql/query` with a `doc_id` ([Scrapfly, 2026](https://scrapfly.io/blog/posts/how-to-scrape-instagram)).
- `api/v1/users/web_profile_info/` reportedly answers HTTP 400 "log in" for a growing share of public profiles, and 429 after a handful of requests from one IP. Do not build on it without testing.
- Throttling is **per IP**, datacenter ranges are throttled sooner, and the login wall can come back as **HTTP 200**. Detection must classify the wall in the body, not only by status code.
- **Comments are largely gated for anonymous users**; at most the visible or preview comments come back logged out. The embed page (`/p/<code>/embed/captioned/`) reportedly gives caption, type, comment count and thumbnail, but not likes or exact time ([Apify actor notes](https://apify.com/cynix_dev/instagram-post-scraper-comments-metadata/changelog.md)). Search and hashtags require login.
- Consequence for planning: plan for **scenario B** (posts, metrics and visible comments, with completeness reported) as the likely case, and keep scenario A as the upside if the HARs and Phase 0 show more.

### 2.5 How to analyse: the method

Work through these steps and write your reasoning into the plan. Where you make a judgement call, say what would change your mind.

1. **Goals and constraints.** Restate the goals from Part 1. For each unanswered D1–D10, propose a default with its trade-off, and ask the owner (batch the questions, at most four at a time). Do not silently assume an answer to D3, D4 or D8.
2. **Verify the reuse matrix** (chapter 7 §2) against HEAD. For every package under `internal/fb`, `internal/crawl`, `internal/platform`, `internal/{api,dispatch,persist,scheduler,media,gdpr,replay,spike,fixtures}`:
   - list its imports of Facebook-specific code (`gen/go/securebrand/fb/v1`, `internal/fb/parse`, `htmlx`, `detect`, the seed queries) with `go list -deps` or by grepping imports;
   - classify it as (a) reuse as-is, (b) parameterise, (c) rewrite;
   - size the refactor (S/M/L).

   Draw the package dependency graph, showing where the Facebook coupling sits.
3. **Code organisation (ADR 0004).** Compare "extract shared packages" with "fork" on these criteria: time to the first Phase 0 run, risk to fbs (its tests must stay green), duplication of fixes, test coverage, team size. Propose the neutral package layout and the **minimal platform interfaces** for the extraction:
   - client parameters: tokens, form and header builder, wall paths, REST request kind;
   - detect rules;
   - parser set;
   - stage bodies and scopes;
   - ref normalisation.
4. **The Instagram client.** Design the request kinds, token struct, identity lifecycle numbers, detect rules and registry seed shape, with each Instagram detail marked as a hypothesis linked to its U-question.
5. **Data model and contracts.** Draft:
   - `securebrand.ig.v1` messages, topics and keys, and the shared `common.v1` question;
   - migrations: profiles, posts with shortcode, collab owners, carousel children, hidden counts as NULL, two-level comments, metrics;
   - id types (int64 vs string);
   - PII classification.
6. **Stage graph and envelope.** Profile → (reels?) → comments → replies → media. Cover incremental strategy variants (with and without a newest-first ordering), deletion-detection formula variants (U9), and session affinity if U8 says cursors are bound.
7. **`fakeig`.** Specify the world, endpoints, renderers, faults and admin API needed for each test layer, and the smallest slice that unblocks the first client test.
8. **Instagram Phase 0.** Specify `igs-spike`:
   - steps, measurements and report fields (fix fbs's gaps: report latency, count every bootstrap, separate depth limited by the page cap);
   - the bundle and the owner's run procedure;
   - how go/no-go thresholds are set **after** reconnaissance.
9. **Scenario planning.** Make the plan branch explicitly:
   - **A, full access:** comments paginate, the grid goes deep. Build everything.
   - **B, partial access:** the grid is fine, but comments are a preview only. Narrow the scope and redefine completeness.
   - **C, walled:** profiles are walled almost at once. NO-GO for logged out; the D4 options.

   Order the milestones so that work done before Phase 0 stays valuable in every scenario: extraction and fixes, contracts, `fakeig`, the client against `fakeig`, the spike kit.
10. **Risks.** Top risks with likelihood, impact, mitigation and **kill criteria**: access, cost per GB, doc_id discovery, cursor binding, legal, persister and DLQ defects inherited from fbs.
11. **Fix-first list.** Which fbs defects to fix in shared code before igs depends on it, each with an owner milestone.
12. **Milestones.** For each: goal, deliverables, packages, tests, **acceptance criteria**, size, dependencies, and **what is needed from the owner, and when**. Mark the critical path.

### 2.6 Rules for this session

- **Facts vs hypotheses.** Cite `path:line` at HEAD for claims about fbs. Label every Instagram claim as a hypothesis, with the U-question that verifies it.
- **Do not contact Instagram** from the cloud session, and do not invent response shapes, field names or doc_ids as facts. Shapes in the plan are illustrative until captured.
- **Never ask for, store or commit credentials.** `proxies.yaml` stays on the owner's machine. Never commit raw HARs or unscrubbed responses.
- **Keep fbs safe.** Any proposed shared-code change must state how fbs's tests (unit, `make test-integration`, `make e2e`) prove it unchanged.
- **Ask, don't guess,** on decisions that belong to the owner (Part 1). Give a recommendation with each question.
- Keep the plan concrete enough that each milestone can be handed to a fresh implementation session with one prompt. Include that prompt per milestone.

### 2.7 Inputs you may receive later

- **Phase 0 bundle** (from the owner): `report.json`/`report.txt`, `docids.json`, `registry.yaml`, scrubbed `fixtures/`, `failures/`, `run-*.jsonl`, `meta.json`. When it arrives, re-plan against scenarios A, B and C, and update the thresholds, config and sizing.
- **HAR-derived fixtures:** they become golden test cases under `testdata/ig/...` after scrubbing.

### 2.8 Template for `docs/instagram/plan.md`

```
# igs implementation plan
1. Summary and recommendation (one page; the scenario we are planning for; what starts now)
2. Goals, non-goals, constraints (D1–D10 as answered or as assumed, with the question to confirm)
3. Assumptions and how each is verified (link to U1–U14)
4. Architecture and code organisation (ADR 0004 summary, package layout, platform interfaces, dependency graph)
5. Component plan (table: component | reuse class | change | size | tests | milestone)
6. Data model and contracts (protos, topics, migrations, ids, PII)
7. Client design (request kinds, tokens, identity, detection, registry seed) — hypotheses marked
8. Stage graph and task envelope (with incremental and deletion variants)
9. fakeig specification
10. Instagram Phase 0 (igs-spike, procedure for the owner, report, threshold-setting)
11. Scenarios A/B/C and what changes in each
12. Milestones (goal, deliverables, acceptance criteria, size, dependencies, owner inputs, prompt for the implementation session); critical path
13. Risks, mitigations, kill criteria
14. fbs fixes to land first
15. What the owner must provide, by milestone
16. Open questions (also in open-questions.md)
```

---

## Prompt to start the planning session

Before starting, merge PR #22 (it adds `docs/fbs-reference/` and this brief), or tell the session to use its branch. Then paste:

```text
You are the planning session for "igs", an Instagram public-profile scraper to be built in this repository (Abdullah95k/securebrand-microservices) on the same design as "fbs", the Facebook page scraper already here.

Start by reading docs/instagram/kickoff-brief.md completely and follow its Part 2. It tells you what to read (docs/fbs-reference/, ADRs, runbooks), what is established about fbs, what is unknown about Instagram, the analysis method, the rules, and the deliverables. If docs/instagram/kickoff-brief.md or docs/fbs-reference/ is missing on your checkout, fetch and check out origin/claude/beautiful-franklin-kphaix (PR #22), which adds them.

Your job is to think, analyse and plan, not to implement:
- Verify the fbs reference against the current code (it was written at commit 265ae3a; main has moved), especially the reuse matrix in docs/fbs-reference/07-instagram-build-plan.md, using go list, grep and builds as evidence.
- Treat every Instagram detail as a hypothesis unless the owner gives you captures; link each to the open question that verifies it.
- Reason through the trade-offs explicitly (share vs fork, client surfaces, data model, stage graph, fakeig, Phase 0, scenarios A/B/C, risks and kill criteria) and record the reasoning, not just the conclusions.
- Ask me, with AskUserQuestion, about owner decisions that are unanswered in Part 1 of the brief (D1–D10), proposing a default for each. Do not assume answers to the access rule, the NO-GO fallback, or the privacy scope.
- Do not write production code. Do not contact Instagram. Never ask for or commit credentials or raw HAR files.

Deliverables: docs/instagram/plan.md (using the template in the brief), docs/instagram/open-questions.md, and docs/adr/0004-instagram-code-organisation.md (plus other ADRs for decisions of similar weight), committed on your branch with a draft PR. Finish with a summary of the recommended plan, the milestones on the critical path, what I need to provide and when, and the open questions.

My answers to Part 1 (leave blank if not decided yet):
- D1 data scope:
- D2 volume:
- D3 access rule:
- D4 NO-GO fallback:
- D5 media:
- D6 infrastructure:
- D7 code organisation:
- D8 privacy:
- D9 budget:
- D10 downstream contract:
- HARs available? (upload them to this session; never commit them):
- Canary profiles / Phase 0 CSV ready?:
```
