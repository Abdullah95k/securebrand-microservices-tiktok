# tks reuse plan: what tks takes from igs

igs (`Abdullah95k/securebrand-microservices-igs`) owns a copy of fbs's code, reshaped so that a second platform plugs in through data and small hooks. This plan classifies every part of igs for TikTok. The decision it supports, that tks starts as a copy of igs, is [ADR 0006](adr/0006-tks-code-organisation.md). The data model is [ADR 0007](adr/0007-tks-data-model-and-contracts.md).

It was checked against **igs main `9463b91`** (2026-10-31), where igs's milestones M0, M1 and M4 are done and M2, M3 and M5 to M10 are not. Line counts are Go source / test lines. igs's own package-by-package audit of fbs is [igs `docs/instagram/reuse-matrix.md`](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/instagram/reuse-matrix.md); this plan builds on it and does not repeat its evidence.

**Classes:**
- **as is:** copied and used unchanged. Only the names that would collide on shared infrastructure change (M1b).
- **change:** copied, then given a seam for TikTok. Facebook's and Instagram's behaviour stays the default, so the copied tests keep passing.
- **new:** tks writes its own, copying the structure of the igs code named.
- **doesn't fit:** TikTok has nothing for it to do. Kept only while copied code needs it.

TikTok behaviour named below is a hypothesis with its T-number ([open-questions.md](open-questions.md)) until the owner's recordings or the probe confirm it.

---

## 1. Summary

| Area | as is | change | new | doesn't fit |
|---|---|---|---|---|
| Scraping layer (`internal/scrape/*`) | transport, proxy, ratelimit, session, htmlx, walk | graphql (GET + decoder), registry (method, variants), detect (JSON status, empty answers), browser (persistent page) | — | docids |
| Instagram code (`internal/ig/*`, `fakeig`, `cmd/igs-spike`) | — | — | `tt/htmlx`, `tt/detect`, `tt/client`, `tt/sign`, `tt/tttest`, `tt/probe`, `fakett`, `cmd/tks-spike` (each copying its igs counterpart's shape) | the Instagram code itself (removed at the seed) |
| Crawl pipeline (`internal/crawl/*`) | lanes | worker, task, checkpoint, jobs, emit, rawsink, shadow (as igs's matrix §4, on tks's envelope) | account and post stages | — |
| Backend (`api`, `persist`, `scheduler`, `media`, `gdpr`, `store`, `dispatch`) | — | the cores igs's matrix §5 names | writer, planner, data routes, eraser | — |
| Platform (`internal/platform/*`, `config`) | breaker, config loader, leader, log, pg, redisx, run, s3x, schemareg | metrics namespace, kafka header, config root | — | — |
| Contracts | `common.v1` (byte-identical to igs's, once igs's M3 lands) | — | `tt.v1`, `tks.task.v1`, `migrations/tt`, `configs/tt/` | `fb.v1`, `fbs.task.v1` (go with the copied Facebook code) |
| Tools and tests | — | Phase 0 orchestration (`internal/spike`, igs's M8 `Measurer`) | `fakett`, e2e stack, `tksctl`, `cmd/tks-*`, Helm chart | `registrysync`'s doc_id discovery (replaced by a canary check) |

**What no TikTok code can avoid:** the page data and its tokens, the GET request format and its signatures, detect rules, entity parsers, stage bodies, the data model, `fakett`, the probe and the Phase 0 measurement. Everything else is shared.

---

## 2. Scraping layer: `internal/scrape/*`

| Package | Lines | What it does in igs | TikTok fit | Class | Change for TikTok |
|---|---|---|---|---|---|
| `transport` | 279 / 0 | Chrome-like TLS and HTTP/2 through `bogdanfinn/tls-client`; header order kept; wire bytes counted | TikTok is reported to check the TLS fingerprint (T5). Any method, any body | as is | Add a current Chrome profile (igs's M2 item 6 adds `chrome_152`; the pinned tls-client ships it) |
| `proxy` | 258 / 123 | Tiers (`static_list` for ISP, `gateway` for residential), exit buckets, breakers | The owner's ISP tier first | as is | — |
| `ratelimit` | 649 / 304 | GCRA limits, the AIMD fleet ceiling, the daily request and byte budget | Same need | as is | Keys under `tks:` (M1b) |
| `session` | 636 / 257 | Identities (exit + fingerprint + cookies + tokens), pool, a fingerprint per identity, HTML loader with `Hooks{Extract, IsWall, AfterLoad}` | TikTok's page carries the values its web client sends (device id, region, `odinId`, cookies such as `ttwid` and `msToken`: T2, T3) | as is | TikTok's hooks live in `internal/tt/client` (data, not an engine change) |
| `htmlx` | 63 / 52 | `JSONBlobs`, ordered pattern extraction | The profile and video pages embed one JSON document (`__UNIVERSAL_DATA_FOR_REHYDRATION__`, T1) | as is | — |
| `walk` | 415 / 99 | Typename-less walker, `Report{Missing,Optional}`, number and time helpers | TikTok's JSON is plain REST: gjson paths do most of the work; the helpers and `Report` still apply | as is | — |
| `detect` | 266 / 77 | Response classes; rules as data (codes, paths, markers) | TikTok answers carry `statusCode`/`status_code` and `status_msg`, not GraphQL `errors`; a rejected signature is reported as HTTP 200 with an **empty body** (T4); captcha pages (T6) | change | The decoder seam (below) maps TikTok status codes onto the existing code lists; an "empty 200" rule classes such an answer `RegistryStale`, so the client falls back to the next variant (for example the signed one); captcha paths and markers as data |
| `graphql` | 380 / 131 | Pacing (identity, per-subject, global, AIMD ceiling), classification, metrics, breakers per (query, variant), raw capture, fallback across registry entries. The request is a POST form; the answer is NDJSON | TikTok's web API is **GET with a query string**, and some calls need a signature computed over that query string (T4) | change | `Built` gains `Method` and `Query`; the `RequestBuilder` gets the full request, so it (or a signer) can sign the final URL; a platform `Decoder` turns a body into parts, errors and has-data (fbs and igs keep `SplitParts` and GraphQL `errors`). The package keeps its name so igs's later fixes cherry-pick cleanly; its doc comment says it is the persisted-query and JSON web-API client |
| `registry` | 524 / 181 | Logical query → doc_id, friendly name, variables template, per-query endpoint and headers; DB-backed; fallbacks | TikTok has no doc_ids, but its operations do drift: paths (`/api/post/item_list/` vs `/api/creator/item_list/`), parameters and signing needs (T4, T25) | change | A `method` field; the variables template renders to an ordered query string for GET; the `doc_id` column holds a **variant label** (`web`, `web-signed`, `creator`), so the existing fallback and breaker logic switches endpoint or signing without new machinery |
| `docids` | 142 / 86 | Finds doc_ids in JavaScript bundles | TikTok has no doc_ids | doesn't fit | Not used by tks. Detecting a new signing script version, if ever needed, is new code in `internal/tt/sign` |
| `browser` | 1,021 / 378 | Minimal CDP client; one fresh headless Chromium per render; network capture; the capped bootstrap fallback | Two TikTok uses: bootstrap cookies when plain HTTP is refused (T3), and the **signing fallback**, which runs requests through TikTok's own page script (T4, T7) | change | A persistent page session (`browser.Session`: open on an identity's exit and fingerprint, keep it, `Evaluate`, read cookies, capture GET query strings), reused for many requests. The one-shot render stays for bootstrap |

Unchanged engine guarantees carry over: header order on the wire, wire bytes counted per request (what a proxy bills), Ctrl-C treated as a stop, credentials redacted in errors, the boundary test that `internal/scrape` imports no platform code (extended to `internal/tt`).

---

## 3. Instagram code

| igs path | Lines | Class | What tks writes instead |
|---|---|---|---|
| `internal/ig/htmlx` | 82 / 52 | new | `internal/tt/htmlx`: the rehydration document's scopes (`webapp.app-context`, `webapp.user-detail`, `webapp.video-detail`: T1), the values the web client needs |
| `internal/ig/detect` | 24 / 25 | new | `internal/tt/detect`: status codes, empty answers, captcha and login paths (T4, T6) |
| `internal/ig/client` | 157 / 340 | new | `internal/tt/client`: page hooks, the GET request builder (parameters in the browser's order, Chrome client hints), the operation seed `configs/tt/operations.seed.yaml`, the `Signer` seam (`none`, `browser`) |
| — | — | new | `internal/tt/sign`: the browser signer on `browser.Session` (T7) |
| `internal/ig/igtest` | 89 / 0 | new | `internal/tt/tttest`: client tests against `fakett` over both transports |
| `internal/ig/probe` | 1,054 / 281 | new, same skeleton | `internal/tt/probe`: the same `Options`/`Step`/`Report`/`Summary`/`Cost`, CSV writer, bundle, raw keeping, images, progress lines and stop-as-stop; TikTok's flow ([probe-spec.md](probe-spec.md)) |
| `cmd/igs-spike` | 217 / 80 | new, same skeleton | `cmd/tks-spike`: flags, `--dry-run` against the built-in fake, the stack, the accounts file reader (`user`, `@user`, `tiktok.com/@user` links) |
| `internal/fakeig` | 644 / 0 | new, same skeleton | `internal/fakett`: seeded world with ground truth, pages that mint cookies, the operations checked the way TikTok checks them, faults (below) |
| `configs/ig/queries.seed.yaml` | — | new | `configs/tt/operations.seed.yaml` |

These paths are removed in tks's seed commit (M1a): they would never run, and keeping them would invite edits to code tks does not own. They stay in igs's history, which tks keeps.

**`fakett` faults** (the TikTok counterparts of `fakeig.Faults`): signature required (an empty 200 for unsigned calls; the fake page serves a small script that signs `fetch` calls, so the browser signer is tested end to end where Chromium is available), captcha on page loads or API calls, rate limits, list walls at a depth, cursors bound to an identity, comments ordered by relevance rather than time, replies, pinned posts out of date order, photo posts, ads, private and deleted accounts, renamed usernames.

---

## 4. Crawl pipeline and backend

At `9463b91` igs still runs fbs's pipeline on fbs's envelope; igs's own envelope, contracts and stages come with its M3, M6 and M7. tks takes the same classes as [igs's matrix §4–§5](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/instagram/reuse-matrix.md#4-crawl-framework) on its own envelope (`securebrand.tks.task.v1`). Where igs has already generalised a package by the time tks needs it, tks ports igs's change instead of writing its own.

TikTok-specific points:
- **The list check also measures.** The profile's video list is expected to return each item in full, with its counts, music, hashtags and mentions (T9). Then the account stage both discovers new posts and refreshes metrics, with no per-post detail request (Instagram needs one post page per new post).
- **Replies may come back.** If TikTok serves replies logged out (T14), the post stage needs fbs's thread pattern, which igs dropped: a post task reads top-level pages and spawns reply reads per comment. fbs's completeness rule Σ(1 + reply_count) = comment_count may then hold again (T15), so keep fbs's per-post passes and seen lists in `crawl/jobs`.
- **Comment order decides the refresh cost.** fbs and igs refresh comments newest first and stop at the first stored one. If TikTok orders top-level comments by relevance (T13), that early stop does not work, and a refresh re-reads the thread up to a cap. ADR 0007 §5 has both paths.
- **Pinned posts come first** in the list regardless of date (T10): fbs's pinned-post exemption applies to the watermark.
- **The ids carry time.** A post id's top 32 bits are reported to be its Unix time (T8). A backfill can stop at the age window from ids alone, as igs does with Instagram pks.
- **Schedule.** The list check runs every 2 h 45 min, not 3 h, to meet the delay target (build brief §8).

---

## 5. Probe, Phase 0, fake site, contracts, deploy

| Asset | Class | Note |
|---|---|---|
| `internal/spike` (Phase 0 kit) | change | Port igs's M8 `Measurer` when it lands; otherwise tks adds it. tks's measurer counts signer calls and browser time as costs |
| `internal/e2e`, `internal/testenv` | new / change | The stack copies igs's (topics `tt.*`, groups `tks-*`); `testenv` with key prefixes |
| `registrysync` | doesn't fit, then new | Its doc_id discovery has no TikTok equivalent. `tks-canary` copies its skeleton (lock, run records, metrics) to check each operation variant on canary accounts and promote or demote variants |
| `api/proto/securebrand/common/v1` | as is | Copy igs's file byte for byte when igs's M3 lands, so one consumer reads all three platforms' media handoff and job events with one schema |
| `api/proto/securebrand/tt/v1`, `tks/task/v1` | new | ADR 0007 §2 |
| `migrations/tt` | new | Own goose table `tt.goose_db_version` |
| `deploy/helm`, compose, CI | change | Copy igs's once igs's M10 lands; otherwise copy fbs's chart as igs plans |

---

## 6. Not GraphQL: what that changes in practice

| GraphQL (fbs, igs) | TikTok web API (hypotheses T2–T4, T11, T12, T25) | Where it lands |
|---|---|---|
| POST form to one or two endpoints | GET to several paths under `/api/`, query string in a fixed order | `graphql.Built{Method, Query}`, `registry.method` |
| doc_ids rotate; found in bundles | Paths and parameters drift; no ids to discover | Registry variants + `tks-canary` instead of doc_id discovery |
| Tokens from the page go in the form (`lsd`, `fb_dtsg`) | Values from the page and cookies go in the query string (device id, `msToken`, region, language) | `tt/client` builder |
| No request signature | Some calls need `X-Bogus`/`X-Gnarly` computed over the query string (T4) | `Signer` seam; `tt/sign` |
| NDJSON with `data` and `errors` | One JSON object with `statusCode` and `status_msg` | `Decoder` seam; `tt/detect` |
| Opaque cursors | List cursor reported to be a time in ms (T11); comment cursor an offset (T12) | Parsers; checkpoints store the cursor as a string either way |
| Persisted-query errors mean a stale doc_id | An empty 200 means an unsigned or badly signed call (T4) | `RegistryStale` → fallback to the signed variant |

---

## 7. What tks inherits that igs has not fixed yet

At `9463b91` igs's M2 is not done, so tks's copy carries the defects in igs's brief §5 and the unknown-kind crash: DLQ replay doing nothing for 7 days, the persister retrying a bad batch forever, jobs without an owner check, suppression enforced only by the crawler, `max_comments_per_post` per segment, hard-coded detect behaviour (400/401/403 as OK, a fixed 60 s cooldown). tks ports igs's fixes by cherry-pick when they land (the paths are the same), or makes them itself in tks's M2 if igs has not by then, and records either in `UPSTREAM.md`. None of them affects the probe, which uses only the scraping layer.
