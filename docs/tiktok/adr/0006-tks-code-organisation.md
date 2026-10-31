# ADR 0006: tks code organisation

Status: accepted (proposed in the kickoff session; the owner chose "copy igs now" on 2026-10-31).

Numbering: tks starts as a copy of igs (§1), whose `docs/adr/` holds ADRs 0001 to 0005 (fbs's three and igs's two). tks's ADRs continue the sequence from 0006 so references stay unambiguous in the copied history. They live under `docs/tiktok/adr/` with the rest of tks's plan.

## Context
tks is the logged-out scraper of public TikTok accounts. It joins fbs (Facebook pages, `Abdullah95k/securebrand-microservices`) and igs (Instagram profiles, `Abdullah95k/securebrand-microservices-igs`). The owner set these rules (build brief §2):
* tks has **its own repository**. The owner calls it tks; on GitHub it is `Abdullah95k/securebrand-microservices-tiktok`.
* tks sessions **never push** to fbs or igs. They read them for reference.
* In testing and staging, tks runs on **fbs's infrastructure** (Kafka, Postgres 18, Valkey, S3) with its own names for every topic, table, group, lock, bucket and metric.

Facts that shape the choice:
* **Go cannot import another module's `internal/` packages.** All of fbs's and igs's reusable code is under `internal/`. Sharing it would mean moving it to public packages in igs's repository, which tks sessions may not touch.
* **igs has already done the generalisation tks needs** (igs's M1, merged 2026-10-31): a neutral scraping layer in `internal/scrape/*` that imports no platform code (enforced by a test); loader hooks; a request-builder seam; detect rules as data; per-query endpoints and headers; a fingerprint per identity; every Valkey key, lock, group, header, bucket and metric name parameterised, with tests that fail on a bare or foreign name. fbs has none of this.
* **igs also built the probe pattern** the owner wants for TikTok (`igs-spike probe`, `make igs-probe`, the runbook), and a fake site with faults (`fakeig`) that the probe rehearses against.
* **igs is mid-build.** At `9463b91` its M2 (shared defect fixes), M3 (contracts) and M5 to M10 are not done. Its pipeline still runs fbs's copied code on fbs's envelope.
* **TikTok's web API is not GraphQL** ([reuse-plan.md §6](../reuse-plan.md#6-not-graphql-what-that-changes-in-practice)): GET requests, query strings that may need a signature, JSON status codes. The scraping layer needs three seams for that, not a new client.

## Decision

### 1. tks starts as a copy of igs, with igs's history
* **Repository:** `Abdullah95k/securebrand-microservices-tiktok`. Go module `github.com/abdullah95k/securebrand-microservices-tiktok`.
* **Seed (M1a):** igs's main at a pinned commit (today `9463b91`; the M1a session re-pins to igs's main of that day and records it), merged into tks's `main` with `git merge --allow-unrelated-histories`, so `git blame` reaches through igs into fbs and cherry-picks from igs keep working. tks's `main` already holds this plan; the two histories have no file in common except `README.md` and `.gitignore`, which keep tks's text plus igs's ignore rules.
* **Seed commit**, in the same PR: rename the module; remove the Instagram-only code (`internal/ig`, `internal/fakeig`, `cmd/igs-spike`, `configs/ig`, `docs/instagram`, `docs/runbooks/igs-probe.md`, the `igs-probe*` Makefile targets); add `UPSTREAM.md`. **The copied Facebook code stays**, as in igs: fbs's unit, `fakefb` end-to-end, chaos and Phase 0 dry-run tests are the only tests of the copied pipeline, so they remain tks's regression net until tks's `fakett` suites cover the same code (M9 removes them). Facebook and Instagram code is never run against Facebook or Instagram from tks.
* **Upstream record.** `UPSTREAM.md` names the igs commit (and, through it, fbs's `f1377a9`) and lists every change ported from igs or fbs, and every tks fix the others may want. Porting is `git cherry-pick` from an `igs` remote while paths match. tks keeps igs's paths for all shared code for that reason.
* **igs's open milestones are ported, not redone.** When igs merges its M2 fixes, M3 contracts (`securebrand.common.v1`), M6 to M8 pipeline generalisation and Phase 0 kit, or M10 deployment, tks cherry-picks them. tks orders its own milestones so it reaches each of those areas after igs, where possible (build brief §6). If igs has not reached one when tks needs it, tks does the work itself in igs's shape and lists it in `UPSTREAM.md` for igs.

### 2. Layout of the tks repository (after M1)

| Path | Holds | From |
|---|---|---|
| `internal/scrape/*` | the neutral scraping layer | igs, with the three seams of §3 |
| `internal/crawl/*`, `internal/platform/*`, `store`, `persist`, `scheduler`, `api`, `media`, `gdpr`, `dispatch` | pipeline and services | igs (fbs's code), generalised as igs does |
| `internal/tt/htmlx` | page data: the rehydration document and the values the web client needs | new |
| `internal/tt/detect` | TikTok's detect rules | new |
| `internal/tt/client` | page hooks, the GET request builder, the operation seed, the `Signer` seam | new |
| `internal/tt/sign` | the browser signer on `browser.Session` | new |
| `internal/tt/parse`, `stage`, `config`, `app`, `tttest`, `probe` | parsers, stage bodies, config root, composition root, test env, the probe | new |
| `internal/fakett` | synthetic TikTok with ground truth and faults | new, `fakeig`'s skeleton |
| `cmd/tks-spike`, `cmd/tks-*`, `cmd/tksctl` | the probe and Phase 0 kit, the binaries, the ops CLI | new |
| `api/proto/securebrand/tt/v1`, `tks/task/v1`, `common/v1` | contracts (ADR 0007) | new; `common/v1` byte-identical to igs's |
| `migrations/tt`, `configs/tt/` | schema `tt`, operation seed, topics, defaults | new |

The boundary test grows: `internal/scrape/...` and `internal/crawl/...` import nothing under `internal/tt`, `internal/ig` or `internal/fb`, and `internal/tt/...` imports nothing under `internal/fb` or `internal/ig`.

### 3. Three seams in the scraping layer (M1c)
Each keeps fbs's and igs's behaviour as the default, so the copied tests prove nothing else moved:
1. **`graphql`: GET and a decoder.** `Built` gains `Method` and `Query`; the builder receives the whole request so it can sign the final URL; a `Decoder` turns a body into parts, errors and has-data. fbs's and igs's builders keep POST, and the default decoder is today's `SplitParts` plus GraphQL `errors`. The package keeps its name (igs's fixes cherry-pick cleanly) and its doc says it is the persisted-query and JSON web-API client.
2. **`registry`: method and variants.** A `method` field (default POST); for GET the variables template becomes the query string in template order; the `doc_id` column carries a variant label for platforms without doc_ids, so the existing fallback and per-(query, variant) breakers switch between, for example, an unsigned and a signed variant of an operation.
3. **`browser`: a persistent page.** `browser.Session` keeps one Chromium page open on an identity's exit and fingerprint for many requests (`Evaluate`, cookies, network capture of GET query strings, close on retire). The one-shot `Render` stays for bootstrap. `detect` needs no engine change: TikTok's status codes reach the existing code lists through the decoder, and an empty-200 rule and captcha markers are data.

### 4. Signing is a seam, and only TikTok's own script signs
`internal/tt/client` asks a `Signer` for the final URL and extra headers of each call. Two implementations:
* `none`: the call goes out as built. Used for every operation the probe shows works unsigned (T4).
* `browser`: the call runs as `fetch` inside a TikTok page held by `browser.Session`, so TikTok's own script adds whatever it adds (T7). One page serves many calls; the pool of pages is small and capped, like igs's browser fallback.
* If T27 holds (a URL signed in one page is accepted from another identity with the same user agent), the cheaper form of `browser` takes the signed URL the page produced and sends it by plain HTTP from the calling identity, so a handful of pages serve the whole fleet. Whether to use that form is part of G1, with the legal team's view.

tks does **not** re-implement TikTok's signature algorithms. Open-source re-implementations exist, but they break whenever TikTok changes its obfuscated script, several carry licences or provenance we cannot accept, and an algorithm written to defeat a protection is the riskiest form of access for the legal review ([methods.md §5](../methods.md#5-legal-and-terms-of-service-notes-for-the-legal-team)). If Phase 0 shows the browser signer cannot meet the cost target, a native signer is a new decision for the owner and legal, not an implementation detail.

### 5. Isolation on shared infrastructure (D6)
fbs and igs keep their values; tks changes every one of them in its copy (M1b). igs already parameterised each name, so this is a substitution guarded by igs's existing tests, retargeted:

| Resource | fbs | igs | tks |
|---|---|---|---|
| Kafka topics | `fb.*.v1`, `fbs.task.*` | `ig.*.v1`, `igs.task.*` | `tt.*.v1`, `tks.task.*` |
| Consumer groups | `fbs-*` | `igs-*` | `tks-*` |
| Record type header | `fbs-type` | `igs-type` | `tks-type` |
| Postgres | its database | database `igs`, schema `ig` | database `tks`, schema `tt`, goose table `tt.goose_db_version` |
| Advisory locks | `4_200_100`, `4_200_001`, `fbs:*` | `4_300_100`, `4_300_001`, `igs:*` | `4_400_100`, `4_400_001`, `tks:registry-sync`, `tks:lane:*`, `tks:client:*` |
| Valkey | unprefixed | `igs:` | `tks:` |
| S3 | fbs's buckets | `igs-media`, `igs-raw`, rule `igs-media-ttl` | `tks-media`, `tks-raw`, rule `tks-media-ttl` |
| Metrics | `fbs_*` | `igs_*` (`igs_ig_*`) | `tks_*` (`tks_tt_*` for the four platform series) |
| Alerts, rules, dashboard | `Fbs…`, `fbs.*`, `fbs-overview` | `Igs…`, `igs.*`, `igs-overview` | `Tks…`, `tks.*`, `tks-overview` |
| Config and env | `fb:`, `FBS_` | `fb:` (renamed `ig:` in igs's M3), `IGS_` | `tt:`, `TKS_`, `TKS_CONFIG`, `TKS_BROWSER_*` |
| API keys | `fbs_…` | `igs_…` | `tks_…` |
| Schema-check subjects | `fbs-check-…` | `igs-check-…` | `tks-check-…` |

**Until M1b is merged, no tks binary runs on shared infrastructure.** The probe (`tks-spike`) touches none of it, which is why it can come first.

### 6. Configuration
tks's root config has igs's sections with `tt:` for the platform client, embedded defaults in `configs/tt/default.yaml`, env prefix `TKS_`, `--config` or `TKS_CONFIG`. The media download switch is `limits.include_media` (`TKS_LIMITS__INCLUDE_MEDIA`), default `false` (build brief D5). The signer is `tt.signer.mode` (`none`, `browser`) with a per-operation override in the registry.

## Consequences
* fbs and igs are untouched; nothing coordinates three repositories at commit time.
* A fix in shared code lands only where it is made. `UPSTREAM.md` in each repository lists what the others may port. Three copies of the pipeline exist until someone extracts a library.
* tks gets igs's isolation guards, probe pattern and fake-site pattern on day one, and a scraping layer that already speaks to two platforms.
* tks depends on igs's pace for M2, M3 and the pipeline generalisation, or duplicates that work if it gets there first. The brief orders tks's milestones to make porting the normal case.
* The three seams change copied code that igs may later change too; cherry-picks touching `graphql`, `registry` or `browser` may conflict and are resolved by hand.
* If the owner later wants one shared library, `internal/scrape/*` in igs (or tks) is where it starts; creating it touches igs and is the owner's decision.

## Alternatives considered
* **Copy fbs, as igs did.** Loses everything igs's M1 and M4 built (seams, isolation guards, probe, fake site) and repeats about ten PRs of work.
* **Wait for igs to finish M2 and M3, then copy.** Fewer ports later, but the probe, which needs only the scraping layer, would wait for work it does not use.
* **A shared library now** (public packages in igs's repository or a fourth repository). The cleanest long-term, but it changes igs's repository and import paths, which the owner's rules forbid from tks sessions.
* **A copy without history.** Simpler, but loses `git blame` and cherry-picks from igs and fbs.
* **Python with an existing TikTok library** (for example TikTok-Api, MIT). Fast to start, but it shares nothing with the Go pipeline, Kafka contracts, privacy code and operations fbs and igs run, and drives a full browser for every call.
* **A native signer** (re-implementing `X-Bogus`/`X-Gnarly`). Rejected for now (§4).
