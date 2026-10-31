# tks build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** M0 and C1 are done: the owner's recordings are analysed in [`har-findings-2026-10-31.md`](har-findings-2026-10-31.md), and the account list (273 accounts) has arrived privately. **The build continues with M1a** (seed tks from igs), then M1b to M1e, and stops at **C2** (the probe test pass on the droplet, through the existing ISP tier and the European ISP tier the owner is buying).

## Decisions taken in the kickoff session (2026-10-31)

| Question | Owner's answer | Effect |
|---|---|---|
| Method for tks? | **The logged-out website through our own tool**: plain HTTP where TikTok accepts it, TikTok's own script in a browser page where it does not; the browser for everything as the fallback; a provider stays the owner's contingency at G1 | D11 approved; `methods.md` §1 and §7 |
| Code organisation? | **Copy igs now**, with its history; the probe first; igs's later work ported by cherry-pick | D7 approved; ADR 0006 accepted; M1a seeds from igs's main of that day |
| How strict is "new posts within 3 hours"? | **At least 99 % within 3 h**: the list is checked every 2 h 45 min | D2 and §8 A2; ADR 0007 §4 |
| Which proxy country? (asked after the plan) | **European ISP proxies** (Germany or the Netherlands recommended), bought by the owner, compared in the probe with the existing tier | The probe runs one tier per country (probe spec); T23 and T30 compared across tiers |
| The account list | **273 public accounts** (media, brands, public figures), sent in the session on 2026-10-31 | Kept only in a git-ignored folder and on the owner's Mac; never committed. The test pass uses 3 to 5 of them |

## Milestones

| # | Milestone | State | PRs | Notes |
|---|---|---|---|---|
| M0 | Plan: methods, open questions, recording guide, probe spec, reuse plan, brief, ADRs 0006 and 0007 | **done** (2026-10-31) | [#1](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/1) | Approved by the owner; no TikTok request made; sources cited in `methods.md` |
| C1 | Owner: recordings | **done** (2026-10-31) | (this session's findings PR) | 5 HARs: 2 profiles, 3 posts with comments and replies; findings in [`har-findings-2026-10-31.md`](har-findings-2026-10-31.md) |
| M1 | The probe: M1a seed, M1b names, M1c seams, M1d TikTok client and `fakett` slice, M1e probe and runbook | **in progress** (2026-10-31) | M1a: [#3](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/3) (merged); M1b: [#4](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/4) | Spec: [`probe-spec.md`](probe-spec.md). M1a: igs's history (`9463b91`, 142 commits) merged into `main`; module renamed (protobuf code regenerated with buf 1.47.2 and protoc-gen-go 1.36.5); Instagram-only code removed; igs's README moved to `docs/fbs-readme.md`; `UPSTREAM.md` for igs and fbs. `go build`, `go vet` and `go test -race ./...` green locally on Go 1.27.2. M1b: every shared-infrastructure name renamed from igs's to tks's (ADR 0006 §5) by one scripted substitution over code, config, CI, Helm, Compose and runbooks; igs's guard tests (Valkey key prefix, metric namespace, monitoring consistency) now check tks's names. The copied pipeline's Kafka topics stay fbs's until M3. M1c: the three seams of ADR 0006 §3, each keeping fbs's behaviour as the default: `graphql` sends GET with a query string (`Built.Method`, `Built.Query`) and reads answers through a platform `Decoder`; `detect` gains `CheckpointCodes`, `UnavailableCodes` and `EmptyIsStale` (an empty 200 falls back to the next variant); `registry` gains `method` (seed, migration `00006`, admin upsert, registry-sync's candidate copy); `browser.Session` keeps one Chromium page open for many in-page `fetch` calls and reports the URL the page actually sent. Tested with Chromium (a page whose script signs `fetch` like TikTok's) and Postgres 16 locally |
| C2 | Owner: probe test pass (3–5 accounts, ISP) | blocked by M1 | | |
| C3 | Owner: probe full pass (whole list, 365 days) | blocked by C2 | | |
| G1 | Owner decision: method, from C2 and C3 | blocked by C3 | | |
| M2 | Inherited fixes (igs's M2, ported or made) | not started | | igs's M2 not merged at `9463b91` |
| M3 | Contracts | not started | | `common.v1` from igs's M3 if it has landed |
| M4 | Parsers, scrubber, golden fixtures | not started | | Needs C1 or C2 data |
| M5 | Crawl stages, full `fakett`, e2e with ground truth | not started | | After G1 |
| M6 | Persister, API, scheduler, media, GDPR | not started | | |
| M7 | Phase 0 kit | not started | | igs's M8 `Measurer` if it has landed |
| C4 | Owner: Phase 0 (ISP; residential only if agreed) | blocked by M7 | | Sets the budget (D9) |
| M8 | Canary and signer operations | not started | | |
| M9 | Deploy and operations; copied Facebook code removed | not started | | |
| C5 | Owner: legal sign-off, staging, canary, ramp | waiting for legal | | No production traffic before legal signs off |

## Open questions

30 hypotheses (T1–T30): the recordings **confirmed** T8, T9, T10, T13, T19, and T12 and T14 for a browser; **partly** answered T1, T2, T11, T15–T18, T21; the rest are **open**. See [`open-questions.md`](open-questions.md). The first to settle, because they choose the method: **T4** (which calls need a signature), **T3** (plain HTTP bootstrap), **T7** and **T27** (browser signing, and whether one page can sign for many identities), **T9** (list items complete), **T12** (comments served logged out at all: public reports conflict), **T13** (comment order), **T14** (replies logged out), **T28** (the JavaScript challenge), **T20** (calls per ISP IP per day).

## Findings

From the owner's recordings (2026-10-31; details in [`har-findings-2026-10-31.md`](har-findings-2026-10-31.md)):
- **A logged-out browser in Iraq got everything the objective asks for**: the account (with the bio link, exact counts in `statsV2`, account creation and last rename times), the post list page by page with counts, pinned and ad flags and photo posts, **top-level comments and replies**.
- **Every call was signed by TikTok's script** (`X-Gnarly`, `X-Dynosaur`; `X-Bogus` was the constant `1`), and the list call was the app's own `/api/post/item_list/`. Whether a crawler gets the same unsigned is now the main question (T4), for the probe.
- **Comments come in TikTok's ranking order** (T13): refreshes re-read to the cap. One post read to the end had 96 % of its count in top-level comments plus replies.
- **Post ids carry their time** (T8), so backfills stop at 365 days from ids.
- **One busy account posts about every 15 minutes**: backfills and comments of such accounts dominate the cost.
- **Comment authors carry `predicted_age_group`**: tks never stores it (ADR 0007 §6).
- The export stripped cookies; post pages were not recorded. Both are for the probe.

From the kickoff research (public sources only; details and citations in [`methods.md`](methods.md)):
- **No official TikTok API fits a commercial monitor of third-party accounts.** The Research API is for non-profit academic researchers in the US, EEA, UK and Switzerland, forbids commercial use, allows 1,000 requests a day, and lags up to 48 h for new videos and 10 days for counts. The Display and Business APIs cover only accounts that authorise the app. The Commercial Content API covers ads in Europe only.
- **Third-party providers charging per call** are cheap at small scale (about $0.01 to $0.03 per account per day for this workload) but reach about $70k to $135k a month at 300,000 accounts; per-result pricing (Apify, Bright Data) costs 10 to 30 times more for this re-polling workload. Using one is the owner's decision (D4).
- **The public evidence favours the simple method for the post list:** yt-dlp and gallery-dl read whole post lists through an unsigned call (`/api/creator/item_list/`) with a random device id and no browser, as of August 2026. The app's own list call needs `X-Gnarly`, and the account call may need a signature (T4).
- **Comments are the biggest risk:** public reports conflict on whether TikTok serves them to logged-out visitors at all (T12). The recordings (R2) and the probe's test pass settle it before the crawler is built.
- **TikTok blocks more than igs's target does:** TLS and header fingerprints, a region-dependent JavaScript challenge since late 2025, IP reputation, captchas, burnt identities and device ids (T5, T6, T28, T29). Empty 200 answers are the usual failure, so tks counts every empty answer as a failure.
- **igs's scraping layer fits TikTok with three seams** (GET and a decoder, registry variants, a persistent browser page); its probe, fake site and isolation guards carry over (`reuse-plan.md`).

## What the owner needs to do

1. **Buy the European ISP proxies** (Germany or the Netherlands) and keep the existing tier; the probe compares them. The proxies file is set up on the droplet when the probe is ready (the runbook gives the steps).
2. **Keep the account list on your Mac** as a plain text file, one account per line. It goes to the droplet by `scp` when the probe is ready, never into git.
3. **Legal:** share [`methods.md` §5](methods.md#5-legal-and-terms-of-service-notes-for-the-legal-team) with the legal team.

## Log

- **2026-10-31 (kickoff session, continued):** the owner sent the account list (273 accounts) and 5 HARs (C1); the session downloaded them into `.local/` (git-ignored) and wrote `har-findings-2026-10-31.md`, updating the open questions, ADR 0007, `methods.md`, the brief and the probe spec. The owner chose European ISP proxies.
- **2026-10-31 (kickoff session):** plan written in `docs/tiktok/` with ADRs 0006 and 0007 ([#1](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/1)). Research by public sources only: no request to any TikTok host, no paid service called. igs read at `9463b91`. The repository was empty, so the session created `main` with one initial commit (`.gitignore`) as the PR's base. The owner approved the method (D11), the copy of igs (D7) and the 99 %-within-3-h target; the PR was merged on green CI.
