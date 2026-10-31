# tks build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** **M1 (the probe) is built** ([#5](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/5)): the owner runs the **test pass, checkpoint C2**, on the droplet through the existing ISP tier and the European one ([runbook](../runbooks/tks-probe.md); steps under "What the owner needs to do" below). The next build session records the answers (T1–T7, T9, T10, T12–T19, T22–T24, T26–T28, T30) here and in the open questions, fixes the probe where a step misread, and asks for the full pass (C3). Meanwhile it can start M2 and M3.

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
| M1 | The probe: M1a seed, M1b names, M1c seams, M1d TikTok client and `fakett` slice, M1e probe and runbook | **done** when #5 merges (2026-10-31) | M1a: [#3](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/3) (merged); M1b: [#4](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/4) (merged); M1c, M1d and M1e: [#5](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/5) | Spec: [`probe-spec.md`](probe-spec.md). M1a: igs's history (`9463b91`, 142 commits) merged into `main`; module renamed (protobuf code regenerated with buf 1.47.2 and protoc-gen-go 1.36.5); Instagram-only code removed; igs's README moved to `docs/fbs-readme.md`; `UPSTREAM.md` for igs and fbs. `go build`, `go vet` and `go test -race ./...` green locally on Go 1.27.2. M1b: every shared-infrastructure name renamed from igs's to tks's (ADR 0006 §5) by one scripted substitution over code, config, CI, Helm, Compose and runbooks; igs's guard tests (Valkey key prefix, metric namespace, monitoring consistency) now check tks's names. The copied pipeline's Kafka topics stay fbs's until M3. M1c: the three seams of ADR 0006 §3, each keeping fbs's behaviour as the default: `graphql` sends GET with a query string (`Built.Method`, `Built.Query`) and reads answers through a platform `Decoder`; `detect` gains `CheckpointCodes`, `UnavailableCodes` and `EmptyIsStale` (an empty 200 falls back to the next variant); `registry` gains `method` (seed, migration `00006`, admin upsert, registry-sync's candidate copy); `browser.Session` keeps one Chromium page open for many in-page `fetch` calls and reports the URL the page actually sent. Tested with Chromium (a page whose script signs `fetch` like TikTok's) and Postgres 16 locally. M1d: the TikTok client: `internal/tt/htmlx` (the rehydration document; an identity's `device_id`, `odinId`, `WebIdLastTime`, `region`, `language` from the page's app context, as the recordings show), `internal/tt/detect` (status codes, empty answers, challenge pages, all marked as hypotheses), `internal/tt/client` (hooks, the GET builder with the recordings' sorted parameters and Chrome's headers, the `statusCode` decoder, the operation seed `configs/tt/operations.seed.yaml`), `internal/fakett` (a seeded world with ground truth served like the recordings, and the faults of probe-spec §4), `internal/tt/tttest`; a `chrome_152` fingerprint. The client is tested against `fakett` over both transports. M1e: `internal/tt/probe` and `cmd/tks-spike probe` ([probe-spec.md](probe-spec.md)): per account the page (form, cookies, values, a second fingerprint on the first), the account (page, call, creator embed), both lists unsigned with page-size checks, the walk back in time with age and page caps, cursor portability, what a call needs (no msToken, no cookies, no device id), the post and photo pages, comments and replies with the order verdict and the completeness ratio, avatars, covers, a video with and without cookies, the browser signer with the parameters TikTok's script adds and the plain-HTTP replay of signed URLs, oEmbed and the creator embed; captchas stop the run at a cap; a Ctrl-C is a stop. Output: `report.json`, `accounts.csv`, `posts.csv`, `comments.csv`, `images/`, `videos/`, `raw/` and a `.tar.gz`, all 0600 under `phase0/`. `make tks-probe`, `tks-probe-all`, `tks-probe-dryrun`, `tks-browser-check`, `tks-spike-bin`; runbook [`tks-probe.md`](../runbooks/tks-probe.md). Tested against `fakett` (every fault of probe-spec §4, the browser signer with Chromium, the dry run as a unit test) |
| C2 | Owner: probe test pass (5 accounts, ISP and European ISP) | **waiting for owner** | | Runbook [`tks-probe.md`](../runbooks/tks-probe.md) |
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

**Checkpoint C2: the probe's test pass on the droplet.** The full steps are in [`docs/runbooks/tks-probe.md`](../runbooks/tks-probe.md); in short:

1. On your Mac, download your proxies file from the droplet: `scp root@YOUR_DROPLET_IP:securebrand-microservices/phase0/proxies.yaml ~/Downloads/proxies.yaml`.
2. On your Mac, open it in TextEdit and add the European ISP tier `isp_eu` (the runbook's step 2 has the block to paste); save.
3. Save the two accounts files the session sent (`tt-accounts.txt`, `tt-accounts-all.txt`) in `~/Downloads`.
4. On the droplet: `cd ~ && git clone https://github.com/Abdullah95k/securebrand-microservices-tiktok`, then `cd ~/securebrand-microservices-tiktok`.
5. On your Mac: `scp ~/Downloads/proxies.yaml ~/Downloads/tt-accounts.txt ~/Downloads/tt-accounts-all.txt root@YOUR_DROPLET_IP:securebrand-microservices-tiktok/phase0/`, then on the droplet `chmod 600 phase0/proxies.yaml phase0/tt-accounts.txt phase0/tt-accounts-all.txt`.
6. On the droplet, install Chrome once (the runbook's step 6: two commands), then `make tks-browser-check`.
7. `make tks-probe-dryrun` (must say `bootstrapped 3/3`), then `tmux new -s tks` and `make tks-probe TKS_PROBE_TIERS=isp,isp_eu`.
8. On your Mac: `scp "root@YOUR_DROPLET_IP:securebrand-microservices-tiktok/phase0/out-tt/*.tar.gz" ~/Downloads/`, put the bundles in your Drive folder and paste the link into the next session.

The output holds every account's details, posts, comments and a sample video: it stays off git.

**Legal:** share [`methods.md` §5](methods.md#5-legal-and-terms-of-service-notes-for-the-legal-team) with the legal team.

## Log

- **2026-10-31 (kickoff session, continued, build):** M1a ([#3](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/3)) and M1b ([#4](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/4)) merged on green CI; M1c, M1d and M1e in [#5](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/5). Each tested locally with `go test -race ./...` and `golangci-lint` on Go 1.27.2 (Postgres 16 and Chromium for the database and browser tests). The probe stops at C2.
- **2026-10-31 (kickoff session, continued):** the owner sent the account list (273 accounts) and 5 HARs (C1); the session downloaded them into `.local/` (git-ignored) and wrote `har-findings-2026-10-31.md`, updating the open questions, ADR 0007, `methods.md`, the brief and the probe spec. The owner chose European ISP proxies.
- **2026-10-31 (kickoff session):** plan written in `docs/tiktok/` with ADRs 0006 and 0007 ([#1](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/1)). Research by public sources only: no request to any TikTok host, no paid service called. igs read at `9463b91`. The repository was empty, so the session created `main` with one initial commit (`.gitignore`) as the PR's base. The owner approved the method (D11), the copy of igs (D7) and the 99 %-within-3-h target; the PR was merged on green CI.
