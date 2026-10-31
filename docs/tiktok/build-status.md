# tks build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** the plan is approved and merged (M0). **The owner records C1** (the [recording guide](recording-guide.md), about 30 minutes) and prepares the account list. **The next build session does M1a** (seed tks from igs), then M1b to M1e, and stops at **C2** (the probe test pass on the droplet). If the recordings arrive first, it analyses them before M1d.

## Decisions taken in the kickoff session (2026-10-31)

| Question | Owner's answer | Effect |
|---|---|---|
| Method for tks? | **The logged-out website through our own tool**: plain HTTP where TikTok accepts it, TikTok's own script in a browser page where it does not; the browser for everything as the fallback; a provider stays the owner's contingency at G1 | D11 approved; `methods.md` §1 and §7 |
| Code organisation? | **Copy igs now**, with its history; the probe first; igs's later work ported by cherry-pick | D7 approved; ADR 0006 accepted; M1a seeds from igs's main of that day |
| How strict is "new posts within 3 hours"? | **At least 99 % within 3 h**: the list is checked every 2 h 45 min | D2 and §8 A2; ADR 0007 §4 |

## Milestones

| # | Milestone | State | PRs | Notes |
|---|---|---|---|---|
| M0 | Plan: methods, open questions, recording guide, probe spec, reuse plan, brief, ADRs 0006 and 0007 | **done** (2026-10-31) | [#1](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/1) | Approved by the owner; no TikTok request made; sources cited in `methods.md` |
| C1 | Owner: recordings R1–R7 | waiting for owner | | Best before M1d |
| M1 | The probe: M1a seed, M1b names, M1c seams, M1d TikTok client and `fakett` slice, M1e probe and runbook | not started | | Spec: [`probe-spec.md`](probe-spec.md) |
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

All 29 hypotheses (T1–T29) are **open**: see [`open-questions.md`](open-questions.md). The first to settle, because they choose the method: **T4** (which calls need a signature), **T3** (plain HTTP bootstrap), **T7** and **T27** (browser signing, and whether one page can sign for many identities), **T9** (list items complete), **T12** (comments served logged out at all: public reports conflict), **T13** (comment order), **T14** (replies logged out), **T28** (the JavaScript challenge), **T20** (calls per ISP IP per day).

## Findings

From the kickoff research (public sources only; details and citations in [`methods.md`](methods.md)):
- **No official TikTok API fits a commercial monitor of third-party accounts.** The Research API is for non-profit academic researchers in the US, EEA, UK and Switzerland, forbids commercial use, allows 1,000 requests a day, and lags up to 48 h for new videos and 10 days for counts. The Display and Business APIs cover only accounts that authorise the app. The Commercial Content API covers ads in Europe only.
- **Third-party providers charging per call** are cheap at small scale (about $0.01 to $0.03 per account per day for this workload) but reach about $70k to $135k a month at 300,000 accounts; per-result pricing (Apify, Bright Data) costs 10 to 30 times more for this re-polling workload. Using one is the owner's decision (D4).
- **The public evidence favours the simple method for the post list:** yt-dlp and gallery-dl read whole post lists through an unsigned call (`/api/creator/item_list/`) with a random device id and no browser, as of August 2026. The app's own list call needs `X-Gnarly`, and the account call may need a signature (T4).
- **Comments are the biggest risk:** public reports conflict on whether TikTok serves them to logged-out visitors at all (T12). The recordings (R2) and the probe's test pass settle it before the crawler is built.
- **TikTok blocks more than igs's target does:** TLS and header fingerprints, a region-dependent JavaScript challenge since late 2025, IP reputation, captchas, burnt identities and device ids (T5, T6, T28, T29). Empty 200 answers are the usual failure, so tks counts every empty answer as a failure.
- **igs's scraping layer fits TikTok with three seams** (GET and a decoder, registry variants, a persistent browser page); its probe, fake site and isolation guards carry over (`reuse-plan.md`).

## What the owner needs to do

1. **Record C1** in Chrome, following [`recording-guide.md`](recording-guide.md): six recordings (a seventh is optional), about 30 minutes. Put the files in your Drive folder and paste the folder link into the next build session.
2. **Prepare the account list** (a few hundred Iraqi accounts) as a plain text file on your Mac, one account per line (`username`, `@username` or a profile link). Keep it private: it goes to the droplet by `scp` when the probe is ready, never into git. Mark 3 to 5 of them for the test pass: one big TV or news account, one brand, one public figure, one small account, and one that posts photo posts.
3. **Tell the next build session which country your ISP proxies are in.** TikTok's US service is run by a separate company since January 2026, and what a US exit sees may differ (T23).
4. **Legal:** share [`methods.md` §5](methods.md#5-legal-and-terms-of-service-notes-for-the-legal-team) with the legal team.

## Log

- **2026-10-31 (kickoff session):** plan written in `docs/tiktok/` with ADRs 0006 and 0007 ([#1](https://github.com/Abdullah95k/securebrand-microservices-tiktok/pull/1)). Research by public sources only: no request to any TikTok host, no paid service called. igs read at `9463b91`. The repository was empty, so the session created `main` with one initial commit (`.gitignore`) as the PR's base. The owner approved the method (D11), the copy of igs (D7) and the 99 %-within-3-h target; the PR was merged on green CI.
