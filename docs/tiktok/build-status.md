# tks build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** **the owner approves the plan** (M0), then **records C1** (the [recording guide](recording-guide.md), about 30 minutes) and sends the account list privately. The next build session does **M1a** (seed tks from igs), then M1b to M1e, and stops at **C2** (the probe test pass on the droplet).

## Decisions taken in the kickoff session (2026-10-31)

| Question | Owner's answer | Effect |
|---|---|---|
| Approve the plan: method (D11), copy of igs (D7), probe as the first milestone? | *pending* (asked at the end of the kickoff session) | — |

## Milestones

| # | Milestone | State | PRs | Notes |
|---|---|---|---|---|
| M0 | Plan: methods, open questions, recording guide, probe spec, reuse plan, brief, ADRs 0006 and 0007 | **in review** (2026-10-31) | (this PR) | No TikTok request made; sources cited in `methods.md` |
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

All 27 hypotheses (T1–T27) are **open**: see [`open-questions.md`](open-questions.md). The first to settle, because they choose the method: **T4** (which calls need a signature), **T3** (plain HTTP bootstrap), **T7** and **T27** (browser signing, and whether one page can sign for many identities), **T9** (list items complete), **T13** (comment order), **T14** (replies logged out), **T20** (calls per ISP IP per day).

## Findings

From the kickoff research (public sources only; details and citations in [`methods.md`](methods.md)):
- **No official TikTok API fits a commercial monitor of third-party accounts.** The Research API is for non-profit academic researchers in the US, EEA, UK and Switzerland, forbids commercial use, allows 1,000 requests a day, and lags up to 48 h for new videos and 10 days for counts. The Display and Business APIs cover only accounts that authorise the app. The Commercial Content API covers ads in Europe only.
- **Third-party providers charging per call** are cheap at small scale (about $0.01 to $0.03 per account per day for this workload) but reach about $70k to $135k a month at 300,000 accounts; per-result pricing (Apify, Bright Data) costs 10 to 30 times more for this re-polling workload. Using one is the owner's decision (D4).
- **igs's scraping layer fits TikTok with three seams** (GET and a decoder, registry variants, a persistent browser page); its probe, fake site and isolation guards carry over (`reuse-plan.md`).

## What the owner needs to do

1. **Approve the plan** (the question at the end of the kickoff session).
2. **Record C1** in Chrome, following [`recording-guide.md`](recording-guide.md): six recordings (a seventh is optional), about 30 minutes. Put the files in your Drive folder and paste the folder link into the next build session.
3. **Prepare the account list** (a few hundred Iraqi accounts) as a plain text file on your Mac, one account per line (`username`, `@username` or a profile link). Keep it private: it goes to the droplet by `scp` when the probe is ready, never into git. Mark 3 to 5 of them for the test pass: one big TV or news account, one brand, one public figure, one small account, and one that posts photo posts.
4. **Legal:** share [`methods.md` §5](methods.md#5-legal-and-terms-of-service-notes-for-the-legal-team) with the legal team.

## Log

- **2026-10-31 (kickoff session):** plan written in `docs/tiktok/` with ADRs 0006 and 0007. Research by public sources only: no request to any TikTok host, no paid service called. igs read at `9463b91`.
