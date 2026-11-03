# tks Phase 0: what the crawler's work costs (checkpoint C4)

Phase 0 does, for a sample of your public accounts, what the crawler will do for each account in its first days, with the crawler's own client, parsers and browser signer, and at the crawler's pace. It measures every request and then says **GO** or **NO-GO**, with a **cost per account per day** at 1,000, 10,000, 100,000 and 300,000 accounts. The owner sets the budget (D9), the sizing and the alert thresholds from it (build brief §6, C4).

| What | How Phase 0 measures it |
|---|---|
| Wire bytes and time per call | Every request: the profile page, the list's check and its backfill back a year, comments and replies (each page signed by TikTok's own script in a headless Chrome page), a video file |
| Captchas and challenges (T6, T28) | Counted per 1,000 requests at the crawler's pace |
| Calls one IP takes (T20) | A **rate ladder**: list checks through one IP at 0.33, 0.66, 1, 2 and 4 requests a second, 10 minutes each, stopping as soon as TikTok pushes back (5 % refused or 3 captchas) |
| The signer's share and browser time | Each signed call's signature time, page openings, and whether TikTok accepted it; pages are shared across accounts, which answers **T32** |
| How long a signed URL stays good (T27) | The first signed comment URL of the first 20 accounts is sent again 2, 10, 30 and 60 minutes later, from another identity |
| Videos from the downloader's own IP (T33) | One video for each of the first 5 accounts, through the proxy exit and from the droplet's own IP, both with the page's cookie |
| Which call variants work today | The **variants** step tries each call unsigned and signed, and keeps the one that answers |

**The results hold no personal data.** Accounts are numbered (their position in the sample), proxy exits are labelled (`isp/3`), and no username, name, id, link or text is written. The bundle refuses to be written if it finds a username or a proxy password in a file. The account list itself stays on the droplet and your Mac.

## Steps of a run

`preflight` (proxies, a page, a browser page) → `variants` → `smoke` (3 accounts, small limits, and an estimate of the run's length) → `run` (150 accounts, 10 at a time) → `ladder` → `report` → `bundle`. A stop (Ctrl-C, a lost connection) keeps what was measured; `TKS_PHASE0_ARGS="--from run"` resumes the run where it stopped (finished accounts are skipped).

How long: the run takes about 40 minutes to 1½ hours for 150 accounts, then waits for the last signed-URL replays (up to an hour after the first ones); the ladder takes up to 50 minutes. **Plan for about 3 hours.**

## Before you start

| | |
|---|---|
| The droplet | The same machine as the probe; tks is cloned in `~/securebrand-microservices-tiktok` |
| Your proxies file | Already at `phase0/proxies.yaml` from the probe; Phase 0 uses its `isp` tier |
| The accounts file | Already at `phase0/tt-accounts-all.txt` from the full pass (C3); Phase 0 takes 150 accounts spread evenly over it |
| Chrome | Installed for the probe (`make tks-browser-check` says `Chromium found`) |

**The ladder pushes two of your ISP IPs** (the tier's first two) up to 4 requests a second for a few minutes. TikTok may refuse them for a while afterwards. Those IPs are shared with the Instagram and Facebook tests; to use one IP only, add `--ladder-ips 1` to `TKS_PHASE0_ARGS`.

## Prices

The report counts every resource (IPs, proxy GB, signer pages, browser hours, stored GB). It turns them into money only for the prices you give; a price left at 0 is reported as "not priced". Add them to `TKS_PHASE0_ARGS`, during the run or afterwards with `--from report`:

| Flag | Price |
|---|---|
| `--ip-month-usd 2.5` | one ISP IP for a month (the tier's `cost_per_gb_usd` in `proxies.yaml` is used for per-GB plans) |
| `--browser-hour-usd 0.01` | one signer page for an hour (a share of a droplet's CPU and memory) |
| `--compute-usd 0.00005` | compute per account per day, besides the browser |
| `--storage-gb-month-usd 0.1` | Postgres storage per GB-month |
| `--budget-usd 0.002` | your budget per account per day at `--target-accounts` (300,000); over it, the report says NO-GO |
| `--comment-growth 0.5` | the share of a post's re-read chances at which its comment count rises (1, the default, is the upper bound) |

A build session can also price the report again from the bundle alone, once you tell it the prices.

## What the report says

`report.txt` starts with the verdict and one line per check:

```
Phase 0: GO

== tier isp: GO ==
PASS  profile pages                        99.3% (149/150)  (want >= 95%)
PASS  list calls                           99.9% (2398/2400)  (want >= 98%)
PASS  captchas and challenges              0.00 per 1000 requests (0 captchas, 0 challenges in 7311)  (want <= 2.0)
PASS  parsers                              0 answers with missing required fields  (want 0)
PASS  signed calls                         99.2% (1210/1220), 0 signer errors  (want >= 95%)
WARN  comments collected (T15)             median 88% of a post's count over 210 whole reads  (want >= 90% (warning only; A4))
PASS  per-IP tolerance (T20)               1.00 requests/s on the weakest of 2 IPs (about 86400 a day, if it lasts)  (want >= 0.33)
INFO  signature lifetime (T27)             accepted when sent again at <=2m 20/20, <=10m 20/20, <=30m 18/20, <=1h 9/20
INFO  signing for other accounts (T32)     99.0% (940/950) of the comment pages signed on a page opened for another account answered
INFO  videos from this machine's IP (T33)  direct 100.0% (5/5); through the exit 100.0% (5/5)
INFO  budget (D9)                          $0.0012 per account per day at 300000 accounts (...)
```

(The numbers above are an illustration, not a measurement.) Then the measurements per operation, per exit and per ladder step, one account's day under tks's schedule (`tt.freshness`), and the cost table. **GO** means no check failed; **NO-GO** names the failed checks with what to do.

Exit codes: 0 GO, 4 NO-GO, 3 a step needs action (the bundle is written: send it), 1 an error.

## The memory cap and the third attempt

`make tks-phase0` runs inside a systemd scope named `tks-phase0` (3 GB of memory, no swap, 2 CPU cores) where systemd runs the machine: past the cap the kernel stops the run, not the machine, and the run resumes with `TKS_PHASE0_ARGS="--from run"`. Stop it at any time with `systemctl stop tks-phase0.scope`. The second attempt (2026-11-02) froze the pilot server: its signer's Chrome grew to 7 GB; the signer is fixed since ([`tks-signers.md`](tks-signers.md), S2 and S3).

The third attempt writes to its own folder, keeps the second's, and climbs only the ladder steps the second did not reach:

```bash
make tks-phase0 TKS_PHASE0_OUT=phase0/out-tt-phase0-3 TKS_PHASE0_ARGS="--ladder-rates 1,2,4 --replay-ages 2m,10m --replay-wait 15m"
```

## Rehearse first (no traffic, under a minute)

```bash
make tks-phase0-dryrun
```

It runs every step against a built-in fake TikTok whose list now needs a signature (so the variants step must find the signed one), with made-up prices, and must end with `phase0: GO.`

## Files

Everything is under `phase0/out-tt-phase0/` (git-ignored, files 0600): `preflight.json`, `variants.json` and `variants.jsonl`, `registry.yaml` (the working variants, for the canary to start from), `smoke-isp.jsonl`, `run-isp.jsonl`, `ladder-isp.jsonl`, `report.txt`, `report.json`, `meta.json`, and **`tks-phase0-bundle.tar.gz`**, which holds all of them and is the file to send back.

## Commands

| Command | What it does |
|---|---|
| `make tks-phase0` | The whole of Phase 0 on `phase0/tt-accounts-all.txt` through `isp` |
| `make tks-phase0 TKS_PHASE0_ARGS="--from run"` | Resume after a stop |
| `make tks-phase0 TKS_PHASE0_ARGS="--from report --ip-month-usd 2.5"` | The report again, with prices |
| `make tks-phase0 TKS_PHASE0_TIERS=isp,isp_eu` | Two tiers side by side (the probe's [second tier](tks-probe.md#adding-a-second-tier-later)) |
| `make tks-phase0-dryrun` | The rehearsal |

`go run ./cmd/tks-spike phase0 --help` lists every flag (sample size, limits, ladder, replays, prices).
