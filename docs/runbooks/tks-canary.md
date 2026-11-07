# tks canary and signer operations

`tks-canary` follows TikTok's changes to the web calls tks makes (T25: TikTok changes paths, parameters or signing every few weeks, often silently, with an empty HTTP 200). TikTok has no doc_ids to discover, so where fbs's registry sync scans bundles, the canary **checks**: every `tt.canary.interval` (1 h) it reads, on the canary accounts, a profile page, then a list page with each variant of `creator_list`, a comment page with each variant of `comments` on a post the list returned, and a reply page with each variant of `replies` on a comment that has replies. Design: build brief M8, `internal/tt/canary`.

## What it does in a run

1. **Twins.** For each variant in use it adds its signed or unsigned twin (`creator` ⇄ `creator-signed`, `web-signed` ⇄ `web`) as a `candidate` with `source = canary`, if the operation lacks it (`tt.canary.twins`). TikTok changes which calls it accepts unsigned (T4), so both ways are always tried.
2. **Checks.** Per operation it calls the `active` variant and the `candidate`s; the `fallback`s when the active one failed, and the unsigned ones also while the active one needs a signature. Each call goes through the crawler's own client (fingerprints, detection rules, builder, browser signer), so a variant that passes here answers the crawler the same way. Verdicts:
   - **ok**: the answer parsed with every required field and had data;
   - **stale**: refused (an empty answer) on `tt.canary.confirm` (2) accounts in the run, since one refusal may be the identity's (a burnt device, a bad signer page);
   - **drift**: answered, but the parser misses required fields;
   - **inconclusive**: blocked, rate limited, not signed, nothing to read, or refused on fewer accounts than `confirm`. Says nothing about the variant.
3. **Promotion** (`tt.canary.promote`, on): when the active variant is stale or drifted, a passing candidate or fallback becomes active, unsigned ones first (`why=failed`); when the active variant needs a signature and an unsigned one passes, the unsigned one becomes active (`why=unsigned`: a signature costs browser time, D11). The former active variant stays a `fallback`, which the crawler's client switches to on an empty answer at once, before the next canary run. Other passing candidates become fallbacks too; at most `keep_fallbacks` (2) are kept. A candidate or fallback that failed `retire_after` (3) runs in a row is `retired`; a retired twin is tried again after `recheck_retired` (24 h).
4. Crawlers pick up promotions on their next registry reload (`tt.registry_refresh`, 30 s).

Each run is stored in `canary_runs` (`GET /v1/admin/canary/runs`, scope `admin`), with each check's verdict and reason; accounts are named by their position in `tt.canary.accounts`, never by username. Every variant's last check is on its row in `operations` (`last_check`, `last_checked_at`, `last_ok_at`, `check_failures`; `GET /v1/admin/operations`).

## Configuration

| Key | Default | What it is |
|---|---|---|
| `tt.canary.accounts` | none | Public usernames that post, get comments and comments with replies, without `@`. Set by the deployment: production's are the owner's, set by the platform as parameters of tks's Argo CD application (`config.tt.canary.accounts[0..2]`; real usernames stay out of this public repository, 2026-11-07); elsewhere `TKS_TT__CANARY__ACCOUNTS=a,b,c`, which wins over the values. Three or more, so `confirm` can be met when one fails to load |
| `tt.canary.operations` | `creator_list, comments, replies` | The crawler's operations |
| `tt.canary.interval`, `timeout` | 1 h, 10 min | How often, and how long a run may take |
| `tt.canary.tier` | the crawler's first tier | The proxy tier |
| `tt.canary.confirm` | 2 | Accounts that must refuse a variant in a run for it to be stale |
| `tt.canary.promote` | true | false: a dry run that records `proposed` promotions and changes nothing |
| `tt.canary.twins`, `keep_fallbacks`, `retire_after`, `recheck_retired` | true, 2, 3, 24 h | Above |

The canary signs with its own signer pages (`tt.signer`, as a crawler's), so it needs Chromium in its image when `tt.signer.mode` is `browser`. Run one replica; runs are serialised by a Postgres advisory lock (`tks:canary`).

One run by hand, printing the report (exit 1 on error, 3 when degraded):

```sh
tks-canary --config /etc/tks/config.yaml --once
```

A run costs, per canary account it needs: one profile page, one list call per list variant, one comment call per comment variant and one reply call per reply variant; usually two or three accounts, a dozen calls and a few signatures an hour.

## An operation is failing

`TksCanaryOperationFailing` (critical): `tks_canary_operation_healthy{operation}` is 0, so the active variant was stale or drifted on the canary accounts and no candidate or fallback passed. The crawler's calls of that operation are refused or unreadable too.

1. Read the latest run: `GET /v1/admin/canary/runs?limit=1`. Under `checks`, the operation's variants with their `verdict` and `reason`.
2. **stale** on every variant: TikTok moved or changed the call. Find the call the web app makes now (the probe's in-browser capture, `tks-spike probe`, records the page's own API calls; or a recording, [recording guide](../tiktok/recording-guide.md)), then add it as a candidate:
   ```sh
   curl -X PUT -H "Authorization: Bearer $ADMIN_KEY" -H 'content-type: application/json' \
     https://tks.example.com/v1/admin/operations/comments \
     -d '{"doc_id":"v2-signed","friendly_name":"comments","parser":"comments","status":"candidate","method":"GET","endpoint":"/api/comment/list/v2/","variables":{"aweme_id":"{{item_id}}","count":"{{count}}","cursor":"{{cursor}}"}}'
   ```
   A label ending in `-signed` is signed by the browser signer. Then run `tks-canary --once`: it checks the candidate and promotes it if it passes. Add it to `configs/tt/operations.seed.yaml` in the next release.
3. **drift**: see [Drift](#drift).
4. **inconclusive** only: the run could not judge; see [The canary is not running](#the-canary-is-not-running).

## The canary is not running

`TksCanaryNotRunning`: no run in 3 hours gave every operation a verdict (`unchecked` in the run report). Usually:
- **No canary account loads** (`accounts[].reason` in the report): an account was renamed, went private or was deleted, or the tier is blocked. Replace the account (in production: the platform's parameter `config.tt.canary.accounts[N]`, through the master).
- **The accounts lack data**: no recent post with comments, or no comment with replies (`no post with comments to read`, `no comment with replies to read`). Use busier accounts.
- **The signer cannot sign** (`sign_error` on the signed checks, `tks_canary_signer_healthy` 0): see [The signer fails](#the-signer-fails).
- `tt.canary.accounts` is empty (the run is an `error`).

## A variant was promoted

`TksCanaryPromoted` (info) says which operation changed and why. Nothing is broken, but:
- `why=failed` to a **signed** variant (for example `creator_list` → `creator-signed`): every such call now costs a signature. For the list that is every check of every account: watch `tks_signer_signs_total`, `TksSignerSaturated` and the browser time; the Phase 0 cost model (`tks-phase0.md`) shows the pages and hours a fully signed list needs. Raise `tt.signer.pages` or add crawler pods if the signer saturates.
- `why=unsigned`: a call no longer needs the signer; its share falls.
- Record the change in `docs/tiktok/open-questions.md` (T25: how often TikTok changes things).

## Drift

`TksCanaryDrift`: a variant answered with a shape the parser cannot read (required fields missing; the check's `reason` names them). The crawler sees the same (`TksSchemaDrift`, `tks_parse_missing_field_total`).
1. Capture an answer: run the crawler with `tt.capture_raw: failures` on one pod, or the probe.
2. Add it as a fixture (`tksctl fixtures add`, [tks-fixtures.md](tks-fixtures.md)), fix the parser's candidate paths in `internal/tt/parse`, and release. The canary passes the variant again at its next run.

## Empty answers

`TksEmptyAnswers`: more than 20 % of an operation's calls by the crawler came back empty for 30 minutes. In order:
1. Is the canary failing or promoting for the same operation? Then TikTok changed the call: see above.
2. Signed operations (`comments`, `replies`): see [Signed calls refused](#signed-calls-refused).
3. Only some exits: burnt identities or IPs; `tks_identity_retired_total` and the proxy breakers (`TksProxyBucketOpen`).

## The signer fails

`TksSignerFailing`: more than 10 % of signatures fail (`tks_signer_signs_total{result="error"}`), or `tks_canary_signer_healthy` is 0.
1. `tks_signer_page_opens_total{result="error"}` rising: pages do not open. Chromium missing or crashing in tks-signer's image (`tt.browser.binary`), or TikTok's page not loading through the tier (page timeouts in its logs). No answer from tks-signer at all is [`TksSignerUnreachable`](tks-deploy.md#tks-signer-unreachable).
2. Pages open but signing fails: TikTok's page script changed how it signs `fetch` (the page no longer adds its parameters, or holds no call). The probe's browser step (`tks-spike probe`) shows what the page adds now. tks never computes a signature itself (ADR 0006 §4).
3. Comments and replies wait in the retry topics meanwhile; nothing is lost.

## Signed calls refused

`TksSignedCallsRefused`: TikTok refuses more than 10 % of the calls a signer page signed (`tks_signed_calls_total{result="refused"}`). The client already signs a refused call once more on another page (`tks_sign_retries_total`), and a page refused `tt.signer.max_refusals` times in a row is replaced (`tks_signer_page_closes_total{reason="refused"}`).
1. Refusals in waves that a fresh page cures: pages go bad within minutes (C4, 2026-11-03). Lower `tt.signer.page_max_signs` or `page_max_age`.
2. Refusals that no fresh page cures, and the canary's signed checks refused too: TikTok checks something new. Compare with the signer comparison (`tks-signers.md`) and the probe.

## The signer is saturated

`TksSignerSaturated`: more than 5 % of signatures waited past `tt.signer.sign_timeout` because every page was busy. The pool is capped per tks-signer replica (`tt.signer.pages`, shown as `tks_signer_pages_max`) to bound the browsers' memory (the pilot server froze at 7 GB, 2026-11-03). Add tks-signer replicas (`components.signer.replicas`; [the signer service](tks-deploy.md#the-signer-service)); raise `tt.signer.pages` only with the pod's memory (each page is a Chrome renderer of a few hundred MB, killed past `tt.signer.kill_page_mb`).

## Signer pages churn

`TksSignerPagesChurn`: more than 60 pages an hour closed for `memory`, `refused` or `error` (`tks_signer_page_closes_total` by `reason`). Each new page is a TikTok page load through the tier and browser time.
- `memory`: pages outgrow `tt.signer.max_page_mb`; check that `tt.signer.freeze` and `open_on: profile` are on (S2).
- `killed`: pages passed `tt.signer.kill_page_mb` while signing and were closed mid-call (the guard); as for `memory`, and see [tks-signer memory](tks-deploy.md#tks-signer-memory).
- `refused`: see [Signed calls refused](#signed-calls-refused).
- `error`: see [The signer fails](#the-signer-fails).

Planned closes are `spent` (`page_max_signs`, `page_max_age`), `idle` (`idle_close`), `room` (a page for another user agent, or a retry's) and `shutdown`.
