# tks: importing accounts, labels, admission and keywords

Bulk import registers a list of TikTok accounts for scheduled checks in one go ([`discovery-plan.md` §6](../tiktok/discovery-plan.md#6-part-a-bulk-import-m10), [ADR 0007 §18](../tiktok/adr/0007-tks-data-model-and-contracts.md#18-settled-in-m10a-sources-labels-admission-and-bulk-import)). It reads nothing from TikTok by itself: each account is read by its normal checks once paced admission lets it start. Imports, sources, labels and blocks are recorded for good (D8).

## Importing a list

The accounts belong to an API client, as with `POST /v1/accounts`. Make one if needed (it prints its key once):

```bash
tksctl apikey create --name owner --scopes jobs:write,accounts:write,data:read,pii:read
```

The file holds one account a line: `username`, `@username`, a profile link, a video link (the account is taken from it), a secUid, `sec:<secUid>` or `id:<user id>` (for accounts tks has read). Blank lines and lines starting with `#` are skipped. Or a CSV whose header names an `account` column and, optionally, `labels` (several separated by `|` or `;`), `interval` (minutes, 30 to 43200), `max_age_days` and `include_media`.

First the dry run, which changes nothing:

```bash
tksctl accounts import accounts.txt --client owner --label list:banks --dry-run
```

Then the same without `--dry-run`. The report (JSON) says:
- `added`: new schedules;
- `joined`: accounts another client monitors already; your registration was added, with no second schedule;
- `already_monitored`: yours already; new labels were added;
- `duplicates`: lines naming an account an earlier line named (case, `@`, links);
- `not_accepted`: line numbers, the text, and why (not a username, or a short link: `vm.tiktok.com` and `tiktok.com/t/` links are not supported until M12 resolves them, since resolving one is a request to TikTok);
- `queued` and `queue`: how many of these wait for admission, the whole queue, its rate and estimated finish.

What tks cleans before checking a line: byte-order marks, zero-width spaces and joiners, direction marks, Windows line ends, full-width characters (`＠`). Usernames are lowercased.

A missing account or a typo is registered: its first read finds TikTok's code, and it gets the lighter checks below. A private account likewise.

It runs about 3 ms a line (10,000 lines in under 30 s in the tests), 500 lines a transaction. If it stops halfway, run it again: finished lines come back `already_monitored`. Files above 500,000 lines must be split. Two imports at once are two batches.

Through the API: `POST /v1/accounts/import` with `{"lines": [...]}` or `{"text": "<the file>"}`, at most 1000 accounts a call, plus `labels` and `dry_run`.

## What became of an import

```bash
tksctl accounts import --report <batch> --client owner --problems
```

`<batch>` is the `batch` of the import's report. Each account's status: `queued` (waiting for admission), `unread`, `active`, `inactive` (read, no post for 90 days), `private`, `unavailable` or `banned`. `--problems` keeps the private, inactive and missing ones. API: `GET /v1/accounts/imports/{batch}?problems=true`.

## Making the file the whole list

`--disable-missing` removes your registrations of accounts that are not in the file. Their data stays, and a schedule stays on while another client is registered. Without `--confirm` it only shows what it would remove:

```bash
tksctl accounts import accounts.txt --client owner --disable-missing
```

```bash
tksctl accounts import accounts.txt --client owner --disable-missing --confirm
```

It refuses to remove more than 10 % of your list unless you add `--force`. Through the API it only runs dry (`disable_missing` with `dry_run`), since a call of 1000 lines may not hold the whole list.

## Labels and sources

Labels are yours (`list:banks`, `import:2026-11-05`): 1 to 64 characters, no spaces or commas. Set them on import or on `POST /v1/accounts` (`labels`). `GET /v1/accounts?label=list:banks` lists your accounts with a label; `?source=import` (or `api`, or a discovery source) by the way they came; `?queued=true` those waiting for admission. `GET /v1/accounts/{account}` shows the account's `sources` (how, via: the import batch, first and last time) and your `labels`.

Removing an account (`DELETE /v1/accounts/{account}`) removes your registration and labels and records a block: discovery never adds the account back. Registering it again yourself lifts your block.

## The admission queue

A new account's first read is its backfill: a year of posts and their comments, about `tt.admission.requests_per_account` requests (1,500). Thousands at once would use the day's proxy data in hours, so new accounts wait in a queue and the scheduler lets them start at a pace; checks of accounts already monitored never wait. Order: your own (API, import) first, then discovered accounts by their Iraq score (M10c), oldest first. An account whose posts tks has read already needs no backfill and starts at once.

The pace:
- `tt.admission.per_hour` when set;
- else, with a request budget (`scheduler.budget.requests_per_day`), what the day's budget leaves after the projected traffic, over the hours left in the day (UTC), at `requests_per_account` each; nothing while the budget pauses backfills;
- else, no budget: every new account starts at once (as before M10a).

`GET /v1/admission` shows the queue by origin, the rate, the estimated finish, and what draining it within a day would take (`to_drain_in_a_day`: requests, IPs and signer pages from Phase 0's measurements in `tt.admission`). Metrics: `tks_admission_queue`, `tks_admitted_accounts_total`.

`TksAdmissionStalled`: accounts wait and none started for 6 hours.
1. `GET /v1/admission`: `per_hour` 0 means the budget leaves nothing after monitoring, or pauses backfills (`tks_budget_backfill_paused`).
2. Raise `scheduler.budget.requests_per_day` as capacity grows, or set `TKS_TT__ADMISSION__PER_HOUR` for a fixed pace.
3. To let one account start now, read it on demand (`POST /v1/jobs`); its schedule still waits its turn.

## Accounts that went quiet

An account you registered that turns out private, banned or missing stays registered and gets lighter checks (`tt.freshness.inactive`): a private one daily for 7 days, then weekly; a banned or missing one weekly for 30 days, then monthly. The schedule's `inactive_since` says since when. A check that finds it active puts it back on its interval. Discovered accounts move to the dormant list instead (M10c).

## Discovery keywords

The Iraq keyword list (fbs's, `configs/tt/discovery/iraq.yaml`: the country, the Kurdistan Region, the 19 governorates and their cities, in English, Arabic, Sorani and Kurmanji) is loaded into the database once:

```bash
tksctl discovery keywords seed
```

Loading again adds only keywords not listed yet and never turns on one someone turned off; spellings typed on different keyboards fold together (`discovery.Normalize`). `tksctl discovery keywords import --file F` loads a YAML in the same shape or a CSV of `term,language[,group[,enabled]]`; `tksctl discovery keywords list --language ar` lists them. API: `GET /v1/discovery/keywords` (`discovery:read`), `POST` and `PATCH /v1/discovery/keywords/{id}` (`discovery:write`) to add one or turn it off. Nothing searches them until part C (M12), after the owner's discovery probe (C6) and counsel (G2).
