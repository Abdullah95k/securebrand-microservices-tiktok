# tks operations

How tks's crawler, scheduler and their queues behave in day-to-day running. The other `tks-*` runbooks cover each part in depth: the API ([`tks-api.md`](tks-api.md)), the canary ([`tks-canary.md`](tks-canary.md)), the objectives and their alerts ([`tks-objectives.md`](tks-objectives.md)), media ([`tks-media.md`](tks-media.md)), imports ([`tks-import.md`](tks-import.md)) and discovery ([`tks-discovery.md`](tks-discovery.md)). fbs's runbooks, for the Facebook pipeline tks was copied from, are in [`docs/fbs-reference/runbooks/`](../fbs-reference/runbooks/).

## Restarts

A crawl identity is a proxy exit, a fingerprint, the cookies and the values (`device_id`, `odinId`, `WebIdLastTime`, `region`, `language`) its first TikTok page load gave it. Without them, each identity starts with a page load, so a restart would load a page on every exit at once.

- **A running crawler checkpoints its identities** (idle or in use: their values, cookies and request counters, on their static exits) and each exit's page-load pacing to Valkey every 2 minutes, under a 5-minute lease: keys `{<kafka.topic_prefix>tks:sessions}:*`. The proxy URL and its credentials are never stored; an identity names its exit (`tier|host:port`).
- **When it stops** (SIGTERM, a rolling update), it saves them once more after its tasks finish and ends its lease.
- **The next crawler takes them**, at start and every minute after (a rolling update starts the new pod before the old one stops), once each: those identities serve TikTok's calls without a page load. A crawler that dies without saving leaves its last checkpoint, at most 2 minutes old, free once its lease ends.
- **Not resumed:** identities saved more than 12 hours ago, on a gateway tier (its sticky sessions expire), retired, on an exit no longer configured or whose breaker is open, with a fingerprint profile the crawler no longer uses (`tt.fingerprint_profiles`, or `tt.fingerprint_profile` when that list is empty), or past the tier's cap.
- **Where to look:** the crawler logs `saved sessions resumed` with the count at start and `resumed a stopped crawler's sessions` (with `saved_on_stop` and the snapshot's age) later; `tks_bootstrap_total{method="resumed",outcome="ok"}` counts the identities taken, `{method="resumed",outcome="dropped"}` those left behind.
- **To start clean** (say, after changing the fingerprint profiles or rotating the exits), stop every crawler and delete the keys: `valkey-cli --scan --pattern '{<prefix>tks:sessions}*' | xargs valkey-cli del`.

The canary keeps its own identities and saves none.

## Identities and exits

- **Waiting for an identity.** A task that needs a new identity while no exit can start one (every exit pacing or backing off its page loads) waits up to 30 s for an identity of its tier to be released, holding its crawl slot, rather than going round the retry topics. With every breaker of the tier open there is nothing to wait for (a released identity would be retired): it retries at once. The wait counts toward `crawler.yield_after`, so a task stays inside its lease.
- **Page-load pacing is off for TikTok** (`tt.proxy.page_loads`, all zeros): TikTok walled no page load in C2, C3 or C4. Turn it on if the canary or `tks_bootstrap_total{outcome!="ok"}` shows walls.
- **Identity lifetime:** `tt.identity_max_requests_min/max` (400–800 calls). fbs raised its own to 1500–3000 after Facebook walled the page loads that replace identities; TikTok has not, so tks keeps 400–800 until the measurements say otherwise.

## Closed jobs and the task queues

The reaper (in `tks-scheduler`) closes a job that stopped making progress: `partial` when its tasks look lost, `failed` when its accounting is gone, and past `scheduler.reaper_max_age` (24 h) even if tasks still wait in its lane. It then **cancels** the job, so its tasks still waiting (on the retry topics, or behind a backlog) stop when they come back instead of cycling for a job that is over; the account's next check starts its own.

The crawl task topics (`tks.task.account.*`, `tks.task.post.*`, `tks.task.thread.*`) therefore keep their tasks for a day, as long as a job can live; **raise `scheduler.reaper_max_age` and their retention together**. The retry topics keep 6 hours (the 1-hour one 12 hours): a retry relay (the crawler's `retry` stage) down for longer loses the retries it had not forwarded, and their jobs end `partial`. Apply a retention change with `tksctl kafka apply`.

## Replaying dead letters

Two topics hold what tks could not finish: **`tks.task.dlq`** (kept 14 days), the crawl tasks that used up their attempts, and **`tks.persist.dlq`** (kept 7 days), the records the persister set aside (undecodable, or refused by Postgres for what they hold; its log gives each one's SQLSTATE). The alerts `TksDLQGrowing` and `TksPersisterQuarantine` point here ([`tks-deploy.md`](tks-deploy.md#stuck-jobs)).

1. **Find the cause** from the records' `x-reason` and `x-error` headers and the logs, fix it and deploy. Replaying before the fix only fills the topic again.
2. **Send them back** with `tksctl dlq replay` (in the cluster: a pod of the default image with `tks-env`, as the chart's hooks run it):
   - `tksctl dlq replay` replays `tks.task.dlq`: each task is readied to run again (its done mark cleared, its attempts reset, its job reopened and finalised again when it ends) and sent to its original topic. A task of a cancelled job is skipped.
   - `tksctl dlq replay --topic tks.persist.dlq` sends the set-aside records back to their entity topics as they were.
   - `--reason R` replays only dead letters whose `x-reason` is `R` (tasks: `max_attempts`, `max_total_attempts`, `max_waits`, `permanent`, `undecodable`, `no_stage`; records: `undecodable`, `refused`; the others are skipped and committed), `--max N` stops after N records (1000), `--idle D` stops after D without a record (10 s).
3. **Progress is kept** in the consumer group `tks-tt-dlq-replay` (with the topic prefix): offsets are committed after each batch is sent, so a replay that stops halfway resumes where it stopped and a second one does not replay the first's records again. The command prints what it did: records replayed, jobs reopened and records skipped.

Not replaying is safe too: the account's next scheduled check reads it again, and nothing collected is lost.
