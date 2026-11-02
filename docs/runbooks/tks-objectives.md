# tks service objectives: metrics and alerts

What tks must achieve is [build brief §8](../tiktok/build-brief.md#8-acceptance-criteria-for-the-finished-service). This runbook covers the two the M6c milestone measures end to end (A1 and A2): what each metric means, how to read it, and what to do when its alert fires. Design: [ADR 0007 §15](../tiktok/adr/0007-tks-data-model-and-contracts.md#15-settled-in-m6c-media-the-objectives-and-the-budget). The metrics are on each binary's ops port (`/metrics`); the alerts are in `deploy/helm/fbs/files/alerts.yaml` (group `tks.objectives`).

## A1: scheduled checks

**Target:** at least 99 % of the day's scheduled list checks succeed within their interval (2 h 45 min unless the account's schedule says otherwise), and no active account goes more than 6 h without a successful check.

- `tks_checks_total{kind, result, window}` counts every scheduled job that ended: `kind` is `check`, `walk` (a metrics walk standing in for a check) or `backfill`; `result` is the job's status (`completed`, `partial`, `failed`, or the reaper's verdict for a job that stopped making progress); `window` is `in` when the job ended within its account's interval of being started (a backfill: within 48 h, A3), `late` otherwise. A reaped job is always `late`.
- `tks_account_check_oldest_seconds` (the leader scheduler, every 5 minutes): the time since the active account with the oldest successful check last succeeded.

Share of the day's checks that met A1:

```
sum(increase(tks_checks_total{kind=~"check|walk",result=~"completed|partial",window="in"}[1d]))
  / sum(increase(tks_checks_total{kind=~"check|walk"}[1d]))
```

**`TksChecksMissingWindow`** (under 99 % over a day, with more than 100 checks) and **`TksAccountUncheckedLong`** (an account over 6 h):
1. Split by result: `sum by (result, window) (increase(tks_checks_total{kind=~"check|walk"}[1d]))`. Many `failed`: TikTok is refusing (look at `tks_tt_requests_total` by class and the proxy tier). Many `late` with `completed`: the crawler is behind (`tks:leader:kafka_consumer_lag` of the `tks-tt-crawler-account-p1` group): add crawler capacity.
2. Find the accounts: `SELECT account_ref, last_success_at, consecutive_failures FROM tt.account_schedule WHERE enabled ORDER BY last_success_at NULLS FIRST LIMIT 20;` and their last jobs' `error` in `tt.jobs`.
3. The budget stretches every account's interval when on-demand and scheduled traffic alone would overrun it (`tks:leader:budget_projected`); a stretched check is still `in` its (longer) interval, but new posts are then late (A2). Raise the budget or reduce the work.

## A1: daily account refresh

**Target:** at least 99.5 % of the daily account refreshes (the profile page: exact counts and the bio link) succeed within a day of being due.

- `tks_account_refresh_overdue` (the leader scheduler): active scheduled accounts whose last full read is older than `tt.freshness.account_refresh` (a day) plus a day.

**`TksAccountRefreshOverdue`** (more than 0.5 % of active accounts):
1. `SELECT a.username, a.profile_refreshed_at FROM tt.accounts a JOIN tt.account_schedule s USING (user_id) WHERE s.enabled AND a.status = 'active' ORDER BY a.profile_refreshed_at NULLS FIRST LIMIT 20;`
2. The page is read by the account's first check after a day; if the list checks succeed but the pages do not, the page loads are refused or walled: look at `tks_tt_requests_total{query="profile_page"}` by class and the page-load gate.

## A2: new posts stored within 3 h

**Target:** at least 99 % of new public posts queryable in Postgres within 3 h of their `createTime`, the median within 1.5 h, 99.9 % within 6 h.

- `tks_post_store_delay_seconds{mode}` (the persister): for each post written for the first time, its row's time less its `createTime`, when the read that found it was a scheduled check (`incremental`) or a metrics walk standing in for one (`refresh`). Posts a backfill found are not new and are not counted.
- In Postgres, `tt.posts.stored_at` and `first_mode` keep the same for every post written since M6c, so the share can be computed for any period:

```
SELECT count(*) FILTER (WHERE stored_at - created_at <= interval '3 hours')::float / count(*) AS within_3h,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM stored_at - created_at)) / 3600 AS median_h
FROM tt.posts WHERE first_mode IN ('incremental', 'refresh') AND stored_at > now() - interval '1 day';
```

**`TksNewPostsLate`** and **`TksNewPostsMedianLate`**:
1. Checks late or failing: see A1 above. A check every 2 h 45 min gives a median near 1.4 h and a worst case near 2 h 45 min plus the check's own time.
2. The persister behind: `tks:leader:kafka_consumer_lag{group="tks-tt-persister"}`.
3. Scheduled posts: on some accounts a post's id is made 1.5–2.7 h before it goes public (C2), and `createTime` is when it went public, so the delay counts from then. If late posts have a `createTime` close to their id's time (the top 32 bits, T8), TikTok is dating them by the id instead: note it in the findings, since such posts cannot be stored within 3 h of `createTime`.
