# Cost tuning

Almost all of fbs's running cost is proxy bandwidth: every Facebook request and page load goes through a paid exit, billed per GB on the wire. Media downloads use direct egress (`media.NewHTTPClient` never uses a proxy), and Postgres, Kafka and compute are comparatively flat. This page shows how to measure what you spend, and which settings move it.

## Measure first

Each tier's proxy price is `cost_per_gb_usd` on its `fb.proxy.tiers` entry. The Phase 0 report (`fbs-spike report`) already projects a monthly cost per tier from measured bytes per request. In production:

| What | PromQL |
|---|---|
| GB per day, per tier | `sum by (tier) (increase(igs_proxy_bytes_total[1d])) / 1e9` |
| Bytes per GraphQL request, per query | `sum by (query) (rate(igs_ig_response_bytes_sum[1h])) / sum by (query) (rate(igs_ig_response_bytes_count[1h]))` (decoded size; the wire is smaller) |
| Requests per day, per query | `sum by (query) (increase(igs_ig_requests_total[1d]))` |
| Share wasted on blocks | `igs:block_ratio:rate5m` (blocked requests are paid for too) |
| Requests per lane today | `igs_budget_used{kind="requests"}` |

Multiply the GB per tier by its price for the daily bill. The overview dashboard's "Proxy bandwidth" and "Requests by query" panels show the same.

## Levers, roughly in order of effect

| Setting | Default | Effect on cost | Trade-off |
|---|---|---|---|
| `fb.proxy.routing` | `default: [isp, residential]` | Sends traffic to the cheapest tier that works. Residential is typically several times the price per GB of ISP. Keep `bootstrap_html` (page loads) on the tier that bootstraps reliably, and everything else on the cheapest one with an acceptable block rate (Phase 0 per-tier report). | A cheaper tier with a higher block rate costs retries and AIMD backoff. |
| `scheduler.tiers` | every tier 3h | Re-scrape frequency per page. A run that finds no new post costs a request or two, so quiet pages are cheap at 3h; most of the traffic is new posts' comments and refreshes. Longer T2/T3/T4 intervals save those few requests. | Quiet pages' new posts arrive later. |
| `scheduler.post_refresh` | 24h / 72h / 7d by age, `max_per_job: 50`, `max_backoff: 3` | Comment refreshes of older posts. An unchanged post costs one request, and the interval doubles while it stays unchanged. Tighten the windows or `max_per_job` if refreshes dominate `igs_post_refreshes_total`. | New comments on older posts arrive later. |
| `scheduler.post_refresh.recrawl_at`, `recrawl_min_comments` | 7d, 30d; 20 comments | Full re-reads of busy posts (reply threads, deletions). Each is a full comment pagination. | Fewer late replies and deletions seen. |
| `limits.max_age_days` | 365 | Backfill depth for new pages: the biggest one-off cost per page. | Less history. |
| `limits.include_replies`, `limits.max_depth` | on, 3 | Reply threads are a request each (plus pagination). | Missing replies. |
| `crawler.timeline_page_size`, `crawler.comments_page_size` | 10, 50 | Bigger pages mean fewer requests for the same data, and less per-request overhead. Raise them only as far as Facebook serves without blocking (Phase 0). | Larger responses; possibly more blocks. |
| `fb.identity_max_requests_min/max` | 150 / 400 | Each identity's bootstrap is a full page load (hundreds of KB). Longer-lived identities amortise it. | Longer identities are easier to rate-limit. |
| `scheduler.budget` | unlimited | A hard daily cap in requests and/or bytes: pauses backfill, then stretches T4/T3/T2 past 3h. On-demand work is never held. With `stretch_tiers: []` every page stays at 3h and the budget pauses backfill only. | Backfills wait; slow tiers go stale. |
| `fb.browser.max_share` | 2% | Browser bootstraps load a whole page, scripts included (media is refused). Keep the share low. | Pages that need JavaScript fail more often. |
| `fb.capture_raw` | `failures` | `all` writes every response to S3 (storage, not proxy). Use it briefly. | — |
| `limits.video_policy` | `sd` (`thumbnail` for backfills) | Media egress and storage (direct, not proxy). | Lower-quality media for the analysis service. |

## A tuning loop

1. Find the biggest consumer: GB per day by tier, then requests per day by query, then by lane (`igs_budget_used`).
2. Pick the lever for it from the table above. For example:
   - **post refreshes** dominate: tighten `post_refresh`;
   - **backfill** dominates: lower `max_age_days` for new pages, or set a byte budget so backfill only uses what is left each day;
   - **blocks** waste more than 5%: fix routing before anything else (block storm runbook).
3. Change one setting at a time and compare a day against the day before. Keep the comment completeness (`igs:comment_completeness:p50_1h`) and the schedule lag (`igs:leader:schedule_lag_seconds`) in view: a cut that loses coverage is not a saving.
4. Once the steady state is known, set `scheduler.budget.bytes_per_day` a little above it. The budget then absorbs spikes (a burst of new pages) by delaying backfill, instead of by overspending.
