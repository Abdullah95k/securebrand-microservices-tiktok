# Load testing

`make loadtest` runs two opt-in tests (`internal/e2e/load_test.go`) against the local infrastructure (`make infra-up`). Each prints a report; set `IGS_LOADTEST_OUT=file.md` to append it to a file. Everything runs in one process against fakefb. The tests therefore find the pipeline's own ceilings (stages, Kafka, Postgres, persister), not Facebook's: in production the fleet is paced far below them by the per-identity and per-page limits and AIMD.

| Test | What it does | Knobs |
|---|---|---|
| `TestLoadPipeline` | Registers nothing. It submits a full backfill job for every fakefb page at once through the API, then waits for every job. Each job passes the page, post and thread stages, the persister and the finalizer. The test fails unless every job completes and every post and comment of fakefb's ground truth is stored. Every 2 seconds it samples each stage's consumer-group backlog (records not yet committed); the report gives the peak and mean. | `IGS_LOADTEST_PAGES` (30), `IGS_LOADTEST_POSTS` (25 per page), `IGS_LOADTEST_COMMENTS` (150, the heavy-tailed maximum per post), `IGS_LOADTEST_LATENCY` (fakefb response latency, 0), `IGS_LOADTEST_TIMEOUT` (20m), `IGS_LOADTEST_CPUPROFILE=file` (a CPU profile of the run, for `go tool pprof`) |
| `TestLoadPersister` | Produces synthetic comments to `fb.comment.v1` as fast as Kafka takes them, and measures until the persister has stored them all. It then produces the same records again: the no-op upsert path every re-scrape takes. | `IGS_LOADTEST_ROWS` (200000) |

## Results (development container, 4 CPUs)

These are measured with every component in one process on a 4-CPU container. They are a floor, not a target.

| Run | Wall time | Facebook requests | Comments and replies | Job duration p50 / max |
|---|---|---|---|---|
| 30 pages × 25 posts, no latency | 35s | 15,791 (456/s) | 109,205 (3,150/s) | 27s / 32s |
| 60 pages × 20 posts, 40 ms latency | 51s | 25,475 (499/s) | 176,870 (3,460/s) | 40s / 45s |
| 100 pages × 30 posts, 100 ms latency | 2m30s | 64,600 (431/s) | 449,888 (3,000/s) | 145s / 149s |
| 200 pages × 30 posts, no latency | 5m05s | 128,550 (422/s) | 894,080 (2,935/s) | 265s / 280s |

Every run stored every post and comment of the ground truth, and every job completed.

| Persister | Rows/s | Batches |
|---|---|---|
| 200k new comments (COPY into a temp table, then insert) | 15,400 | about 1,980 rows each, 67 ms mean (both passes) |
| The same 200k again (no-op upsert) | 28,700 | |
| 1M new comments | 16,300 | 1,988 rows each, 87 ms mean (both passes) |
| The same 1M again | 25,700 | |

Consumer-group backlog in the 200-page run, in records:

| Stage | Peak | Mean |
|---|---|---|
| page | 46 | 2 |
| post | 2,313 | 302 |
| thread | 67,709 | 43,704 |
| persister | 161,514 | 37,988 |

Where the CPU went in the 200-page run, from its profile (573 CPU-seconds over 305 seconds, about 1.9 of the 4 cores; Postgres and Redpanda used most of the rest):

| Part | Share | Notes |
|---|---|---|
| Crawler stages | 43% | 245 s for 128,550 requests: **about 1.9 ms of CPU per Facebook request**, all included (HTTP, parsing, protobuf, Kafka, Redis). The thread stage alone is 28%; comment parsing 12.5%. |
| fakefb | 16% | Rendering the synthetic responses; not part of fbs |
| Garbage collection | 7% (background marking) | Allocation-heavy JSON parsing |
| Persister | 2.4% | About 15 µs per comment |
| Everything else | the rest | HTTP servers, Kafka clients, the scheduler and finalizer |

## Reading them

- **The stages are CPU-bound here.** Adding 40 ms, then 100 ms of latency did not lower the request rate (431/s with 100 ms against 422/s without), so the limit was CPU (fakefb rendering, parsing, protobuf, every component sharing 4 cores), not the 96 shared slots. In production each crawler pod has its own cores, and the requests are paced by Facebook's limits.
- **What saturates first is the thread stage.** It holds almost all of the crawl backlog: every post fans out into reply threads, and each thread is one or more requests. The page and post stages keep up. In production this means the thread consumers' lag is the first to grow, and KEDA scales `crawler-thread` on it. Give it the most replicas (the chart's defaults do).
- **The crawler's CPU does not limit production.** At about 1.9 ms per request, 16M requests a day (the README's sizing, about 185 a second) need about 0.4 of a core across the fleet. Requests are limited by pacing and proxies, never by crawler CPU. The crawler pods' memory and slots are what to size for, not CPU.
- **The persister's backlog comes in bursts.** It peaked at 160k records when many threads finished together, then drained within seconds; it never limited a job by more than a few seconds.
- **Jobs progress together.** Job p50 is close to max: the shared slots interleave all jobs instead of finishing them one by one. The jobs are submitted one after the other, a few milliseconds apart. On-demand (p0) work is still served first when lanes compete.
- **Completion waits for the persister.** A job completes only after the persister's committed offsets pass the end offsets taken when its last task finished, so "completed" means "queryable".
- **Persister headroom.** The sizing target in the README is about 100M row writes a day, about 1,200 rows/s on average. One persister pod on this container did 16k new rows/s over a million rows, and 26k for unchanged rows: about 13 times the average. The rate held from 200k to 1M rows, as the monthly comment partition grew. The chart's 2–6 persister pods (KEDA on `fb.comment.v1` lag) leave a wide margin; Postgres IOPS will be the limit first.

## Running larger tests

- Use a bigger world: for example `IGS_LOADTEST_PAGES=300 IGS_LOADTEST_POSTS=40 make loadtest`. The 200-page run above took 5 minutes.
- For production-like numbers, run the services as separate processes against compose or a staging cluster, pointed at fakefb (`deploy/compose`). Watch the "fbs / Overview" dashboard while it runs: lane slots, consumer lag and persister batch time show which stage saturates first.

## Soak and chaos tests

Both check that restarts lose nothing and that the store ends up exactly like the fakefb world.

**`TestChaosRestartsAndFaults`** runs in `make test-integration`.
- **Setup:** three pages are crawled with rate limits and latency. Each page is paced at 5 requests a second, so the jobs run through the restarts.
- **Chaos:** while the jobs run, the test makes 12 restarts at random:
  - the crawler, which abandons its in-flight tasks after 20 ms, as a killed pod would;
  - the persister;
  - the finalizer, as a scheduler leader change would.
- **Checks:**
  - every job completes;
  - every post and comment is stored exactly once, nothing extra, at full completeness;
  - the job counters undercount nothing.
- **Second round:** full jobs over the same pages, also under restarts, find nothing deleted or edited.

**`make soak`** (`TestSoak`) is opt-in and runs for `IGS_SOAK_DURATION` (20m).
- **Setup:** `IGS_SOAK_PAGES` (6) pages are scheduled every 30 seconds. Short refresh windows and recrawl ages make post refreshes and recrawls run along the way.
- **Changes and restarts:**
  - every few seconds the world changes: new posts and comments, comments on older posts, edits and deletions;
  - every `IGS_SOAK_RESTART_EVERY` (90s; 0 = never), a component restarts.
- **Every 30 seconds it records** goroutines, heap, Redis keys and jobs. It fails on a failed, partial or stuck job.
- **At the end:**
  1. A full job per page.
  2. Every comment on Facebook must be stored, visible and with its current text. Every comment the test deleted must be marked deleted.
  3. No job may be left in the finalize set, no seen list in Redis, and goroutines must not have grown.
- **Report:** `IGS_SOAK_OUT=file.md` writes the report.

A 6-minute run on this container (4 pages, a restart every 40s), from `IGS_SOAK_DURATION=6m IGS_SOAK_PAGES=4 IGS_SOAK_RESTART_EVERY=40s make soak`:
- 48 jobs; 133 changes, including 27 deletions and 25 edits; 8 restarts;
- 59 comments marked deleted: the 27 deletions and their replies;
- edits counted on 19 of the 22 edited comments that are still up. The other edits came before the comment was first stored;
- goroutines between 190 and 324, heap at most 66 MB.

**Longer runs on a GitHub runner:** Actions → **soak** → Run workflow, with the duration, pages and restart interval. The report is in the run's summary and in its `soak` artifact, with the log.

A 90-minute run there ([run 40882655887](https://github.com/Abdullah95k/securebrand-microservices/actions/runs/40882655887), commit `4c26a05`; 20 pages, a restart every 2 minutes; 1h56m with the final full jobs and checks) passed:
- 2,870 jobs, all completed;
- 2,024 changes to the world: 681 rounds of new posts and comments (0–1 posts and 1–4 comments each), 616 rounds of comments on older posts (1–4 each), 375 edits and 352 deletions;
- 44 restarts: the crawler 21 times, the finalizer 17, the persister 6;
- 19,683 comments stored. All 18,910 comments left in fakefb's world are stored, visible and with their current text. The other 773 are marked deleted, and each is one the test deleted or a reply under one;
- 328 of the 336 edited comments still up have an edit counted. An edit made before a comment is first stored is not an edit for us, so some are expected to go uncounted; the test does not check the 8 one by one;
- goroutines 300 after 2 minutes, at most 495, and 279 at the end: they did not grow with the restarts. Heap in use rose steadily, to a peak of 359 MB; see **Memory** below.

An earlier run with the same settings on the development container got through its 90 minutes (2,844 jobs, all completed, none partial or failed), but the container's Docker daemon stopped during the final checks.

**Memory.** Every component runs in the test's one process, together with fakefb and an in-memory S3, so the heap in use covers all of them. The report gives the heap left after a full collection after 2 minutes and at the end, and `IGS_SOAK_HEAPPROFILE` writes a heap profile at both points (the workflow prints the difference). In a 30-minute run with the same settings ([run 40905417646](https://github.com/Abdullah95k/securebrand-microservices/actions/runs/40905417646)):
- the live heap went from 64 MB after 2 minutes to 114 MB at the end;
- 47.6 MB of the 50 MB growth is the in-memory S3 holding the downloaded media. Nothing in the test acknowledges media, so it stays until `media.ttl` (72h). In production, media goes to the S3 bucket;
- the other 2–3 MB is spread over entries of half a megabyte or less, up and down: connection buffers, fakefb's growing world. Nothing in fbs's own code grew with the run.

**What these tests found, all fixed:**
- **Lost posts (load test):** when a page's timeline held both a post and a share of it, the parser returned the post only once, quoted inside the share. The post itself and its 654 comments were never crawled, and the jobs still completed. The parser now drops only true repeats (the same object at the same place) and keeps a quoted post as well as the post itself.
- **Lost tasks:** a crawler that hit its drain timeout committed the offsets of the tasks it abandoned, so those tasks were lost and their jobs never finished. Abandoned records are now left for the partition's next owner.
- **Lost jobs:** the finalizer removed ready jobs from Redis before finalising them. A scheduler restart while it waited for the persister lost them; they stayed `running`. Jobs now leave the set only once finalised.
- **Undercounted jobs:** job counters dropped the pages read before a retry or redelivery. They are now saved with the checkpoint.
- **False partial jobs:** a post refreshed by a job, then reached again by the same job's timeline scan, went back to `pending`. The job then ended `partial`.
