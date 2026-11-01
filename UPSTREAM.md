# Upstream: igs and fbs

This repository (tks, the TikTok account scraper) owns a copy of the code of igs, the Instagram scraper, which owns a copy of the code of fbs, the Facebook page scraper. The decision and its reasons are in [ADR 0006](docs/tiktok/adr/0006-tks-code-organisation.md). tks work never changes igs's or fbs's repositories.

- **Upstream repositories:** igs, [`Abdullah95k/securebrand-microservices-igs`](https://github.com/Abdullah95k/securebrand-microservices-igs); fbs, [`Abdullah95k/securebrand-microservices`](https://github.com/Abdullah95k/securebrand-microservices).
- **Copied from:** igs `9463b91` (main, 2026-10-31), with its full history, merged into tks's `main` (`git merge --allow-unrelated-histories`), so `git blame` reaches back through igs into fbs (igs was copied from fbs at `f1377a9`).
- **Seed changes:** the Go module renamed to `github.com/abdullah95k/securebrand-microservices-tiktok` (protobuf code regenerated); the Instagram-only code removed (`internal/ig`, `internal/fakeig`, `cmd/igs-spike`, `configs/ig`, `docs/instagram`, `docs/runbooks/igs-probe.md`, the `igs-probe*` and `igs-spike-bin` Makefile targets, `phase0/ig-profiles.example.txt`); igs's README moved to [`docs/fbs-readme.md`](docs/fbs-readme.md). The copied Facebook code stays as the regression net of the copied pipeline until tks's own suites replace it (ADR 0006 §1).
- **Names on shared infrastructure** were renamed from igs's to tks's in M1b (`tks:` Valkey keys, locks `4_400_*` and `tks:*`, groups `tks-*`, header `tks-type`, API keys `tks_…`, buckets `tks-media`/`tks-raw`, metrics `tks_` with `tks_tt_*` for the platform series, alerts `Tks…`, env `TKS_`). **The copied pipeline's Kafka topics are still fbs's** (`fb.*.v1`, `fbs.task.*`) until M3 adds `tt.*.v1` and `tks.task.*`, so tks's pipeline does not run on shared Kafka before M3 (ADR 0006 §5). The probe uses none of it.

To port an igs change: `git remote add igs https://github.com/Abdullah95k/securebrand-microservices-igs`, `git fetch igs`, then `git cherry-pick <sha>` while the paths still match, or port it by hand. Record it below either way. Fetching is read-only; never push to igs's or fbs's repositories.

A cherry-picked change conflicts on any import line it touches, because the module paths differ. Resolve those by keeping this repository's paths, then run `go build ./...`; if it adds or changes a `.proto` file, run `buf generate` afterwards.

## Ported from igs or fbs

igs has not done its M2 (at `8ed9c61`, 2026-11-01, only probe changes since `9463b91`). fbs has: between `f1377a9` and `b50b4a7` (2026-10-31 to 11-01) it merged 42 commits of review fixes. tks's M2 ported, by hand, the parts that fix M2's defects; the others are listed under [fbs fixes not ported yet](#fbs-fixes-not-ported-yet).

| Date | Upstream commit | What | tks PR |
|---|---|---|---|
| 2026-11-01 | fbs `ca4769e` | A task of a kind the worker has no stage for is dead-lettered as received (it crashed the crawler) | M2 |
| 2026-11-01 | fbs `907e08a` (part) | The Kafka consumer recovers a handler's panic as an error (`tks_kafka_handler_panics_total`); `checkpoint.ClearDone` | M2 |
| 2026-11-01 | fbs `9b1863c` (part) | Job ownership: `GET`, `cancel` and `results` of another client's job are 404; a scheduled job shows to the clients registered to its page (`dispatch.GetFor`, `visibleTo`) | M2 |
| 2026-11-01 | fbs `586a6ed`, `9b1863c` (part) | Suppression in the persister: `gdpr.Guard` (shared lock, tks's own number `4_400_002`) on every write, the erasure's exclusive lock, `gdpr.Redact`; posts, comments, media and profiles of an erased page dropped, an erased author's comments stored redacted | M2 |
| 2026-11-01 | fbs `b783e17`, `b6510a1` (part) | The persister bisects a batch Postgres refuses and skips at most 3 refused records (`isDataError`); an undecodable record is skipped, not written as far as it decoded | M2 |
| 2026-11-01 | fbs `adf2964` | CI's integration job (and `make test-integration`) runs every package | M2 |

## fbs fixes not ported yet

From fbs's review rounds (`f1377a9..b50b4a7`). Areas tks reuses as they are, so each matters before tks's pipeline runs on it (M5, M6):
- **crawl pipeline:** leases owned per run (a second copy of a task waits); one retry-relay consumer per retry topic; a task's completion not dropped when Redis fails; page suppression checked in `Worker.Run`; post "incomplete marks" for lost reply threads and the pass logic around them; `crawler.max_waits` (waits for exits counted apart from stalls, tier chosen by stalls); cancelled segments marked incomplete;
- **Kafka consumer:** revoked or lost partitions cancel their handlers and refuse their records;
- **persister:** NUL characters stripped (tks sets such a record aside instead), comments past retention not written, a vanity moving between pages, missing metrics or snapshot partitions skipped in a savepoint, `jsonbSafe`, erased authors' mentions cleared;
- **erasure and media:** erasure retries with backoff (migration), media author keys, tombstones and `source_expired`, the janitor acting on stored media, MediaDeleted sent before rows are marked, the raw bucket's lifecycle rule;
- **scheduler:** maintenance steps independent, the reaper paging and sparing queued jobs, the finalizer's terminal event before its update, shared `deleted_at`;
- **API:** results streamed in batches with an end line, page registration checks on `PATCH`/purge, canonical page references, timeouts;
- **leader:** a 3 s lock check, a connection whose unlock fails closed.

Facebook-only fixes (parsers, page-load gates and walls, the pilot, the Phase 0 report, the fixture scrubber) are not for tks.

## tks fixes igs or fbs may want

Fixes made in tks's copy of shared code that igs or fbs still lack. Their own sessions decide whether to port them.

| Date | tks PR | Defect | Upstream location |
|---|---|---|---|
| 2026-11-01 | M2 | `fbctl dlq replay` re-produced dead letters that the worker then skipped as done for 7 days (igs; fbs clears the mark with a header). tks's `stage.Replayer` also resets the task's retry budget (a replay was dead-lettered again on its first failure) and reopens its job: pending again, `tasks_failed` less one, a finalised job back to `running` and finalised again (`jobs.Tracker.Reopen`; the finalizer skips a job with pending tasks) | igs and fbs: `cmd/fbctl/commands.go` `dlqReplay`, `internal/crawl/jobs`, `internal/scheduler/finalizer.go` |
| 2026-11-01 | M2 | Records the persister skips are only logged (fbs) or retried forever (igs): tks sets them aside in a quarantine topic (`fbs.persist.dlq`), counted by `tks_persist_quarantined_total` with the alert `TksPersisterQuarantine`, replayable with `fbctl dlq replay --topic fbs.persist.dlq` | igs and fbs: `internal/persist/consumer.go` |
| 2026-11-01 | M2 | `limits.max_comments_per_post` capped each segment of a post's comment stream, not the post: `PostScope.items_seen` (field 11) carries the count to the continuation | igs and fbs: `internal/crawl/stage/post.go`, `tasks.proto` |
| 2026-11-01 | M2 | HTTP 400, 401, 403 and other 4xx read as OK when their body held no error: 401 is a login wall, 403 a block, 400 a stale request on an API call, other 4xx soft errors; detect rules can be replaced from the configuration (`fb.detect`, `detect.Overrides`) | igs: `internal/scrape/detect`; fbs: `internal/fb/detect` |
| 2026-11-01 | M2 | The rate-limit cooldown never grew past a minute (the second block in a row retired the identity first): rate limits in a row now rest the identity 1, 2 and 4 minutes, and the 4th retires it | igs: `internal/scrape/session/identity.go`; fbs: `internal/fb/session/identity.go` |
| 2026-11-01 | M2 | Every identity of a pod used `fb.fingerprint_profile`: `fb.fingerprint_profiles` passes a list to `session.PoolConfig.Profiles` | igs: `internal/app/crawler.go`, `registrysync.go` |
| 2026-11-01 | M2 | `TestBlockedIdentitiesRetryAndTruncation` failed now and then (once in igs's CI): a task could stall 5 times in a row under the 1-in-4 rate-limit fault; the test now allows 12 | igs and fbs: `internal/e2e/e2e_test.go` |
