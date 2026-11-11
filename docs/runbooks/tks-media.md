# tks media downloads

tks always publishes media **metadata** (`tt.media.v1`: covers, photos, video renditions, avatars, music covers, comment images) and stores it in `tt.media`. It downloads the **files** only when `tt.media.include` is on (`TKS_TT__MEDIA__INCLUDE=true`), which is **off by default** (build brief D5); production turns it on for the analysis service (owner, 2026-11-10). Design: [ADR 0007 §15](../tiktok/adr/0007-tks-data-model-and-contracts.md#15-settled-in-m6c-media-the-objectives-and-the-budget).

**Which kinds.** `tt.media.kinds` lists the kinds downloaded (`cover`, `origin_cover`, `dynamic_cover`, `video`, `photo`, `avatar`, `music_cover`, `comment_image`): every kind by default. A kind left out is never queued; its metadata is still published. The video policy still picks the renditions, and `none` still means no covers. Production leaves out `music_cover` (the master, 2026-11-11), because sb-analysis skips music covers by policy (`policy:image/audio_cover`).

## What happens when it is on

1. The crawler queues a download task (`tks.task.media`) for each post's cover, one video rendition (`sd`, or `hd` with the job's video policy) and each photo of a photo post; for comment images also when `limits.include_comment_media` is on. A video's task carries the `tt_chain_token` cookie and user agent of the identity that read its link: TikTok's video hosts refuse a video without that cookie (T18).
2. `tks-media-downloader` fetches the file over its own egress (never the crawl proxies), sending the cookie only to the hosts of `tt.media.cookie_domains` (`tiktok.com` and its subdomains, as a browser does), stores it in the `tks-media` bucket under `media/<2 hex>/<media id>.<ext>`, marks the row `stored` and then `ready`, and announces it on **`tt.media.ready.v1`** (`common.v1.MediaReady`, `platform: TIKTOK`, **no `delete_after`**) and, with the shared contract on, on `content.media.ready.v1` with the deleting service's deadline (below).
3. A file handed over once is never downloaded again, however often its post is read.

## Who deletes the files

**tks does** (owner, 2026-11-10, replacing the rule of 2026-11-02 that another service would; [ADR 0007 §29](../tiktok/adr/0007-tks-data-model-and-contracts.md#29-settled-in-m20-tks-deletes-its-own-media-files)). `tks-media-downloader`'s **sweeper**, every `tt.content.media.sweep_every` (5 min), deletes each `ready` file:
- once **every** consumer of `tt.content.media.required_consumers` (`[sb-analysis]`) has acknowledged it as processed, failed for good or skipped (reason `acked`); a pending retryable failure does not count;
- otherwise at its **deadline**, the file's stored time + `tt.content.media.delete_after` (**72 h**; reason `expired`).

Each deletion removes the object, goes out on `tt.media.deleted.v1` (`ACKED` or `TTL_EXPIRED`) and, with the contract on, `content.media.deleted.v1` (`PROCESSED` or `EXPIRED`), and marks the row `deleted`. Rows are claimed with `SKIP LOCKED`, so several downloader pods sweep side by side. tks still installs no bucket lifecycle rule; at start the downloader warns of any rule that would expire objects. The bucket's credentials must allow deleting under `s3.media_prefix`. The sweeper deletes **only** objects in `s3.media_bucket` whose key starts with `s3.media_prefix` (`tks/`): the media bucket is shared with the other platforms' services (`igs/`, `yts/`, …) and tks's key may delete anywhere in it, so a row naming any other object is never deleted (checked in the query and again before each delete).

The contract for the consuming services:
- **Read** `tt.media.ready.v1`; the object is at `object.bucket`/`object.key` (a presigned link in `presigned_url` when `media.presign` is on).
- **Acknowledge** on `tt.media.processed.v1` (`common.v1.MediaProcessed`, `consumer` set to the service's name): `PROCESSED` or `FAILED_PERMANENT` is recorded in `tt.media_acks` (and `processed_at`); `FAILED_RETRYABLE` makes tks announce the file again (`attempt` 2, 3, …) up to `media.max_retryable_failures` times, then nothing more.
- **Deletions** go out on **`tt.media.deleted.v1`** (key: the media id) from tks's sweeper; the ledger also accepts them from elsewhere and marks the row `deleted` with `deleted_at`, so the API no longer names an object that is gone; the file is not downloaded again.

## The shared contract's hand-off (M15b)

With `tt.content.enabled` on, every announcement also goes on **`content.media.ready.v1`** (`securebrand.media.v1.MediaReady`, the contract's; [ADR 0007 §25](../tiktok/adr/0007-tks-data-model-and-contracts.md#25-settled-in-m15b-the-shared-contracts-media-hand-off-coverage-and-candidates)), in the same batch as tks's own:
- `media_id` is `tt:media:<uuid>`; `owner_id` the post, comment, sound or account; `account_id` the account; `object.sha256` in hex.
- **`delete_after`** = the time the file was stored + `tt.content.media.delete_after` (**72 h** by default; at least 24 h, or tks refuses to start). The same deadline on every announcement of a file. It is the sweeper's deadline.
- **`required_consumers`** = `tt.content.media.required_consumers` (`[sb-analysis]` by default; names from the contract's registry).
- `attempt` 1, then 2, 3, 4 after `FAILED_RETRYABLE` acknowledgements (`media.max_retryable_failures: 3`, the contract's limit).

`tks-media-downloader`'s ledger then also reads **`content.media.processed.v1`** and **`content.media.deleted.v1`** (the contract's `MediaProcessed` and `MediaDeleted`, `media_id` `tt:media:<uuid>`), and records them as it records tks's own topics: an acknowledgement in `tt.media_acks` (`SKIPPED` is recorded as `skipped`), a retryable failure announced again on both topics, a deletion marked on the row. `tks_content_events_total{topic="content.media.ready.v1"}` counts the announcements (`sent`, `invalid`).

## Turning it on

1. Make sure the analysis service acknowledges as `sb-analysis` (else every file waits for its 72 h deadline). Watch `tks_media_deleted_total{reason}`.
2. Set `TKS_TT__MEDIA__INCLUDE=true` on `tks-crawler` (it queues the tasks) and run `tks-media-downloader` with the same configuration (`s3.media_bucket`, `media.concurrency`, `media.max_bytes`, `media.presign`).
3. Watch `tks_media_download_total` by `kind` and `outcome` (`ready`, `skipped`, `expired`, `refused`, `failed`, `retry`) and `tks_media_discovery_to_stored_seconds`.

## Refused downloads

`tks_media_download_total{outcome="refused"}` counts files a media host refused (HTTP 403 or 410) **before** their link expired. For videos this most likely means the host did not accept the reading identity's `tt_chain_token` (from the downloader's own IP and TLS fingerprint: T33, for Phase 0 to confirm). The row is marked `source_expired`, and the next read of the post brings a fresh link and cookie, which is downloaded. The alert `TksMediaRefused` fires above 10 % for 30 minutes:
1. Compare by kind: images refused too means the links themselves are refused (expired early, or the egress is blocked).
2. Only videos: check that the crawler's identities still get a `tt_chain_token` from their page loads (fakett and the probe's `video_chain_cookie` step show how), and whether the downloads succeed from the crawl tier's exits (a Phase 0 measurement).

`outcome="expired"` counts links that had expired before the download: the downloader is behind the crawler (`media.concurrency`) or the task waited too long.
