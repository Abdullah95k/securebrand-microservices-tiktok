# tks media downloads

tks always publishes media **metadata** (`tt.media.v1`: covers, photos, video renditions, avatars, music covers, comment images) and stores it in `tt.media`. It downloads the **files** only when `tt.media.include` is on (`TKS_TT__MEDIA__INCLUDE=true`), which is **off by default** until the analysis service exists (build brief D5). Design: [ADR 0007 §15](../tiktok/adr/0007-tks-data-model-and-contracts.md#15-settled-in-m6c-media-the-objectives-and-the-budget).

## What happens when it is on

1. The crawler queues a download task (`tks.task.media`) for each post's cover, one video rendition (`sd`, or `hd` with the job's video policy) and each photo of a photo post; for comment images also when `limits.include_comment_media` is on. A video's task carries the `tt_chain_token` cookie and user agent of the identity that read its link: TikTok's video hosts refuse a video without that cookie (T18).
2. `tks-media-downloader` fetches the file over its own egress (never the crawl proxies), sending the cookie only to the hosts of `tt.media.cookie_domains` (`tiktok.com` and its subdomains, as a browser does), stores it in the `tks-media` bucket under `media/<2 hex>/<media id>.<ext>`, marks the row `stored` and then `ready`, and announces it on **`tt.media.ready.v1`** (`common.v1.MediaReady`, `platform: TIKTOK`, **no `delete_after`**).
3. A file handed over once is never downloaded again, however often its post is read.

## Who deletes the files

**Not tks.** Deleting downloaded files is another service's job (owner, 2026-11-02): tks sets no TTL, installs no bucket lifecycle rule and runs no janitor. At start, `tks-media-downloader` logs a warning for any bucket rule that would expire objects.

The contract for the consuming and deleting services:
- **Read** `tt.media.ready.v1`; the object is at `object.bucket`/`object.key` (a presigned link in `presigned_url` when `media.presign` is on).
- **Acknowledge** on `tt.media.processed.v1` (`common.v1.MediaProcessed`, `consumer` set to the service's name): `PROCESSED` or `FAILED_PERMANENT` is recorded in `tt.media_acks` (and `processed_at`); `FAILED_RETRYABLE` makes tks announce the file again (`attempt` 2, 3, …) up to `media.max_retryable_failures` times, then nothing more.
- **After deleting a file**, publish `common.v1.MediaDeleted` on **`tt.media.deleted.v1`** (key: the media id). tks marks the row `deleted` with `deleted_at`, so the API no longer names an object that is gone; the file is not downloaded again.

## Turning it on

1. Make sure the deleting service runs, or the bucket will only grow.
2. Set `TKS_TT__MEDIA__INCLUDE=true` on `tks-crawler` (it queues the tasks) and run `tks-media-downloader` with the same configuration (`s3.media_bucket`, `media.concurrency`, `media.max_bytes`, `media.presign`).
3. Watch `tks_media_download_total` by `kind` and `outcome` (`ready`, `skipped`, `expired`, `refused`, `failed`, `retry`) and `tks_media_discovery_to_stored_seconds`.

## Refused downloads

`tks_media_download_total{outcome="refused"}` counts files a media host refused (HTTP 403 or 410) **before** their link expired. For videos this most likely means the host did not accept the reading identity's `tt_chain_token` (from the downloader's own IP and TLS fingerprint: T33, for Phase 0 to confirm). The row is marked `source_expired`, and the next read of the post brings a fresh link and cookie, which is downloaded. The alert `TksMediaRefused` fires above 10 % for 30 minutes:
1. Compare by kind: images refused too means the links themselves are refused (expired early, or the egress is blocked).
2. Only videos: check that the crawler's identities still get a `tt_chain_token` from their page loads (fakett and the probe's `video_chain_cookie` step show how), and whether the downloads succeed from the crawl tier's exits (a Phase 0 measurement).

`outcome="expired"` counts links that had expired before the download: the downloader is behind the crawler (`media.concurrency`) or the task waited too long.
