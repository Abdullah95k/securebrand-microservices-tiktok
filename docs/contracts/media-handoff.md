# Media handoff contract (fbs → analysis service)

fbs downloads every media item it discovers (post photos, video renditions,
thumbnails, comment photos/GIFs/stickers) into **temporary** object storage,
tells the analysis service about it, and deletes it once the analysis service
says it is done. Nothing is kept long term.

```
fbs-media-downloader ──fb.media.ready.v1──▶ analysis service
                                               │ reads s3://bucket/key
analysis service ──fb.media.processed.v1──▶ fbs-media-janitor ──▶ DeleteObject
                                                               └─▶ fb.media.deleted.v1
```

Schemas: `api/proto/securebrand/fb/v1/media.proto` (generated Go:
`gen/go/securebrand/fb/v1`). Records are protobuf; the `igs-type` header holds
the full message name and `content-type` is `application/x-protobuf`. Topic
names get `kafka.topic_prefix` prepended.

## 1. `fb.media.ready.v1` (fbs produces), key = `media_id`

| Field | Meaning |
|---|---|
| `media_id` | Stable UUIDv5 of (Facebook media id, variant). The same image seen in two scrapes has the same id and is **downloaded once**. |
| `kind`, `variant` | `PHOTO`/`VIDEO`/`REEL`/`THUMBNAIL`/`COMMENT_PHOTO`/`COMMENT_GIF`/`STICKER`/`COMMENT_VIDEO`; variant `original`, `sd`, `hd`, `thumbnail`, `link_preview`. |
| `page_id`, `post_id`, `comment_id` | Owner. Join with `fb.post.v1` / `fb.comment.v1` or the REST API. |
| `object` | `bucket`, `key`, `size`, `sha256`, `content_type`. |
| `width`, `height`, `duration_ms`, `alt_text` | Metadata from Facebook (alt text is Facebook's automatic description when present). |
| `delete_after` | The object is deleted at this time **even without an ack** (default 72h after storage). |
| `attempt` | 1 for the first announcement; higher after a `FAILED_RETRYABLE` re-announcement. |
| `presigned_url` | Only when `media.presign: true`; otherwise read with the read-only credentials for the media bucket. |
| `job_ids` | The job that discovered the media. |

Delivery is at least once. Deduplicate on `event_id`
(`<media_id>:ready:<attempt>`).

## 2. `fb.media.processed.v1` (analysis service produces), key = `media_id`

```proto
message MediaProcessed {
  string event_id = 1;      // unique per ack, e.g. "<media_id>:<consumer>"
  string media_id = 2;
  string consumer = 3;      // must match an entry in media.required_consumers
  ProcessStatus status = 4; // PROCESSED | FAILED_PERMANENT | FAILED_RETRYABLE
  google.protobuf.Timestamp processed_at = 5;
  string error = 6;
}
```

* `PROCESSED`, `FAILED_PERMANENT`: you are done with the file. Once **every**
  consumer listed in `media.required_consumers` acked one of these, the object
  is deleted and `fb.media.deleted.v1` is published (reason `ACKED`).
* `FAILED_RETRYABLE`: fbs re-publishes `MediaReady` (`attempt+1`) up to
  `media.max_retryable_failures` times, then treats it as permanent.
* Acks for unknown or already deleted media are ignored, so replays are safe.

## 3. `fb.media.deleted.v1` (fbs produces)

Reason `ACKED`, `TTL_EXPIRED` (no ack before `delete_after`), `ERASURE`
(GDPR erasure of the author or page) or `ORPHAN` (reconciliation found an
object without a ready row). After this event the key no longer exists.

## Obligations of the analysis service

1. Process before `delete_after`. Watch `igs_media_deleted_total{reason="ttl_expired"}`:
   a rising rate means you are lagging.
2. Ack every `MediaReady` exactly with your configured consumer name.
3. Do not copy media to long-term storage unless your own retention and
   privacy review covers it; erasure events (`fb.erasure.v1`) apply to you too.
4. Honour `fb.erasure.v1`: delete everything derived from the subject
   (author key or page id).

## Example consumer (Go, franz-go)

```go
for {
    fetches := cl.PollFetches(ctx)
    fetches.EachRecord(func(r *kgo.Record) {
        var m fbv1.MediaReady
        if proto.Unmarshal(r.Value, &m) != nil { return }
        obj, _ := s3.GetObject(ctx, m.Object.Bucket, m.Object.Key, minio.GetObjectOptions{})
        status := fbv1.ProcessStatus_PROCESS_STATUS_PROCESSED
        if err := analyse(obj, &m); err != nil {
            status = fbv1.ProcessStatus_PROCESS_STATUS_FAILED_RETRYABLE
        }
        ack, _ := proto.Marshal(&fbv1.MediaProcessed{EventId: m.MediaId + ":analysis",
            MediaId: m.MediaId, Consumer: "analysis", Status: status, ProcessedAt: timestamppb.Now()})
        cl.Produce(ctx, &kgo.Record{Topic: "fb.media.processed.v1", Key: []byte(m.MediaId), Value: ack}, nil)
    })
    cl.CommitUncommittedOffsets(ctx)
}
```

For local testing, `fbctl media ack --consumer analysis` acks every ready item.

## Safety nets

* The janitor sweeps hourly for `ready` media past `delete_after`.
* A bucket lifecycle rule expires `media/` after `ceil(ttl/1 day)` days
  (S3 lifecycle works in whole days). Stores without lifecycle support log a
  warning; the sweep still enforces the TTL.
* A daily reconciliation deletes objects with no `ready` row.
* Keep bucket versioning **off** so deletes free storage.
