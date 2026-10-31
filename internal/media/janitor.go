package media

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Janitor deletes stored media once every required consumer acknowledged
// it, and enforces the TTL as a safety net.
type Janitor struct {
	DB                   *pgxpool.Pool
	S3                   *minio.Client
	Bucket               string
	Producer             *kafka.Producer
	RequiredConsumers    []string
	MaxRetryableFailures int
	TTL                  time.Duration
}

// Handle consumes fb.media.processed.v1.
func (j *Janitor) Handle(ctx context.Context, rec *kgo.Record) error {
	var ack fbv1.MediaProcessed
	if err := proto.Unmarshal(rec.Value, &ack); err != nil {
		slog.Error("janitor: undecodable ack", "err", err)
		return nil
	}
	if ack.MediaId == "" || ack.Consumer == "" {
		return nil
	}
	status := strings.ToLower(strings.TrimPrefix(ack.Status.String(), "PROCESS_STATUS_"))
	at := time.Now().UTC()
	if ack.ProcessedAt != nil {
		at = ack.ProcessedAt.AsTime()
	}
	tag, err := j.DB.Exec(ctx, `INSERT INTO media_acks (media_id, consumer, status, error, acked_at)
		SELECT $1::uuid, $2, $3, NULLIF($4,''), $5 WHERE EXISTS (SELECT 1 FROM media WHERE media_id = $1::uuid)
		ON CONFLICT (media_id, consumer) DO UPDATE SET status = EXCLUDED.status, error = EXCLUDED.error, acked_at = EXCLUDED.acked_at`,
		ack.MediaId, ack.Consumer, status, ack.Error, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // unknown media (already purged)
	}
	if ack.Status == fbv1.ProcessStatus_PROCESS_STATUS_FAILED_RETRYABLE {
		return j.retry(ctx, ack.MediaId)
	}
	return j.deleteIfAcked(ctx, ack.MediaId)
}

// deleteIfAcked deletes the object when all required consumers are done.
func (j *Janitor) deleteIfAcked(ctx context.Context, mediaID string) error {
	var done int
	if err := j.DB.QueryRow(ctx, `SELECT count(DISTINCT consumer) FROM media_acks WHERE media_id = $1
		AND consumer = ANY($2) AND status IN ('processed','failed_permanent')`, mediaID, j.RequiredConsumers).Scan(&done); err != nil {
		return err
	}
	if done < len(j.RequiredConsumers) {
		return nil
	}
	return j.delete(ctx, mediaID, fbv1.DeleteReason_DELETE_REASON_ACKED)
}

func (j *Janitor) retry(ctx context.Context, mediaID string) error {
	var failures int
	var bucket, key *string
	var size *int64
	err := j.DB.QueryRow(ctx, `UPDATE media SET retryable_failures = retryable_failures + 1 WHERE media_id = $1 AND status = 'ready'
		RETURNING retryable_failures, s3_bucket, s3_key, size_bytes`, mediaID).Scan(&failures, &bucket, &key, &size)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if failures > j.MaxRetryableFailures {
		return j.delete(ctx, mediaID, fbv1.DeleteReason_DELETE_REASON_ACKED)
	}
	ready := &fbv1.MediaReady{EventId: mediaID + ":ready:" + strconv.Itoa(failures+1), MediaId: mediaID,
		Object: &fbv1.S3Object{Bucket: deref(bucket), Key: deref(key), Size: derefInt(size)}, Attempt: uint32(failures + 1)}
	if err := j.fillReady(ctx, ready); err != nil {
		return err
	}
	b := j.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.MediaReady, mediaID, ready); err != nil {
		return err
	}
	return b.Wait(ctx)
}

func (j *Janitor) fillReady(ctx context.Context, r *fbv1.MediaReady) error {
	var kind, variant, fbID string
	var post, page, comment *int64
	var w, h *int32
	var dur *int64
	var sha []byte
	var ct, alt *string
	var deleteAfter *time.Time
	err := j.DB.QueryRow(ctx, `SELECT fb_media_id, kind, variant, post_id, page_id, comment_id, width, height, duration_ms, sha256,
		mime, alt_text, delete_after FROM media WHERE media_id = $1`, r.MediaId).
		Scan(&fbID, &kind, &variant, &post, &page, &comment, &w, &h, &dur, &sha, &ct, &alt, &deleteAfter)
	if err != nil {
		return err
	}
	r.FbMediaId, r.Variant = fbID, variant
	r.Kind = fbv1.MediaKind(fbv1.MediaKind_value["MEDIA_KIND_"+strings.ToUpper(kind)])
	r.PostId, r.PageId, r.CommentId = derefInt(post), derefInt(page), comment
	r.Width, r.Height, r.DurationMs = derefInt32(w), derefInt32(h), derefInt(dur)
	r.Object.Sha256, r.Object.ContentType, r.AltText = sha, deref(ct), deref(alt)
	if deleteAfter != nil {
		r.DeleteAfter = timestamppb.New(*deleteAfter)
	}
	return nil
}

func (j *Janitor) delete(ctx context.Context, mediaID string, reason fbv1.DeleteReason) error {
	var bucket, key *string
	err := j.DB.QueryRow(ctx, `SELECT s3_bucket, s3_key FROM media WHERE media_id = $1 AND status = 'ready'`, mediaID).Scan(&bucket, &key)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if key != nil {
		if err := j.S3.RemoveObject(ctx, deref(bucket), *key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
	}
	status := "deleted"
	if reason == fbv1.DeleteReason_DELETE_REASON_TTL_EXPIRED {
		status = "expired"
	}
	if _, err := j.DB.Exec(ctx, `UPDATE media SET status = $2, deleted_at = now(),
		processed_at = CASE WHEN $2 = 'deleted' THEN now() ELSE processed_at END WHERE media_id = $1`, mediaID, status); err != nil {
		return err
	}
	name := strings.ToLower(strings.TrimPrefix(reason.String(), "DELETE_REASON_"))
	metrics.MediaDeleted.WithLabelValues(name).Inc()
	b := j.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.MediaDeleted, mediaID, &fbv1.MediaDeleted{EventId: mediaID + ":" + name, MediaId: mediaID,
		Object: &fbv1.S3Object{Bucket: deref(bucket), Key: deref(key)}, Reason: reason, DeletedAt: timestamppb.Now()}); err != nil {
		return err
	}
	return b.Wait(ctx)
}

// Sweep deletes ready objects past delete_after (no ack in time).
func (j *Janitor) Sweep(ctx context.Context) error {
	for {
		rows, err := j.DB.Query(ctx, `SELECT media_id::text FROM media WHERE status = 'ready' AND delete_after < now() LIMIT 500`)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := j.delete(ctx, id, fbv1.DeleteReason_DELETE_REASON_TTL_EXPIRED); err != nil {
				return err
			}
		}
		if len(ids) < 500 {
			return nil
		}
	}
}

// Orphans deletes stored objects that no ready media row references, older
// than the TTL plus a grace period.
func (j *Janitor) Orphans(ctx context.Context) error {
	cutoff := time.Now().Add(-j.TTL - time.Hour)
	for obj := range j.S3.ListObjects(ctx, j.Bucket, minio.ListObjectsOptions{Prefix: "media/", Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if obj.LastModified.After(cutoff) {
			continue
		}
		base := obj.Key[strings.LastIndexByte(obj.Key, '/')+1:]
		id := strings.SplitN(base, ".", 2)[0]
		var ready bool
		_ = j.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM media WHERE media_id::text = $1 AND status = 'ready')`, id).Scan(&ready)
		if ready {
			continue
		}
		if err := j.S3.RemoveObject(ctx, j.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		metrics.MediaDeleted.WithLabelValues("orphan").Inc()
		slog.Info("janitor: orphan removed", "key", obj.Key)
	}
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func derefInt32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

// Lifecycle installs the bucket lifecycle rule (whole days only) that backs
// up the janitor's TTL. Stores without lifecycle support log and continue.
func Lifecycle(ctx context.Context, s3 *minio.Client, bucket string, ttl time.Duration) {
	days := int(ttl.Hours()/24) + 1
	cfg := lifecycleConfig(days)
	if err := s3.SetBucketLifecycle(ctx, bucket, cfg); err != nil {
		slog.Warn("media: bucket lifecycle not applied (store may not support it)", "bucket", bucket, "err", err)
	}
}
