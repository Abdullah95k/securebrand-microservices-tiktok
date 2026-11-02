package media

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Ledger records what happens to a file after its handoff, and deletes
// nothing (owner, 2026-11-02: deleting the files is another service's job):
//   - a consumer's acknowledgement on tt.media.processed.v1 goes to
//     media_acks; a retryable failure announces the file again (attempt n+1)
//     up to MaxRetryableFailures times, as fbs's janitor does, and then
//     leaves it to the deleting service;
//   - a deletion on tt.media.deleted.v1 marks the row deleted, so the API no
//     longer names an object that is gone, and the file is never downloaded
//     again.
type Ledger struct {
	// DB is a pool pinned to schema tt.
	DB                   *pgxpool.Pool
	Producer             *kafka.Producer
	MaxRetryableFailures int
	Now                  func() time.Time
}

// LedgerTopics are the topics the Ledger consumes.
var LedgerTopics = []string{topics.MediaProcessed, topics.MediaDeleted}

func (l *Ledger) now() time.Time {
	if l.Now != nil {
		return l.Now().UTC()
	}
	return time.Now().UTC()
}

// Handle implements kafka.Handler for LedgerTopics.
func (l *Ledger) Handle(ctx context.Context, rec *kgo.Record) error {
	if strings.HasSuffix(rec.Topic, topics.MediaDeleted) {
		return l.deleted(ctx, rec)
	}
	return l.processed(ctx, rec)
}

func (l *Ledger) processed(ctx context.Context, rec *kgo.Record) error {
	var ack commonv1.MediaProcessed
	if err := proto.Unmarshal(rec.Value, &ack); err != nil {
		slog.Error("media ledger: undecodable ack", "err", err)
		return nil
	}
	if ack.MediaId == "" || ack.Consumer == "" {
		return nil
	}
	status := strings.ToLower(strings.TrimPrefix(ack.Status.String(), "PROCESS_STATUS_"))
	at := l.now()
	if ack.ProcessedAt != nil {
		at = ack.ProcessedAt.AsTime()
	}
	tag, err := l.DB.Exec(ctx, `INSERT INTO media_acks (media_id, consumer, status, error, acked_at)
		SELECT $1::uuid, $2, $3, NULLIF($4,''), $5 WHERE EXISTS (SELECT 1 FROM media WHERE media_id = $1::uuid)
		ON CONFLICT (media_id, consumer) DO UPDATE SET status = EXCLUDED.status, error = EXCLUDED.error, acked_at = EXCLUDED.acked_at`,
		ack.MediaId, ack.Consumer, status, ack.Error, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // not a file tks downloaded
	}
	switch ack.Status {
	case commonv1.ProcessStatus_PROCESS_STATUS_PROCESSED:
		_, err := l.DB.Exec(ctx, `UPDATE media SET processed_at = $2 WHERE media_id = $1 AND processed_at IS NULL`, ack.MediaId, at)
		return err
	case commonv1.ProcessStatus_PROCESS_STATUS_FAILED_RETRYABLE:
		return l.retry(ctx, ack.MediaId)
	}
	return nil
}

// retry announces a stored file again for a consumer that failed on it.
func (l *Ledger) retry(ctx context.Context, mediaID string) error {
	var failures int
	obj := &commonv1.S3Object{}
	var kind, variant string
	var user, post, comment *int64
	var w, h *int32
	var dur *int64
	var storedAt, discoveredAt *time.Time
	// stored too: an ack can come before the downloader marks the file
	// ready, right after its event went out.
	err := l.DB.QueryRow(ctx, `UPDATE media SET retryable_failures = retryable_failures + 1
		WHERE media_id = $1 AND status IN ('stored','ready')
		RETURNING retryable_failures, coalesce(s3_bucket, ''), coalesce(s3_key, ''), coalesce(size_bytes, 0),
			coalesce(sha256, ''::bytea), coalesce(mime, ''), kind, variant, user_id, post_id, comment_id, width, height,
			duration_ms, stored_at, discovered_at`, mediaID).
		Scan(&failures, &obj.Bucket, &obj.Key, &obj.Size, &obj.Sha256, &obj.ContentType, &kind, &variant, &user, &post, &comment,
			&w, &h, &dur, &storedAt, &discoveredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if failures > l.MaxRetryableFailures {
		// The file stays: deleting it is the deleting service's call.
		metrics.MediaOutcome.WithLabelValues(kind, "retries_exhausted").Inc()
		return nil
	}
	ready := Ready(mediaID, uint32(failures+1), obj)
	ready.Kind, ready.PlatformKind, ready.Variant = neutralOf(kind), kind, variant
	ready.AccountId, ready.PostId, ready.CommentId = deref(user), post, comment
	ready.Width, ready.Height, ready.DurationMs = deref32(w), deref32(h), deref(dur)
	if storedAt != nil {
		ready.StoredAt = timestamppb.New(*storedAt)
	}
	if discoveredAt != nil {
		ready.DiscoveredAt = timestamppb.New(*discoveredAt)
	}
	b := l.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.MediaReady, mediaID, ready); err != nil {
		return err
	}
	if err := b.Wait(ctx); err != nil {
		return err
	}
	metrics.MediaOutcome.WithLabelValues(kind, "announced_again").Inc()
	return nil
}

// deleted records that the deleting service removed a file.
func (l *Ledger) deleted(ctx context.Context, rec *kgo.Record) error {
	var ev commonv1.MediaDeleted
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		slog.Error("media ledger: undecodable deletion", "err", err)
		return nil
	}
	if ev.MediaId == "" {
		return nil
	}
	at := l.now()
	if ev.DeletedAt != nil {
		at = ev.DeletedAt.AsTime()
	}
	tag, err := l.DB.Exec(ctx, `UPDATE media SET status = 'deleted', deleted_at = $2 WHERE media_id = $1::uuid AND status IN ('stored','ready')`,
		ev.MediaId, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		reason := strings.ToLower(strings.TrimPrefix(ev.Reason.String(), "DELETE_REASON_"))
		metrics.MediaDeleted.WithLabelValues(reason).Inc()
	}
	return nil
}

// neutralOf maps schema tt's kind name back to common.v1's kind.
func neutralOf(kind string) commonv1.MediaKind {
	switch kind {
	case "cover", "origin_cover", "dynamic_cover":
		return commonv1.MediaKind_MEDIA_KIND_COVER
	case "video":
		return commonv1.MediaKind_MEDIA_KIND_VIDEO
	case "photo":
		return commonv1.MediaKind_MEDIA_KIND_PHOTO
	case "avatar":
		return commonv1.MediaKind_MEDIA_KIND_AVATAR
	case "music_cover":
		return commonv1.MediaKind_MEDIA_KIND_AUDIO_COVER
	case "comment_image":
		return commonv1.MediaKind_MEDIA_KIND_COMMENT_IMAGE
	}
	return commonv1.MediaKind_MEDIA_KIND_UNSPECIFIED
}

func deref(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func deref32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
