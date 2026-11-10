package media

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-contracts/conformance"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Sweeper deletes the files tks handed over (owner, 2026-11-10: tks
// deletes its own files, replacing D5): a ready file once every required
// consumer has acknowledged it (processed, failed for good, or skipped by
// the consumer's policy), else when its delete_after passes (stored plus
// tt.content.media.delete_after, 72 h). Each deletion removes the object,
// goes out on tt.media.deleted.v1 and, with the contract on (Contract),
// content.media.deleted.v1, and marks the row deleted: the ledger and the
// API then know it is gone, and the file is never downloaded again.
type Sweeper struct {
	DB       *pgxpool.Pool
	S3       *minio.Client
	Producer *kafka.Producer
	// Bucket and Prefix bound what the sweeper deletes: only objects in
	// Bucket whose key starts with Prefix (s3.media_bucket and
	// s3.media_prefix, "tks/" in production). The media bucket is shared
	// with the other platforms' services (igs/, yts/, ...), and tks's key
	// may delete anywhere in it, so a row naming any other object is left
	// alone, never deleted.
	Bucket string
	Prefix string
	// RequiredConsumers must all acknowledge a file before it goes early;
	// DeleteAfter is the deadline after its stored time.
	RequiredConsumers []string
	DeleteAfter       time.Duration
	// Contract sends content.media.deleted.v1 too; Options are its checks.
	Contract bool
	Options  conformance.Options
	// Batch is how many files one transaction deletes.
	Batch int
	Now   func() time.Time
}

func (s *Sweeper) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

// owns reports whether an object is tks's to delete: in its bucket, under
// its prefix.
func (s *Sweeper) owns(bucket, key string) bool {
	return bucket == s.Bucket && key != "" && strings.HasPrefix(key, s.Prefix)
}

// done are the acknowledgements that end a consumer's interest in a file.
var done = []string{"processed", "failed_permanent", "skipped"}

// Sweep deletes every due file, a batch per transaction, and returns how
// many it deleted. Due rows are claimed with SKIP LOCKED, so several
// downloader pods sweep side by side without deleting a file twice.
func (s *Sweeper) Sweep(ctx context.Context) (int, error) {
	if len(s.RequiredConsumers) == 0 || s.DeleteAfter <= 0 {
		return 0, fmt.Errorf("media sweeper: required consumers and delete_after must be set")
	}
	if s.Bucket == "" {
		return 0, fmt.Errorf("media sweeper: the media bucket must be set")
	}
	batch := s.Batch
	if batch <= 0 {
		batch = 100
	}
	total := 0
	for {
		n, err := s.sweepBatch(ctx, batch)
		total += n
		if err != nil || n < batch {
			return total, err
		}
	}
}

type dueFile struct {
	id, bucket, key, kind string
	size                  int64
	acked                 bool
}

func (s *Sweeper) sweepBatch(ctx context.Context, batch int) (int, error) {
	now := s.now()
	n := 0
	err := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT m.media_id::text, coalesce(m.s3_bucket, ''), coalesce(m.s3_key, ''), m.kind,
				coalesce(m.size_bytes, 0),
				NOT EXISTS (SELECT 1 FROM unnest($1::text[]) AS c(consumer) WHERE NOT EXISTS (
					SELECT 1 FROM media_acks a WHERE a.media_id = m.media_id AND a.consumer = c.consumer AND a.status = ANY($2)))
			FROM media m
			WHERE m.status = 'ready' AND m.s3_bucket = $6 AND left(m.s3_key, length($7)) = $7
			  AND (coalesce(m.delete_after, m.stored_at + $3::interval) <= $4
			    OR NOT EXISTS (SELECT 1 FROM unnest($1::text[]) AS c(consumer) WHERE NOT EXISTS (
					SELECT 1 FROM media_acks a WHERE a.media_id = m.media_id AND a.consumer = c.consumer AND a.status = ANY($2))))
			ORDER BY m.stored_at LIMIT $5 FOR UPDATE OF m SKIP LOCKED`,
			s.RequiredConsumers, done, s.DeleteAfter, now, batch, s.Bucket, s.Prefix)
		if err != nil {
			return err
		}
		files, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (dueFile, error) {
			var f dueFile
			return f, r.Scan(&f.id, &f.bucket, &f.key, &f.kind, &f.size, &f.acked)
		})
		if err != nil || len(files) == 0 {
			return err
		}
		b := s.Producer.NewBatch()
		ids := make([]string, 0, len(files))
		for _, f := range files {
			if !s.owns(f.bucket, f.key) {
				// The query selects none; checked again before any delete.
				return fmt.Errorf("media sweeper: %s names %s/%s, outside %s/%s*", f.id, f.bucket, f.key, s.Bucket, s.Prefix)
			}
			if f.key != "" {
				// Removing an object that is already gone succeeds.
				if err := s.S3.RemoveObject(ctx, f.bucket, f.key, minio.RemoveObjectOptions{}); err != nil {
					return fmt.Errorf("media sweeper: remove %s: %w", f.id, err)
				}
			}
			reason := commonv1.DeleteReason_DELETE_REASON_TTL_EXPIRED
			if f.acked {
				reason = commonv1.DeleteReason_DELETE_REASON_ACKED
			}
			ev := &commonv1.MediaDeleted{EventId: f.id + ":deleted", MediaId: f.id, Reason: reason, DeletedAt: timestamppb.New(now),
				Platform: commonv1.Platform_PLATFORM_TIKTOK,
				Object:   &commonv1.S3Object{Bucket: f.bucket, Key: f.key, Size: f.size}}
			if err := b.AddProto(ctx, topics.MediaDeleted, f.id, ev); err != nil {
				return err
			}
			if s.Contract {
				m := content.MediaDeleted(f.id, f.acked, now)
				if err := content.Check(m, s.Options); err != nil {
					metrics.ContentEvents.WithLabelValues(content.TopicMediaDeleted, "invalid").Inc()
					slog.Warn("media sweeper: deleted event breaks the contract, not sent", "media", f.id, "err", err)
				} else {
					rec, err := content.Record(s.Producer, content.TopicMediaDeleted, m.MediaId, m)
					if err != nil {
						return err
					}
					b.Add(ctx, rec)
					metrics.ContentEvents.WithLabelValues(content.TopicMediaDeleted, "sent").Inc()
				}
			}
			ids = append(ids, f.id)
		}
		if err := b.Wait(ctx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE media SET status = 'deleted', deleted_at = $2 WHERE media_id = ANY($1::uuid[])`, ids, now); err != nil {
			return err
		}
		for _, f := range files {
			reason := "expired"
			if f.acked {
				reason = "acked"
			}
			metrics.MediaDeleted.WithLabelValues(reason).Inc()
		}
		n = len(files)
		return nil
	})
	return n, err
}

// Run sweeps every interval until ctx ends; a failed sweep is logged and
// tried again at the next tick.
func (s *Sweeper) Run(ctx context.Context, every time.Duration) error {
	if every <= 0 {
		every = 5 * time.Minute
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		n, err := s.Sweep(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			slog.Warn("media sweeper: sweep failed", "deleted", n, "err", err)
		case n > 0:
			slog.Info("media sweeper: files deleted", "deleted", n)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
