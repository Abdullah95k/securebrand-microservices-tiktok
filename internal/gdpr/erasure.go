package gdpr

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Eraser processes pending erasure requests.
type Eraser struct {
	DB       *pgxpool.Pool
	S3       *minio.Client
	Producer *kafka.Producer
}

type request struct {
	id        uuid.UUID
	subject   string
	authorKey []byte
	pageID    *int64
	created   time.Time
}

// Tick processes up to 10 pending requests.
func (e *Eraser) Tick(ctx context.Context) error {
	rows, err := e.DB.Query(ctx, `SELECT id, subject_type, author_key, page_id, created_at FROM erasure_requests
		WHERE status = 'pending' ORDER BY created_at LIMIT 10`)
	if err != nil {
		return err
	}
	reqs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (request, error) {
		var q request
		return q, r.Scan(&q.id, &q.subject, &q.authorKey, &q.pageID, &q.created)
	})
	if err != nil {
		return err
	}
	for _, r := range reqs {
		stats, err := e.process(ctx, r)
		if err != nil {
			slog.Error("erasure failed", "request", r.id, "err", err)
			_, _ = e.DB.Exec(ctx, `UPDATE erasure_requests SET status = 'failed', error = $2 WHERE id = $1`, r.id, err.Error())
			continue
		}
		sb, _ := json.Marshal(stats)
		if _, err := e.DB.Exec(ctx, `UPDATE erasure_requests SET status = 'done', stats = $2, completed_at = now() WHERE id = $1`, r.id, sb); err != nil {
			return err
		}
		slog.Info("erasure done", "request", r.id, "subject", r.subject, "stats", stats)
	}
	return nil
}

func (e *Eraser) process(ctx context.Context, r request) (map[string]int64, error) {
	stats := map[string]int64{}
	ev := &fbv1.ErasureEvent{EventId: r.id.String(), RequestId: r.id.String(), RequestedAt: timestamppb.New(r.created)}
	switch r.subject {
	case "author":
		if len(r.authorKey) == 0 {
			return nil, fmt.Errorf("author erasure without author key")
		}
		ev.SubjectType, ev.AuthorKey = fbv1.SubjectType_SUBJECT_TYPE_AUTHOR, r.authorKey
		var commentIDs []int64
		err := pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('author', $1) ON CONFLICT DO NOTHING`,
				hex.EncodeToString(r.authorKey)); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `UPDATE comments SET text = NULL, text_ranges = NULL, attachments = NULL, author_id = NULL,
				url = NULL, extra = NULL, redacted = true WHERE author_key = $1 RETURNING comment_id`, r.authorKey)
			if err != nil {
				return err
			}
			commentIDs, err = pgx.CollectRows(rows, pgx.RowTo[int64])
			if err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `DELETE FROM comment_authors WHERE author_key = $1`, r.authorKey)
			stats["authors_deleted"] = tag.RowsAffected()
			return err
		})
		if err != nil {
			return nil, err
		}
		stats["comments_redacted"] = int64(len(commentIDs))
		n, err := e.deleteMedia(ctx, `comment_id = ANY($1)`, commentIDs)
		if err != nil {
			return nil, err
		}
		stats["media_deleted"] = n
	case "page":
		if r.pageID == nil {
			return nil, fmt.Errorf("page erasure without page id")
		}
		pid := *r.pageID
		ev.SubjectType, ev.PageId = fbv1.SubjectType_SUBJECT_TYPE_PAGE, pid
		if _, err := e.DB.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('page', $1) ON CONFLICT DO NOTHING`,
			strconv.FormatInt(pid, 10)); err != nil {
			return nil, err
		}
		n, err := e.deleteMedia(ctx, `page_id = $1`, pid)
		if err != nil {
			return nil, err
		}
		stats["media_deleted"] = n
		err = pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
			for _, q := range []struct{ name, sql string }{
				{"comments", `DELETE FROM comments WHERE page_id = $1`},
				{"post_metrics", `DELETE FROM post_metrics WHERE page_id = $1`},
				{"posts", `DELETE FROM posts WHERE page_id = $1`},
				{"post_index", `DELETE FROM post_index WHERE page_id = $1`},
				{"page_snapshots", `DELETE FROM page_snapshots WHERE page_id = $1`},
				{"crawl_cursors", `DELETE FROM crawl_cursors WHERE scope_type = 'page_timeline' AND scope_id = $1`},
				{"schedule", `UPDATE page_schedule SET enabled = false WHERE page_id = $1`},
				{"pages", `UPDATE pages SET status = 'suppressed', vanity = NULL, url = NULL, name = NULL, categories = NULL,
					intro = NULL, followers_count = NULL, likes_count = NULL, profile_pic_url = NULL, cover_url = NULL,
					contact = NULL, extra = NULL WHERE page_id = $1`},
			} {
				tag, err := tx.Exec(ctx, q.sql, pid)
				if err != nil {
					return fmt.Errorf("%s: %w", q.name, err)
				}
				stats[q.name] = tag.RowsAffected()
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown subject type %q", r.subject)
	}
	ev.CompletedAt = timestamppb.Now()
	b := e.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.Erasure, r.id.String(), ev); err != nil {
		return nil, err
	}
	return stats, b.Wait(ctx)
}

// deleteMedia removes stored objects matching where (with one argument)
// and marks the rows deleted.
func (e *Eraser) deleteMedia(ctx context.Context, where string, arg any) (int64, error) {
	rows, err := e.DB.Query(ctx, `SELECT media_id::text, s3_bucket, s3_key FROM media WHERE `+where+
		` AND s3_key IS NOT NULL AND status IN ('stored','ready')`, arg)
	if err != nil {
		return 0, err
	}
	type obj struct{ id, bucket, key string }
	objs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (obj, error) {
		var o obj
		return o, r.Scan(&o.id, &o.bucket, &o.key)
	})
	if err != nil {
		return 0, err
	}
	b := e.Producer.NewBatch()
	for _, o := range objs {
		if e.S3 != nil {
			if err := e.S3.RemoveObject(ctx, o.bucket, o.key, minio.RemoveObjectOptions{}); err != nil {
				return 0, err
			}
		}
		if _, err := e.DB.Exec(ctx, `UPDATE media SET status = 'deleted', deleted_at = now() WHERE media_id = $1`, o.id); err != nil {
			return 0, err
		}
		_ = b.AddProto(ctx, topics.MediaDeleted, o.id, &fbv1.MediaDeleted{EventId: o.id + ":erasure", MediaId: o.id,
			Object: &fbv1.S3Object{Bucket: o.bucket, Key: o.key}, Reason: fbv1.DeleteReason_DELETE_REASON_ERASURE, DeletedAt: timestamppb.Now()})
		metrics.MediaDeleted.WithLabelValues("erasure").Inc()
	}
	if _, err := e.DB.Exec(ctx, `UPDATE media SET status = 'deleted', deleted_at = now(), source_url = NULL WHERE `+where+
		` AND status NOT IN ('deleted')`, arg); err != nil {
		return 0, err
	}
	return int64(len(objs)), b.Wait(ctx)
}
