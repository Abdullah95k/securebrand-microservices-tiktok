package gdpr

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
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
	attempts  int
	authorID  string // the Facebook id, when the request named it
}

// MaxErasureAttempts is how many times a request is tried before it is
// marked failed. With the backoff below that spans about 16 hours.
const MaxErasureAttempts = 12

// errInvalid marks a request that no retry can complete.
var errInvalid = errors.New("invalid erasure request")

// retryAfter is the wait after the n-th failed attempt: 1m, 2m, 4m, ...
// up to 4h.
func retryAfter(n int) time.Duration {
	return min(time.Minute<<min(n-1, 8), 4*time.Hour)
}

// Tick processes up to 10 pending requests that are due. Every step of an
// erasure is idempotent, so a request that fails part-way is retried from
// the start.
func (e *Eraser) Tick(ctx context.Context) error {
	rows, err := e.DB.Query(ctx, `SELECT id, subject_type, author_key, page_id, created_at, attempts, coalesce(input->>'author_id', '')
		FROM erasure_requests
		WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY created_at LIMIT 10`)
	if err != nil {
		return err
	}
	reqs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (request, error) {
		var q request
		return q, r.Scan(&q.id, &q.subject, &q.authorKey, &q.pageID, &q.created, &q.attempts, &q.authorID)
	})
	if err != nil {
		return err
	}
	for _, r := range reqs {
		stats, err := e.process(ctx, r)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			n := r.attempts + 1
			if errors.Is(err, errInvalid) || n >= MaxErasureAttempts {
				slog.Error("erasure failed", "request", r.id, "attempts", n, "err", err)
				_, _ = e.DB.Exec(ctx, `UPDATE erasure_requests SET status = 'failed', attempts = $2, error = $3 WHERE id = $1`, r.id, n, err.Error())
				continue
			}
			slog.Warn("erasure will be retried", "request", r.id, "attempts", n, "err", err)
			_, _ = e.DB.Exec(ctx, `UPDATE erasure_requests SET attempts = $2, error = $3, next_attempt_at = now() + $4 * interval '1 second' WHERE id = $1`,
				r.id, n, err.Error(), int64(retryAfter(n)/time.Second))
			continue
		}
		sb, _ := json.Marshal(stats)
		// Done: the request keeps no trace of who was erased.
		if _, err := e.DB.Exec(ctx, `UPDATE erasure_requests SET status = 'done', stats = $2, completed_at = now(),
			input = input - 'author_id' - 'profile_url' WHERE id = $1`, r.id, sb); err != nil {
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
			return nil, fmt.Errorf("%w: author erasure without author key", errInvalid)
		}
		ev.SubjectType, ev.AuthorKey = fbv1.SubjectType_SUBJECT_TYPE_AUTHOR, r.authorKey
		var commentIDs []int64
		// The Facebook id finds the author's mentions in other people's
		// text, below: from the request, else from the author row (pii_mode
		// full), read before the transaction deletes it. A retry reads the
		// request again, so it never depends on that row.
		fbID := r.authorID
		err := pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
			// Writers wait for this transaction (see Guard): what they wrote
			// before is redacted here, what they write after is redacted by
			// them.
			if err := lockExclusive(ctx, tx); err != nil {
				return err
			}
			if fbID == "" {
				var stored *string
				if err := tx.QueryRow(ctx, `SELECT author_id FROM comment_authors WHERE author_key = $1`, r.authorKey).
					Scan(&stored); err != nil && !errors.Is(err, pgx.ErrNoRows) {
					return err
				}
				if stored != nil && *stored != "" {
					fbID = *stored
					// Kept with the request, so a retry after this
					// transaction (which deletes the row) still has it.
					if _, err := tx.Exec(ctx, `UPDATE erasure_requests SET input = input || jsonb_build_object('author_id', $2::text) WHERE id = $1`,
						r.id, fbID); err != nil {
						return err
					}
				}
			}
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
		// By author key too: the media of a comment not stored yet.
		n, err := e.deleteMedia(ctx, `(author_key = $1 OR comment_id = ANY($2))`, r.authorKey, commentIDs)
		if err != nil {
			return nil, err
		}
		stats["media_deleted"] = n
		if fbID != "" {
			n, err := e.scrubMentions(ctx, fbID, r.authorKey)
			if err != nil {
				return nil, err
			}
			stats["mentions_scrubbed"] = n
		}
	case "page":
		if r.pageID == nil {
			return nil, fmt.Errorf("%w: page erasure without page id", errInvalid)
		}
		pid := *r.pageID
		ev.SubjectType, ev.PageId = fbv1.SubjectType_SUBJECT_TYPE_PAGE, pid
		// Once this commits, writers drop the page's data (see Guard);
		// whatever they wrote before is deleted below.
		if err := pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
			if err := lockExclusive(ctx, tx); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('page', $1) ON CONFLICT DO NOTHING`,
				strconv.FormatInt(pid, 10))
			return err
		}); err != nil {
			return nil, err
		}
		n, err := e.deleteMedia(ctx, `page_id = $1`, pid)
		if err != nil {
			return nil, err
		}
		stats["media_deleted"] = n
		err = pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
			for _, q := range []struct{ name, sql string }{
				// By page id: every row, including comments of a post that was
				// never stored. A scan, but a page erasure is rare and holds no
				// lock writers wait on.
				{"comments", `DELETE FROM comments WHERE page_id = $1`},
				{"post_metrics", `DELETE FROM post_metrics WHERE page_id = $1`},
				{"posts", `DELETE FROM posts WHERE page_id = $1`},
				{"post_index", `DELETE FROM post_index WHERE page_id = $1`},
				{"page_snapshots", `DELETE FROM page_snapshots WHERE page_id = $1`},
				{"crawl_cursors", `DELETE FROM crawl_cursors WHERE scope_type = 'page_timeline' AND scope_id = $1`},
				// No schedule or job keeps the page's vanity, and none
				// crawls it: jobs name it by id; a schedule registered under
				// the vanity (its key) goes, with its registrations; one
				// under the id is disabled. They read the vanity before
				// "pages" clears it.
				{"jobs", `UPDATE jobs SET page_ref = 'id:' || $1::bigint::text WHERE page_ref <> 'id:' || $1::bigint::text
					AND (page_id = $1 OR page_ref = (SELECT 'v:' || lower(vanity) FROM pages WHERE page_id = $1))`},
				{"schedules_deleted", `DELETE FROM page_schedule WHERE page_ref LIKE 'v:%'
					AND (page_id = $1 OR page_ref = (SELECT 'v:' || lower(vanity) FROM pages WHERE page_id = $1))`},
				{"schedule", `UPDATE page_schedule SET enabled = false WHERE page_id = $1 OR page_ref = 'id:' || $1::bigint::text`},
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
		return nil, fmt.Errorf("%w: unknown subject type %q", errInvalid, r.subject)
	}
	ev.CompletedAt = timestamppb.Now()
	b := e.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.Erasure, r.id.String(), ev); err != nil {
		return nil, err
	}
	return stats, b.Wait(ctx)
}

// scrubMentions clears the id, profile URL and name of the mentions of a
// Facebook user in stored text ranges, as ScrubMentions does: in the posts
// the user commented on (found by author key, which the redacted comments
// keep), where replies tag the person they answer. A mention anywhere else
// is cleared when its comment is next scraped (the crawlers scrub erased
// authors' mentions). It runs outside the erasure lock.
func (e *Eraser) scrubMentions(ctx context.Context, fbID string, authorKey []byte) (int64, error) {
	const scrub = `SET text_ranges = (
			SELECT jsonb_agg(CASE WHEN x.r->>'entity_id' = $1 THEN x.r - 'entity_id' - 'url' - 'name' ELSE x.r END ORDER BY x.i)
			FROM jsonb_array_elements(text_ranges) WITH ORDINALITY AS x(r, i))`
	const mentioned = `text_ranges @> jsonb_build_array(jsonb_build_object('entity_id', $1::text))`
	var n int64
	for _, q := range []struct{ name, sql string }{
		{"comments", `UPDATE comments ` + scrub + ` WHERE post_id IN (SELECT DISTINCT post_id FROM comments WHERE author_key = $2) AND ` + mentioned},
		{"posts", `UPDATE posts ` + scrub + ` WHERE (post_created_at, post_id) IN (SELECT i.post_created_at, i.post_id FROM post_index i
			WHERE i.post_id IN (SELECT DISTINCT post_id FROM comments WHERE author_key = $2)) AND ` + mentioned},
	} {
		tag, err := e.DB.Exec(ctx, q.sql, fbID, authorKey)
		if err != nil {
			return n, fmt.Errorf("mentions in %s: %w", q.name, err)
		}
		n += tag.RowsAffected()
	}
	return n, nil
}

// deleteMedia removes the stored objects matching where, announces their
// deletion, and deletes the rows.
func (e *Eraser) deleteMedia(ctx context.Context, where string, args ...any) (int64, error) {
	rows, err := e.DB.Query(ctx, `SELECT media_id::text, s3_bucket, s3_key FROM media WHERE `+where+
		` AND s3_key IS NOT NULL AND status IN ('stored','ready')`, args...)
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
	// Objects first, then the events, then the rows: until the events are
	// out, the rows stay stored or ready, so a retried erasure finds them
	// again and sends the events it couldn't.
	b := e.Producer.NewBatch()
	for _, o := range objs {
		if e.S3 != nil {
			if err := e.S3.RemoveObject(ctx, o.bucket, o.key, minio.RemoveObjectOptions{}); err != nil {
				return 0, err
			}
		}
		if err := b.AddProto(ctx, topics.MediaDeleted, o.id, &fbv1.MediaDeleted{EventId: o.id + ":erasure", MediaId: o.id,
			Object: &fbv1.S3Object{Bucket: o.bucket, Key: o.key}, Reason: fbv1.DeleteReason_DELETE_REASON_ERASURE,
			DeletedAt: timestamppb.Now()}); err != nil {
			return 0, err
		}
	}
	if err := b.Wait(ctx); err != nil {
		return 0, err
	}
	metrics.MediaDeleted.WithLabelValues("erasure").Add(float64(len(objs)))
	// Every matching row goes, stored or not: the erased subject's media
	// keeps no record (suppression keeps it from being downloaded again).
	if _, err := e.DB.Exec(ctx, `DELETE FROM media WHERE `+where, args...); err != nil {
		return 0, err
	}
	return int64(len(objs)), nil
}
