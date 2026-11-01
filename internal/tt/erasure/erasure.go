// Package erasure carries out tks's erasure requests on schema tt (docs/tiktok
// ADR 0007 §6): an author (a person, by author key) or an account (by user
// id). It is internal/gdpr's Eraser for TikTok's tables, under the same
// erasure lock the writer takes (gdpr.GuardAccounts), so a record either is
// written before the erasure, which then removes or redacts it, or sees the
// suppression entry and is dropped.
//
// On TikTok an account and a person are the same user: the author key is
// the HMAC of the user id. So an account's erasure also erases it as an
// author (its comments on other accounts' posts are redacted, its mentions
// cleared), and both suppression entries stop it being collected again.
package erasure

import (
	"context"
	"encoding/base64"
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

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Subject types (erasure_requests.subject_type, suppression.subject_type).
const (
	SubjectAuthor  = "author"
	SubjectAccount = "account"
)

// Eraser processes pending erasure requests. DB must be a pool pinned to
// schema tt (ttstore.Open).
type Eraser struct {
	DB       *pgxpool.Pool
	S3       *minio.Client // nil when no media is stored (D5's switch off)
	Producer *kafka.Producer
	// HMACKey is privacy.author_hmac_key: an account's author key.
	HMACKey []byte
}

type request struct {
	id        uuid.UUID
	subject   string
	authorKey []byte
	userID    *int64
	created   time.Time
	attempts  int
}

// MaxAttempts is how many times a request is tried before it is marked
// failed (gdpr.MaxErasureAttempts: about 16 hours with the backoff).
const MaxAttempts = gdpr.MaxErasureAttempts

// errInvalid marks a request that no retry can complete.
var errInvalid = errors.New("invalid erasure request")

// retryAfter is the wait after the n-th failed attempt: 1m, 2m, 4m, ...
// up to 4h, as fbs's.
func retryAfter(n int) time.Duration {
	return min(time.Minute<<min(n-1, 8), 4*time.Hour)
}

// personal are the request inputs that name the subject; a request that is
// done keeps none of them (the user id of an account stays in user_id: its
// suppression entry needs it anyway).
var personal = []string{"username", "sec_uid", "account", "user_id"}

// Tick processes up to 10 pending requests that are due. Every step is
// idempotent, so a request that fails part-way is retried from the start.
func (e *Eraser) Tick(ctx context.Context) error {
	rows, err := e.DB.Query(ctx, `SELECT id, subject_type, author_key, user_id, created_at, attempts FROM erasure_requests
		WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY created_at LIMIT 10`)
	if err != nil {
		return err
	}
	reqs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (request, error) {
		var q request
		return q, r.Scan(&q.id, &q.subject, &q.authorKey, &q.userID, &q.created, &q.attempts)
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
			if errors.Is(err, errInvalid) || n >= MaxAttempts {
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
		if _, err := e.DB.Exec(ctx, `UPDATE erasure_requests SET status = 'done', stats = $2, completed_at = now(), error = NULL,
			input = input - $3::text[] WHERE id = $1`, r.id, sb, personal); err != nil {
			return err
		}
		slog.Info("erasure done", "request", r.id, "subject", r.subject, "stats", stats)
	}
	return nil
}

func (e *Eraser) process(ctx context.Context, r request) (map[string]int64, error) {
	stats := map[string]int64{}
	ev := &commonv1.ErasureEvent{EventId: r.id.String(), RequestId: r.id.String(), RequestedAt: timestamppb.New(r.created),
		Platform: commonv1.Platform_PLATFORM_TIKTOK}
	switch r.subject {
	case SubjectAuthor:
		if len(r.authorKey) == 0 {
			return nil, fmt.Errorf("%w: author erasure without an author key", errInvalid)
		}
		ev.SubjectType, ev.AuthorKey = commonv1.SubjectType_SUBJECT_TYPE_AUTHOR, r.authorKey
		if err := e.eraseAuthor(ctx, r.authorKey, 0, stats); err != nil {
			return nil, err
		}
	case SubjectAccount:
		if r.userID == nil || *r.userID <= 0 {
			return nil, fmt.Errorf("%w: account erasure without a user id", errInvalid)
		}
		key := emit.AuthorKey(e.HMACKey, *r.userID)
		ev.SubjectType, ev.AccountId, ev.AuthorKey = commonv1.SubjectType_SUBJECT_TYPE_ACCOUNT, *r.userID, key
		if err := e.eraseAuthor(ctx, key, *r.userID, stats); err != nil {
			return nil, err
		}
		if err := e.eraseAccount(ctx, *r.userID, stats); err != nil {
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

// eraseAuthor suppresses a person and redacts what is stored of them: their
// comments keep only their ids and the author key, their people row goes,
// their media goes, and their mentions keep only the author key. With
// account, the account's suppression entry is added in the same
// transaction, so writers drop the account's data from then on.
func (e *Eraser) eraseAuthor(ctx context.Context, key []byte, account int64, stats map[string]int64) error {
	var commentIDs []int64
	err := pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
		if err := gdpr.LockExclusive(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('author', $1) ON CONFLICT DO NOTHING`,
			hex.EncodeToString(key)); err != nil {
			return err
		}
		if account != 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('account', $1) ON CONFLICT DO NOTHING`,
				strconv.FormatInt(account, 10)); err != nil {
				return err
			}
		}
		rows, err := tx.Query(ctx, `UPDATE comments SET text = NULL, text_ranges = NULL, author_user_id = NULL, labels = NULL,
			image_media_ids = NULL, redacted = true WHERE author_key = $1 AND NOT redacted RETURNING comment_id`, key)
		if err != nil {
			return err
		}
		if commentIDs, err = pgx.CollectRows(rows, pgx.RowTo[int64]); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM people WHERE author_key = $1`, key)
		stats["people_deleted"] = tag.RowsAffected()
		return err
	})
	if err != nil {
		return err
	}
	stats["comments_redacted"] = int64(len(commentIDs))
	// By author key too: an avatar, or the image of a comment not stored yet.
	n, err := e.deleteMedia(ctx, `(author_key = $1 OR comment_id = ANY($2))`, key, commentIDs)
	if err != nil {
		return err
	}
	stats["media_deleted"] += n
	n, err = e.scrubMentions(ctx, key)
	if err != nil {
		return err
	}
	stats["mentions_scrubbed"] = n
	return nil
}

// scrubMentions clears what names an erased person in the stored text
// ranges that mention them (stored with protojson's field names): the
// mention keeps its place and the author key, nothing else. It covers the
// posts that mention them (post_mentions) and the comments of the posts
// they commented on, where replies tag the person they answer; a mention
// anywhere else is cleared when its comment is next written (the writer
// clears erased people's mentions), as in fbs. It runs outside the lock.
func (e *Eraser) scrubMentions(ctx context.Context, key []byte) (int64, error) {
	k := base64.StdEncoding.EncodeToString(key)
	const scrub = `SET text_ranges = (
			SELECT jsonb_agg(CASE WHEN x.r->'person'->>'author_key' = $1
				THEN (x.r - 'name') || jsonb_build_object('person', jsonb_build_object('author_key', $1::text)) ELSE x.r END ORDER BY x.i)
			FROM jsonb_array_elements(text_ranges) WITH ORDINALITY AS x(r, i))`
	// Only ranges that still name more than the key, so a retry counts
	// nothing twice.
	const named = `EXISTS (SELECT 1 FROM jsonb_array_elements(text_ranges) r WHERE r->'person'->>'author_key' = $1
			AND (r ? 'name' OR r->'person' <> jsonb_build_object('author_key', $1::text)))`
	var n int64
	for _, q := range []struct{ name, sql string }{
		{"comments", `UPDATE comments ` + scrub + ` WHERE (post_created_at, post_id) IN (SELECT DISTINCT post_created_at, post_id
			FROM comments WHERE author_key = $2) AND text_ranges IS NOT NULL AND ` + named},
		{"posts", `UPDATE posts ` + scrub + ` WHERE (created_at, post_id) IN (SELECT i.created_at, i.post_id FROM post_index i
			JOIN post_mentions m USING (post_id) WHERE m.author_key = $2) AND text_ranges IS NOT NULL AND ` + named},
	} {
		tag, err := e.DB.Exec(ctx, q.sql, k, key)
		if err != nil {
			return n, fmt.Errorf("mentions in %s: %w", q.name, err)
		}
		n += tag.RowsAffected()
	}
	// The mention rows themselves name the person by user id; the writer
	// never stores an erased person's (persist.Posts).
	if _, err := e.DB.Exec(ctx, `DELETE FROM post_mentions WHERE author_key = $1`, key); err != nil {
		return n, fmt.Errorf("post_mentions: %w", err)
	}
	return n, nil
}

// eraseAccount deletes what is stored of an account once its suppression
// entry is in (eraseAuthor): posts, metrics, comments, snapshots, the
// username history, media, the list's cursor. No schedule or job keeps its
// username or secUid: jobs name it by id; a schedule registered under a
// username or secUid goes, with its registrations; one under its id is
// disabled. The account row goes last.
func (e *Eraser) eraseAccount(ctx context.Context, uid int64, stats map[string]int64) error {
	n, err := e.deleteMedia(ctx, `(user_id = $1 OR post_id IN (SELECT post_id FROM post_index WHERE user_id = $1))`, uid)
	if err != nil {
		return err
	}
	stats["media_deleted"] += n
	return pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
		// The references that name the account, read before the rows that
		// hold its names go.
		var refs []string
		rows, err := tx.Query(ctx, `SELECT 'u:' || lower(username) FROM account_usernames WHERE user_id = $1
			UNION SELECT 'u:' || lower(username) FROM accounts WHERE user_id = $1 AND username IS NOT NULL
			UNION SELECT 'sec:' || sec_uid FROM accounts WHERE user_id = $1
			UNION SELECT account_ref FROM account_schedule WHERE user_id = $1
			UNION SELECT account_ref FROM jobs WHERE user_id = $1`, uid)
		if err != nil {
			return err
		}
		if refs, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
			return err
		}
		idRef := "id:" + strconv.FormatInt(uid, 10)
		for _, q := range []struct {
			name, sql string
			args      []any
		}{
			// By user id: every row, including comments of a post that was
			// never stored. A scan, but an account erasure is rare and holds
			// no lock writers wait on.
			{"comments", `DELETE FROM comments WHERE user_id = $1`, []any{uid}},
			{"post_metrics", `DELETE FROM post_metrics WHERE user_id = $1`, []any{uid}},
			{"post_mentions", `DELETE FROM post_mentions WHERE post_id IN (SELECT post_id FROM post_index WHERE user_id = $1)`, []any{uid}},
			{"posts", `DELETE FROM posts WHERE user_id = $1`, []any{uid}},
			{"post_index", `DELETE FROM post_index WHERE user_id = $1`, []any{uid}},
			{"post_refresh", `DELETE FROM post_refresh WHERE user_id = $1`, []any{uid}},
			{"account_snapshots", `DELETE FROM account_snapshots WHERE user_id = $1`, []any{uid}},
			{"account_usernames", `DELETE FROM account_usernames WHERE user_id = $1`, []any{uid}},
			{"crawl_cursors", `DELETE FROM crawl_cursors WHERE scope_type = 'account_list' AND scope_id = $1`, []any{uid}},
			{"jobs", `UPDATE jobs SET account_ref = $2, user_id = $1, params = params - 'account'
				WHERE (user_id = $1 OR account_ref = ANY($3)) AND (account_ref <> $2 OR params ? 'account')`, []any{uid, idRef, refs}},
			{"schedules_deleted", `DELETE FROM account_schedule WHERE account_ref <> $2 AND (user_id = $1 OR account_ref = ANY($3))`,
				[]any{uid, idRef, refs}},
			{"schedule", `UPDATE account_schedule SET enabled = false, user_id = $1 WHERE account_ref = $2 AND enabled`, []any{uid, idRef}},
			{"accounts", `DELETE FROM accounts WHERE user_id = $1`, []any{uid}},
		} {
			tag, err := tx.Exec(ctx, q.sql, q.args...)
			if err != nil {
				return fmt.Errorf("%s: %w", q.name, err)
			}
			stats[q.name] = tag.RowsAffected()
		}
		return nil
	})
}

// deleteMedia removes the stored objects matching where, announces their
// deletion on tt.media.deleted.v1, and deletes the rows (fbs's order:
// objects, then events, then rows, so a retry finds what it did not
// finish).
func (e *Eraser) deleteMedia(ctx context.Context, where string, args ...any) (int64, error) {
	rows, err := e.DB.Query(ctx, `SELECT media_id::text, s3_bucket, s3_key FROM media WHERE `+where+
		` AND s3_key IS NOT NULL AND status IN ('stored','ready')`, args...)
	if err != nil {
		return 0, err
	}
	type obj struct{ id, bucket, key string }
	objs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (obj, error) {
		var o obj
		var bucket *string
		err := r.Scan(&o.id, &bucket, &o.key)
		if bucket != nil {
			o.bucket = *bucket
		}
		return o, err
	})
	if err != nil {
		return 0, err
	}
	if len(objs) > 0 && e.S3 == nil {
		// Deleting the rows would orphan the objects: retried until the
		// eraser runs with S3 again.
		return 0, fmt.Errorf("%d stored media objects and no S3 client (tt.media.include is off)", len(objs))
	}
	b := e.Producer.NewBatch()
	for _, o := range objs {
		if o.bucket != "" {
			if err := e.S3.RemoveObject(ctx, o.bucket, o.key, minio.RemoveObjectOptions{}); err != nil {
				return 0, err
			}
		}
		if err := b.AddProto(ctx, topics.MediaDeleted, o.id, &commonv1.MediaDeleted{EventId: o.id + ":erasure", MediaId: o.id,
			Object: &commonv1.S3Object{Bucket: o.bucket, Key: o.key}, Reason: commonv1.DeleteReason_DELETE_REASON_ERASURE,
			DeletedAt: timestamppb.Now(), Platform: commonv1.Platform_PLATFORM_TIKTOK}); err != nil {
			return 0, err
		}
	}
	if err := b.Wait(ctx); err != nil {
		return 0, err
	}
	metrics.MediaDeleted.WithLabelValues("erasure").Add(float64(len(objs)))
	// Every matching row goes, stored or not: suppression keeps the media
	// from being downloaded again.
	if _, err := e.DB.Exec(ctx, `DELETE FROM media WHERE `+where, args...); err != nil {
		return 0, err
	}
	return int64(len(objs)), nil
}
