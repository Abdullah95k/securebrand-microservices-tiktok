package persist

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

var commentCols = []string{"post_created_at", "post_id", "comment_id", "user_id", "parent_comment_id",
	"reply_to_comment_id", "depth", "author_key", "author_user_id", "text", "text_ranges", "created_at", "like_count",
	"reply_count", "liked_by_creator", "pinned_by_creator", "stick_position", "has_images", "image_media_ids", "labels",
	"language", "redacted", "content_hash", "first_seen_at", "last_seen_at"}

var peopleCols = []string{"author_key", "user_id", "sec_uid", "username", "nickname", "avatar_url", "verified",
	"predicted_age_group", "first_seen_at", "last_seen_at"}

func commentRow(c *ttv1.Comment) []any {
	seen := observed(c.Obs)
	var authorKey []byte
	var authorID *int64
	if a := c.Author; a != nil {
		if len(a.AuthorKey) > 0 {
			authorKey = a.AuthorKey
		}
		if !c.Redacted {
			authorID = a.UserId
		}
	}
	var images []string
	for _, id := range c.ImageMediaIds {
		if mediaUUID(id) != nil {
			images = append(images, id)
		}
	}
	var created *time.Time
	if c.CreatedAt != nil {
		v := c.CreatedAt.AsTime()
		created = &v
	}
	var stick *int32
	if c.StickPosition != 0 {
		stick = &c.StickPosition
	}
	return []any{c.PostCreatedAt.AsTime(), c.PostId, c.CommentId, c.UserId, c.ParentCommentId, c.ReplyToCommentId,
		int16(c.Depth), authorKey, authorID, nilIfEmpty(c.Text), rangesJSON(c.TextRanges), created, c.LikeCount,
		c.ReplyCount, c.LikedByCreator, c.PinnedByCreator, stick, len(c.ImageMediaIds) > 0, images, c.Labels,
		nilIfEmpty(c.Language), c.Redacted, hashOf(c.Obs), seen, seen}
}

// Comments upserts comments and replies with their authors, and applies
// deletions. An observation older than the stored one changes nothing;
// a newer one with other text counts as an edit, and one newer than the
// comment's deletion makes it (and the replies deleted with it) visible
// again.
// RecentPostAge splits tks_comments_new_total: a comment first stored
// while its post is at most this old is "recent" (the comment windows reach
// 30 days), an older post's is "older" (a backfill's, or a re-read the
// weekly walk started).
const RecentPostAge = 30 * 24 * time.Hour

func (w *Writer) Comments(ctx context.Context, cs []*ttv1.Comment) error {
	cs = latest(cs, func(c *ttv1.Comment) string { return strconv.FormatInt(c.CommentId, 10) })
	kept := cs[:0]
	var cutoff time.Time
	if w.CommentsRetention > 0 {
		cutoff = time.Now().Add(-w.CommentsRetention)
	}
	for _, c := range cs {
		switch {
		case c.CommentId == 0 || c.PostId == 0 || c.PostCreatedAt == nil:
			metrics.PersistRows.WithLabelValues("comment", "rejected").Inc()
		case !cutoff.IsZero() && c.PostCreatedAt.AsTime().Before(cutoff):
			metrics.PersistRows.WithLabelValues("comment", "past_retention").Inc()
		default:
			kept = append(kept, c)
		}
	}
	if cs = kept; len(cs) == 0 {
		return nil
	}
	start := time.Now()
	times := make([]time.Time, 0, len(cs))
	for _, c := range cs {
		times = append(times, c.PostCreatedAt.AsTime())
	}
	var n, edited, restored, upserted int64
	got := sightingCounts{}
	var stored map[int64]bool // the last attempt's first-time comments
	err := w.withPartitions(ctx, "comments", times, func() error {
		n, edited, restored, upserted = 0, 0, 0, 0
		clear(got)
		fresh := map[int64]bool{} // comments written for the first time
		stored = fresh
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			rows := make([][]any, 0, len(cs))
			var dels []*ttv1.Comment
			people := map[string][]any{}
			for _, c := range cs {
				if c.Status == ttv1.CommentStatus_COMMENT_STATUS_DELETED {
					dels = append(dels, c)
					continue
				}
				rows = append(rows, commentRow(c))
				if a := c.Author; a != nil && len(a.AuthorKey) > 0 && !c.Redacted {
					seen := observed(c.Obs)
					if prev, ok := people[string(a.AuthorKey)]; ok && prev[9].(time.Time).After(seen) {
						continue
					}
					people[string(a.AuthorKey)] = []any{a.AuthorKey, a.UserId, nilIfEmpty(a.SecUid), nilIfEmpty(a.Username),
						nilIfEmpty(a.Nickname), nilIfEmpty(a.AvatarUrl), a.Verified, nilIfEmpty(a.PredictedAgeGroup), seen, seen}
				}
			}
			upserted = int64(len(rows))
			if len(rows) > 0 {
				if err := upsertComments(ctx, tx, rows, &n, &edited, &restored, fresh); err != nil {
					return err
				}
			}
			if len(dels) > 0 {
				if _, err := markDeleted(ctx, tx, dels); err != nil {
					return err
				}
			}
			if err := writePeople(ctx, tx, people); err != nil {
				return err
			}
			if !w.partB() || len(fresh) == 0 {
				return nil
			}
			direct, replies := w.commentSightings(cs, fresh)
			if err := recordSightings(ctx, tx, direct, got); err != nil {
				return err
			}
			return recordReplies(ctx, tx, replies, got)
		})
	})
	if err != nil {
		return fmt.Errorf("persist: comments: %w", err)
	}
	written("comment", n, start)
	got.publish()
	metrics.PersistRows.WithLabelValues("comment", "unchanged").Add(float64(upserted - n))
	recent, older := 0, 0
	for _, c := range cs {
		if !stored[c.CommentId] {
			continue
		}
		if start.Sub(c.PostCreatedAt.AsTime()) > RecentPostAge {
			older++
		} else {
			recent++
		}
	}
	metrics.CommentsNew.WithLabelValues("recent").Add(float64(recent))
	metrics.CommentsNew.WithLabelValues("older").Add(float64(older))
	metrics.CommentChanges.WithLabelValues("edited").Add(float64(edited))
	metrics.CommentChanges.WithLabelValues("restored").Add(float64(restored))
	return nil
}

// writePeople upserts comment authors.
func writePeople(ctx context.Context, tx pgx.Tx, people map[string][]any) error {
	if len(people) == 0 {
		return nil
	}
	prows := make([][]any, 0, len(people))
	for _, r := range people {
		prows = append(prows, r)
	}
	if err := copyTemp(ctx, tx, "tmp_people", "people", peopleCols, prows); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO people AS p (`+strings.Join(peopleCols, ",")+`)
		SELECT `+strings.Join(peopleCols, ",")+` FROM tmp_people ORDER BY author_key
		ON CONFLICT (author_key) DO UPDATE SET `+set(peopleCols, map[string]string{"author_key": "", "first_seen_at": "",
		"last_seen_at": "GREATEST(p.last_seen_at, EXCLUDED.last_seen_at)",
		// The last band TikTok served stays when a read carries none.
		"predicted_age_group": "COALESCE(EXCLUDED.predicted_age_group, p.predicted_age_group)"})+`
		WHERE p.last_seen_at <= EXCLUDED.last_seen_at
		  AND (p.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
		   OR (p.user_id, p.sec_uid, p.username, p.nickname, p.avatar_url, p.verified, p.predicted_age_group)
		      IS DISTINCT FROM (EXCLUDED.user_id, EXCLUDED.sec_uid, EXCLUDED.username, EXCLUDED.nickname,
		                        EXCLUDED.avatar_url, EXCLUDED.verified,
		                        COALESCE(EXCLUDED.predicted_age_group, p.predicted_age_group)))`)
	return err
}

const (
	// Other text is an edit (redaction is not).
	commentEdited = `(NOT EXCLUDED.redacted AND t.text IS DISTINCT FROM EXCLUDED.text)`
	// An observation newer than the deletion: the comment is visible again.
	commentRestored = `(t.status = 'deleted' AND EXCLUDED.last_seen_at > t.deleted_at)`
)

// upsertComments writes comments, counting what it wrote, edited and
// restored, and marks in fresh the comments it inserted.
func upsertComments(ctx context.Context, tx pgx.Tx, rows [][]any, written, edited, restored *int64, fresh map[int64]bool) error {
	if err := copyTemp(ctx, tx, "tmp_comments", "comments", commentCols, rows); err != nil {
		return err
	}
	r, err := tx.Query(ctx, `INSERT INTO comments AS t (`+strings.Join(commentCols, ",")+`)
		SELECT `+strings.Join(commentCols, ",")+` FROM tmp_comments ORDER BY post_created_at, post_id, comment_id
		ON CONFLICT (post_created_at, post_id, comment_id) DO UPDATE SET `+set(commentCols,
		map[string]string{"post_created_at": "", "post_id": "", "comment_id": "", "first_seen_at": ""},
		"edit_count = t.edit_count + CASE WHEN "+commentEdited+" THEN 1 ELSE 0 END",
		"edited_at = CASE WHEN "+commentEdited+" THEN EXCLUDED.last_seen_at ELSE t.edited_at END",
		"status = CASE WHEN "+commentRestored+" THEN 'visible' ELSE t.status END",
		"deleted_at = CASE WHEN "+commentRestored+" THEN NULL ELSE t.deleted_at END")+`
		WHERE NOT t.redacted AND EXCLUDED.last_seen_at >= t.last_seen_at
		  AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash
		   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
		   OR `+commentRestored+`)
		RETURNING new.edit_count > coalesce(old.edit_count, 0),
			coalesce(old.status = 'deleted' AND new.status = 'visible', false),
			new.post_created_at, new.post_id, new.comment_id, new.depth, old.deleted_at, old.comment_id IS NULL`)
	if err != nil {
		return err
	}
	var (
		created []time.Time
		posts   []int64
		roots   []int64
		at      []time.Time
	)
	for r.Next() {
		var e, rs bool
		var pc time.Time
		var post, id int64
		var depth int16
		var deletedAt *time.Time
		var inserted bool
		if err := r.Scan(&e, &rs, &pc, &post, &id, &depth, &deletedAt, &inserted); err != nil {
			r.Close()
			return err
		}
		*written++
		if inserted {
			fresh[id] = true
		}
		if e {
			*edited++
		}
		if rs {
			*restored++
			if depth == 0 && deletedAt != nil {
				created, posts, roots, at = append(created, pc), append(posts, post), append(roots, id), append(at, *deletedAt)
			}
		}
	}
	r.Close()
	if err := r.Err(); err != nil || len(roots) == 0 {
		return err
	}
	// The replies deleted with a comment that is visible again were only
	// deleted because of it.
	tag, err := tx.Exec(ctx, `UPDATE comments c SET status = 'visible', deleted_at = NULL
		FROM unnest($1::timestamptz[], $2::bigint[], $3::bigint[], $4::timestamptz[]) AS d(created, post_id, root, at)
		WHERE c.post_created_at = d.created AND c.post_id = d.post_id AND c.parent_comment_id = d.root AND c.depth > 0
		  AND c.status = 'deleted' AND c.deleted_at = d.at`, created, posts, roots, at)
	if err != nil {
		return err
	}
	*restored += tag.RowsAffected()
	return nil
}

// MarkDeleted sets visible comments to deleted, as of their observation
// time, unless they were seen after it, and returns how many it changed.
// The finalizer calls it for the comments a whole read no longer returned
// (ADR 0007 §5).
func MarkDeleted(ctx context.Context, db *pgxpool.Pool, cs []*ttv1.Comment) (int64, error) {
	return markDeleted(ctx, db, cs)
}

func markDeleted(ctx context.Context, db execer, cs []*ttv1.Comment) (int64, error) {
	created := make([]time.Time, 0, len(cs))
	posts := make([]int64, 0, len(cs))
	ids := make([]int64, 0, len(cs))
	at := make([]time.Time, 0, len(cs))
	for _, c := range cs {
		if c.PostCreatedAt == nil {
			continue
		}
		created = append(created, c.PostCreatedAt.AsTime())
		posts = append(posts, c.PostId)
		ids = append(ids, c.CommentId)
		at = append(at, observed(c.Obs))
	}
	tag, err := db.Exec(ctx, `UPDATE comments c SET status = 'deleted', deleted_at = d.at
		FROM unnest($1::timestamptz[], $2::bigint[], $3::bigint[], $4::timestamptz[]) AS d(created, post_id, comment_id, at)
		WHERE c.post_created_at = d.created AND c.post_id = d.post_id AND c.comment_id = d.comment_id
		  AND c.status = 'visible' AND NOT c.redacted AND c.last_seen_at < d.at`, created, posts, ids, at)
	if err != nil {
		return 0, err
	}
	metrics.CommentChanges.WithLabelValues("deleted").Add(float64(tag.RowsAffected()))
	return tag.RowsAffected(), nil
}
