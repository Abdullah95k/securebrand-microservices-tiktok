package persist

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// Writer performs batched upserts.
type Writer struct {
	DB *pgxpool.Pool
}

// latest keeps the most recently observed message per key.
func latest[T interface{ GetObs() *fbv1.Observation }](xs []T, key func(T) string) []T {
	idx := map[string]int{}
	out := make([]T, 0, len(xs))
	for _, x := range xs {
		k := key(x)
		if i, ok := idx[k]; ok {
			if observed(x.GetObs()).After(observed(out[i].GetObs())) {
				out[i] = x
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, x)
	}
	return out
}

// withPartitions retries fn once after creating any missing partitions.
func (w *Writer) withPartitions(ctx context.Context, table string, months []time.Time, fn func() error) error {
	err := fn()
	if !store.IsNoPartition(err) {
		return err
	}
	for _, m := range months {
		if perr := store.EnsurePartitionFor(ctx, w.DB, table, m); perr != nil {
			return errors.Join(err, perr)
		}
		if table == "posts" {
			if perr := store.EnsurePartitionFor(ctx, w.DB, "comments", m); perr != nil {
				return errors.Join(err, perr)
			}
		}
	}
	return fn()
}

func copyTemp(ctx context.Context, tx pgx.Tx, tmp, like string, cols []string, rows [][]any) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP", tmp, like)); err != nil {
		return err
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{tmp}, cols, pgx.CopyFromRows(rows))
	return err
}

// Posts upserts posts, maintains post_index and appends post_metrics rows
// when engagement changed.
func (w *Writer) Posts(ctx context.Context, posts []*fbv1.Post) error {
	posts = latest(posts, func(p *fbv1.Post) string { return strconv.FormatInt(p.PostId, 10) })
	if len(posts) == 0 {
		return nil
	}
	start := time.Now()
	rows := make([][]any, len(posts))
	var months []time.Time
	for i, p := range posts {
		rows[i] = postRow(p)
		months = append(months, p.CreatedAt.AsTime())
	}
	var written int64
	err := w.withPartitions(ctx, "posts", months, func() error {
		written = 0
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			if err := copyTemp(ctx, tx, "tmp_posts", "posts", postCols, rows); err != nil {
				return err
			}
			set := make([]string, 0, len(postCols))
			for _, c := range postCols {
				if c == "post_created_at" || c == "post_id" || c == "first_seen_at" {
					continue
				}
				set = append(set, c+" = EXCLUDED."+c)
			}
			q := `INSERT INTO posts AS t (` + strings.Join(postCols, ",") + `)
				SELECT ` + strings.Join(postCols, ",") + ` FROM tmp_posts
				ON CONFLICT (post_created_at, post_id) DO UPDATE SET ` + strings.Join(set, ", ") + `
				WHERE t.content_hash IS DISTINCT FROM EXCLUDED.content_hash
				   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
				RETURNING new.post_id, new.page_id, new.last_seen_at, new.reaction_count, new.reactions,
					new.comment_count_total, new.share_count, new.view_count,
					(old.post_id IS NULL
					 OR old.reaction_count IS DISTINCT FROM new.reaction_count
					 OR old.comment_count_total IS DISTINCT FROM new.comment_count_total
					 OR old.share_count IS DISTINCT FROM new.share_count
					 OR old.view_count IS DISTINCT FROM new.view_count) AS metrics_changed`
			r, err := tx.Query(ctx, q)
			if err != nil {
				return err
			}
			var metricRows [][]any
			for r.Next() {
				var (
					postID, pageID                         int64
					seen                                   time.Time
					reactionCount, comments, shares, views *int64
					reactions                              []byte
					changed                                bool
				)
				if err := r.Scan(&postID, &pageID, &seen, &reactionCount, &reactions, &comments, &shares, &views, &changed); err != nil {
					r.Close()
					return err
				}
				written++
				if changed {
					metricRows = append(metricRows, []any{postID, seen, pageID, reactionCount, reactions, comments, shares, views})
				}
			}
			r.Close()
			if err := r.Err(); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO post_index (post_id, page_id, post_created_at)
				SELECT post_id, page_id, post_created_at FROM tmp_posts ON CONFLICT (post_id) DO NOTHING`); err != nil {
				return err
			}
			if len(metricRows) > 0 {
				if err := copyMetrics(ctx, tx, metricRows); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("persist: posts: %w", err)
	}
	metrics.PersistRows.WithLabelValues("post", "written").Add(float64(written))
	metrics.PersistRows.WithLabelValues("post", "unchanged").Add(float64(int64(len(posts)) - written))
	metrics.PersistBatch.WithLabelValues("post").Observe(time.Since(start).Seconds())
	return nil
}

func copyMetrics(ctx context.Context, tx pgx.Tx, rows [][]any) error {
	cols := []string{"post_id", "captured_at", "page_id", "reaction_count", "reactions", "comment_count", "share_count", "view_count"}
	if _, err := tx.Exec(ctx, "CREATE TEMP TABLE tmp_metrics (LIKE post_metrics) ON COMMIT DROP"); err != nil {
		return err
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_metrics"}, cols, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO post_metrics SELECT * FROM tmp_metrics ON CONFLICT DO NOTHING`)
	if store.IsNoPartition(err) {
		// Metrics partitions are daily; a missing one is created by the
		// scheduler. Never fail the post batch for it.
		return nil
	}
	return err
}

// Comments upserts comments and their authors and applies deletions. An
// observation older than the stored one (a replayed batch) changes nothing;
// a newer one with other text counts as an edit, and one newer than the
// comment's deletion makes it visible again.
func (w *Writer) Comments(ctx context.Context, cs []*fbv1.Comment) error {
	cs = latest(cs, func(c *fbv1.Comment) string { return strconv.FormatInt(c.CommentId, 10) })
	if len(cs) == 0 {
		return nil
	}
	start := time.Now()
	rows := make([][]any, 0, len(cs))
	var dels []*fbv1.Comment
	var months []time.Time
	authors := map[string][]any{}
	for _, c := range cs {
		if c.PostCreatedAt == nil {
			continue
		}
		months = append(months, c.PostCreatedAt.AsTime())
		if c.Status == fbv1.CommentStatus_COMMENT_STATUS_DELETED {
			dels = append(dels, c)
			continue
		}
		rows = append(rows, commentRow(c))
		if a := c.Author; a != nil && len(a.AuthorKey) > 0 && !c.Redacted {
			seen := observed(c.Obs)
			authors[string(a.AuthorKey)] = []any{a.AuthorKey, nilIfEmpty(a.Id), nilIfEmpty(a.Name), nilIfEmpty(a.Url),
				nilIfEmpty(a.AvatarUrl), nilIfEmpty(a.Type), a.Verified, seen, seen}
		}
	}
	var written, edited, restored int64
	err := w.withPartitions(ctx, "comments", months, func() error {
		written, edited, restored = 0, 0, 0
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			if len(rows) > 0 {
				if err := upsertComments(ctx, tx, rows, &written, &edited, &restored); err != nil {
					return err
				}
			}
			if len(dels) > 0 {
				if _, err := markDeleted(ctx, tx, dels); err != nil {
					return err
				}
			}
			if len(authors) == 0 {
				return nil
			}
			arows := make([][]any, 0, len(authors))
			for _, r := range authors {
				arows = append(arows, r)
			}
			acols := []string{"author_key", "author_id", "name", "profile_url", "avatar_url", "author_type", "verified", "first_seen_at", "last_seen_at"}
			if err := copyTemp(ctx, tx, "tmp_authors", "comment_authors", acols, arows); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO comment_authors AS a SELECT * FROM tmp_authors
				ON CONFLICT (author_key) DO UPDATE SET author_id = EXCLUDED.author_id, name = EXCLUDED.name,
					profile_url = EXCLUDED.profile_url, avatar_url = EXCLUDED.avatar_url,
					author_type = EXCLUDED.author_type, verified = EXCLUDED.verified,
					last_seen_at = GREATEST(a.last_seen_at, EXCLUDED.last_seen_at)
				WHERE a.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
				   OR a.name IS DISTINCT FROM EXCLUDED.name OR a.author_id IS DISTINCT FROM EXCLUDED.author_id`)
			return err
		})
	})
	if err != nil {
		return fmt.Errorf("persist: comments: %w", err)
	}
	metrics.PersistRows.WithLabelValues("comment", "written").Add(float64(written))
	metrics.PersistRows.WithLabelValues("comment", "unchanged").Add(float64(int64(len(rows)) - written))
	metrics.CommentChanges.WithLabelValues("edited").Add(float64(edited))
	metrics.CommentChanges.WithLabelValues("restored").Add(float64(restored))
	metrics.PersistBatch.WithLabelValues("comment").Observe(time.Since(start).Seconds())
	return nil
}

const (
	// Other text is an edit (redaction is not).
	commentEdited = `(NOT EXCLUDED.redacted AND t.text IS DISTINCT FROM EXCLUDED.text)`
	// An observation newer than the deletion: the comment is visible again.
	commentRestored = `(t.status = 'deleted' AND EXCLUDED.last_seen_at > t.deleted_at)`
)

func upsertComments(ctx context.Context, tx pgx.Tx, rows [][]any, written, edited, restored *int64) error {
	if err := copyTemp(ctx, tx, "tmp_comments", "comments", commentCols, rows); err != nil {
		return err
	}
	set := make([]string, 0, len(commentCols)+4)
	for _, c := range commentCols {
		if c == "post_created_at" || c == "post_id" || c == "comment_id" || c == "first_seen_at" {
			continue
		}
		set = append(set, c+" = EXCLUDED."+c)
	}
	set = append(set,
		"edit_count = t.edit_count + CASE WHEN "+commentEdited+" THEN 1 ELSE 0 END",
		"edited_at = CASE WHEN "+commentEdited+" THEN EXCLUDED.last_seen_at ELSE t.edited_at END",
		"status = CASE WHEN "+commentRestored+" THEN 'visible' ELSE t.status END",
		"deleted_at = CASE WHEN "+commentRestored+" THEN NULL ELSE t.deleted_at END")
	r, err := tx.Query(ctx, `INSERT INTO comments AS t (`+strings.Join(commentCols, ",")+`)
		SELECT `+strings.Join(commentCols, ",")+` FROM tmp_comments
		ON CONFLICT (post_created_at, post_id, comment_id) DO UPDATE SET `+strings.Join(set, ", ")+`
		WHERE NOT t.redacted AND EXCLUDED.last_seen_at >= t.last_seen_at
		  AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash
		   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
		   OR `+commentRestored+`)
		RETURNING new.edit_count > coalesce(old.edit_count, 0),
			coalesce(old.status = 'deleted' AND new.status = 'visible', false),
			new.post_created_at, new.post_id, new.comment_id, new.depth, old.deleted_at`)
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
		if err := r.Scan(&e, &rs, &pc, &post, &id, &depth, &deletedAt); err != nil {
			r.Close()
			return err
		}
		*written++
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
		WHERE c.post_created_at = d.created AND c.post_id = d.post_id AND c.root_comment_id = d.root AND c.depth > 0
		  AND c.status = 'deleted' AND c.deleted_at = d.at`, created, posts, roots, at)
	if err != nil {
		return err
	}
	*restored += tag.RowsAffected()
	return nil
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// MarkDeleted sets visible comments to deleted, as of their observation
// time, unless they were seen after it. It returns how many it changed.
func MarkDeleted(ctx context.Context, db *pgxpool.Pool, cs []*fbv1.Comment) (int64, error) {
	return markDeleted(ctx, db, cs)
}

func markDeleted(ctx context.Context, db execer, cs []*fbv1.Comment) (int64, error) {
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

// Pages upserts page profiles, records follower snapshots and resolves
// scheduled page references to page ids.
func (w *Writer) Pages(ctx context.Context, ps []*fbv1.Page) error {
	ps = latest(ps, func(p *fbv1.Page) string { return strconv.FormatInt(p.PageId, 10) })
	start := time.Now()
	for _, p := range ps {
		seen := observed(p.Obs)
		err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			if p.Status == fbv1.PageStatus_PAGE_STATUS_UNAVAILABLE {
				_, err := tx.Exec(ctx, `INSERT INTO pages (page_id, status, last_scraped_at) VALUES ($1, 'unavailable', $2)
					ON CONFLICT (page_id) DO UPDATE SET status = 'unavailable', last_scraped_at = $2`, p.PageId, seen)
				return err
			}
			vanity := p.Vanity
			if vanity == "" && strings.HasPrefix(p.RequestedRef, "v:") {
				vanity = strings.TrimPrefix(p.RequestedRef, "v:")
			}
			var pic, cover *string
			if p.ProfilePicture != nil {
				pic = nilIfEmpty(p.ProfilePicture.Url)
			}
			if p.CoverPhoto != nil {
				cover = nilIfEmpty(p.CoverPhoto.Url)
			}
			var hash []byte
			if p.Obs != nil {
				hash = p.Obs.ContentHash
			}
			var changed bool
			err := tx.QueryRow(ctx, `
				INSERT INTO pages AS t (page_id, vanity, url, name, categories, intro, followers_count, likes_count, verified,
					profile_pic_url, cover_url, contact, extra, status, content_hash, first_seen_at, last_scraped_at)
				VALUES ($1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), $5, NULLIF($6,''), $7, $8, $9, $10, $11, $12, $13, 'active', $14, $15, $15)
				ON CONFLICT (page_id) DO UPDATE SET
					vanity = COALESCE(EXCLUDED.vanity, t.vanity), url = COALESCE(EXCLUDED.url, t.url),
					name = COALESCE(EXCLUDED.name, t.name), categories = EXCLUDED.categories, intro = EXCLUDED.intro,
					followers_count = EXCLUDED.followers_count, likes_count = EXCLUDED.likes_count,
					verified = EXCLUDED.verified, profile_pic_url = EXCLUDED.profile_pic_url, cover_url = EXCLUDED.cover_url,
					contact = EXCLUDED.contact, extra = EXCLUDED.extra, status = 'active',
					content_hash = EXCLUDED.content_hash, last_scraped_at = EXCLUDED.last_scraped_at
				RETURNING old.page_id IS NULL OR old.followers_count IS DISTINCT FROM new.followers_count
					OR old.likes_count IS DISTINCT FROM new.likes_count OR old.name IS DISTINCT FROM new.name`,
				p.PageId, vanity, p.Url, p.Name, p.Categories, p.Intro, p.FollowersCount, p.LikesCount, p.Verified,
				pic, cover, jsonOf(p.Contact), extraJSON(p.ExtraJson), hash, seen).Scan(&changed)
			if err != nil {
				return err
			}
			if changed {
				_, err := tx.Exec(ctx, `INSERT INTO page_snapshots (page_id, captured_at, name, followers_count, likes_count, verified)
					VALUES ($1, $2, NULLIF($3,''), $4, $5, $6) ON CONFLICT DO NOTHING`,
					p.PageId, seen, p.Name, p.FollowersCount, p.LikesCount, p.Verified)
				if err != nil && !store.IsNoPartition(err) {
					return err
				}
			}
			if p.RequestedRef != "" {
				if _, err := tx.Exec(ctx, `UPDATE page_schedule SET page_id = $1 WHERE page_ref = $2 AND page_id IS DISTINCT FROM $1`,
					p.PageId, p.RequestedRef); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE jobs SET page_id = $1 WHERE page_ref = $2 AND page_id IS NULL AND status IN ('running','finalizing')`,
					p.PageId, p.RequestedRef); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("persist: page %d: %w", p.PageId, err)
		}
	}
	metrics.PersistRows.WithLabelValues("page", "written").Add(float64(len(ps)))
	metrics.PersistBatch.WithLabelValues("page").Observe(time.Since(start).Seconds())
	return nil
}

// Media upserts media metadata without touching download state.
func (w *Writer) Media(ctx context.Context, ms []*fbv1.Media) error {
	ms = latest(ms, func(m *fbv1.Media) string { return m.MediaId })
	if len(ms) == 0 {
		return nil
	}
	start := time.Now()
	rows := make([][]any, len(ms))
	for i, m := range ms {
		rows[i] = mediaRow(m)
	}
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_media ON COMMIT DROP AS
			SELECT `+strings.Join(mediaCols, ",")+` FROM media WITH NO DATA`); err != nil {
			return err
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_media"}, mediaCols, pgx.CopyFromRows(rows)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO media AS t (`+strings.Join(mediaCols, ",")+`)
			SELECT `+strings.Join(mediaCols, ",")+` FROM tmp_media
			ON CONFLICT (media_id) DO UPDATE SET source_url = EXCLUDED.source_url,
				source_expires_at = EXCLUDED.source_expires_at, width = COALESCE(EXCLUDED.width, t.width),
				height = COALESCE(EXCLUDED.height, t.height), duration_ms = COALESCE(EXCLUDED.duration_ms, t.duration_ms),
				alt_text = COALESCE(EXCLUDED.alt_text, t.alt_text)
			WHERE t.source_url IS DISTINCT FROM EXCLUDED.source_url`)
		return err
	})
	if err != nil {
		return fmt.Errorf("persist: media: %w", err)
	}
	metrics.PersistRows.WithLabelValues("media", "written").Add(float64(len(ms)))
	metrics.PersistBatch.WithLabelValues("media").Observe(time.Since(start).Seconds())
	return nil
}
