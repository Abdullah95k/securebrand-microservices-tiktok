package persist

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

var postCols = []string{"created_at", "post_id", "user_id", "kind", "caption", "text_ranges", "hashtags", "hashtag_ids",
	"duration_ms", "width", "height", "cover_media_id", "photo_count", "music_id", "is_pinned", "is_ad", "labels",
	"category", "playlist_id", "comments_disabled", "region", "text_language", "discovered_via",
	"play_count", "like_count", "comment_count", "share_count", "save_count", "repost_count",
	"content_hash", "status", "first_seen_at", "last_seen_at", "stored_at", "first_mode", "title"}

// mention is one person a post or comment mentions.
type mention struct {
	key     []byte
	userID  *int64
	offsets []int32
}

// mentions groups a text's mentions by person.
func mentions(rs []*ttv1.TextRange) []*mention {
	var out []*mention
	by := map[string]*mention{}
	for _, r := range rs {
		p := r.GetPerson()
		if r.Type != "mention" || len(p.GetAuthorKey()) == 0 {
			continue
		}
		m, ok := by[string(p.AuthorKey)]
		if !ok {
			m = &mention{key: p.AuthorKey, userID: p.UserId}
			by[string(p.AuthorKey)] = m
			out = append(out, m)
		}
		m.offsets = append(m.offsets, r.Offset)
	}
	return out
}

// modeName is a crawl mode as first_mode stores it (full, incremental, ...).
func modeName(o *commonv1.Observation) *string {
	if o.GetMode() == commonv1.CrawlMode_CRAWL_MODE_UNSPECIFIED {
		return nil
	}
	n := enumName(o.GetMode(), "CRAWL_MODE_")
	return &n
}

// countsDelay reports whether a new post's delay counts for build brief §8
// A2: found by a scheduled check or a metrics walk standing in for one, not
// by a backfill (whose posts are not new).
func countsDelay(o *commonv1.Observation) bool {
	switch o.GetMode() {
	case commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, commonv1.CrawlMode_CRAWL_MODE_REFRESH:
		return true
	}
	return false
}

func postRow(p *ttv1.Post, storedAt time.Time) []any {
	seen := observed(p.Obs)
	var tags []string
	var tagIDs []int64
	for _, h := range p.Hashtags {
		tags = append(tags, strings.ToLower(h.Name))
		tagIDs = append(tagIDs, h.Id)
	}
	var photos *int32
	if n := int32(len(p.Photos)); n > 0 || p.Kind == ttv1.PostKind_POST_KIND_PHOTO {
		photos = &n
	}
	var labels []byte
	if p.Labels != nil {
		labels = protoJSON(p.Labels)
	}
	c := p.GetCounts()
	status := "visible"
	if p.Status != ttv1.PostStatus_POST_STATUS_UNSPECIFIED {
		status = enumName(p.Status, "POST_STATUS_")
	}
	via := "list"
	if p.DiscoveredVia != ttv1.DiscoveredVia_DISCOVERED_VIA_UNSPECIFIED {
		via = enumName(p.DiscoveredVia, "DISCOVERED_VIA_")
	}
	return []any{p.CreatedAt.AsTime(), p.PostId, p.UserId, enumName(p.Kind, "POST_KIND_"), nilIfEmpty(p.Caption),
		rangesJSON(p.TextRanges), tags, tagIDs, nilIfZero(p.DurationMs), nilIfZero32(p.Width), nilIfZero32(p.Height),
		mediaUUID(p.CoverMediaId), photos, p.MusicId, p.Pinned, p.Ad, labels, p.Category, nilIfEmpty(p.PlaylistId),
		p.CommentsDisabled, nilIfEmpty(p.Region), nilIfEmpty(p.TextLanguage), via,
		c.Plays, c.Likes, c.Comments, c.Shares, c.Saves, c.Reposts, hashOf(p.Obs), status, seen, seen, storedAt, modeName(p.Obs),
		nilIfEmpty(p.Title)}
}

// unavailableKept is true, in an upsert of posts, when the stored post was
// marked unavailable after the read being written was observed: the read
// does not bring it back.
const unavailableKept = "(t.status = 'unavailable' AND t.unavailable_since > EXCLUDED.last_seen_at)"

// Posts upserts posts, their index rows and the people their captions
// mention, and applies status-only records (a post no longer public:
// markUnavailable). The pinned and ad flags, known only from lists, are kept
// when a read does not carry them. A full read observed after a post was
// marked unavailable makes it visible again; every status change is kept in
// post_status_changes (docs/tiktok/discovery-plan.md §17).
func (w *Writer) Posts(ctx context.Context, ps []*ttv1.Post) error {
	ps = latest(ps, func(p *ttv1.Post) string { return strconv.FormatInt(p.PostId, 10) })
	kept := ps[:0]
	var marks []*ttv1.Post
	for _, p := range ps {
		switch {
		case p.PostId == 0 || p.UserId == 0 || p.CreatedAt == nil:
			metrics.PersistRows.WithLabelValues("post", "rejected").Inc()
		case p.Status == ttv1.PostStatus_POST_STATUS_UNAVAILABLE:
			marks = append(marks, p)
		default:
			kept = append(kept, p)
		}
	}
	if len(marks) > 0 {
		if err := w.markUnavailable(ctx, marks); err != nil {
			return err
		}
	}
	if ps = kept; len(ps) == 0 {
		return nil
	}
	start := time.Now()
	times := make([]time.Time, len(ps))
	for i, p := range ps {
		times[i] = p.CreatedAt.AsTime()
	}
	var n int64
	var now time.Time
	var fresh []int64 // posts written for the first time
	got := sightingCounts{}
	err := w.withPartitions(ctx, "posts", times, func() error {
		n, now, fresh = 0, w.now(), nil
		clear(got)
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			rows := make([][]any, 0, len(ps))
			var mrows [][]any
			for _, p := range ps {
				rows = append(rows, postRow(p, now))
				for _, m := range mentions(p.TextRanges) {
					mrows = append(mrows, []any{p.PostId, m.key, m.userID, m.offsets, observed(p.Obs)})
				}
			}
			if err := copyTemp(ctx, tx, "tmp_posts", "posts", postCols, rows); err != nil {
				return err
			}
			// The status changes this batch makes, before it makes them.
			changed, err := tx.Query(ctx, `INSERT INTO post_status_changes AS h (post_id, changed_at, status, previous)
				SELECT n.post_id, n.last_seen_at, n.status, t.status
				FROM tmp_posts n JOIN posts t ON t.created_at = n.created_at AND t.post_id = n.post_id
				WHERE t.status IS DISTINCT FROM n.status AND t.last_seen_at <= n.last_seen_at
				  AND NOT `+strings.ReplaceAll(unavailableKept, "EXCLUDED.", "n.")+`
				ON CONFLICT DO NOTHING RETURNING h.status`)
			if err != nil {
				return err
			}
			statuses, err := pgx.CollectRows(changed, pgx.RowTo[string])
			if err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `INSERT INTO posts AS t (`+strings.Join(postCols, ",")+`)
				SELECT `+strings.Join(postCols, ",")+` FROM tmp_posts
				ON CONFLICT (created_at, post_id) DO UPDATE SET `+set(postCols, map[string]string{
				"created_at": "", "post_id": "", "first_seen_at": "", "discovered_via": "", "stored_at": "", "first_mode": "",
				"is_pinned": "COALESCE(EXCLUDED.is_pinned, t.is_pinned)", "is_ad": "COALESCE(EXCLUDED.is_ad, t.is_ad)",
				"status": "CASE WHEN " + unavailableKept + " THEN t.status ELSE EXCLUDED.status END"},
				"unavailable_since = CASE WHEN "+unavailableKept+" THEN t.unavailable_since END",
				"status_code = CASE WHEN "+unavailableKept+" THEN t.status_code END",
				"status_region = CASE WHEN "+unavailableKept+" THEN t.status_region END")+`
				WHERE t.last_seen_at <= EXCLUDED.last_seen_at
				  AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash
				   OR t.status IS DISTINCT FROM EXCLUDED.status
				   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day')`)
			if err != nil {
				return err
			}
			n = tag.RowsAffected()
			for _, st := range statuses {
				metrics.PostStatusChanges.WithLabelValues(st).Inc()
			}
			// post_index is written with the post, so a post new to it is new.
			idx, err := tx.Query(ctx, `INSERT INTO post_index (post_id, user_id, created_at, kind)
				SELECT post_id, user_id, created_at, kind FROM tmp_posts ON CONFLICT (post_id) DO NOTHING RETURNING post_id`)
			if err != nil {
				return err
			}
			if fresh, err = pgx.CollectRows(idx, pgx.RowTo[int64]); err != nil {
				return err
			}
			if err := recordSightings(ctx, tx, w.postSightings(ps, fresh), got); err != nil {
				return err
			}
			if len(mrows) == 0 {
				return nil
			}
			if err := copyTemp(ctx, tx, "tmp_mentions", "post_mentions",
				[]string{"post_id", "author_key", "user_id", "offsets", "first_seen_at"}, mrows); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO post_mentions AS t (post_id, author_key, user_id, offsets, first_seen_at)
				SELECT post_id, author_key, user_id, offsets, first_seen_at FROM tmp_mentions
				ON CONFLICT (post_id, author_key) DO UPDATE SET user_id = EXCLUDED.user_id, offsets = EXCLUDED.offsets
				WHERE t.offsets IS DISTINCT FROM EXCLUDED.offsets OR t.user_id IS DISTINCT FROM EXCLUDED.user_id`)
			return err
		})
	})
	if err != nil {
		return fmt.Errorf("persist: posts: %w", err)
	}
	written("post", n, start)
	got.publish()
	metrics.PersistRows.WithLabelValues("post", "unchanged").Add(float64(int64(len(ps)) - n))
	observeDelay(ps, fresh, now)
	return nil
}

// markUnavailable applies status-only records: a visible post that a list
// read covering its date no longer returned, and whose page confirmed it, is
// marked unavailable with the page's statusCode and the exit's region. Its
// content stays (D8). A post read after the mark was observed, or already
// not visible, is left as it is.
func (w *Writer) markUnavailable(ctx context.Context, ps []*ttv1.Post) error {
	created := make([]time.Time, 0, len(ps))
	ids := make([]int64, 0, len(ps))
	users := make([]int64, 0, len(ps))
	at := make([]time.Time, 0, len(ps))
	codes := make([]*int64, 0, len(ps))
	regions := make([]*string, 0, len(ps))
	for _, p := range ps {
		created = append(created, p.CreatedAt.AsTime())
		ids = append(ids, p.PostId)
		users = append(users, p.UserId)
		at = append(at, observed(p.Obs))
		codes = append(codes, p.StatusCode)
		regions = append(regions, nilIfEmpty(p.StatusRegion))
	}
	start := time.Now()
	var n int64
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		// The subquery reads the rows as they were before the update, so
		// the history gets their previous status.
		tag, err := tx.Exec(ctx, `WITH m AS (
				SELECT t.created_at, t.post_id, t.status AS previous, d.at, d.code, d.region
				FROM unnest($1::timestamptz[], $2::bigint[], $3::bigint[], $4::timestamptz[], $5::bigint[], $6::text[])
					AS d(created, post_id, user_id, at, code, region)
				JOIN posts t ON t.created_at = d.created AND t.post_id = d.post_id AND t.user_id = d.user_id
				WHERE t.status = 'visible' AND t.last_seen_at < d.at),
			u AS (UPDATE posts t SET status = 'unavailable', unavailable_since = m.at, status_code = m.code,
					status_region = m.region
				FROM m WHERE t.created_at = m.created_at AND t.post_id = m.post_id
				RETURNING t.post_id)
			INSERT INTO post_status_changes (post_id, changed_at, status, previous, status_code, region)
			SELECT m.post_id, m.at, 'unavailable', m.previous, m.code, m.region FROM m JOIN u USING (post_id)
			ON CONFLICT DO NOTHING`, created, ids, users, at, codes, regions)
		n = tag.RowsAffected()
		return err
	})
	if err != nil {
		return fmt.Errorf("persist: post statuses: %w", err)
	}
	written("post_status", n, start)
	metrics.PersistRows.WithLabelValues("post_status", "unchanged").Add(float64(int64(len(ps)) - n))
	metrics.PostStatusChanges.WithLabelValues("unavailable").Add(float64(n))
	return nil
}

// observeDelay records, once committed, how long each new post took from its
// createTime to its row (build brief §8 A2), for the posts a scheduled check
// or a metrics walk found.
func observeDelay(ps []*ttv1.Post, fresh []int64, storedAt time.Time) {
	if len(fresh) == 0 {
		return
	}
	isNew := make(map[int64]bool, len(fresh))
	for _, id := range fresh {
		isNew[id] = true
	}
	for _, p := range ps {
		if !isNew[p.PostId] || !countsDelay(p.Obs) {
			continue
		}
		d := max(storedAt.Sub(p.CreatedAt.AsTime()), 0)
		metrics.PostStoreDelay.WithLabelValues(*modeName(p.Obs)).Observe(d.Seconds())
	}
}

// PostMetrics appends a post's counts to post_metrics only when they
// differ from the post's previous row (ADR 0007 §4: written on change). A
// batch keeps the latest capture per post. A capture whose daily partition
// does not exist is skipped (the scheduler creates them ahead).
func (w *Writer) PostMetrics(ctx context.Context, ms []*ttv1.PostMetrics) error {
	ms = latest(ms, func(m *ttv1.PostMetrics) string { return strconv.FormatInt(m.PostId, 10) })
	kept := ms[:0]
	for _, m := range ms {
		if m.PostId != 0 && m.CapturedAt != nil && m.Counts != nil {
			kept = append(kept, m)
		}
	}
	if ms = kept; len(ms) == 0 {
		return nil
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].CapturedAt.AsTime().Before(ms[j].CapturedAt.AsTime()) })
	start := time.Now()
	var n int64
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		cols := []string{"post_id", "captured_at", "user_id", "play_count", "like_count", "comment_count", "share_count",
			"save_count", "repost_count"}
		rows := make([][]any, 0, len(ms))
		for _, m := range ms {
			c := m.Counts
			rows = append(rows, []any{m.PostId, m.CapturedAt.AsTime(), m.UserId, c.Plays, c.Likes, c.Comments, c.Shares, c.Saves, c.Reposts})
		}
		if err := copyTemp(ctx, tx, "tmp_metrics", "post_metrics", cols, rows); err != nil {
			return err
		}
		var err error
		n, err = optionalInsert(ctx, tx, `INSERT INTO post_metrics (`+strings.Join(cols, ",")+`)
			SELECT m.post_id, m.captured_at, m.user_id, m.play_count, m.like_count, m.comment_count, m.share_count,
				m.save_count, m.repost_count
			FROM tmp_metrics m
			LEFT JOIN LATERAL (SELECT true AS found, p.play_count, p.like_count, p.comment_count, p.share_count,
					p.save_count, p.repost_count
				FROM post_metrics p WHERE p.post_id = m.post_id AND p.captured_at < m.captured_at
				ORDER BY p.captured_at DESC LIMIT 1) prev ON true
			WHERE prev.found IS NULL
			   OR (prev.play_count, prev.like_count, prev.comment_count, prev.share_count, prev.save_count, prev.repost_count)
			      IS DISTINCT FROM (m.play_count, m.like_count, m.comment_count, m.share_count, m.save_count, m.repost_count)
			ON CONFLICT DO NOTHING`)
		return err
	})
	if err != nil {
		return fmt.Errorf("persist: post metrics: %w", err)
	}
	written("post_metrics", n, start)
	metrics.PersistRows.WithLabelValues("post_metrics", "unchanged").Add(float64(int64(len(ms)) - n))
	return nil
}
