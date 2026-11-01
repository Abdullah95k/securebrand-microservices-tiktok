package persist

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

var postCols = []string{"created_at", "post_id", "user_id", "kind", "caption", "text_ranges", "hashtags", "hashtag_ids",
	"duration_ms", "width", "height", "cover_media_id", "photo_count", "music_id", "is_pinned", "is_ad", "labels",
	"category", "playlist_id", "comments_disabled", "region", "text_language", "discovered_via",
	"play_count", "like_count", "comment_count", "share_count", "save_count", "repost_count",
	"content_hash", "status", "first_seen_at", "last_seen_at"}

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

// scrubMentions leaves only the author key of erased people a text
// mentions (the emitter does the same with the crawler's copy of the
// suppression list, which may lag).
func scrubMentions(sup *gdpr.Suppression, rs []*ttv1.TextRange) {
	for _, r := range rs {
		if p := r.GetPerson(); p != nil && sup.AuthorSuppressed(p.AuthorKey) {
			r.Person = &ttv1.Person{AuthorKey: p.AuthorKey}
			if r.Type == "mention" {
				r.Name = ""
			}
		}
	}
}

func mentionKeys(rs []*ttv1.TextRange) [][]byte {
	var out [][]byte
	for _, r := range rs {
		if k := r.GetPerson().GetAuthorKey(); len(k) > 0 {
			out = append(out, k)
		}
	}
	return out
}

func postRow(p *ttv1.Post) []any {
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
		c.Plays, c.Likes, c.Comments, c.Shares, c.Saves, c.Reposts, hashOf(p.Obs), status, seen, seen}
}

// Posts upserts posts, their index rows and the people their captions
// mention. Posts of an erased account are dropped; erased people's
// mentions are cleared. The pinned and ad flags, known only from lists,
// are kept when a read does not carry them.
func (w *Writer) Posts(ctx context.Context, ps []*ttv1.Post) error {
	ps = latest(ps, func(p *ttv1.Post) string { return strconv.FormatInt(p.PostId, 10) })
	kept := ps[:0]
	for _, p := range ps {
		if p.PostId == 0 || p.UserId == 0 || p.CreatedAt == nil {
			metrics.PersistRows.WithLabelValues("post", "rejected").Inc()
			continue
		}
		kept = append(kept, p)
	}
	if ps = kept; len(ps) == 0 {
		return nil
	}
	start := time.Now()
	ids := make([]int64, len(ps))
	times := make([]time.Time, len(ps))
	var keys [][]byte
	for i, p := range ps {
		ids[i], times[i] = p.UserId, p.CreatedAt.AsTime()
		keys = append(keys, mentionKeys(p.TextRanges)...)
	}
	var n int64
	var dropped int
	err := w.withPartitions(ctx, "posts", times, func() error {
		n, dropped = 0, 0
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			sup, err := guard(ctx, tx, keys, ids)
			if err != nil {
				return err
			}
			rows := make([][]any, 0, len(ps))
			var mrows [][]any
			for _, p := range ps {
				if sup.PageSuppressed(p.UserId) {
					dropped++
					continue
				}
				scrubMentions(sup, p.TextRanges)
				rows = append(rows, postRow(p))
				for _, m := range mentions(p.TextRanges) {
					if !sup.AuthorSuppressed(m.key) {
						mrows = append(mrows, []any{p.PostId, m.key, m.userID, m.offsets, observed(p.Obs)})
					}
				}
			}
			if len(rows) == 0 {
				return nil
			}
			if err := copyTemp(ctx, tx, "tmp_posts", "posts", postCols, rows); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `INSERT INTO posts AS t (`+strings.Join(postCols, ",")+`)
				SELECT `+strings.Join(postCols, ",")+` FROM tmp_posts
				ON CONFLICT (created_at, post_id) DO UPDATE SET `+set(postCols, map[string]string{
				"created_at": "", "post_id": "", "first_seen_at": "", "discovered_via": "",
				"is_pinned": "COALESCE(EXCLUDED.is_pinned, t.is_pinned)", "is_ad": "COALESCE(EXCLUDED.is_ad, t.is_ad)"})+`
				WHERE t.last_seen_at <= EXCLUDED.last_seen_at
				  AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash
				   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day')`)
			if err != nil {
				return err
			}
			n = tag.RowsAffected()
			if _, err := tx.Exec(ctx, `INSERT INTO post_index (post_id, user_id, created_at, kind)
				SELECT post_id, user_id, created_at, kind FROM tmp_posts ON CONFLICT (post_id) DO NOTHING`); err != nil {
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
	metrics.PersistRows.WithLabelValues("post", "unchanged").Add(float64(int64(len(ps)) - n - int64(dropped)))
	suppressed("post", dropped)
	return nil
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
	ids := make([]int64, len(ms))
	for i, m := range ms {
		ids[i] = m.UserId
	}
	var n int64
	var dropped int
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		sup, err := guard(ctx, tx, nil, ids)
		if err != nil {
			return err
		}
		dropped = 0
		cols := []string{"post_id", "captured_at", "user_id", "play_count", "like_count", "comment_count", "share_count",
			"save_count", "repost_count"}
		rows := make([][]any, 0, len(ms))
		for _, m := range ms {
			if sup.PageSuppressed(m.UserId) {
				dropped++
				continue
			}
			c := m.Counts
			rows = append(rows, []any{m.PostId, m.CapturedAt.AsTime(), m.UserId, c.Plays, c.Likes, c.Comments, c.Shares, c.Saves, c.Reposts})
		}
		if len(rows) == 0 {
			return nil
		}
		if err := copyTemp(ctx, tx, "tmp_metrics", "post_metrics", cols, rows); err != nil {
			return err
		}
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
	metrics.PersistRows.WithLabelValues("post_metrics", "unchanged").Add(float64(int64(len(ms)) - n - int64(dropped)))
	suppressed("post_metrics", dropped)
	return nil
}
