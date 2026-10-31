package scheduler

import (
	"context"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// DormantTier is the tier of pages that have not posted for DormantAfter.
const DormantTier = "T4"

// TierFor picks the tier and interval for a page posting postsPerDay, last
// at lastPost (nil when it never posted).
func TierFor(c config.Scheduler, postsPerDay float64, lastPost *time.Time, now time.Time) (string, time.Duration) {
	if c.DormantAfter > 0 && (lastPost == nil || now.Sub(*lastPost) > c.DormantAfter) {
		return DormantTier, c.DormantInterval
	}
	for _, t := range c.Tiers {
		if postsPerDay >= t.MinPostsPerDay {
			return t.Name, t.Interval
		}
	}
	if n := len(c.Tiers); n > 0 {
		return c.Tiers[n-1].Name, c.Tiers[n-1].Interval
	}
	return "T2", c.DefaultInterval
}

// PostingRate is posts per day over window, or over the span actually
// stored when the page's known history is shorter (a new page, a short
// backfill), but never less than a day.
func PostingRate(recent int64, first *time.Time, window time.Duration, now time.Time) float64 {
	span := window
	if first != nil && now.Sub(*first) < span {
		span = max(now.Sub(*first), 24*time.Hour)
	}
	return float64(recent) / span.Hours() * 24
}

// Adaptive keeps scheduled pages' tiers and post refreshes up to date.
type Adaptive struct {
	DB  *pgxpool.Pool
	Cfg *config.Config
	Now func() time.Time
}

func (a *Adaptive) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now().UTC()
}

// maxRefreshAge is the age after which a post is neither refreshed nor
// recrawled.
func maxRefreshAge(c config.PostRefresh) time.Duration {
	var m time.Duration
	for _, w := range c.Windows {
		m = max(m, w.MaxAge)
	}
	for _, r := range c.RecrawlAt {
		m = max(m, r)
	}
	return m
}

// AfterJob runs once a job on pageID has finished and its data is stored:
// it re-tiers the page from its posting rate and registers its recent posts
// for comment refresh. Pages that are not scheduled are left alone.
func (a *Adaptive) AfterJob(ctx context.Context, pageID int64) error {
	sc := a.Cfg.Scheduler
	now := a.now()
	var scheduled bool
	if err := a.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM page_schedule WHERE page_id = $1 AND enabled)`, pageID).
		Scan(&scheduled); err != nil || !scheduled {
		return err
	}
	window := sc.RateWindow
	if window <= 0 {
		window = 14 * 24 * time.Hour
	}
	var recent int64
	var last, first *time.Time
	// The page's known history starts at the oldest post its timeline scans
	// reached (crawl_cursors), not at the oldest post of the lookback.
	if err := a.DB.QueryRow(ctx, `SELECT count(*) FILTER (WHERE post_created_at > $2), max(post_created_at),
			(SELECT oldest_seen_at FROM crawl_cursors WHERE scope_type = 'page_timeline' AND scope_id = $1 AND ordering = 'default')
		FROM posts WHERE page_id = $1 AND post_created_at > $3`,
		pageID, now.Add(-window), now.Add(-max(window, sc.DormantAfter))).Scan(&recent, &last, &first); err != nil {
		return err
	}
	rate := PostingRate(recent, first, window, now)
	tier, interval := TierFor(sc, rate, last, now)
	if interval <= 0 {
		interval = max(sc.DefaultInterval, time.Hour)
	}
	secs := int(interval.Seconds())
	if _, err := a.DB.Exec(ctx, `UPDATE page_schedule SET posts_per_day = $2, last_post_at = $3,
			tier_changed_at = CASE WHEN tier IS DISTINCT FROM $4 THEN $6 ELSE tier_changed_at END,
			tier = $4,
			next_run_at = CASE WHEN adaptive AND $5 < interval_seconds
				THEN LEAST(next_run_at, $6 + make_interval(secs => $5)) ELSE next_run_at END,
			interval_seconds = CASE WHEN adaptive THEN $5 ELSE interval_seconds END
		WHERE page_id = $1`, pageID, rate, last, tier, secs, now); err != nil {
		return err
	}
	if !sc.PostRefresh.Enabled {
		return nil
	}
	oldest := now.Add(-maxRefreshAge(sc.PostRefresh))
	if _, err := a.DB.Exec(ctx, `DELETE FROM post_refresh WHERE page_id = $1 AND post_created_at < $2`, pageID, oldest); err != nil {
		return err
	}
	// Posts younger than the timeline overlap are re-read by every scan;
	// their first refresh is due once they age out of it.
	_, err := a.DB.Exec(ctx, `INSERT INTO post_refresh (post_id, page_id, post_created_at, next_refresh_at, last_comment_count)
		SELECT post_id, page_id, post_created_at, post_created_at + make_interval(secs => $3), comments_collected
		FROM posts WHERE page_id = $1 AND post_created_at > $2 AND status = 'visible'
		ON CONFLICT (post_id) DO NOTHING`, pageID, oldest, sc.IncrementalOverlap.Seconds())
	return err
}

type dueRefresh struct {
	postID                      int64
	pageID                      int64
	created                     time.Time
	refreshes, unchanged        int
	recrawls                    int
	lastCount, collected, total *int64
	feedbackID, permalink       *string
	newestComment               *time.Time
}

// DueRefreshes plans up to PostRefresh.MaxPerJob due refreshes of pageID
// (in tier). It changes nothing: commit, called once the job carrying the
// refreshes was dispatched, records them and schedules the next ones, so a
// failed dispatch leaves them due. Posts past every window are dropped by
// commit too.
func (a *Adaptive) DueRefreshes(ctx context.Context, pageID int64, tier string) (roots []dispatch.PostRoot, commit func(context.Context) error, err error) {
	pr := a.Cfg.Scheduler.PostRefresh
	noop := func(context.Context) error { return nil }
	if !pr.Enabled || pageID == 0 || pr.MaxPerJob <= 0 {
		return nil, noop, nil
	}
	now := a.now()
	rows, err := a.DB.Query(ctx, `SELECT r.post_id, r.post_created_at, r.refreshes, r.unchanged, r.recrawls, r.last_comment_count,
			p.comments_collected, p.comment_count_total, p.feedback_id, p.permalink,
			(SELECT max(c.created_at) FROM comments c WHERE c.post_created_at = r.post_created_at AND c.post_id = r.post_id)
		FROM post_refresh r JOIN posts p ON p.post_created_at = r.post_created_at AND p.post_id = r.post_id
		WHERE r.page_id = $1 AND r.next_refresh_at <= $2
		ORDER BY r.next_refresh_at LIMIT $3`, pageID, now, pr.MaxPerJob)
	if err != nil {
		return nil, noop, err
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (dueRefresh, error) {
		d := dueRefresh{pageID: pageID}
		return d, r.Scan(&d.postID, &d.created, &d.refreshes, &d.unchanged, &d.recrawls, &d.lastCount,
			&d.collected, &d.total, &d.feedbackID, &d.permalink, &d.newestComment)
	})
	if err != nil {
		return nil, noop, err
	}
	var batch pgx.Batch
	kinds := map[string]float64{}
	for _, d := range list {
		act, root, next, unchanged, recrawls := plan(pr, tier, d, now)
		switch act {
		case actDrop:
			batch.Queue(`DELETE FROM post_refresh WHERE post_id = $1`, d.postID)
		case actPostpone:
			batch.Queue(`UPDATE post_refresh SET next_refresh_at = $2 WHERE post_id = $1`, d.postID, next)
		default:
			batch.Queue(`UPDATE post_refresh SET next_refresh_at = $2, last_refresh_at = $3,
				refreshes = refreshes + 1, unchanged = $4, recrawls = $5, last_comment_count = $6 WHERE post_id = $1`,
				d.postID, next, now, unchanged, recrawls, d.collected)
			roots = append(roots, root)
			kind := "refresh"
			if root.Mode == fbv1.CrawlMode_CRAWL_MODE_RECRAWL {
				kind = "recrawl"
			}
			kinds[kind]++
		}
	}
	commit = func(ctx context.Context) error {
		if batch.Len() == 0 {
			return nil
		}
		if err := a.DB.SendBatch(ctx, &batch).Close(); err != nil {
			return err
		}
		for k, n := range kinds {
			metrics.PostRefreshes.WithLabelValues(k).Add(n)
		}
		return nil
	}
	return roots, commit, nil
}

func val[T any](p *T) (v T) {
	if p != nil {
		v = *p
	}
	return v
}

type action int

const (
	actRun      action = iota // run a refresh now
	actPostpone               // nothing now; come back at next
	actDrop                   // too old: forget the post
)

// plan decides one due refresh: run it (and when the next is due),
// postpone it to the next recrawl age, or drop the post.
func plan(pr config.PostRefresh, tier string, d dueRefresh, now time.Time) (act action, root dispatch.PostRoot, next time.Time, unchanged, recrawls int) {
	age := now.Sub(d.created)
	collected := val(d.collected)
	unchanged = d.unchanged
	if d.refreshes > 0 {
		if d.lastCount != nil && collected == *d.lastCount {
			unchanged++
		} else {
			unchanged = 0
		}
	}
	// Recrawl once per recrawl age reached; ages passed together (a post
	// registered late) count as one recrawl.
	recrawls = d.recrawls
	recrawl := false
	for recrawls < len(pr.RecrawlAt) && age >= pr.RecrawlAt[recrawls] {
		recrawls++
		recrawl = true
	}
	busy := collected >= pr.RecrawlMinComments
	recrawl = recrawl && busy

	var win *config.RefreshWindow
	for i := range pr.Windows {
		if w := &pr.Windows[i]; age < w.MaxAge && (len(w.Tiers) == 0 || contains(w.Tiers, tier)) {
			win = w
			break
		}
	}
	if win != nil {
		backoff := math.Pow(2, float64(min(unchanged, max(pr.MaxBackoff, 0))))
		next = now.Add(time.Duration(float64(win.Interval) * backoff))
	}
	if recrawls < len(pr.RecrawlAt) && busy {
		if at := d.created.Add(pr.RecrawlAt[recrawls]); next.IsZero() || at.Before(next) {
			next = at
		}
	}
	if !recrawl && win == nil {
		if next.IsZero() {
			return actDrop, root, next, unchanged, recrawls
		}
		return actPostpone, root, next, unchanged, recrawls
	}
	if next.IsZero() {
		next = now.Add(24 * time.Hour) // last visit; the post is dropped then
	}

	// The post is older than the timeline scan, so its stored count is the
	// last one known: seen = reported, and the incremental pass does not
	// take the gap between top-level comments and the total (replies) for
	// growth. Replies added to old threads are caught by the recrawls.
	reported := val(d.total)
	sc := &taskv1.PostScope{PostId: d.postID, PageId: d.pageID, PostCreatedAt: timestamppb.New(d.created),
		FeedbackId: val(d.feedbackID), ReportedCommentCount: reported, Permalink: val(d.permalink), CommentsSeen: reported}
	if sc.FeedbackId == "" {
		sc.FeedbackId = parse.FeedbackID(d.postID)
	}
	mode := fbv1.CrawlMode_CRAWL_MODE_RECRAWL
	if !recrawl {
		mode = fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL
		if d.newestComment == nil {
			mode = fbv1.CrawlMode_CRAWL_MODE_FULL // nothing stored yet: read it all
		} else {
			sc.CommentsWatermark = timestamppb.New(*d.newestComment)
		}
	}
	return actRun, dispatch.PostRoot{Mode: mode, Scope: sc}, next, unchanged, recrawls
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
