package stage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
)

// confirmUnavailable is how many attempts a page load that names nothing
// takes before the page is marked unavailable: on a later attempt the task
// usually runs on another identity and exit.
const confirmUnavailable = 3

// PageStage loads the page profile and paginates the timeline. It emits
// posts, metrics and media, and spawns one comments task per post whose
// comments need (re)collecting.
type PageStage struct{ D *Deps }

// refreshHorizon: in incremental mode posts younger than this are always
// re-read so their counts stay fresh.
const refreshHorizon = 72 * time.Hour

func (s *PageStage) Run(ctx context.Context, t *taskv1.Task) Outcome {
	d := s.D
	sc := t.GetPage()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "page task without page scope"}
	}
	id, err := d.identity(ctx, "timeline", t.Stalls)
	if err != nil {
		return noIdentity(t, err, c)
	}
	defer d.Pool.Release(id)

	st, _, err := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	if err != nil {
		return retry(t, "", "30s", "checkpoint: "+err.Error(), c)
	}
	st.Resume(c)
	if st.Cursor == "" {
		st.Cursor = t.Cursor
		st.PostsSeen = sc.PostsSeen
		st.ConsecutiveOld = sc.ConsecutiveOld
		if sc.NewestSeenAt != nil {
			st.NewestSeenUnix = sc.NewestSeenAt.AsTime().Unix()
		}
	}

	// Page HTML: profile + resolve + bootstrap (pagePlan). Page loads are
	// what Facebook walls for logged-out sessions that view many pages,
	// GraphQL is not, so they go through the exit's page-load gate.
	load, profile := d.pagePlan(t, sc, &st, id, c)
	if load {
		url := sc.Url
		if url == "" {
			ref := sc.PageRef
			if sc.PageId != 0 {
				ref = "id:" + strconv.FormatInt(sc.PageId, 10)
			}
			u, _ := id.Base.Parse(task.RefPath(ref))
			url = u.String()
		}
		pl, err := d.Loader.Load(ctx, id, url)
		if errors.Is(err, session.ErrPaced) {
			// The exit can't load a page now (nothing was sent).
			if !id.Bootstrapped() || sc.PageId == 0 {
				return pacedRetry(t, st.Cursor, err, c)
			}
			c["profile_deferred"]++ // only the profile needed it
			goto timeline
		}
		d.observeLoad(pl, err, t.Lane)
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			return retry(t, st.Cursor, "30s", "page load: "+err.Error(), c)
		}
		c["wire_bytes"] += pl.Bytes
		switch pl.Class {
		case detect.OK:
		case detect.ContentUnavailable:
			if pl.NamesNothing && t.Attempt+1 < confirmUnavailable {
				// One load that names nothing may be this exit's country or a
				// degraded response: confirm it on later attempts first.
				return retry(t, st.Cursor, "30s", pl.Reason, c)
			}
			if t.Seq == 0 {
				_ = d.emitUnavailable(ctx, t, sc)
			}
			c["pages_unavailable"]++
			return done(c)
		default:
			return classOutcome(t, pl.Class, pl.Reason, st.Cursor, c)
		}
		if profile {
			page, rep := parse.Page(pl.HTML, sc.PageId, 0)
			for _, f := range rep.Optional {
				metrics.ParseMissingField.WithLabelValues("page_html", f).Inc()
			}
			if !rep.OK() || page == nil {
				metrics.ParseTotal.WithLabelValues("page_html", "1", "drift").Inc()
				return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), "", c)
			}
			metrics.ParseTotal.WithLabelValues("page_html", "1", "ok").Inc()
			if d.Suppression.PageSuppressed(page.PageId) {
				c["pages_suppressed"]++
				return done(c)
			}
			sc.PageId = page.PageId
			page.RequestedRef = sc.PageRef
			b := d.Producer.NewBatch()
			if err := d.Emit.Page(ctx, b, emit.Source{JobID: t.JobId, Query: "page_html", ParserVersion: parse.PageVersion, Mode: t.Mode}, page); err != nil {
				return retry(t, "", "30s", err.Error(), c)
			}
			if err := b.Wait(ctx); err != nil {
				return retry(t, "", "30s", "produce: "+err.Error(), c)
			}
			c["pages"]++
		}
	}
timeline:
	if sc.PageId == 0 {
		return Outcome{Result: Fail, Reason: "page id unresolved", Counters: c}
	}
	if d.Suppression.PageSuppressed(sc.PageId) {
		c["pages_suppressed"]++
		return done(c)
	}

	now := d.now()
	cutoff := task.MaxAgeCutoff(t.Limits, now)
	mode := t.Mode
	var stopBefore time.Time // incremental boundary
	if mode == fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL {
		wm := sc.Watermark.AsTime()
		if sc.Watermark == nil {
			w, ok, err := d.pageWatermark(ctx, sc.PageId)
			if err != nil {
				return retry(t, st.Cursor, "30s", "watermark: "+err.Error(), c)
			}
			if !ok {
				mode = fbv1.CrawlMode_CRAWL_MODE_FULL // never crawled: full pass
			} else {
				wm = w.Add(-d.Cfg.Scheduler.IncrementalOverlap)
				sc.Watermark = timestamppb.New(wm)
			}
		}
		if mode == fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL {
			stopBefore = wm
			if h := now.Add(-refreshHorizon); h.Before(stopBefore) {
				stopBefore = h
			}
		}
	}

	stopOld := int32(d.Cfg.Crawler.ConsecutiveOldStop)
	q, _ := d.GQL.Registry.Get("timeline")
	src := emit.Source{JobID: t.JobId, Query: "timeline", DocID: q.DocID, ParserVersion: parse.TimelineVersion, Mode: mode}
	started := time.Now()
	var oldest time.Time
	finished, truncated := false, ""

	for !finished {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		vars := map[string]any{"page_id": strconv.FormatInt(sc.PageId, 10), "count": d.Cfg.Crawler.TimelinePageSize}
		if st.Cursor != "" {
			vars["cursor"] = st.Cursor
		}
		resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: vars, PageID: sc.PageId,
			Fast: t.Lane == taskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane), Referer: id.Base.String()})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class == detect.LoginWall {
			if wallTruncates(&st, st.Cursor) {
				truncated = "login_wall"
				metrics.TimelineTruncated.WithLabelValues("login_wall").Inc()
				break
			}
			_ = d.saveCkpt(ctx, t, st, c)
		}
		if resp.Class != detect.OK {
			return classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		}
		tl, rep := parse.Timeline(resp.Parts, sc.PageId, id.Base.String())
		d.GQL.Parsed(resp, rep.OK())
		if rep.OK() {
			d.Shadow.CompareTimeline("timeline", resp, tl, sc.PageId, id.Base.String())
		}
		for _, f := range rep.Optional {
			metrics.ParseMissingField.WithLabelValues("timeline", f).Inc()
		}
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues("timeline", "1", "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		}
		metrics.ParseTotal.WithLabelValues("timeline", "1", "ok").Inc()
		if tl.NodeNull && st.Pages == 0 && st.Cursor == "" {
			_ = d.emitUnavailable(ctx, t, sc)
			c["pages_unavailable"]++
			return done(c)
		}
		if tl.NoFeed {
			// The page id names something without a timeline (a new-style
			// page's older Page object): this page's problem, so not drift,
			// which would count against the query's breaker.
			metrics.ParseTotal.WithLabelValues("timeline", "1", "no_feed").Inc()
			c["pages_no_feed"]++
			return done(c)
		}

		b := d.Producer.NewBatch()
		var children []*taskv1.Task
		for _, pb := range tl.Posts {
			p := pb.Post
			created := p.CreatedAt.AsTime()
			pinned := p.IsPinned != nil && *p.IsPinned
			if !cutoff.IsZero() && created.Before(cutoff) || !stopBefore.IsZero() && created.Before(stopBefore) {
				if !pinned {
					st.ConsecutiveOld++
				}
				continue
			}
			st.ConsecutiveOld = 0
			if t.Limits.GetMaxPosts() > 0 && st.PostsSeen >= t.Limits.MaxPosts {
				finished = true
				break
			}
			st.PostsSeen++
			if u := created.Unix(); u > st.NewestSeenUnix {
				st.NewestSeenUnix = u
			}
			if oldest.IsZero() || created.Before(oldest) {
				oldest = created
			}
			if err := d.Emit.Post(ctx, b, src, p); err != nil {
				return retry(t, st.Cursor, "30s", err.Error(), c)
			}
			n, err := d.media(ctx, b, t, src, pb.Media, false)
			if err != nil {
				return retry(t, st.Cursor, "30s", err.Error(), c)
			}
			c["posts"]++
			c["media_queued"] += n
			if child, state := s.commentsTask(ctx, t, mode, p); child != nil {
				// Not again when this job already collected the post (a
				// refresh riding on the job).
				if collected, _ := d.Jobs.SetPost(ctx, t.JobId, p.PostId, created.Unix(), p.GetCommentCount(), jobs.PostPending); !collected {
					children = append(children, child)
				}
			} else {
				_, _ = d.Jobs.SetPost(ctx, t.JobId, p.PostId, created.Unix(), p.GetCommentCount(), state)
			}
		}
		if err := b.Wait(ctx); err != nil {
			return retry(t, st.Cursor, "30s", "produce: "+err.Error(), c)
		}
		if err := d.spawn(ctx, children...); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		st.Pages++
		if st.ConsecutiveOld >= stopOld || !tl.PageInfo.HasNext || tl.PageInfo.EndCursor == "" {
			finished = true
		} else {
			st.Cursor = tl.PageInfo.EndCursor
		}
		st.Walls, st.WallCursor = 0, ""
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("page: checkpoint save failed", "err", err)
		}
	}
	c["pages_fetched"] += int64(st.Pages)

	if finished || truncated != "" {
		state := "complete"
		if truncated != "" {
			state = "truncated"
			c["timelines_truncated"]++
		}
		if err := d.savePageWatermark(ctx, sc.PageId, passNewest(st, mode, truncated != ""), oldest, state, truncated, mode == fbv1.CrawlMode_CRAWL_MODE_FULL); err != nil {
			return retry(t, st.Cursor, "30s", "watermark: "+err.Error(), c)
		}
		return done(c)
	}
	// Yield: continue the stream in a new segment.
	next := task.Continuation(t, st.Cursor)
	ns := next.GetPage()
	ns.PageId, ns.PostsSeen, ns.ConsecutiveOld, ns.Watermark = sc.PageId, st.PostsSeen, st.ConsecutiveOld, sc.Watermark
	if st.NewestSeenUnix > 0 {
		ns.NewestSeenAt = timestamppb.New(time.Unix(st.NewestSeenUnix, 0))
	}
	next.Mode = mode
	metrics.TaskYields.WithLabelValues("page").Inc()
	return Outcome{Result: Continue, Next: next, Counters: c}
}

// commentsTask decides whether a post's comments must be (re)collected.
func (s *PageStage) commentsTask(ctx context.Context, t *taskv1.Task, mode fbv1.CrawlMode, p *fbv1.Post) (*taskv1.Task, string) {
	lim := t.Limits
	reported := p.GetCommentCount()
	if !lim.GetIncludeComments() {
		return nil, jobs.PostSkipped
	}
	if reported == 0 && p.CommentCount != nil {
		return nil, jobs.PostComplete // nothing to collect
	}
	scope := &taskv1.PostScope{PostId: p.PostId, PageId: p.PageId, PostCreatedAt: p.CreatedAt,
		FeedbackId: p.FeedbackId, ReportedCommentCount: reported, Permalink: p.Permalink}
	if scope.FeedbackId == "" {
		scope.FeedbackId = parse.FeedbackID(p.PostId)
	}
	cmode := fbv1.CrawlMode_CRAWL_MODE_FULL
	if mode == fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL {
		if prev, ok := s.D.Ckpt.GetPost(ctx, p.PostId); ok {
			// A post one of whose reply threads was lost is never skipped;
			// PostStage reads it whole.
			if reported <= prev.Reported && prev.ThreadsComplete && s.D.Ckpt.Incomplete(ctx, p.PostId) == "" {
				return nil, jobs.PostSkipped
			}
			cmode = fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL
			if prev.NewestComment > 0 {
				scope.CommentsWatermark = timestamppb.New(time.Unix(prev.NewestComment, 0))
			}
			scope.CommentsSeen = prev.Reported
		}
	}
	return task.NewPost(t, cmode, scope), ""
}

func (d *Deps) emitUnavailable(ctx context.Context, t *taskv1.Task, sc *taskv1.PageScope) error {
	if sc.PageId == 0 {
		return nil
	}
	b := d.Producer.NewBatch()
	p := &fbv1.Page{PageId: sc.PageId, RequestedRef: sc.PageRef, Status: fbv1.PageStatus_PAGE_STATUS_UNAVAILABLE}
	if err := d.Emit.Page(ctx, b, emit.Source{JobID: t.JobId, Query: "page_html", Mode: t.Mode}, p); err != nil {
		return err
	}
	return b.Wait(ctx)
}

// passNewest is the newest post time a finished timeline pass records as
// the page's watermark (zero: leave it). An incremental pass cut short by
// walls didn't reach its old watermark: the posts between where it stopped
// and the watermark are still unread, so the watermark stays and the next
// pass reads back to it again. (A pass stopped by the job's max_posts
// advances it: the posts past the cap are left out on purpose.)
func passNewest(st checkpoint.State, mode fbv1.CrawlMode, truncated bool) time.Time {
	if st.NewestSeenUnix <= 0 || truncated && mode == fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL {
		return time.Time{}
	}
	return time.Unix(st.NewestSeenUnix, 0).UTC()
}

// wallTruncates records a login wall at cursor and reports whether it has
// now been seen on enough distinct identities to call the stream truncated.
func wallTruncates(st *checkpoint.State, cursor string) bool {
	if st.WallCursor != cursor {
		st.WallCursor, st.Walls = cursor, 0
	}
	st.Walls++
	return st.Walls >= 2
}
