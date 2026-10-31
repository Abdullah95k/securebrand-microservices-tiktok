package stage

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// incrementalOlderStop: newest-first incremental reads stop after this many
// consecutive comments older than the watermark.
const incrementalOlderStop = 5

// PostStage paginates a post's top-level comments (with any inline replies)
// and spawns a thread task per reply thread that needs its own pagination.
type PostStage struct{ D *Deps }

func (s *PostStage) Run(ctx context.Context, t *taskv1.Task) Outcome {
	d := s.D
	sc := t.GetPost()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "post task without post scope"}
	}
	id, err := d.identity(ctx, "comments", t.Attempt)
	if err != nil {
		return retry(t, "", "30s", "no identity: "+err.Error(), c)
	}
	defer d.Pool.Release(id)
	if pl, err := d.ensureBootstrapped(ctx, id, sc.PageId, t.Lane); err != nil {
		if pl != nil {
			return classOutcome(t, pl.Class, pl.Reason, t.Cursor, c)
		}
		return retry(t, t.Cursor, "30s", "bootstrap: "+err.Error(), c)
	}

	st, _, err := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	if err != nil {
		return retry(t, "", "30s", "checkpoint: "+err.Error(), c)
	}
	st.Resume(c)
	if st.Cursor == "" {
		st.Cursor = t.Cursor
	}

	query := "comments"
	var watermark time.Time
	if t.Mode == fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL && sc.CommentsWatermark != nil {
		query = "comments_newest"
		watermark = sc.CommentsWatermark.AsTime().Add(-time.Minute)
	}
	q, _ := d.GQL.Registry.Get(query)
	src := emit.Source{JobID: t.JobId, Query: query, DocID: q.DocID, ParserVersion: parse.CommentsVersion, Mode: t.Mode}
	cc := parse.CommentContext{PostID: sc.PostId, PageID: sc.PageId, PostCreatedAt: sc.PostCreatedAt,
		PageAuthorID: strconv.FormatInt(sc.PageId, 10)}

	pass, err := d.fullPass(ctx, t, &st)
	if err != nil {
		return retry(t, st.Cursor, "30s", "full pass: "+err.Error(), c)
	}

	started := time.Now()
	finished, truncated, exhausted := false, false, false
	var olderRun int
	var reported int64 // Facebook's comment count on the last page read
	for !finished {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		vars := map[string]any{"feedback_id": sc.FeedbackId}
		if st.Cursor != "" {
			vars["cursor"] = st.Cursor
		}
		resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: query, Vars: vars, PageID: sc.PageId,
			Fast: t.Lane == taskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane), Referer: sc.Permalink})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class == detect.LoginWall {
			if wallTruncates(&st, st.Cursor) {
				truncated = true
				break
			}
			_ = d.saveCkpt(ctx, t, st, c)
		}
		if resp.Class != detect.OK {
			return classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		}
		res, rep := parse.Comments(resp.Parts, cc)
		d.GQL.Parsed(resp, rep.OK())
		if rep.OK() {
			d.Shadow.CompareComments(query, resp, res, cc)
		}
		for _, f := range rep.Optional {
			metrics.ParseMissingField.WithLabelValues(query, f).Inc()
		}
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues(query, "1", "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		}
		metrics.ParseTotal.WithLabelValues(query, "1", "ok").Inc()
		if res.NodeNull && st.Pages == 0 {
			_ = d.Jobs.UpdatePostState(ctx, t.JobId, sc.PostId, jobs.PostComplete)
			return done(map[string]int64{"posts_unavailable": 1})
		}

		b := d.Producer.NewBatch()
		n, older, err := emitComments(ctx, d, b, t, src, res, watermark, &st, c)
		if err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		if !watermark.IsZero() {
			if n > 0 && older == n {
				olderRun += older
			} else {
				olderRun = older
			}
		}
		threads := s.threads(ctx, t, res.Threads)
		if err := b.Wait(ctx); err != nil {
			return retry(t, st.Cursor, "30s", "produce: "+err.Error(), c)
		}
		if err := d.spawn(ctx, threads...); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		if res.TotalCount > 0 {
			reported = res.TotalCount
		}
		if pass {
			if err := d.Jobs.AddSeen(ctx, t.JobId, sc.PostId, topLevelSeen(res)); err != nil {
				return retry(t, st.Cursor, "30s", "seen: "+err.Error(), c)
			}
		}
		c["threads_spawned"] += int64(len(threads))
		st.Pages++
		st.Walls, st.WallCursor = 0, ""
		max := t.Limits.GetMaxCommentsPerPost()
		switch {
		case !res.PageInfo.HasNext || res.PageInfo.EndCursor == "":
			finished, exhausted = true, true
		case max > 0 && st.ItemsSeen >= max:
			finished = true
		case !watermark.IsZero() && olderRun >= incrementalOlderStop:
			finished = true
		default:
			st.Cursor = res.PageInfo.EndCursor
		}
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("post: checkpoint save failed", "err", err)
		}
	}
	c["comment_pages"] += int64(st.Pages)

	if finished || truncated {
		if pass && exhausted && !truncated {
			if err := d.Jobs.EndPass(ctx, t.JobId, t.LineageId, sc.PostId, sc.PostCreatedAt.AsTime(), reported); err != nil {
				return retry(t, st.Cursor, "30s", "full pass: "+err.Error(), c)
			}
		}
		state := jobs.PostComplete
		if truncated {
			state = jobs.PostTruncated
			c["posts_truncated"]++
		}
		_ = d.Jobs.UpdatePostState(ctx, t.JobId, sc.PostId, state)
		prev, _ := d.Ckpt.GetPost(ctx, sc.PostId)
		newest := prev.NewestComment
		if st.NewestSeenUnix > newest {
			newest = st.NewestSeenUnix
		}
		_ = d.Ckpt.SetPost(ctx, sc.PostId, checkpoint.PostCache{Reported: sc.ReportedCommentCount, NewestComment: newest, ThreadsComplete: !truncated})
		// An incremental newest-first pass cannot see replies added to older
		// comments. If the count grew more than we found, do a full pass.
		out := done(c)
		if t.Mode == fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL && !truncated {
			grew := sc.ReportedCommentCount - sc.CommentsSeen
			if grew > 0 && st.ItemsSeen < grew {
				out.FollowUps = append(out.FollowUps, task.NewPost(t, fbv1.CrawlMode_CRAWL_MODE_RECRAWL, cloneScope(sc)))
				c["recrawls_spawned"]++
			}
		}
		return out
	}
	next := task.Continuation(t, st.Cursor)
	metrics.TaskYields.WithLabelValues("post").Inc()
	return Outcome{Result: Continue, Next: next, Counters: c}
}

// saveCkpt saves the segment's resume point with its counters so far.
func (d *Deps) saveCkpt(ctx context.Context, t *taskv1.Task, st checkpoint.State, c map[string]int64) error {
	st.Counters = maps.Clone(c)
	return d.Ckpt.Save(ctx, t.LineageId, t.Seq, st)
}

// fullPass reports whether this segment belongs to a full pass over a
// known post's comments, whose seen ids let the finalizer find deleted
// comments. Only a lineage that starts from the first page, on a recrawl or
// on a full read of a post crawled before, begins a pass; later segments and
// retries follow that decision.
func (d *Deps) fullPass(ctx context.Context, t *taskv1.Task, st *checkpoint.State) (bool, error) {
	sc := t.GetPost()
	if !d.Cfg.Scheduler.PostRefresh.DetectDeletions || t.JobId == "" ||
		(t.Mode != fbv1.CrawlMode_CRAWL_MODE_RECRAWL && t.Mode != fbv1.CrawlMode_CRAWL_MODE_FULL) {
		return false, nil
	}
	if t.Seq == 0 && st.Pages == 0 && st.Cursor == "" {
		known := t.Mode == fbv1.CrawlMode_CRAWL_MODE_RECRAWL
		if !known {
			_, known = d.Ckpt.GetPost(ctx, sc.PostId)
		}
		if known {
			return true, d.Jobs.BeginPass(ctx, t.JobId, t.LineageId, sc.PostId, time.Now())
		}
	}
	return d.Jobs.InPass(ctx, t.JobId, t.LineageId)
}

func topLevelSeen(res parse.CommentsResult) []jobs.SeenComment {
	out := make([]jobs.SeenComment, 0, len(res.Comments))
	for _, cb := range res.Comments {
		if cb.Comment.Depth == 0 {
			out = append(out, jobs.SeenComment{ID: cb.Comment.CommentId, Replies: cb.Comment.GetReplyCount()})
		}
	}
	return out
}

func cloneScope(sc *taskv1.PostScope) *taskv1.PostScope {
	cp := proto.Clone(sc).(*taskv1.PostScope)
	cp.CommentsWatermark = nil
	return cp
}

// emitComments publishes one page of comments and their media. It returns
// how many comments were seen and how many were older than watermark.
func emitComments(ctx context.Context, d *Deps, b *kafka.Batch, t *taskv1.Task, src emit.Source, res parse.CommentsResult,
	watermark time.Time, st *checkpoint.State, c map[string]int64) (int, int, error) {
	older := 0
	for _, cb := range res.Comments {
		cm := cb.Comment
		if cm.CreatedAt != nil {
			u := cm.CreatedAt.AsTime().Unix()
			if u > st.NewestSeenUnix {
				st.NewestSeenUnix = u
			}
			if !watermark.IsZero() && cm.CreatedAt.AsTime().Before(watermark) {
				older++
			}
		}
		keep, err := d.Emit.Comment(ctx, b, src, cm)
		if err != nil {
			return 0, 0, err
		}
		st.ItemsSeen++
		if cm.Depth > 0 {
			c["replies"]++
		} else {
			c["comments"]++
		}
		if !keep {
			c["comments_redacted"]++
			continue
		}
		n, err := d.media(ctx, b, t, src, cb.Media, true)
		if err != nil {
			return 0, 0, err
		}
		c["media_queued"] += n
	}
	return len(res.Comments), older, nil
}

// threads returns the reply-thread tasks to spawn.
func (s *PostStage) threads(ctx context.Context, t *taskv1.Task, refs []parse.ThreadRef) []*taskv1.Task {
	sc := t.GetPost()
	return threadTasks(ctx, s.D, t, refs, sc.PostId, sc.PageId, sc.PostCreatedAt)
}

// threadTasks turns reply-thread references into thread tasks, skipping
// threads beyond max depth and, in incremental mode, unchanged threads.
func threadTasks(ctx context.Context, d *Deps, t *taskv1.Task, refs []parse.ThreadRef, postID, pageID int64, created *timestamppb.Timestamp) []*taskv1.Task {
	lim := t.Limits
	if !lim.GetIncludeReplies() {
		return nil
	}
	maxDepth := lim.GetMaxDepth()
	if maxDepth <= 0 {
		maxDepth = 3
	}
	var out []*taskv1.Task
	for _, th := range refs {
		if th.Depth+1 >= maxDepth || th.FeedbackID == "" {
			continue
		}
		if t.Mode == fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL {
			if prev, ok := d.Ckpt.GetThread(ctx, th.CommentID); ok && th.ReplyCount <= prev {
				continue
			}
		}
		out = append(out, task.NewThread(t, fbv1.CrawlMode_CRAWL_MODE_FULL, &taskv1.ThreadScope{
			PostId: postID, PageId: pageID, PostCreatedAt: created, CommentId: th.CommentID,
			RootCommentId: th.RootCommentID, Depth: th.Depth + 1, FeedbackId: th.FeedbackID,
			ExpansionToken: th.ExpansionToken, ReportedReplyCount: th.ReplyCount,
		}))
	}
	return out
}

// ThreadStage paginates the replies of one comment.
type ThreadStage struct{ D *Deps }

func (s *ThreadStage) Run(ctx context.Context, t *taskv1.Task) Outcome {
	d := s.D
	sc := t.GetThread()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "thread task without thread scope"}
	}
	id, err := d.identity(ctx, "replies", t.Attempt)
	if err != nil {
		return retry(t, "", "30s", "no identity: "+err.Error(), c)
	}
	defer d.Pool.Release(id)
	if pl, err := d.ensureBootstrapped(ctx, id, sc.PageId, t.Lane); err != nil {
		if pl != nil {
			return classOutcome(t, pl.Class, pl.Reason, t.Cursor, c)
		}
		return retry(t, t.Cursor, "30s", "bootstrap: "+err.Error(), c)
	}
	st, _, err := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	if err != nil {
		return retry(t, "", "30s", "checkpoint: "+err.Error(), c)
	}
	st.Resume(c)
	if st.Cursor == "" {
		st.Cursor = t.Cursor
	}
	q, _ := d.GQL.Registry.Get("replies")
	src := emit.Source{JobID: t.JobId, Query: "replies", DocID: q.DocID, ParserVersion: parse.CommentsVersion, Mode: t.Mode}
	cc := parse.CommentContext{PostID: sc.PostId, PageID: sc.PageId, PostCreatedAt: sc.PostCreatedAt,
		PageAuthorID: strconv.FormatInt(sc.PageId, 10), BaseDepth: sc.Depth,
		ParentCommentID: sc.CommentId, RootCommentID: sc.RootCommentId, Replies: true}

	started := time.Now()
	finished, truncated := false, false
	for !finished {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		vars := map[string]any{"feedback_id": sc.FeedbackId, "expansion_token": sc.ExpansionToken}
		if st.Cursor != "" {
			vars["cursor"] = st.Cursor
		}
		resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "replies", Vars: vars, PageID: sc.PageId,
			Fast: t.Lane == taskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane)})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class == detect.LoginWall {
			if wallTruncates(&st, st.Cursor) {
				truncated = true
				break
			}
			_ = d.saveCkpt(ctx, t, st, c)
		}
		if resp.Class != detect.OK {
			return classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		}
		res, rep := parse.Comments(resp.Parts, cc)
		d.GQL.Parsed(resp, rep.OK())
		if rep.OK() {
			d.Shadow.CompareComments("replies", resp, res, cc)
		}
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues("replies", "1", "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		}
		metrics.ParseTotal.WithLabelValues("replies", "1", "ok").Inc()
		b := d.Producer.NewBatch()
		if _, _, err := emitComments(ctx, d, b, t, src, res, time.Time{}, &st, c); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		nested := threadTasks(ctx, d, t, res.Threads, sc.PostId, sc.PageId, sc.PostCreatedAt)
		if err := b.Wait(ctx); err != nil {
			return retry(t, st.Cursor, "30s", "produce: "+err.Error(), c)
		}
		if err := d.spawn(ctx, nested...); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		c["threads_spawned"] += int64(len(nested))
		st.Pages++
		st.Walls, st.WallCursor = 0, ""
		if !res.PageInfo.HasNext || res.PageInfo.EndCursor == "" {
			finished = true
		} else {
			st.Cursor = res.PageInfo.EndCursor
		}
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("thread: checkpoint save failed", "err", err)
		}
	}
	if finished || truncated {
		if truncated {
			c["threads_truncated"]++
			_ = d.Jobs.UpdatePostState(ctx, t.JobId, sc.PostId, jobs.PostTruncated)
		} else {
			_ = d.Ckpt.SetThread(ctx, sc.CommentId, sc.ReportedReplyCount)
		}
		return done(c)
	}
	metrics.TaskYields.WithLabelValues("thread").Inc()
	return Outcome{Result: Continue, Next: task.Continuation(t, st.Cursor), Counters: c}
}
