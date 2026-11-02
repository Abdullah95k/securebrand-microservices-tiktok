package stage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// The comment operations. Both need TikTok's signature (C2, C3): their
// variants are labelled "-signed", so the client's signer has a browser
// page sign each call (build brief D11, G1).
const (
	commentsQuery = "comments"
	repliesQuery  = "replies"
)

// PostStage reads a post's top-level comments, with the replies TikTok
// quotes inline, and spawns a thread task for each comment whose replies
// need their own reads. TikTok serves comments in its ranking order (T13),
// so every read starts at the first page and goes to the end or to the cap
// per post (limits.max_comments_per_post, across continuations): what is
// new is upserted, like and reply counts are updated, and a read that
// reached the end feeds deletion detection (ADR 0007 §5).
type PostStage struct{ D *Deps }

func (s *PostStage) Run(ctx context.Context, t *tkstaskv1.Task) Outcome {
	d := s.D
	sc := t.GetPost()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "post task without post scope"}
	}
	if sc.Username == "" {
		return Outcome{Result: Fail, Reason: "post task without the account's username"}
	}
	id, err := d.identity(ctx, "comments", t.Stalls)
	if err != nil {
		return noIdentity(t, err, c)
	}
	defer d.Pool.Release(id)
	page := postURL(id, sc.Username, sc.PostId, sc.Photo)
	if out, stop := d.ensurePage(ctx, t, id, page, c); stop {
		return out
	}

	st, saved, err := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	if err != nil {
		return retry(t, "", "30s", "checkpoint: "+err.Error(), c)
	}
	d.adoptIncomplete(ctx, t, sc, st)
	st.Resume(c)
	if st.Cursor == "" {
		st.Cursor = t.Cursor
	}
	if !saved {
		// The stream's earlier segments' comments come with the task, so
		// the cap is per post.
		st.ItemsSeen = sc.ItemsSeen
	}
	pass, err := d.fullPass(ctx, t, &st)
	if err != nil {
		return retry(t, st.Cursor, "30s", "full pass: "+err.Error(), c)
	}
	q, _ := d.Client.Registry.Get(commentsQuery)
	src := emit.Source{JobID: t.JobId, Query: commentsQuery, Variant: q.DocID, ParserVersion: parse.Version, Mode: t.Mode}
	cc := parse.CommentContext{PostID: sc.PostId, UserID: sc.UserId, PostCreatedAt: sc.CreatedAt}
	popt := parse.Options{AuthorKey: d.Emit.Key, Query: commentsQuery, Variant: q.DocID}

	started := time.Now()
	finished, exhausted, stuck := false, false, false
	for !finished {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		cursor := offset(st.Cursor)
		resp, err := d.Client.Do(ctx, id, graphql.Request{Query: commentsQuery,
			Vars:   map[string]any{"item_id": sc.PostId, "count": d.Cfg.TT.CommentsPageSize, "cursor": cursor},
			PageID: sc.UserId, Fast: t.Lane == tkstaskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane), Referer: page})
		c["requests"]++
		if err != nil {
			return requestFailed(t, st.Cursor, err, c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class != detect.OK {
			return classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		}
		res, rep := parse.Comments(resp.Body, cc, popt)
		d.Client.Parsed(resp, rep.OK())
		for _, f := range rep.Optional {
			metrics.ParseMissingField.WithLabelValues(commentsQuery, f).Inc()
		}
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues(commentsQuery, strconv.Itoa(parse.Version), "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		}
		metrics.ParseTotal.WithLabelValues(commentsQuery, strconv.Itoa(parse.Version), "ok").Inc()
		for k, n := range res.Dropped {
			c["dropped_"+k] += int64(n)
		}

		b := d.Producer.NewBatch()
		if err := emitComments(ctx, d, b, t, src, res, &st, c); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		threads := s.threads(ctx, t, sc, res.Threads, c)
		if err := b.Wait(ctx); err != nil {
			return retry(t, st.Cursor, "30s", "produce: "+err.Error(), c)
		}
		if err := d.spawn(ctx, threads...); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		if pass {
			if err := d.Jobs.AddSeen(ctx, t.JobId, sc.PostId, topLevelSeen(res)); err != nil {
				return retry(t, st.Cursor, "30s", "seen: "+err.Error(), c)
			}
		}
		c["threads_spawned"] += int64(len(threads))
		st.Pages++
		capped := t.Limits.GetMaxCommentsPerPost() > 0 && st.ItemsSeen >= t.Limits.GetMaxCommentsPerPost()
		switch {
		case !res.HasMore:
			finished, exhausted = true, true
		case capped:
			finished = true
		case res.Cursor <= cursor:
			// More said, but the offset did not move: a loop.
			finished, stuck = true, true
		default:
			st.Cursor = strconv.FormatInt(res.Cursor, 10)
		}
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("post: checkpoint save failed", "err", err)
		}
	}
	c["comment_pages"] += int64(st.Pages)
	if !finished {
		next := task.Continuation(t, st.Cursor)
		next.GetPost().ItemsSeen = st.ItemsSeen
		metrics.TaskYields.WithLabelValues("post").Inc()
		return Outcome{Result: Continue, Next: next, Counters: c}
	}

	if pass && exhausted {
		if err := d.Jobs.EndPass(ctx, t.JobId, t.LineageId, sc.PostId, sc.CreatedAt.AsTime(), sc.CommentCountSeen); err != nil {
			return retry(t, st.Cursor, "30s", "full pass: "+err.Error(), c)
		}
	}
	state := jobs.PostComplete
	switch {
	case stuck:
		state = jobs.PostTruncated
		c["comments_stuck"]++
	case !exhausted:
		state = jobs.PostPartial
		c["comments_capped"]++
	}
	_ = d.Jobs.UpdatePostState(ctx, t.JobId, sc.PostId, state)
	metrics.CommentReads.WithLabelValues("post", state).Inc()
	prev, _ := d.Ckpt.GetPost(ctx, sc.PostId)
	newest := max(prev.NewestComment, st.NewestSeenUnix)
	// Reported is the post's comment count this read was planned on: the
	// account stage reads the post again once the count goes past it.
	_ = d.Ckpt.SetPost(ctx, sc.PostId, checkpoint.PostCache{Reported: sc.CommentCountSeen, NewestComment: newest,
		ThreadsComplete: !stuck, ReadAt: d.now().Unix()})
	if sc.IncompleteMark != "" && exhausted {
		_ = d.Ckpt.ClearIncomplete(ctx, sc.PostId, sc.IncompleteMark)
	}
	return done(c)
}

// ensurePage gives an identity without page values (device id and the
// rest) its values from the post's page. stop ends the task with out.
func (d *Deps) ensurePage(ctx context.Context, t *tkstaskv1.Task, id *session.Identity, page string, c map[string]int64) (Outcome, bool) {
	if id.Bootstrapped() {
		return Outcome{}, false
	}
	pl, err := d.Loader.Bootstrap(ctx, id, page)
	if errors.Is(err, session.ErrPaced) || errors.Is(err, session.ErrExitPaused) {
		return pacedRetry(t, t.Cursor, err, c), true
	}
	d.observeLoad(pl, err, t.Lane)
	c["requests"]++
	if pl != nil {
		c["wire_bytes"] += pl.Bytes
	}
	if err != nil {
		if pl != nil && pl.Class != detect.OK {
			return classOutcome(t, pl.Class, pl.Reason, t.Cursor, c), true
		}
		c["transport_errors"]++
		return retry(t, t.Cursor, "30s", "bootstrap: "+err.Error(), c), true
	}
	return Outcome{}, false
}

// requestFailed retries a call that got no answer: the signer had no page
// or failed (not the task's doing: it waits like a task without an exit),
// or the request did not get through.
func requestFailed(t *tkstaskv1.Task, cursor string, err error, c map[string]int64) Outcome {
	if errors.Is(err, graphql.ErrSign) {
		c["sign_errors"]++
		o := retry(t, cursor, "30s", err.Error(), c)
		o.Waiting = true
		return o
	}
	c["transport_errors"]++
	return retry(t, cursor, "30s", err.Error(), c)
}

// fullPass reports whether this segment belongs to a full read of a post
// read before, whose seen comment ids let the finalizer mark the stored
// ones that are gone (fbs chapter 4 §11; ADR 0007 §5). Only a stream that
// starts at the first page begins a pass; its later segments and retries
// follow that decision.
func (d *Deps) fullPass(ctx context.Context, t *tkstaskv1.Task, st *checkpoint.State) (bool, error) {
	sc := t.GetPost()
	if !d.Cfg.Scheduler.PostRefresh.DetectDeletions || t.JobId == "" || t.Mode != commonv1.CrawlMode_CRAWL_MODE_FULL {
		return false, nil
	}
	if t.Seq == 0 && st.Pages == 0 && st.Cursor == "" {
		if _, known := d.Ckpt.GetPost(ctx, sc.PostId); known {
			return true, d.Jobs.BeginPass(ctx, t.JobId, t.LineageId, sc.PostId, d.now())
		}
	}
	return d.Jobs.InPass(ctx, t.JobId, t.LineageId)
}

// adoptIncomplete takes the post's incomplete mark (a reply thread lost
// since its last whole read) on a read's first segment: the read then
// re-reads every thread, and clears the mark once it read the post whole.
// A retry of a read under way keeps its own decision.
func (d *Deps) adoptIncomplete(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.PostScope, st checkpoint.State) {
	if t.Seq != 0 || sc.IncompleteMark != "" || st.Pages != 0 || st.Cursor != "" || t.Cursor != "" {
		return
	}
	sc.IncompleteMark = d.Ckpt.Incomplete(ctx, sc.PostId)
}

// threads returns the thread tasks of a page's comments: those with more
// replies than the page quoted inline, unless the thread was read whole
// before with as many replies (the post's incomplete mark re-reads them
// all). A thread the inline replies cover is done.
func (s *PostStage) threads(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.PostScope, refs []parse.ThreadRef, c map[string]int64) []*tkstaskv1.Task {
	d := s.D
	if !t.Limits.GetIncludeReplies() {
		return nil
	}
	var out []*tkstaskv1.Task
	for _, th := range refs {
		if th.ReplyCount <= int64(th.Inline) {
			_ = d.Ckpt.SetThread(ctx, th.CommentID, th.ReplyCount)
			continue
		}
		if prev, ok := d.Ckpt.GetThread(ctx, th.CommentID); ok && th.ReplyCount <= prev && sc.IncompleteMark == "" {
			c["threads_unchanged"]++
			continue
		}
		out = append(out, task.NewThread(t, commonv1.CrawlMode_CRAWL_MODE_FULL, &tkstaskv1.ThreadScope{
			PostId: sc.PostId, UserId: sc.UserId, PostCreatedAt: sc.CreatedAt, CommentId: th.CommentID,
			ReportedReplyCount: th.ReplyCount, Username: sc.Username, Photo: sc.Photo,
		}))
	}
	return out
}

// emitComments publishes one page of comments (and the replies quoted
// inline) with their images' metadata.
func emitComments(ctx context.Context, d *Deps, b *kafka.Batch, t *tkstaskv1.Task, src emit.Source, res parse.CommentsResult,
	st *checkpoint.State, c map[string]int64) error {
	for _, cb := range res.Comments {
		cm := cb.Comment
		if cm.CreatedAt != nil {
			st.NewestSeenUnix = max(st.NewestSeenUnix, cm.CreatedAt.AsTime().Unix())
		}
		if err := d.Emit.Comment(ctx, b, src, cm); err != nil {
			return err
		}
		st.ItemsSeen++
		if cm.Depth > 0 {
			c["replies"]++
		} else {
			c["comments"]++
		}
		n, err := d.media(ctx, b, t, src, cb.Media, true, fetcher{})
		if err != nil {
			return err
		}
		c["media_queued"] += n
	}
	return nil
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

// ThreadStage reads the replies of one comment, 3 a page as the web app
// asks (each page one signature), to the end.
type ThreadStage struct{ D *Deps }

func (s *ThreadStage) Run(ctx context.Context, t *tkstaskv1.Task) Outcome {
	d := s.D
	sc := t.GetThread()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "thread task without thread scope"}
	}
	if sc.Username == "" {
		return Outcome{Result: Fail, Reason: "thread task without the account's username"}
	}
	id, err := d.identity(ctx, "replies", t.Stalls)
	if err != nil {
		return noIdentity(t, err, c)
	}
	defer d.Pool.Release(id)
	page := postURL(id, sc.Username, sc.PostId, sc.Photo)
	if out, stop := d.ensurePage(ctx, t, id, page, c); stop {
		return out
	}
	st, saved, err := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	if err != nil {
		return retry(t, "", "30s", "checkpoint: "+err.Error(), c)
	}
	st.Resume(c)
	if st.Cursor == "" {
		st.Cursor = t.Cursor
	}
	if !saved {
		st.ItemsSeen = sc.ItemsSeen
	}
	q, _ := d.Client.Registry.Get(repliesQuery)
	src := emit.Source{JobID: t.JobId, Query: repliesQuery, Variant: q.DocID, ParserVersion: parse.Version, Mode: t.Mode}
	cc := parse.CommentContext{PostID: sc.PostId, UserID: sc.UserId, PostCreatedAt: sc.PostCreatedAt, Replies: true, ParentCommentID: sc.CommentId}
	popt := parse.Options{AuthorKey: d.Emit.Key, Query: repliesQuery, Variant: q.DocID}

	started := time.Now()
	finished, exhausted := false, false
	for !finished {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		cursor := offset(st.Cursor)
		resp, err := d.Client.Do(ctx, id, graphql.Request{Query: repliesQuery,
			Vars:   map[string]any{"item_id": sc.PostId, "comment_id": sc.CommentId, "count": d.Cfg.TT.RepliesPageSize, "cursor": cursor},
			PageID: sc.UserId, Fast: t.Lane == tkstaskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane), Referer: page})
		c["requests"]++
		if err != nil {
			return requestFailed(t, st.Cursor, err, c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class != detect.OK {
			return classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		}
		res, rep := parse.Comments(resp.Body, cc, popt)
		d.Client.Parsed(resp, rep.OK())
		for _, f := range rep.Optional {
			metrics.ParseMissingField.WithLabelValues(repliesQuery, f).Inc()
		}
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues(repliesQuery, strconv.Itoa(parse.Version), "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		}
		metrics.ParseTotal.WithLabelValues(repliesQuery, strconv.Itoa(parse.Version), "ok").Inc()
		for k, n := range res.Dropped {
			c["dropped_"+k] += int64(n)
		}
		b := d.Producer.NewBatch()
		if err := emitComments(ctx, d, b, t, src, res, &st, c); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		if err := b.Wait(ctx); err != nil {
			return retry(t, st.Cursor, "30s", "produce: "+err.Error(), c)
		}
		st.Pages++
		capped := t.Limits.GetMaxCommentsPerPost() > 0 && st.ItemsSeen >= t.Limits.GetMaxCommentsPerPost()
		switch {
		case !res.HasMore:
			finished, exhausted = true, true
		case capped || res.Cursor <= cursor:
			finished = true
		default:
			st.Cursor = strconv.FormatInt(res.Cursor, 10)
		}
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("thread: checkpoint save failed", "err", err)
		}
	}
	c["reply_pages"] += int64(st.Pages)
	if !finished {
		next := task.Continuation(t, st.Cursor)
		next.GetThread().ItemsSeen = st.ItemsSeen
		metrics.TaskYields.WithLabelValues("thread").Inc()
		return Outcome{Result: Continue, Next: next, Counters: c}
	}
	state := jobs.PostComplete
	if !exhausted {
		state = jobs.PostPartial
		c["threads_capped"]++
	}
	metrics.CommentReads.WithLabelValues("thread", state).Inc()
	// Read as far as it will be: the next read of the post re-reads it once
	// its reply count grows.
	_ = d.Ckpt.SetThread(ctx, sc.CommentId, sc.ReportedReplyCount)
	return done(c)
}

// offset reads a comment stream's saved cursor: TikTok's offset.
func offset(saved string) int64 {
	v, _ := strconv.ParseInt(saved, 10, 64)
	return max(v, 0)
}

// postURL is a post's page, the comment calls' referer: /@<username>/video/
// <id>, or /photo/ for a photo post.
func postURL(id *session.Identity, username string, postID int64, photo bool) string {
	kind := "/video/"
	if photo {
		kind = "/photo/"
	}
	u, _ := id.Base.Parse(task.ProfilePath(username) + kind + strconv.FormatInt(postID, 10))
	return u.String()
}
