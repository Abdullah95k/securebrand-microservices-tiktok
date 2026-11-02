package stage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Why a list walk stopped (tt.crawl_cursors.truncated_reason for the
// truncated ones, the list_stops metric for all; build brief §8 A3).
const (
	// ListStopFirstPost: the list said no older posts remain.
	ListStopFirstPost = "first_post"
	// ListStopAge: the walk reached its age window (the backfill's 365
	// days, or a metrics walk's depth).
	ListStopAge = "age"
	// ListStopWatermark: an incremental check reached the newest post
	// stored before it (less the overlap).
	ListStopWatermark = "watermark"
	// ListStopMaxPosts: the job's max_posts.
	ListStopMaxPosts = "max_posts"
	// ListStopStuck: pages holding only the cursor's post, stuckPages in a
	// row (C3 §4): truncated.
	ListStopStuck = "stuck"
)

// listQuery is the post list's operation: the creator list, unsigned
// (build brief D11, G1; C2, C3).
const listQuery = "creator_list"

// stuckPages is how many pages in a row may hold only the cursor's post
// before the walk stops (the probe's rule, M1j).
const stuckPages = 3

// softPause is the pause before a soft error (statusCode 10101 or 10102,
// 0.12 % of the C3 full pass) is retried once on the same identity, as the
// probe does; a second one in a row retries the task.
const softPause = 2 * time.Second

// AccountStage checks one account: its profile page when the account is due
// a refresh (the only source of exact counts and the bio link: T31), then
// its post list back to the newest stored post (an incremental check), to
// the age window (a backfill or a metrics walk) or to the first post. Every
// list item yields the post with its counts, its music and its media; the
// first item's author refreshes the account on every check. A post whose
// comment count went up, within its comment window, gets a post task.
type AccountStage struct {
	D *Deps
	// SoftPause overrides softPause (tests).
	SoftPause time.Duration
}

// walk is one segment's state of a list walk.
type walk struct {
	mode       commonv1.CrawlMode
	cutoff     time.Time // the age window; zero: none
	stopBefore time.Time // an incremental check's boundary; zero: none
	oldest     time.Time
	stop       string
}

func (s *AccountStage) Run(ctx context.Context, t *tkstaskv1.Task) Outcome {
	d := s.D
	sc := t.GetAccount()
	c := map[string]int64{}
	if sc == nil {
		return Outcome{Result: Fail, Reason: "account task without account scope"}
	}
	id, err := d.identity(ctx, "list", t.Stalls)
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
		if sc.NewestSeenAt != nil {
			st.NewestSeenUnix = sc.NewestSeenAt.AsTime().Unix()
		}
	}

	// The profile page, on the stream's first segment: when the account is
	// due, when the list's key (secUid) is not known yet, or to bootstrap an
	// identity without page values.
	if t.Seq == 0 && st.Cursor == "" && st.Pages == 0 {
		if out, stop := s.profile(ctx, t, sc, id, c); stop {
			return out
		}
	} else if !id.Bootstrapped() {
		if out, stop := s.bootstrap(ctx, t, sc, id, st.Cursor, c); stop {
			return out
		}
	}
	if sc.SecUid == "" {
		return Outcome{Result: Fail, Reason: "account unresolved: no secUid", Counters: c}
	}

	w, err := s.plan(ctx, t, sc)
	if err != nil {
		return retry(t, st.Cursor, "30s", "watermark: "+err.Error(), c)
	}
	q, _ := d.Client.Registry.Get(listQuery)
	src := emit.Source{JobID: t.JobId, Query: listQuery, Variant: q.DocID, ParserVersion: parse.Version, Mode: w.mode}
	popt := parse.Options{AuthorKey: d.Emit.Key, Query: listQuery, Variant: q.DocID}
	started := time.Now()
	finished := false
	softRetried := false
	for !finished {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		cursor, edgeIDs := s.cursor(st.Cursor)
		resp, err := d.Client.Do(ctx, id, graphql.Request{Query: listQuery,
			Vars:   map[string]any{"sec_uid": sc.SecUid, "count": d.Cfg.TT.ListPageSize, "cursor": cursor},
			PageID: sc.UserId, Fast: t.Lane == tkstaskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane),
			Referer: profileURL(id, sc.Username)})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class == detect.ContentUnavailable {
			return s.unavailable(ctx, t, sc, statusOf(resp.Errors), c)
		}
		if resp.Class == detect.SoftError && !softRetried {
			// TikTok's occasional list error: once more after a pause.
			softRetried = true
			c["soft_errors"]++
			if err := sleep(ctx, s.pause()); err != nil {
				return retry(t, st.Cursor, "30s", err.Error(), c)
			}
			continue
		}
		if resp.Class != detect.OK {
			return classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		}
		softRetried = false
		res, rep := parse.PostList(resp.Body, popt)
		d.Client.Parsed(resp, rep.OK())
		for _, f := range rep.Optional {
			metrics.ParseMissingField.WithLabelValues(listQuery, f).Inc()
		}
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues(listQuery, strconv.Itoa(parse.Version), "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		}
		metrics.ParseTotal.WithLabelValues(listQuery, strconv.Itoa(parse.Version), "ok").Inc()
		for k, n := range res.Dropped {
			c["dropped_"+k] += int64(n)
		}

		b := d.Producer.NewBatch()
		if st.Pages == 0 && t.Seq == 0 && res.Account != nil {
			// The list's author: name, bio, avatar and flags on every check
			// (renames show within one check); its counts are rounded, so
			// the persister keeps the page's exact ones (T31).
			if out, stop := s.listAccount(ctx, b, t, sc, res, fetcherOf(id), c); stop {
				return out
			}
		}
		children, out, stop := s.posts(ctx, b, t, sc, &st, &w, src, res, edgeIDs, fetcherOf(id), c)
		if stop {
			return out
		}
		if err := b.Wait(ctx); err != nil {
			return retry(t, st.Cursor, "30s", "produce: "+err.Error(), c)
		}
		if err := d.spawn(ctx, children...); err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		st.Pages++

		next, more := ttclient.NextCreatorCursor(resp.Parts[0], cursor)
		if allAtCursor(res, cursor) {
			st.Stuck++
		} else {
			st.Stuck = 0
		}
		bound := w.cutoff
		if w.stopBefore.After(bound) {
			bound = w.stopBefore
		}
		switch {
		case w.stop != "":
			finished = true
		case st.Stuck >= stuckPages:
			w.stop, finished = ListStopStuck, true
		case !bound.IsZero() && next < bound.UnixMilli():
			w.stop, finished = w.boundReason(), true
		case !more:
			w.stop, finished = ListStopFirstPost, true
		default:
			st.Cursor = listCursor(next, edge(res, next, edgeIDs))
		}
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("account: checkpoint save failed", "err", err)
		}
	}
	c["pages_fetched"] += int64(st.Pages)
	if finished {
		return s.finish(ctx, t, sc, st, w, c)
	}
	// Yield: continue the stream in a new segment.
	next := task.Continuation(t, st.Cursor)
	ns := next.GetAccount()
	ns.UserId, ns.SecUid, ns.Username, ns.PostsSeen = sc.UserId, sc.SecUid, sc.Username, st.PostsSeen
	ns.Watermark, ns.AccountSeenAt, ns.PageSeenAt = sc.Watermark, sc.AccountSeenAt, sc.PageSeenAt
	if st.NewestSeenUnix > 0 {
		ns.NewestSeenAt = timestamppb.New(time.Unix(st.NewestSeenUnix, 0))
	}
	next.Mode = w.mode
	metrics.TaskYields.WithLabelValues("account").Inc()
	return Outcome{Result: Continue, Next: next, Counters: c}
}

func (s *AccountStage) pause() time.Duration {
	if s.SoftPause > 0 {
		return s.SoftPause
	}
	return softPause
}

// cursor reads a walk's saved cursor: the list's cursor, a time in
// milliseconds (the next second's at the start: posts older than it are
// served), and the ids already read at its second, which a cursor that
// keeps its second serves again (listCursor).
func (s *AccountStage) cursor(saved string) (int64, []int64) {
	ms, ids, _ := strings.Cut(saved, "~")
	v, err := strconv.ParseInt(ms, 10, 64)
	if err != nil || v <= 0 {
		return s.D.now().Add(time.Second).UnixMilli(), nil
	}
	var edge []int64
	for _, f := range strings.Split(ids, ",") {
		if id, err := strconv.ParseInt(f, 10, 64); err == nil {
			edge = append(edge, id)
		}
	}
	return v, edge
}

// listCursor is the saved form of a walk's cursor: "<ms>" or
// "<ms>~<id>,<id>" (the edge's ids). It is opaque to everything else.
func listCursor(ms int64, edge []int64) string {
	out := strconv.FormatInt(ms, 10)
	for i, id := range edge {
		if i == 0 {
			out += "~"
		} else {
			out += ","
		}
		out += strconv.FormatInt(id, 10)
	}
	return out
}

// due reports whether a refresh last done at seen is due after every.
func (d *Deps) due(seen *timestamppb.Timestamp, every time.Duration) bool {
	return seen == nil || every <= 0 || d.now().Sub(seen.AsTime()) >= every
}

// profile reads the profile page when the stream starts and it is needed,
// and emits the account it holds. stop ends the task with out.
func (s *AccountStage) profile(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, id *session.Identity, c map[string]int64) (Outcome, bool) {
	d := s.D
	f := d.Cfg.TT.Freshness
	due := d.due(sc.AccountSeenAt, f.AccountRefresh) || d.due(sc.PageSeenAt, f.PageRefresh)
	if !due && sc.SecUid != "" && id.Bootstrapped() {
		return Outcome{}, false
	}
	if sc.Username == "" {
		if sc.SecUid != "" && id.Bootstrapped() {
			return Outcome{}, false // the list's author names it
		}
		return Outcome{Result: Fail, Reason: "account unresolved: no username to load its page", Counters: c}, true
	}
	pl, err := d.Loader.Load(ctx, id, profileURL(id, sc.Username))
	if errors.Is(err, session.ErrPaced) {
		if id.Bootstrapped() && sc.SecUid != "" {
			c["profile_deferred"]++ // only the refresh needed it
			return Outcome{}, false
		}
		return pacedRetry(t, "", err, c), true
	}
	d.observeLoad(pl, err, t.Lane)
	c["requests"]++
	if err != nil {
		c["transport_errors"]++
		return retry(t, "", "30s", "page load: "+err.Error(), c), true
	}
	c["wire_bytes"] += pl.Bytes
	switch pl.Class {
	case detect.OK:
	case detect.ContentUnavailable:
		// A renamed account's old username, or one gone: the list (by its
		// secUid) tells which.
		c["profile_unavailable"]++
		if sc.SecUid != "" && id.Bootstrapped() {
			return Outcome{}, false
		}
		if t.Attempt+1 < confirmUnavailable {
			return retry(t, "", "30s", pl.Reason, c), true
		}
		return s.unavailable(ctx, t, sc, ttv1.AccountStatus_ACCOUNT_STATUS_UNAVAILABLE, c), true
	default:
		return classOutcome(t, pl.Class, pl.Reason, "", c), true
	}
	res, rep := parse.ProfilePage(pl.HTML, parse.Options{AuthorKey: d.Emit.Key, Query: "profile_page"})
	for k, n := range res.Dropped {
		c["dropped_"+k] += int64(n)
	}
	if res.Account == nil {
		if res.StatusCode != 0 {
			// No account under this username: renamed (the list, by its
			// secUid, names the new one) or gone.
			c["profile_unavailable"]++
			if sc.SecUid != "" && id.Bootstrapped() {
				return Outcome{}, false
			}
			return s.unavailable(ctx, t, sc, res.Status, c), true
		}
		// A client-rendered shell without the account (T1): the page's
		// values still bootstrap the identity, and the list carries the
		// account's names.
		metrics.ParseTotal.WithLabelValues("profile_page", strconv.Itoa(parse.Version), "shell").Inc()
		c["profile_shells"]++
		if sc.SecUid != "" && id.Bootstrapped() {
			return Outcome{}, false
		}
		return classOutcome(t, detect.SoftError, "profile page without the account: "+fmt.Sprint(rep.Missing), "", c), true
	}
	metrics.ParseTotal.WithLabelValues("profile_page", strconv.Itoa(parse.Version), "ok").Inc()
	a := res.Account
	sc.UserId, sc.SecUid, sc.Username = a.UserId, a.SecUid, a.Username
	b := d.Producer.NewBatch()
	src := emit.Source{JobID: t.JobId, Query: "profile_page", ParserVersion: parse.Version, Mode: t.Mode}
	if err := d.Emit.Account(ctx, b, src, a); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	if _, err := d.media(ctx, b, t, src, res.Media, false, fetcherOf(id)); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", "produce: "+err.Error(), c), true
	}
	now := timestamppb.New(d.now())
	sc.AccountSeenAt, sc.PageSeenAt = now, now
	c["accounts"]++
	c["account_pages"]++
	return Outcome{}, false
}

// confirmUnavailable is how many attempts a profile page that is not there
// takes before the account is marked unavailable without a list to ask: a
// later attempt runs on another identity and exit.
const confirmUnavailable = 3

// bootstrap gives an identity without page values (a continuation's, or a
// retry's) its values, from the account's own page.
func (s *AccountStage) bootstrap(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, id *session.Identity, cursor string, c map[string]int64) (Outcome, bool) {
	d := s.D
	if sc.Username == "" {
		return Outcome{Result: Fail, Reason: "account unresolved: no username to load its page", Counters: c}, true
	}
	pl, err := d.Loader.Bootstrap(ctx, id, profileURL(id, sc.Username))
	if errors.Is(err, session.ErrPaced) || errors.Is(err, session.ErrExitPaused) {
		return pacedRetry(t, cursor, err, c), true
	}
	d.observeLoad(pl, err, t.Lane)
	c["requests"]++
	if pl != nil {
		c["wire_bytes"] += pl.Bytes
	}
	if err != nil {
		if pl != nil && pl.Class != detect.OK {
			return classOutcome(t, pl.Class, pl.Reason, cursor, c), true
		}
		c["transport_errors"]++
		return retry(t, cursor, "30s", "bootstrap: "+err.Error(), c), true
	}
	return Outcome{}, false
}

// listAccount emits the list's author as the account, and resolves the
// scope's keys from it (a secUid reference names no user id or username).
func (s *AccountStage) listAccount(ctx context.Context, b *kafka.Batch, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, res parse.PostListResult, f fetcher, c map[string]int64) (Outcome, bool) {
	d := s.D
	a := res.Account
	if a.SecUid != "" && a.SecUid != sc.SecUid {
		// Not this account's list: never store another account's posts
		// under it.
		return Outcome{Result: Fail, Reason: "post list of another account", Counters: c}, true
	}
	if sc.UserId == 0 {
		sc.UserId = a.UserId
	}
	if a.Username != "" {
		sc.Username = a.Username
	}
	src := emit.Source{JobID: t.JobId, Query: listQuery, ParserVersion: parse.Version, Mode: t.Mode}
	if err := d.Emit.Account(ctx, b, src, a); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	if _, err := d.media(ctx, b, t, src, res.AccountMedia, false, f); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	c["accounts"]++
	return Outcome{}, false
}

// plan sets the walk's bounds from the task's mode: a backfill (FULL) reads
// to the age window; an incremental check back to the newest stored post
// less the overlap (or, never checked, as a backfill); a metrics walk
// (REFRESH) to its age window whatever is stored.
func (s *AccountStage) plan(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope) (walk, error) {
	d := s.D
	w := walk{mode: t.Mode, cutoff: task.MaxAgeCutoff(t.Limits, d.now())}
	if w.mode != commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL {
		if w.mode == commonv1.CrawlMode_CRAWL_MODE_UNSPECIFIED {
			w.mode = commonv1.CrawlMode_CRAWL_MODE_FULL
		}
		return w, nil
	}
	wm := sc.Watermark.AsTime()
	if sc.Watermark == nil {
		stored, ok, err := d.listWatermark(ctx, sc.UserId)
		if err != nil {
			return w, err
		}
		if !ok {
			w.mode = commonv1.CrawlMode_CRAWL_MODE_FULL // never checked: a backfill
			return w, nil
		}
		wm = stored.Add(-d.Cfg.Scheduler.IncrementalOverlap)
		sc.Watermark = timestamppb.New(wm)
	}
	w.stopBefore = wm
	return w, nil
}

func (w walk) boundReason() string {
	if !w.stopBefore.IsZero() && !w.stopBefore.Before(w.cutoff) {
		return ListStopWatermark
	}
	return ListStopAge
}

// posts emits a list page's posts within the age window and returns the
// post tasks of those whose comments are due.
func (s *AccountStage) posts(ctx context.Context, b *kafka.Batch, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, st *checkpoint.State, w *walk,
	src emit.Source, res parse.PostListResult, edgeIDs []int64, f fetcher, c map[string]int64) ([]*tkstaskv1.Task, Outcome, bool) {
	d := s.D
	var children []*tkstaskv1.Task
	repeated := map[int64]bool{}
	for _, id := range edgeIDs {
		repeated[id] = true
	}
	for _, pb := range res.Posts {
		p := pb.Post
		if repeated[p.PostId] {
			c["posts_repeated"]++ // the previous page's, at the cursor's second
			continue
		}
		if p.UserId != 0 && p.UserId != sc.UserId {
			c["posts_foreign"]++ // never under this account
			continue
		}
		created := p.CreatedAt.AsTime()
		// Every post on a page read is refreshed (its counts came with the
		// page), also past an incremental check's watermark, which only
		// ends the walk; the age window is a limit on what is kept.
		if !w.cutoff.IsZero() && created.Before(w.cutoff) {
			c["posts_old"]++
			continue
		}
		if lim := t.Limits.GetMaxPosts(); lim > 0 && st.PostsSeen >= lim {
			w.stop = ListStopMaxPosts
			break
		}
		st.PostsSeen++
		if u := created.Unix(); u > st.NewestSeenUnix {
			st.NewestSeenUnix = u
		}
		if w.oldest.IsZero() || created.Before(w.oldest) {
			w.oldest = created
		}
		if err := d.Emit.Post(ctx, b, src, p); err != nil {
			return nil, retry(t, st.Cursor, "30s", err.Error(), c), true
		}
		if err := d.Emit.Music(ctx, b, src, pb.Music); err != nil {
			return nil, retry(t, st.Cursor, "30s", err.Error(), c), true
		}
		n, err := d.media(ctx, b, t, src, pb.Media, false, f)
		if err != nil {
			return nil, retry(t, st.Cursor, "30s", err.Error(), c), true
		}
		c["posts"]++
		c["media_queued"] += n
		child, state := s.commentsTask(ctx, t, sc, w.mode, p)
		if child == nil {
			_, _ = d.Jobs.SetPost(ctx, t.JobId, p.PostId, created.Unix(), p.GetCounts().GetComments(), state)
			if state == jobs.PostSkipped && t.Limits.GetIncludeComments() {
				c["comments_not_due"]++
			}
			continue
		}
		// Not again when this job already collected the post.
		if collected, _ := d.Jobs.SetPost(ctx, t.JobId, p.PostId, created.Unix(), p.GetCounts().GetComments(), jobs.PostPending); !collected {
			children = append(children, child)
		}
	}
	return children, Outcome{}, false
}

// commentsTask decides whether a post's comments must be read (ADR 0007
// §4, §5): never read, or its comment count went up since the last read
// and its comment window allows another read. A post past the last window
// is read again only on a metrics walk (REFRESH) that sees its count grow.
func (s *AccountStage) commentsTask(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, mode commonv1.CrawlMode, p *ttv1.Post) (*tkstaskv1.Task, string) {
	d := s.D
	if !t.Limits.GetIncludeComments() {
		return nil, jobs.PostSkipped
	}
	count := p.GetCounts().GetComments()
	if count == 0 || p.GetCommentsDisabled() {
		return nil, jobs.PostComplete // nothing to collect
	}
	prev, read := d.Ckpt.GetPost(ctx, p.PostId)
	if read {
		if count <= prev.Reported && prev.ThreadsComplete && d.Ckpt.Incomplete(ctx, p.PostId) == "" {
			return nil, jobs.PostSkipped
		}
		if !commentsDue(d.Cfg.TT.Freshness.CommentWindows, d.now(), p.CreatedAt.AsTime(), prev.ReadAt, mode) {
			return nil, jobs.PostSkipped
		}
	}
	// The count the read is planned on: the post is read again once its
	// count goes past it.
	scope := &tkstaskv1.PostScope{PostId: p.PostId, UserId: p.UserId, CreatedAt: p.CreatedAt, CommentCountSeen: count,
		Username: sc.Username, Photo: p.Kind == ttv1.PostKind_POST_KIND_PHOTO}
	// Ranked comments (T13) are re-read to the cap on every read; a newest
	// first order would read only what is new (tt.comments.order).
	cmode := commonv1.CrawlMode_CRAWL_MODE_FULL
	if read && d.Cfg.TT.Comments.Order == config.CommentsNewest {
		cmode = commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL
	}
	return task.NewPost(t, cmode, scope), ""
}

// commentsDue applies the comment windows to a post read before (at
// readUnix) whose count went up.
func commentsDue(windows []config.CommentWindow, now, created time.Time, readUnix int64, mode commonv1.CrawlMode) bool {
	age := now.Sub(created)
	for _, w := range windows {
		if age < w.MaxAge {
			return readUnix == 0 || now.Sub(time.Unix(readUnix, 0)) >= w.Interval
		}
	}
	// Past the last window: only a metrics walk reads it again.
	return len(windows) == 0 || mode == commonv1.CrawlMode_CRAWL_MODE_REFRESH
}

// finish records the end of a list walk.
func (s *AccountStage) finish(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, st checkpoint.State, w walk, c map[string]int64) Outcome {
	d := s.D
	state, truncated := "complete", ""
	if w.stop == ListStopStuck {
		state, truncated = "truncated", w.stop
		c["lists_truncated"]++
	}
	// An incremental check cut short did not reach its old watermark: the
	// posts between are unread, so the watermark stays for the next check.
	var newest time.Time
	if st.NewestSeenUnix > 0 && (truncated == "" || w.mode != commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL) {
		newest = time.Unix(st.NewestSeenUnix, 0).UTC()
	}
	if err := d.saveListWatermark(ctx, sc.UserId, newest, w.oldest, state, truncated, w.mode == commonv1.CrawlMode_CRAWL_MODE_FULL); err != nil {
		return retry(t, st.Cursor, "30s", "watermark: "+err.Error(), c)
	}
	metrics.ListStops.WithLabelValues(modeName(w.mode), w.stop).Inc()
	c["lists_"+w.stop]++
	return done(c)
}

// unavailable publishes the account's status when the list or the page
// says it is private, gone or banned, and ends the task.
func (s *AccountStage) unavailable(ctx context.Context, t *tkstaskv1.Task, sc *tkstaskv1.AccountScope, status ttv1.AccountStatus, c map[string]int64) Outcome {
	d := s.D
	c["accounts_"+statusName(status)]++
	if sc.UserId == 0 || t.Seq > 0 {
		return done(c)
	}
	b := d.Producer.NewBatch()
	a := &ttv1.Account{UserId: sc.UserId, SecUid: sc.SecUid, Username: sc.Username, Status: status}
	if err := d.Emit.Account(ctx, b, emit.Source{JobID: t.JobId, Query: listQuery, Mode: t.Mode}, a); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", "produce: "+err.Error(), c)
	}
	return done(c)
}

// statusOf maps an unavailable answer's statusCode to the account's status.
func statusOf(errs []detect.GQLError) ttv1.AccountStatus {
	for _, e := range errs {
		switch e.Code {
		case ttdetect.StatusPrivateAccount:
			return ttv1.AccountStatus_ACCOUNT_STATUS_PRIVATE
		case ttdetect.StatusUserBanned:
			return ttv1.AccountStatus_ACCOUNT_STATUS_BANNED
		}
	}
	return ttv1.AccountStatus_ACCOUNT_STATUS_UNAVAILABLE
}

func statusName(s ttv1.AccountStatus) string {
	switch s {
	case ttv1.AccountStatus_ACCOUNT_STATUS_PRIVATE:
		return "private"
	case ttv1.AccountStatus_ACCOUNT_STATUS_BANNED:
		return "banned"
	default:
		return "unavailable"
	}
}

func modeName(m commonv1.CrawlMode) string {
	switch m {
	case commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL:
		return "incremental"
	case commonv1.CrawlMode_CRAWL_MODE_REFRESH:
		return "refresh"
	default:
		return "full"
	}
}

// edge returns the ids read at the next cursor's second: a cursor that keeps
// its second (client.NextCreatorCursor) serves them again. prev are the ids
// the page was asked to skip, kept when the second is the same.
func edge(res parse.PostListResult, next int64, prev []int64) []int64 {
	if next%1000 != 999 {
		return nil
	}
	var out []int64
	if len(res.Posts) == 0 || allAtSecond(res, next/1000) {
		out = append(out, prev...)
	}
	for _, pb := range res.Posts {
		if pb.Post.CreatedAt.AsTime().Unix() == next/1000 {
			out = append(out, pb.Post.PostId)
		}
	}
	return out
}

// allAtCursor reports a page whose every post is at the cursor's second:
// TikTok answers a window before the cursor, and after a gap a page can
// hold only the post the cursor was taken from (C3 §4).
func allAtCursor(res parse.PostListResult, cursor int64) bool {
	return len(res.Posts) > 0 && allAtSecond(res, cursor/1000)
}

func allAtSecond(res parse.PostListResult, sec int64) bool {
	for _, pb := range res.Posts {
		if pb.Post.CreatedAt.AsTime().Unix() != sec {
			return false
		}
	}
	return true
}

func profileURL(id *session.Identity, username string) string {
	u, _ := id.Base.Parse(task.ProfilePath(username))
	return u.String()
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
