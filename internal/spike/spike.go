// Package spike is the Phase 0 measurement harness. It crawls a sample of
// real pages logged out, with no Kafka or database, and records what
// Facebook actually serves: bootstrap success, timeline depth, comment
// completeness per ordering, block rates per proxy tier, bytes per query,
// cursor portability and CDN behaviour. Report turns the JSONL into the
// go/no-go summary.
package spike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Options control a run.
type Options struct {
	Tier             string
	MaxTimelinePages int      // per page
	PostsPerPage     int      // posts sampled for comment measurement
	MaxCommentPages  int      // per post and ordering
	ThreadsPerPost   int      // reply threads followed per post
	Orderings        []string // registry names, e.g. comments, comments_newest, comments_relevant
	MediaSamples     int      // CDN downloads per page
	// MaxPostComments prefers posts with at most this many comments, so a
	// measurement can finish within the limits above (0 = busiest first).
	MaxPostComments int64
	// ComparePosts is how many sampled posts get the comparison orderings
	// (Orderings[1:]); 0 = all of them.
	ComparePosts int
	Concurrency  int           // pages in parallel
	PageTimeout  time.Duration // per page

	// Recorded in the run header so reports can project cost and compare runs.
	RPS          float64 // per-identity pacing
	CostPerGBUSD float64 // proxy price of the tier
	Version      string  // fbs-spike build
}

// Record is one JSONL line.
type Record struct {
	// run | page | request | bootstrap | timeline | comments | portability | media | page_done
	Type string    `json:"type"`
	At   time.Time `json:"at"`
	Page string    `json:"page,omitempty"`
	Tier string    `json:"tier"`

	Labels map[string]string `json:"labels,omitempty"` // page: strata from the pages file

	// run header
	RPS          float64 `json:"rps,omitempty"`
	Concurrency  int     `json:"concurrency,omitempty"`
	CostPerGBUSD float64 `json:"cost_per_gb_usd,omitempty"`
	Version      string  `json:"version,omitempty"`

	Query    string `json:"query,omitempty"`
	Class    string `json:"class,omitempty"`
	Reason   string `json:"reason,omitempty"`
	Status   int    `json:"status,omitempty"`
	Bytes    int    `json:"bytes,omitempty"`      // decoded body
	Wire     int64  `json:"wire_bytes,omitempty"` // on the wire, in + out (what proxies bill)
	MS       int64  `json:"ms,omitempty"`
	Identity string `json:"identity,omitempty"`

	OK          *bool           `json:"ok,omitempty"`
	TokensFound map[string]bool `json:"tokens_found,omitempty"`
	Consent     bool            `json:"consent,omitempty"`

	Posts       int     `json:"posts,omitempty"`
	DaysCovered float64 `json:"days_covered,omitempty"`
	EndReason   string  `json:"end_reason,omitempty"` // end | wall | max_pages | error
	PagesRead   int     `json:"pages_read,omitempty"`

	PostID    int64  `json:"post_id,omitempty"`
	Ordering  string `json:"ordering,omitempty"`
	Reported  int64  `json:"reported,omitempty"`
	TopLevel  int    `json:"top_level,omitempty"`
	Replies   int    `json:"replies,omitempty"`
	Collected int    `json:"collected,omitempty"`
	// Capped means the spike's own limits (--comment-pages,
	// --threads-per-post) stopped the measurement, not Facebook.
	Capped bool `json:"capped,omitempty"`

	ExpiresInH float64  `json:"expires_in_h,omitempty"`
	MissingOpt []string `json:"missing_optional,omitempty"`
}

// Runner executes the spike.
type Runner struct {
	Pool   *session.Pool
	Loader *session.Loader
	GQL    *graphql.Client
	HTTP   *http.Client // direct CDN downloads
	Out    io.Writer
	Opt    Options
	// Progress, when set, is called after each page with the number of
	// pages finished so far.
	Progress func(done, total int, page string)
	// FailDir, when set, is where Capture keeps a scrubbed copy of each
	// response that was not OK, so the bundle shows what Facebook said.
	FailDir string

	mu sync.Mutex
}

func (r *Runner) emit(rec Record) {
	rec.At = time.Now().UTC()
	rec.Tier = r.Opt.Tier
	rec.Reason = Redact(rec.Reason)
	b, _ := json.Marshal(rec)
	r.mu.Lock()
	_, _ = r.Out.Write(append(b, '\n'))
	r.mu.Unlock()
}

func boolp(b bool) *bool { return &b }

// tokensFound reports which page tokens were found, without their values.
func tokensFound(tok map[string]string) map[string]bool {
	return map[string]bool{"lsd": tok["lsd"] != "", "js_datr": tok["js_datr"] != "", "spin_r": tok["__spin_r"] != "",
		"hsi": tok["__hsi"] != "", "rev": tok["__rev"] != "", "haste_session": tok["__hs"] != "", "dtsg": tok["fb_dtsg"] != ""}
}

// Run writes a run header, then measures every page. Each page ends with a
// page_done record (see Done), whatever its outcome. Run stops starting new
// pages once ctx is cancelled.
func (r *Runner) Run(ctx context.Context, pages []PageRef) {
	r.emit(Record{Type: "run", RPS: r.Opt.RPS, Concurrency: r.Opt.Concurrency,
		CostPerGBUSD: r.Opt.CostPerGBUSD, Version: r.Opt.Version})
	sem := make(chan struct{}, max(1, r.Opt.Concurrency))
	var wg sync.WaitGroup
	var finished atomic.Int64
	for _, p := range pages {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(p PageRef) {
			defer wg.Done()
			defer func() { <-sem }()
			pctx, cancel := context.WithTimeout(ctx, r.Opt.PageTimeout)
			defer cancel()
			if err := r.page(pctx, p); err != nil {
				slog.Warn("spike: page failed", "page", p.Ref, "err", Redact(err.Error()))
			}
			if r.Progress != nil && ctx.Err() == nil {
				r.Progress(int(finished.Add(1)), len(pages), p.Ref)
			}
		}(p)
	}
	wg.Wait()
}

func (r *Runner) do(ctx context.Context, page string, id *session.Identity, req graphql.Request) (*graphql.Response, error) {
	resp, err := r.GQL.Do(ctx, id, req)
	rec := Record{Type: "request", Page: page, Query: req.Query, Identity: id.ID}
	if err != nil {
		rec.Class, rec.Reason = detect.Transport.String(), err.Error()
		r.emit(rec)
		return nil, err
	}
	rec.Class, rec.Reason, rec.Status, rec.Bytes, rec.MS = resp.Class.String(), resp.Reason, resp.Status, len(resp.Body), resp.Elapsed.Milliseconds()
	rec.Wire = resp.BytesIn + resp.BytesOut
	r.emit(rec)
	return resp, nil
}

// loadAttempts is how many identities a page load is tried on.
const loadAttempts = 2

// loadPage loads ref's page on an identity of the tier, and returns the
// identity (for the caller to release) with the bootstrap record of the
// attempt that answered. A load that got no response, a dead or stalled
// exit, retires its identity so that no other page lands on it, and is tried
// once more on a fresh one; failed, when set, gets that attempt's record.
// On an error there is no identity to release.
func (r *Runner) loadPage(ctx context.Context, ref string, failed func(Record)) (*session.Identity, *session.PageLoad, Record, error) {
	for attempt := 1; ; attempt++ {
		rec := Record{Type: "bootstrap", Page: ref}
		id, err := r.Pool.Acquire(ctx, r.Opt.Tier)
		if err != nil {
			return nil, nil, rec, err
		}
		u, _ := id.Base.Parse(task.RefPath(ref))
		start := time.Now()
		pl, err := r.Loader.Load(ctx, id, u.String())
		rec.Identity, rec.MS = id.ID, time.Since(start).Milliseconds()
		if err == nil {
			return id, pl, rec, nil
		}
		if !errors.Is(err, session.ErrTransport) || ctx.Err() != nil {
			r.Pool.Release(id)
			return nil, nil, rec, err
		}
		r.Pool.Retire(id, "transport")
		if attempt == loadAttempts {
			return nil, nil, rec, err
		}
		if failed != nil {
			rec.OK, rec.Reason = boolp(false), err.Error()
			failed(rec)
		}
		slog.Warn("spike: page load failed; trying a fresh identity", "page", ref, "err", Redact(err.Error()))
	}
}

func (r *Runner) page(ctx context.Context, p PageRef) error {
	ref, err := task.NormalizeRef(p.Ref)
	if err != nil {
		return err
	}
	r.emit(Record{Type: "page", Page: ref, Labels: p.Labels})
	defer func() {
		// A page interrupted by shutdown is measured again on resume; one
		// that hit PageTimeout counts as measured.
		if !errors.Is(ctx.Err(), context.Canceled) {
			r.emit(Record{Type: "page_done", Page: ref})
		}
	}()
	// A failed attempt counts as a failed bootstrap: the tier's exits are
	// part of what is measured.
	id, pl, brec, err := r.loadPage(ctx, ref, r.emit)
	if err != nil {
		brec.OK, brec.Reason = boolp(false), err.Error()
		r.emit(brec)
		return err
	}
	defer r.Pool.Release(id)
	tok := htmlx.ExtractTokens(pl.HTML)
	brec.OK = boolp(pl.Class == detect.OK && id.Bootstrapped())
	brec.Class, brec.Reason, brec.Status, brec.Bytes, brec.Wire = pl.Class.String(), pl.Reason, pl.Status, len(pl.HTML), pl.Bytes
	brec.Consent = htmlx.ConsentWall(pl.HTML)
	brec.TokensFound = tokensFound(tok)
	r.emit(brec)
	if !*brec.OK {
		return nil
	}
	pg, rep := parse.Page(pl.HTML, 0, task.RefID(ref))
	if pg == nil {
		r.emit(Record{Type: "timeline", Page: ref, EndReason: "no_page_id", MissingOpt: rep.Missing})
		return nil
	}
	pageID := pg.PageId

	// Timeline depth.
	var posts []*parse.PostBundle
	var cursors []string
	cursor, pagesRead, end := "", 0, "end"
	var newest, oldest time.Time
	for pagesRead < r.Opt.MaxTimelinePages {
		vars := map[string]any{"page_id": strconv.FormatInt(pageID, 10), "count": 10}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		resp, err := r.do(ctx, ref, id, graphql.Request{Query: "timeline", Vars: vars})
		if err != nil {
			end = "error"
			break
		}
		if resp.Class == detect.LoginWall {
			end = "wall"
			break
		}
		if resp.Class != detect.OK {
			end = "blocked:" + resp.Class.String()
			break
		}
		pagesRead++
		tl, rep := parse.Timeline(resp.Parts, pageID, id.Base.String())
		if !rep.OK() {
			end = "drift"
			break
		}
		if tl.NoFeed {
			end = "no_feed" // the page id has no timeline
			break
		}
		for i := range tl.Posts {
			b := tl.Posts[i]
			posts = append(posts, &b)
			c := b.Post.CreatedAt.AsTime()
			if newest.IsZero() || c.After(newest) {
				newest = c
			}
			if oldest.IsZero() || c.Before(oldest) {
				oldest = c
			}
		}
		if !tl.PageInfo.HasNext || tl.PageInfo.EndCursor == "" {
			break
		}
		cursor = tl.PageInfo.EndCursor
		cursors = append(cursors, cursor)
	}
	if pagesRead == r.Opt.MaxTimelinePages {
		end = "max_pages"
	}
	days := 0.0
	if !oldest.IsZero() {
		days = time.Since(oldest).Hours() / 24
	}
	r.emit(Record{Type: "timeline", Page: ref, Posts: len(posts), DaysCovered: days, EndReason: end, PagesRead: pagesRead})

	// Cursor portability: replay a mid-stream cursor on a fresh identity.
	if len(cursors) > 0 {
		u, _ := id.Base.Parse(task.RefPath(ref))
		r.portability(ctx, ref, pageID, u.String(), cursors[len(cursors)/2])
	}

	// Comment completeness: the first ordering on every sampled post, the
	// others (for comparison) on the first ComparePosts of them.
	for i, b := range SamplePosts(posts, r.Opt.PostsPerPage, r.Opt.MaxPostComments) {
		p := b.Post
		for j, ord := range r.Opt.Orderings {
			if j > 0 && r.Opt.ComparePosts > 0 && i >= r.Opt.ComparePosts {
				break
			}
			r.comments(ctx, ref, id, pageID, p.PostId, p.FeedbackId, p.GetCommentCount(), ord)
		}
	}

	// CDN behaviour from datacenter egress.
	n := 0
	for _, b := range posts {
		for _, m := range b.Media {
			if n >= r.Opt.MediaSamples {
				break
			}
			n++
			r.media(ctx, ref, m.SourceUrl, m.SourceExpiresAt.AsTime())
		}
	}
	return nil
}

func (r *Runner) portability(ctx context.Context, ref string, pageID int64, pageURL, cursor string) {
	id, err := r.Pool.Acquire(ctx, r.Opt.Tier)
	if err != nil {
		return
	}
	defer r.Pool.Release(id)
	if _, err := r.Loader.Bootstrap(ctx, id, pageURL); err != nil {
		r.emit(Record{Type: "portability", Page: ref, OK: boolp(false), Reason: "bootstrap: " + err.Error()})
		return
	}
	resp, err := r.do(ctx, ref, id, graphql.Request{Query: "timeline",
		Vars: map[string]any{"page_id": strconv.FormatInt(pageID, 10), "count": 10, "cursor": cursor}})
	ok := err == nil && resp.Class == detect.OK
	posts := 0
	if ok {
		tl, _ := parse.Timeline(resp.Parts, pageID, id.Base.String())
		posts = len(tl.Posts)
		ok = posts > 0
	}
	rec := Record{Type: "portability", Page: ref, OK: boolp(ok), Posts: posts}
	if resp != nil {
		rec.Class = resp.Class.String()
	}
	r.emit(rec)
}

func (r *Runner) comments(ctx context.Context, ref string, id *session.Identity, pageID, postID int64, feedbackID string, reported int64, ordering string) {
	seen := map[int64]bool{}
	top, replies := 0, 0
	var threads []parse.ThreadRef
	cursor, pages, end := "", 0, "end"
	for pages < r.Opt.MaxCommentPages {
		vars := map[string]any{"feedback_id": feedbackID}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		resp, err := r.do(ctx, ref, id, graphql.Request{Query: ordering, Vars: vars})
		if err != nil {
			end = "error"
			break
		}
		if resp.Class == detect.LoginWall {
			end = "wall"
			break
		}
		if resp.Class != detect.OK {
			end = "blocked:" + resp.Class.String()
			break
		}
		pages++
		res, rep := parse.Comments(resp.Parts, parse.CommentContext{PostID: postID, PageID: pageID})
		if !rep.OK() {
			end = "drift"
			break
		}
		for _, c := range res.Comments {
			if !seen[c.Comment.CommentId] {
				seen[c.Comment.CommentId] = true
				if c.Comment.Depth == 0 {
					top++
				} else {
					replies++
				}
			}
		}
		threads = append(threads, res.Threads...)
		if !res.PageInfo.HasNext || res.PageInfo.EndCursor == "" {
			break
		}
		cursor = res.PageInfo.EndCursor
	}
	if pages == r.Opt.MaxCommentPages {
		end = "max_pages"
	}
	// Follow reply threads (nested ones included) up to ThreadsPerPost.
	capped := pages == r.Opt.MaxCommentPages
	for followed := 0; len(threads) > 0; followed++ {
		if followed >= r.Opt.ThreadsPerPost {
			capped = true
			break
		}
		th := threads[0]
		threads = threads[1:]
		c := ""
		for p := 0; ; p++ {
			if p >= r.Opt.MaxCommentPages {
				capped = true
				break
			}
			vars := map[string]any{"feedback_id": th.FeedbackID, "expansion_token": th.ExpansionToken}
			if c != "" {
				vars["cursor"] = c
			}
			resp, err := r.do(ctx, ref, id, graphql.Request{Query: "replies", Vars: vars})
			if err != nil || resp.Class != detect.OK {
				break
			}
			res, _ := parse.Comments(resp.Parts, parse.CommentContext{PostID: postID, PageID: pageID,
				BaseDepth: th.Depth + 1, ParentCommentID: th.CommentID, RootCommentID: th.RootCommentID, Replies: true})
			for _, x := range res.Comments {
				if !seen[x.Comment.CommentId] {
					seen[x.Comment.CommentId] = true
					replies++
				}
			}
			threads = append(threads, res.Threads...)
			if !res.PageInfo.HasNext || res.PageInfo.EndCursor == "" {
				break
			}
			c = res.PageInfo.EndCursor
		}
	}
	r.emit(Record{Type: "comments", Page: ref, PostID: postID, Ordering: ordering, Reported: reported,
		TopLevel: top, Replies: replies, Collected: len(seen), EndReason: end, PagesRead: pages, Capped: capped})
}

func (r *Runner) media(ctx context.Context, ref, rawURL string, expires time.Time) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return
	}
	start := time.Now()
	resp, err := r.HTTP.Do(req)
	rec := Record{Type: "media", Page: ref, MS: time.Since(start).Milliseconds()}
	if !expires.IsZero() && expires.Unix() > 0 {
		rec.ExpiresInH = time.Until(expires).Hours()
	}
	if err != nil {
		rec.OK, rec.Reason = boolp(false), err.Error()
		r.emit(rec)
		return
	}
	n, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, 50<<20))
	_ = resp.Body.Close()
	rec.Status, rec.Bytes, rec.OK = resp.StatusCode, int(n), boolp(resp.StatusCode == 200)
	r.emit(rec)
}

// SamplePosts picks up to n posts with comments for the completeness
// measurement: the busiest with at most maxComments comments first, then,
// if those are too few, the smallest of the bigger ones. maxComments 0
// means the busiest posts.
func SamplePosts(posts []*parse.PostBundle, n int, maxComments int64) []*parse.PostBundle {
	var fit, big []*parse.PostBundle
	for _, b := range posts {
		c := b.Post.GetCommentCount()
		switch {
		case c == 0:
		case maxComments <= 0 || c <= maxComments:
			fit = append(fit, b)
		default:
			big = append(big, b)
		}
	}
	sort.SliceStable(fit, func(i, j int) bool { return fit[i].Post.GetCommentCount() > fit[j].Post.GetCommentCount() })
	sort.SliceStable(big, func(i, j int) bool { return big[i].Post.GetCommentCount() < big[j].Post.GetCommentCount() })
	out := append(fit, big...)
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// ErrNoPages is returned when the input is empty.
var ErrNoPages = fmt.Errorf("spike: no pages")
