package fbtest

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// crawlPage walks a whole page through the client layer and returns the
// collected post ids and comment ids per post.
func crawlPage(t *testing.T, env *Env, id *session.Identity, p *fakefb.Page) (map[int64]map[int64]bool, int) {
	t.Helper()
	ctx := context.Background()
	pl, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	page, rep := parse.Page(pl.HTML, 0, 0)
	if !rep.OK() || page.PageId != p.ID || page.Name != p.Name {
		t.Fatalf("page parse: %+v %+v", page, rep)
	}
	if page.FollowersCount == nil || page.Categories[0] != p.Category {
		t.Fatalf("page details missing: %+v", page)
	}

	got := map[int64]map[int64]bool{}
	cursor := ""
	pagesFetched := 0
	for {
		vars := map[string]any{"count": 3, "page_id": p.ID}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: vars})
		if err != nil || resp.Class != detect.OK {
			t.Fatalf("timeline: %v %v %s", err, resp.Class, resp.Reason)
		}
		pagesFetched++
		tl, rep := parse.Timeline(resp.Parts, p.ID, env.HTTP.URL)
		if !rep.OK() {
			t.Fatalf("timeline drift: %v", rep.Missing)
		}
		for _, b := range tl.Posts {
			cs := map[int64]bool{}
			got[b.Post.PostId] = cs
			if b.Post.FeedbackId == "" || b.Post.CommentCount == nil {
				t.Fatalf("post %d missing feedback: %+v", b.Post.PostId, b.Post)
			}
			collectComments(t, env, id, p.ID, b.Post.PostId, b.Post.FeedbackId, cs)
		}
		if !tl.PageInfo.HasNext {
			break
		}
		cursor = tl.PageInfo.EndCursor
	}
	return got, pagesFetched
}

func collectComments(t *testing.T, env *Env, id *session.Identity, pageID, postID int64, feedbackID string, out map[int64]bool) {
	ctx := context.Background()
	var threads []parse.ThreadRef
	cursor := ""
	for {
		vars := map[string]any{"feedback_id": feedbackID}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "comments", Vars: vars})
		if err != nil || resp.Class != detect.OK {
			t.Fatalf("comments: %v %v", err, resp)
		}
		res, rep := parse.Comments(resp.Parts, parse.CommentContext{PostID: postID, PageID: pageID})
		if !rep.OK() {
			t.Fatalf("comments drift: %v", rep.Missing)
		}
		for _, c := range res.Comments {
			out[c.Comment.CommentId] = true
		}
		threads = append(threads, res.Threads...)
		if !res.PageInfo.HasNext {
			break
		}
		cursor = res.PageInfo.EndCursor
	}
	for len(threads) > 0 {
		th := threads[0]
		threads = threads[1:]
		cursor := ""
		for {
			vars := map[string]any{"feedback_id": th.FeedbackID, "expansion_token": th.ExpansionToken}
			if cursor != "" {
				vars["cursor"] = cursor
			}
			resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "replies", Vars: vars})
			if err != nil || resp.Class != detect.OK {
				t.Fatalf("replies: %v %+v", err, resp)
			}
			res, rep := parse.Comments(resp.Parts, parse.CommentContext{PostID: postID, PageID: pageID,
				BaseDepth: th.Depth + 1, ParentCommentID: th.CommentID, RootCommentID: th.RootCommentID, Replies: true})
			if !rep.OK() {
				t.Fatalf("replies drift: %v", rep.Missing)
			}
			for _, c := range res.Comments {
				if c.Comment.ParentCommentId == nil || c.Comment.Depth != th.Depth+1 {
					t.Fatalf("reply %d has wrong parent/depth: %+v", c.Comment.CommentId, c.Comment)
				}
				out[c.Comment.CommentId] = true
			}
			threads = append(threads, res.Threads...)
			if !res.PageInfo.HasNext {
				break
			}
			cursor = res.PageInfo.EndCursor
		}
	}
}

func TestClientLayerCollectsEverything(t *testing.T) {
	for _, tls := range []bool{false, true} {
		name := "std"
		if tls {
			name = "tls-client"
		}
		t.Run(name, func(t *testing.T) {
			env := New(t, Options{UseTLS: tls})
			ctx := context.Background()
			for _, p := range env.World.Pages() {
				id, err := env.Pool.Acquire(ctx, "direct")
				if err != nil {
					t.Fatal(err)
				}
				got, _ := crawlPage(t, env, id, p)
				env.Pool.Release(id)
				truth, _ := env.World.TruthFor(p.ID, timeZero)
				if len(got) != len(truth.Posts) {
					t.Fatalf("page %d: got %d posts, want %d", p.ID, len(got), len(truth.Posts))
				}
				totalWant, totalGot := 0, 0
				for _, pt := range truth.Posts {
					cs := got[pt.PostID]
					for _, cid := range pt.CommentIDs {
						if !cs[cid] {
							t.Errorf("post %d: comment %d missing", pt.PostID, cid)
						}
					}
					totalWant += len(pt.CommentIDs)
					totalGot += len(cs)
				}
				if totalGot != totalWant {
					t.Fatalf("page %d: got %d comments, want %d", p.ID, totalGot, totalWant)
				}
				t.Logf("page %d: %d posts, %d comments (incl. replies) collected", p.ID, len(got), totalGot)
			}
		})
	}
}

func TestLoginWallAndRateLimitAreClassified(t *testing.T) {
	env := New(t, Options{Faults: fakefb.Faults{LoginWallAfter: 2}})
	ctx := context.Background()
	id, _ := env.Pool.Acquire(ctx, "direct")
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	var last detect.Class
	for i := 0; i < 3; i++ {
		resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"count": 3, "page_id": p.ID}})
		if err != nil {
			t.Fatal(err)
		}
		last = resp.Class
	}
	if last != detect.LoginWall {
		t.Fatalf("want login wall, got %v", last)
	}
	if id.RetireReason() == "" {
		t.Fatal("identity should be marked for retirement after a login wall")
	}

	env.Fake.SetFaults(fakefb.Faults{RateLimitEvery: 1})
	id2, _ := env.Pool.Acquire(ctx, "direct")
	if _, err := env.Loader.Bootstrap(ctx, id2, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	resp, err := env.GQL.Do(ctx, id2, graphql.Request{Query: "timeline", Vars: map[string]any{"count": 3, "page_id": p.ID}})
	if err != nil || resp.Class != detect.RateLimited {
		t.Fatalf("want rate limited, got %v %v", err, resp.Class)
	}
}

// A page of posts with a partial field error beside it (as real Facebook
// streams) is OK, not a soft error to retry.
func TestPartialFieldErrorsAreOK(t *testing.T) {
	env := New(t, Options{Faults: fakefb.Faults{FieldErrors: true}})
	ctx := context.Background()
	id, _ := env.Pool.Acquire(ctx, "direct")
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"count": 3, "page_id": p.ID}})
	if err != nil || resp.Class != detect.OK || len(resp.Errors) != 1 {
		t.Fatalf("got %v %v (%s), errors %v", err, resp.Class, resp.Reason, resp.Errors)
	}
	tl, rep := parse.Timeline(resp.Parts, p.ID, env.HTTP.URL)
	if !rep.OK() || len(tl.Posts) == 0 {
		t.Fatalf("parse: %d posts, missing %v", len(tl.Posts), rep.Missing)
	}
}

func TestDocRotationFallsBackToFallbackEntry(t *testing.T) {
	env := New(t, Options{})
	ctx := context.Background()
	id, _ := env.Pool.Acquire(ctx, "direct")
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	env.Fake.RotateDoc("ProfileCometTimelineFeedRefetchQuery", "1234")
	resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"count": 3, "page_id": p.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Class != detect.RegistryStale {
		t.Fatalf("want registry stale without a fallback, got %v", resp.Class)
	}
}

func TestSchemaDriftIsReported(t *testing.T) {
	env := New(t, Options{Faults: fakefb.Faults{Drift: true}})
	ctx := context.Background()
	id, _ := env.Pool.Acquire(ctx, "direct")
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	resp, _ := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"count": 3, "page_id": p.ID}})
	tl, rep := parse.Timeline(resp.Parts, p.ID, env.HTTP.URL)
	// message is optional, so drift in it must not drop posts...
	if len(tl.Posts) == 0 || !rep.OK() {
		t.Fatalf("posts dropped on optional drift: %+v", rep)
	}
	// ...but the text is lost, which the optional report records via empty text.
	for _, b := range tl.Posts {
		if b.Post.Text != "" {
			t.Fatalf("drifted message should not parse, got %q", b.Post.Text)
		}
	}
}

// TestQueryBreakerShortCircuits checks that a query whose responses keep
// failing to parse stops being sent until its cooldown ends.
func TestQueryBreakerShortCircuits(t *testing.T) {
	env := New(t, Options{})
	env.GQL.Breakers = breaker.New(breaker.Config{MinEvents: 3, OpenRate: 0.5, Cooldown: time.Hour})
	ctx := context.Background()
	id, err := env.Pool.Acquire(ctx, "direct")
	if err != nil {
		t.Fatal(err)
	}
	defer env.Pool.Release(id)
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	req := graphql.Request{Query: "timeline", Vars: map[string]any{"page_id": p.ID, "count": 3}}
	for i := 0; i < 3; i++ {
		resp, err := env.GQL.Do(ctx, id, req)
		if err != nil || resp.Class != detect.OK {
			t.Fatalf("request %d: %v %v", i, err, resp)
		}
		env.GQL.Parsed(resp, false) // as if required fields were missing
	}
	sent := env.Fake.Stats().ByQuery["ProfileCometTimelineFeedRefetchQuery"]
	resp, err := env.GQL.Do(ctx, id, req)
	if err != nil || resp.Class != detect.SchemaDrift || !strings.Contains(resp.Reason, "breaker open") {
		t.Fatalf("want a short-circuited drift response, got %v %+v", err, resp)
	}
	if got := env.Fake.Stats().ByQuery["ProfileCometTimelineFeedRefetchQuery"]; got != sent {
		t.Fatalf("an open breaker must not send requests (%d -> %d)", sent, got)
	}
	// Other queries are unaffected.
	if resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "comments", Vars: map[string]any{"feedback_id": parse.FeedbackID(p.Posts[0].ID)}}); err != nil || resp.Class != detect.OK {
		t.Fatalf("comments: %v %+v", err, resp)
	}
}

type recordingLimiter struct {
	mu   sync.Mutex
	seen map[string]int
}

func (l *recordingLimiter) Wait(_ context.Context, key string, perSecond int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen[key] = perSecond
	return nil
}

type fixedCeiling int

func (c fixedCeiling) RPS() int { return int(c) }

type countingOutcomes struct{ n, blocked int }

func (c *countingOutcomes) Observe(cls detect.Class, _ string, _ int64) {
	c.n++
	if cls.Blocking() {
		c.blocked++
	}
}

// TestGlobalLimitFollowsTheCeiling checks that requests are paced against
// the AIMD ceiling when one is set, and that outcomes are reported for it.
func TestGlobalLimitFollowsTheCeiling(t *testing.T) {
	env := New(t, Options{})
	lim := &recordingLimiter{seen: map[string]int{}}
	out := &countingOutcomes{}
	env.GQL.Limiter, env.GQL.GlobalRPS, env.GQL.Outcomes = lim, 50, out
	ctx := context.Background()
	id, err := env.Pool.Acquire(ctx, "direct")
	if err != nil {
		t.Fatal(err)
	}
	defer env.Pool.Release(id)
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	req := graphql.Request{Query: "timeline", Vars: map[string]any{"page_id": p.ID, "count": 3}}
	if _, err := env.GQL.Do(ctx, id, req); err != nil {
		t.Fatal(err)
	}
	if lim.seen["global"] != 50 {
		t.Fatalf("without a ceiling, GlobalRPS applies: %v", lim.seen)
	}
	env.GQL.Ceiling = fixedCeiling(17)
	if _, err := env.GQL.Do(ctx, id, req); err != nil {
		t.Fatal(err)
	}
	if lim.seen["global"] != 17 || out.n != 2 || out.blocked != 0 {
		t.Fatalf("ceiling %v, outcomes %+v", lim.seen, out)
	}
}
