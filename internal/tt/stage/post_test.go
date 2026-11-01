package stage

import (
	"context"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// testSigner stands in for the browser pages (internal/tt/sign has their
// own tests with Chromium): it adds the parameter fakett checks, and fails
// the first failFirst calls.
type testSigner struct {
	calls     atomic.Int64
	failFirst int64
}

func (s *testSigner) SignURL(_ context.Context, _ *session.Identity, _, raw, _ string) (string, error) {
	if s.calls.Add(1) <= s.failFirst {
		return "", errors.New("no signer page")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("X-Gnarly", "test-signature")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// signedComments is fakett answering comment and reply calls only when
// signed, as TikTok does (C2, C3).
var signedComments = fakett.Faults{RequireSignature: true, SignedPaths: []string{"/api/comment/list/", "/api/comment/list/reply/"}}

// commentHarness runs every stage, with the comment calls signed by s (nil:
// none, so they go out unsigned).
func commentHarness(t *testing.T, o tttest.Options, s ttclient.URLSigner) *harness {
	t.Helper()
	h := newHarness(t, o)
	h.runComments = true
	h.d.Cfg.TT.CommentsPageSize = 10 // continuations within a post's comments
	if s != nil {
		h.d.Client.Signer = ttclient.SignerFor(config.SignerBrowser, s)
	}
	return h
}

func comments(h *harness, job string) map[int64]*ttv1.Comment {
	out := map[int64]*ttv1.Comment{}
	for _, c := range records(h, topics.Comment, job, func() *ttv1.Comment { return &ttv1.Comment{} }) {
		out[c.CommentId] = c
	}
	return out
}

// sameComments compares the comments published for a post with fakett's:
// every top-level comment and reply, nothing else.
func sameComments(t *testing.T, got map[int64]*ttv1.Comment, p *fakett.Post) {
	t.Helper()
	want := 0
	check := func(c *fakett.Comment, parent int64) {
		want++
		g, ok := got[c.ID]
		switch {
		case !ok:
			t.Errorf("post %d: comment %d (parent %d) missing", p.ID, c.ID, parent)
		case g.PostId != p.ID, g.UserId != p.Owner.UID, g.Text != c.Text, g.GetLikeCount() != c.Likes,
			g.GetAuthor().GetUserId() != c.Author.UID, len(g.GetAuthor().GetAuthorKey()) == 0, !g.CreatedAt.AsTime().Equal(c.Created):
			t.Errorf("post %d: comment %d: got %+v", p.ID, c.ID, g)
		case parent == 0 && (g.Depth != 0 || g.ParentCommentId != nil || g.GetReplyCount() != int64(len(c.Replies))):
			t.Errorf("post %d: top-level comment %d: depth %d parent %v replies %d", p.ID, c.ID, g.Depth, g.ParentCommentId, g.GetReplyCount())
		case parent != 0 && (g.Depth != 1 || g.GetParentCommentId() != parent):
			t.Errorf("post %d: reply %d: depth %d parent %d, want %d", p.ID, c.ID, g.Depth, g.GetParentCommentId(), parent)
		}
	}
	for _, c := range p.Comments {
		check(c, 0)
		for _, r := range c.Replies {
			check(r, c.ID)
		}
	}
	n := 0
	for _, g := range got {
		if g.PostId == p.ID {
			n++
		}
	}
	if n != want {
		t.Errorf("post %d: %d comments published, want %d", p.ID, n, want)
	}
}

// A backfill with comments: every post's top-level comments and replies,
// read through the signer (every comment and reply call signed, the list
// never), equal fakett's; a thread task only for comments with more replies
// than the one quoted inline; each post's read is recorded for the next
// check's "count went up" rule.
func TestCommentsEqualGroundTruth(t *testing.T) {
	sg := &testSigner{}
	h := commentHarness(t, tttest.Options{Faults: signedComments}, sg)
	w := h.env.World
	acc := w.Accounts()[0]
	st := h.check("comments-backfill", commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
	if st.Counters["tasks_failed"] != 0 || st.Counters["lists_first_post"] != 1 {
		t.Fatalf("counters %v", st.Counters)
	}
	got := comments(h, "comments-backfill")
	threads, withComments := int64(0), 0
	for _, p := range acc.Posts {
		sameComments(t, got, p)
		for _, c := range p.Comments {
			if len(c.Replies) > 1 {
				threads++
			}
		}
		if len(p.Comments) == 0 {
			continue
		}
		withComments++
		pc, ok := h.d.Ckpt.GetPost(context.Background(), p.ID)
		if !ok || pc.Reported != p.CommentCount() || pc.ReadAt == 0 || !pc.ThreadsComplete {
			t.Errorf("post %d: cache %+v (ok %v), want reported %d", p.ID, pc, ok, p.CommentCount())
		}
	}
	if st.Counters["threads_spawned"] != threads {
		t.Errorf("%d thread tasks, want %d (comments with more replies than the inline one)", st.Counters["threads_spawned"], threads)
	}
	if st.Counters["comments"]+st.Counters["replies"] < int64(len(got)) {
		t.Errorf("counters %v for %d comments", st.Counters, len(got))
	}
	states, err := h.d.Jobs.Posts(context.Background(), "comments-backfill")
	if err != nil {
		t.Fatal(err)
	}
	complete := 0
	for _, ps := range states {
		if ps.State == jobs.PostComplete {
			complete++
		}
	}
	if complete < withComments {
		t.Errorf("%d posts complete, want %d", complete, withComments)
	}
	for _, path := range []string{"/api/comment/list/", "/api/comment/list/reply/"} {
		reqs := h.env.Requests(path)
		if len(reqs) == 0 {
			t.Fatalf("no %s calls", path)
		}
		for _, r := range reqs {
			if u, _ := url.Parse(r.URL); u.Query().Get("X-Gnarly") == "" {
				t.Fatalf("an unsigned %s call: %s", path, r.URL)
			}
		}
	}
	if int(sg.calls.Load()) != len(h.env.Requests("/api/comment/list/"))+len(h.env.Requests("/api/comment/list/reply/")) {
		t.Errorf("%d signatures for %d comment and reply calls", sg.calls.Load(),
			len(h.env.Requests("/api/comment/list/"))+len(h.env.Requests("/api/comment/list/reply/")))
	}
	for _, r := range h.env.Requests("/api/creator/item_list/") {
		if u, _ := url.Parse(r.URL); u.Query().Get("X-Gnarly") != "" {
			t.Fatal("the creator list was signed")
		}
	}
}

// The next check reads again only the posts whose comment count went up:
// the whole ranked list again (new comments anywhere), only the threads
// whose reply count grew, and a read that reached the end records the
// comments it saw, so the finalizer can mark a deleted one.
func TestCommentsRereadWhenCountGrows(t *testing.T) {
	h := commentHarness(t, tttest.Options{Faults: signedComments}, &testSigner{})
	h.redeliver = true // every task twice: read once
	w := h.env.World
	acc := w.Accounts()[0]
	sc := task.ScopeFromRef("u:" + acc.Username)
	h.check("first", commonv1.CrawlMode_CRAWL_MODE_FULL, sc)

	// The two newest posts (within the first comment window) change: a
	// comment deleted and two added on one, two replies on a thread of the
	// other.
	var grown, threaded *fakett.Post
	var deleted, thread *fakett.Comment
	for _, p := range acc.Posts[:2] {
		for _, c := range p.Comments {
			if grown == nil && len(c.Replies) == 0 {
				grown, deleted = p, c
			}
		}
	}
	for _, p := range acc.Posts[:2] {
		if p == grown {
			continue
		}
		for _, c := range p.Comments {
			if thread == nil && len(c.Replies) > 1 {
				threaded, thread = p, c
			}
		}
	}
	if grown == nil || threaded == nil {
		t.Skip("the seeded world has no fitting posts")
	}
	w.DeleteComment(grown, deleted)
	w.AddComments(grown, 2)
	w.AddReplies(thread, 2)
	w.Advance(7 * time.Hour)

	st := h.check("second", commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, task.ScopeFromRef("u:"+acc.Username))
	if st.Counters["tasks_failed"] != 0 {
		t.Fatalf("counters %v", st.Counters)
	}
	read := postTasks(h, "second")
	if len(read) != 2 || read[grown.ID] == nil || read[threaded.ID] == nil {
		t.Fatalf("posts read again: %v, want %d and %d", keys(read), grown.ID, threaded.ID)
	}
	// Every top-level comment of both posts again (ranked: re-read whole),
	// and every reply of the grown thread; the unchanged threads only with
	// their inline reply.
	got := comments(h, "second")
	for _, p := range []*fakett.Post{grown, threaded} {
		for _, c := range p.Comments {
			if g, ok := got[c.ID]; !ok || g.Depth != 0 || g.GetReplyCount() != int64(len(c.Replies)) {
				t.Errorf("post %d: comment %d: %v", p.ID, c.ID, g)
			}
		}
	}
	for _, r := range thread.Replies {
		if g, ok := got[r.ID]; !ok || g.GetParentCommentId() != thread.ID {
			t.Errorf("reply %d of the grown thread: %v", r.ID, g)
		}
	}
	if _, ok := got[deleted.ID]; ok {
		t.Error("a deleted comment was published again")
	}
	if st.Counters["threads_spawned"] != 1 || st.Counters["threads_unchanged"] == 0 {
		t.Errorf("threads spawned %d (want 1, the grown one), unchanged %d", st.Counters["threads_spawned"], st.Counters["threads_unchanged"])
	}
	passes, _, err := h.d.Jobs.Passes(context.Background(), "second")
	if err != nil {
		t.Fatal(err)
	}
	if len(passes) != 2 {
		t.Fatalf("%d full passes recorded, want 2", len(passes))
	}
	seen, err := h.d.Jobs.Seen(context.Background(), "second", grown.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := seen[deleted.ID]; ok || len(seen) != len(grown.Comments) {
		t.Errorf("seen %d top-level comments (deleted one in: %v), want %d", len(seen), ok, len(grown.Comments))
	}
}

// The cap per post holds across continuations (one comment page a
// segment); a signer without a page now and then delays the read, which
// still ends; a read stopped at the cap is partial.
func TestCommentsCapAndSignerFailures(t *testing.T) {
	sg := &testSigner{failFirst: 2}
	h := commentHarness(t, tttest.Options{Faults: signedComments, Spec: fakett.Spec{Seed: 3, Accounts: 1, PostsPerAccount: 4,
		MaxComments: 45, MaxReplies: 5, PostSpacing: 20 * time.Hour}}, sg)
	h.d.Cfg.Crawler.YieldPages = 1
	h.d.Cfg.TT.Comments.MaxPerPost = 25
	h.d.Cfg.TT.Comments.IncludeReplies = false
	acc := h.env.World.Accounts()[0]
	st := h.check("capped", commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
	if st.Counters["tasks_failed"] != 0 || st.Counters["sign_errors"] != 2 {
		t.Fatalf("counters %v", st.Counters)
	}
	got := comments(h, "capped")
	states, _ := h.d.Jobs.Posts(context.Background(), "capped")
	state := map[int64]string{}
	for _, ps := range states {
		state[ps.PostID] = ps.State
	}
	for _, p := range acc.Posts {
		n, inline := 0, 0
		for _, c := range p.Comments {
			if len(c.Replies) > 0 {
				inline++
			}
		}
		for _, g := range got {
			if g.PostId == p.ID {
				n++
			}
		}
		total := len(p.Comments) + inline // what the pages hold
		switch {
		case total > 30 && (n < 25 || n > 30 || state[p.ID] != jobs.PostPartial):
			t.Errorf("post %d (%d on its pages): %d read, state %q; want the cap (25, to the page's end) and partial", p.ID, total, n, state[p.ID])
		case total <= 20 && n != total:
			t.Errorf("post %d: %d read of %d", p.ID, n, total)
		}
	}
}

// Without a signer (tt.signer.mode none) TikTok refuses the comment calls:
// the post tasks retry, then go to the dead letter queue, and the posts are
// marked for a whole read next time.
func TestCommentsUnsignedDeadLetter(t *testing.T) {
	h := commentHarness(t, tttest.Options{Faults: signedComments, Spec: fakett.Spec{Seed: 4, Accounts: 1, PostsPerAccount: 2,
		MaxComments: 10, PostSpacing: 20 * time.Hour}}, nil)
	acc := h.env.World.Accounts()[0]
	st := h.check("unsigned", commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
	withComments := 0
	for _, p := range acc.Posts {
		if len(p.Comments) > 0 {
			withComments++
			if h.d.Ckpt.Incomplete(context.Background(), p.ID) == "" {
				t.Errorf("post %d not marked incomplete", p.ID)
			}
		}
	}
	dlq := records(h, topics.DLQ, "unsigned", func() *tkstaskv1.Task { return &tkstaskv1.Task{} })
	if int(st.Counters["tasks_failed"]) != withComments || len(dlq) != withComments || st.Counters["drift"] == 0 {
		t.Fatalf("%d dead letters, counters %v; want %d", len(dlq), st.Counters, withComments)
	}
	if len(comments(h, "unsigned")) != 0 {
		t.Fatal("comments published from refused calls")
	}
}

// With media on (D5's switch), every post's cover and one video rendition
// are queued for download; off (the default), none.
func TestMediaTasksOnlyWhenSwitchedOn(t *testing.T) {
	h := newHarness(t, tttest.Options{Spec: fakett.Spec{Seed: 5, Accounts: 1, PostsPerAccount: 5, PostSpacing: 20 * time.Hour}})
	h.d.Cfg.Limits.BackfillVideoPolicy = "sd"
	acc := h.env.World.Accounts()[0]
	h.check("media-off", commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
	if n := len(records(h, topics.TaskMedia, "media-off", func() *tkstaskv1.Task { return &tkstaskv1.Task{} })); n != 0 {
		t.Fatalf("%d media tasks with the switch off", n)
	}
	h.d.Cfg.TT.Media.Include = true
	h.check("media-on", commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
	media := records(h, topics.TaskMedia, "media-on", func() *tkstaskv1.Task { return &tkstaskv1.Task{} })
	videos, covers := map[int64]int{}, map[int64]int{}
	for _, m := range media {
		mm := m.GetMedia().GetMedia()
		switch mm.Kind {
		case ttv1.MediaKind_MEDIA_KIND_VIDEO:
			videos[mm.GetPostId()]++
		case ttv1.MediaKind_MEDIA_KIND_COVER:
			covers[mm.GetPostId()]++
		}
	}
	for _, p := range acc.Posts {
		if covers[p.ID] != 1 || (p.Kind == "video" && videos[p.ID] != 1) {
			t.Errorf("post %d (%s): %d covers, %d videos queued", p.ID, p.Kind, covers[p.ID], videos[p.ID])
		}
	}
}

func keys[V any](m map[int64]V) []int64 {
	var out []int64
	for k := range m {
		out = append(out, k)
	}
	return out
}
