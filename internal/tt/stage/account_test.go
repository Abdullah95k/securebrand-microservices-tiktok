package stage

import (
	"context"
	"testing"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

const day = 24 * time.Hour

// samePost compares a published post with fakett's ground truth.
func samePost(t *testing.T, got *ttv1.Post, want *fakett.Post) {
	t.Helper()
	kind := ttv1.PostKind_POST_KIND_VIDEO
	if want.Kind == "photo" {
		kind = ttv1.PostKind_POST_KIND_PHOTO
	}
	c := got.GetCounts()
	switch {
	case got.UserId != want.Owner.UID, got.Kind != kind, !got.CreatedAt.AsTime().Equal(want.Created), got.Caption != want.Desc:
		t.Errorf("post %d: got user %d kind %v at %v %q, want %d %v %v %q", want.ID, got.UserId, got.Kind, got.CreatedAt.AsTime(), got.Caption,
			want.Owner.UID, kind, want.Created, want.Desc)
	case got.GetPinned() != want.Pinned, got.GetAd() != want.Ad:
		t.Errorf("post %d: pinned %v ad %v, want %v %v", want.ID, got.GetPinned(), got.GetAd(), want.Pinned, want.Ad)
	case c.GetPlays() != want.Plays, c.GetLikes() != want.Likes, c.GetComments() != want.CommentCount(),
		c.GetShares() != want.Shares, c.GetSaves() != want.Saves, c.GetReposts() != want.Reposts:
		t.Errorf("post %d: counts %v, want plays %d likes %d comments %d shares %d saves %d reposts %d", want.ID, c,
			want.Plays, want.Likes, want.CommentCount(), want.Shares, want.Saves, want.Reposts)
	case got.GetMusicId() != want.Music.ID, len(got.Hashtags) != len(want.Hashtags):
		t.Errorf("post %d: music %d hashtags %v, want %d %v", want.ID, got.GetMusicId(), got.Hashtags, want.Music.ID, want.Hashtags)
	}
}

// A backfill of every account (one job each, continuations every 3 pages)
// publishes exactly fakett's posts with their counts, music and metrics,
// the account from its page (exact counts, bio link) and from the list,
// a post task for every post with comments, and the list's watermark.
func TestBackfillEqualsGroundTruth(t *testing.T) {
	h := newHarness(t, tttest.Options{})
	w := h.env.World
	for _, acc := range w.Accounts() {
		job := "backfill-" + acc.Username
		st := h.check(job, commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
		if st.Counters["lists_first_post"] != 1 || st.Counters["tasks_failed"] != 0 {
			t.Fatalf("%s: counters %v", acc.Username, st.Counters)
		}
		if st.Spawned < 3 { // the root, its continuations, post tasks
			t.Errorf("%s: %d tasks spawned: no continuation?", acc.Username, st.Spawned)
		}
		got := posts(h, job)
		if len(got) != len(acc.Posts) {
			t.Fatalf("%s: %d posts published, want %d", acc.Username, len(got), len(acc.Posts))
		}
		for _, want := range acc.Posts {
			p, ok := got[want.ID]
			if !ok {
				t.Fatalf("%s: post %d missing", acc.Username, want.ID)
			}
			samePost(t, p, want)
		}
		metrics := records(h, topics.PostMetrics, job, func() *ttv1.PostMetrics { return &ttv1.PostMetrics{} })
		music := records(h, topics.Music, job, func() *ttv1.Music { return &ttv1.Music{} })
		if len(metrics) != len(acc.Posts) || len(music) != len(acc.Posts) {
			t.Errorf("%s: %d metrics and %d music records for %d posts", acc.Username, len(metrics), len(music), len(acc.Posts))
		}
		var page, list *ttv1.Account
		for _, a := range accounts(h, job) {
			switch a.Source {
			case ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE:
				page = a
			case ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST:
				list = a
			}
		}
		if page == nil || list == nil {
			t.Fatalf("%s: account from the page %v, from the list %v", acc.Username, page != nil, list != nil)
		}
		if page.UserId != acc.UID || page.SecUid != acc.SecUID || page.GetBioLink() != acc.BioLink ||
			page.GetCounts().GetFollowers() != acc.Followers || !page.GetCounts().GetExact() || len(page.AuthorKey) == 0 {
			t.Errorf("%s: page account %v", acc.Username, page)
		}
		if list.UserId != acc.UID || list.Username != acc.Username || list.GetCounts().GetExact() {
			t.Errorf("%s: list account %v", acc.Username, list)
		}
		tasks := postTasks(h, job)
		for _, p := range acc.Posts {
			if _, ok := tasks[p.ID]; ok != (p.CommentCount() > 0) {
				t.Errorf("%s: post %d with %d comments: post task %v", acc.Username, p.ID, p.CommentCount(), ok)
			}
		}
		var newest, state string
		var stop *string
		err := h.d.DB.QueryRow(context.Background(), `SELECT newest_seen_at::text, state, truncated_reason FROM crawl_cursors
			WHERE scope_type = 'account_list' AND scope_id = $1`, acc.UID).Scan(&newest, &state, &stop)
		if err != nil || state != "complete" || stop != nil {
			t.Fatalf("%s: watermark %q %q %v: %v", acc.Username, newest, state, stop, err)
		}
		wm, _, _ := h.d.listWatermark(context.Background(), acc.UID)
		if !wm.Equal(acc.Posts[0].Created) {
			t.Errorf("%s: watermark %v, want the newest post's %v", acc.Username, wm, acc.Posts[0].Created)
		}
	}
}

// A backfill stops at the age window (365 days), not at the first post.
func TestBackfillStopsAtAgeWindow(t *testing.T) {
	h := newHarness(t, tttest.Options{Spec: fakett.Spec{Seed: 3, Accounts: 1, PostsPerAccount: 500, PostSpacing: 20 * time.Hour}})
	acc := h.env.World.Accounts()[0]
	st := h.check("age", commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
	if st.Counters["lists_age"] != 1 {
		t.Fatalf("counters %v", st.Counters)
	}
	want := inWindow(h.env.World, acc, 365)
	if len(want) == len(acc.Posts) {
		t.Fatal("the world has no post past the window")
	}
	got := posts(h, "age")
	if len(got) != len(want) {
		t.Fatalf("%d posts published, want the %d within 365 days", len(got), len(want))
	}
	for _, p := range want {
		if _, ok := got[p.ID]; !ok {
			t.Fatalf("post %d of %v missing", p.ID, p.Created)
		}
	}
}

// An incremental check reads back to the newest stored post: new posts are
// published with their counts, the list stops at the watermark after a
// page, and post tasks go to the new posts and to the posts whose comment
// count went up within their window, not to the others.
func TestIncrementalCheck(t *testing.T) {
	h := newHarness(t, tttest.Options{Spec: fakett.Spec{Seed: 5, Accounts: 1, PostsPerAccount: 80, MaxComments: 10, PostSpacing: 3 * time.Hour}})
	w := h.env.World
	acc := w.Accounts()[0]
	h.check("first", commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
	// The post stage (M5b) records each read; here every post was read 7 h
	// ago at its present count.
	ctx := context.Background()
	for _, p := range acc.Posts {
		if err := h.d.Ckpt.SetPost(ctx, p.ID, checkpoint.PostCache{Reported: p.CommentCount(), ThreadsComplete: true,
			ReadAt: w.Now.Add(-7 * time.Hour).Unix()}); err != nil {
			t.Fatal(err)
		}
	}
	w.Advance(2 * time.Hour)
	fresh := []*fakett.Post{w.AddPost(acc, 2), w.AddPost(acc, 0)}
	w.Advance(30 * time.Minute)
	young := acc.Posts[4]              // ~12 h old: its 6 h window has passed
	old := acc.Posts[len(acc.Posts)-1] // ~10 days old: weekly window, read 7 h ago
	still := acc.Posts[6]              // unchanged
	w.AddComments(young, 3)
	w.AddComments(old, 3)
	w.Mutate(func() { still.Likes += 5 }) // a metric changes, comments do not

	h.d.Cfg.Scheduler.IncrementalOverlap = time.Hour
	sc := task.ScopeFromRef("u:" + acc.Username)
	sc.UserId, sc.SecUid = acc.UID, acc.SecUID
	sc.AccountSeenAt, sc.PageSeenAt = nowTS(w.Now.Add(-time.Hour)), nowTS(w.Now.Add(-time.Hour)) // no page due
	st := h.check("incr", commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, sc)
	if st.Counters["lists_watermark"] != 1 || st.Counters["account_pages"] != 0 {
		t.Fatalf("counters %v", st.Counters)
	}
	got := posts(h, "incr")
	for _, p := range fresh {
		if g, ok := got[p.ID]; !ok {
			t.Fatalf("new post %d not published", p.ID)
		} else {
			samePost(t, g, p)
		}
	}
	if _, ok := got[old.ID]; ok {
		t.Errorf("a 10-day-old post was read by an incremental check")
	}
	if len(got) > 2*h.d.Cfg.TT.ListPageSize {
		t.Errorf("%d posts read: the check went past its watermark", len(got))
	}
	tasks := postTasks(h, "incr")
	if _, ok := tasks[fresh[0].ID]; !ok {
		t.Error("no post task for a new post with comments")
	}
	if _, ok := tasks[fresh[1].ID]; ok {
		t.Error("a post task for a new post without comments")
	}
	if _, ok := tasks[young.ID]; !ok {
		t.Error("no post task for a young post whose comment count went up")
	}
	if _, ok := tasks[still.ID]; ok {
		t.Error("a post task for a post whose comment count did not change")
	}
	if g := got[still.ID]; g == nil || g.GetCounts().GetLikes() != still.Likes {
		t.Errorf("changed likes not published: %v", g.GetCounts())
	}

	// A metrics walk (REFRESH) back 30 days reaches the old post, and its
	// comment count went up: past its window's interval? It was read 7 h
	// ago in a weekly window, so not yet.
	ref := task.ScopeFromRef("u:" + acc.Username)
	ref.UserId, ref.SecUid, ref.AccountSeenAt, ref.PageSeenAt = acc.UID, acc.SecUID, sc.AccountSeenAt, sc.PageSeenAt
	lim := task.DefaultLimits(h.d.Cfg.Limits, h.d.Cfg.TT, false)
	lim.MaxAgeSeconds = int64(30 * day / time.Second)
	root := task.NewAccount("walk", tkstaskv1.Lane_LANE_P2, commonv1.CrawlMode_CRAWL_MODE_REFRESH, ref, lim)
	if err := h.d.spawn(ctx, root); err != nil {
		t.Fatal(err)
	}
	h.drain("walk")
	if g := posts(h, "walk"); len(g) != len(acc.Posts) {
		t.Errorf("metrics walk read %d posts, want all %d", len(g), len(acc.Posts))
	}
	if _, ok := postTasks(h, "walk")[old.ID]; ok {
		t.Error("the weekly window let a post read 7 h ago be read again")
	}
}

// A renamed account is found by its secUid: its old page names nothing, the
// list carries the new username. A private account and a gone one end the
// check with their status. tks erases no one: every account is read.
func TestAccountChanges(t *testing.T) {
	h := newHarness(t, tttest.Options{Spec: fakett.Spec{Seed: 7, Accounts: 4, PostsPerAccount: 20, PostSpacing: 10 * time.Hour}})
	w := h.env.World
	accs := w.Accounts()
	renamed, private, gone, plain := accs[0], accs[1], accs[2], accs[3]
	scopes := map[*fakett.Account]*tkstaskv1.AccountScope{}
	for _, a := range accs {
		sc := task.ScopeFromRef("u:" + a.Username)
		sc.UserId, sc.SecUid = a.UID, a.SecUID
		scopes[a] = sc
	}
	old := renamed.Username
	w.Rename(renamed, "renamed_account")
	w.Mutate(func() { private.Private, gone.Gone = true, true })

	st := h.check("renamed", commonv1.CrawlMode_CRAWL_MODE_FULL, scopes[renamed])
	if st.Counters["profile_unavailable"] != 1 || st.Counters["lists_first_post"] != 1 {
		t.Fatalf("renamed: counters %v", st.Counters)
	}
	var names []string
	for _, a := range accounts(h, "renamed") {
		names = append(names, a.Username)
	}
	if len(names) != 1 || names[0] != "renamed_account" {
		t.Errorf("renamed %s: accounts published %v", old, names)
	}
	if len(posts(h, "renamed")) != len(renamed.Posts) {
		t.Errorf("renamed: %d posts", len(posts(h, "renamed")))
	}

	for _, c := range []struct {
		acc    *fakett.Account
		job    string
		status ttv1.AccountStatus
	}{
		{private, "private", ttv1.AccountStatus_ACCOUNT_STATUS_PRIVATE},
		{gone, "gone", ttv1.AccountStatus_ACCOUNT_STATUS_UNAVAILABLE},
	} {
		h.check(c.job, commonv1.CrawlMode_CRAWL_MODE_FULL, scopes[c.acc])
		var last *ttv1.Account
		for _, a := range accounts(h, c.job) {
			last = a
		}
		if last == nil || last.Status != c.status || last.UserId != c.acc.UID {
			t.Errorf("%s: last account %v, want status %v", c.job, last, c.status)
		}
		if n := len(posts(h, c.job)); n != 0 {
			t.Errorf("%s: %d posts published", c.job, n)
		}
	}
	// The gone account's page says banned (10221): the page is read first.
	for _, a := range accounts(h, "gone") {
		if a.Status != ttv1.AccountStatus_ACCOUNT_STATUS_BANNED && a.Status != ttv1.AccountStatus_ACCOUNT_STATUS_UNAVAILABLE {
			t.Errorf("gone: status %v", a.Status)
		}
	}

	h.check("plain", commonv1.CrawlMode_CRAWL_MODE_FULL, scopes[plain])
	if len(accounts(h, "plain")) == 0 || len(posts(h, "plain")) != len(plain.Posts) {
		t.Errorf("plain: %d accounts, %d posts, want %d", len(accounts(h, "plain")), len(posts(h, "plain")), len(plain.Posts))
	}
}

// TikTok's occasional list errors (statusCode 10102), rate limits, its
// list window (a week before the cursor, so a quiet month answers empty
// pages) and every task delivered twice still give every post once, and
// the job's counters count each page once.
func TestBackfillThroughFaults(t *testing.T) {
	h := newHarness(t, tttest.Options{Spec: fakett.Spec{Seed: 9, Accounts: 1, PostsPerAccount: 120, PostSpacing: 15 * time.Hour},
		Faults: fakett.Faults{ListErrorEvery: 4, RateLimitEvery: 9, ListWindow: 7 * day}})
	h.redeliver = true
	acc := h.env.World.Accounts()[0]
	// A month without posts in the middle of the year.
	h.env.World.Mutate(func() {
		for _, p := range acc.Posts[60:] {
			p.Created = p.Created.Add(-30 * day)
		}
	})
	st := h.check("faults", commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username))
	if st.Counters["soft_errors"] == 0 || st.Counters["blocked"] == 0 {
		t.Errorf("the faults did not happen: %v", st.Counters)
	}
	if n := len(records(h, topics.Post, "faults", func() *ttv1.Post { return &ttv1.Post{} })); n != len(inWindow(h.env.World, acc, 365)) {
		t.Errorf("%d post records for %d posts: a redelivered task ran twice", n, len(inWindow(h.env.World, acc, 365)))
	}
	got := posts(h, "faults")
	want := inWindow(h.env.World, acc, 365)
	if len(got) != len(want) {
		t.Fatalf("%d posts, want %d", len(got), len(want))
	}
	for _, p := range want {
		samePost(t, got[p.ID], p)
	}
}

// Comment windows: a post read before is read again when its count went up
// and its window's interval has passed; past the last window only a metrics
// walk reads it.
func TestCommentsDue(t *testing.T) {
	ws := []config.CommentWindow{{MaxAge: 48 * time.Hour, Interval: 6 * time.Hour}, {MaxAge: 7 * day, Interval: day}, {MaxAge: 30 * day, Interval: 7 * day}}
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	inc, ref := commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL, commonv1.CrawlMode_CRAWL_MODE_REFRESH
	for _, c := range []struct {
		name     string
		age, ago time.Duration
		mode     commonv1.CrawlMode
		want     bool
	}{
		{"young, read 7 h ago", 20 * time.Hour, 7 * time.Hour, inc, true},
		{"young, read 2 h ago", 20 * time.Hour, 2 * time.Hour, inc, false},
		{"3 days, read 7 h ago", 3 * day, 7 * time.Hour, inc, false},
		{"3 days, read 25 h ago", 3 * day, 25 * time.Hour, inc, true},
		{"20 days, read 3 days ago", 20 * day, 3 * day, inc, false},
		{"20 days, read 8 days ago", 20 * day, 8 * day, inc, true},
		{"60 days, incremental", 60 * day, 30 * day, inc, false},
		{"60 days, metrics walk", 60 * day, 30 * day, ref, true},
		{"read time unknown", 3 * day, 0, inc, true},
	} {
		read := int64(0)
		if c.ago > 0 {
			read = now.Add(-c.ago).Unix()
		}
		if got := commentsDue(ws, now, now.Add(-c.age), read, c.mode); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}
