package stage

import (
	"context"
	"testing"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// store writes a job's post records to schema tt, as the persister would.
func store(t *testing.T, h *harness, job string) {
	t.Helper()
	ps := records(h, topics.Post, job, func() *ttv1.Post { return &ttv1.Post{} })
	w := &persist.Writer{DB: h.d.DB, Now: h.d.Now}
	if err := w.Posts(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
}

// A stored post a full list page covering its date no longer returns is
// checked once on its page: a post made private (10216) is published as
// unavailable with the code and the exit's region; a code not known to mean
// a post that is gone (10204, unconfirmed: T34) marks nothing; four missing
// from one page is a reading problem; a private account's posts keep their
// status. A post once marked is not checked again, and one that comes back
// is published as visible (docs/tiktok/discovery-plan.md §17).
func TestPostsNoLongerPublic(t *testing.T) {
	h := newHarness(t, tttest.Options{})
	w := h.env.World
	accs := w.Accounts()
	a, b, c := accs[0], accs[1], accs[2]
	for _, acc := range accs {
		job := "backfill-" + acc.Username
		if st := h.check(job, commonv1.CrawlMode_CRAWL_MODE_FULL, task.ScopeFromRef("u:"+acc.Username)); st.Counters["tasks_failed"] != 0 {
			t.Fatalf("%s: %v", acc.Username, st.Counters)
		}
		store(t, h, job)
	}

	w.Advance(time.Hour) // the marks come after the reads
	hidden, deleted := a.Posts[20], a.Posts[35]
	w.HidePost(hidden, 10216)
	w.DeletePost(deleted)
	for _, p := range append([]*fakett.Post(nil), b.Posts[16:20]...) {
		w.HidePost(p, 10216)
	}
	w.HidePost(c.Posts[20], 10216)
	w.Mutate(func() { c.Private = true })

	walk := func(acc *fakett.Account, job string) map[string]int64 {
		t.Helper()
		st := h.check(job, commonv1.CrawlMode_CRAWL_MODE_REFRESH, task.ScopeFromRef("u:"+acc.Username))
		if st.Counters["tasks_failed"] != 0 {
			t.Fatalf("%s: %v", job, st.Counters)
		}
		return st.Counters
	}
	unavailable := func(job string) map[int64]*ttv1.Post {
		out := map[int64]*ttv1.Post{}
		for id, p := range posts(h, job) {
			if p.Status == ttv1.PostStatus_POST_STATUS_UNAVAILABLE {
				out[id] = p
			}
		}
		return out
	}

	got := walk(a, "walk-a")
	marks := unavailable("walk-a")
	if p, ok := marks[hidden.ID]; !ok || len(marks) != 1 || p.GetStatusCode() != 10216 || p.StatusRegion != "IQ" ||
		p.UserId != a.UID || !p.CreatedAt.AsTime().Equal(hidden.Created) || p.Caption != "" {
		t.Fatalf("account a: marks %v", marks)
	}
	if got["vanish_unavailable"] != 1 || got["vanish_unknown_code"] != 1 {
		t.Fatalf("account a: counters %v", got)
	}

	got = walk(b, "walk-b")
	if got["vanish_reading_problem"] != 4 || len(unavailable("walk-b")) != 0 || got["vanish_unavailable"] != 0 {
		t.Fatalf("account b: counters %v, marks %v", got, unavailable("walk-b"))
	}

	got = walk(c, "walk-c")
	if got["accounts_private"] != 1 || len(unavailable("walk-c")) != 0 {
		t.Fatalf("account c: counters %v", got)
	}

	// Stored as unavailable: the next walk does not read its page again
	// (the deleted post, still visible in the store, is checked again).
	store(t, h, "walk-a")
	got = walk(a, "walk-a2")
	if got["vanish_unavailable"] != 0 || got["vanish_unknown_code"] != 1 || len(unavailable("walk-a2")) != 0 {
		t.Fatalf("account a, second walk: counters %v", got)
	}

	// It comes back: listed, and stored as visible again.
	w.Advance(time.Hour)
	w.ShowPost(hidden)
	walk(a, "walk-a3")
	if p := posts(h, "walk-a3")[hidden.ID]; p == nil || p.Status != ttv1.PostStatus_POST_STATUS_VISIBLE {
		t.Fatalf("post back: %v", p)
	}
	store(t, h, "walk-a3")
	var status string
	if err := h.d.DB.QueryRow(context.Background(), `SELECT status FROM posts WHERE post_id = $1`, hidden.ID).Scan(&status); err != nil || status != "visible" {
		t.Fatalf("post back: stored %q (%v)", status, err)
	}
}
