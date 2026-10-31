package scheduler

import (
	"testing"
	"time"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

func testScheduler(t *testing.T) config.Scheduler {
	t.Helper()
	c, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	return c.Scheduler
}

func TestTierFor(t *testing.T) {
	sc := testScheduler(t)
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	recent := now.Add(-time.Hour)
	for _, tc := range []struct {
		rate     float64
		last     *time.Time
		tier     string
		interval time.Duration
	}{
		// Every tier crawls for new posts every 3h; the tier still picks
		// the comment refreshes and what a budget slows first.
		{25, &recent, "T0", 3 * time.Hour},
		{10, &recent, "T0", 3 * time.Hour},
		{3, &recent, "T1", 3 * time.Hour},
		{0.2, &recent, "T2", 3 * time.Hour},
		{0.05, &recent, "T3", 3 * time.Hour},
		{0, nil, "T4", 3 * time.Hour},
	} {
		tier, iv := TierFor(sc, tc.rate, tc.last, now)
		if tier != tc.tier || iv != tc.interval {
			t.Errorf("rate %v: got %s %v, want %s %v", tc.rate, tier, iv, tc.tier, tc.interval)
		}
	}
	old := now.Add(-91 * 24 * time.Hour)
	if tier, _ := TierFor(sc, 0, &old, now); tier != "T4" {
		t.Fatalf("no post for 91 days is dormant, got %s", tier)
	}
}

func TestPostingRate(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	window := 14 * 24 * time.Hour
	long := now.Add(-100 * 24 * time.Hour)
	short := now.Add(-7 * 24 * time.Hour)
	tiny := now.Add(-time.Hour)
	if r := PostingRate(14, &long, window, now); r != 1 {
		t.Fatalf("14 posts in 14 days: %v", r)
	}
	if r := PostingRate(14, &short, window, now); r != 2 {
		t.Fatalf("history of 7 days: %v", r)
	}
	if r := PostingRate(3, &tiny, window, now); r != 3 {
		t.Fatalf("a span under a day counts as a day: %v", r)
	}
	if r := PostingRate(0, nil, window, now); r != 0 {
		t.Fatal(r)
	}
}

func ptr[T any](v T) *T { return &v }

func TestPlanRefresh(t *testing.T) {
	pr := testScheduler(t).PostRefresh
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	newest := now.Add(-2 * time.Hour)
	at := func(age time.Duration) time.Time { return now.Add(-age) }
	day := 24 * time.Hour
	base := func(age time.Duration) dueRefresh {
		return dueRefresh{postID: 1, pageID: 2, created: at(age), collected: ptr(int64(5)), total: ptr(int64(7)),
			feedbackID: ptr("ZmVlZGJhY2s6MQ=="), newestComment: &newest}
	}

	// A 3-day-old post: incremental from the stored watermark, next in 24h.
	act, root, next, unchanged, _ := plan(pr, "T1", base(3*day), now)
	if act != actRun || root.Mode != fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL || next != now.Add(day) || unchanged != 0 ||
		!root.Scope.CommentsWatermark.AsTime().Equal(newest) || root.Scope.ReportedCommentCount != 7 {
		t.Fatalf("3d: %v %+v %v", act, root, next)
	}
	// Seen equals the last reported total, so the post stage does not take
	// replies (total 7 vs 5 top-level stored) for growth and recrawl.
	if root.Scope.CommentsSeen != root.Scope.ReportedCommentCount {
		t.Fatalf("seen %d, reported %d", root.Scope.CommentsSeen, root.Scope.ReportedCommentCount)
	}

	// Nothing stored yet: read it all.
	d := base(3 * day)
	d.newestComment = nil
	if _, root, _, _, _ := plan(pr, "T1", d, now); root.Mode != fbv1.CrawlMode_CRAWL_MODE_FULL || root.Scope.CommentsWatermark != nil {
		t.Fatalf("no watermark: %+v", root)
	}

	// Two refreshes in a row found nothing: the interval doubles twice.
	d = base(3 * day)
	d.refreshes, d.unchanged, d.lastCount = 2, 1, ptr(int64(5))
	if _, _, next, unchanged, _ := plan(pr, "T1", d, now); unchanged != 2 || next != now.Add(4*day) {
		t.Fatalf("backoff: unchanged=%d next=%v", unchanged, next.Sub(now))
	}
	// ...capped at 2^max_backoff, and reset when comments arrive.
	d.unchanged = 9
	if _, _, next, _, _ := plan(pr, "T1", d, now); next != now.Add(8*day) {
		t.Fatalf("backoff cap: %v", next.Sub(now))
	}
	d.lastCount = ptr(int64(3))
	if _, _, next, unchanged, _ := plan(pr, "T1", d, now); unchanged != 0 || next != now.Add(day) {
		t.Fatalf("reset: %d %v", unchanged, next.Sub(now))
	}

	// 7 days with enough comments: one full recrawl, then the 72h window.
	d = base(7*day + time.Hour)
	d.collected = ptr(int64(40))
	act, root, next, _, recrawls := plan(pr, "T1", d, now)
	if act != actRun || root.Mode != fbv1.CrawlMode_CRAWL_MODE_RECRAWL || recrawls != 1 || next != now.Add(3*day) {
		t.Fatalf("7d recrawl: %v %v recrawls=%d next=%v", act, root.Mode, recrawls, next.Sub(now))
	}
	// A quiet post at 7 days is not recrawled.
	d = base(7*day + time.Hour)
	if _, root, _, _, recrawls := plan(pr, "T1", d, now); root.Mode != fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL || recrawls != 1 {
		t.Fatalf("quiet 7d: %v %d", root.Mode, recrawls)
	}

	// 20 days on a T2 page: outside its windows, but a busy post waits for
	// its 30-day recrawl.
	d = base(20 * day)
	d.collected, d.recrawls = ptr(int64(40)), 1
	act, _, next, _, _ = plan(pr, "T2", d, now)
	if act != actPostpone || next != d.created.Add(30*day) {
		t.Fatalf("T2 20d: %v %v", act, next)
	}
	// The same post on a T1 page is refreshed weekly.
	if act, root, next, _, _ := plan(pr, "T1", d, now); act != actRun || root.Mode != fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL || next != now.Add(7*day) {
		t.Fatalf("T1 20d: %v %v %v", act, root.Mode, next.Sub(now))
	}
	// A quiet one is dropped.
	d.collected = ptr(int64(2))
	if act, _, _, _, _ := plan(pr, "T2", d, now); act != actDrop {
		t.Fatalf("quiet T2 20d: %v", act)
	}

	// 30 days: the last recrawl, then nothing.
	d = base(30*day + time.Hour)
	d.collected, d.recrawls = ptr(int64(40)), 1
	act, root, _, _, recrawls = plan(pr, "T1", d, now)
	if act != actRun || root.Mode != fbv1.CrawlMode_CRAWL_MODE_RECRAWL || recrawls != 2 {
		t.Fatalf("30d: %v %v %d", act, root.Mode, recrawls)
	}
	d.recrawls = 2
	if act, _, _, _, _ := plan(pr, "T1", d, now); act != actDrop {
		t.Fatalf("after the last recrawl: %v", act)
	}
}
