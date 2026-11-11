package scheduler

import (
	"context"
	"testing"
	"time"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

var walks = []config.MetricsWalk{{Every: 24 * time.Hour, Back: 720 * time.Hour}, {Every: 168 * time.Hour, Back: 8760 * time.Hour}}

func TestDueWalk(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	daily, weekly := WalkKey(walks[0]), WalkKey(walks[1])
	cases := map[string]struct {
		last map[string]time.Time
		want time.Duration // 0: none due
	}{
		"never walked: the deepest": {map[string]time.Time{}, 8760 * time.Hour},
		"both fresh":                {map[string]time.Time{daily: now.Add(-time.Hour), weekly: now.Add(-time.Hour)}, 0},
		"daily due":                 {map[string]time.Time{daily: now.Add(-25 * time.Hour), weekly: now.Add(-48 * time.Hour)}, 720 * time.Hour},
		"weekly due covers daily":   {map[string]time.Time{daily: now.Add(-25 * time.Hour), weekly: now.Add(-169 * time.Hour)}, 8760 * time.Hour},
		"weekly due alone":          {map[string]time.Time{daily: now.Add(-time.Hour), weekly: now.Add(-168 * time.Hour)}, 8760 * time.Hour},
	}
	for name, c := range cases {
		w, ok := DueWalk(walks, c.last, now)
		switch {
		case c.want == 0 && ok:
			t.Errorf("%s: walk %v due, want none", name, w.Back)
		case c.want != 0 && (!ok || w.Back != c.want):
			t.Errorf("%s: walk %v (%v), want %v", name, w.Back, ok, c.want)
		}
	}
	if _, ok := DueWalk(nil, nil, now); ok {
		t.Error("a walk is due with none configured")
	}
}

func TestWalksDone(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	daily, weekly := WalkKey(walks[0]), WalkKey(walks[1])
	if daily != "2592000" || weekly != "31536000" {
		t.Fatalf("keys %s %s", daily, weekly)
	}
	if got := WalksDone(walks, dispatch.ModeFull, "", at); len(got) != 2 || !got[daily].Equal(at) || !got[weekly].Equal(at) {
		t.Errorf("a backfill covers %v, want both walks", got)
	}
	if got := WalksDone(walks, dispatch.ModeRefresh, daily, at); len(got) != 1 || !got[daily].Equal(at) {
		t.Errorf("the daily walk covers %v, want itself", got)
	}
	if got := WalksDone(walks, dispatch.ModeRefresh, weekly, at); len(got) != 2 {
		t.Errorf("the weekly walk covers %v, want both", got)
	}
	if got := WalksDone(walks, dispatch.ModeIncremental, "", at); len(got) != 0 {
		t.Errorf("a check covers %v, want none", got)
	}
	if got := WalksDone(walks, dispatch.ModeRefresh, "bad", at); len(got) != 0 {
		t.Errorf("a walk without its depth covers %v", got)
	}
	// Round trip through account_schedule.walks.
	if got := Walks([]byte(`{"2592000":"2026-11-01T12:00:00Z"}`)); !got[daily].Equal(at) {
		t.Errorf("Walks read %v", got)
	}
}

func TestStatus(t *testing.T) {
	cases := map[string]struct {
		cancelling bool
		counters   map[string]int64
		comp       Completeness
		want       string
	}{
		"clean":                 {false, map[string]int64{"posts": 3}, Completeness{Posts: 2, PostsComplete: 2}, "completed"},
		"capped comments":       {false, map[string]int64{"posts": 3}, Completeness{Posts: 1, PostsCapped: 1}, "completed"},
		"private account":       {false, map[string]int64{"accounts_private": 1}, Completeness{}, "completed"},
		"banned account":        {false, map[string]int64{"accounts_banned": 1}, Completeness{}, "completed"},
		"unknown account":       {false, map[string]int64{"accounts_unavailable": 1}, Completeness{}, "failed"},
		"dead-lettered comment": {false, map[string]int64{"posts": 3}, Completeness{Posts: 1, PostsFailed: 1}, "partial"},
		"stuck comments":        {false, map[string]int64{"posts": 3}, Completeness{Posts: 1, PostsTruncated: 1}, "partial"},
		"stuck list":            {false, map[string]int64{"posts": 3, "lists_truncated": 1}, Completeness{}, "partial"},
		"failed task":           {false, map[string]int64{"posts": 3, "tasks_failed": 1}, Completeness{}, "partial"},
		"cancelled":             {true, map[string]int64{"tasks_failed": 1}, Completeness{}, "cancelled"},
	}
	for name, c := range cases {
		if got := Status(c.cancelling, c.counters, c.comp); got != c.want {
			t.Errorf("%s: %s, want %s", name, got, c.want)
		}
	}
}

func TestTopicLane(t *testing.T) {
	for topic, want := range map[string]int{
		"tks.task.account.p0": int(tkstaskv1.Lane_LANE_P0), "tks.task.post.p1": int(tkstaskv1.Lane_LANE_P1),
		"tks.task.thread.p2": int(tkstaskv1.Lane_LANE_P2), "tks.task.media": -1, "tks.task.retry.30s": -1,
		"fbs.task.page.p1": -1, "tks.task.account.p9": -1,
	} {
		l, ok := topicLane(topic)
		if (want < 0) == ok || ok && int(l) != want {
			t.Errorf("%s: lane %v %v, want %d", topic, l, ok, want)
		}
	}
}

func TestStalled(t *testing.T) {
	now := time.Now()
	old := jobs.Status{Spawned: 2, Pending: 1, LastActivity: now.Add(-time.Hour)}
	if f, _ := stalled(jobs.Status{}, false, false, time.Minute, now); f != "failed" {
		t.Errorf("lost state: %q", f)
	}
	if f, _ := stalled(old, false, false, time.Minute, now); f != "partial" {
		t.Errorf("stalled: %q", f)
	}
	if f, _ := stalled(old, true, false, time.Minute, now); f != "" {
		t.Errorf("queued behind a backlog: %q", f)
	}
	if f, _ := stalled(old, true, true, time.Minute, now); f != "partial" {
		t.Errorf("overdue: %q", f)
	}
}

func TestApplyOptions(t *testing.T) {
	l := &tkstaskv1.Limits{MaxAgeSeconds: 365 * 86400, IncludeComments: true, MaxCommentsPerPost: 1000}
	applyOptions(l, []byte(`{"max_age_days": 30, "include_comments": false, "max_comments_per_post": 50, "video_policy": "none"}`))
	if l.MaxAgeSeconds != 30*86400 || l.IncludeComments || l.MaxCommentsPerPost != 50 {
		t.Errorf("limits %+v", l)
	}
	applyOptions(l, []byte(`not json`))
	if l.MaxAgeSeconds != 30*86400 {
		t.Error("bad options changed the limits")
	}
}

// While the budget pauses the backfill lane, a due metrics walk waits and
// the account's check runs in its place, in the checks' lane: a walk stands
// in for the check, so a paused walk would leave the account unchecked.
func TestPausedBackfillKeepsChecks(t *testing.T) {
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO accounts (user_id, sec_uid, username, author_key, status, profile_refreshed_at, page_read_at)
			VALUES (9, 'SEC9', 'nine', '\x01', 'active', now(), now())`,
		`INSERT INTO account_usernames (user_id, username, first_seen_at, last_seen_at) VALUES (9, 'nine', now(), now())`,
		`INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', 9, 'complete')`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	p := &Planner{DB: db, Cfg: cfg}
	d := due{ref: "u:nine"} // no walk recorded: the deepest is due
	r, err := p.request(ctx, d, false)
	if err != nil || r.Lane != tkstaskv1.Lane_LANE_P2 || r.Mode != dispatch.ModeRefresh {
		t.Fatalf("a due walk: %+v %v", r, err)
	}
	r, err = p.request(ctx, d, true)
	if err != nil || r.Lane != tkstaskv1.Lane_LANE_P1 || r.Mode != dispatch.ModeIncremental || r.Params != nil {
		t.Fatalf("backfill paused: want the check, got %+v %v", r, err)
	}
}

// An account never read: its backfill in p2, or, while the budget pauses
// p2 and tt.freshness.first_read_days is on, a light first read of those
// days in the check lane (no walk recorded by it, so the year-deep walk is
// due afterwards).
func TestFirstReadWhileBackfillPaused(t *testing.T) {
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TT.Freshness.FirstReadDays != 3 {
		t.Fatalf("first_read_days default %d, want 3", cfg.TT.Freshness.FirstReadDays)
	}
	for _, q := range []string{
		`INSERT INTO accounts (user_id, sec_uid, username, author_key, status, profile_refreshed_at, page_read_at)
			VALUES (8, 'SEC8', 'eight', '\x01', 'active', now(), now())`,
		`INSERT INTO account_usernames (user_id, username, first_seen_at, last_seen_at) VALUES (8, 'eight', now(), now())`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	p := &Planner{DB: db, Cfg: cfg}
	d := due{ref: "u:eight"} // no list cursor: never read
	r, err := p.request(ctx, d, false)
	if err != nil || r.Kind != dispatch.KindBackfill || r.Lane != tkstaskv1.Lane_LANE_P2 || r.Mode != dispatch.ModeFull {
		t.Fatalf("p2 running: want the backfill, got %+v %v", r, err)
	}
	r, err = p.request(ctx, d, true)
	if err != nil || r.Lane != tkstaskv1.Lane_LANE_P1 || r.Mode != dispatch.ModeIncremental || r.Limits.GetMaxAgeSeconds() != 3*86400 ||
		r.Params["first_read"] != true {
		t.Fatalf("p2 paused: want the 3-day first read in p1, got %+v %v", r, err)
	}
	if done := WalksDone(cfg.TT.Freshness.MetricsWalks, r.Mode, "", time.Now()); len(done) != 0 {
		t.Errorf("the first read records walks %v; the year-deep walk must stay due", done)
	}
	cfg.TT.Freshness.FirstReadDays = 0
	r, err = p.request(ctx, d, true)
	if err != nil || r.Mode != dispatch.ModeFull || r.Lane != tkstaskv1.Lane_LANE_P2 {
		t.Fatalf("first reads off: want the backfill, got %+v %v", r, err)
	}
}
