package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestAccountingSurvivesDuplicatesAndRetries(t *testing.T) {
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	tr := New(r)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(tr.Spawn(ctx, "j1", "root"))
	// root spawns two children (and a duplicate spawn of one of them).
	must(tr.Spawn(ctx, "j1", "a", "b"))
	must(tr.Spawn(ctx, "j1", "a"))
	if p, _ := tr.Done(ctx, "j1", "root", map[string]int64{"posts": 2}); p != 2 {
		t.Fatalf("pending after root = %d", p)
	}
	// Redelivered root: no double counting.
	if p, _ := tr.Done(ctx, "j1", "root", map[string]int64{"posts": 2}); p != 2 {
		t.Fatalf("pending after duplicate root = %d", p)
	}
	if _, err := tr.Done(ctx, "j1", "a", nil); err != nil {
		t.Fatal(err)
	}
	if ready, _ := tr.ReadyToFinalize(ctx, 10); len(ready) != 0 {
		t.Fatalf("job finalised early: %v", ready)
	}
	if p, _ := tr.Done(ctx, "j1", "b", map[string]int64{"comments": 5}); p != 0 {
		t.Fatalf("pending at end = %d", p)
	}
	ready, _ := tr.ReadyToFinalize(ctx, 10)
	if len(ready) != 1 || ready[0] != "j1" {
		t.Fatalf("finalize set = %v", ready)
	}
	// Still there until finalised.
	if again, _ := tr.ReadyToFinalize(ctx, 10); len(again) != 1 {
		t.Fatalf("finalize set after a read = %v", again)
	}
	must(tr.Finalized(ctx, "j1"))
	if left, _ := tr.ReadyToFinalize(ctx, 10); len(left) != 0 {
		t.Fatalf("finalize set after Finalized = %v", left)
	}
	st, _ := tr.Status(ctx, "j1")
	if st.Counters["posts"] != 2 || st.Counters["comments"] != 5 || st.Spawned != 3 || st.Done != 3 {
		t.Fatalf("status = %+v", st)
	}
	if _, err := tr.Done(ctx, "j1", "never-spawned", nil); err == nil {
		t.Fatal("done for an unspawned task must fail")
	}
}

func TestPostStates(t *testing.T) {
	mr := miniredis.RunT(t)
	tr := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	ctx := context.Background()
	_, _ = tr.SetPost(ctx, "j", 7, 1700000000, 40, PostPending)
	_ = tr.UpdatePostState(ctx, "j", 7, PostComplete)
	ps, err := tr.Posts(ctx, "j")
	if err != nil || len(ps) != 1 || ps[0].State != PostComplete || ps[0].Reported != 40 {
		t.Fatalf("posts = %+v %v", ps, err)
	}
	// The timeline reaching a post the job already collected keeps its
	// state, takes the newer count and says so.
	if collected, err := tr.SetPost(ctx, "j", 7, 1700000000, 41, PostPending); err != nil || !collected {
		t.Fatalf("SetPost on a collected post: %v, %v", collected, err)
	}
	if ps, _ := tr.Posts(ctx, "j"); ps[0].State != PostComplete || ps[0].Reported != 41 {
		t.Fatalf("a collected post became %+v", ps[0])
	}
	// A pending post can be recorded again.
	if collected, _ := tr.SetPost(ctx, "j", 8, 1700000000, 3, PostPending); collected {
		t.Fatal("a new post reported as collected")
	}
	_, _ = tr.SetPost(ctx, "j", 8, 1700000000, 5, PostSkipped)
	for _, p := range func() []PostState { ps, _ := tr.Posts(ctx, "j"); return ps }() {
		if p.PostID == 8 && (p.State != PostSkipped || p.Reported != 5) {
			t.Fatalf("post 8 = %+v", p)
		}
	}
}

func TestFullPasses(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	tr := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	job := "job-1"
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	created := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)

	if err := tr.BeginPass(ctx, job, "lineage-a", 42, start); err != nil {
		t.Fatal(err)
	}
	// A retried root keeps the first start.
	if err := tr.BeginPass(ctx, job, "lineage-a", 42, start.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	// A second lineage of the same post that never ends its pass.
	if err := tr.BeginPass(ctx, job, "lineage-b", 42, start); err != nil {
		t.Fatal(err)
	}
	if err := tr.BeginPass(ctx, job, "lineage-c", 7, start); err != nil {
		t.Fatal(err)
	}
	if in, err := tr.InPass(ctx, job, "lineage-a"); err != nil || !in {
		t.Fatalf("lineage-a in pass = %v, %v", in, err)
	}
	if in, _ := tr.InPass(ctx, job, "lineage-z"); in {
		t.Fatal("lineage-z was never begun")
	}
	// A retried page appends the same comments again.
	for _, ids := range [][]int64{{1, 2, 3}, {3, 4}, {3, 4}, {1 << 62}} {
		var cs []SeenComment
		for _, id := range ids {
			cs = append(cs, SeenComment{ID: id, Replies: id % 3})
		}
		if err := tr.AddSeen(ctx, job, 42, cs); err != nil {
			t.Fatal(err)
		}
	}
	if err := tr.EndPass(ctx, job, "lineage-a", 42, created, 9); err != nil {
		t.Fatal(err)
	}
	if err := tr.EndPass(ctx, job, "lineage-z", 42, created, 9); err == nil {
		t.Fatal("ending a pass that never began must fail")
	}

	done, posts, err := tr.Passes(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 1 || done[0].PostID != 42 || !done[0].PostCreated.Equal(created) || !done[0].Start.Equal(start) || done[0].Reported != 9 {
		t.Fatalf("passes %+v", done)
	}
	if len(posts) != 2 {
		t.Fatalf("posts with seen lists %v, want 42 and 7", posts)
	}
	seen, err := tr.Seen(ctx, job, 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 5 {
		t.Fatalf("seen %v", seen)
	}
	for _, id := range []int64{1, 2, 3, 4, 1 << 62} {
		if r, ok := seen[id]; !ok || r != id%3 {
			t.Fatalf("id %d: seen %v with %d replies (%v)", id, ok, r, seen)
		}
	}

	if err := tr.ClearPasses(ctx, job, posts); err != nil {
		t.Fatal(err)
	}
	done, posts, _ = tr.Passes(ctx, job)
	seen, _ = tr.Seen(ctx, job, 42)
	if len(done) != 0 || len(posts) != 0 || len(seen) != 0 {
		t.Fatalf("after clear: %v %v %v", done, posts, seen)
	}
}
