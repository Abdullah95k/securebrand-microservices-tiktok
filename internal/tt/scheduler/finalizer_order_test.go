package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// The finalizer takes on-demand jobs first, then checks, then the backfill
// lane, the oldest first within a lane; a job it cannot find goes last.
func TestFinalizerByLane(t *testing.T) {
	e := newEnv(t)
	f := &Finalizer{DB: e.db}
	ids := make([]string, 5)
	lanes := []int{3, 2, 3, 1, 2}
	// Within a lane, the oldest first: 4 is older than 1, 2 older than 0.
	ages := []time.Duration{time.Hour, 2 * time.Hour, 3 * time.Hour, time.Hour, 4 * time.Hour}
	for i, lane := range lanes {
		id := uuid.New()
		ids[i] = id.String()
		e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at)
			VALUES ($1, 'scheduled', $2, 'incremental', 'id:1', '{}', 'running', now() - make_interval(secs => $3))`, id, lane, ages[i].Seconds())
	}
	unknown := uuid.New().String()
	got := f.byLane(context.Background(), append([]string{unknown}, ids...))
	want := []string{ids[3], ids[4], ids[1], ids[2], ids[0], unknown}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order %v, want %v", got, want)
		}
	}
}

// A job ready longer than Timeout (its tasks' last activity) is finalised
// at once, without waiting for the persister again: a failed finalise or a
// leader change would otherwise restart its wait every time.
func TestFinalizerWaitedJob(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	id := uuid.New()
	e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at)
		VALUES ($1, 'scheduled', 2, 'incremental', 'id:1', '{}', 'running', now())`, id)
	if err := e.trk.Spawn(ctx, id.String(), "t1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.trk.Done(ctx, id.String(), "t1", nil); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	f := &Finalizer{DB: e.db, Jobs: e.trk, Producer: e.prod, Cfg: e.cfg, Timeout: 10 * time.Minute, Now: func() time.Time { return at }}
	if f.waited(ctx, id.String()) {
		t.Fatal("a job ready now counts as waited")
	}
	at = at.Add(11 * time.Minute)
	if !f.waited(ctx, id.String()) {
		t.Fatal("a job ready 11 minutes does not count as waited")
	}
	if f.waited(ctx, uuid.NewString()) {
		t.Fatal("an unknown job counts as waited")
	}
	// Ticked then, it is finalised in that tick, with no batch held for it.
	if err := f.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var st string
	if err := e.db.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, id).Scan(&st); err != nil || st == "running" {
		t.Fatalf("status %q (%v), want terminal", st, err)
	}
	if f.held[id.String()] || len(f.pending) != 0 {
		t.Fatalf("held %v, pending %d", f.held, len(f.pending))
	}
}

// At most finalize_batch jobs are finalised a tick, checks first; the rest
// stay in the set for the next tick.
func TestFinalizerBatch(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cfg := *e.cfg
	cfg.Scheduler.FinalizeBatch, cfg.Scheduler.FinalizeWorkers = 2, 1
	var ids []uuid.UUID
	for _, lane := range []int{3, 2, 3} {
		id := uuid.New()
		ids = append(ids, id)
		e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at)
			VALUES ($1, 'scheduled', $2, 'incremental', 'id:1', '{}', 'running', now())`, id, lane)
		if err := e.trk.Spawn(ctx, id.String(), "t1"); err != nil {
			t.Fatal(err)
		}
		if _, err := e.trk.Done(ctx, id.String(), "t1", nil); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Now().Add(11 * time.Minute) // all three have waited
	f := &Finalizer{DB: e.db, Jobs: e.trk, Producer: e.prod, Cfg: &cfg, Timeout: 10 * time.Minute, Now: func() time.Time { return at }}
	running := func() map[uuid.UUID]bool {
		out := map[uuid.UUID]bool{}
		for _, id := range ids {
			var st string
			if err := e.db.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, id).Scan(&st); err != nil {
				t.Fatal(err)
			}
			out[id] = st == "running"
		}
		return out
	}
	if err := f.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	r := running()
	if r[ids[1]] {
		t.Fatal("the check was not finalised first")
	}
	if n := map[bool]int{}; true {
		for _, v := range r {
			n[v]++
		}
		if n[true] != 1 {
			t.Fatalf("running after one tick: %v, want one of three", r)
		}
	}
	if err := f.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	for id, v := range running() {
		if v {
			t.Fatalf("%s still running after the second tick", id)
		}
	}
}

// A post's stored comments are counted when it is first read, and again
// only once coverage_recount_after has passed; in between a read keeps the
// counts and still writes the state and the read time.
func TestCoverageRecount(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	created := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
	if _, err := ttstore.EnsurePartitions(ctx, e.db, time.Now(), store.PartitionWindow{MonthsBack: 1, MonthsAhead: 1, DaysBack: 3, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	e.exec(t, `INSERT INTO posts (created_at, post_id, user_id, kind, content_hash, first_seen_at, last_seen_at, comment_count)
		VALUES ($1, 77, 1, 'video', '\x00', now(), now(), 4)`, created)
	addComment := func(id int64, depth int) {
		var parent *int64
		if depth > 0 {
			p := int64(1)
			parent = &p
		}
		e.exec(t, `INSERT INTO comments (post_created_at, post_id, comment_id, user_id, depth, parent_comment_id, author_key,
			content_hash, first_seen_at, last_seen_at) VALUES ($1, 77, $2, 1, $3, $4, '\x00', '\x00', now(), now())`, created, id, depth, parent)
	}
	addComment(1, 0)
	addComment(2, 1)
	at := time.Now()
	cfg := *e.cfg
	cfg.Scheduler.CoverageRecountAfter = 6 * time.Hour
	f := &Finalizer{DB: e.db, Cfg: &cfg, Now: func() time.Time { return at }}
	posts := []jobs.PostState{{PostID: 77, CreatedAt: created, Reported: 4, State: jobs.PostComplete}}
	read := func() (top, replies int64, ratio float64) {
		t.Helper()
		_, out, err := f.coverage(ctx, posts, "p1")
		if err != nil || len(out) != 1 {
			t.Fatalf("coverage: %v %v", out, err)
		}
		return out[0].Collected, out[0].RepliesCollected, out[0].Completeness
	}
	if top, rep, ratio := read(); top != 1 || rep != 1 || ratio != 0.5 {
		t.Fatalf("first read: %d top, %d replies, %v", top, rep, ratio)
	}
	addComment(3, 0)
	at = at.Add(time.Hour)
	if top, rep, ratio := read(); top != 1 || rep != 1 || ratio != 0.5 {
		t.Fatalf("read within the window: %d top, %d replies, %v (want the kept counts)", top, rep, ratio)
	}
	var synced time.Time
	if err := e.db.QueryRow(ctx, `SELECT comments_synced_at FROM posts WHERE post_id = 77`).Scan(&synced); err != nil ||
		!synced.Equal(at.UTC().Truncate(time.Microsecond)) {
		t.Fatalf("synced %v (%v), want the second read's time", synced, err)
	}
	at = at.Add(6 * time.Hour)
	if top, rep, ratio := read(); top != 2 || rep != 1 || ratio != 0.75 {
		t.Fatalf("read after the window: %d top, %d replies, %v", top, rep, ratio)
	}
}
