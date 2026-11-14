package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The finalizer takes on-demand jobs first, then checks, then the backfill
// lane, keeping their order within a lane; a job it cannot find goes last.
func TestFinalizerByLane(t *testing.T) {
	e := newEnv(t)
	f := &Finalizer{DB: e.db}
	ids := make([]string, 5)
	lanes := []int{3, 2, 3, 1, 2}
	for i, lane := range lanes {
		id := uuid.New()
		ids[i] = id.String()
		e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at)
			VALUES ($1, 'scheduled', $2, 'incremental', 'id:1', '{}', 'running', now())`, id, lane)
	}
	unknown := uuid.New().String()
	got := f.byLane(context.Background(), append([]string{unknown}, ids...))
	want := []string{ids[3], ids[1], ids[4], ids[0], ids[2], unknown}
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
