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
