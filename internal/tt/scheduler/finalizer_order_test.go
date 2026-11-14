package scheduler

import (
	"context"
	"testing"

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
