package stage

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
)

// Replayer readies dead-lettered tasks to run again (fbctl dlq replay).
//
// A dead letter is marked done, so that redeliveries and parked copies of
// it are skipped, and counted in its job as finished and failed. Sent back
// to its topic as it is, it would be skipped as done for a week (the
// mark's lifetime), and would be dead-lettered again on its first failure,
// its retry budget being spent. Prepare undoes all of that first.
type Replayer struct {
	Ckpt *checkpoint.Store
	Jobs *jobs.Tracker
	// DB re-opens a finalised job (nil: jobs stay as finalised).
	DB execer
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Replay is what Prepare did with one dead letter.
type Replay struct {
	// Task is the task to produce; nil to skip the dead letter (its job
	// was cancelled: the task would only finish as cancelled).
	Task *taskv1.Task
	// Reopened: the job counts the task as pending again, and a finalised
	// job is running again, to be finalised once more when the task (and
	// all it spawns) finishes. False when the job's state is gone (a day
	// after it was finalised): the task then runs outside job accounting,
	// and the job keeps its final state.
	Reopened bool
}

// Prepare readies the dead-lettered task t to run again: its done mark is
// cleared, its retry budget starts afresh, and its job is reopened.
// Preparing a task again is harmless: a task not done is not reopened
// twice.
func (r *Replayer) Prepare(ctx context.Context, t *taskv1.Task) (Replay, error) {
	if r.Jobs.Cancelled(ctx, t.JobId) {
		return Replay{}, nil
	}
	out := proto.Clone(t).(*taskv1.Task)
	out.Attempt, out.Stalls, out.Waits, out.NotBefore = 0, 0, 0, nil
	reopened, err := r.Jobs.Reopen(ctx, t.JobId, t.TaskId)
	if err != nil {
		return Replay{}, fmt.Errorf("replay: reopen job: %w", err)
	}
	if reopened && r.DB != nil {
		id, err := uuid.Parse(t.JobId)
		if err == nil {
			// The finalizer sets the final state again; a cancelled job
			// stays cancelled (and was skipped above anyway).
			if _, err := r.DB.Exec(ctx, `UPDATE jobs SET status = 'running', finished_at = NULL
				WHERE job_id = $1 AND status IN ('completed','partial','failed')`, id); err != nil {
				return Replay{}, fmt.Errorf("replay: reopen job: %w", err)
			}
		}
	}
	if err := r.Ckpt.ClearDone(ctx, t.TaskId); err != nil {
		return Replay{}, fmt.Errorf("replay: clear done: %w", err)
	}
	return Replay{Task: out, Reopened: reopened}, nil
}
