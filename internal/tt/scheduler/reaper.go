package scheduler

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Reaper marks jobs that stopped making progress as partial (or failed when
// their accounting state is gone), as fbs's does.
type Reaper struct {
	DB       *pgxpool.Pool
	Jobs     *jobs.Tracker
	Producer *kafka.Producer
	// After is how long a job may go without progress. While tasks wait in
	// its lane's topics (Backlog), its own are taken to be queued behind
	// them, not lost, and it is left alone until MaxAge.
	After  time.Duration
	MaxAge time.Duration
	// Backlog returns the tasks waiting per lane (TaskBacklog); nil takes
	// every lane to be empty.
	Backlog func(context.Context) (map[int16]int64, error)
	// PageSize bounds the jobs one tick looks at (default 500).
	PageSize int

	// The tick after a full page goes on from its last job (by creation),
	// so more than a page of long-running jobs can't keep a stuck one from
	// ever being looked at.
	afterCreated time.Time
	afterID      uuid.UUID
}

// stalled decides what becomes of a job without progress: failed when its
// accounting state is gone, partial when its tasks look lost (no progress
// for after while nothing waits in its lane, or older than maxAge).
func stalled(st jobs.Status, queued, overdue bool, after time.Duration, now time.Time) (final, reason string) {
	switch {
	case st.Spawned == 0:
		return "failed", "job state lost"
	case st.Pending > 0 && now.Sub(st.LastActivity) > after && (!queued || overdue):
		return "partial", "stalled with pending tasks"
	}
	return "", ""
}

// Tick closes the stalled jobs of one page and announces each on
// tt.job.v1.
func (r *Reaper) Tick(ctx context.Context) error {
	maxAge := max(r.MaxAge, r.After)
	page := cmp.Or(r.PageSize, 500)
	rows, err := r.DB.Query(ctx, `SELECT job_id, kind, account_ref, coalesce(user_id, 0), created_at, lane,
			created_at < now() - $2::interval FROM jobs
		WHERE status IN ('running','cancelling') AND created_at < now() - $1::interval AND (created_at, job_id) > ($3, $4)
		ORDER BY created_at, job_id LIMIT $5`, r.After.String(), maxAge.String(), r.afterCreated, r.afterID, page)
	if err != nil {
		return err
	}
	type row struct {
		id      uuid.UUID
		kind    string
		ref     string
		userID  int64
		created time.Time
		lane    int16
		overdue bool
	}
	list, err := pgx.CollectRows(rows, func(c pgx.CollectableRow) (row, error) {
		var x row
		return x, c.Scan(&x.id, &x.kind, &x.ref, &x.userID, &x.created, &x.lane, &x.overdue)
	})
	if err != nil {
		return err
	}
	if len(list) < page {
		r.afterCreated, r.afterID = time.Time{}, uuid.UUID{} // all seen: start over next tick
	} else {
		r.afterCreated, r.afterID = list[len(list)-1].created, list[len(list)-1].id
	}
	if len(list) == 0 {
		return nil
	}
	queued := func(int16) bool { return false }
	if r.Backlog != nil {
		backlog, err := r.Backlog(ctx)
		switch {
		case err != nil:
			// Unknown: take every lane to be busy, so only jobs past MaxAge
			// (or without state) are closed until the backlog reads again.
			slog.Warn("reaper: task backlog", "err", err)
			queued = func(int16) bool { return true }
		default:
			queued = func(l int16) bool { return backlog[l] > 0 }
		}
	}
	now := time.Now()
	for _, j := range list {
		st, err := r.Jobs.Status(ctx, j.id.String())
		if err != nil {
			continue
		}
		final, reason := stalled(st, queued(j.lane), j.overdue, r.After, now)
		if final == "" {
			continue
		}
		eb, _ := json.Marshal(map[string]any{"message": reason, "pending_tasks": st.Pending})
		cb, _ := json.Marshal(st.Counters)
		var prev string
		if err := r.DB.QueryRow(ctx, `UPDATE jobs SET status = $2, error = $3, counters = $4, finished_at = now()
			WHERE job_id = $1 AND status IN ('running','cancelling') RETURNING old.status`, j.id, final, eb, cb).Scan(&prev); err != nil {
			continue // finalized meanwhile, or the database failed: next tick
		}
		b := r.Producer.NewBatch()
		err = b.AddProto(ctx, topics.Job, j.id.String(), &commonv1.JobEvent{EventId: j.id.String() + ":" + final, JobId: j.id.String(),
			Status: jobStatus(final), Kind: j.kind, SubjectRef: j.ref, SubjectId: j.userID, CreatedAt: timestamppb.New(j.created),
			FinishedAt: timestamppb.Now(), Counters: st.Counters, Errors: []string{reason}, Platform: commonv1.Platform_PLATFORM_TIKTOK})
		if err == nil {
			err = b.Wait(ctx)
		}
		if err != nil {
			// Not announced: reopen the job, so the next tick closes it
			// again and sends the event.
			if _, rerr := r.DB.Exec(ctx, `UPDATE jobs SET status = $2, error = NULL, finished_at = NULL WHERE job_id = $1 AND status = $3`,
				j.id, prev, final); rerr != nil {
				slog.Error("reaper: job closed but its event was not sent", "job", j.id, "err", errors.Join(err, rerr))
			}
			continue
		}
		// The schedule counts it as a failed check, so a stuck account
		// backs off.
		if _, err := r.DB.Exec(ctx, `UPDATE account_schedule SET consecutive_failures = consecutive_failures + 1
			WHERE last_job_id = $1`, j.id); err != nil {
			slog.Warn("reaper: schedule not updated", "job", j.id, "err", err)
		}
		metrics.Jobs.WithLabelValues(final, topics.LaneName(tkstaskv1.Lane(j.lane))).Inc()
		slog.Warn("reaper: job closed", "job", j.id, "status", final, "reason", reason)
	}
	return nil
}

// CrawlerGroups is the prefix of tks's crawler consumer groups
// (app.ConsumerGroup: tks-tt-crawler-<stage>-<lane>).
const CrawlerGroups = "tks-tt-crawler-"

// TaskBacklog returns how many crawl tasks (account, post and thread) wait
// in each lane's topics, unconsumed or unfinished, keyed by lane as stored
// in jobs.lane. Lanes nothing waits in are absent.
func TaskBacklog(ctx context.Context, admin *kadm.Client, prefix string) (map[int16]int64, error) {
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	var groups []string
	for _, g := range listed.Groups() {
		if strings.HasPrefix(g, prefix+CrawlerGroups) {
			groups = append(groups, g)
		}
	}
	out := map[int16]int64{}
	if len(groups) == 0 {
		return out, nil
	}
	lags, err := admin.Lag(ctx, groups...)
	if err != nil {
		return nil, err
	}
	for _, gl := range lags {
		if err := gl.Error(); err != nil {
			return nil, err
		}
		for _, tl := range gl.Lag.TotalByTopic() {
			if lane, ok := topicLane(strings.TrimPrefix(tl.Topic, prefix)); ok && tl.Lag > 0 {
				out[int16(lane)] += tl.Lag
			}
		}
	}
	return out, nil
}

// topicLane returns the lane of an account, post or thread task topic.
func topicLane(topic string) (tkstaskv1.Lane, bool) {
	rest, ok := strings.CutPrefix(topic, "tks.task.")
	if !ok {
		return 0, false
	}
	stage, lane, ok := strings.Cut(rest, ".")
	if !ok || (stage != "account" && stage != "post" && stage != "thread") {
		return 0, false
	}
	switch lane {
	case "p0", "p1", "p2":
		return topics.ParseLane(lane), true
	}
	return 0, false
}
