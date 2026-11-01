package e2e

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scheduler"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
)

// The reaper leaves a quiet job alone while tasks wait in its lane, closes
// it once the lane is empty or the job is past its age cap, and fails a job
// whose accounting state is gone.
func TestReaperWaitsForTheLaneBacklog(t *testing.T) {
	s := Start(t, Options{Spec: fakefb.Spec{Seed: 5, Pages: 1, PostsPerPage: 1, MaxComments: 1}})
	ctx := context.Background()
	job := func(age time.Duration, spawn bool) uuid.UUID {
		id := uuid.Must(uuid.NewV7())
		if _, err := s.DB.Exec(ctx, `INSERT INTO jobs (job_id, kind, lane, mode, page_ref, params, status, created_at)
			VALUES ($1, 'backfill', 3, 'full', 'v:reaper', '{}', 'running', now() - $2::interval)`, id, age.String()); err != nil {
			t.Fatal(err)
		}
		if spawn {
			if err := s.tracker.Spawn(ctx, id.String(), id.String()+":0"); err != nil {
				t.Fatal(err)
			}
			// No progress for two hours.
			if err := s.Redis.Set(ctx, redisx.KeyPrefix+"{job:"+id.String()+"}:last", strconv.FormatInt(time.Now().Add(-2*time.Hour).Unix(), 10), 0).Err(); err != nil {
				t.Fatal(err)
			}
		}
		return id
	}
	status := func(id uuid.UUID) string {
		var st string
		if err := s.DB.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, id).Scan(&st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	queued, old, lost := job(3*time.Hour, true), job(30*time.Hour, true), job(time.Hour, false)

	r := &scheduler.Reaper{DB: s.DB, Jobs: s.tracker, Producer: s.Producer, After: 30 * time.Minute, MaxAge: 24 * time.Hour,
		Backlog: func(context.Context) (map[int16]int64, error) { return map[int16]int64{3: 40}, nil }}
	if err := r.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if st := status(queued); st != "running" {
		t.Fatalf("a job queued in a busy lane: %s", st)
	}
	if st, st2 := status(old), status(lost); st != "partial" || st2 != "failed" {
		t.Fatalf("past the age cap: %s; state lost: %s", st, st2)
	}

	// The real backlog: a task waits in the backfill lane while the crawler
	// is down, and the job is left alone.
	admin := kadm.NewClient(s.Producer.Client())
	r.Backlog = func(ctx context.Context) (map[int16]int64, error) {
		return scheduler.TaskBacklog(ctx, admin, s.Cfg.Kafka.TopicPrefix)
	}
	topic := s.Producer.Topic(topics.Task(taskv1.Kind_KIND_PAGE, taskv1.Lane_LANE_P2))
	junk := func() {
		if err := s.Producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("not a task")}); err != nil {
			t.Fatal(err)
		}
	}
	// The lane's group commits once, as a long-running one has.
	junk()
	group := s.Cfg.Kafka.TopicPrefix + "tks-crawler-page-p2"
	waitFor(t, "the lane group commits", time.Minute, func() bool {
		o, err := admin.FetchOffsets(ctx, group)
		return err == nil && len(o[topic]) > 0
	})
	s.stop("crawler")
	junk()
	if b, err := r.Backlog(ctx); err != nil || b[3] != 1 {
		t.Fatalf("backlog with the crawler down: %v, err %v", b, err)
	}
	if err := r.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if st := status(queued); st != "running" {
		t.Fatalf("a job behind the lane's backlog: %s", st)
	}

	// Once the crawler has taken it (to the dead-letter topic), the lane is
	// empty and the quiet job is closed.
	s.RestartCrawler(t)
	waitFor(t, "the lane drains", time.Minute, func() bool {
		b, err := r.Backlog(ctx)
		return err == nil && b[3] == 0
	})
	if err := r.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if st := status(queued); st != "partial" {
		t.Fatalf("a quiet job in an empty lane: %s", st)
	}
}
