package stage

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
)

// stub is a stage that returns what its next function says.
type stub struct{ next func(*taskv1.Task) Outcome }

func (s stub) Run(_ context.Context, t *taskv1.Task) Outcome { return s.next(t) }

// workerEnv is a worker on miniredis and a real broker (TKS_TEST_KAFKA_BROKERS),
// with topics under a fresh prefix.
func workerEnv(t *testing.T, stages map[taskv1.Kind]Stage) (*Worker, *redis.Client, *kgo.Client) {
	t.Helper()
	brokers := testenv.Kafka(t)
	r := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	prefix := testenv.TopicPrefix()
	kcfg := kafka.ClientConfig{Brokers: brokers, ClientID: "stage-test", TopicPrefix: prefix, Linger: time.Millisecond}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	specs := []kafka.TopicSpec{{Name: topics.DLQ, Partitions: 1}, {Name: topics.Task(taskv1.Kind_KIND_POST, taskv1.Lane_LANE_P1), Partitions: 1}}
	if err := kafka.EnsureTopics(context.Background(), p.Client(), prefix, specs, 1); err != nil {
		t.Fatal(err)
	}
	dlq, err := kgo.NewClient(append(kcfg.Opts(), kgo.ConsumeTopics(prefix+topics.DLQ), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dlq.Close)
	w := &Worker{D: &Deps{Producer: p, Jobs: jobs.New(r), Ckpt: checkpoint.New(r)}, Stages: stages, MaxAttempts: 3, LeaseTTL: time.Minute}
	return w, r, dlq
}

// nextDLQ returns the next dead letter.
func nextDLQ(t *testing.T, cl *kgo.Client) *kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatal("no dead letter")
		}
		if recs := fs.Records(); len(recs) > 0 {
			return recs[0]
		}
	}
}

// A task of a kind the worker has no stage for (an empty record, a newer
// producer, another service's task on a shared topic) is dead-lettered as
// received. Before M2 it panicked the crawler: dead-lettering it asked
// topics.Task for the kind's topic name, and the record came back after
// every restart.
func TestUnknownKindIsDeadLettered(t *testing.T) {
	w, _, dlq := workerEnv(t, map[taskv1.Kind]Stage{taskv1.Kind_KIND_POST: stub{func(*taskv1.Task) Outcome { return done(nil) }}})
	for _, k := range []taskv1.Kind{taskv1.Kind(200), taskv1.Kind_KIND_UNSPECIFIED, taskv1.Kind_KIND_THREAD} {
		b, _ := proto.Marshal(&taskv1.Task{TaskId: "x", Kind: k})
		rec := &kgo.Record{Topic: "some.topic", Key: []byte("k"), Value: b}
		if err := w.Handle(context.Background(), rec); err != nil {
			t.Fatalf("kind %v: %v", k, err)
		}
		got := nextDLQ(t, dlq)
		if kafka.HeaderValue(got, kafka.HeaderReason) != "no_stage" || kafka.HeaderValue(got, kafka.HeaderOrigTopic) != "some.topic" ||
			string(got.Value) != string(b) {
			t.Fatalf("kind %v: dead letter %s %v", k, got.Topic, got.Headers)
		}
	}
	if err := w.Run(context.Background(), &taskv1.Task{Kind: taskv1.Kind(200)}); err == nil {
		t.Fatal("Run of an unknown kind must fail, not panic")
	}
}

// A dead letter replayed after the fix runs again, and its job waits for
// it and is finalised again without the failure. Before M2 the replay was
// skipped as done for a week (fbs reference, Known issues).
func TestReplayedDeadLetterRunsAgain(t *testing.T) {
	ctx := context.Background()
	fixed := false
	runs := 0
	w, r, dlq := workerEnv(t, map[taskv1.Kind]Stage{taskv1.Kind_KIND_POST: stub{func(t *taskv1.Task) Outcome {
		runs++
		if !fixed {
			return Outcome{Result: Fail, Reason: "a bug", Counters: map[string]int64{"requests": 1}}
		}
		return done(map[string]int64{"comments": 5})
	}}})
	tr := jobs.New(r)
	root := task.NewPage("job-1", taskv1.Lane_LANE_P1, fbv1.CrawlMode_CRAWL_MODE_FULL, "v:x", 1, "", nil)
	post := task.NewPost(root, fbv1.CrawlMode_CRAWL_MODE_FULL, &taskv1.PostScope{PostId: 9, PageId: 1})
	if err := tr.Spawn(ctx, "job-1", root.TaskId, post.TaskId); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Done(ctx, "job-1", root.TaskId, nil); err != nil {
		t.Fatal(err)
	}

	// The bug: the task fails and is dead-lettered; its job finishes.
	if err := w.Run(ctx, post); err != nil {
		t.Fatal(err)
	}
	letter := nextDLQ(t, dlq)
	st, _ := tr.Status(ctx, "job-1")
	if st.Pending != 0 || st.Counters["tasks_failed"] != 1 {
		t.Fatalf("after the dead letter: %+v", st)
	}
	if ready, _ := tr.ReadyToFinalize(ctx, 10); len(ready) != 1 {
		t.Fatalf("job not queued for finalisation: %v", ready)
	}
	var dead taskv1.Task
	if err := proto.Unmarshal(letter.Value, &dead); err != nil {
		t.Fatal(err)
	}
	// As before M2: the record replayed as it is is skipped as done.
	if err := w.Run(ctx, &dead); err != nil || runs != 1 {
		t.Fatalf("a plain redelivery ran the task: runs=%d err=%v", runs, err)
	}

	// The fix ships; the operator replays (fbctl dlq replay).
	fixed = true
	dead.Attempt, dead.Stalls, dead.Waits = 7, 2, 30 // what a task carries after its retries
	rp := &Replayer{Ckpt: w.D.Ckpt, Jobs: tr}
	rep, err := rp.Prepare(ctx, &dead)
	if err != nil || rep.Task == nil || !rep.Reopened {
		t.Fatalf("prepare: %+v %v", rep, err)
	}
	if rep.Task.Attempt != 0 || rep.Task.Stalls != 0 || rep.Task.Waits != 0 || rep.Task.TaskId != dead.TaskId {
		t.Fatalf("replayed task: attempt %d stalls %d id %s", rep.Task.Attempt, rep.Task.Stalls, rep.Task.TaskId)
	}
	st, _ = tr.Status(ctx, "job-1")
	if st.Pending != 1 || st.Counters["tasks_failed"] != 0 {
		t.Fatalf("after reopening: %+v", st)
	}
	if ready, _ := tr.ReadyToFinalize(ctx, 10); len(ready) != 0 {
		t.Fatalf("a reopened job must wait for its task: %v", ready)
	}
	// Preparing it again (a second replay run) changes nothing.
	if again, err := rp.Prepare(ctx, &dead); err != nil || again.Reopened {
		t.Fatalf("second prepare: %+v %v", again, err)
	}
	if err := w.Run(ctx, rep.Task); err != nil || runs != 2 {
		t.Fatalf("the replay did not run: runs=%d err=%v", runs, err)
	}
	st, _ = tr.Status(ctx, "job-1")
	if st.Pending != 0 || st.Counters["tasks_failed"] != 0 || st.Counters["comments"] != 5 {
		t.Fatalf("after the replay: %+v", st)
	}
	if ready, _ := tr.ReadyToFinalize(ctx, 10); len(ready) != 1 {
		t.Fatalf("job not queued again: %v", ready)
	}

	// A task of a cancelled job is not replayed.
	if err := tr.Cancel(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	if rep, err := rp.Prepare(ctx, &dead); err != nil || rep.Task != nil {
		t.Fatalf("cancelled job: %+v %v", rep, err)
	}
}

// A finalised job whose accounting still lives is running again after a
// replay, so the finalizer sets its final state once more; one cancelled
// keeps its state.
func TestReplayReopensAFinalisedJob(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	r := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	tr := jobs.New(r)
	rp := &Replayer{Ckpt: checkpoint.New(r), Jobs: tr, DB: db}
	for _, c := range []struct{ job, status, want string }{
		{"3f1c7a52-0c1e-4b8e-9a51-0d5c1c2b7a01", "partial", "running"},
		{"3f1c7a52-0c1e-4b8e-9a51-0d5c1c2b7a02", "completed", "running"},
		{"3f1c7a52-0c1e-4b8e-9a51-0d5c1c2b7a03", "cancelled", "cancelled"},
	} {
		if _, err := db.Exec(ctx, `INSERT INTO jobs (job_id, kind, lane, mode, page_ref, params, status, finished_at)
			VALUES ($1, 'on_demand', 1, 'full', 'v:x', '{}', $2, now())`, c.job, c.status); err != nil {
			t.Fatal(err)
		}
		dead := &taskv1.Task{TaskId: "t-" + c.job, JobId: c.job, Kind: taskv1.Kind_KIND_POST}
		if err := tr.Spawn(ctx, c.job, dead.TaskId); err != nil {
			t.Fatal(err)
		}
		if _, err := tr.Done(ctx, c.job, dead.TaskId, map[string]int64{"tasks_failed": 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := rp.Prepare(ctx, dead); err != nil {
			t.Fatal(err)
		}
		var status string
		var finished *time.Time
		if err := db.QueryRow(ctx, `SELECT status, finished_at FROM jobs WHERE job_id = $1`, c.job).Scan(&status, &finished); err != nil {
			t.Fatal(err)
		}
		if status != c.want || (c.want == "running") != (finished == nil) {
			t.Fatalf("%s job: %s (finished %v), want %s", c.status, status, finished, c.want)
		}
	}
}
