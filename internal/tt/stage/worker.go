package stage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Worker runs tks's stage tasks consumed from Kafka (internal/crawl/stage's
// Worker on tks's envelope). It owns the generic task
// lifecycle: duplicate suppression, cancellation, stream leases, retries
// with escalating delay, the dead letter queue and job accounting.
type Worker struct {
	D      *Deps
	Stages map[tkstaskv1.Kind]Stage
	// MaxAttempts bounds the retries since a task last advanced its
	// checkpoint (Task.stalls): a stream that moves forward between rate
	// limits is not dead-lettered for them.
	MaxAttempts uint32
	// MaxTotalAttempts bounds every retry of a task, progress or not
	// (Task.attempt); 0 means 10 × MaxAttempts.
	MaxTotalAttempts uint32
	// MaxWaits bounds the retries spent waiting for proxy exits
	// (Task.waits), which count toward neither of the above; 0 means
	// defaultMaxWaits.
	MaxWaits uint32
	LeaseTTL time.Duration
}

var stageNames = map[tkstaskv1.Kind]string{
	tkstaskv1.Kind_KIND_ACCOUNT: "account", tkstaskv1.Kind_KIND_POST: "post", tkstaskv1.Kind_KIND_THREAD: "thread",
}

// Handle implements kafka.Handler.
func (w *Worker) Handle(ctx context.Context, rec *kgo.Record) error {
	var t tkstaskv1.Task
	if err := proto.Unmarshal(rec.Value, &t); err != nil {
		return w.deadLetterRaw(ctx, rec, "undecodable", err.Error())
	}
	if _, ok := w.Stages[t.Kind]; !ok {
		// Dead-lettered as received: a kind this worker has no stage for
		// (an empty record, a newer producer, another service's task) may
		// have no topic name of its own (topics.Task panics on it).
		return w.deadLetterRaw(ctx, rec, "no_stage", "no stage for kind "+t.Kind.String())
	}
	return w.Run(ctx, &t)
}

// Run processes one decoded task (exported for tests and tools).
func (w *Worker) Run(ctx context.Context, t *tkstaskv1.Task) error {
	d := w.D
	name := stageNames[t.Kind]
	lane := topics.LaneName(t.Lane)
	st, ok := w.Stages[t.Kind]
	if !ok {
		// Handle dead-letters these as received; a caller of Run gets an
		// error (dead-lettering one needs its topic, which an unknown kind
		// does not have).
		return fmt.Errorf("stage: no stage for kind %v", t.Kind)
	}
	if d.Jobs.Cancelled(ctx, t.JobId) {
		metrics.Tasks.WithLabelValues(name, lane, "cancelled").Inc()
		// What the task left unread makes the post's next pass read it
		// whole; a post whose pass never began keeps its last state.
		if w.begun(ctx, t) {
			w.markIncomplete(ctx, t)
		}
		return w.finish(ctx, t, map[string]int64{"cancelled": 1})
	}
	if isDone, err := d.Ckpt.IsDone(ctx, t.TaskId); err != nil {
		return err
	} else if isDone {
		_, err := d.Jobs.Done(ctx, t.JobId, t.TaskId, nil)
		return ignoreUnspawned(err)
	}
	// The owner is this run: another copy of the same task (a redelivery
	// while this run still goes, a record produced twice) waits for it
	// instead of running alongside it, where both would produce the next
	// segment and every child twice, and so on for the rest of the stream.
	lease, ok, err := d.Ckpt.Acquire(ctx, task.ScopeKey(t), t.LineageId+"/"+uuid.NewString(), w.LeaseTTL)
	if err != nil {
		return err
	}
	if !ok {
		// Another run is paginating the same stream; come back later.
		metrics.Tasks.WithLabelValues(name, lane, "lease_busy").Inc()
		return w.toRetry(ctx, t, "30s")
	}

	// The checkpoint before the run tells whether a run that failed still
	// moved its stream forward.
	before, _, beforeErr := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	start := time.Now()
	out := w.runStage(ctx, st, t, lease)
	metrics.TaskSeconds.WithLabelValues(name).Observe(time.Since(start).Seconds())

	switch out.Result {
	case Done:
		metrics.Tasks.WithLabelValues(name, lane, "done").Inc()
		if out.Counters["cancelled"] > 0 && w.begun(ctx, t) {
			w.markIncomplete(ctx, t) // cut short by a cancel
		}
		if err := d.spawn(ctx, out.FollowUps...); err != nil {
			return err
		}
		return w.finish(ctx, t, out.Counters)
	case Continue:
		metrics.Tasks.WithLabelValues(name, lane, "yield").Inc()
		if err := d.spawn(ctx, append([]*tkstaskv1.Task{out.Next}, out.FollowUps...)...); err != nil {
			return err
		}
		return w.finish(ctx, t, out.Counters)
	case Retry:
		next := out.Next
		after, _, afterErr := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
		next.Stalls = stallsAfter(t.Stalls, beforeErr == nil && afterErr == nil && advanced(before, after), out.Waiting)
		if out.Waiting {
			// The exits' doing, not the task's: counted apart.
			next.Attempt, next.Waits = t.Attempt, t.Waits+1
		}
		if reason := w.exhausted(next); reason != "" {
			metrics.Tasks.WithLabelValues(name, lane, "failed").Inc()
			slog.Warn("task failed after retries", "stage", name, "job", t.JobId, "task", t.TaskId,
				"attempts", next.Attempt, "stalls", next.Stalls, "reason", out.Reason)
			return w.deadLetter(ctx, t, reason, out.Reason, out.Counters)
		}
		// What the attempt spent after its last saved checkpoint: the saved
		// part resumes into the retry's counters and is reported when the
		// task finishes; the rest would be lost.
		if afterErr == nil && t.JobId != "" {
			if cost := unsaved(costCounters(out.Counters), after.Counters); len(cost) > 0 {
				if err := d.Jobs.AddCounters(ctx, t.JobId, cost); err != nil {
					slog.Debug("jobs: retry counters", "job", t.JobId, "err", err)
				}
			}
		}
		metrics.Tasks.WithLabelValues(name, lane, "retry").Inc()
		metrics.Retries.WithLabelValues(name, out.Delay).Inc()
		slog.Debug("task retry", "stage", name, "task", t.TaskId, "attempt", next.Attempt, "stalls", next.Stalls,
			"delay", out.Delay, "reason", out.Reason)
		return w.toRetry(ctx, next, out.Delay)
	default:
		metrics.Tasks.WithLabelValues(name, lane, "failed").Inc()
		return w.deadLetter(ctx, t, "permanent", out.Reason, out.Counters)
	}
}

// costKeys are the counters a retried attempt adds to its job for the work
// its checkpoint did not save: what it cost and why it failed, not what it
// collected (the retry collects that again).
var costKeys = []string{"requests", "wire_bytes", "blocked", "soft_errors", "transport_errors", "drift", "sign_errors"}

// unsaved returns c less what the checkpoint saved, keeping what grew.
func unsaved(c, saved map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range c {
		if d := v - saved[k]; d > 0 {
			out[k] = d
		}
	}
	return out
}

func costCounters(c map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for _, k := range costKeys {
		if v := c[k]; v != 0 {
			out[k] = v
		}
	}
	return out
}

// advanced reports whether a stream's checkpoint moved between two loads:
// the run saved at least one page. Only saved progress counts, so a run
// whose checkpoint write failed is a stall.
func advanced(before, after checkpoint.State) bool {
	return after.Cursor != before.Cursor || after.Pages > before.Pages
}

// stallsAfter counts a retry without progress against the task, unless it
// was only waiting for proxy exits to cool down; progress resets the count.
func stallsAfter(prev uint32, progressed, waiting bool) uint32 {
	switch {
	case progressed:
		return 0
	case waiting:
		return prev
	}
	return prev + 1
}

// defaultMaxWaits is crawler.max_waits when unset: 4 hours of 30s waits.
const defaultMaxWaits = 480

// exhausted returns the dead-letter reason for a task about to be retried,
// or "" while it may retry.
func (w *Worker) exhausted(next *tkstaskv1.Task) string {
	total := w.MaxTotalAttempts
	if total == 0 {
		total = 10 * w.MaxAttempts
	}
	switch {
	case next.Stalls >= w.MaxAttempts:
		return "max_attempts"
	case next.Attempt >= total:
		return "max_total_attempts"
	case next.Waits >= cmp.Or(w.MaxWaits, defaultMaxWaits):
		return "max_waits"
	}
	return ""
}

// runStage runs the stage, then releases the lease (before follow-on work
// is produced, so the next segment can claim it at once), also when the
// stage panics: the consumer recovers a panic, and a lease left behind
// would be kept alive forever and block its stream.
func (w *Worker) runStage(ctx context.Context, st Stage, t *tkstaskv1.Task, lease *checkpoint.Lease) Outcome {
	defer lease.Release(ctx)
	return st.Run(ctx, t)
}

// begun reports whether t's stream has read anything: a reply thread (its
// post's pass is done), a continuation, or a first segment retried after
// progress (it carries a cursor, or its checkpoint has one).
func (w *Worker) begun(ctx context.Context, t *tkstaskv1.Task) bool {
	if t.Kind == tkstaskv1.Kind_KIND_THREAD || t.Seq > 0 || t.Cursor != "" {
		return true
	}
	st, _, err := w.D.Ckpt.Load(ctx, t.LineageId, t.Seq)
	return err != nil || st.Pages > 0 || st.Cursor != ""
}

// markIncomplete marks the post whose comments t was collecting: its next
// pass reads it whole.
func (w *Worker) markIncomplete(ctx context.Context, t *tkstaskv1.Task) {
	var post int64
	switch t.Kind {
	case tkstaskv1.Kind_KIND_POST:
		post = t.GetPost().GetPostId()
	case tkstaskv1.Kind_KIND_THREAD:
		post = t.GetThread().GetPostId()
	}
	if post != 0 {
		if err := w.D.Ckpt.MarkIncomplete(ctx, post); err != nil {
			slog.Warn("stage: incomplete mark not set", "post", post, "err", err)
		}
	}
}

func (w *Worker) finish(ctx context.Context, t *tkstaskv1.Task, c map[string]int64) error {
	if err := w.D.Ckpt.MarkDone(ctx, t.TaskId); err != nil {
		return err
	}
	_, err := w.D.Jobs.Done(ctx, t.JobId, t.TaskId, c)
	return ignoreUnspawned(err)
}

// toRetry parks t on the retry topic for delay.
func (w *Worker) toRetry(ctx context.Context, t *tkstaskv1.Task, delay string) error {
	topic, wait := topics.Retry30s, 30*time.Second
	switch delay {
	case "5m":
		topic, wait = topics.Retry5m, 5*time.Minute
	case "1h":
		topic, wait = topics.Retry1h, time.Hour
	}
	notBefore := time.Now().Add(wait).UnixMilli()
	rec, err := w.D.Producer.Record(topic, task.ScopeKey(t), t,
		kgo.RecordHeader{Key: kafka.HeaderOrigTopic, Value: []byte(topics.Task(t.Kind, t.Lane))},
		kgo.RecordHeader{Key: kafka.HeaderNotBefore, Value: []byte(strconv.FormatInt(notBefore, 10))})
	if err != nil {
		return err
	}
	return w.D.Producer.ProduceSync(ctx, rec)
}

func (w *Worker) deadLetter(ctx context.Context, t *tkstaskv1.Task, reason, detail string, c map[string]int64) error {
	metrics.DLQ.WithLabelValues(stageNames[t.Kind], reason).Inc()
	rec, err := w.D.Producer.Record(topics.DLQ, task.ScopeKey(t), t,
		kgo.RecordHeader{Key: kafka.HeaderOrigTopic, Value: []byte(topics.Task(t.Kind, t.Lane))},
		kgo.RecordHeader{Key: kafka.HeaderReason, Value: []byte(reason)},
		kgo.RecordHeader{Key: kafka.HeaderError, Value: []byte(truncate(detail, 1000))})
	if err != nil {
		return err
	}
	if err := w.D.Producer.ProduceSync(ctx, rec); err != nil {
		return err
	}
	if c == nil {
		c = map[string]int64{}
	}
	c["tasks_failed"]++
	// The post's comments are incomplete: its next pass reads it whole.
	switch t.Kind {
	case tkstaskv1.Kind_KIND_POST:
		_ = w.D.Jobs.UpdatePostState(ctx, t.JobId, t.GetPost().GetPostId(), jobs.PostFailed)
	case tkstaskv1.Kind_KIND_THREAD:
		_ = w.D.Jobs.UpdatePostState(ctx, t.JobId, t.GetThread().GetPostId(), jobs.PostTruncated)
	}
	w.markIncomplete(ctx, t)
	// Marked done like a finished task, so a redelivery or a parked copy is
	// skipped; a replay of the dead letter (fbctl dlq replay, through a
	// Replayer) clears the mark.
	return w.finish(ctx, t, c)
}

func (w *Worker) deadLetterRaw(ctx context.Context, rec *kgo.Record, reason, detail string) error {
	metrics.DLQ.WithLabelValues("unknown", reason).Inc()
	out := &kgo.Record{Topic: w.D.Producer.Topic(topics.DLQ), Key: rec.Key, Value: rec.Value, Headers: []kgo.RecordHeader{
		{Key: kafka.HeaderOrigTopic, Value: []byte(rec.Topic)},
		{Key: kafka.HeaderReason, Value: []byte(reason)},
		{Key: kafka.HeaderError, Value: []byte(truncate(detail, 1000))},
	}}
	return w.D.Producer.ProduceSync(ctx, out)
}

// ignoreUnspawned drops the error of a task produced outside job accounting
// (e.g. a DLQ replay of an old job), which must not block the consumer. Any
// other error (Redis unreachable) is returned, so the record is retried:
// otherwise the job would never see the task done and would hang until the
// reaper closed it.
func ignoreUnspawned(err error) error {
	if errors.Is(err, jobs.ErrNeverSpawned) {
		slog.Debug("jobs: done", "err", err)
		return nil
	}
	return err
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// RetryRelay moves parked tasks back to their original topic once due
// (internal/crawl/stage's). Each record waits for its own due time, so run
// one consumer per retry topic, with room for many records in flight.
type RetryRelay struct {
	Producer *kafka.Producer
}

// retryRelayInflight is how many parked tasks a relay consumer holds waiting
// for their due time (internal/crawl/stage's value).
const retryRelayInflight = 1024

// RetryRelays returns the consumers that move parked tks tasks back to their
// topic once due: one per tks.task.retry.* topic, so a task parked for an
// hour never holds up one due in 30 seconds.
func RetryRelays(kcfg kafka.ClientConfig, producer *kafka.Producer) ([]*kafka.Consumer, error) {
	relay := &RetryRelay{Producer: producer}
	out := make([]*kafka.Consumer, 0, len(topics.RetryTopics))
	for _, topic := range topics.RetryTopics {
		cons, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "tks-tt-retry-relay", Topics: []string{topic},
			MaxInflight: retryRelayInflight, MaxInflightPerPartition: retryRelayInflight, DrainTimeout: 5 * time.Second}, relay.Handle)
		if err != nil {
			for _, c := range out {
				c.Client().Close()
			}
			return nil, err
		}
		out = append(out, cons)
	}
	return out, nil
}

func (r *RetryRelay) Handle(ctx context.Context, rec *kgo.Record) error {
	orig := kafka.HeaderValue(rec, kafka.HeaderOrigTopic)
	if orig == "" {
		return nil
	}
	if nb, err := strconv.ParseInt(kafka.HeaderValue(rec, kafka.HeaderNotBefore), 10, 64); err == nil {
		wait := time.Until(time.UnixMilli(nb))
		if wait > 0 {
			t := time.NewTimer(wait)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	out := &kgo.Record{Topic: r.Producer.Topic(orig), Key: rec.Key, Value: rec.Value}
	for _, h := range rec.Headers {
		if h.Key == kafka.HeaderType || h.Key == kafka.HeaderContentType || h.Key == kafka.HeaderTraceParent {
			out.Headers = append(out.Headers, h)
		}
	}
	if err := r.Producer.ProduceSync(ctx, out); err != nil {
		return fmt.Errorf("retry relay: %w", err)
	}
	return nil
}
