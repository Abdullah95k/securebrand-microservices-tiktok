// Package dlq sends tks's dead letters back to where they came from
// (tksctl dlq replay): tasks the crawler gave up on (tks.task.dlq), readied
// to run again, and records the persister set aside (tks.persist.dlq), as
// they were. fbs's fbctl dlq replay does the same for Facebook's tasks;
// tks's is a package so its end-to-end test runs the code the CLI runs.
package dlq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/stage"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// Group is the consumer group a replay commits its progress with, so a
// second run continues after the first instead of replaying it again.
const Group = "tks-tt-dlq-replay"

// Options say what to replay.
type Options struct {
	// Topic: topics.DLQ (tasks, readied by Replayer) or
	// topics.PersistQuarantine (records sent back as they are).
	Topic string
	// Reason, when set, replays only dead letters with this x-reason
	// (the crawler's max_attempts, max_total_attempts, max_waits,
	// permanent, undecodable or no_stage; the persister's undecodable or
	// refused); the others are skipped and committed.
	Reason string
	// Max stops after this many records replayed (0: 1000).
	Max int
	// Idle stops after this long without a record (0: 10 s).
	Idle time.Duration
}

// Result is what a replay did.
type Result struct {
	Topic    string `json:"topic"`
	Replayed int    `json:"replayed"`
	// Reopened: jobs that counted a replayed task as pending again (a
	// finalised one runs again and is finalised once more).
	Reopened int `json:"jobs_reopened"`
	// Skipped: another reason, no original topic, or a cancelled job.
	Skipped int `json:"skipped"`
}

// Replay reads dead letters from o.Topic with a consumer of cl's options
// (the caller's Kafka connection; the group and topic are added here) and
// produces each one back to its original topic through p. Dead-lettered
// tasks are readied by r first, so they run again instead of being skipped
// as done. Offsets are committed after each batch is produced, so a replay
// that stops halfway resumes where it stopped.
func Replay(ctx context.Context, opts []kgo.Opt, prefix string, p *kafka.Producer, r *stage.Replayer, o Options) (Result, error) {
	tasks := o.Topic == topics.DLQ
	if !tasks && o.Topic != topics.PersistQuarantine {
		return Result{}, fmt.Errorf("dlq: topic must be %s or %s", topics.DLQ, topics.PersistQuarantine)
	}
	if o.Max <= 0 {
		o.Max = 1000
	}
	if o.Idle <= 0 {
		o.Idle = 10 * time.Second
	}
	cl, err := kgo.NewClient(append(opts, kgo.ConsumerGroup(prefix+Group), kgo.ConsumeTopics(prefix+o.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())...)
	if err != nil {
		return Result{}, err
	}
	defer cl.Close()
	res := Result{Topic: o.Topic}
	for res.Replayed < o.Max {
		pctx, cancel := context.WithTimeout(ctx, o.Idle)
		recs := cl.PollRecords(pctx, min(100, o.Max-res.Replayed)).Records()
		cancel()
		if len(recs) == 0 {
			break
		}
		var out []*kgo.Record
		for _, rec := range recs {
			orig := kafka.HeaderValue(rec, kafka.HeaderOrigTopic)
			if (o.Reason != "" && kafka.HeaderValue(rec, kafka.HeaderReason) != o.Reason) || orig == "" {
				res.Skipped++
				continue
			}
			base := strings.TrimPrefix(orig, prefix)
			var t tkstaskv1.Task
			if !tasks || proto.Unmarshal(rec.Value, &t) != nil || t.TaskId == "" {
				// A record the persister set aside, or a task dead-lettered
				// as received (it did not decode): it goes back as it is.
				out = append(out, &kgo.Record{Topic: p.Topic(base), Key: rec.Key, Value: rec.Value, Headers: typeHeaders(rec)})
				continue
			}
			rep, err := r.Prepare(ctx, &t)
			if err != nil {
				return res, err
			}
			if rep.Task == nil {
				res.Skipped++ // its job was cancelled
				continue
			}
			if rep.Reopened {
				res.Reopened++
			}
			back, err := p.Record(base, string(rec.Key), rep.Task)
			if err != nil {
				return res, err
			}
			out = append(out, back)
		}
		if err := p.ProduceSync(ctx, out...); err != nil {
			return res, err
		}
		if err := cl.CommitRecords(ctx, recs...); err != nil {
			return res, err
		}
		res.Replayed += len(out)
	}
	return res, nil
}

// typeHeaders keeps the headers that say how a record is encoded: the
// persister decodes by them.
func typeHeaders(r *kgo.Record) []kgo.RecordHeader {
	var out []kgo.RecordHeader
	for _, h := range r.Headers {
		if h.Key == kafka.HeaderType || h.Key == kafka.HeaderContentType {
			out = append(out, h)
		}
	}
	return out
}
