package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

// Handler processes one record. Returning nil marks the record done. An
// error means "not processed": the consumer retries it with backoff until it
// succeeds or the consumer shuts down, so the offset is never committed past
// an unprocessed record. Handlers that want to give up on a record must
// route it elsewhere (retry topic, DLQ) themselves and return nil.
type Handler func(ctx context.Context, rec *kgo.Record) error

// ConsumerConfig configures a group consumer.
type ConsumerConfig struct {
	ClientConfig
	// Group and Topics are base names; TopicPrefix is applied to both so
	// stacks sharing a cluster stay isolated.
	Group                   string
	Topics                  []string
	MaxInflight             int
	MaxInflightPerPartition int
	CommitInterval          time.Duration
	// DrainTimeout bounds how long in-flight records may run after shutdown
	// starts. Handlers see IsDraining(ctx) == true during this window.
	DrainTimeout time.Duration
	// ResetToStart consumes from the earliest offset when the group has no
	// committed offset (default: earliest, so no task is ever skipped).
	ResetToLatest bool
	// Slots, when set, is capacity this consumer shares with others (the
	// priority lanes of a crawl stage): every record also holds one of its
	// slots while it runs. MaxInflight then caps this consumer alone.
	Slots Slots
}

// Slots is capacity shared between consumers.
type Slots interface {
	// Acquire blocks until a slot is free or ctx is done (false).
	Acquire(ctx context.Context) bool
	Release()
}

// Consumer is a group consumer with in-partition concurrency.
type Consumer struct {
	cfg     ConsumerConfig
	cl      *kgo.Client
	h       Handler
	tr      *tracker
	global  chan struct{}
	drainCh chan struct{}
	// drained is set once the drain is over (in-flight records finished,
	// or the drain timeout gave up on them): what is still pending will
	// never be done, so the revoke of leaving the group does not wait for it.
	drained atomic.Bool
}

type drainKey struct{}

// IsDraining reports whether the consumer that invoked the handler is
// shutting down; long handlers should checkpoint and return.
func IsDraining(ctx context.Context) bool {
	ch, ok := ctx.Value(drainKey{}).(chan struct{})
	if !ok {
		return false
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// WithDrain returns a context carrying a drain signal, for tests and callers
// that invoke handlers directly.
func WithDrain(ctx context.Context, ch chan struct{}) context.Context {
	return context.WithValue(ctx, drainKey{}, ch)
}

func NewConsumer(cfg ConsumerConfig, h Handler) (*Consumer, error) {
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 64
	}
	if cfg.MaxInflightPerPartition <= 0 {
		cfg.MaxInflightPerPartition = cfg.MaxInflight
	}
	if cfg.CommitInterval <= 0 {
		cfg.CommitInterval = 2 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 60 * time.Second
	}
	c := &Consumer{
		cfg:     cfg,
		h:       h,
		tr:      newTracker(cfg.MaxInflightPerPartition),
		global:  make(chan struct{}, cfg.MaxInflight),
		drainCh: make(chan struct{}),
	}
	topics := make([]string, len(cfg.Topics))
	for i, t := range cfg.Topics {
		topics[i] = cfg.TopicPrefix + t
	}
	reset := kgo.NewOffset().AtStart()
	if cfg.ResetToLatest {
		reset = kgo.NewOffset().AtEnd()
	}
	cl, err := kgo.NewClient(append(cfg.Opts(),
		kgo.ConsumerGroup(cfg.TopicPrefix+cfg.Group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.ConsumeResetOffset(reset),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
	)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: consumer %s: %w", cfg.Group, err)
	}
	c.cl = cl
	return c, nil
}

// Client exposes the underlying client.
func (c *Consumer) Client() *kgo.Client { return c.cl }

// Run consumes until ctx is cancelled, then drains in-flight records,
// commits and leaves the group.
func (c *Consumer) Run(ctx context.Context) error {
	handlerCtx, cancelHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHandlers()
	handlerCtx = WithDrain(handlerCtx, c.drainCh)

	commitDone := make(chan struct{})
	commitCtx, stopCommit := context.WithCancel(context.Background())
	go func() {
		defer close(commitDone)
		t := time.NewTicker(c.cfg.CommitInterval)
		defer t.Stop()
		for {
			select {
			case <-commitCtx.Done():
				return
			case <-t.C:
				c.commit(commitCtx, nil)
			}
		}
	}()

	for {
		fetches := c.cl.PollRecords(ctx, c.cfg.MaxInflight)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				slog.Warn("kafka: fetch error", "topic", t, "partition", p, "err", err)
			}
		})
		stop := false
		fetches.EachRecord(func(r *kgo.Record) {
			if stop {
				return
			}
			e, psem, pctx := c.tr.add(r, handlerCtx)
			if e == nil {
				return // its partition was revoked meanwhile: the next owner's
			}
			if !acquire(ctx, c.global) {
				stop = true
				return
			}
			if !acquire(ctx, psem) {
				<-c.global
				stop = true
				return
			}
			// Shared slots last: a record waiting for its partition must not
			// hold capacity other lanes could use.
			if c.cfg.Slots != nil && !c.cfg.Slots.Acquire(ctx) {
				<-psem
				<-c.global
				stop = true
				return
			}
			go func() {
				defer func() {
					if c.cfg.Slots != nil {
						c.cfg.Slots.Release()
					}
					<-psem
					<-c.global
				}()
				// A record abandoned at shutdown, or whose partition was
				// revoked while it waited for capacity, is not done: leaving
				// its offset uncommitted hands it to the partition's next
				// owner.
				if pctx.Err() == nil && c.tr.start(e, r) && c.process(pctx, r) {
					c.tr.markDone(e)
				}
			}()
		})
		if stop {
			break
		}
	}

	// Drain: tell handlers to wrap up, wait for them (bounded), commit, leave.
	close(c.drainCh)
	deadline := time.NewTimer(c.cfg.DrainTimeout)
	defer deadline.Stop()
	for i := 0; i < cap(c.global); i++ {
		select {
		case c.global <- struct{}{}:
		case <-deadline.C:
			slog.Warn("kafka: drain timeout; abandoning in-flight records", "group", c.cfg.Group)
			cancelHandlers()
			i = cap(c.global)
		}
	}
	c.drained.Store(true)
	stopCommit()
	<-commitDone
	finalCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.commit(finalCtx, nil)
	c.cl.Close()
	return nil
}

func acquire(ctx context.Context, sem chan struct{}) bool {
	select {
	case sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// process runs the handler until it succeeds (true) or ctx is cancelled
// (false: the record was abandoned).
func (c *Consumer) process(ctx context.Context, r *kgo.Record) bool {
	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := c.call(ctx, r)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		slog.Warn("kafka: handler failed; retrying", "topic", r.Topic, "partition", r.Partition,
			"offset", r.Offset, "attempt", attempt, "err", err)
		jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff + jitter):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// call runs the handler, turning a panic into an error: the record is
// retried with backoff and its partition waits (visible as lag), while the
// process and its other partitions go on. Unrecovered, the panic would kill
// the process, and the uncommitted record would kill it again on restart.
func (c *Consumer) call(ctx context.Context, r *kgo.Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("kafka: handler panicked", "group", c.cfg.Group, "topic", r.Topic, "partition", r.Partition,
				"offset", r.Offset, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			metrics.KafkaHandlerPanics.WithLabelValues(c.cfg.Group).Inc()
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	return c.h(ctx, r)
}

func (c *Consumer) commit(ctx context.Context, only map[string][]int32) {
	offsets := c.tr.commitable(only)
	if len(offsets) == 0 {
		return
	}
	var commitErr error
	c.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if p.ErrorCode != 0 {
					commitErr = fmt.Errorf("commit %s/%d: error code %d", t.Topic, p.Partition, p.ErrorCode)
				}
			}
		}
	})
	if commitErr != nil {
		metrics.KafkaCommitted.WithLabelValues(c.cfg.Group, "error").Inc()
		slog.Warn("kafka: commit failed", "group", c.cfg.Group, "err", commitErr)
		return
	}
	metrics.KafkaCommitted.WithLabelValues(c.cfg.Group, "ok").Inc()
	c.tr.markCommitted(offsets)
}

func (c *Consumer) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	c.tr.assign(assigned)
}

func (c *Consumer) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	// No record of the revoked partitions starts any more; those running get
	// a short window to finish so the new owner does not redo them, then
	// what is done is committed.
	c.tr.disown(revoked)
	deadline := time.Now().Add(15 * time.Second)
	for !c.drained.Load() && c.tr.pending(revoked) > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	c.commit(ctx, revoked)
	c.tr.drop(revoked)
}

func (c *Consumer) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.tr.drop(lost)
}
