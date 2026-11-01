package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A handler that panics fails its record instead of the process: the
// consumer retries the record (and its partition waits) while everything
// else goes on. Before M2 a task of an unknown kind panicked the crawler
// this way, again on every restart.
func TestHandlerPanicIsAnError(t *testing.T) {
	calls := 0
	c := &Consumer{cfg: ConsumerConfig{Group: "g"}, h: func(context.Context, *kgo.Record) error {
		calls++
		if calls == 1 {
			panic("boom")
		}
		return errors.New("plain")
	}}
	rec := &kgo.Record{Topic: "t", Partition: 1, Offset: 7}
	if err := c.call(context.Background(), rec); err == nil || !strings.Contains(err.Error(), "handler panic: boom") {
		t.Fatalf("panic: %v", err)
	}
	if err := c.call(context.Background(), rec); err == nil || err.Error() != "plain" {
		t.Fatalf("plain error: %v", err)
	}
}

// Once the drain is over, the revoke of leaving the group doesn't wait for
// records still marked running: the drain timeout abandoned them.
func TestRevokeAfterDrainDoesNotWait(t *testing.T) {
	c := &Consumer{tr: newTracker(1)}
	c.tr.assign(map[string][]int32{"t": {0}})
	r := &kgo.Record{Topic: "t", Partition: 0, Offset: 5}
	e, _, _ := c.tr.add(r, context.Background())
	c.tr.start(e, r) // abandoned by the drain timeout: never done
	c.tr.markCommitted(map[string]map[int32]kgo.EpochOffset{"t": {0: {Offset: 5}}})
	c.drained.Store(true)
	start := time.Now()
	c.onRevoked(context.Background(), nil, map[string][]int32{"t": {0}})
	if d := time.Since(start); d > time.Second {
		t.Fatalf("revoke after the drain waited %v", d)
	}
}

// A shutdown doesn't wait 15s for a record that was fetched but never
// started (it waited for capacity when the shutdown came): leaving the
// group revokes its partition, and the drain is already over. A stop that
// overruns the container's grace period is killed before its last commit.
func TestShutdownDoesNotWaitForRecordsNeverStarted(t *testing.T) {
	b := os.Getenv("TKS_TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("TKS_TEST_KAFKA_BROKERS not set")
	}
	cfg := ClientConfig{Brokers: strings.Split(b, ","), TopicPrefix: fmt.Sprintf("t%d.", time.Now().UnixNano())}
	const topic = "tks.test.shutdown"
	prod, err := NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx := context.Background()
	for _, v := range []string{"first", "second"} {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: prod.Topic(topic), Key: []byte("same partition"), Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{}, 2)
	c, err := NewConsumer(ConsumerConfig{ClientConfig: cfg, Group: "shutdown", Topics: []string{topic}, MaxInflight: 1, DrainTimeout: 30 * time.Second},
		func(ctx context.Context, _ *kgo.Record) error {
			started <- struct{}{}
			for !IsDraining(ctx) && ctx.Err() == nil {
				time.Sleep(10 * time.Millisecond)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(rctx) }()
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("the first record never started")
	}
	time.Sleep(time.Second) // the second record waits for the only slot
	cancel()
	stopping := time.Now()
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("Run did not return")
	}
	if d := time.Since(stopping); d > 10*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
}

// While a revoke waits for the records running on its partitions, none of
// their records waiting for capacity starts: they are the next owner's.
func TestRevokeStopsWaitingRecordsFromStarting(t *testing.T) {
	c := &Consumer{tr: newTracker(2)}
	c.tr.assign(map[string][]int32{"t": {0}})
	r1, r2 := &kgo.Record{Topic: "t", Partition: 0, Offset: 1}, &kgo.Record{Topic: "t", Partition: 0, Offset: 2}
	e1, _, _ := c.tr.add(r1, context.Background())
	e2, _, _ := c.tr.add(r2, context.Background())
	c.tr.start(e1, r1)
	c.tr.markCommitted(map[string]map[int32]kgo.EpochOffset{"t": {0: {Offset: 3}}}) // nothing left to commit
	done := make(chan struct{})
	go func() {
		c.onRevoked(context.Background(), nil, map[string][]int32{"t": {0}})
		close(done)
	}()
	time.Sleep(300 * time.Millisecond) // the revoke waits for r1
	if c.tr.start(e2, r2) {
		t.Error("a record of a partition being revoked started")
	}
	c.tr.markDone(e1)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the revoke did not end once the running record was done")
	}
}
