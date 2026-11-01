package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

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
