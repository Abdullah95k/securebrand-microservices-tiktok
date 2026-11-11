// Package scheduler holds the loop helpers tks-scheduler's leader runs its
// loops with; tks's own planner, finalizer, reaper and maintenance are in
// internal/tt/scheduler.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

// Every runs fn every interval until ctx is done, logging errors.
func Every(ctx context.Context, name string, interval time.Duration, fn func(context.Context) error) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("scheduler: loop failed", "loop", name, "err", err)
			metrics.LoopErrors.WithLabelValues(name).Inc()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
