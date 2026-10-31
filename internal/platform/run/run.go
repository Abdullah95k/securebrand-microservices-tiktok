// Package run runs a set of long-lived components until the first one fails
// or the process receives SIGINT/SIGTERM.
package run

import (
	"context"
	"errors"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"
)

// Func is a component. It must return when ctx is cancelled.
type Func func(ctx context.Context) error

// Group runs every Func concurrently. When any returns (or a signal
// arrives), the shared context is cancelled and Group waits for the rest.
// It returns the first non-nil, non-cancellation error.
func Group(parent context.Context, fns ...Func) error {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	for _, fn := range fns {
		wg.Add(1)
		go func(fn Func) {
			defer wg.Done()
			err := fn(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				once.Do(func() { firstErr = err })
				slog.Error("component stopped", "err", err)
			}
			cancel()
		}(fn)
	}
	wg.Wait()
	return firstErr
}
