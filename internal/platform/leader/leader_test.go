package leader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

// The leader's work stops when its lock's session dies, another process
// takes the lock, and a leader that finishes hands the lock back.
func TestLeaderLosesTheLockWithItsSession(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const key = 4242

	leading := make(chan struct{})
	stopped := make(chan error, 1)
	go func() {
		_ = Run(ctx, pool, key, func(lctx context.Context) error {
			select {
			case leading <- struct{}{}:
			default:
				return nil // a later term: step down at once
			}
			<-lctx.Done()
			stopped <- lctx.Err()
			return lctx.Err()
		})
	}()
	select {
	case <-leading:
	case <-ctx.Done():
		t.Fatal("never led")
	}

	// Kill the session that holds the lock.
	if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND objid = $1`, key); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("work stopped with %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the work kept running after its lock was gone")
	}

	// Another process takes the lock and, when done, gives it back.
	for i := range 2 {
		took := make(chan struct{}, 1)
		tctx, tcancel := context.WithTimeout(ctx, 20*time.Second)
		go func() {
			_ = Run(tctx, pool, key, func(context.Context) error {
				took <- struct{}{}
				tcancel()
				return nil
			})
		}()
		select {
		case <-took:
		case <-tctx.Done():
			if len(took) == 0 {
				t.Fatalf("run %d never took the lock", i)
			}
		}
		<-tctx.Done()
	}
}
