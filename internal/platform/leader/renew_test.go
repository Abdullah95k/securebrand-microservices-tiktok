package leader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A renewal that fails on a dropped connection is tried again while the
// lease still holds, so the leader keeps its work; one that keeps failing
// gives up before the lease can expire; a lease another process took is
// answered at once.
func TestRenewRetries(t *testing.T) {
	dropped := errors.New("FATAL: server conn crashed? (SQLSTATE 08P01)")
	orig, origRetry := acquireLease, renewRetry
	t.Cleanup(func() { acquireLease, renewRetry = orig, origRetry })
	renewRetry = 10 * time.Millisecond
	ctx := context.Background()

	calls := 0
	acquireLease = func(context.Context, *pgxpool.Pool, string) (bool, error) {
		calls++
		if calls < 3 {
			return false, dropped
		}
		return true, nil
	}
	if ok, err := renew(ctx, nil, "l", time.Now()); !ok || err != nil || calls != 3 {
		t.Fatalf("renew after two drops: %v %v after %d calls", ok, err, calls)
	}

	calls = 0
	acquireLease = func(context.Context, *pgxpool.Pool, string) (bool, error) { calls++; return false, dropped }
	// Held 12 s ago: 3 s left, less a second: no time for another 3 s call.
	start := time.Now()
	if ok, err := renew(ctx, nil, "l", start.Add(-12*time.Second)); ok || !errors.Is(err, dropped) || calls != 1 {
		t.Fatalf("renew late: %v %v after %d calls", ok, err, calls)
	}
	// Held just now: retries until a second plus a call before the lease ends.
	calls = 0
	if ok, err := renew(ctx, nil, "l", start); ok || !errors.Is(err, dropped) || calls < 2 {
		t.Fatalf("renew always failing: %v %v after %d calls", ok, err, calls)
	}
	if left := time.Until(start.Add(TTL)); left < time.Second {
		t.Fatalf("gave up %v before the lease ends, want at least 1 s", left)
	}

	calls = 0
	acquireLease = func(context.Context, *pgxpool.Pool, string) (bool, error) { calls++; return false, nil }
	if ok, err := renew(ctx, nil, "l", time.Now()); ok || err != nil || calls != 1 {
		t.Fatalf("lease taken: %v %v after %d calls", ok, err, calls)
	}
}
