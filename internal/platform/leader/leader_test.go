package leader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

// The leader's work stops when its lease is lost (taken by another holder
// after it expired), another process then leads, a lease someone else holds
// is not taken, and a leader that finishes hands the lease back at once.
func TestLeaseLeadership(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pool.Exec(ctx, `CREATE TABLE leases (name text PRIMARY KEY, holder text NOT NULL, until timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	const key = 4242

	leading := make(chan struct{})
	stopped := make(chan error, 1)
	rctx, rcancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = Run(rctx, pool, key, func(lctx context.Context) error {
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
	var holder string
	if err := pool.QueryRow(ctx, `SELECT holder FROM leases WHERE name = $1 AND until > now()`, Name(key)).Scan(&holder); err != nil || holder != Holder {
		t.Fatalf("lease holder %q (%v), want %q", holder, err, Holder)
	}

	// While another process holds a lease, Hold does not run.
	if _, err := pool.Exec(ctx, `INSERT INTO leases VALUES ('other', 'someone-else', now() + interval '1 minute')`); err != nil {
		t.Fatal(err)
	}
	if ran, err := Hold(ctx, pool, "other", func(context.Context) error { return nil }); ran || err != nil {
		t.Fatalf("ran %v (%v) on a lease another process holds", ran, err)
	}

	// The lease goes to someone else (as when this holder missed its
	// renewals): the next renewal fails and the work stops.
	if _, err := pool.Exec(ctx, `UPDATE leases SET holder = 'someone-else', until = now() + interval '1 minute' WHERE name = $1`, Name(key)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("work stopped with %v", err)
		}
	case <-time.After(3 * Renew):
		t.Fatal("the work kept running after its lease was gone")
	}

	rcancel()
	<-runDone

	// Once it expires, this process leads again; when its work ends, the
	// lease is released at once.
	if _, err := pool.Exec(ctx, `UPDATE leases SET until = now() - interval '1 second' WHERE name = $1`, Name(key)); err != nil {
		t.Fatal(err)
	}
	if ran, err := Hold(ctx, pool, Name(key), func(context.Context) error { return nil }); !ran || err != nil {
		t.Fatalf("an expired lease was not taken: ran %v (%v)", ran, err)
	}
	var expired bool
	if err := pool.QueryRow(ctx, `SELECT until <= now() FROM leases WHERE name = $1`, Name(key)).Scan(&expired); err != nil || !expired {
		t.Fatalf("lease not released after the work ended (%v)", err)
	}
}
