// Package leader elects one leader among replicas with a lease row in the
// leases table (tks: tt.leases, migration 00016), renewed every few
// seconds. A session advisory lock cannot be used: production's PostgreSQL
// is reached through PgBouncer in transaction mode, where a session lock
// stays on a pooled server session that another client may then use (two
// leaders), and an unlock may run on another session (the master,
// 2026-11-07). A lease is a plain row written in one statement, so it holds
// whatever the pool mode.
package leader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// TTL is how long a lease holds without a renewal; Renew how often its
	// holder renews it. A holder that misses one renewal stops its work
	// well before another process can take the lease.
	TTL   = 15 * time.Second
	Renew = 5 * time.Second
	// callTimeout bounds each acquire, renewal and release.
	callTimeout = 3 * time.Second
)

// Holder names this process in the leases it holds: the host name and a
// random suffix, so two processes on one host never share a lease.
var Holder = func() string {
	h, _ := os.Hostname()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return h + "-" + hex.EncodeToString(b)
}()

// Name is the lease name of a numeric key (the scheduler's leaderKey).
func Name(key int64) string { return "leader:" + strconv.FormatInt(key, 10) }

// Run blocks until this process holds the lease of key, then calls fn with
// a context that is cancelled when a renewal fails. It retries until ctx is
// done: every TTL while another process holds the lease.
func Run(ctx context.Context, pool *pgxpool.Pool, key int64, fn func(ctx context.Context) error) error {
	name := Name(key)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ran, err := Hold(ctx, pool, name, fn)
		if err != nil && ctx.Err() == nil {
			slog.Warn("leader: lost or failed", "lease", name, "err", err)
		}
		// A standby tries again once a lease could have expired (TTL), not
		// every Renew: its connection then idles long enough for PgBouncer
		// to close it. A takeover waits at most TTL longer.
		wait := TTL
		if ran {
			wait = Renew
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// Hold takes the lease name once and, if it got it, runs fn while renewing
// it every Renew; fn's context is cancelled when a renewal fails, and Hold
// waits for fn before it releases the lease. It reports whether fn ran: a
// lease another process holds is not an error.
func Hold(ctx context.Context, pool *pgxpool.Pool, name string, fn func(ctx context.Context) error) (bool, error) {
	got, err := acquire(ctx, pool, name)
	if err != nil || !got {
		return false, err
	}
	slog.Info("leader: lease acquired", "lease", name)
	defer release(pool, name)

	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn(lctx) }()
	tick := time.NewTicker(Renew)
	defer tick.Stop()
	// The lease holds at least TTL from the start of the last renewal that
	// succeeded (the database sets until = now() + TTL during the call).
	held := time.Now()
	for {
		select {
		case err := <-done:
			return true, err
		case <-tick.C:
			start := time.Now()
			ok, err := renew(ctx, pool, name, held)
			if err == nil && !ok {
				err = errors.New("leader: lease taken by another process")
			}
			if err == nil {
				held = start
			}
			if err != nil {
				// The work stops before the lease can expire for another
				// process to take it.
				cancel()
				if ferr := <-done; ferr != nil {
					slog.Debug("leader: work stopped after losing the lease", "err", ferr)
				}
				return true, err
			}
		}
	}
}

// acquireLease is acquire; tests replace it.
var acquireLease = acquire

// renewRetry is the pause between renewal attempts after one failed.
var renewRetry = 500 * time.Millisecond

// renew renews the lease, trying again on a new connection while an error
// leaves time to: a connection the database or PgBouncer dropped (SQLSTATE
// 08P01, 57P01) failed about 47 renewals an hour on 2026-11-14, and each
// cancelled the leader's work (the finalizer's batches with it) though the
// lease still held. An attempt starts only if it ends, at callTimeout, a
// second before the lease held since held could expire; a lease another
// process holds is answered at once.
func renew(ctx context.Context, pool *pgxpool.Pool, name string, held time.Time) (bool, error) {
	deadline := held.Add(TTL - time.Second)
	for {
		ok, err := acquireLease(ctx, pool, name)
		if err == nil || ctx.Err() != nil {
			return ok, err
		}
		if time.Now().Add(renewRetry + callTimeout).After(deadline) {
			return false, err
		}
		slog.Info("leader: renewal failed; trying again", "lease", name, "err", err)
		select {
		case <-ctx.Done():
			return false, err
		case <-time.After(renewRetry):
		}
	}
}

// acquire takes or renews the lease, in one statement: it succeeds when
// nobody holds it, this process holds it, or its holder let it expire.
// Times are the database's, so the replicas' clocks do not matter.
func acquire(ctx context.Context, pool *pgxpool.Pool, name string) (bool, error) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var until time.Time
	err := pool.QueryRow(cctx, `INSERT INTO leases AS l (name, holder, until) VALUES ($1, $2, now() + $3::interval)
		ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, until = EXCLUDED.until
		WHERE l.holder = EXCLUDED.holder OR l.until < now()
		RETURNING until`, name, Holder, TTL.String()).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// release ends this process's lease at once, so another can take it
// without waiting for it to expire.
func release(pool *pgxpool.Pool, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if _, err := pool.Exec(ctx, `UPDATE leases SET until = now() - interval '1 second' WHERE name = $1 AND holder = $2`,
		name, Holder); err != nil {
		slog.Warn("leader: lease not released (it expires by itself)", "lease", name, "err", err)
	}
}
