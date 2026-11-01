// Package leader implements leader election with a Postgres session-level
// advisory lock, which behaves the same under Compose and Kubernetes.
package leader

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Run blocks until this process holds the lock identified by key, then calls
// fn with a context that is cancelled if the lock's connection dies. It
// retries until ctx is done.
func Run(ctx context.Context, pool *pgxpool.Pool, key int64, fn func(ctx context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := runOnce(ctx, pool, key, fn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			slog.Warn("leader: lost or failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// pingTimeout bounds each check that the lock's connection is alive, and
// the unlock.
const pingTimeout = 3 * time.Second

// drop closes a pooled connection instead of returning it to the pool.
func drop(conn *pgxpool.Conn) {
	c := conn.Hijack()
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	_ = c.Close(ctx)
}

func runOnce(ctx context.Context, pool *pgxpool.Pool, key int64, fn func(ctx context.Context) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&got); err != nil {
		drop(conn)
		return err
	}
	if !got {
		conn.Release()
		return nil
	}
	slog.Info("leader: acquired", "key", key)
	defer func() {
		// Unlock and hand the connection back; if that fails, close it,
		// which releases the lock with the session, so a connection still
		// holding it never returns to the pool.
		uctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		defer cancel()
		if _, err := conn.Exec(uctx, "SELECT pg_advisory_unlock($1)", key); err != nil {
			drop(conn)
			return
		}
		conn.Release()
	}()

	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn(lctx) }()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-tick.C:
			pctx, pcancel := context.WithTimeout(ctx, pingTimeout)
			err := conn.Ping(pctx)
			pcancel()
			if err != nil {
				// A hung connection counts as lost: the work stops before
				// another process can take the lock.
				cancel()
				if ferr := <-done; ferr != nil {
					slog.Debug("leader: work stopped after losing the lock", "err", ferr)
				}
				return err
			}
		}
	}
}
