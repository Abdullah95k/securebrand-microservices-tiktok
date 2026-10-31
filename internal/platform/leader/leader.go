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

func runOnce(ctx context.Context, pool *pgxpool.Pool, key int64, fn func(ctx context.Context) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&got); err != nil {
		return err
	}
	if !got {
		return nil
	}
	slog.Info("leader: acquired", "key", key)
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", key)
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
			if err := conn.Ping(ctx); err != nil {
				cancel()
				if ferr := <-done; ferr != nil {
					slog.Debug("leader: work stopped after losing the lock", "err", ferr)
				}
				return err
			}
		}
	}
}
