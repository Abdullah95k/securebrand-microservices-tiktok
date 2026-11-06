// Package halt is tks's stop switch (M18; docs/runbooks/tks-production.md):
// `tksctl stop` sets it in Valkey, and within seconds no crawler takes a
// new task and no request goes to TikTok (the crawlers' TikTok client and
// the canary wait while it is set); `tksctl start` clears it. Nothing is
// lost: tasks wait in Kafka and resume where they stopped. It stops tks
// without touching the deployment, so the platform's GitOps does not undo
// it.
package halt

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/redisx"
)

// State is the switch.
type State struct {
	Stopped bool      `json:"stopped"`
	Reason  string    `json:"reason,omitempty"`
	At      time.Time `json:"at,omitzero"`
}

func key(ns string) string { return redisx.KeyPrefix + ns + "halt" }

// Stop sets the switch; it stays until Start.
func Stop(ctx context.Context, rdb *redis.Client, ns, reason string, now time.Time) (State, error) {
	st := State{Stopped: true, Reason: reason, At: now.UTC()}
	b, _ := json.Marshal(st)
	return st, rdb.Set(ctx, key(ns), b, 0).Err()
}

// Start clears the switch.
func Start(ctx context.Context, rdb *redis.Client, ns string) error {
	return rdb.Del(ctx, key(ns)).Err()
}

// Get reads the switch.
func Get(ctx context.Context, rdb *redis.Client, ns string) (State, error) {
	b, err := rdb.Get(ctx, key(ns)).Bytes()
	if errors.Is(err, redis.Nil) {
		return State{}, nil
	}
	if err != nil {
		return State{}, err
	}
	var st State
	err = json.Unmarshal(b, &st)
	return st, err
}

// Gate follows the switch for a process.
type Gate struct {
	rdb     *redis.Client
	ns      string
	every   time.Duration
	stopped atomic.Bool
	changed chan struct{}
}

// NewGate returns a gate that reads the switch every 5 seconds.
func NewGate(rdb *redis.Client, ns string) *Gate {
	return &Gate{rdb: rdb, ns: ns, every: 5 * time.Second, changed: make(chan struct{}, 1)}
}

// Stopped reports the last state read.
func (g *Gate) Stopped() bool { return g.stopped.Load() }

// Refresh reads the switch now. A read that fails keeps the last state.
func (g *Gate) Refresh(ctx context.Context) error {
	st, err := Get(ctx, g.rdb, g.ns)
	if err != nil {
		return err
	}
	if g.stopped.Swap(st.Stopped) != st.Stopped {
		if st.Stopped {
			slog.Warn("tks stopped by tksctl stop: no new work goes to TikTok", "reason", st.Reason, "at", st.At)
		} else {
			slog.Info("tks started again")
		}
		select {
		case g.changed <- struct{}{}:
		default:
		}
	}
	return nil
}

// Run refreshes the gate until ctx ends.
func (g *Gate) Run(ctx context.Context) error {
	t := time.NewTicker(g.every)
	defer t.Stop()
	for {
		if err := g.Refresh(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("halt: refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Wait returns at once while tks runs, and blocks while it is stopped
// until it starts again or ctx ends.
func (g *Gate) Wait(ctx context.Context) error {
	for g.stopped.Load() {
		t := time.NewTimer(g.every)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		case <-g.changed:
			t.Stop()
		}
	}
	return nil
}
