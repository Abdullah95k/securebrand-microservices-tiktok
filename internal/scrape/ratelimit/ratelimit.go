// Package ratelimit provides the fleet-wide limiters (Redis GCRA) that keep
// the request rate to Facebook bounded no matter how many pods run.
package ratelimit

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// Limiter blocks until a request under key is allowed at perSecond.
type Limiter interface {
	Wait(ctx context.Context, key string, perSecond int) error
}

// Redis is a distributed GCRA limiter. If Redis is unreachable it fails
// open after logging, relying on per-identity pacing, so a Redis blip does
// not stop crawling.
type Redis struct {
	l *redis_rate.Limiter
}

func NewRedis(c *redis.Client) *Redis { return &Redis{l: redis_rate.NewLimiter(c)} }

func (r *Redis) Wait(ctx context.Context, key string, perSecond int) error {
	if perSecond <= 0 {
		return nil
	}
	for {
		res, err := r.l.Allow(ctx, redisx.KeyPrefix+"rl:"+key, redis_rate.PerSecond(perSecond))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("ratelimit: redis unavailable, failing open", "key", key, "err", err)
			return nil
		}
		if res.Allowed > 0 {
			return nil
		}
		wait := res.RetryAfter
		if wait <= 0 {
			wait = 10 * time.Millisecond
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Noop never waits.
type Noop struct{}

func (Noop) Wait(context.Context, string, int) error { return nil }
