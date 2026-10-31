package redisx_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
)

// TestEveryKeyIsPrefixed drives each Valkey-backed store and checks that
// every key it leaves behind starts with KeyPrefix. fbs's keys have no
// prefix, so an unprefixed key here could collide with fbs's on a shared
// Valkey (ADR 0004 §4).
func TestEveryKeyIsPrefixed(t *testing.T) {
	if redisx.KeyPrefix == "" {
		t.Fatal("KeyPrefix must not be empty: fbs's keys have no prefix")
	}
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	ck := checkpoint.New(r)
	must(ck.Save(ctx, "lineage", 1, checkpoint.State{Cursor: "c"}))
	must(ck.MarkDone(ctx, "task"))
	lease, ok, err := ck.Acquire(ctx, "post:1", "lineage", time.Minute)
	must(err)
	if !ok {
		t.Fatal("lease not acquired")
	}
	defer lease.Release(ctx)
	must(ck.SetPost(ctx, 1, checkpoint.PostCache{}))
	must(ck.SetThread(ctx, 2, 3))

	tr := jobs.New(r)
	must(tr.Spawn(ctx, "job", "root"))
	must(tr.AddCounters(ctx, "job", map[string]int64{"posts": 1}))
	must(tr.BeginPass(ctx, "job", "lineage", 1, time.Now()))
	must(tr.AddSeen(ctx, "job", 1, []jobs.SeenComment{{ID: 10, Replies: 1}}))
	if _, err := tr.Done(ctx, "job", "root", nil); err != nil {
		t.Fatal(err)
	}
	must(tr.Cancel(ctx, "job2"))

	must(ratelimit.NewRedis(r).Wait(ctx, "global", 1000))
	rep := ratelimit.NewReporter(r, "")
	rep.Observe(detect.OK, "p1", 100)
	must(rep.Flush(ctx))

	keys := mr.Keys()
	if len(keys) < 8 {
		t.Fatalf("only %d keys written: %v", len(keys), keys)
	}
	for _, k := range keys {
		// redis_rate adds its own "rate:" in front of the key it is given.
		if !strings.HasPrefix(strings.TrimPrefix(k, "rate:"), redisx.KeyPrefix) {
			t.Errorf("key %q does not start with %q", k, redisx.KeyPrefix)
		}
	}
}
