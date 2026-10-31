package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
)

var testCfg = config.AIMD{Enabled: true, MinRPS: 5, MaxRPS: 500, Increase: 0.02, Decrease: 0.7,
	LowBlockRate: 0.02, HighBlockRate: 0.06, MinSamples: 100, MinUtilization: 0.8}

func TestDecide(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cur            int
		total, blocked int64
		to             int
		reason         string
	}{
		{"few samples", 50, 99, 99, 50, "hold:few_samples"},
		{"high blocks", 50, 500, 40, 35, "decrease"},
		{"floor", 6, 100, 50, 5, "decrease"},
		{"low blocks, busy", 50, 450, 0, 51, "increase"}, // 450 / (50 x 10s) = 0.9
		{"low blocks, big step", 300, 2900, 10, 306, "increase"},
		{"cap", 499, 4990, 0, 500, "increase"},
		{"low blocks, idle", 50, 300, 0, 50, "hold:underused"},
		{"in between", 50, 500, 20, 50, "hold:block_rate"}, // 4%
		{"start below min", 2, 10, 0, 5, "clamp"},
		{"decrease from what was sent", 300, 1000, 100, 70, "decrease"}, // 100 rps sent, not 300
	} {
		d := Decide(testCfg, tc.cur, tc.total, tc.blocked, Window)
		if d.To != tc.to || d.Reason != tc.reason {
			t.Errorf("%s: got %d %s, want %d %s", tc.name, d.To, d.Reason, tc.to, tc.reason)
		}
	}
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func TestReporterControllerCeiling(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	clk := &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}

	rep := NewReporter(rdb, "ns.")
	rep.now = clk.now
	ctl := NewController(rdb, "ns.", testCfg, 50)
	ctl.now = clk.now
	ceil := NewCeiling(rdb, "ns.", 50)

	// Window 1: 500 responses, 10% blocked; transport failures are ignored.
	for i := 0; i < 500; i++ {
		c := detect.OK
		if i%10 == 0 {
			c = detect.RateLimited
		}
		rep.Observe(c, "p1", 100)
		rep.Observe(detect.Transport, "p1", 0)
	}
	if err := rep.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// The window is not settled yet.
	clk.t = clk.t.Add(Window + time.Second)
	if err := ctl.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ceil.Refresh(ctx); err != nil || ceil.RPS() != 50 {
		t.Fatalf("published before the window settled: %d %v", ceil.RPS(), err)
	}
	clk.t = clk.t.Add(settle)
	if err := ctl.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ceil.Refresh(ctx); err != nil || ceil.RPS() != 35 {
		t.Fatalf("want 50 x 0.7 = 35, got %d %v", ceil.RPS(), err)
	}
	// Ticking again on the same window changes nothing.
	if err := ctl.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	_ = ceil.Refresh(ctx)
	if ceil.RPS() != 35 {
		t.Fatalf("a window is used once, got %d", ceil.RPS())
	}

	// Window 2: busy and clean, so the ceiling grows by one step.
	for i := 0; i < 340; i++ { // 340 / (35 x 10s) = 0.97
		rep.Observe(detect.OK, "p0", 100)
	}
	_ = rep.Flush(ctx)
	clk.t = clk.t.Add(Window)
	if err := ctl.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	_ = ceil.Refresh(ctx)
	if ceil.RPS() != 36 {
		t.Fatalf("want 36, got %d", ceil.RPS())
	}

	// Window 3: quiet. Holds, and the published value stays.
	clk.t = clk.t.Add(Window)
	_ = ctl.Tick(ctx)
	_ = ceil.Refresh(ctx)
	if ceil.RPS() != 36 {
		t.Fatalf("hold, got %d", ceil.RPS())
	}

	// A start above max_rps is kept rather than clamped on the first window.
	high := NewController(rdb, "high.", testCfg, 5000)
	high.now = clk.now
	if err := high.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	hc := NewCeiling(rdb, "high.", 0)
	if err := hc.Refresh(ctx); err != nil || hc.RPS() != 5000 {
		t.Fatalf("want 5000, got %d %v", hc.RPS(), err)
	}

	// Without a published ceiling, crawlers use the configured one; another
	// namespace is independent.
	other := NewCeiling(rdb, "other.", 77)
	if err := other.Refresh(ctx); err != nil || other.RPS() != 77 {
		t.Fatal(other.RPS(), err)
	}
}

func TestReporterKeepsCountsWhenRedisFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rep := NewReporter(rdb, "")
	rep.Observe(detect.OK, "p1", 10)
	rep.Observe(detect.LoginWall, "p2", 10)
	mr.Close()
	if err := rep.Flush(context.Background()); err == nil {
		t.Fatal("want an error")
	}
	if rep.total.Load() != 2 || rep.blocked.Load() != 1 {
		t.Fatalf("counts lost: %d %d", rep.total.Load(), rep.blocked.Load())
	}
}
