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

// fill reports n requests of lane in each 5-minute slice of the hour
// before now, then flushes slice by slice.
func fill(t *testing.T, rep *Reporter, clk *clock, now time.Time, lanes map[string]int) {
	t.Helper()
	for i := 12; i >= 1; i-- {
		clk.t = now.Add(-time.Duration(i) * BudgetSlice).Add(time.Second)
		for lane, n := range lanes {
			for range n {
				rep.Observe(detect.OK, lane, 1000)
			}
		}
		if err := rep.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	clk.t = now
}

func TestBudgetPausesBackfillThenStretches(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	noon := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	clk := &clock{t: noon}
	rep := NewReporter(rdb, "ns.")
	rep.now = clk.now
	cfg := config.Budget{Enabled: true, RequestsPerDay: 90_000, Stretch: 2, StretchTiers: []string{"T4", "T3", "T2"}, ProjectionSpan: time.Hour}
	ctl := NewBudgetController(rdb, "ns.", cfg)
	ctl.now = clk.now
	gate := NewBudgetGate(rdb, "ns.")
	var changes []bool
	gate.OnChange = func(st BudgetState) { changes = append(changes, st.PauseP2) }

	// Last hour: 300 p1 + 300 p2 requests per slice = 7200/h (2/s); 12h
	// left in the day: 7200 used + 86400 projected = 93.6k > 90k. Without
	// p2: 7200 + 43200 fits. Backfill pauses, nothing is stretched.
	fill(t, rep, clk, noon, map[string]int{"p1": 300, "p2": 300})
	if err := ctl.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := LoadBudgetState(ctx, rdb, "ns.")
	if !st.PauseP2 || st.Stretch != 1 || st.Used.Requests[2] != 3600 || st.P2ReqRate != 1 {
		t.Fatalf("want backfill paused: %+v", st)
	}
	if err := gate.Refresh(ctx); err != nil || len(changes) != 1 || !changes[0] {
		t.Fatalf("gate: %v %v", changes, err)
	}

	// An hour later p2 stopped (paused): 10.8k used, 11h left. Resuming at
	// its last rate (1/s) would land at 90k, above the 95% resume margin,
	// so it stays paused.
	later := noon.Add(time.Hour)
	fill(t, rep, clk, later, map[string]int{"p1": 300})
	_ = ctl.Tick(ctx)
	st, _ = LoadBudgetState(ctx, rdb, "ns.")
	if !st.PauseP2 || st.P2ReqRate != 1 {
		t.Fatalf("must stay paused (hysteresis): %+v", st)
	}

	// Scheduled traffic alone now overruns: stretch the slow tiers.
	evening := later.Add(time.Hour)
	fill(t, rep, clk, evening, map[string]int{"p0": 400, "p1": 800})
	_ = ctl.Tick(ctx)
	st, _ = LoadBudgetState(ctx, rdb, "ns.")
	if !st.PauseP2 || st.Stretch != 2 || st.StretchFor("T3") != 2 || st.StretchFor("T0") != 1 || st.Explanation == "" {
		t.Fatalf("want stretch: %+v", st)
	}

	// A new day starts from zero use: quiet traffic fits and both lift.
	nextDay := noon.Add(24 * time.Hour)
	fill(t, rep, clk, nextDay, map[string]int{"p1": 10})
	_ = ctl.Tick(ctx)
	st, _ = LoadBudgetState(ctx, rdb, "ns.")
	if st.PauseP2 || st.Stretch != 1 {
		t.Fatalf("want everything lifted: %+v", st)
	}
	_ = gate.Refresh(ctx)
	if len(changes) != 2 || changes[1] {
		t.Fatalf("gate must report the resume: %v", changes)
	}
}

func TestBudgetBytes(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	noon := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	clk := &clock{t: noon}
	rep := NewReporter(rdb, "")
	rep.now = clk.now
	ctl := NewBudgetController(rdb, "", config.Budget{Enabled: true, BytesPerDay: 50_000_000, ProjectionSpan: time.Hour})
	ctl.now = clk.now
	// 1000 bytes per request, 600 p2 requests per slice: 7.2 MB/h, ~86 MB
	// projected for the day > 50 MB.
	fill(t, rep, clk, noon, map[string]int{"p2": 600})
	if err := ctl.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ := LoadBudgetState(context.Background(), rdb, "")
	if !st.PauseP2 || st.Projected[1] < 50_000_000 {
		t.Fatalf("byte budget: %+v", st)
	}
}

func TestNoStateIsNeutral(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st, err := LoadBudgetState(context.Background(), rdb, "none.")
	if err != nil || st.PauseP2 || st.StretchFor("T4") != 1 {
		t.Fatalf("%+v %v", st, err)
	}
}

func TestBudgetStateExpiresWithoutController(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	noon := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	clk := &clock{t: noon}
	rep := NewReporter(rdb, "")
	rep.now = clk.now
	ctl := NewBudgetController(rdb, "", config.Budget{Enabled: true, RequestsPerDay: 1000, Interval: time.Minute, ProjectionSpan: time.Hour})
	ctl.now = clk.now
	fill(t, rep, clk, noon, map[string]int{"p2": 100})
	if err := ctl.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	gate := NewBudgetGate(rdb, "")
	resumed := false
	gate.OnChange = func(st BudgetState) { resumed = !st.PauseP2 }
	_ = gate.Refresh(context.Background())
	if !gate.State().PauseP2 {
		t.Fatal("want paused")
	}
	// The controller stops (budget switched off, leader gone): the state
	// expires and backfill resumes instead of staying paused forever.
	mr.FastForward(11 * time.Minute)
	_ = gate.Refresh(context.Background())
	if gate.State().PauseP2 || !resumed {
		t.Fatalf("a stale pause must lift: %+v", gate.State())
	}
}
