package lanes

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hold acquires n slots of lane in the background and reports each grant.
func hold(t *testing.T, ctx context.Context, s *Scheduler, lane, n int, granted *atomic.Int64) {
	t.Helper()
	for i := 0; i < n; i++ {
		go func() {
			if s.Acquire(ctx, lane) {
				granted.Add(1)
			}
		}()
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func used(s *Scheduler) []int {
	var out []int
	for _, st := range s.Stats() {
		out = append(out, st.Used)
	}
	return out
}

func TestIdleCapacityIsLent(t *testing.T) {
	s := New(10, Lane{Name: "p0", Max: 10, Min: 1}, Lane{Name: "p1", Max: 10, Min: 1}, Lane{Name: "p2", Max: 4, Min: 1})
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if !s.Acquire(ctx, 1) {
			t.Fatal("p1 may use idle slots")
		}
	}
	for i := 0; i < 4; i++ {
		s.Acquire(ctx, 2)
	}
	if got := used(s); got[1] != 6 || got[2] != 4 {
		t.Fatalf("used %v", got)
	}
	// p2 is at its cap; a fifth p2 task waits even though p0 is idle.
	var g atomic.Int64
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	hold(t, cctx, s, 2, 1, &g)
	waitFor(t, "p2 waiter", func() bool { return s.Stats()[2].Waiting == 1 })
	if g.Load() != 0 {
		t.Fatal("p2 above its cap")
	}
}

func TestPriorityOnRelease(t *testing.T) {
	s := New(4, Lane{Name: "p0", Max: 4}, Lane{Name: "p1", Max: 4}, Lane{Name: "p2", Max: 4})
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		s.Acquire(ctx, 2)
	}
	var p0, p1, p2 atomic.Int64
	hold(t, ctx, s, 2, 2, &p2)
	hold(t, ctx, s, 1, 2, &p1)
	hold(t, ctx, s, 0, 2, &p0)
	waitFor(t, "waiters", func() bool {
		st := s.Stats()
		return st[0].Waiting == 2 && st[1].Waiting == 2 && st[2].Waiting == 2
	})
	for i := 0; i < 4; i++ {
		s.Release(2)
	}
	waitFor(t, "grants", func() bool { return p0.Load()+p1.Load() == 4 })
	if p0.Load() != 2 || p1.Load() != 2 || p2.Load() != 0 {
		t.Fatalf("freed slots go by priority: p0=%d p1=%d p2=%d", p0.Load(), p1.Load(), p2.Load())
	}
}

func TestMinimumShareBeatsPriority(t *testing.T) {
	s := New(4, Lane{Name: "p0", Max: 4}, Lane{Name: "p2", Max: 4, Min: 1})
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		s.Acquire(ctx, 0)
	}
	var p0, p2 atomic.Int64
	hold(t, ctx, s, 0, 3, &p0)
	hold(t, ctx, s, 1, 3, &p2)
	waitFor(t, "waiters", func() bool { st := s.Stats(); return st[0].Waiting == 3 && st[1].Waiting == 3 })
	s.Release(0)
	waitFor(t, "a grant", func() bool { return p0.Load()+p2.Load() == 1 })
	if p2.Load() != 1 {
		t.Fatal("a waiting lane below its minimum is served first")
	}
	s.Release(0)
	waitFor(t, "a grant", func() bool { return p0.Load()+p2.Load() == 2 })
	if p0.Load() != 1 {
		t.Fatal("once at its minimum, the lane waits for higher priorities")
	}
}

func TestCancelledWaiter(t *testing.T) {
	s := New(1, Lane{Name: "p0", Max: 1}, Lane{Name: "p1", Max: 1})
	ctx := context.Background()
	s.Acquire(ctx, 0)
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan bool)
	go func() { done <- s.Acquire(cctx, 1) }()
	waitFor(t, "waiter", func() bool { return s.Stats()[1].Waiting == 1 })
	cancel()
	if <-done {
		t.Fatal("cancelled Acquire must fail")
	}
	if st := s.Stats(); st[1].Waiting != 0 || st[1].Used != 0 {
		t.Fatalf("%+v", st)
	}
	s.Release(0)
	if !s.Acquire(ctx, 1) {
		t.Fatal("slot must be free")
	}
}

func TestClamping(t *testing.T) {
	s := New(10, Lane{Name: "a", Max: 50, Min: 8}, Lane{Name: "b", Max: 0, Min: 8})
	if s.lanes[0].Max != 10 || s.lanes[0].Min != 8 || s.lanes[1].Max != 1 || s.lanes[1].Min != 1 {
		t.Fatalf("%+v %+v", s.lanes[0].Lane, s.lanes[1].Lane)
	}
}

func TestStress(t *testing.T) {
	s := New(8, Lane{Name: "p0", Max: 8, Min: 1}, Lane{Name: "p1", Max: 8, Min: 1}, Lane{Name: "p2", Max: 3, Min: 1})
	var inUse, peak, peakP2 atomic.Int64
	var p2 atomic.Int64
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for g := 0; g < 48; g++ {
		wg.Add(1)
		go func(lane int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if !s.Acquire(ctx, lane) {
					return
				}
				n := inUse.Add(1)
				if lane == 2 {
					if m := p2.Add(1); m > peakP2.Load() {
						peakP2.Store(m)
					}
				}
				if n > peak.Load() {
					peak.Store(n)
				}
				if lane == 2 {
					p2.Add(-1)
				}
				inUse.Add(-1)
				s.Release(lane)
			}
		}(g % 3)
	}
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("deadlock")
	}
	if peak.Load() > 8 || peakP2.Load() > 3 {
		t.Fatalf("over capacity: total %d, p2 %d", peak.Load(), peakP2.Load())
	}
	for _, st := range s.Stats() {
		if st.Used != 0 || st.Waiting != 0 {
			t.Fatalf("leaked: %+v", st)
		}
	}
}

func TestSetMaxPausesAndResumes(t *testing.T) {
	s := New(4, Lane{Name: "p0", Max: 4}, Lane{Name: "p2", Max: 2, Min: 1})
	ctx := context.Background()
	s.SetMax(1, 0)
	var g atomic.Int64
	hold(t, ctx, s, 1, 2, &g)
	waitFor(t, "waiters", func() bool { return s.Stats()[1].Waiting == 2 })
	time.Sleep(10 * time.Millisecond)
	if g.Load() != 0 {
		t.Fatal("a paused lane gets no slots, not even its minimum")
	}
	s.SetMax(1, 2)
	waitFor(t, "resumed", func() bool { return g.Load() == 2 })
	if st := s.Stats()[1]; st.Used != 2 || s.lanes[1].Min != 1 {
		t.Fatalf("%+v min=%d", st, s.lanes[1].Min)
	}
}
