package breaker

import (
	"sync"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }
func testSet(cfg Config) (*Set, *clock) {
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s := New(cfg)
	s.now = c.now
	return s, c
}

func observe(s *Set, key string, ok, failed int) {
	for i := 0; i < ok; i++ {
		s.Observe(key, false)
	}
	for i := 0; i < failed; i++ {
		s.Observe(key, true)
	}
}

func TestOpensOnFailureRate(t *testing.T) {
	s, _ := testSet(Config{MinEvents: 20, OpenRate: 0.15})
	observe(s, "a", 16, 2) // 18 events: below MinEvents
	if s.State("a") != Closed {
		t.Fatal("must stay closed below MinEvents")
	}
	observe(s, "a", 1, 0) // 19 events
	observe(s, "a", 0, 1) // 20 events, 3 failed = 15%
	if s.State("a") != Open || s.Allow("a") {
		t.Fatalf("want open, got %v", s.State("a"))
	}
	if s.State("b") != Closed || !s.Allow("b") {
		t.Fatal("other keys are independent")
	}
}

func TestOldEventsLeaveTheWindow(t *testing.T) {
	s, c := testSet(Config{Window: time.Minute, MinEvents: 10, OpenRate: 0.5})
	observe(s, "a", 0, 4)
	c.add(61 * time.Second)
	observe(s, "a", 6, 0) // the 4 failures are out of the window
	observe(s, "a", 0, 4) // 4 of 10
	if s.State("a") != Closed {
		t.Fatal("failures older than the window must not count")
	}
}

func TestHalfOpenProbeCloses(t *testing.T) {
	var transitions []string
	s, c := testSet(Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Minute, ProbeSuccesses: 2})
	s.OnChange = func(_ string, from, to State) { transitions = append(transitions, from.String()+">"+to.String()) }
	observe(s, "a", 0, 2)
	c.add(59 * time.Second)
	if s.Allow("a") {
		t.Fatal("open during cooldown")
	}
	c.add(time.Second)
	if !s.Allow("a") {
		t.Fatal("half-open admits a probe")
	}
	if s.Allow("a") {
		t.Fatal("only one probe at a time")
	}
	s.Observe("a", false)
	if !s.Allow("a") || s.State("a") != HalfOpen {
		t.Fatal("after a successful probe, the next one is admitted")
	}
	s.Observe("a", false)
	if s.State("a") != Closed {
		t.Fatalf("want closed, got %v", s.State("a"))
	}
	want := "closed>open open>half_open half_open>closed"
	if got := join(transitions); got != want {
		t.Fatalf("transitions %q, want %q", got, want)
	}
}

func TestFailedProbeDoublesCooldown(t *testing.T) {
	s, c := testSet(Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Minute, MaxCooldown: 3 * time.Minute})
	observe(s, "a", 0, 2)
	for _, wait := range []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute, 3 * time.Minute} {
		c.add(wait - time.Second)
		if s.Allow("a") {
			t.Fatalf("open until %v", wait)
		}
		c.add(time.Second)
		if !s.Allow("a") {
			t.Fatalf("probe after %v", wait)
		}
		s.Observe("a", true)
	}
}

func TestLostProbeIsReplaced(t *testing.T) {
	s, c := testSet(Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Minute})
	observe(s, "a", 0, 2)
	c.add(time.Minute)
	if !s.Allow("a") || s.Allow("a") {
		t.Fatal("one probe")
	}
	c.add(time.Minute)
	if !s.Allow("a") {
		t.Fatal("a probe that never reported back is replaced after a cooldown")
	}
}

func TestConcurrentUse(t *testing.T) {
	s := New(Config{MinEvents: 5, OpenRate: 0.5, Cooldown: time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				s.Observe("k", (i+j)%3 == 0)
				_ = s.Allow("k")
				_ = s.IsOpen("k")
			}
		}(i)
	}
	wg.Wait()
}

func join(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += " "
		}
		out += x
	}
	return out
}
