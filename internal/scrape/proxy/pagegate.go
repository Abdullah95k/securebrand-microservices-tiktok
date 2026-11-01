package proxy

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
)

// ErrExitsPaced means every exit of the tier whose breaker is closed is
// waiting before its next page load (fb.proxy.page_loads).
var ErrExitsPaced = errors.New("proxy: every exit of the tier is pacing its page loads")

// pageGate paces page loads per static exit. Facebook walls page
// loads from an exit that views many pages while its GraphQL keeps working,
// and the exit breaker needs more events than page loads ever reach, so
// page loads get their own gate: at most one per interval, and none for a
// doubling backoff after a walled or failed one.
type pageGate struct {
	cfg config.PageLoads
	now func() time.Time

	mu    sync.Mutex
	exits map[string]*gateExit // gated exits only (tier|host:port of static endpoints)
}

type gateExit struct {
	next        time.Time     // no page load before this (the interval)
	pausedUntil time.Time     // no page load before this (after a failure)
	backoff     time.Duration // the pause after the next failure is twice this
}

// pageResult is what a page load says about its exit.
type pageResult int

const (
	pageLoaded pageResult = iota // the page loaded: resets the pause
	pageFailed                   // walled, blocked or transport: pauses the exit
)

func newPageGate(cfg config.PageLoads) *pageGate {
	return &pageGate{cfg: cfg, now: time.Now, exits: map[string]*gateExit{}}
}

func (g *pageGate) off() bool { return g.cfg.Interval <= 0 && g.cfg.Backoff <= 0 }

// gate adds exit to the gated set.
func (g *pageGate) gate(exit string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.exits[exit]; !ok {
		g.exits[exit] = &gateExit{}
	}
}

// take reserves the next page load on exit. It reports why not ("paced"
// or "backoff") when the exit must wait; restore undoes a reservation that
// went unused.
func (g *pageGate) take(exit string) (ok bool, why string, restore func()) {
	noop := func() {}
	if g.off() {
		return true, "", noop
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.exits[exit]
	if e == nil {
		return true, "", noop // not gated (a gateway, or direct)
	}
	now := g.now()
	switch {
	case now.Before(e.pausedUntil):
		return false, "backoff", noop
	case now.Before(e.next):
		return false, "paced", noop
	}
	prev := e.next
	e.next = now.Add(g.cfg.Interval)
	return true, "", func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if e.next.Equal(now.Add(g.cfg.Interval)) {
			e.next = prev
		}
	}
}

// observe records a page load's outcome on exit. A page that loads resets
// the backoff (a pause already running keeps running); a failure pauses
// the exit for the next, doubled, backoff.
func (g *pageGate) observe(exit string, r pageResult) {
	if g.off() {
		return
	}
	g.mu.Lock()
	e := g.exits[exit]
	if e == nil {
		g.mu.Unlock()
		return
	}
	if r == pageLoaded {
		e.backoff = 0
		g.mu.Unlock()
		return
	}
	if e.backoff == 0 {
		e.backoff = g.cfg.Backoff
	} else {
		e.backoff = min(2*e.backoff, max(g.cfg.MaxBackoff, g.cfg.Backoff))
	}
	if until := g.now().Add(e.backoff); until.After(e.pausedUntil) {
		e.pausedUntil = until
	}
	pause := e.backoff
	g.mu.Unlock()
	if pause > 0 {
		slog.Info("proxy: page loads paused on an exit", "exit", exit, "for", pause.String())
	}
}

func deferred(tier, why string) {
	metrics.PageLoadsDeferred.WithLabelValues(tier, why).Inc()
}
