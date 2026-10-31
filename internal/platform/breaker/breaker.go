// Package breaker implements keyed circuit breakers driven by failure rate
// over a sliding window. A breaker opens when, within Window, at least
// MinEvents outcomes were seen and the failure share reached OpenRate. After
// Cooldown it lets one probe through (half-open); ProbeSuccesses successful
// probes close it, a failed probe reopens it with a doubled cooldown.
package breaker

import (
	"sync"
	"time"
)

// State of one breaker.
type State int

const (
	Closed State = iota
	HalfOpen
	Open
)

func (s State) String() string {
	switch s {
	case HalfOpen:
		return "half_open"
	case Open:
		return "open"
	default:
		return "closed"
	}
}

// Config tunes a Set. Zero values take the defaults noted.
type Config struct {
	Window         time.Duration `koanf:"window"`          // default 1m
	MinEvents      int           `koanf:"min_events"`      // default 20
	OpenRate       float64       `koanf:"open_rate"`       // default 0.15
	Cooldown       time.Duration `koanf:"cooldown"`        // default 2m
	MaxCooldown    time.Duration `koanf:"max_cooldown"`    // default 30m
	ProbeSuccesses int           `koanf:"probe_successes"` // default 3
}

func (c Config) withDefaults() Config {
	if c.Window <= 0 {
		c.Window = time.Minute
	}
	if c.MinEvents <= 0 {
		c.MinEvents = 20
	}
	if c.OpenRate <= 0 {
		c.OpenRate = 0.15
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 2 * time.Minute
	}
	if c.MaxCooldown < c.Cooldown {
		c.MaxCooldown = max(c.Cooldown, 30*time.Minute)
	}
	if c.ProbeSuccesses <= 0 {
		c.ProbeSuccesses = 3
	}
	return c
}

const slots = 10 // window resolution: Window/slots

type slot struct {
	start      time.Time
	ok, failed int
}

type entry struct {
	ring      [slots]slot
	state     State
	openUntil time.Time
	cooldown  time.Duration
	probeAt   time.Time // when the in-flight probe was admitted; zero = none
	successes int
}

// Set holds one breaker per key. It is safe for concurrent use.
type Set struct {
	cfg Config
	// OnChange, when set, is called (outside the lock) on every transition.
	OnChange func(key string, from, to State)
	now      func() time.Time

	mu      sync.Mutex
	entries map[string]*entry
}

// New returns a Set.
func New(cfg Config) *Set {
	return &Set{cfg: cfg.withDefaults(), now: time.Now, entries: map[string]*entry{}}
}

func (s *Set) entry(key string) *entry {
	e := s.entries[key]
	if e == nil {
		e = &entry{cooldown: s.cfg.Cooldown}
		s.entries[key] = e
	}
	return e
}

type change struct {
	key      string
	from, to State
}

func (s *Set) notify(ch []change) {
	if s.OnChange == nil {
		return
	}
	for _, c := range ch {
		s.OnChange(c.key, c.from, c.to)
	}
}

// advance moves an open breaker whose cooldown ended to half-open.
func (s *Set) advance(key string, e *entry, now time.Time, ch *[]change) {
	if e.state == Open && !now.Before(e.openUntil) {
		*ch = append(*ch, change{key, Open, HalfOpen})
		e.state, e.probeAt, e.successes = HalfOpen, time.Time{}, 0
	}
}

// Observe records one outcome under key.
func (s *Set) Observe(key string, failed bool) {
	var ch []change
	s.mu.Lock()
	now := s.now()
	e := s.entry(key)
	s.advance(key, e, now, &ch)
	switch e.state {
	case HalfOpen:
		if failed {
			e.cooldown = min(e.cooldown*2, s.cfg.MaxCooldown)
			s.open(key, e, now, &ch)
			break
		}
		e.successes++
		e.probeAt = time.Time{} // the probe answered; admit the next one
		if e.successes >= s.cfg.ProbeSuccesses {
			ch = append(ch, change{key, HalfOpen, Closed})
			e.state, e.cooldown, e.ring = Closed, s.cfg.Cooldown, [slots]slot{}
		}
	case Closed:
		width := s.cfg.Window / slots
		start := now.Truncate(width)
		sl := &e.ring[(start.UnixNano()/int64(width))%slots]
		if !sl.start.Equal(start) {
			*sl = slot{start: start}
		}
		if failed {
			sl.failed++
		} else {
			sl.ok++
		}
		var ok, bad int
		for _, x := range e.ring {
			if now.Sub(x.start) < s.cfg.Window {
				ok, bad = ok+x.ok, bad+x.failed
			}
		}
		if n := ok + bad; n >= s.cfg.MinEvents && float64(bad)/float64(n) >= s.cfg.OpenRate {
			s.open(key, e, now, &ch)
		}
	case Open:
		// Outcomes of requests already in flight when it opened.
	}
	s.mu.Unlock()
	s.notify(ch)
}

func (s *Set) open(key string, e *entry, now time.Time, ch *[]change) {
	*ch = append(*ch, change{key, e.state, Open})
	e.state, e.openUntil, e.ring, e.probeAt, e.successes = Open, now.Add(e.cooldown), [slots]slot{}, time.Time{}, 0
}

// Allow reports whether work under key may proceed. Closed: always. Open:
// never. Half-open: one probe at a time; a probe that never reports back is
// replaced after a cooldown.
func (s *Set) Allow(key string) bool {
	var ch []change
	s.mu.Lock()
	now := s.now()
	e := s.entries[key]
	allowed := true
	if e != nil {
		s.advance(key, e, now, &ch)
		switch e.state {
		case Open:
			allowed = false
		case HalfOpen:
			if e.probeAt.IsZero() || now.Sub(e.probeAt) >= e.cooldown {
				e.probeAt = now
			} else {
				allowed = false
			}
		}
	}
	s.mu.Unlock()
	s.notify(ch)
	return allowed
}

// IsOpen reports whether key is open (cooling down), without admitting a
// probe.
func (s *Set) IsOpen(key string) bool {
	return s.State(key) == Open
}

// State returns the state of key.
func (s *Set) State(key string) State {
	var ch []change
	s.mu.Lock()
	e := s.entries[key]
	st := Closed
	if e != nil {
		s.advance(key, e, s.now(), &ch)
		st = e.state
	}
	s.mu.Unlock()
	s.notify(ch)
	return st
}
