package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
)

// PoolConfig configures identity creation.
type PoolConfig struct {
	BaseURL string
	Profile string // default fingerprint profile
	// Profiles, when set, are the fingerprint profiles new identities pick
	// from at random, so one pool does not show a single browser build;
	// empty means Profile alone.
	Profiles       []string
	Timeout        time.Duration
	IdentityRPS    float64
	IdentityBurst  int
	MaxRequestsMin int
	MaxRequestsMax int
	// MaxPerTier caps live identities per tier; Acquire waits at the cap.
	MaxPerTier int
	// Store, when set, keeps sessions across a restart (Save, Resume).
	Store Store
	// PacedWait is how long Acquire waits for an identity of the tier to be
	// released when no exit can start a new one (they pace or back off
	// their page loads) before it gives up (default 30s).
	PacedWait time.Duration
}

// pacedCreateEvery is how often a waiting Acquire tries again to start a
// new identity, between releases.
const pacedCreateEvery = 5 * time.Second

// DoerFactory builds a transport for an identity (swappable in tests).
type DoerFactory func(o transport.Options) (transport.Doer, error)

// Pool owns this pod's identities. Each identity is leased exclusively.
type Pool struct {
	cfg     PoolConfig
	base    *url.URL
	proxies *proxy.Pool
	fps     map[string]Fingerprint
	newDoer DoerFactory

	// owner names this pool's snapshots in the store (Checkpoint).
	owner string

	mu   sync.Mutex
	cond *sync.Cond
	idle map[string][]*Identity
	live map[string]int
	all  map[*Identity]struct{} // every live identity, idle or in use
}

func NewPool(cfg PoolConfig, proxies *proxy.Pool, fps map[string]Fingerprint, f DoerFactory) (*Pool, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("session: base url: %w", err)
	}
	if len(cfg.Profiles) == 0 {
		cfg.Profiles = []string{cfg.Profile}
	}
	for _, name := range cfg.Profiles {
		if _, ok := fps[name]; !ok {
			return nil, fmt.Errorf("session: no fingerprint for profile %q", name)
		}
	}
	if f == nil {
		f = transport.NewTLS
	}
	if cfg.MaxPerTier <= 0 {
		cfg.MaxPerTier = 512
	}
	if cfg.PacedWait <= 0 {
		cfg.PacedWait = 30 * time.Second
	}
	p := &Pool{cfg: cfg, base: base, proxies: proxies, fps: fps, newDoer: f, owner: "crawler-" + randAlnum(10),
		idle: map[string][]*Identity{}, live: map[string]int{}, all: map[*Identity]struct{}{}}
	p.cond = sync.NewCond(&p.mu)
	return p, nil
}

// Base returns the platform's base URL.
func (p *Pool) Base() *url.URL { return p.base }

// Acquire returns an identity on tier for exclusive use. Fresh identities
// are not bootstrapped yet; the caller bootstraps them on first use.
//
// When no exit can start a new identity (they pace or back off their page
// loads) while identities of the tier are at work, Acquire waits up to
// PacedWait for one of them to be released: the task holds its slot rather
// than going round the retry topics, which under load turns into a hot loop
// of millions of retries.
func (p *Pool) Acquire(ctx context.Context, tier string) (*Identity, error) {
	return p.acquire(ctx, tier, nil)
}

// AcquireForPageLoad is Acquire for a task whose first request is a page
// load (a mention check): it takes an idle identity only when that
// identity's exit can load a page now, or the identity still holds the page
// load reserved when it was created; otherwise it starts a new identity on
// an exit that can (Next reserves the load), or waits as Acquire does.
// Taking any idle identity, such a task would mostly find its exit pacing
// its page loads, give up, and on its retry get the same identity back
// (the one released last is handed out first).
func (p *Pool) AcquireForPageLoad(ctx context.Context, tier string) (*Identity, error) {
	return p.acquire(ctx, tier, func(id *Identity) bool {
		return id.holdsReservation() || p.proxies.CanLoadPage(id.Endpoint)
	})
}

// acquire is Acquire, taking only idle identities usable accepts (nil:
// any).
func (p *Pool) acquire(ctx context.Context, tier string, usable func(*Identity) bool) (*Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var waitUntil, createAt time.Time
	var pacedErr error
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		list := p.idle[tier]
		for i := len(list) - 1; i >= 0; i-- {
			id := list[i]
			if p.proxies.IsOpen(id.Endpoint) {
				// Its exit bucket is cooling down: retire rather than reuse.
				list = append(list[:i], list[i+1:]...)
				p.idle[tier] = list
				p.live[tier]--
				delete(p.all, id)
				metrics.IdentityActive.WithLabelValues(tier).Set(float64(p.live[tier]))
				metrics.IdentityRetired.WithLabelValues(tier, "breaker").Inc()
				go id.Doer.Close()
				continue
			}
			if id.coolingDown() || (usable != nil && !usable(id)) {
				continue
			}
			p.idle[tier] = append(list[:i], list[i+1:]...)
			return id, nil
		}
		if p.live[tier] < p.cfg.MaxPerTier && !time.Now().Before(createAt) {
			p.live[tier]++
			p.mu.Unlock()
			id, err := p.create(tier)
			p.mu.Lock()
			if err == nil {
				p.all[id] = struct{}{}
				metrics.IdentityActive.WithLabelValues(tier).Set(float64(p.live[tier]))
				return id, nil
			}
			p.live[tier]--
			// Exits that pace or back off their page loads still serve the
			// identities at work; with every breaker open, a released one
			// would be retired, so there is nothing to wait for.
			waitable := errors.Is(err, proxy.ErrExitsPaced) || (errors.Is(err, proxy.ErrTierOpen) && p.proxies.Available(tier))
			if !waitable || p.live[tier] == 0 {
				return nil, err // nothing to wait for
			}
			now := time.Now()
			if waitUntil.IsZero() {
				waitUntil = now.Add(p.cfg.PacedWait)
			}
			createAt, pacedErr = now.Add(pacedCreateEvery), err
		}
		// At the cap, or no exit can start an identity: wait for a release
		// (or a cooldown to end). A release may never suit a page load at
		// the cap: that wait is bounded too.
		if usable != nil && waitUntil.IsZero() && p.live[tier] >= p.cfg.MaxPerTier {
			waitUntil, pacedErr = time.Now().Add(p.cfg.PacedWait), proxy.ErrExitsPaced
		}
		wake := time.Second
		if !waitUntil.IsZero() {
			left := time.Until(waitUntil)
			if left <= 0 {
				return nil, pacedErr
			}
			wake = min(wake, left)
		}
		stop := context.AfterFunc(ctx, func() { p.mu.Lock(); p.cond.Broadcast(); p.mu.Unlock() })
		timer := time.AfterFunc(wake, func() { p.mu.Lock(); p.cond.Broadcast(); p.mu.Unlock() })
		p.cond.Wait()
		stop()
		timer.Stop()
	}
}

// Release returns id to the pool, or retires it.
func (p *Pool) Release(id *Identity) {
	tier := id.Endpoint.Tier
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.cond.Broadcast()
	if reason := id.RetireReason(); reason != "" {
		p.live[tier]--
		delete(p.all, id)
		metrics.IdentityActive.WithLabelValues(tier).Set(float64(p.live[tier]))
		metrics.IdentityRetired.WithLabelValues(tier, reason).Inc()
		slog.Debug("session: identity retired", "identity", id.ID, "tier", tier, "reason", reason)
		go id.Doer.Close()
		return
	}
	p.idle[tier] = append(p.idle[tier], id)
}

// Retire marks id as unusable and releases it.
func (p *Pool) Retire(id *Identity, reason string) {
	id.markRetired(reason)
	p.Release(id)
}

func (p *Pool) create(tier string) (*Identity, error) {
	ep, err := p.proxies.Next(tier)
	if err != nil {
		return nil, err
	}
	fp := p.fps[p.cfg.Profiles[randRange(0, len(p.cfg.Profiles))]]
	d, err := p.newDoer(transport.Options{Profile: fp.Profile, ProxyURL: ep.URL, Timeout: p.cfg.Timeout})
	if err != nil {
		return nil, err
	}
	maxReq := randRange(p.cfg.MaxRequestsMin, p.cfg.MaxRequestsMax)
	id := newIdentity(newID(), ep, fp, d, p.base, p.cfg.IdentityRPS, p.cfg.IdentityBurst, maxReq)
	id.reserved = true // proxies.Next reserved its first page load
	id.observe = p.observer(ep)
	return id, nil
}

// observer reports an identity's outcomes on ep to its exit's breaker or,
// for page loads, its page-load gate.
func (p *Pool) observer(ep proxy.Endpoint) func(detect.Class, bool) {
	return func(c detect.Class, pageLoad bool) {
		if pageLoad {
			p.proxies.ObservePageLoad(ep, c)
		} else {
			p.proxies.Observe(ep, c)
		}
	}
}

// Checkpoint leases: a crawler checkpoints its sessions every
// CheckpointEvery and holds its snapshot for checkpointLease, so one that
// dies without saving leaves sessions at most CheckpointEvery old, free
// for the next crawler within checkpointLease.
const (
	CheckpointEvery = 2 * time.Minute
	checkpointLease = 5 * time.Minute
)

// snapshot returns the sessions of every live identity, idle or in use, and
// the exits' page-load pacing. Identities retired, without tokens, or on a
// gateway are left out.
func (p *Pool) snapshot(final bool) Snapshot {
	p.mu.Lock()
	ids := make([]*Identity, 0, len(p.all))
	for id := range p.all {
		ids = append(ids, id)
	}
	p.mu.Unlock()
	snap := Snapshot{Owner: p.owner, SavedAt: time.Now(), Final: final, Exits: p.proxies.PageGateState()}
	for _, id := range ids {
		if s, ok := id.saved(); ok {
			snap.Sessions = append(snap.Sessions, s)
		}
	}
	return snap
}

// Checkpoint stores this crawler's sessions and pacing under its lease
// (RunCheckpoint calls it every CheckpointEvery). It returns how many
// sessions it stored.
func (p *Pool) Checkpoint(ctx context.Context) (int, error) {
	if p.cfg.Store == nil {
		return 0, nil
	}
	snap := p.snapshot(false)
	return len(snap.Sessions), p.cfg.Store.Checkpoint(ctx, p.owner, snap, checkpointLease)
}

// RunCheckpoint checkpoints every interval until ctx ends. A failed
// checkpoint is retried at the next one.
func (p *Pool) RunCheckpoint(ctx context.Context, interval time.Duration) error {
	if p.cfg.Store == nil {
		return nil
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := p.Checkpoint(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("session: checkpoint failed", "err", err)
			}
		}
	}
}

// Save stores this crawler's sessions and pacing as it stops and ends its
// lease, so the next crawler takes them at once, and closes the
// identities. Call it once the crawler has stopped using the pool. It
// returns how many sessions it stored.
func (p *Pool) Save(ctx context.Context) (int, error) {
	if p.cfg.Store == nil {
		return 0, nil
	}
	snap := p.snapshot(true)
	p.mu.Lock()
	ids := make([]*Identity, 0, len(p.all))
	for id := range p.all {
		ids = append(ids, id)
	}
	p.idle, p.live, p.all = map[string][]*Identity{}, map[string]int{}, map[*Identity]struct{}{}
	p.mu.Unlock()
	err := p.cfg.Store.Checkpoint(ctx, p.owner, snap, checkpointLease)
	if err == nil {
		err = p.cfg.Store.Release(ctx, p.owner)
	}
	for _, id := range ids {
		id.Doer.Close()
	}
	if err != nil {
		return 0, err
	}
	return len(snap.Sessions), nil
}

// Resume takes what crawlers that stopped left behind (Save, or the last
// Checkpoint of one that died): their exits' page-load pauses, and their
// sessions, which become idle identities with their tokens and cookies, so
// the tasks they serve need no page load. Each snapshot is taken once.
// Sessions it can't rebuild are dropped: an exit no longer configured or
// cooling down, another fingerprint profile, or a tier at its cap. It
// returns how many sessions resumed.
func (p *Pool) Resume(ctx context.Context) (int, error) {
	if p.cfg.Store == nil {
		return 0, nil
	}
	snaps, err := p.cfg.Store.TakeOrphans(ctx, p.owner)
	n := 0
	for _, snap := range snaps {
		p.proxies.RestorePageGate(snap.Exits)
		added := p.resumeSessions(snap.Sessions)
		n += added
		// Final=false: the crawler stopped without saving (it crashed or was
		// killed), and these are from its last checkpoint.
		slog.Info("session: resumed a stopped crawler's sessions", "sessions", added, "saved", len(snap.Sessions),
			"exits", len(snap.Exits), "saved_on_stop", snap.Final, "age", time.Since(snap.SavedAt).Round(time.Second).String())
	}
	return n, err
}

func (p *Pool) resumeSessions(saved []Saved) int {
	n := 0
	for _, s := range saved {
		id := p.rebuild(s)
		if id == nil {
			continue
		}
		p.mu.Lock()
		if p.live[s.Tier] >= p.cfg.MaxPerTier {
			p.mu.Unlock()
			id.Doer.Close()
			continue
		}
		p.live[s.Tier]++
		p.idle[s.Tier] = append(p.idle[s.Tier], id)
		p.all[id] = struct{}{}
		metrics.IdentityActive.WithLabelValues(s.Tier).Set(float64(p.live[s.Tier]))
		p.cond.Broadcast()
		p.mu.Unlock()
		metrics.Bootstraps.WithLabelValues("resumed", "ok").Inc()
		n++
	}
	if len(saved) > n {
		metrics.Bootstraps.WithLabelValues("resumed", "dropped").Add(float64(len(saved) - n))
	}
	return n
}

// RunResume resumes, every interval, what crawlers that stopped after this
// one started left behind: a rolling update starts the new pod first, and
// a crawler that died frees its last checkpoint only when its lease ends.
func (p *Pool) RunResume(ctx context.Context, interval time.Duration) error {
	if p.cfg.Store == nil {
		return nil
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if n, err := p.Resume(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("session: saved sessions not resumed", "resumed", n, "err", err)
			}
		}
	}
}

// rebuild makes a saved session an identity again, on its exit, or returns
// nil when it can't.
func (p *Pool) rebuild(s Saved) *Identity {
	if !slices.Contains(p.cfg.Profiles, s.Profile) || len(s.Tokens) == 0 {
		return nil
	}
	fp := p.fps[s.Profile]
	ep, ok := p.proxies.Resume(s.Tier, s.Exit)
	if !ok {
		return nil
	}
	d, err := p.newDoer(transport.Options{Profile: fp.Profile, ProxyURL: ep.URL, Timeout: p.cfg.Timeout})
	if err != nil {
		return nil
	}
	for name, v := range s.Cookies {
		d.SetCookie(p.base, name, v)
	}
	id := newIdentity(s.ID, ep, fp, d, p.base, p.cfg.IdentityRPS, p.cfg.IdentityBurst, s.MaxRequests)
	id.tokens, id.bootstrapped = s.Tokens, s.Bootstrapped
	id.sessionID, id.requests = s.SessionID, s.Requests
	id.req.Store(s.Req)
	id.observe = p.observer(ep)
	return id
}

// Outcome records a GraphQL request's outcome on id and its exit's breaker.
func Outcome(id *Identity, c detect.Class) { outcome(id, c, false) }

// PageOutcome records a page load's outcome on id and its exit's page-load
// gate (not its breaker: see proxy.Pool.ObservePageLoad).
func PageOutcome(id *Identity, c detect.Class) { outcome(id, c, true) }

func outcome(id *Identity, c detect.Class, pageLoad bool) {
	if id.observe != nil {
		id.observe(c, pageLoad)
	}
	if retire, reason := id.record(c, pageLoad); retire {
		slog.Debug("session: identity marked for retirement", "identity", id.ID, "reason", reason, "class", c.String())
	}
}
