package session

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
)

func TestBreakerRetiresIdleIdentitiesAndFeedsOutcomes(t *testing.T) {
	proxies, err := proxy.New(config.Proxy{
		Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list",
			Endpoints: []string{"http://203.0.113.10:8000", "http://198.51.100.10:8000"}}},
		Breaker: breaker.Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.facebook.com", Profile: "chrome_146", IdentityRPS: 1, IdentityBurst: 1,
		MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 4}, proxies,
		fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a, err := pool.Acquire(ctx, "isp")
	if err != nil || a.Endpoint.Bucket != "isp|203.0.113.0/24" {
		t.Fatalf("%+v %v", a.Endpoint, err)
	}
	pool.Release(a)

	// Outcomes recorded on an identity reach its bucket's breaker.
	b, _ := pool.Acquire(ctx, "isp")
	if b != a {
		t.Fatal("the idle identity is reused while its bucket is closed")
	}
	Outcome(b, detect.Transport)
	Outcome(b, detect.Transport)
	if !proxies.IsOpen(a.Endpoint) {
		t.Fatal("two transport failures of two must open the bucket")
	}
	pool.Release(b)

	c, err := pool.Acquire(ctx, "isp")
	if err != nil || c == a || c.Endpoint.Bucket != "isp|198.51.100.0/24" {
		t.Fatalf("want a fresh identity on the other /24: %+v %v", c.Endpoint, err)
	}
	if pool.live["isp"] != 1 {
		t.Fatalf("the identity on the open bucket must be retired: live=%d", pool.live["isp"])
	}
}

// An exit that stopped answering retires its identity; failures broken up
// by answers do not.
func TestTransportFailuresRetireAnIdentity(t *testing.T) {
	id := newIdentity("x", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, nil, 1, 1, 0)
	for range 20 {
		for range MaxTransportFailures - 1 {
			if retire, _ := id.Record(detect.Transport); retire {
				t.Fatal("retired on failures broken up by answers")
			}
		}
		id.Record(detect.OK)
	}
	for i := 1; i <= MaxTransportFailures; i++ {
		retire, reason := id.Record(detect.Transport)
		if retire != (i == MaxTransportFailures) || (retire && reason != "transport") {
			t.Fatalf("failure %d: retire=%v reason=%q", i, retire, reason)
		}
	}
	if id.RetireReason() != "transport" {
		t.Fatalf("reason %q", id.RetireReason())
	}
}

// Rate limits in a row send an identity to ever longer cooldowns before
// they retire it; an answer between them starts the count again. Before
// M2 the second one in a row retired the identity, so the cooldown never
// grew past a minute (fbs reference, chapter 1 §15).
func TestRateLimitCooldownGrows(t *testing.T) {
	id := newIdentity("x", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, nil, 1, 1, 0)
	cooldown := func() time.Duration {
		id.mu.Lock()
		defer id.mu.Unlock()
		return time.Until(id.cooldown).Round(time.Minute)
	}
	for range 3 {
		if retire, _ := id.Record(detect.RateLimited); retire || cooldown() != time.Minute {
			t.Fatalf("a rate limit after an answer: retire=%v cooldown=%s, want 1m", retire, cooldown())
		}
		id.Record(detect.OK)
	}
	for i, want := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute} {
		if retire, reason := id.Record(detect.RateLimited); retire {
			t.Fatalf("rate limit %d in a row retired the identity (%s)", i+1, reason)
		}
		if got := cooldown(); got != want {
			t.Fatalf("rate limit %d in a row: cooldown %s, want %s", i+1, got, want)
		}
		if !id.coolingDown() {
			t.Fatal("not cooling down")
		}
	}
	if retire, reason := id.Record(detect.RateLimited); !retire || reason != "blocked" {
		t.Fatalf("rate limit %d in a row: retire=%v reason=%q", RateLimitStreak, retire, reason)
	}
	if RateLimitCooldown(5) != 15*time.Minute || RateLimitCooldown(9) != 15*time.Minute || RateLimitCooldown(0) != time.Minute {
		t.Fatalf("cooldown bounds: %s %s %s", RateLimitCooldown(0), RateLimitCooldown(5), RateLimitCooldown(9))
	}
	// Login walls and checkpoints still retire at once.
	for _, c := range []detect.Class{detect.LoginWall, detect.Checkpoint} {
		id := newIdentity("y", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, nil, 1, 1, 0)
		if retire, _ := id.Record(c); !retire {
			t.Fatalf("%s did not retire", c)
		}
	}
}

// doer fails every request with the error fail returns.
type doer struct {
	transport.Doer
	fail func() error
}

func (d doer) Do(context.Context, *transport.Request) (*transport.Response, error) {
	return nil, d.fail()
}

// A load that got no response is marked ErrTransport and counts against the
// exit; one cancelled by its caller does not.
func TestCancelledLoadsDoNotRetireAnIdentity(t *testing.T) {
	base, _ := url.Parse("https://www.facebook.com")
	l := &Loader{Rules: detect.DefaultRules(), Hooks: Hooks{Extract: func([]byte) (Tokens, bool) { return nil, false }}}
	cancelled := newIdentity("c", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, base, 1000, 10, 0)
	dead := newIdentity("d", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, base, 1000, 10, 0)
	std := transport.NewStd(time.Second)
	for range MaxTransportFailures {
		ctx, cancel := context.WithCancel(context.Background())
		cancelled.Doer = doer{Doer: std, fail: func() error { cancel(); return ctx.Err() }}
		if _, err := l.Load(ctx, cancelled, "https://www.facebook.com/x"); !errors.Is(err, ErrTransport) {
			t.Fatalf("cancelled load: %v", err)
		}
		dead.Doer = doer{Doer: std, fail: func() error { return errors.New("awaiting headers") }}
		if _, err := l.Load(context.Background(), dead, "https://www.facebook.com/x"); !errors.Is(err, ErrTransport) {
			t.Fatalf("dead load: %v", err)
		}
	}
	if r := cancelled.RetireReason(); r != "" {
		t.Fatalf("cancelled loads retired the identity: %q", r)
	}
	if r := dead.RetireReason(); r != "transport" {
		t.Fatalf("dead exit: %q", r)
	}
}

// serves answers every request with a 200 and body.
type serves struct {
	transport.Doer
	body string
}

func (s serves) Do(context.Context, *transport.Request) (*transport.Response, error) {
	return &transport.Response{Status: 200, Header: http.Header{"Content-Type": {"text/html"}}, Body: []byte(s.body)}, nil
}

// lsdHooks are a platform's hooks for these tests: a page carrying "LSD"
// has tokens, and one that says "restricted" is not the page asked for.
func lsdHooks() Hooks {
	return Hooks{
		Extract: func(html []byte) (Tokens, bool) {
			if bytes.Contains(html, []byte(`"LSD"`)) {
				return Tokens{"lsd": "x"}, true
			}
			return nil, false
		},
		Unavailable: func(html []byte, _ Tokens) bool { return bytes.Contains(html, []byte("restricted")) },
	}
}

// A page the platform's Unavailable hook rejects (Facebook: one restricted
// to logged-in viewers, or removed) is unavailable, marked as such, and its
// session tokens are kept; a page it accepts is left as it was. (Facebook's
// rule is tested in internal/fb/client.)
func TestLoadPagesThatNameNothing(t *testing.T) {
	const tokens = `<script>{"define":[["LSD",[],{"token":"AVq"},323]]}</script>`
	base, _ := url.Parse("https://www.facebook.com")
	l := &Loader{Rules: detect.DefaultRules(), Hooks: lsdHooks()}
	for name, c := range map[string]struct {
		body string
		want detect.Class
	}{
		"restricted":  {`<html>restricted` + tokens + `</html>`, detect.ContentUnavailable},
		"public page": {tokens + `<script>{"user":{"id":"100064000000777"}}</script>`, detect.OK},
	} {
		id := newIdentity(name, proxy.Endpoint{Tier: "isp"}, Fingerprint{}, serves{Doer: transport.NewStd(time.Second), body: c.body}, base, 1000, 10, 0)
		pl, err := l.Load(context.Background(), id, "https://www.facebook.com/x")
		if err != nil || pl.Class != c.want || pl.NamesNothing != (c.want == detect.ContentUnavailable) || !id.Bootstrapped() {
			t.Errorf("%s: class %v (%v), bootstrapped %v, err %v", name, pl.Class, pl.Reason, id.Bootstrapped(), err)
		}
	}
}

func TestLoadRefusesOtherSites(t *testing.T) {
	base, _ := url.Parse("https://www.facebook.com")
	l := &Loader{Rules: detect.DefaultRules(), Hooks: lsdHooks()}
	for _, raw := range []string{
		"https://attacker.example/x",
		"https://facebook.com.attacker.example/x",
		"https://notfacebook.com/x",
		"ftp://www.facebook.com/x",
		"/relative",
		"://bad",
	} {
		id := newIdentity("a", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, failDoer{transport.NewStd(time.Second)}, base, 1000, 10, 0)
		if _, err := l.Load(context.Background(), id, raw); err == nil || id.RetireReason() != "" {
			t.Errorf("%s: loaded (err %v, retired %q)", raw, err, id.RetireReason())
		}
	}
	const tokens = `<script>{"define":[["LSD",[],{"token":"AVq"},323]]}</script>`
	for _, raw := range []string{"https://www.facebook.com/nike", "https://m.facebook.com/nike"} {
		id := newIdentity("b", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, serves{Doer: transport.NewStd(time.Second), body: tokens}, base, 1000, 10, 0)
		if _, err := l.Load(context.Background(), id, raw); err != nil {
			t.Errorf("%s: %v", raw, err)
		}
	}
}

// failDoer fails the test if a request is sent.
type failDoer struct{ transport.Doer }

func (failDoer) Do(context.Context, *transport.Request) (*transport.Response, error) {
	panic("a request was sent")
}

// Walls on page loads pause the exit's page loads but leave its breaker
// alone, so identities that already have tokens keep serving GraphQL there;
// GraphQL blocks still open it.
func TestPageLoadWallsLeaveTheExitServing(t *testing.T) {
	proxies, err := proxy.New(config.Proxy{
		Tiers:     []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{"http://isp.proxy.example:10001"}}},
		Breaker:   breaker.Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Hour},
		PageLoads: config.PageLoads{Interval: time.Minute, Backoff: time.Hour, MaxBackoff: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.facebook.com", Profile: "chrome_146", IdentityRPS: 1, IdentityBurst: 1,
		MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 4}, proxies,
		fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	id, err := pool.Acquire(context.Background(), "isp")
	if err != nil {
		t.Fatal(err)
	}
	if !id.TakeReservation() || id.TakeReservation() {
		t.Fatal("a new identity holds one reserved page load")
	}
	for range 5 {
		PageOutcome(id, detect.LoginWall)
	}
	if proxies.IsOpen(id.Endpoint) {
		t.Fatal("page-load walls opened the exit")
	}
	if proxies.TakePageLoad(id.Endpoint) {
		t.Fatal("a walled exit must back off its page loads")
	}
	Outcome(id, detect.RateLimited)
	Outcome(id, detect.RateLimited)
	if !proxies.IsOpen(id.Endpoint) {
		t.Fatal("GraphQL blocks must open the exit")
	}
}

type gateFunc func(proxy.Endpoint) (bool, string)

func (f gateFunc) PageLoad(ep proxy.Endpoint) (bool, string) { return f(ep) }

// Loader.Load takes a slot from the exit's gate before any page load.
// Refused, nothing is sent: ErrPaced (ErrExitPaused while the exit backs
// off). An identity without tokens is then retired, so the next task gets
// a fresh one on an exit that can load; one with tokens is kept for
// GraphQL. A new identity's reservation spares it the gate once, while
// young.
func TestLoaderPageGate(t *testing.T) {
	refuse := func(why string) *Loader {
		return &Loader{Hooks: lsdHooks(), Gate: gateFunc(func(proxy.Endpoint) (bool, string) { return false, why })}
	}
	id := newIdentity("a", proxy.Endpoint{Tier: "isp", Exit: "isp|h:1"}, Fingerprint{}, nil, nil, 1, 1, 100)
	if _, err := refuse("paced").Load(context.Background(), id, "https://www.facebook.com/x"); !errors.Is(err, ErrPaced) || errors.Is(err, ErrExitPaused) {
		t.Fatalf("paced: %v", err)
	}
	if id.RetireReason() != "paced" {
		t.Fatalf("an identity without tokens is retired: %q", id.RetireReason())
	}
	ok := newIdentity("b", proxy.Endpoint{Tier: "isp", Exit: "isp|h:1"}, Fingerprint{}, nil, nil, 1, 1, 100)
	ok.SetTokens(Tokens{"lsd": "x"})
	if _, err := refuse("backoff").Load(context.Background(), ok, "https://www.facebook.com/x"); !errors.Is(err, ErrExitPaused) || !errors.Is(err, ErrPaced) {
		t.Fatalf("backoff: %v", err)
	}
	if ok.RetireReason() != "" {
		t.Fatalf("an identity with tokens is kept: %q", ok.RetireReason())
	}

	young := newIdentity("c", proxy.Endpoint{}, Fingerprint{}, nil, nil, 1, 1, 100)
	young.reserved = true
	if !young.TakeReservation() || young.TakeReservation() {
		t.Fatal("a young identity's reservation is good once")
	}
	old := newIdentity("d", proxy.Endpoint{}, Fingerprint{}, nil, nil, 1, 1, 100)
	old.reserved, old.born = true, time.Now().Add(-time.Minute)
	if old.TakeReservation() {
		t.Fatal("a reservation older than its TTL is gone")
	}
}

// Soft errors in a row drop the identity's tokens at the second (so its
// next use reloads them) and retire it at MaxSoftErrors, fresh tokens or not:
// the page load that brings them doesn't end the run, a request that works
// does, and transport failures leave it as it is.
func TestSoftErrorsRefreshThenRetireAnIdentity(t *testing.T) {
	id := newIdentity("s", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, nil, 1, 1, 0)
	tokens := Tokens{"lsd": "lsd"}
	id.SetTokens(tokens)
	if retire, _ := id.Record(detect.SoftError); retire || !id.Bootstrapped() {
		t.Fatal("one soft error keeps the tokens")
	}
	id.Record(detect.OK)
	id.Record(detect.SoftError)
	id.Record(detect.Transport)
	if !id.Bootstrapped() {
		t.Fatal("soft errors broken up by a success are not in a row")
	}
	id.Record(detect.SoftError) // second in a row: the transport failure counts for neither
	if id.Bootstrapped() {
		t.Fatal("two soft errors in a row drop the tokens")
	}
	PageOutcome(id, detect.OK) // the page load for fresh tokens
	id.SetTokens(tokens)
	if retire, _ := id.Record(detect.SoftError); retire || !id.Bootstrapped() {
		t.Fatal("the third soft error drops the fresh tokens again, or retires the identity")
	}
	if retire, reason := id.Record(detect.SoftError); !retire || reason != "soft_errors" {
		t.Fatalf("after %d soft errors in a row: retire=%v reason=%q", MaxSoftErrors, retire, reason)
	}
}
