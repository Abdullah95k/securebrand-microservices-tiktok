package proxy

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func gatedPool(t *testing.T, pl config.PageLoads) (*Pool, *clock) {
	t.Helper()
	p, err := New(config.Proxy{
		Tiers: []config.ProxyTier{
			{Name: "isp", Kind: "static_list", Endpoints: []string{"http://u:p@isp.proxy.example:10001", "http://u:p@isp.proxy.example:10002"}},
			{Name: "res", Kind: "gateway", Gateway: "http://gw.example:7777", Username: "u", Password: "p", Country: "us"},
		},
		Routing:   map[string][]string{"default": {"isp"}},
		Breaker:   breaker.Config{MinEvents: 4, OpenRate: 0.5, Cooldown: time.Hour},
		PageLoads: pl,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := &clock{t: time.Date(2026, 11, 1, 3, 0, 0, 0, time.UTC)}
	p.pages.now = c.now
	return p, c
}

var pilotGate = config.PageLoads{Interval: 2 * time.Minute, Backoff: 15 * time.Minute, MaxBackoff: time.Hour}

// A new identity starts with a page load: each exit starts at most one per
// interval, and when every exit is only waiting, the error says so.
func TestNewIdentitiesArePacedPerExit(t *testing.T) {
	p, c := gatedPool(t, pilotGate)
	a, err := p.Next("isp")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Next("isp")
	if err != nil || b.Bucket == a.Bucket {
		t.Fatalf("the second identity goes to the other exit: %+v %v", b, err)
	}
	if _, err := p.Next("isp"); !errors.Is(err, ErrExitsPaced) || errors.Is(err, ErrTierOpen) {
		t.Fatalf("both exits just loaded a page: %v", err)
	}
	if !p.Available("isp") {
		t.Fatal("pacing is not a closed tier")
	}
	c.add(2 * time.Minute)
	if _, err := p.Next("isp"); err != nil {
		t.Fatalf("after the interval: %v", err)
	}
	// A gateway gives every session its own IP: never paced.
	for range 5 {
		if _, err := p.Next("res"); err != nil {
			t.Fatalf("gateway: %v", err)
		}
	}
}

// A walled page load pauses the exit's page loads, doubling up to the cap,
// and a page that loads resets the pause. Walls never open the exit's
// breaker, so its identities keep serving GraphQL; a transport failure does
// count, since the exit itself is failing.
func TestPageLoadWallsBackOffWithoutOpeningTheExit(t *testing.T) {
	p, c := gatedPool(t, pilotGate)
	ep, _ := p.Next("isp")
	for i, pause := range []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, time.Hour} {
		p.ObservePageLoad(ep, detect.LoginWall)
		c.add(pause - time.Second)
		if p.TakePageLoad(ep) {
			t.Fatalf("wall %d: a page load before the %s pause ended", i+1, pause)
		}
		c.add(time.Second)
		if !p.TakePageLoad(ep) {
			t.Fatalf("wall %d: no page load after the %s pause", i+1, pause)
		}
	}
	if p.IsOpen(ep) || !p.Available("isp") {
		t.Fatal("page-load walls must not open the exit")
	}
	// With every exit backing off after walls, a new identity waits as long
	// as for an open breaker (ErrTierOpen), not the short pacing wait.
	other, _ := p.Next("isp")
	p.ObservePageLoad(other, detect.LoginWall)
	p.ObservePageLoad(ep, detect.LoginWall)
	if _, err := p.Next("isp"); !errors.Is(err, ErrTierOpen) || errors.Is(err, ErrExitsPaced) {
		t.Fatalf("every exit backing off: %v", err)
	}
	c.add(2 * time.Hour)
	p.ObservePageLoad(ep, detect.OK)
	p.ObservePageLoad(ep, detect.LoginWall)
	c.add(15 * time.Minute)
	if !p.TakePageLoad(ep) {
		t.Fatal("a page that loads resets the pause to its start")
	}
	for range 4 {
		p.ObservePageLoad(ep, detect.Transport)
	}
	if !p.IsOpen(ep) {
		t.Fatal("transport failures on page loads open the exit")
	}
	// GraphQL blocks still open it as before.
	q, _ := gatedPool(t, pilotGate)
	e2, _ := q.Next("isp")
	block(q, e2, 4)
	if !q.IsOpen(e2) {
		t.Fatal("GraphQL blocks open the exit")
	}
}

// Zero settings turn the gate off; direct egress is never gated.
func TestPageLoadGateOff(t *testing.T) {
	p, _ := gatedPool(t, config.PageLoads{})
	for range 6 {
		ep, err := p.Next("isp")
		if err != nil {
			t.Fatal(err)
		}
		p.ObservePageLoad(ep, detect.LoginWall)
		if !p.TakePageLoad(ep) {
			t.Fatal("gate off")
		}
	}
	if !p.TakePageLoad(Endpoint{Tier: "direct"}) {
		t.Fatal("direct is never gated")
	}
}

// Each static exit (IP:port) has its own gate, even when several share a
// /24 bucket for the breaker.
func TestPageLoadsArePacedPerIP(t *testing.T) {
	p, err := New(config.Proxy{
		Tiers:     []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{"http://u:p@203.0.113.10:8000", "http://u:p@203.0.113.11:8000"}}},
		PageLoads: pilotGate,
	})
	if err != nil {
		t.Fatal(err)
	}
	a, err1 := p.Next("isp")
	b, err2 := p.Next("isp")
	if err1 != nil || err2 != nil || a.Bucket != b.Bucket || a.Exit == b.Exit {
		t.Fatalf("two IPs of one /24 load a page each: %+v %v / %+v %v", a, err1, b, err2)
	}
	if _, err := p.Next("isp"); !errors.Is(err, ErrExitsPaced) {
		t.Fatalf("both IPs just loaded a page: %v", err)
	}
}

// After a pause ends the exit is only paced (a short wait), and outcomes
// that say nothing about the exit (no tokens, a missing page) neither pause
// it nor reset its doubling.
func TestPageLoadPauseAndNeutralOutcomes(t *testing.T) {
	p, c := gatedPool(t, pilotGate)
	ep, _ := p.Next("isp")
	other, _ := p.Next("isp")
	p.ObservePageLoad(ep, detect.LoginWall) // 15m
	p.ObservePageLoad(other, detect.LoginWall)
	c.add(15 * time.Minute)
	if !p.TakePageLoad(ep) || !p.TakePageLoad(other) {
		t.Fatal("the pause is over")
	}
	if _, err := p.Next("isp"); !errors.Is(err, ErrExitsPaced) {
		t.Fatalf("right after the pause both exits are only paced: %v", err)
	}
	p.ObservePageLoad(ep, detect.SoftError)
	p.ObservePageLoad(ep, detect.ContentUnavailable)
	c.add(2 * time.Minute)
	if !p.TakePageLoad(ep) {
		t.Fatal("neutral outcomes don't pause the exit")
	}
	p.ObservePageLoad(ep, detect.LoginWall) // the doubling continues: 30m
	c.add(29 * time.Minute)
	if p.TakePageLoad(ep) {
		t.Fatal("neutral outcomes must not reset the doubling")
	}
}

// A proxy endpoint that is not a URL is refused without echoing it (it
// holds credentials); a tier with endpoints but no kind is refused rather
// than silently sending traffic from the machine's own IP.
func TestProxyConfigIsValidated(t *testing.T) {
	for _, ep := range []string{"isp.proxy.example:10003:user1:secret1", "user1:secret1@isp.proxy.example:10004", "203.0.113.12:8000:user1:secret1"} {
		_, err := New(config.Proxy{Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{ep}}}})
		if err == nil || strings.Contains(err.Error(), "secret1") || strings.Contains(err.Error(), "user1") {
			t.Errorf("%q: %v", ep, err)
		}
	}
	if _, err := New(config.Proxy{Tiers: []config.ProxyTier{{Name: "isp", Endpoints: []string{"http://203.0.113.10:8000"}}}}); err == nil {
		t.Error("endpoints without a kind")
	}
	if _, err := New(config.Proxy{Tiers: []config.ProxyTier{{Name: "res", Kind: "gateway", Gateway: "gw.example"}}}); err == nil {
		t.Error("a gateway that is not a URL")
	}
	p, err := New(config.Proxy{Tiers: []config.ProxyTier{{Name: "dc", Kind: "direct"}}})
	if err != nil {
		t.Fatal(err)
	}
	if ep, err := p.Next("dc"); err != nil || ep.Tier != "dc" {
		t.Fatalf("a direct tier keeps its name: %+v %v", ep, err)
	}
}
