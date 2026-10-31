// Package tttest wires the TikTok client stack against an in-process
// fakett server for tests, as igs's igtest and fbs's fbtest do.
package tttest

import (
	"context"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
)

// AcceptLanguage is the language the test stack sends.
const AcceptLanguage = "en-US,en;q=0.9"

// Env is a ready-to-use client stack against fakett.
type Env struct {
	World    *fakett.World
	Fake     *fakett.Server
	HTTP     *httptest.Server
	Seed     registry.Seed
	Registry *registry.Static
	Pool     *session.Pool
	Loader   *session.Loader
	Client   *graphql.Client

	mu   sync.Mutex
	reqs []*transport.Request
}

// Options tweak the environment.
type Options struct {
	Spec   fakett.Spec
	Faults fakett.Faults
	UseTLS bool // the fingerprinted tls-client transport (default std)
}

// recorder keeps every request a doer sends.
type recorder struct {
	transport.Doer
	env *Env
}

func (r recorder) Do(ctx context.Context, req *transport.Request) (*transport.Response, error) {
	r.env.mu.Lock()
	r.env.reqs = append(r.env.reqs, req)
	r.env.mu.Unlock()
	return r.Doer.Do(ctx, req)
}

// Requests returns the requests sent so far whose path is path ("" for
// all).
func (e *Env) Requests(path string) []*transport.Request {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []*transport.Request
	for _, r := range e.reqs {
		if u, err := url.Parse(r.URL); err == nil && (path == "" || u.Path == path) {
			out = append(out, r)
		}
	}
	return out
}

// New starts fakett and builds the client stack, closed via t.Cleanup.
func New(t testing.TB, o Options) *Env {
	t.Helper()
	if o.Spec.Accounts == 0 {
		o.Spec = fakett.DefaultSpec()
	}
	seed, err := ttclient.Seed()
	if err != nil {
		t.Fatal(err)
	}
	w := fakett.NewWorld(o.Spec)
	fake := fakett.NewServer(w, fakett.Options{Faults: o.Faults})
	hs := httptest.NewServer(fake)
	t.Cleanup(hs.Close)
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	proxies, err := proxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	env := &Env{World: w, Fake: fake, HTTP: hs, Seed: seed}
	factory := func(opts transport.Options) (transport.Doer, error) {
		var d transport.Doer
		if o.UseTLS {
			var err error
			if d, err = transport.NewTLS(opts); err != nil {
				return nil, err
			}
		} else {
			d = transport.NewStd(10 * time.Second)
		}
		return recorder{Doer: d, env: env}, nil
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: hs.URL, Profile: "chrome_152", Timeout: 10 * time.Second,
		IdentityRPS: 1000, IdentityBurst: 10, MaxRequestsMin: 100000, MaxRequestsMax: 100001, MaxPerTier: 64}, proxies, fps, factory)
	if err != nil {
		t.Fatal(err)
	}
	rules := ttdetect.Rules()
	env.Registry = registry.NewStatic(seed)
	env.Pool = pool
	env.Loader = &session.Loader{Rules: rules, AcceptLanguage: AcceptLanguage, Hooks: ttclient.Hooks()}
	env.Client = &graphql.Client{Registry: env.Registry, Rules: rules, Builder: ttclient.Builder{AcceptLanguage: AcceptLanguage}, Decode: ttclient.Decode}
	return env
}

// Identity returns an identity bootstrapped on the account's profile page.
func (e *Env) Identity(t testing.TB, username string) *session.Identity {
	t.Helper()
	id, err := e.Pool.Acquire(context.Background(), "direct")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Loader.Bootstrap(context.Background(), id, e.HTTP.URL+"/@"+username); err != nil {
		t.Fatal(err)
	}
	return id
}
