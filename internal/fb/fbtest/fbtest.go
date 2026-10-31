// Package fbtest wires the Facebook client layer against an in-process
// fakefb server for tests.
package fbtest

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	fbclient "github.com/abdullah95k/securebrand-microservices-igs/internal/fb/client"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// Env is a ready-to-use client stack against fakefb.
type Env struct {
	World    *fakefb.World
	Fake     *fakefb.Server
	HTTP     *httptest.Server
	Registry *registry.Static
	Pool     *session.Pool
	Loader   *session.Loader
	GQL      *graphql.Client
}

// Options tweak the environment.
type Options struct {
	Spec        fakefb.Spec
	Faults      fakefb.Faults
	IdentityRPS float64
	UseTLS      bool // use the fingerprinted tls-client transport (default std)
	// RotateDocIDs makes fakefb accept only rotated doc_ids, so the seed
	// registry is stale until the ids are rediscovered.
	RotateDocIDs bool
	// Doers, when set, builds every identity's transport (UseTLS aside).
	Doers session.DoerFactory
}

// New starts fakefb and builds the client stack. It is closed via t.Cleanup.
func New(t testing.TB, o Options) *Env {
	t.Helper()
	if o.Spec.Pages == 0 {
		o.Spec = fakefb.DefaultSpec()
	}
	if o.IdentityRPS == 0 {
		o.IdentityRPS = 1000
	}
	seedBytes, err := configs.FS.ReadFile("queries.seed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string][]string{}
	for _, q := range seed.Queries {
		docs[q.FriendlyName] = append(docs[q.FriendlyName], q.DocID)
	}
	if o.RotateDocIDs {
		docs = fakefb.RotatedDocs(docs)
	}
	w := fakefb.NewWorld(o.Spec)
	fs := fakefb.NewServer(w, fakefb.Options{Docs: docs, InlineReply: true, Faults: o.Faults})
	hs := httptest.NewServer(fs)
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
	var factory session.DoerFactory
	switch {
	case o.Doers != nil:
		factory = o.Doers
	case !o.UseTLS:
		factory = func(transport.Options) (transport.Doer, error) { return transport.NewStd(10 * time.Second), nil }
	}
	pool, err := session.NewPool(session.PoolConfig{
		BaseURL: hs.URL, Profile: "chrome_146", Timeout: 10 * time.Second,
		IdentityRPS: o.IdentityRPS, IdentityBurst: 10, MaxRequestsMin: 100000, MaxRequestsMax: 100001, MaxPerTier: 64,
	}, proxies, fps, factory)
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.NewStatic(seed)
	rules := detect.DefaultRules()
	return &Env{
		World: w, Fake: fs, HTTP: hs, Registry: reg, Pool: pool,
		Loader: &session.Loader{Rules: rules, AcceptLanguage: "en-US,en;q=0.9", Hooks: fbclient.Hooks("en-US,en;q=0.9")},
		GQL:    &graphql.Client{Registry: reg, Rules: rules, Builder: fbclient.Builder{AcceptLanguage: "en-US,en;q=0.9"}},
	}
}
