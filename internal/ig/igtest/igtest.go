// Package igtest wires the Instagram client stack against an in-process
// fakeig server for tests, as fbtest does for Facebook.
package igtest

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// Env is a ready-to-use client stack against fakeig.
type Env struct {
	World    *fakeig.World
	Fake     *fakeig.Server
	HTTP     *httptest.Server
	Seed     registry.Seed
	Registry *registry.Static
	Pool     *session.Pool
	Loader   *session.Loader
	GQL      *graphql.Client
}

// Options tweak the environment.
type Options struct {
	Spec   fakeig.Spec
	Faults fakeig.Faults
	UseTLS bool // the fingerprinted tls-client transport (default std)
	// RotateDocIDs makes fakeig accept only other doc_ids than the seed's.
	RotateDocIDs bool
}

// New starts fakeig and builds the client stack, closed via t.Cleanup.
func New(t testing.TB, o Options) *Env {
	t.Helper()
	if o.Spec.Profiles == 0 {
		o.Spec = fakeig.DefaultSpec()
	}
	seed, err := igclient.Seed()
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string][]string{}
	for _, q := range seed.Queries {
		id := q.DocID
		if o.RotateDocIDs {
			id = "9" + id[1:]
		}
		docs[q.FriendlyName] = append(docs[q.FriendlyName], id)
	}
	w := fakeig.NewWorld(o.Spec)
	fake := fakeig.NewServer(w, fakeig.Options{Docs: docs, Faults: o.Faults})
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
	var factory session.DoerFactory
	if !o.UseTLS {
		factory = func(transport.Options) (transport.Doer, error) { return transport.NewStd(10 * time.Second), nil }
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: hs.URL, Profile: "chrome_146", Timeout: 10 * time.Second,
		IdentityRPS: 1000, IdentityBurst: 10, MaxRequestsMin: 100000, MaxRequestsMax: 100001, MaxPerTier: 64}, proxies, fps, factory)
	if err != nil {
		t.Fatal(err)
	}
	rules := igdetect.Rules()
	reg := registry.NewStatic(seed)
	return &Env{World: w, Fake: fake, HTTP: hs, Seed: seed, Registry: reg, Pool: pool,
		Loader: &session.Loader{Rules: rules, AcceptLanguage: "en-US,en;q=0.9", Hooks: igclient.Hooks()},
		GQL:    &graphql.Client{Registry: reg, Rules: rules, Builder: igclient.Builder{AcceptLanguage: "en-US,en;q=0.9"}}}
}
