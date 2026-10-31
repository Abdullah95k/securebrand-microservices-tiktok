package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

func testPool(t *testing.T, cfg PoolConfig) *Pool {
	t.Helper()
	proxies, err := proxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	cfg.IdentityRPS, cfg.IdentityBurst, cfg.MaxRequestsMin, cfg.MaxRequestsMax = 1000, 1000, 1000, 1001
	pool, err := NewPool(cfg, proxies, fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(5 * time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

// Another platform's hooks drive the loader: its wall path is not followed
// but classified, its tokens are installed, and its AfterLoad answers an
// interstitial and has the page loaded again.
func TestLoaderHooks(t *testing.T) {
	var pageHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		switch r.URL.Path {
		case "/walled":
			http.Redirect(w, r, "/accounts/login/?next=/walled", http.StatusFound)
		case "/accounts/login/":
			fmt.Fprint(w, `<html>log in token=LOGIN</html>`)
		case "/page":
			if pageHits.Add(1) == 1 {
				fmt.Fprint(w, `<html>interstitial token=T1</html>`)
				return
			}
			fmt.Fprint(w, `<html>token=T2</html>`)
		}
	}))
	defer srv.Close()
	pool := testPool(t, PoolConfig{BaseURL: srv.URL, Profile: "chrome_146", MaxPerTier: 4})
	rules := detect.DefaultRules()
	rules.LoginPaths = []string{"/accounts/login"}
	token := regexp.MustCompile(`token=(\w+)`)
	var reloads int
	l := &Loader{Rules: rules, Hooks: Hooks{
		Extract: func(html []byte) (Tokens, bool) {
			m := token.FindSubmatch(html)
			if m == nil {
				return nil, false
			}
			return Tokens{"csrf_token": string(m[1])}, true
		},
		IsWall: func(path string) bool { return strings.HasPrefix(path, "/accounts/login") },
		AfterLoad: func(_ context.Context, _ *Identity, _ string, html []byte, _ Tokens, canReload bool) bool {
			if canReload && bytes.Contains(html, []byte("interstitial")) {
				reloads++
				return true
			}
			return false
		},
	}}
	ctx := context.Background()
	id, err := pool.Acquire(ctx, "direct")
	if err != nil {
		t.Fatal(err)
	}
	pl, err := l.Load(ctx, id, srv.URL+"/walled")
	if err != nil || pl.Class != detect.LoginWall || id.Bootstrapped() {
		t.Fatalf("wall redirect: %v %+v bootstrapped=%v", err, pl, id.Bootstrapped())
	}
	if _, err := l.Bootstrap(ctx, id, srv.URL+"/page"); err != nil {
		t.Fatal(err)
	}
	if reloads != 1 || pageHits.Load() != 2 || id.Tokens()["csrf_token"] != "T2" {
		t.Fatalf("reloads %d, hits %d, tokens %v", reloads, pageHits.Load(), id.Tokens())
	}
	// A loader without its platform's hooks fails cleanly.
	if _, err := (&Loader{Rules: rules}).Load(ctx, id, srv.URL+"/page"); !errors.Is(err, ErrNoHooks) {
		t.Fatalf("no hooks: %v", err)
	}
}

// New identities pick their fingerprint from the pool's list; a profile
// the fingerprint file lacks is refused.
func TestPoolFingerprintProfiles(t *testing.T) {
	pool := testPool(t, PoolConfig{BaseURL: "https://www.example.com", Profile: "chrome_146",
		Profiles: []string{"chrome_146", "chrome_144"}, MaxPerTier: 64})
	seen := map[string]int{}
	for range 40 {
		id, err := pool.Acquire(context.Background(), "direct")
		if err != nil {
			t.Fatal(err)
		}
		seen[id.FP.Profile]++
	}
	if len(seen) != 2 || seen["chrome_146"] == 0 || seen["chrome_144"] == 0 {
		t.Fatalf("profiles used: %v", seen)
	}
	single := testPool(t, PoolConfig{BaseURL: "https://www.example.com", Profile: "chrome_144", MaxPerTier: 4})
	if id, _ := single.Acquire(context.Background(), "direct"); id.FP.Profile != "chrome_144" {
		t.Fatalf("default profile: %s", id.FP.Profile)
	}
	proxies, _ := proxy.New(config.Proxy{})
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, _ := LoadFingerprints(fpBytes)
	if _, err := NewPool(PoolConfig{BaseURL: "https://www.example.com", Profile: "chrome_146",
		Profiles: []string{"chrome_146", "chrome_999"}}, proxies, fps, nil); err == nil {
		t.Fatal("an unknown profile was accepted")
	}
}
