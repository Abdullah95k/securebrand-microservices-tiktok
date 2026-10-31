package spike

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/fbtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// echoClients answers the IP echo with ipFor(endpoint, request number).
func echoClients(ipFor func(ep proxy.Endpoint, n int64) string) func(proxy.Endpoint) *http.Client {
	var n atomic.Int64
	return func(ep proxy.Endpoint) *http.Client {
		return &http.Client{Transport: rtFunc(func(*http.Request) (*http.Response, error) {
			body := fmt.Sprintf(`{"ip":%q}`, ipFor(ep, n.Add(1)))
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
		})}
	}
}

func gatewayPool(t *testing.T) *proxy.Pool {
	t.Helper()
	p, err := proxy.New(config.Proxy{Tiers: []config.ProxyTier{{Name: "residential", Kind: "gateway",
		Gateway: "http://gw.example:7777", Username: testUser, Password: testPassword,
		UsernameTemplate: "{user}-session-{session}", StickyTTL: 10 * time.Minute}}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func check(t *testing.T, r PreflightResult, tier, name string) PreflightCheck {
	t.Helper()
	for _, c := range r.Checks {
		if c.Tier == tier && c.Name == name {
			return c
		}
	}
	t.Fatalf("no %s/%s check in %+v", tier, name, r.Checks)
	return PreflightCheck{}
}

func TestPreflightExits(t *testing.T) {
	cases := []struct {
		name   string
		ipFor  func(proxy.Endpoint, int64) string
		status string
	}{
		{"sticky and rotating", func(ep proxy.Endpoint, _ int64) string { return "ip-" + ep.SessionID }, StatusPass},
		{"not sticky", func(_ proxy.Endpoint, n int64) string { return fmt.Sprintf("ip-%d", n) }, StatusFail},
		{"one exit for every session", func(proxy.Endpoint, int64) string { return "203.0.113.1" }, StatusWarn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Preflight(context.Background(), PreflightDeps{Proxies: gatewayPool(t), EchoClient: echoClients(tc.ipFor)},
				PreflightOptions{Tiers: []string{"residential"}, IPEchoURL: "https://echo.example", BaseURL: "https://www.facebook.com"})
			c := check(t, res, "residential", "exits")
			if c.Status != tc.status {
				t.Fatalf("exits: %+v", c)
			}
			if res.OK != (tc.status != StatusFail) {
				t.Fatalf("ok=%v with %s", res.OK, c.Status)
			}
			for _, c := range res.Checks {
				if strings.Contains(c.Detail, testPassword) || strings.Contains(c.Detail, testUser) {
					t.Fatalf("credential in check: %+v", c)
				}
			}
		})
	}
}

func TestPreflightConfigChecks(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "proxies.yaml")
	if err := os.WriteFile(cfg, []byte("fb: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cfg, 0o644); err != nil {
		t.Fatal(err)
	}
	res := Preflight(context.Background(), PreflightDeps{Proxies: gatewayPool(t)},
		PreflightOptions{Tiers: []string{"nope"}, ConfigPath: cfg, Seed: embeddedSeed(t), BaseURL: "https://www.facebook.com"})
	if c := check(t, res, "", "config"); c.Status != StatusWarn || !strings.Contains(c.Detail, "chmod 600") {
		t.Fatalf("config: %+v", c)
	}
	if c := check(t, res, "", "registry"); c.Status != StatusWarn || !strings.Contains(c.Detail, "placeholder") {
		t.Fatalf("registry: %+v", c)
	}
	if c := check(t, res, "nope", "tier"); c.Status != StatusFail || res.OK {
		t.Fatalf("unknown tier must fail: %+v", c)
	}
	_ = os.Chmod(cfg, 0o600)
	res = Preflight(context.Background(), PreflightDeps{Proxies: gatewayPool(t)},
		PreflightOptions{ConfigPath: cfg, Seed: embeddedSeed(t), BaseURL: "http://localhost:8090"})
	if c := check(t, res, "", "config"); c.Status != StatusPass {
		t.Fatalf("0600 config: %+v", c)
	}
	if c := check(t, res, "", "registry"); c.Status != StatusPass {
		t.Fatalf("placeholders are fine against fakefb: %+v", c)
	}
}

func TestPreflightFacebookLoad(t *testing.T) {
	env := fbtest.New(t, fbtest.Options{})
	direct, _ := proxy.New(config.Proxy{})
	res := Preflight(context.Background(), PreflightDeps{Proxies: direct, Pool: env.Pool, Loader: env.Loader},
		PreflightOptions{Tiers: []string{"direct"}, Page: env.World.Pages()[0].Vanity, BaseURL: env.HTTP.URL})
	if c := check(t, res, "direct", "facebook"); c.Status != StatusPass || !res.OK {
		t.Fatalf("facebook: %+v", c)
	}
	if c := check(t, res, "direct", "exits"); c.Status != StatusSkip {
		t.Fatalf("no echo url must skip: %+v", c)
	}
	res = Preflight(context.Background(), PreflightDeps{Proxies: direct, Pool: env.Pool, Loader: env.Loader},
		PreflightOptions{Tiers: []string{"direct"}, Page: "login", BaseURL: env.HTTP.URL})
	if c := check(t, res, "direct", "facebook"); c.Status != StatusFail {
		t.Fatalf("a login wall must fail: %+v", c)
	}
	var b bytes.Buffer
	b.WriteString(FormatPreflight(res))
	if !strings.Contains(b.String(), "preflight: FAILED") {
		t.Fatal(b.String())
	}
}
