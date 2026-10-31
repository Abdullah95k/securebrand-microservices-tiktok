package browser

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	scrapeproxy "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

func chromium(t *testing.T) string {
	t.Helper()
	bin, err := FindBinary("")
	if err != nil {
		t.Skip(err)
	}
	return bin
}

func render(t *testing.T, opts Options) (*Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return Render(ctx, chromium(t), opts)
}

// proxy is a forward proxy requiring Basic credentials.
func proxy(t *testing.T, user, pass string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
	var served atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != want {
			w.Header().Set("Proxy-Authenticate", `Basic realm="fbs"`)
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		served.Add(1)
		out, _ := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
		for k, v := range r.Header {
			if !strings.HasPrefix(k, "Proxy-") {
				out.Header[k] = v
			}
		}
		resp, err := http.DefaultTransport.RoundTrip(out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	return srv, &served
}

func TestRenderThroughAnAuthenticatedProxy(t *testing.T) {
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html><body>a page</body></html>")
	}))
	t.Cleanup(page.Close)
	px, served := proxy(t, "user-session-1", "s3cret")
	pu := strings.Replace(px.URL, "http://", "http://user-session-1:s3cret@", 1)
	res, err := render(t, Options{URL: page.URL + "/brand", Proxy: mustURL(t, pu)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != 200 || served.Load() == 0 {
		t.Fatalf("status %d, proxied %d", res.Status, served.Load())
	}
	// Wrong credentials: the render fails instead of looping on the
	// challenge.
	bad := strings.Replace(px.URL, "http://", "http://user-session-1:wrong@", 1)
	if _, err := render(t, Options{URL: page.URL + "/brand", Proxy: mustURL(t, bad)}); err == nil {
		t.Fatal("render through a proxy refusing the credentials succeeded")
	}
}

// testIdentity is a fresh identity on https://www.example.com.
func testIdentity(t *testing.T) *session.Identity {
	t.Helper()
	proxies, err := scrapeproxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: "https://www.example.com", Profile: "chrome_146",
		IdentityRPS: 1000, IdentityBurst: 1000, MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 1}, proxies, fps,
		func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	id, err := pool.Acquire(context.Background(), "direct")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// testHooks read a "token" value and treat /login as a wall.
var testHooks = session.Hooks{
	Extract: func(html []byte) (session.Tokens, bool) {
		m := regexp.MustCompile(`"token":"(\w+)"`).FindSubmatch(html)
		if m == nil {
			return nil, false
		}
		return session.Tokens{"lsd": string(m[1])}, true
	},
	IsWall: func(path string) bool { return strings.HasPrefix(path, "/login") },
}

func TestBootstrapperShareCap(t *testing.T) {
	var renders atomic.Int32
	b := &Bootstrapper{MaxShare: 0.1, Timeout: time.Second, sem: make(chan struct{}, 1), credit: 1, burst: 1, Hooks: testHooks,
		render: func(context.Context, string, Options) (*Result, error) {
			renders.Add(1)
			return &Result{HTML: []byte("<html></html>")}, nil
		}}
	id := testIdentity(t)
	ctx := context.Background()
	_, _ = b.Bootstrap(ctx, id, "http://x/")
	if _, err := b.Bootstrap(ctx, id, "http://x/"); !errors.Is(err, ErrCapped) {
		t.Fatalf("second use without loads in between: %v", err)
	}
	for range 9 {
		b.Observe()
	}
	if _, err := b.Bootstrap(ctx, id, "http://x/"); !errors.Is(err, ErrCapped) {
		t.Fatalf("9 loads at 10%% are not a whole browser use: %v", err)
	}
	b.Observe()
	_, _ = b.Bootstrap(ctx, id, "http://x/")
	if renders.Load() != 2 {
		t.Fatalf("renders %d, want 2", renders.Load())
	}
	// The credit does not pile up beyond the burst.
	for range 1000 {
		b.Observe()
	}
	_, _ = b.Bootstrap(ctx, id, "http://x/")
	if _, err := b.Bootstrap(ctx, id, "http://x/"); !errors.Is(err, ErrCapped) {
		t.Fatalf("credit beyond the burst: %v", err)
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestHintsFrom(t *testing.T) {
	h := HintsFrom(`"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"`, `"Windows"`, "?0")
	if h == nil || h.Platform != "Windows" || h.Mobile || len(h.Brands) != 3 || h.Brands[2] != (Brand{"Google Chrome", "146"}) {
		t.Fatalf("%+v", h)
	}
	if navigatorPlatform(h.Platform) != "Win32" || HintsFrom("", "", "") != nil {
		t.Fatal("platform")
	}
}

func TestSocksProxyWithCredentialsIsRefused(t *testing.T) {
	_, err := Render(context.Background(), "/nonexistent-browser", Options{URL: "http://x/", Proxy: mustURL(t, "socks5://u:p@127.0.0.1:1080")})
	if err == nil || !strings.Contains(err.Error(), "SOCKS") {
		t.Fatalf("want a SOCKS error before launching, got %v", err)
	}
}

// A render that lands on a login wall is not a bootstrap, though the wall
// carries an LSD token.
func TestBootstrapRefusesWalls(t *testing.T) {
	b := &Bootstrapper{MaxShare: 1, Timeout: time.Second, sem: make(chan struct{}, 1), credit: 1, burst: 1, Rules: detect.DefaultRules(),
		Hooks: testHooks, render: func(context.Context, string, Options) (*Result, error) {
			return &Result{URL: "https://www.example.com/login/?next=x", Status: 200,
				HTML: []byte(`<html><script>["LSD",[],{"token":"AVq1"},323]</script></html>`)}, nil
		}}
	id := testIdentity(t)
	pl, err := b.Bootstrap(context.Background(), id, "https://www.example.com/brand")
	if err == nil || pl == nil || pl.Class != detect.LoginWall || id.Bootstrapped() {
		t.Fatalf("wall taken for a bootstrap: %v %+v bootstrapped=%v", err, pl, id.Bootstrapped())
	}
}

// Media is refused, a page that keeps polling still settles, the wire
// bytes are counted, and no Chromium process outlives the render.
func TestRenderBlocksMediaAndSettles(t *testing.T) {
	bin := chromium(t)
	// The render's profile directory goes under a directory of this test's
	// own, so the leftover count cannot see renders of other packages that
	// go test runs in parallel (internal/fb/client renders too).
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	var images atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/x.png":
			images.Add(1)
			w.Header().Set("Content-Type", "image/png")
		case "/beacon":
			w.WriteHeader(http.StatusNoContent)
		default:
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, `<html><body><img src="/x.png"><script>setInterval(function(){fetch("/beacon")},50)</script>`+
				strings.Repeat("<p>filler</p>", 500)+`</body></html>`)
		}
	}))
	defer srv.Close()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := Render(ctx, bin, Options{URL: srv.URL + "/", BlockMedia: true, MaxSettle: 2 * time.Second, Idle: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if images.Load() != 0 || res.Bytes < 5000 || time.Since(start) > 30*time.Second {
		t.Fatalf("images fetched %d, bytes %d, took %s", images.Load(), res.Bytes, time.Since(start))
	}
	if n := browserProcesses(t, tmp); n != 0 {
		t.Fatalf("%d Chromium processes left after the render", n)
	}
}

// browserProcesses counts live processes started with a browser profile
// under tmp.
func browserProcesses(t *testing.T, tmp string) int {
	t.Helper()
	time.Sleep(200 * time.Millisecond)
	entries, _ := os.ReadDir("/proc")
	n := 0
	for _, e := range entries {
		b, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err == nil && strings.Contains(string(b), "--user-data-dir="+filepath.Join(tmp, "igs-browser-")) {
			stat, _ := os.ReadFile("/proc/" + e.Name() + "/stat")
			if !strings.Contains(string(stat), ") Z ") { // zombies are reaped by the test's parent
				n++
			}
		}
	}
	return n
}
