package sign_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// exitOpener records the exit and fingerprint each page opened with.
type exitOpener struct {
	opener
	mu    sync.Mutex
	opens []*session.Identity
}

func (o *exitOpener) open(ctx context.Context, id *session.Identity, pageURL string) (sign.Page, error) {
	o.mu.Lock()
	o.opens = append(o.opens, id)
	o.mu.Unlock()
	return o.opener.open(ctx, id, pageURL)
}

func signerProxies(t *testing.T) *proxy.Pool {
	t.Helper()
	p, err := proxy.New(config.Proxy{Tiers: []config.ProxyTier{
		{Name: "isp", Kind: "static_list", Endpoints: []string{"http://u:secret@192.0.2.10:8000", "http://u:secret@192.0.2.11:8000"}},
		{Name: "res", Kind: "gateway", Gateway: "http://gw.example:7777", Username: "u", Password: "secret", Country: "us"},
	}, Routing: map[string][]string{"default": {"isp"}}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// signerService runs tks-signer's handler over pool, as the deployment does.
func signerService(t *testing.T, pool *sign.Pool, proxies *proxy.Pool, self bool) (*httptest.Server, *sign.Remote) {
	t.Helper()
	base, _ := url.Parse("https://www.example.com")
	srv := &sign.Server{Signer: pool, Proxies: proxies, Base: base}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	if self {
		srv.Self = hs.URL
	}
	return hs, sign.NewRemote(hs.URL+"/", 2*time.Second, 2*time.Second)
}

// The crawler's identity, as the caller holds it: its exit with the proxy
// credentials, which never go to tks-signer.
func exitIdentity(t *testing.T, proxies *proxy.Pool, tier, ua string) *session.Identity {
	t.Helper()
	ep, err := proxies.Next(tier)
	if err != nil {
		t.Fatal(err)
	}
	id := identity(ua)
	id.Endpoint = ep
	id.FP.SecChUa, id.FP.SecChUaPlatform, id.FP.SecChUaMobile = `"Chromium";v="152"`, `"Windows"`, "?0"
	return id
}

// Through tks-signer a call is signed in a page opened on the identity's
// own exit (rebuilt from tks-signer's proxy file: no credential is sent),
// with the identity's browser fingerprint, on the call's referer; pages are
// reused per user agent as in one process.
func TestRemoteSignsOnTheIdentitysExit(t *testing.T) {
	ctx := context.Background()
	o := &exitOpener{}
	c := cfg()
	c.PageMaxSigns = 100
	pool := sign.New(c, o.open)
	defer pool.Close()
	proxies := signerProxies(t)
	var bodies []string
	var mu sync.Mutex
	base, _ := url.Parse("https://www.example.com")
	inner := (&sign.Server{Signer: pool, Proxies: proxies, Base: base}).Handler()
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 4096)
		n, _ := r.Body.Read(b)
		mu.Lock()
		bodies = append(bodies, string(b[:n]))
		mu.Unlock()
		r.Body = readCloser{strings.NewReader(string(b[:n]))}
		inner.ServeHTTP(w, r)
	}))
	defer hs.Close()
	remote := sign.NewRemote(hs.URL, 2*time.Second, 2*time.Second)

	a := exitIdentity(t, proxies, "isp", "UA")
	ref := "https://www.example.com/@someone/video/1"
	got, err := remote.SignURL(ctx, a, "comments", "https://www.example.com/api/comment/list/?q=1", ref)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, "&X-Gnarly=page0") {
		t.Fatalf("signed %q", got)
	}
	if len(o.opens) != 1 {
		t.Fatalf("%d pages opened", len(o.opens))
	}
	op := o.opens[0]
	if op.Endpoint.URL != a.Endpoint.URL || op.Endpoint.Exit != a.Endpoint.Exit || op.FP != a.FP || o.pages[0].url != ref {
		t.Fatalf("page opened on %+v %+v at %q, want the identity's exit %s and fingerprint", op.Endpoint, op.FP, o.pages[0].url, a.Endpoint.Exit)
	}
	for _, b := range bodies {
		if strings.Contains(b, "secret") {
			t.Fatalf("a proxy credential went to tks-signer: %s", b)
		}
	}
	// The same user agent on another exit reuses the page (T27: a signed
	// URL is accepted from any exit), as the process's own pool does.
	b := exitIdentity(t, proxies, "isp", "UA")
	if b.Endpoint.Exit == a.Endpoint.Exit {
		t.Fatal("want another exit")
	}
	if _, err := remote.SignURL(ctx, b, "comments", "https://www.example.com/api/comment/list/?q=2", ref); err != nil || len(o.opens) != 1 {
		t.Fatalf("same user agent: %v, %d pages", err, len(o.opens))
	}
	// A gateway's identity: its page opens on the same sticky session.
	g := exitIdentity(t, proxies, "res", "UA2")
	if _, err := remote.SignURL(ctx, g, "comments", "https://www.example.com/api/comment/list/?q=3", ref); err != nil {
		t.Fatal(err)
	}
	if last := o.opens[len(o.opens)-1]; last.Endpoint.URL != g.Endpoint.URL || last.Endpoint.SessionID != g.Endpoint.SessionID {
		t.Fatalf("gateway page on %s, want %s", last.Endpoint.Redacted(), g.Endpoint.Redacted())
	}
	// An exit tks-signer's proxy file does not have: refused, no page.
	n := len(o.opens)
	x := identity("UA")
	x.Endpoint = proxy.Endpoint{Tier: "isp", Exit: "isp|198.51.100.1:8000"}
	if _, err := remote.SignURL(ctx, x, "comments", "https://www.example.com/api/comment/list/?q=4", ref); err == nil || !strings.Contains(err.Error(), "proxy file") || len(o.opens) != n {
		t.Fatalf("unknown exit: %v, %d pages", err, len(o.opens))
	}
}

type readCloser struct{ *strings.Reader }

func (readCloser) Close() error { return nil }

// A refused call is reported to the replica that signed, which notes the
// page for the client's retry; the retry, sent to the service, signs on
// another page. Pages whose calls are refused max_refusals times in a row
// are replaced, as in one process.
func TestRemoteRetryAvoidsTheRefusingPage(t *testing.T) {
	ctx := context.Background()
	o := &opener{}
	c := cfg()
	c.PageMaxSigns, c.Pages, c.MaxRefusals = 1000, 2, 2
	pool := sign.New(c, o.open)
	defer pool.Close()
	proxies := signerProxies(t)
	_, remote := signerService(t, pool, proxies, true)
	a := exitIdentity(t, proxies, "isp", "UA")
	signAs := func(ctx context.Context, q string) string {
		t.Helper()
		got, err := remote.SignURL(ctx, a, "comments", "https://www.example.com/api/comment/list/?q="+q, "")
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	o.hold = 50 * time.Millisecond
	var wg sync.WaitGroup
	for _, q := range []string{"a", "b"} {
		wg.Add(1)
		go func() { defer wg.Done(); signAs(ctx, q) }()
	}
	wg.Wait()
	o.hold = 0
	if len(o.pages) != 2 {
		t.Fatalf("%d pages", len(o.pages))
	}
	retry := &graphql.SignRetry{}
	rctx := graphql.WithSignRetry(ctx, retry)
	got := signAs(rctx, "d")
	first := got[strings.LastIndex(got, "page"):]
	// The caller sends it on its own host.
	sent := strings.Replace(got, "https://www.example.com", "https://edge.example", 1)
	remote.SignOutcome(rctx, sent, detect.RegistryStale)
	if _, ok := retry.Avoid.(sign.PageID); !ok {
		t.Fatalf("the refused call's page not noted: %#v", retry.Avoid)
	}
	again := signAs(rctx, "d")
	if again[strings.LastIndex(again, "page"):] == first {
		t.Fatalf("the retry signed on the refused page (%s)", first)
	}
	// Two refusals in a row: the page is replaced.
	for i := range 3 {
		got := signAs(ctx, "r"+string(rune('0'+i)))
		if got[strings.LastIndex(got, "page"):] == first {
			remote.SignOutcome(ctx, got, detect.RegistryStale)
		}
	}
	closed := 0
	for _, p := range o.pages {
		if p.closed.Load() {
			closed++
		}
	}
	if closed == 0 {
		t.Fatal("a page refused in a row was not replaced")
	}
}

// What a caller makes of tks-signer's failures: every page busy is
// ErrBusy (the call waits in a retry), a page that does not open is an
// error, and tks-signer down is an error that says so.
func TestRemoteFailures(t *testing.T) {
	ctx := context.Background()
	o := &opener{hold: 300 * time.Millisecond}
	c := cfg()
	c.Pages, c.SignTimeout = 1, 100*time.Millisecond
	pool := sign.New(c, o.open)
	defer pool.Close()
	proxies := signerProxies(t)
	hs, remote := signerService(t, pool, proxies, false)
	a := exitIdentity(t, proxies, "isp", "UA")
	var busy atomic.Int32
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := remote.SignURL(ctx, a, "comments", "https://www.example.com/api/x?q=1", ""); errors.Is(err, sign.ErrBusy) {
				busy.Add(1)
			}
		}()
	}
	wg.Wait()
	if busy.Load() == 0 {
		t.Fatal("no call saw every page busy")
	}
	o.hold = 0
	o.fail.Store(true)
	if _, err := remote.SignURL(ctx, a, "comments", "https://www.example.com/api/x?q=2", ""); err == nil || errors.Is(err, sign.ErrBusy) {
		t.Fatalf("a page that fails: %v", err)
	}
	hs.Close()
	if _, err := remote.SignURL(ctx, a, "comments", "https://www.example.com/api/x?q=3", ""); err == nil || !strings.Contains(err.Error(), "tks-signer") {
		t.Fatalf("tks-signer down: %v", err)
	}
	// An outcome it cannot report changes nothing.
	r := &graphql.SignRetry{}
	remote.SignOutcome(graphql.WithSignRetry(ctx, r), "https://www.example.com/api/x?q=1", detect.RegistryStale)
	if r.Avoid != nil {
		t.Fatal("an unreported outcome noted a page")
	}
}

// The guard: a page whose browser passes kill_page_mb while it signs is
// closed by the sweep, its call fails, and the next call opens a fresh one.
func TestPoolKillsARunawayPageWhileItSigns(t *testing.T) {
	ctx := context.Background()
	o := &opener{}
	c := cfg()
	c.Pages, c.PageMaxSigns, c.MaxPageMB, c.KillPageMB, c.SignTimeout = 1, 1000, 800, 1200, 5*time.Second
	p := sign.New(c, o.open)
	defer p.Close()
	a := identity("UA")
	// Over max_page_mb but under the kill limit while signing: left alone.
	o.hold = 2 * time.Second
	errc := make(chan error, 1)
	go func() {
		_, err := p.SignURL(ctx, a, "comments", "/api/x?q=2", "")
		errc <- err
	}()
	pg := o.page(t, 0)
	time.Sleep(50 * time.Millisecond)
	pg.mb.Store(1000)
	p.Sweep()
	if pg.closed.Load() {
		t.Fatal("a page under kill_page_mb was closed mid-call")
	}
	pg.mb.Store(1500)
	start := time.Now()
	p.Sweep()
	err := <-errc
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("the runaway's call: %v after %s", err, time.Since(start))
	}
	if !pg.closed.Load() || p.Pages() != 0 {
		t.Fatalf("runaway page: closed %v, %d open", pg.closed.Load(), p.Pages())
	}
	o.hold = 0
	if _, err := p.SignURL(ctx, a, "comments", "/api/x?q=3", ""); err != nil || len(o.pages) != 2 || o.page(t, 1).closed.Load() {
		t.Fatalf("after the kill: %v, %d pages", err, len(o.pages))
	}
}

// End to end with Chromium against fakett, whose comment calls need a
// signature: the client's signer is tks-signer's handler (a real page,
// opened on the identity's exit), the identity sends the signed URL by
// plain HTTP and is answered.
func TestRemoteBrowserSignerAgainstFakett(t *testing.T) {
	bin, err := browser.FindBinary("")
	if err != nil {
		t.Skip(err)
	}
	env := tttest.New(t, tttest.Options{Faults: fakett.Faults{RequireSignature: true, SignerRefuseEvery: 3,
		SignedPaths: []string{"/api/comment/list/", "/api/comment/list/reply/"}}})
	acc := env.World.Accounts()[0]
	var post *fakett.Post
	for _, p := range acc.Posts {
		if len(p.Comments) > 0 {
			post = p
			break
		}
	}
	id := env.Identity(t, acc.Username)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	bc := cfg()
	bc.Pages, bc.PageTimeout, bc.PageMaxSigns, bc.MaxRefusals, bc.SignTimeout = 1, 30*time.Second, 1000, 2, 30*time.Second
	bc.KillPageMB = 1200
	pool := sign.New(bc, sign.BrowserOpener(sign.BrowserConfig{Binary: bin, NoSandbox: true, AcceptLanguage: tttest.AcceptLanguage,
		CallTimeout: 30 * time.Second, Freeze: true, Lite: true}))
	defer pool.Close()
	proxies, err := proxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	base, _ := url.Parse(env.HTTP.URL)
	srv := &sign.Server{Signer: pool, Proxies: proxies, Base: base}
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()
	srv.Self = hs.URL
	env.Client.Signer = ttclient.SignerFor(config.SignerRemote, sign.NewRemote(hs.URL, bc.SignTimeout, bc.PageTimeout))
	ref := env.HTTP.URL + "/@" + acc.Username
	answered, retried := 0, 0
	for i := range 6 {
		resp, err := env.Client.Do(ctx, id, graphql.Request{Query: "comments", Vars: map[string]any{"item_id": post.ID, "count": 20, "cursor": i}, Referer: ref})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Class == detect.OK {
			answered++
		}
		if resp.SignRetried {
			retried++
		}
	}
	if answered != 6 || retried == 0 {
		t.Fatalf("%d of 6 answered, %d retried on another page", answered, retried)
	}
	for _, r := range env.Requests("/api/comment/list/") {
		if !strings.HasPrefix(r.URL, env.HTTP.URL) {
			t.Fatalf("sent to %s", r.URL)
		}
	}
	if pool.Pages() > 1 {
		t.Fatalf("%d pages, cap 1", pool.Pages())
	}
	t.Logf("6 of 6 answered through tks-signer, %d after a retry on another page", retried)
}

// A replica's address is taken only as http:// and a pod IP and port:
// any other "by" in an answer is ignored, and the outcome goes to the
// service's own address.
func TestRemoteReportsOnlyToReplicaAddresses(t *testing.T) {
	var outcomes atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("outcome sent to an address that is not a replica's: %s", r.URL)
	}))
	defer other.Close()
	otherByName := strings.Replace(other.URL, "127.0.0.1", "localhost", 1)
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case sign.SignPath:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"url":"https://www.example.com/api/x?q=1&X-Gnarly=s","by":"` + otherByName + `"}`))
		case sign.OutcomePath:
			outcomes.Add(1)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer hs.Close()
	remote := sign.NewRemote(hs.URL, time.Second, time.Second)
	got, err := remote.SignURL(context.Background(), identity("UA"), "comments", "https://www.example.com/api/x?q=1", "")
	if err != nil {
		t.Fatal(err)
	}
	remote.SignOutcome(context.Background(), got, detect.OK)
	if outcomes.Load() != 1 {
		t.Fatalf("%d outcomes reached the service", outcomes.Load())
	}
}
