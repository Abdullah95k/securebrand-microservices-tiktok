package sign_test

import (
	"context"
	"errors"
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
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// fakePage signs by appending its number; it fails when told to.
type fakePage struct {
	n      int
	url    string
	closed atomic.Bool
	fail   *atomic.Bool
	busy   *atomic.Int32
	most   *atomic.Int32
	hold   time.Duration
}

func (p *fakePage) Sign(ctx context.Context, raw string) (string, error) {
	if n := p.busy.Add(1); n > p.most.Load() {
		p.most.Store(n)
	}
	defer p.busy.Add(-1)
	if p.closed.Load() {
		return "", errors.New("closed")
	}
	if p.fail.Load() {
		return "", errors.New("page broke")
	}
	select {
	case <-time.After(p.hold):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return raw + "&X-Gnarly=page" + string(rune('0'+p.n)), nil
}

func (p *fakePage) Close() { p.closed.Store(true) }

type opener struct {
	mu    sync.Mutex
	pages []*fakePage
	fail  atomic.Bool
	busy  atomic.Int32
	most  atomic.Int32
	hold  time.Duration
	delay time.Duration // opening a page takes this long
}

func (o *opener) open(_ context.Context, _ *session.Identity, pageURL string) (sign.Page, error) {
	time.Sleep(o.delay)
	o.mu.Lock()
	defer o.mu.Unlock()
	p := &fakePage{n: len(o.pages), url: pageURL, fail: &o.fail, busy: &o.busy, most: &o.most, hold: o.hold}
	o.pages = append(o.pages, p)
	return p, nil
}

func identity(ua string) *session.Identity {
	base, _ := url.Parse("https://www.example.com")
	id := &session.Identity{Base: base}
	id.FP.UserAgent = ua
	return id
}

func cfg() config.Signer {
	return config.Signer{Mode: config.SignerBrowser, Pages: 2, PageMaxSigns: 3, PageMaxAge: time.Hour,
		SignTimeout: 2 * time.Second, PageTimeout: 2 * time.Second, MaxShare: 0.5}
}

// Pages are reused per user agent, opened on the call's referer, reopened
// after page_max_signs and page_max_age, and replaced after a failure;
// never more than tt.signer.pages are open or signing at once.
func TestPoolReusesAndRecyclesPages(t *testing.T) {
	ctx := context.Background()
	o := &opener{}
	p := sign.New(cfg(), o.open)
	now := time.Unix(1_800_000_000, 0)
	p.Now = func() time.Time { return now }
	a := identity("UA-a")
	ref := "https://www.example.com/@someone/video/1"
	for i := range 3 {
		got, err := p.SignURL(ctx, a, "comments", "/api/comment/list/?cursor="+string(rune('0'+i)), ref)
		if err != nil || !strings.HasSuffix(got, "page0") {
			t.Fatalf("sign %d: %q %v", i, got, err)
		}
	}
	if len(o.pages) != 1 || o.pages[0].url != ref {
		t.Fatalf("pages %d, first on %q", len(o.pages), o.pages[0].url)
	}
	// Spent after 3 signatures: a new page.
	if got, _ := p.SignURL(ctx, a, "comments", "/api/x?q=1", "https://elsewhere.example/x"); !strings.HasSuffix(got, "page1") || !o.pages[0].closed.Load() {
		t.Fatalf("after page_max_signs: %q, first closed %v", got, o.pages[0].closed.Load())
	}
	if o.pages[1].url != "https://www.example.com" {
		t.Fatalf("a referer on another site opens on the identity's site, got %q", o.pages[1].url)
	}
	// Another user agent gets its own page; a third closes the oldest idle.
	b, c := identity("UA-b"), identity("UA-c")
	if got, _ := p.SignURL(ctx, b, "comments", "/api/x?q=2", ref); !strings.HasSuffix(got, "page2") {
		t.Fatalf("UA-b: %q", got)
	}
	if got, _ := p.SignURL(ctx, c, "comments", "/api/x?q=3", ref); !strings.HasSuffix(got, "page3") || !o.pages[1].closed.Load() || p.Pages() != 2 {
		t.Fatalf("UA-c: %q, page1 closed %v, %d open", got, o.pages[1].closed.Load(), p.Pages())
	}
	// page_max_age.
	now = now.Add(2 * time.Hour)
	if got, _ := p.SignURL(ctx, c, "comments", "/api/x?q=4", ref); !strings.HasSuffix(got, "page4") {
		t.Fatalf("after page_max_age: %q", got)
	}
	// A failure closes the page; the next call opens another.
	o.fail.Store(true)
	if _, err := p.SignURL(ctx, c, "comments", "/api/x?q=5", ref); err == nil || !o.pages[4].closed.Load() {
		t.Fatalf("a failed sign: %v, page closed %v", err, o.pages[4].closed.Load())
	}
	o.fail.Store(false)
	if got, _ := p.SignURL(ctx, c, "comments", "/api/x?q=6", ref); !strings.HasSuffix(got, "page5") {
		t.Fatalf("after a failure: %q", got)
	}
	p.Close()
	if _, err := p.SignURL(ctx, c, "comments", "/api/x?q=7", ref); !errors.Is(err, sign.ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
	for _, pg := range o.pages {
		if !pg.closed.Load() {
			t.Fatalf("page %d left open after Close", pg.n)
		}
	}
}

// Many workers share the pages: at most tt.signer.pages sign at once, and
// a call that waits longer than the sign timeout is ErrBusy.
func TestPoolCapsConcurrency(t *testing.T) {
	ctx := context.Background()
	o := &opener{hold: 20 * time.Millisecond}
	c := cfg()
	c.PageMaxSigns = 1000
	p := sign.New(c, o.open)
	var wg sync.WaitGroup
	var errs atomic.Int32
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.SignURL(ctx, identity("UA"), "comments", "/api/x?q="+string(rune('a'+i)), ""); err != nil {
				errs.Add(1)
			}
		}()
	}
	wg.Wait()
	if errs.Load() != 0 || o.most.Load() > 2 || p.Pages() > 2 {
		t.Fatalf("errors %d, most at once %d, pages %d", errs.Load(), o.most.Load(), p.Pages())
	}
	// A page that takes long to open holds its slot: a second call waits
	// past the sign timeout and is ErrBusy.
	slow := &opener{delay: 500 * time.Millisecond}
	c.SignTimeout, c.Pages = 100*time.Millisecond, 1
	q := sign.New(c, slow.open)
	go func() { _, _ = q.SignURL(ctx, identity("UA"), "comments", "/api/x?q=1", "") }()
	time.Sleep(20 * time.Millisecond)
	if _, err := q.SignURL(ctx, identity("UA"), "comments", "/api/x?q=2", ""); !errors.Is(err, sign.ErrBusy) {
		t.Fatalf("a call waiting past the sign timeout: %v", err)
	}
	// A page slower than the page timeout is not waited for.
	c.PageTimeout = 50 * time.Millisecond
	r := sign.New(c, (&opener{delay: 300 * time.Millisecond}).open)
	if _, err := r.SignURL(ctx, identity("UA"), "comments", "/api/x?q=3", ""); err == nil || r.Pages() != 0 {
		t.Fatalf("a page past the page timeout: %v, %d pages", err, r.Pages())
	}
}

// End to end with Chromium against fakett, whose comment calls need a
// signature: the client's signer has a page on the post's account sign the
// call without sending it, and the identity's plain-HTTP request with the
// signed URL is answered; the unsigned one is refused. The post list goes
// out as built.
func TestBrowserSignerAgainstFakett(t *testing.T) {
	bin, err := browser.FindBinary("")
	if err != nil {
		t.Skip(err)
	}
	env := tttest.New(t, tttest.Options{Faults: fakett.Faults{RequireSignature: true,
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
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ref := env.HTTP.URL + "/@" + acc.Username
	req := graphql.Request{Query: "comments", Vars: map[string]any{"item_id": post.ID, "count": 20, "cursor": 0}, Referer: ref}

	resp, err := env.Client.Do(ctx, id, req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Class == detect.OK {
		t.Fatal("unsigned comments answered: fakett must refuse them")
	}
	pool := sign.New(cfg(), sign.BrowserOpener(bin, true, tttest.AcceptLanguage, 30*time.Second))
	defer pool.Close()
	env.Client.Signer = ttclient.SignerFor(config.SignerBrowser, pool)
	before := len(env.Requests("/api/comment/list/"))
	for cursor := 0; cursor < 2; cursor++ {
		req.Vars["cursor"] = cursor * 20
		resp, err = env.Client.Do(ctx, id, req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Class != detect.OK || len(resp.Parts) == 0 || !resp.Parts[0].Get("comments").IsArray() && cursor == 0 {
			t.Fatalf("signed comments, cursor %d: %v (%s)", cursor, resp.Class, resp.Reason)
		}
	}
	sent := env.Requests("/api/comment/list/")[before:]
	if len(sent) != 2 {
		t.Fatalf("%d plain-HTTP comment requests, want 2", len(sent))
	}
	for _, r := range sent {
		u, _ := url.Parse(r.URL)
		if u.Query().Get("X-Gnarly") == "" || u.Query().Get("device_id") == "" || !strings.HasPrefix(r.URL, env.HTTP.URL) {
			t.Fatalf("sent %s", r.URL)
		}
	}
	if pool.Pages() != 1 {
		t.Fatalf("%d pages for one user agent", pool.Pages())
	}
	// Unsigned variants are not signed.
	list, err := env.Client.Do(ctx, id, graphql.Request{Query: "creator_list", Vars: map[string]any{"sec_uid": acc.SecUID, "count": 15,
		"cursor": (env.World.Now.Unix() + 1) * 1000}, Referer: ref})
	if err != nil || list.Class != detect.OK {
		t.Fatalf("creator list: %v %v", list, err)
	}
	if u, _ := url.Parse(env.Requests("/api/creator/item_list/")[0].URL); u.Query().Get("X-Gnarly") != "" {
		t.Fatal("the creator list went through the signer")
	}
}
