package sign_test

import (
	"context"
	"errors"
	"net/url"
	"strconv"
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
	mb     atomic.Int64
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

// MemoryMB is the page's browser memory, as the opener sets it.
func (p *fakePage) MemoryMB() float64 { return float64(p.mb.Load()) }

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

// PageQuery puts the page's values in place of the identity's and leaves
// out msToken and earlier signatures, which the page's script adds.
func TestPageQuery(t *testing.T) {
	raw := "https://www.tiktok.com/api/comment/list/?aid=1988&aweme_id=7&cursor=20&device_id=111&msToken=abc&odinId=1&tz_name=Asia%2FBaghdad&X-Gnarly=old"
	got, _ := url.Parse(sign.PageQuery(raw, map[string]string{"device_id": "222", "odinId": "9", "tz_name": "UTC", "screen_width": "800"}))
	q := got.Query()
	for k, want := range map[string]string{"device_id": "222", "odinId": "9", "tz_name": "UTC", "screen_width": "800", "aweme_id": "7", "cursor": "20", "aid": "1988", "msToken": "", "X-Gnarly": ""} {
		if q.Get(k) != want {
			t.Errorf("%s = %q, want %q (%s)", k, q.Get(k), want, got)
		}
	}
	if got.Path != "/api/comment/list/" || got.Host != "www.tiktok.com" {
		t.Fatal(got)
	}
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
	bc := cfg()
	bc.PageTimeout = 30 * time.Second // a real page and its values, on a busy runner
	pool := sign.New(bc, sign.BrowserOpener(sign.BrowserConfig{Binary: bin, NoSandbox: true, AcceptLanguage: tttest.AcceptLanguage, CallTimeout: 30 * time.Second}))
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
	// The page signs with its own device id and environment, not the
	// identity's: fakett, like TikTok (Phase 0, 2026-11-02), refuses a
	// signature over another device's values.
	for _, r := range sent {
		u, _ := url.Parse(r.URL)
		q := u.Query()
		if q.Get("X-Gnarly") == "" || q.Get("device_id") == "" || !strings.HasPrefix(r.URL, env.HTTP.URL) {
			t.Fatalf("sent %s", r.URL)
		}
		if q.Get("device_id") == id.Tokens()["device_id"] || q.Get("tz_name") == "Asia/Baghdad" || q.Get("msToken") == id.Doer.Cookie(id.Base, "msToken") {
			t.Fatalf("signed with the identity's values, not the page's: %s", r.URL)
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

// The safety nets of 2026-11-03: a page unused for idle_close is closed by
// the sweep (and never handed out), a page over max_page_mb is closed after
// its call or by the sweep, and the next call opens a fresh one; with
// open_on profile a page opens on the account's profile page, not the
// post's.
func TestPoolClosesIdleAndBigPages(t *testing.T) {
	ctx := context.Background()
	o := &opener{}
	c := cfg()
	c.PageMaxSigns, c.IdleClose, c.MaxPageMB, c.OpenOn = 1000, 2*time.Minute, 800, config.SignerOpenOnProfile
	p := sign.New(c, o.open)
	defer p.Close()
	now := time.Unix(1_800_000_000, 0)
	var mu sync.Mutex
	p.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	a := identity("UA")
	ref := "https://www.example.com/@someone/video/1?lang=en"
	if _, err := p.SignURL(ctx, a, "comments", "/api/x?q=1", ref); err != nil {
		t.Fatal(err)
	}
	if o.pages[0].url != "https://www.example.com/@someone" {
		t.Fatalf("open_on profile opened %q", o.pages[0].url)
	}
	// Idle under idle_close: kept and reused.
	advance(time.Minute)
	p.Sweep()
	if _, err := p.SignURL(ctx, a, "comments", "/api/x?q=2", ref); err != nil || len(o.pages) != 1 || o.pages[0].closed.Load() {
		t.Fatalf("reuse: %v, %d pages", err, len(o.pages))
	}
	// Idle past idle_close: the sweep closes it.
	advance(2 * time.Minute)
	p.Sweep()
	if !o.pages[0].closed.Load() || p.Pages() != 0 {
		t.Fatalf("idle page: closed %v, %d open", o.pages[0].closed.Load(), p.Pages())
	}
	if _, err := p.SignURL(ctx, a, "comments", "/api/x?q=3", ref); err != nil || len(o.pages) != 2 {
		t.Fatalf("after the sweep: %v, %d pages", err, len(o.pages))
	}
	// Over max_page_mb: closed as its call ends.
	o.pages[1].mb.Store(900)
	if _, err := p.SignURL(ctx, a, "comments", "/api/x?q=4", ref); err != nil || !o.pages[1].closed.Load() || p.Pages() != 0 {
		t.Fatalf("big page after its call: %v, closed %v, %d open", err, o.pages[1].closed.Load(), p.Pages())
	}
	// Growing while idle: the sweep closes it.
	if _, err := p.SignURL(ctx, a, "comments", "/api/x?q=5", ref); err != nil {
		t.Fatal(err)
	}
	o.pages[2].mb.Store(1200)
	p.Sweep()
	if !o.pages[2].closed.Load() || p.Pages() != 0 {
		t.Fatalf("big idle page: closed %v, %d open", o.pages[2].closed.Load(), p.Pages())
	}
	// The sweeper runs on its own.
	sign.SweepEvery = 10 * time.Millisecond
	defer func() { sign.SweepEvery = 10 * time.Second }()
	q := sign.New(c, o.open)
	defer q.Close()
	if _, err := q.SignURL(ctx, a, "comments", "/api/x?q=6", ref); err != nil {
		t.Fatal(err)
	}
	o.pages[3].mb.Store(5000)
	deadline := time.Now().Add(2 * time.Second)
	for q.Pages() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if q.Pages() != 0 || !o.pages[3].closed.Load() {
		t.Fatal("the sweeper did not close a page over max_page_mb")
	}
}

// The crawler's signer with the runaway's cures on (tt.signer.freeze, lite,
// open_on profile) against fakett: the page opens on the account's profile
// page, sleeps frozen between calls, and every comment page it signs is
// answered.
func TestBrowserSignerCuredAgainstFakett(t *testing.T) {
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
	bc := cfg()
	bc.PageTimeout, bc.PageMaxSigns, bc.OpenOn, bc.IdleClose, bc.MaxPageMB = 30*time.Second, 100, config.SignerOpenOnProfile, time.Minute, 800
	var opened []string
	open := sign.BrowserOpener(sign.BrowserConfig{Binary: bin, NoSandbox: true, AcceptLanguage: tttest.AcceptLanguage,
		CallTimeout: 30 * time.Second, Freeze: true, Lite: true})
	pool := sign.New(bc, func(ctx context.Context, id *session.Identity, pageURL string) (sign.Page, error) {
		opened = append(opened, pageURL)
		return open(ctx, id, pageURL)
	})
	defer pool.Close()
	env.Client.Signer = ttclient.SignerFor(config.SignerBrowser, pool)
	ref := env.HTTP.URL + "/@" + acc.Username + "/video/" + strconv.FormatInt(post.ID, 10)
	for cursor := range 3 {
		resp, err := env.Client.Do(ctx, id, graphql.Request{Query: "comments", Vars: map[string]any{"item_id": post.ID, "count": 20, "cursor": cursor * 20}, Referer: ref})
		if err != nil || resp.Class != detect.OK {
			t.Fatalf("cursor %d: %v %v", cursor, resp, err)
		}
		time.Sleep(300 * time.Millisecond) // frozen meanwhile
	}
	if len(opened) != 1 || opened[0] != env.HTTP.URL+"/@"+acc.Username || pool.Pages() != 1 {
		t.Fatalf("pages opened on %v, %d open", opened, pool.Pages())
	}
}

// A page whose signed calls are refused max_refusals times in a row is
// replaced; an answered call resets the count; a report for a call the pool
// did not sign is ignored.
func TestPoolReplacesRefusedPages(t *testing.T) {
	ctx := context.Background()
	o := &opener{}
	c := cfg()
	c.PageMaxSigns, c.MaxRefusals, c.Pages = 1000, 2, 1
	p := sign.New(c, o.open)
	defer p.Close()
	a := identity("UA")
	sign1 := func(q string) string {
		t.Helper()
		got, err := p.SignURL(ctx, a, "comments", "https://page.example/api/comment/list/?q="+q, "")
		if err != nil {
			t.Fatal(err)
		}
		// The identity sends it to its own host.
		return strings.Replace(got, "https://page.example", "https://www.example.com", 1)
	}
	p.SignOutcome(ctx, "https://www.example.com/api/comment/list/?q=unknown", detect.RegistryStale)
	p.SignOutcome(ctx, sign1("1"), detect.RegistryStale)
	p.SignOutcome(ctx, sign1("2"), detect.OK) // resets
	p.SignOutcome(ctx, sign1("3"), detect.RegistryStale)
	if len(o.pages) != 1 || o.pages[0].closed.Load() {
		t.Fatalf("a page refused once in a row was replaced: %d pages", len(o.pages))
	}
	p.SignOutcome(ctx, sign1("4"), detect.RegistryStale) // twice in a row
	p.SignOutcome(ctx, sign1("5"), detect.OK)
	if len(o.pages) != 2 || !o.pages[0].closed.Load() || o.pages[1].closed.Load() {
		t.Fatalf("after two refusals in a row: %d pages, first closed %v", len(o.pages), o.pages[0].closed.Load())
	}
	// Other classes (the identity's trouble, not the page's) do not count.
	p.SignOutcome(ctx, sign1("6"), detect.RateLimited)
	p.SignOutcome(ctx, sign1("7"), detect.Checkpoint)
	if _, err := p.SignURL(ctx, a, "comments", "https://page.example/api/comment/list/?q=8", ""); err != nil || len(o.pages) != 2 {
		t.Fatalf("rate limits replaced the page: %d pages", len(o.pages))
	}
}

// End to end with Chromium against a fakett whose signer pages go bad
// after 3 signed calls, as in C4: with max_refusals 2 the client's reports
// replace each bad page, and most calls are answered; without, every call
// after the third is refused.
func TestBrowserSignerReplacesPagesGoneBad(t *testing.T) {
	bin, err := browser.FindBinary("")
	if err != nil {
		t.Skip(err)
	}
	for _, maxRefusals := range []int{0, 2} {
		env := tttest.New(t, tttest.Options{Faults: fakett.Faults{RequireSignature: true, SignerStaleAfter: 3,
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
		bc := cfg()
		bc.Pages, bc.PageTimeout, bc.PageMaxSigns, bc.MaxRefusals = 1, 30*time.Second, 1000, maxRefusals
		var opened atomic.Int32
		open := sign.BrowserOpener(sign.BrowserConfig{Binary: bin, NoSandbox: true, AcceptLanguage: tttest.AcceptLanguage,
			CallTimeout: 30 * time.Second, Freeze: true, Lite: true})
		pool := sign.New(bc, func(ctx context.Context, id *session.Identity, pageURL string) (sign.Page, error) {
			opened.Add(1)
			return open(ctx, id, pageURL)
		})
		env.Client.Signer = ttclient.SignerFor(config.SignerBrowser, pool)
		ref := env.HTTP.URL + "/@" + acc.Username
		answered := 0
		for i := range 12 {
			resp, err := env.Client.Do(ctx, id, graphql.Request{Query: "comments", Vars: map[string]any{"item_id": post.ID, "count": 20, "cursor": i}, Referer: ref})
			if err != nil {
				t.Fatal(err)
			}
			if resp.Class == detect.OK {
				answered++
			}
		}
		pool.Close()
		cancel()
		switch {
		case maxRefusals == 0 && (answered != 3 || opened.Load() != 1):
			t.Fatalf("without replacement: %d of 12 answered, %d pages", answered, opened.Load())
		case maxRefusals == 2 && (answered < 7 || opened.Load() < 3):
			t.Fatalf("with max_refusals 2: %d of 12 answered, %d pages", answered, opened.Load())
		}
		t.Logf("max_refusals %d: %d of 12 answered, %d pages opened", maxRefusals, answered, opened.Load())
	}
}
