package browser

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// signingPage is a page whose script rewrites the page's own fetch calls,
// the way TikTok's script is reported to add its signatures: the Session
// must return both the answer and the URL actually sent.
const signingPage = `<!doctype html><html><head><script>
const orig = window.fetch;
window.fetch = (input, init) => {
  const u = new URL(input, location.href);
  if (u.pathname.startsWith("/api/")) { u.searchParams.set("X-Sig", "page-" + u.searchParams.get("q")); }
  return orig(u.toString(), init);
};
document.cookie = "tok=abc; path=/";
</script></head><body>signing page</body></html>`

func TestSessionFetchesFromThePage(t *testing.T) {
	bin := chromium(t)
	var sawSig, sawCookie string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/"):
			sawSig = r.URL.Query().Get("X-Sig")
			if c, err := r.Cookie("tok"); err == nil {
				sawCookie = c.Value
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"statusCode":0,"q":"` + r.URL.Query().Get("q") + `"}`))
		default:
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(signingPage))
		}
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := OpenSession(ctx, bin, Options{URL: srv.URL + "/@someone", NoSandbox: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Status != 200 {
		t.Fatalf("page status %d", s.Status)
	}
	for _, q := range []string{"one", "two"} {
		f, err := s.Fetch(ctx, "/api/post/item_list/?q="+q)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Q string `json:"q"`
		}
		if f.Status != 200 || json.Unmarshal(f.Body, &body) != nil || body.Q != q {
			t.Fatalf("%s: status %d body %s", q, f.Status, f.Body)
		}
		sent, err := url.Parse(f.SentURL)
		if err != nil || sent.Query().Get("X-Sig") != "page-"+q {
			t.Fatalf("%s: sent URL %q does not carry the page's signature", q, f.SentURL)
		}
		if sawSig != "page-"+q || sawCookie != "abc" {
			t.Fatalf("%s: the server saw sig %q cookie %q", q, sawSig, sawCookie)
		}
	}
	cookies, err := s.Cookies(ctx)
	if err != nil || len(cookies) == 0 || cookies[0].Name != "tok" {
		t.Fatalf("cookies %v %v", cookies, err)
	}
	if _, err := s.Evaluate(ctx, "throw new Error('boom')"); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("a script error must be returned: %v", err)
	}
	if s.Bytes() <= 0 {
		t.Fatal("no wire bytes counted")
	}
	s.Close()
	s.Close() // twice is fine
	if _, err := s.Fetch(ctx, "/api/x"); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("fetch after close: %v", err)
	}
}

// TestSessionCapturesThePagesOwnCalls loads a page whose script calls the
// API on its own, then follows a link to a second page that does too: the
// Session must report each page's own calls with their answers.
func TestSessionCapturesThePagesOwnCalls(t *testing.T) {
	bin := chromium(t)
	page := func(call string) string {
		return `<!doctype html><html><head><script>fetch("/api/` + call + `/?n=1").then(r => r.text());` +
			`window.addEventListener("scroll", () => fetch("/api/` + call + `/?n=2"));</script></head>` +
			`<body style="height:5000px">` + call + `</body></html>`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"path":"` + r.URL.Path + `","n":"` + r.URL.Query().Get("n") + `"}`))
		case r.URL.Path == "/video":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(page("comment/list")))
		default:
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(page("post/item_list")))
		}
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := OpenSession(ctx, bin, Options{URL: srv.URL + "/@someone", NoSandbox: true, HideAutomation: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	own := s.Captured(ctx, 0)
	if len(own) != 1 || !strings.Contains(own[0].URL, "/api/post/item_list/?n=1") || !strings.Contains(string(own[0].Body), `"n":"1"`) {
		t.Fatalf("the page's own call: %+v", own)
	}
	v, err := s.Evaluate(ctx, "navigator.webdriver === true")
	if err != nil || string(v) != "false" {
		t.Fatalf("navigator.webdriver with HideAutomation: %s %v", v, err)
	}
	mark := s.Mark()
	if err := s.Scroll(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Captured(ctx, mark); len(got) != 1 || !strings.Contains(got[0].URL, "n=2") {
		t.Fatalf("after scrolling: %+v", got)
	}
	mark = s.Mark()
	if err := s.Navigate(ctx, srv.URL+"/video"); err != nil {
		t.Fatal(err)
	}
	if s.Status != 200 || !strings.HasSuffix(s.URL, "/video") {
		t.Fatalf("after navigating: %s %d", s.URL, s.Status)
	}
	got := s.Captured(ctx, mark)
	if len(got) != 1 || !strings.Contains(got[0].URL, "/api/comment/list/?n=1") || !strings.Contains(string(got[0].Body), "comment/list") {
		t.Fatalf("the second page's own call: %+v", got)
	}
	// Fetch still works on the new page.
	if f, err := s.Fetch(ctx, "/api/comment/list/?n=9"); err != nil || f.Status != 200 {
		t.Fatalf("fetch after navigating: %v %v", f, err)
	}
}

// A call the page never finishes, and a page that never loads, fail after
// the session's CallTimeout instead of hanging.
func TestSessionCallsTimeOut(t *testing.T) {
	bin := chromium(t)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/hang", "/never":
			<-release
		default:
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<!doctype html><html><body>page</body></html>`))
		}
	}))
	defer srv.Close()
	defer close(release) // runs before srv.Close, so the handlers return
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := OpenSession(ctx, bin, Options{URL: srv.URL + "/@someone", NoSandbox: true, CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now()
	if _, err := s.Fetch(ctx, "/api/hang"); err == nil || !strings.Contains(err.Error(), "no answer") {
		t.Fatalf("a hung fetch: %v", err)
	}
	if err := s.Navigate(ctx, srv.URL+"/never"); err == nil {
		t.Fatal("a page that never loads must fail")
	}
	if took := time.Since(start); took > 15*time.Second {
		t.Fatalf("timeouts took %s", took)
	}
}

// The sent URL is the request carrying every parameter asked for, not
// another call to the same path.
func TestSameCall(t *testing.T) {
	asked, _ := url.Parse("https://x/api/comment/list/?aweme_id=1&cursor=0")
	for raw, want := range map[string]bool{
		"https://x/api/comment/list/?aweme_id=1&cursor=0&X-Gnarly=g&msToken=m": true,
		"https://x/api/comment/list/?aweme_id=1&cursor=20&X-Gnarly=g":          false,
		"https://x/api/comment/list/?cursor=0":                                 false,
		"https://x/api/post/item_list/?aweme_id=1&cursor=0":                    false,
	} {
		sent, _ := url.Parse(raw)
		if got := SameCall(asked, sent); got != want {
			t.Errorf("%s: %v, want %v", raw, got, want)
		}
	}
}
