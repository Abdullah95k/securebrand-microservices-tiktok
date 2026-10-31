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
