package browser

import (
	"bufio"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// upstreamProxy is an exit that wants credentials: it answers CONNECT and
// absolute-URI requests, and counts the refused ones.
func upstreamProxy(t *testing.T, user, pass string) (*url.URL, *atomic.Int64) {
	t.Helper()
	var refused atomic.Int64
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != want {
			refused.Add(1)
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		if r.Method != http.MethodConnect {
			out := r.Clone(r.Context())
			out.RequestURI = ""
			out.Header.Del("Proxy-Authorization")
			resp, err := http.DefaultTransport.RoundTrip(out)
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			return
		}
		up, err := net.Dial("tcp", r.Host)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		c, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
		go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
		_, _ = io.Copy(c, up)
		_ = c.Close()
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	u.User = url.UserPassword(user, pass)
	return u, &refused
}

func TestForwarderAddsTheExitsCredentials(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("hello")) }))
	defer site.Close()
	tls := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("secure hello")) }))
	defer tls.Close()
	exit, refused := upstreamProxy(t, "user-1", "s3cret-pass")

	for _, tc := range []struct {
		name string
		up   *url.URL
	}{{"through the exit", exit}, {"direct", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := NewForwarder(tc.up)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			if f.URL.User != nil || strings.Contains(f.URL.String(), "s3cret") {
				t.Fatalf("the browser's proxy carries credentials: %s", f.URL)
			}
			// A plain request (the fake TikTok is plain HTTP).
			c := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(f.URL)}}
			resp, err := c.Get(site.URL)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if string(b) != "hello" {
				t.Fatalf("plain: %q", b)
			}
			// A tunnel (TikTok is HTTPS).
			tr := tls.Client().Transport.(*http.Transport).Clone()
			tr.Proxy = http.ProxyURL(f.URL)
			resp, err = (&http.Client{Transport: tr}).Get(tls.URL)
			if err != nil {
				t.Fatal(err)
			}
			b, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if string(b) != "secure hello" {
				t.Fatalf("tunnel: %q", b)
			}
			if f.Bytes() < 100 {
				t.Fatalf("counted %d bytes", f.Bytes())
			}
		})
	}
	if refused.Load() != 0 {
		t.Fatalf("the exit refused %d requests: credentials not added", refused.Load())
	}
}

func TestForwarderReportsARefusedTunnel(t *testing.T) {
	exit, _ := upstreamProxy(t, "user-1", "right")
	exit.User = url.UserPassword("user-1", "wrong")
	f, err := NewForwarder(exit)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	c, err := net.Dial("tcp", f.URL.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_, _ = c.Write([]byte("CONNECT example.test:443 HTTP/1.1\r\nHost: example.test:443\r\n\r\n"))
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway || strings.Contains(string(b), "wrong") {
		t.Fatalf("got %d %q", resp.StatusCode, b)
	}
}

func TestForwarderRefusesOtherExits(t *testing.T) {
	if _, err := NewForwarder(&url.URL{Scheme: "socks5", Host: "127.0.0.1:1080"}); err == nil {
		t.Fatal("a SOCKS exit was accepted")
	}
}

// lightpanda is the Lightpanda binary for the tests: $TKS_LIGHTPANDA_BINARY.
func lightpanda(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("TKS_LIGHTPANDA_BINARY")
	if bin == "" {
		t.Skip("TKS_LIGHTPANDA_BINARY not set")
	}
	return bin
}

func TestLightpandaSignsWithoutSendingThroughAForwarder(t *testing.T) {
	bin := lightpanda(t)
	var apiCalls atomic.Int32
	var ua atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua.Store(r.Header.Get("User-Agent"))
		if strings.HasPrefix(r.URL.Path, "/api/") {
			apiCalls.Add(1)
			_, _ = w.Write([]byte(`{"ok":1}`))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(signingPage))
	}))
	defer srv.Close()
	f, err := NewForwarder(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := OpenSession(ctx, bin, Options{URL: srv.URL, Engine: EngineLightpanda, UserAgent: "Mozilla/5.0"}); err == nil {
		t.Fatal("Lightpanda took another browser's user agent")
	}
	s, err := OpenSession(ctx, bin, Options{URL: srv.URL + "/@someone", Engine: EngineLightpanda, Proxy: f.URL, BlockMedia: true, AcceptLanguage: "en-US,en;q=0.9"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, q := range []string{"one", "two"} {
		raw, err := s.Sign(ctx, "/api/comment/list/?aweme_id=1&q="+q)
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(raw)
		if err != nil || u.Query().Get("X-Sig") != "page-"+q {
			t.Fatalf("%s: signed URL %q", q, raw)
		}
	}
	if n := apiCalls.Load(); n != 0 {
		t.Fatalf("the page sent %d signed calls; it must send none", n)
	}
	got, err := s.Fetch(ctx, "/api/comment/list/?aweme_id=1&q=three")
	if err != nil || got.Status != 200 || string(got.Body) != `{"ok":1}` {
		t.Fatalf("fetch from the page: %+v, %v", got, err)
	}
	if v, _ := ua.Load().(string); !strings.Contains(v, "Lightpanda") {
		t.Fatalf("Lightpanda sent user agent %q", v)
	}
	if f.Bytes() == 0 {
		t.Fatal("the page's requests did not go through the forwarder")
	}
}
