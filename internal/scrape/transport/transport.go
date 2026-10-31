// Package transport performs HTTP requests with a browser-consistent
// TLS/HTTP2 fingerprint. Callers depend on the Doer interface so the
// underlying library can be swapped or faked.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

// Request is a single HTTP request. Header order is preserved on the wire.
type Request struct {
	Method string
	URL    string
	Header [][2]string
	Body   []byte
}

// Response is a fully read, decoded response.
type Response struct {
	Status   int
	Header   http.Header
	Body     []byte
	Location string
	// Wire bytes for this request (compressed, including TLS overhead).
	BytesIn  int64
	BytesOut int64
	Elapsed  time.Duration
}

// Doer is the transport used by identities.
type Doer interface {
	Do(ctx context.Context, req *Request) (*Response, error)
	Cookie(u *url.URL, name string) string
	SetCookie(u *url.URL, name, value string)
	Cookies(u *url.URL) map[string]string
	Close()
}

// Options configures a TLS client.
type Options struct {
	Profile  string // tls-client profile key, e.g. chrome_146
	ProxyURL string // empty = direct
	Timeout  time.Duration
	MaxBody  int64
}

// ErrBodyTooLarge is returned when a response exceeds Options.MaxBody.
var ErrBodyTooLarge = errors.New("transport: response body too large")

type tlsDoer struct {
	c       tls_client.HttpClient
	maxBody int64
}

// NewTLS returns a Doer backed by bogdanfinn/tls-client.
func NewTLS(o Options) (Doer, error) {
	prof, ok := profiles.MappedTLSClients[o.Profile]
	if !ok {
		return nil, fmt.Errorf("transport: unknown fingerprint profile %q", o.Profile)
	}
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Second
	}
	if o.MaxBody <= 0 {
		o.MaxBody = 32 << 20
	}
	opts := []tls_client.HttpClientOption{
		tls_client.WithClientProfile(prof),
		tls_client.WithTimeoutMilliseconds(int(o.Timeout.Milliseconds())),
		tls_client.WithNotFollowRedirects(),
		tls_client.WithCookieJar(tls_client.NewCookieJar()),
		tls_client.WithRandomTLSExtensionOrder(),
		tls_client.WithBandwidthTracker(),
		tls_client.WithDisableHttp3(),
	}
	if o.ProxyURL != "" {
		opts = append(opts, tls_client.WithProxyUrl(o.ProxyURL))
	}
	c, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), opts...)
	if err != nil {
		return nil, fmt.Errorf("transport: %w", err)
	}
	return &tlsDoer{c: c, maxBody: o.MaxBody}, nil
}

func (d *tlsDoer) Do(ctx context.Context, r *Request) (*Response, error) {
	var body io.Reader
	if r.Body != nil {
		body = bytesReader(r.Body)
	}
	req, err := fhttp.NewRequestWithContext(ctx, r.Method, r.URL, body)
	if err != nil {
		return nil, err
	}
	order := make([]string, 0, len(r.Header))
	for _, kv := range r.Header {
		req.Header.Add(kv[0], kv[1])
		order = append(order, kv[0])
	}
	req.Header[fhttp.HeaderOrderKey] = order

	bt := d.c.GetBandwidthTracker()
	in0, out0 := bt.GetReadBytes(), bt.GetWriteBytes()
	start := time.Now()
	resp, err := d.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	rc := fhttp.DecompressBody(resp)
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, d.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("transport: read body: %w", err)
	}
	if int64(len(b)) > d.maxBody {
		return nil, ErrBodyTooLarge
	}
	h := http.Header{}
	for k, v := range resp.Header {
		h[k] = v
	}
	in, out := bt.GetReadBytes()-in0, bt.GetWriteBytes()-out0
	if in == 0 && out == 0 && req.URL.Scheme == "http" {
		// tls-client only meters TLS connections; plain HTTP (fakefb in
		// development) falls back to the payload sizes.
		in, out = int64(len(b)), int64(len(r.Body))
	}
	return &Response{
		Status:   resp.StatusCode,
		Header:   h,
		Body:     b,
		Location: resp.Header.Get("Location"),
		BytesIn:  in,
		BytesOut: out,
		Elapsed:  time.Since(start),
	}, nil
}

func (d *tlsDoer) Cookie(u *url.URL, name string) string {
	for _, c := range d.c.GetCookies(u) {
		if c.Name == name {
			return c.Value
		}
	}
	return ""
}

func (d *tlsDoer) SetCookie(u *url.URL, name, value string) {
	d.c.SetCookies(u, []*fhttp.Cookie{{Name: name, Value: value, Path: "/", Domain: cookieDomain(u)}})
}

func (d *tlsDoer) Cookies(u *url.URL) map[string]string {
	out := map[string]string{}
	for _, c := range d.c.GetCookies(u) {
		out[c.Name] = c.Value
	}
	return out
}

func (d *tlsDoer) Close() { d.c.CloseIdleConnections() }

// cookieDomain returns ".facebook.com" for www.facebook.com and the bare
// host for anything else (fakefb, IPs).
func cookieDomain(u *url.URL) string {
	h := u.Hostname()
	if len(h) > 4 && h[:4] == "www." {
		return h[3:]
	}
	return h
}
