package transport

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"time"
)

// stdDoer uses net/http. It has Go's TLS fingerprint, so it is only for
// tests and for traffic where fingerprinting does not matter.
type stdDoer struct {
	c       *http.Client
	maxBody int64
}

// NewStd returns a net/http based Doer that does not follow redirects.
func NewStd(timeout time.Duration) Doer {
	jar, _ := cookiejar.New(nil)
	return &stdDoer{
		c: &http.Client{
			Timeout:       timeout,
			Jar:           jar,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		maxBody: 32 << 20,
	}
}

func (d *stdDoer) Do(ctx context.Context, r *Request) (*Response, error) {
	req, err := http.NewRequestWithContext(ctx, r.Method, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return nil, err
	}
	for _, kv := range r.Header {
		if kv[0] == "accept-encoding" {
			continue // let net/http negotiate gzip and decode transparently
		}
		req.Header.Add(kv[0], kv[1])
	}
	start := time.Now()
	resp, err := d.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, d.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > d.maxBody {
		return nil, ErrBodyTooLarge
	}
	return &Response{
		Status:   resp.StatusCode,
		Header:   resp.Header,
		Body:     b,
		Location: resp.Header.Get("Location"),
		BytesIn:  int64(len(b)),
		BytesOut: int64(len(r.Body)),
		Elapsed:  time.Since(start),
	}, nil
}

func (d *stdDoer) Cookie(u *url.URL, name string) string {
	for _, c := range d.c.Jar.Cookies(u) {
		if c.Name == name {
			return c.Value
		}
	}
	return ""
}

func (d *stdDoer) SetCookie(u *url.URL, name, value string) {
	// Client-side jar entry; Secure/HttpOnly are server-set attributes.
	d.c.Jar.SetCookies(u, []*http.Cookie{{Name: name, Value: value, Path: "/"}}) //nolint:gosec
}

func (d *stdDoer) Cookies(u *url.URL) map[string]string {
	out := map[string]string{}
	for _, c := range d.c.Jar.Cookies(u) {
		out[c.Name] = c.Value
	}
	return out
}

func (d *stdDoer) Close() { d.c.CloseIdleConnections() }
