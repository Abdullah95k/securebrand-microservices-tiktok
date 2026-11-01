package browser

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Session is one headless Chromium page kept open on an identity's exit
// and fingerprint for many requests. TikTok's signing fallback (docs/tiktok
// ADR 0006 §4) runs the platform's own API calls from inside its page, so
// the page's script adds whatever it adds (signatures, tokens) and tks never
// re-implements it. Unlike Render, which starts and stops a browser per
// page, a Session keeps the page and answers its events (proxy challenges,
// blocked media) in the background until Close.
//
// One Session belongs to one identity. Its methods are safe for concurrent
// use; evaluations run one at a time.
type Session struct {
	// URL and Status are the page's document after redirects.
	URL    string
	Status int

	r    *renderer
	stop func()

	mu   sync.Mutex // guards r's maps and counters, which the pump writes
	eval sync.Mutex // one evaluation at a time on the page

	// ctx lives as long as the Session (not OpenSession's context): the
	// pump answers the page's events with it until Close cancels it.
	ctx       context.Context
	cancel    context.CancelFunc
	quit      chan struct{}
	pumpDone  chan struct{}
	closeOnce sync.Once
}

// keepExchanges bounds the captured requests a long Session remembers.
const keepExchanges = 256

// OpenSession starts a browser for opts (proxy, user agent and hints,
// language, cookies, media blocking), loads opts.URL and waits for it to
// settle. opts.Capture selects the requests whose URLs Fetch reports (by
// default the platform's API calls: paths under /api/).
func OpenSession(ctx context.Context, binary string, opts Options) (*Session, error) {
	if opts.Capture == nil {
		opts.Capture = func(raw string) bool {
			u, err := url.Parse(raw)
			return err == nil && strings.HasPrefix(u.Path, "/api/")
		}
	}
	if opts.Idle <= 0 {
		opts.Idle = 500 * time.Millisecond
	}
	if opts.MaxSettle <= 0 {
		opts.MaxSettle = 5 * time.Second
	}
	r, stop, err := launchPage(ctx, binary, opts)
	if err != nil {
		return nil, err
	}
	if err := r.navigate(ctx); err != nil {
		stop()
		return nil, err
	}
	if err := r.settle(ctx); err != nil {
		stop()
		return nil, err
	}
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &Session{r: r, stop: stop, ctx: sctx, cancel: cancel, quit: make(chan struct{}), pumpDone: make(chan struct{})}
	doc := r.docs[r.frameID]
	s.URL, s.Status = doc.url, doc.status
	if s.URL == "" {
		s.URL = opts.URL
	}
	go s.pump()
	return s, nil
}

// pump handles the page's events until Close: proxy challenges must be
// answered and paused requests continued while the Session idles between
// calls, or the page's own requests hang.
func (s *Session) pump() {
	defer close(s.pumpDone)
	for {
		select {
		case ev := <-s.r.c.events:
			s.mu.Lock()
			_ = s.r.handle(s.ctx, ev)
			s.mu.Unlock()
		case <-s.r.c.done:
			return
		case <-s.quit:
			return
		}
	}
}

// Evaluate runs a JavaScript expression in the page, awaits it when it is
// a promise, and returns its value as JSON.
func (s *Session) Evaluate(ctx context.Context, expr string) (json.RawMessage, error) {
	s.eval.Lock()
	defer s.eval.Unlock()
	select {
	case <-s.quit:
		return nil, ErrSessionClosed
	default:
	}
	var out struct {
		Result struct {
			Value json.RawMessage `json:"value"`
		} `json:"result"`
		ExceptionDetails *struct {
			Text      string `json:"text"`
			Exception *struct {
				Description string `json:"description"`
			} `json:"exception"`
		} `json:"exceptionDetails"`
	}
	if err := s.r.c.call(ctx, s.r.s, "Runtime.evaluate", map[string]any{
		"expression": expr, "returnByValue": true, "awaitPromise": true,
	}, &out); err != nil {
		return nil, err
	}
	if e := out.ExceptionDetails; e != nil {
		msg := e.Text
		if e.Exception != nil && e.Exception.Description != "" {
			msg = e.Exception.Description
		}
		return nil, fmt.Errorf("browser: page script: %s", msg)
	}
	return out.Result.Value, nil
}

// Fetched is one call made from inside the page.
type Fetched struct {
	Status int
	Body   []byte
	// SentURL is the URL the page's network stack actually sent, after the
	// page's own scripts changed it (TikTok's adds its signatures and
	// msToken). Empty when the request was not captured.
	SentURL string
	Elapsed time.Duration
}

// Fetch makes a GET with the page's fetch(), with the page's cookies, and
// returns the answer and the URL actually sent.
func (s *Session) Fetch(ctx context.Context, rawURL string) (*Fetched, error) {
	target, err := url.Parse(s.URL)
	if err != nil {
		return nil, err
	}
	if target, err = target.Parse(rawURL); err != nil {
		return nil, err
	}
	s.mu.Lock()
	before := len(s.r.order)
	s.mu.Unlock()
	lit, _ := json.Marshal(target.String())
	start := time.Now()
	v, err := s.Evaluate(ctx, `(async () => { const r = await fetch(`+string(lit)+`, {credentials: "include"}); return {status: r.status, body: await r.text()}; })()`)
	if err != nil {
		return nil, err
	}
	var res struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal(v, &res); err != nil {
		return nil, fmt.Errorf("browser: fetch result: %w", err)
	}
	f := &Fetched{Status: res.Status, Body: []byte(res.Body), Elapsed: time.Since(start)}
	// The request's event may reach the pump just after the call returns.
	deadline := time.Now().Add(time.Second)
	for f.SentURL == "" {
		s.mu.Lock()
		for _, id := range s.r.order[min(before, len(s.r.order)):] {
			if x := s.r.reqs[id]; x != nil {
				if u, err := url.Parse(x.URL); err == nil && u.Path == target.Path {
					f.SentURL = x.URL
				}
			}
		}
		s.prune()
		s.mu.Unlock()
		if f.SentURL != "" || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return f, nil
		case <-time.After(10 * time.Millisecond):
		}
	}
	return f, nil
}

// Mark returns the position of the newest captured request, for Captured.
func (s *Session) Mark() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.seq
}

// Captured returns the captured requests that started after mark and have
// finished, with their response bodies: the page's own API calls as well as
// Fetch's. Only the newest requests are remembered (keepExchanges).
func (s *Session) Captured(ctx context.Context, mark int) []Exchange {
	s.mu.Lock()
	var ids []string
	var out []Exchange
	for _, id := range s.r.order {
		x := s.r.reqs[id]
		if x == nil || x.seq <= mark || x.Status == 0 || s.r.inflight[id] {
			continue
		}
		ids = append(ids, id)
		out = append(out, *x)
	}
	s.mu.Unlock()
	for i, id := range ids {
		var body struct {
			Body          string `json:"body"`
			Base64Encoded bool   `json:"base64Encoded"`
		}
		if err := s.r.c.call(ctx, s.r.s, "Network.getResponseBody", map[string]any{"requestId": id}, &body); err != nil {
			continue
		}
		if body.Base64Encoded {
			out[i].Body, _ = base64.StdEncoding.DecodeString(body.Body)
		} else {
			out[i].Body = []byte(body.Body)
		}
	}
	return out
}

// Navigate loads another URL in the page, as following a link would, and
// waits for it to load and its captured requests to settle. URL and Status
// then describe the new document.
func (s *Session) Navigate(ctx context.Context, rawURL string) error {
	s.eval.Lock()
	defer s.eval.Unlock()
	select {
	case <-s.quit:
		return ErrSessionClosed
	default:
	}
	s.mu.Lock()
	s.r.loaded = false
	s.mu.Unlock()
	var nav struct {
		FrameID   string `json:"frameId"`
		ErrorText string `json:"errorText"`
	}
	if err := s.r.c.call(ctx, s.r.s, "Page.navigate", map[string]any{"url": rawURL}, &nav); err != nil {
		return err
	}
	if nav.ErrorText != "" {
		return fmt.Errorf("browser: navigate %s: %s", rawURL, nav.ErrorText)
	}
	if err := s.wait(ctx, func() bool { return s.r.loaded }); err != nil {
		return err
	}
	if err := s.quiet(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	if nav.FrameID != "" {
		s.r.frameID = nav.FrameID
	}
	doc := s.r.docs[s.r.frameID]
	s.mu.Unlock()
	s.URL, s.Status = doc.url, doc.status
	if s.URL == "" {
		s.URL = rawURL
	}
	return nil
}

// Scroll scrolls the page to its bottom, as a person reading on would, and
// waits for the requests that starts to settle.
func (s *Session) Scroll(ctx context.Context) error {
	if _, err := s.Evaluate(ctx, "window.scrollTo(0, document.body.scrollHeight)"); err != nil {
		return err
	}
	s.eval.Lock()
	defer s.eval.Unlock()
	return s.quiet(ctx)
}

// quiet waits until no captured request is in flight and none started for
// the session's idle time, or its settle bound passed. The pump keeps the
// state current meanwhile.
func (s *Session) quiet(ctx context.Context) error {
	s.mu.Lock()
	s.r.lastNet = time.Now()
	idle, deadline := s.r.opts.Idle, time.Now().Add(s.r.opts.MaxSettle)
	s.mu.Unlock()
	return s.wait(ctx, func() bool {
		return (len(s.r.inflight) == 0 && time.Since(s.r.lastNet) >= idle) || time.Now().After(deadline)
	})
}

// wait polls done, under s.mu, until it holds; the pump handles the page's
// events meanwhile.
func (s *Session) wait(ctx context.Context, done func() bool) error {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		s.mu.Lock()
		ok, failed := done(), s.r.authFailed
		s.mu.Unlock()
		if failed {
			return errors.New("browser: proxy rejected the credentials")
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.quit:
			return ErrSessionClosed
		case <-s.pumpDone:
			return errors.New("browser: devtools connection closed")
		case <-t.C:
		}
	}
}

// prune forgets all but the newest captured requests. Callers hold s.mu.
func (s *Session) prune() {
	if n := len(s.r.order) - keepExchanges; n > 0 {
		for _, id := range s.r.order[:n] {
			delete(s.r.reqs, id)
			delete(s.r.inflight, id)
		}
		s.r.order = append([]string(nil), s.r.order[n:]...)
	}
}

// Cookies returns the page's cookies.
func (s *Session) Cookies(ctx context.Context) ([]Cookie, error) {
	var out struct {
		Cookies []Cookie `json:"cookies"`
	}
	if err := s.r.c.call(ctx, s.r.s, "Network.getCookies", map[string]any{"urls": []string{s.URL}}, &out); err != nil {
		return nil, err
	}
	return out.Cookies, nil
}

// HTML returns the page's DOM after its scripts ran.
func (s *Session) HTML(ctx context.Context) ([]byte, error) {
	v, err := s.Evaluate(ctx, "document.documentElement.outerHTML")
	if err != nil {
		return nil, err
	}
	var html string
	if err := json.Unmarshal(v, &html); err != nil {
		return nil, err
	}
	return []byte(html), nil
}

// Bytes is what the page's requests moved on the wire so far (compressed),
// which is what a proxy bills.
func (s *Session) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.wire
}

// ErrSessionClosed is returned by calls on a closed Session.
var ErrSessionClosed = errors.New("browser: session closed")

// Close ends the browser. It is safe to call more than once.
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		close(s.quit)
		<-s.pumpDone
		s.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.r.c.call(ctx, "", "Browser.close", nil, nil)
		cancel()
		s.stop()
	})
}
