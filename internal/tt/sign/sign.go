// Package sign is tks's browser signer (build brief D11, G1; docs/tiktok
// ADR 0006 §4): a few TikTok pages in headless Chromium, without its
// automation flag, whose own script signs the calls that need a signature
// (comments and replies). The page never sends a call it signs
// (browser.Session.Sign); the plain-HTTP identity that asked sends the
// signed URL, from any exit (T27). tks never computes a signature itself.
//
// One signature covers one query (a changed cursor breaks it: run 3), so
// every comment or reply page costs one signature; the pages are a capped
// pool shared by the process's workers, and a page is reopened after
// tt.signer.page_max_signs signatures or page_max_age.
package sign

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
)

// Page is one open TikTok page that signs calls.
type Page interface {
	// Sign returns the URL the page's script made of rawURL, unsent.
	Sign(ctx context.Context, rawURL string) (string, error)
	Close()
}

// Opener opens a page on pageURL with id's exit and fingerprint.
type Opener func(ctx context.Context, id *session.Identity, pageURL string) (Page, error)

// ErrBusy is returned when every page stayed busy for the sign timeout.
var ErrBusy = errors.New("sign: every signer page is busy")

// ErrClosed is returned after Close.
var ErrClosed = errors.New("sign: signer closed")

// Pool is the process's signer pages. It is safe for concurrent use.
type Pool struct {
	cfg  config.Signer
	open Opener
	// Now is the clock (tests).
	Now func() time.Time

	slots chan struct{} // one per page: a page signs one call at a time

	mu     sync.Mutex
	idle   []*page
	total  int // idle and in use
	closed bool
}

type page struct {
	Page
	ua     string
	opened time.Time
	signs  int
}

// New returns a pool of cfg.Pages pages opened by open.
func New(cfg config.Signer, open Opener) *Pool {
	n := max(cfg.Pages, 1)
	return &Pool{cfg: cfg, open: open, slots: make(chan struct{}, n)}
}

func (p *Pool) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// SignURL implements client.URLSigner: a page with id's user agent (the
// signed URL carries the browser's version, so the identity that sends it
// must present the same) signs rawURL. A new page opens on referer, the
// page the call is made from, or on the identity's site.
func (p *Pool) SignURL(ctx context.Context, id *session.Identity, query, rawURL, referer string) (string, error) {
	wait, cancel := context.WithTimeout(ctx, p.cfg.SignTimeout)
	defer cancel()
	select {
	case p.slots <- struct{}{}:
	case <-wait.Done():
		metrics.SignerSigns.WithLabelValues(query, "busy").Inc()
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", ErrBusy
	}
	defer func() { <-p.slots }()

	pg, err := p.take(ctx, id, referer)
	if err != nil {
		metrics.SignerSigns.WithLabelValues(query, "error").Inc()
		return "", err
	}
	start := time.Now()
	sctx, scancel := context.WithTimeout(ctx, p.cfg.SignTimeout)
	signed, err := pg.Sign(sctx, rawURL)
	scancel()
	metrics.SignerSeconds.WithLabelValues(query).Observe(time.Since(start).Seconds())
	if err != nil {
		// A page that failed may be broken (closed, crashed, navigated
		// away): the next call opens a fresh one.
		p.discard(pg)
		metrics.SignerSigns.WithLabelValues(query, "error").Inc()
		return "", fmt.Errorf("sign: %s: %w", query, err)
	}
	pg.signs++
	p.give(pg)
	metrics.SignerSigns.WithLabelValues(query, "ok").Inc()
	return signed, nil
}

// take returns an idle page for id's user agent, or opens one. The caller
// holds a slot, so at most cfg.Pages-1 other pages are in use.
func (p *Pool) take(ctx context.Context, id *session.Identity, referer string) (*page, error) {
	ua := id.FP.UserAgent
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	var stale []*page
	var got *page
	keep := p.idle[:0]
	for _, pg := range p.idle {
		switch {
		case p.spent(pg):
			stale = append(stale, pg)
		case got == nil && pg.ua == ua:
			got = pg
		default:
			keep = append(keep, pg)
		}
	}
	p.idle = keep
	p.total -= len(stale)
	if got == nil && p.total >= cap(p.slots) && len(p.idle) > 0 {
		// Room for a page with this user agent: close the oldest idle one.
		stale = append(stale, p.idle[0])
		p.idle = p.idle[1:]
		p.total--
	}
	if got == nil {
		p.total++ // reserved for the page about to open
	}
	p.mu.Unlock()
	for _, pg := range stale {
		pg.Close()
		metrics.SignerPages.Dec()
	}
	if got != nil {
		return got, nil
	}
	pg, err := p.openPage(ctx, id, pageURL(id, referer))
	if err != nil {
		p.mu.Lock()
		p.total--
		p.mu.Unlock()
		metrics.SignerPageOpens.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("sign: open page: %w", err)
	}
	metrics.SignerPageOpens.WithLabelValues("ok").Inc()
	metrics.SignerPages.Inc()
	return &page{Page: pg, ua: ua, opened: p.now()}, nil
}

// openPage opens a page within tt.signer.page_timeout. The page outlives
// the call, and a browser's connection keeps its opening context's
// deadline, so the page opens on a context without one; a page that opens
// after the timeout is closed when it does.
func (p *Pool) openPage(ctx context.Context, id *session.Identity, pageURL string) (Page, error) {
	type opened struct {
		pg  Page
		err error
	}
	ch := make(chan opened, 1)
	go func() {
		pg, err := p.open(context.WithoutCancel(ctx), id, pageURL)
		ch <- opened{pg, err}
	}()
	t := time.NewTimer(p.cfg.PageTimeout)
	defer t.Stop()
	select {
	case o := <-ch:
		return o.pg, o.err
	case <-t.C:
	case <-ctx.Done():
	}
	go func() {
		if o := <-ch; o.err == nil {
			o.pg.Close()
		}
	}()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, fmt.Errorf("no page after %s", p.cfg.PageTimeout)
}

// spent reports a page due to be reopened.
func (p *Pool) spent(pg *page) bool {
	return (p.cfg.PageMaxSigns > 0 && pg.signs >= p.cfg.PageMaxSigns) ||
		(p.cfg.PageMaxAge > 0 && p.now().Sub(pg.opened) >= p.cfg.PageMaxAge)
}

// give returns a page to the idle list, or closes it when it is spent or
// the pool is closed.
func (p *Pool) give(pg *page) {
	p.mu.Lock()
	if p.closed || p.spent(pg) {
		p.total--
		p.mu.Unlock()
		pg.Close()
		metrics.SignerPages.Dec()
		return
	}
	p.idle = append(p.idle, pg)
	p.mu.Unlock()
}

func (p *Pool) discard(pg *page) {
	p.mu.Lock()
	p.total--
	p.mu.Unlock()
	pg.Close()
	metrics.SignerPages.Dec()
}

// Pages returns how many pages are open (idle or signing).
func (p *Pool) Pages() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total
}

// Close closes the idle pages; pages in use close when their call ends.
func (p *Pool) Close() {
	p.mu.Lock()
	idle := p.idle
	p.idle, p.closed = nil, true
	p.total -= len(idle)
	p.mu.Unlock()
	for _, pg := range idle {
		pg.Close()
		metrics.SignerPages.Dec()
	}
}

// pageURL is where a new page opens: the call's referer when it is on the
// identity's site (the profile or post page the call belongs to, as the
// probe signed from: C2 run 2, run 3), else the site itself.
func pageURL(id *session.Identity, referer string) string {
	if u, err := url.Parse(referer); err == nil && id.Base != nil && u.Host == id.Base.Host && u.Path != "" {
		return referer
	}
	if id.Base != nil {
		return id.Base.String()
	}
	return referer
}

// BrowserOpener opens signer pages in headless Chromium: on the identity's
// exit, with its user agent and client hints, without the automation flag
// (comments were refused with it on: C2 run 1), media blocked (which also
// makes the page's requests pause, so Sign can hold them).
func BrowserOpener(binary string, noSandbox bool, acceptLanguage string, callTimeout time.Duration) Opener {
	return func(ctx context.Context, id *session.Identity, pageURL string) (Page, error) {
		var proxyURL *url.URL
		if id.Endpoint.URL != "" {
			u, err := url.Parse(id.Endpoint.URL)
			if err != nil {
				return nil, fmt.Errorf("sign: exit: %w", err)
			}
			proxyURL = u
		}
		s, err := browser.OpenSession(ctx, binary, browser.Options{URL: pageURL, Proxy: proxyURL, UserAgent: id.FP.UserAgent,
			AcceptLanguage: acceptLanguage, Hints: browser.HintsFrom(id.FP.SecChUa, id.FP.SecChUaPlatform, id.FP.SecChUaMobile),
			BlockMedia: true, HideAutomation: true, NoSandbox: noSandbox, MaxSettle: 15 * time.Second, CallTimeout: callTimeout})
		if err != nil {
			return nil, err
		}
		return s, nil
	}
}
