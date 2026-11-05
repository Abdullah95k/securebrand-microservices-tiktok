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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/htmlx"
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
	// CloseTimeout bounds how long closing a page holds its caller (the
	// sign that replaces a spent page, the sweeper); a page still closing
	// then is left to finish on its own. Zero is DefaultCloseTimeout.
	CloseTimeout time.Duration

	slots chan struct{} // one per page: a page signs one call at a time

	mu     sync.Mutex
	idle   []*page
	total  int // idle and in use
	closed bool
	// pageSeconds integrates total over time since BrowserSeconds last
	// read it (from since): the pool's browser time, which tks's cost
	// prices (build brief §8 A5).
	pageSeconds float64
	since       time.Time
	// signedBy remembers which page signed each recent call (by path and
	// query), until TikTok's answer is reported (SignOutcome).
	signedBy map[string]*page

	quit chan struct{} // stops the sweeper
	done chan struct{}
}

type page struct {
	Page
	ua       string
	opened   time.Time
	lastUsed time.Time
	signs    int
	// refusals are the page's signed calls refused in a row; bad marks a
	// page past tt.signer.max_refusals, closed instead of reused.
	refusals int
	bad      bool
}

// SweepEvery is how often a pool with tt.signer.idle_close or max_page_mb
// looks at its idle pages.
var SweepEvery = 10 * time.Second

// New returns a pool of cfg.Pages pages opened by open. With
// cfg.IdleClose or cfg.MaxPageMB it sweeps its idle pages every
// SweepEvery until Close.
func New(cfg config.Signer, open Opener) *Pool {
	n := max(cfg.Pages, 1)
	p := &Pool{cfg: cfg, open: open, slots: make(chan struct{}, n), quit: make(chan struct{}), done: make(chan struct{}),
		signedBy: map[string]*page{}}
	metrics.SignerPagesMax.Set(float64(n))
	if cfg.IdleClose > 0 || cfg.MaxPageMB > 0 {
		go p.sweeper(SweepEvery)
	} else {
		close(p.done)
	}
	return p
}

func (p *Pool) sweeper(every time.Duration) {
	defer close(p.done)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-p.quit:
			return
		case <-t.C:
			p.Sweep()
		}
	}
}

// Sweep closes the idle pages unused for tt.signer.idle_close or whose
// browser uses more than tt.signer.max_page_mb: an idle page costs memory
// and does nothing (the pilot server froze when an idle Chrome page grew
// to 7 GB, 2026-11-03), and the next call opens a fresh one.
func (p *Pool) Sweep() {
	p.mu.Lock()
	var stale []closing
	keep := p.idle[:0]
	for _, pg := range p.idle {
		why := ""
		switch {
		case pg.bad:
			why = CloseRefused
		case p.idleTooLong(pg):
			why = CloseIdle
		case p.tooBig(pg):
			why = CloseMemory
		}
		if why != "" {
			stale = append(stale, closing{pg, why})
		} else {
			keep = append(keep, pg)
		}
	}
	p.idle = keep
	p.addTotal(-len(stale))
	p.mu.Unlock()
	for _, c := range stale {
		p.closePage(c.pg, c.why)
	}
}

// Why a signer page was closed (tks_signer_page_closes_total).
const (
	CloseSpent    = "spent"    // page_max_signs or page_max_age reached
	CloseIdle     = "idle"     // unused for idle_close
	CloseMemory   = "memory"   // its browser past max_page_mb
	CloseRefused  = "refused"  // its signed calls refused max_refusals times in a row
	CloseError    = "error"    // a signature failed
	CloseRoom     = "room"     // closed to open a page for another user agent, or for a retry
	CloseShutdown = "shutdown" // the pool closed
	CloseStuck    = "stuck"    // did not close within CloseTimeout (left to finish)
)

type closing struct {
	pg  *page
	why string
}

// closePage closes a page the pool no longer counts.
// DefaultCloseTimeout is Pool.CloseTimeout's default: a browser.Session
// closes within about 7 seconds even when its page is stuck.
const DefaultCloseTimeout = 15 * time.Second

// closePage closes pg, waiting at most CloseTimeout. A page that would not
// close held the C6 probe's lane for good (2026-11-05): browser.Session's
// Close is bounded since, and this bound keeps any Page from doing it again.
func (p *Pool) closePage(pg *page, why string) {
	done := make(chan struct{})
	go func() {
		pg.Close()
		close(done)
	}()
	wait := p.CloseTimeout
	if wait <= 0 {
		wait = DefaultCloseTimeout
	}
	t := time.NewTimer(wait)
	select {
	case <-done:
	case <-t.C:
		why = CloseStuck
		slog.Warn("sign: a signer page did not close in time; left to finish on its own", "after", wait)
	}
	t.Stop()
	metrics.SignerPages.Dec()
	metrics.SignerPageCloses.WithLabelValues(why).Inc()
}

func (p *Pool) idleTooLong(pg *page) bool {
	return p.cfg.IdleClose > 0 && p.now().Sub(pg.lastUsed) >= p.cfg.IdleClose
}

// tooBig reports a page whose browser uses more than tt.signer.max_page_mb.
func (p *Pool) tooBig(pg *page) bool {
	m, ok := pg.Page.(interface{ MemoryMB() float64 })
	return ok && p.cfg.MaxPageMB > 0 && m.MemoryMB() > p.cfg.MaxPageMB
}

// addTotal changes the page count (under p.mu), first adding the time the
// pages were open at the old count to the pool's browser time.
func (p *Pool) addTotal(n int) {
	p.integrate()
	p.total += n
}

func (p *Pool) integrate() {
	now := p.now()
	if !p.since.IsZero() && now.After(p.since) {
		p.pageSeconds += float64(p.total) * now.Sub(p.since).Seconds()
	}
	p.since = now
}

// BrowserSeconds returns the seconds the pool's pages were open (a page
// counts from the moment its opening starts to its close) since the last
// call: browser time, priced by tks's cost (tt.cost.browser_hour_usd).
func (p *Pool) BrowserSeconds() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.integrate()
	s := p.pageSeconds
	p.pageSeconds = 0
	return s
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
	pg.lastUsed = p.now()
	p.mu.Lock()
	if len(p.signedBy) >= maxSignedBy {
		clear(p.signedBy) // answers never reported (the caller gave up): forget them
	}
	p.signedBy[callKey(signed)] = pg
	p.mu.Unlock()
	p.give(pg)
	metrics.SignerSigns.WithLabelValues(query, "ok").Inc()
	return signed, nil
}

// take returns an idle page for id's user agent, or opens one. The caller
// holds a slot, so at most cfg.Pages-1 other pages are in use.
func (p *Pool) take(ctx context.Context, id *session.Identity, referer string) (*page, error) {
	ua := id.FP.UserAgent
	var avoid *page
	if r := graphql.SignRetryFrom(ctx); r != nil {
		avoid, _ = r.Avoid.(*page)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	var stale []closing
	var got *page
	keep := p.idle[:0]
	for _, pg := range p.idle {
		switch {
		case pg.bad:
			stale = append(stale, closing{pg, CloseRefused})
		case p.spent(pg):
			stale = append(stale, closing{pg, CloseSpent})
		case p.idleTooLong(pg):
			stale = append(stale, closing{pg, CloseIdle})
		case got == nil && pg.ua == ua && pg != avoid:
			got = pg
		default:
			keep = append(keep, pg)
		}
	}
	p.idle = keep
	p.addTotal(-len(stale))
	if got == nil && p.total >= cap(p.slots) && len(p.idle) > 0 {
		// Room for a page with this user agent: close the oldest idle one,
		// or the one a retry avoids.
		i := 0
		for j, pg := range p.idle {
			if pg == avoid {
				i = j
			}
		}
		stale = append(stale, closing{p.idle[i], CloseRoom})
		p.idle = append(p.idle[:i], p.idle[i+1:]...)
		p.addTotal(-1)
	}
	if got == nil {
		p.addTotal(1) // reserved for the page about to open
	}
	p.mu.Unlock()
	for _, c := range stale {
		p.closePage(c.pg, c.why)
	}
	if got != nil {
		return got, nil
	}
	pg, err := p.openPage(ctx, id, p.pageURL(id, referer))
	if err != nil {
		p.mu.Lock()
		p.addTotal(-1)
		p.mu.Unlock()
		metrics.SignerPageOpens.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("sign: open page: %w", err)
	}
	metrics.SignerPageOpens.WithLabelValues("ok").Inc()
	metrics.SignerPages.Inc()
	now := p.now()
	return &page{Page: pg, ua: ua, opened: now, lastUsed: now}, nil
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
	big := p.tooBig(pg)
	p.mu.Lock()
	why := ""
	switch {
	case p.closed:
		why = CloseShutdown
	case pg.bad:
		why = CloseRefused
	case p.spent(pg):
		why = CloseSpent
	case big:
		why = CloseMemory
	}
	if why != "" {
		p.addTotal(-1)
		p.mu.Unlock()
		p.closePage(pg, why)
		return
	}
	p.idle = append(p.idle, pg)
	p.mu.Unlock()
}

func (p *Pool) discard(pg *page) {
	p.mu.Lock()
	p.addTotal(-1)
	p.mu.Unlock()
	p.closePage(pg, CloseError)
}

// maxSignedBy bounds the calls whose answer the pool waits to hear.
const maxSignedBy = 4096

// callKey identifies a signed call by its path and query: the page signs
// on its own host, and the call goes to the identity's.
func callKey(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Path + "?" + u.RawQuery
}

// SignOutcome implements graphql.SignReporter: TikTok's answer to a call a
// page signed. A refused call (an empty answer) is noted for the client's
// retry (graphql.SignRetry), which then signs on another page: an idle one
// with the same user agent, or a new page on the call's own account
// (C4's third attempt: refusals came only from pages opened for other
// accounts). A page whose signed calls are refused tt.signer.max_refusals
// times in a row is closed and the next call opens a fresh one: in C4's
// second attempt (2026-11-03) pages went bad within minutes, every call
// they signed was refused until they were replaced, and a fresh page was
// answered again. An answered call resets the count.
func (p *Pool) SignOutcome(ctx context.Context, sentURL string, cls detect.Class) {
	key := callKey(sentURL)
	p.mu.Lock()
	defer p.mu.Unlock()
	pg := p.signedBy[key]
	if pg == nil {
		return
	}
	delete(p.signedBy, key)
	switch cls {
	case detect.OK:
		pg.refusals = 0
		metrics.SignedCalls.WithLabelValues("answered").Inc()
	case detect.RegistryStale:
		metrics.SignedCalls.WithLabelValues("refused").Inc()
		if r := graphql.SignRetryFrom(ctx); r != nil && r.Avoid == nil {
			r.Avoid = pg // the client signs the call once more, elsewhere
		}
		pg.refusals++
		if p.cfg.MaxRefusals > 0 && pg.refusals >= p.cfg.MaxRefusals && !pg.bad {
			pg.bad = true
			metrics.SignerPagesRefused.Inc()
		}
	}
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
	if !p.closed {
		close(p.quit)
	}
	idle := p.idle
	p.idle, p.closed = nil, true
	p.addTotal(-len(idle))
	p.mu.Unlock()
	for _, pg := range idle {
		p.closePage(pg, CloseShutdown)
	}
	<-p.done
}

// pageURL is where a new page opens: the call's referer when it is on the
// identity's site (the profile or post page the call belongs to, as the
// probe signed from: C2 run 2, run 3), or with tt.signer.open_on profile
// the profile page of the account the call belongs to (no video player:
// TikTok's video page grew a signer's Chrome past 1 GB in 30 s, S1), else
// the site itself.
func (p *Pool) pageURL(id *session.Identity, referer string) string {
	if u, err := url.Parse(referer); err == nil && id.Base != nil && u.Host == id.Base.Host && u.Path != "" {
		if p.cfg.OpenOn == config.SignerOpenOnProfile {
			if name, _, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/"); strings.HasPrefix(name, "@") {
				u.Path, u.RawQuery, u.Fragment = "/"+name, "", ""
				return u.String()
			}
		}
		return referer
	}
	if id.Base != nil {
		return id.Base.String()
	}
	return referer
}

// BrowserConfig is the crawler's signer browser.
type BrowserConfig struct {
	Binary         string
	NoSandbox      bool
	AcceptLanguage string
	CallTimeout    time.Duration
	// Freeze keeps each page frozen between its signatures
	// (tt.signer.freeze): no script or timer runs while it waits.
	Freeze bool
	// Lite starts Chrome without autoplay and images (tt.signer.lite).
	Lite bool
}

// BrowserOpener opens signer pages in headless Chromium: on the identity's
// exit, with its user agent and client hints, without the automation flag
// (comments were refused with it on: C2 run 1, and from every browser that
// showed it, S1), media blocked (which also makes the page's requests
// pause, so Sign can hold them). Each page reads its own page values and
// environment once, and signs every call with them in place of the
// identity's (PageQuery).
func BrowserOpener(c BrowserConfig) Opener {
	binary, noSandbox, acceptLanguage, callTimeout := c.Binary, c.NoSandbox, c.AcceptLanguage, c.CallTimeout
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
			BlockMedia: true, HideAutomation: true, NoSandbox: noSandbox, MaxSettle: 15 * time.Second, CallTimeout: callTimeout, Lite: c.Lite})
		if err != nil {
			return nil, err
		}
		values, err := pageValues(ctx, s)
		if err != nil {
			s.Close()
			return nil, err
		}
		pg := &browserPage{Session: s, values: values, freeze: c.Freeze}
		if c.Freeze {
			if err := s.Freeze(ctx); err != nil {
				s.Close()
				return nil, fmt.Errorf("sign: freeze: %w", err)
			}
		}
		return pg, nil
	}
}

// browserPage signs with the page's own values, thawing a frozen page for
// the call.
type browserPage struct {
	*browser.Session
	values map[string]string
	freeze bool
}

func (p *browserPage) Sign(ctx context.Context, rawURL string) (string, error) {
	if err := p.thaw(ctx); err != nil {
		return "", err
	}
	defer p.refreeze(ctx)
	return p.Session.Sign(ctx, PageQuery(rawURL, p.values))
}

// PageFetch has the page send the call itself, signed by its script as
// Sign would sign it, and returns TikTok's answer.
func (p *browserPage) PageFetch(ctx context.Context, rawURL string) (*browser.Fetched, error) {
	if err := p.thaw(ctx); err != nil {
		return nil, err
	}
	defer p.refreeze(ctx)
	return p.Fetch(ctx, PageQuery(rawURL, p.values))
}

// ProxyBytes is what the page moved on the wire so far.
func (p *browserPage) ProxyBytes() int64 { return p.Bytes() }

func (p *browserPage) thaw(ctx context.Context) error {
	if !p.freeze {
		return nil
	}
	if err := p.Thaw(ctx); err != nil {
		return fmt.Errorf("sign: thaw: %w", err)
	}
	return nil
}

func (p *browserPage) refreeze(ctx context.Context) {
	if p.freeze {
		_ = p.Freeze(context.WithoutCancel(ctx))
	}
}

// pageValues reads what the page's own calls carry and tks's builder would
// otherwise fill from the plain-HTTP identity: the page's device id and
// the other values of its app context (ttclient.PageTokens), and the
// browser's time zone, screen and history length.
func pageValues(ctx context.Context, s *browser.Session) (map[string]string, error) {
	html, err := s.HTML(ctx)
	if err != nil {
		return nil, fmt.Errorf("sign: page: %w", err)
	}
	tok, ok := htmlx.Extract(html)
	if !ok {
		return nil, errors.New("sign: the signer page carried no page values (no device id)")
	}
	out := map[string]string{}
	for _, k := range ttclient.PageTokens {
		if tok[k] != "" {
			out[k] = tok[k]
		}
	}
	var env struct {
		TZ      string  `json:"tz"`
		Width   float64 `json:"w"`
		Height  float64 `json:"h"`
		History float64 `json:"history"`
	}
	v, err := s.Evaluate(ctx, `({tz: Intl.DateTimeFormat().resolvedOptions().timeZone, w: screen.width, h: screen.height, history: history.length})`)
	if err == nil && json.Unmarshal(v, &env) == nil {
		if env.TZ != "" {
			out["tz_name"] = env.TZ
		}
		if env.Width > 0 && env.Height > 0 {
			out["screen_width"], out["screen_height"] = strconv.Itoa(int(env.Width)), strconv.Itoa(int(env.Height))
		}
		if env.History > 0 {
			out["history_len"] = strconv.Itoa(int(env.History))
		}
	}
	return out, nil
}

// PageQuery is the URL a signer page signs: the call's own query with the
// page's values (device id, app context, time zone, screen, history) in
// place of the identity's, and without msToken or any earlier signature,
// which the page's script adds itself. TikTok refused comment URLs signed
// with the plain-HTTP identity's values (Phase 0, 2026-11-02) and answered
// those built with the page's (the probe, C2 run 3); the signed URL is then
// sent by the identity from any exit (T27).
func PageQuery(rawURL string, values map[string]string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := u.Query()
	flat := make(map[string]string, len(q)+len(values))
	for k, vs := range q {
		if len(vs) > 0 {
			flat[k] = vs[0]
		}
	}
	for _, k := range []string{"msToken", "X-Bogus", "X-Gnarly", "X-Dynosaur"} {
		delete(flat, k)
	}
	for k, v := range values {
		flat[k] = v
	}
	u.RawQuery = ttclient.Encode(flat)
	return u.String()
}
