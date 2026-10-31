package browser

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Errors of a capped fallback.
var (
	ErrCapped = errors.New("browser: share cap reached")
	ErrBusy   = errors.New("browser: all browsers busy")
)

// Bootstrapper is the session.BrowserFallback: it renders the page through
// the identity's own exit and fingerprint, copies the cookies into the
// identity's transport and installs the tokens found in the rendered DOM.
// At most MaxShare of HTML loads may use it (a credit that every observed
// load adds to, capped at a small burst) and MaxConcurrent run at once; a
// capped call fails at once rather than holding a crawl slot.
type Bootstrapper struct {
	Binary         string
	AcceptLanguage string
	Timeout        time.Duration
	MaxShare       float64
	NoSandbox      bool
	Rules          detect.Rules
	// Hooks are the platform's page hooks, as the HTTP loader's: tokens,
	// wall paths and AfterLoad (run with canReload false).
	Hooks session.Hooks

	sem    chan struct{}
	mu     sync.Mutex
	credit float64
	burst  float64
	render func(ctx context.Context, binary string, opts Options) (*Result, error)
}

// NewBootstrapper returns a fallback for cfg, or an error when no browser
// binary is found.
func NewBootstrapper(cfg config.Browser, acceptLanguage string, hooks session.Hooks) (*Bootstrapper, error) {
	bin, err := FindBinary(cfg.Binary)
	if err != nil {
		return nil, err
	}
	n := max(cfg.MaxConcurrent, 1)
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	// The burst lets the first pages of a fresh pod through; the credit
	// then grows by MaxShare per HTML load.
	return &Bootstrapper{Binary: bin, AcceptLanguage: acceptLanguage, Timeout: timeout, MaxShare: cfg.MaxShare, NoSandbox: cfg.NoSandbox,
		Rules: detect.DefaultRules(), Hooks: hooks,
		sem: make(chan struct{}, n), credit: float64(n), burst: float64(n), render: Render}, nil
}

// Observe counts one HTTP page load.
func (b *Bootstrapper) Observe() {
	b.mu.Lock()
	b.credit = min(b.credit+b.MaxShare, b.burst)
	b.mu.Unlock()
}

func (b *Bootstrapper) take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.credit < 1-1e-9 { // MaxShare sums are not exact in floating point
		return false
	}
	b.credit--
	return true
}

// Bootstrap renders rawURL for id. It returns a PageLoad whenever a render
// happened, with the outcome recorded on the identity (see
// session.BrowserFallback), and nil when it was capped or failed to run.
func (b *Bootstrapper) Bootstrap(ctx context.Context, id *session.Identity, rawURL string) (*session.PageLoad, error) {
	if b.Hooks.Extract == nil {
		return nil, session.ErrNoHooks
	}
	if !b.take() {
		metrics.Bootstraps.WithLabelValues("browser", "capped").Inc()
		return nil, ErrCapped
	}
	select {
	case b.sem <- struct{}{}:
	default:
		metrics.Bootstraps.WithLabelValues("browser", "busy").Inc()
		return nil, ErrBusy
	}
	defer func() { <-b.sem }()
	// One page load for the identity's pacing; the subrequests are the
	// page's own.
	if err := id.Wait(ctx); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, b.Timeout)
	defer cancel()
	opts, err := b.options(id, rawURL)
	if err != nil {
		return nil, err
	}
	opts.Capture = func(string) bool { return false }
	opts.Idle = 300 * time.Millisecond
	res, err := b.render(ctx, b.Binary, opts)
	if err != nil {
		metrics.Bootstraps.WithLabelValues("browser", "error").Inc()
		return nil, err
	}
	metrics.ProxyBytes.WithLabelValues(id.Endpoint.Tier, "in").Add(float64(res.Bytes))
	pl := &session.PageLoad{URL: res.URL, Status: res.Status, HTML: res.HTML, Bytes: res.Bytes}
	// The same classification as an HTTP load: a wall or a block is not a
	// bootstrap, even when the page carries an LSD token.
	cls, reason := b.Rules.Classify(detect.Input{Status: res.Status, Body: res.HTML, ContentType: "text/html", Expect: "html"})
	if u, err := url.Parse(res.URL); err == nil && b.Hooks.Wall(u.Path) {
		cls, reason = detect.LoginWall, "browser landed on "+u.Path
		for _, p := range b.Rules.CheckpointPaths {
			if strings.HasPrefix(u.Path, p) {
				cls = detect.Checkpoint
			}
		}
	}
	if cls != detect.OK {
		metrics.Bootstraps.WithLabelValues("browser", cls.String()).Inc()
		session.Outcome(id, cls)
		pl.Class, pl.Reason = cls, reason
		return pl, fmt.Errorf("browser: %s: %s", cls, reason)
	}
	for _, c := range res.Cookies {
		id.Doer.SetCookie(id.Base, c.Name, c.Value)
	}
	tok, ok := b.Hooks.Extract(res.HTML)
	if b.Hooks.AfterLoad != nil {
		b.Hooks.AfterLoad(ctx, id, res.URL, res.HTML, tok, false)
	}
	if !ok {
		metrics.Bootstraps.WithLabelValues("browser", "no_tokens").Inc()
		session.Outcome(id, detect.SoftError)
		pl.Class, pl.Reason = detect.SoftError, "no tokens after a browser render"
		return pl, errors.New(pl.Reason)
	}
	id.SetTokens(tok)
	session.Outcome(id, detect.OK)
	metrics.Bootstraps.WithLabelValues("browser", "ok").Inc()
	pl.Class = detect.OK
	return pl, nil
}

// options builds a render of rawURL as id: its exit, user agent and client
// hints, its cookies, media blocked.
func (b *Bootstrapper) options(id *session.Identity, rawURL string) (Options, error) {
	opts := Options{URL: rawURL, UserAgent: id.FP.UserAgent, AcceptLanguage: b.AcceptLanguage,
		Cookies: id.Doer.Cookies(id.Base), NoSandbox: b.NoSandbox, BlockMedia: true,
		Hints: HintsFrom(id.FP.SecChUa, id.FP.SecChUaPlatform, id.FP.SecChUaMobile)}
	if id.Endpoint.URL != "" {
		p, err := url.Parse(id.Endpoint.URL)
		if err != nil {
			return opts, fmt.Errorf("browser: proxy: %w", err)
		}
		opts.Proxy = p
	}
	return opts, nil
}

// Capture renders rawURL through id's exit and fingerprint, scrolling
// twice, and returns the GraphQL requests the page made. It shares the
// concurrency limit with bootstraps but not the share cap: callers use it
// rarely (doc_id discovery).
func (b *Bootstrapper) Capture(ctx context.Context, id *session.Identity, rawURL string) ([]Exchange, error) {
	select {
	case b.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-b.sem }()
	ctx, cancel := context.WithTimeout(ctx, b.Timeout)
	defer cancel()
	opts, err := b.options(id, rawURL)
	if err != nil {
		return nil, err
	}
	opts.Scrolls = 2
	res, err := b.render(ctx, b.Binary, opts)
	if err != nil {
		return nil, err
	}
	metrics.ProxyBytes.WithLabelValues(id.Endpoint.Tier, "in").Add(float64(res.Bytes))
	return res.Captured, nil
}
