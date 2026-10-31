package session

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// PageLoad is the result of loading a page's HTML.
type PageLoad struct {
	URL    string
	Status int
	HTML   []byte
	Class  detect.Class
	Reason string
	Bytes  int64
}

// ErrBootstrap means no usable tokens could be obtained.
var ErrBootstrap = errors.New("session: bootstrap failed")

// ErrNoHooks means a loader or bootstrapper was built without its
// platform's Extract hook.
var ErrNoHooks = errors.New("session: no Extract hook: the platform's page hooks are not set")

// ErrTransport marks a page load that got no response at all (a timeout, a
// refused or reset connection): the exit failed, not the platform.
var ErrTransport = errors.New("transport")

// BrowserFallback renders a page in a real browser for an identity whose
// HTML load carried no tokens (a page that needs JavaScript first), and
// installs the cookies and tokens it found. Observe counts the HTTP loads
// the fallback's share cap is measured against. A non-nil PageLoad means a
// render happened and its outcome is already recorded on the identity; a
// nil one (capped, busy, no browser) leaves the HTTP load's outcome to the
// caller.
type BrowserFallback interface {
	Observe()
	Bootstrap(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error)
}

// Hooks are what the loader needs to know about a platform's pages.
type Hooks struct {
	// Extract returns the tokens a page carries and whether they are
	// enough to send queries. Required.
	Extract func(html []byte) (tok Tokens, ok bool)
	// IsWall reports a redirect path that leads to a login or checkpoint
	// wall; the loader does not follow it, so the redirect is classified.
	IsWall func(path string) bool
	// AfterLoad, when set, runs on every good page before its tokens are
	// checked. It may set cookies, or answer an interstitial (Facebook's
	// cookie-consent wall) and return true to load the page again;
	// canReload is false when the loader has no redirects left, or after a
	// browser render.
	AfterLoad func(ctx context.Context, id *Identity, page string, html []byte, tok Tokens, canReload bool) (reload bool)
}

// Wall reports whether path is a wall under h.
func (h Hooks) Wall(path string) bool { return h.IsWall != nil && h.IsWall(path) }

// Loader loads a platform's HTML pages for an identity and refreshes its
// tokens from them. Every page load doubles as a bootstrap.
type Loader struct {
	Rules          detect.Rules
	AcceptLanguage string
	Hooks          Hooks
	// Browser, when set, bootstraps identities whose HTML carries no
	// tokens (fb.browser).
	Browser BrowserFallback
}

// NavHeaders are Chrome's top-level navigation headers, in wire order.
func NavHeaders(fp Fingerprint, acceptLanguage, secFetchSite string) [][2]string {
	return [][2]string{
		{"sec-ch-ua", fp.SecChUa},
		{"sec-ch-ua-mobile", fp.SecChUaMobile},
		{"sec-ch-ua-platform", fp.SecChUaPlatform},
		{"upgrade-insecure-requests", "1"},
		{"user-agent", fp.UserAgent},
		{"accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
		{"sec-fetch-site", secFetchSite},
		{"sec-fetch-mode", "navigate"},
		{"sec-fetch-user", "?1"},
		{"sec-fetch-dest", "document"},
		{"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", acceptLanguage},
		{"priority", "u=0, i"},
	}
}

// Load fetches rawURL, follows same-site redirects (locale, consent) other
// than walls, runs the platform's AfterLoad hook (Facebook accepts the
// cookie-consent wall there), and installs the tokens found in the HTML on
// id.
func (l *Loader) Load(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error) {
	if l.Hooks.Extract == nil {
		return nil, ErrNoHooks
	}
	target := rawURL
	site := "none"
	var total int64
	for hop := 0; hop < 4; hop++ {
		if err := id.Wait(ctx); err != nil {
			return nil, err
		}
		resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: target,
			Header: NavHeaders(id.FP, l.AcceptLanguage, site)})
		if err != nil {
			if ctx.Err() == nil { // a cancelled load says nothing about the exit
				Outcome(id, detect.Transport)
			}
			metrics.FBRequests.WithLabelValues("page_html", detect.Transport.String(), id.Endpoint.Tier).Inc()
			return nil, fmt.Errorf("session: load %s: %w: %w", target, ErrTransport, err)
		}
		total += resp.BytesIn + resp.BytesOut
		metrics.ProxyBytes.WithLabelValues(id.Endpoint.Tier, "in").Add(float64(resp.BytesIn))
		metrics.ProxyBytes.WithLabelValues(id.Endpoint.Tier, "out").Add(float64(resp.BytesOut))

		if resp.Status >= 300 && resp.Status < 400 && resp.Location != "" {
			next, err := id.Base.Parse(resp.Location)
			if err == nil && sameSite(id.Base, next) && !l.Hooks.Wall(next.Path) {
				target, site = next.String(), "same-origin"
				continue
			}
		}
		cls, reason := l.Rules.Classify(detect.Input{Status: resp.Status, Location: resp.Location,
			ContentType: resp.Header.Get("Content-Type"), Body: resp.Body, Expect: "html"})
		metrics.FBRequests.WithLabelValues("page_html", cls.String(), id.Endpoint.Tier).Inc()
		if l.Browser != nil {
			l.Browser.Observe()
		}
		pl := &PageLoad{URL: target, Status: resp.Status, HTML: resp.Body, Class: cls, Reason: reason, Bytes: total}
		if cls != detect.OK {
			Outcome(id, cls)
			return pl, nil
		}
		tok, ok := l.Hooks.Extract(resp.Body)
		if l.Hooks.AfterLoad != nil && l.Hooks.AfterLoad(ctx, id, target, resp.Body, tok, hop < 3) {
			site = "same-origin"
			continue
		}
		if !ok {
			metrics.Bootstraps.WithLabelValues("http", "no_tokens").Inc()
			if l.Browser != nil {
				bpl, err := l.Browser.Bootstrap(ctx, id, target)
				if bpl != nil {
					bpl.Bytes += total
					return bpl, nil
				}
				if err != nil {
					pl.Reason = "no tokens in html; browser: " + err.Error()
				}
			}
			Outcome(id, detect.SoftError)
			pl.Class = detect.SoftError
			if pl.Reason == "" {
				pl.Reason = "no tokens in html"
			}
			return pl, nil
		}
		id.SetTokens(tok)
		Outcome(id, detect.OK)
		metrics.Bootstraps.WithLabelValues("http", "ok").Inc()
		return pl, nil
	}
	return nil, fmt.Errorf("%w: too many redirects for %s", ErrBootstrap, rawURL)
}

// Bootstrap loads url (usually the target page) with retries on fresh
// identities handled by the caller. It returns ErrBootstrap if the identity
// still has no tokens.
func (l *Loader) Bootstrap(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error) {
	pl, err := l.Load(ctx, id, rawURL)
	if err != nil {
		return nil, err
	}
	if !id.Bootstrapped() {
		return pl, fmt.Errorf("%w: %s (%s)", ErrBootstrap, pl.Class, pl.Reason)
	}
	return pl, nil
}

func sameSite(base, u *url.URL) bool {
	return u.Host == "" || u.Host == base.Host || strings.HasSuffix(u.Hostname(), "."+strings.TrimPrefix(base.Hostname(), "www."))
}

// Origin is u's scheme and host, as browsers send it in the origin header.
func Origin(u *url.URL) string { return u.Scheme + "://" + u.Host }
