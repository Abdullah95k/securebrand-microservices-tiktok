package sign

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
)

// Agent is what a signer browser says it is, read from its own navigator:
// the values a call it signs carries (browser_version, browser_platform,
// browser_language, os) and the headers that call is then sent with, so
// the call and its signature describe the same browser.
type Agent struct {
	UserAgent  string `json:"user_agent"`
	AppVersion string `json:"app_version"`
	Platform   string `json:"platform"`
	Language   string `json:"language"`
	// Webdriver is navigator.webdriver, as the browser sets it.
	Webdriver bool `json:"webdriver"`
	// The client hints, from navigator.userAgentData; empty when the
	// browser has none.
	SecChUa         string `json:"sec_ch_ua,omitempty"`
	SecChUaMobile   string `json:"sec_ch_ua_mobile,omitempty"`
	SecChUaPlatform string `json:"sec_ch_ua_platform,omitempty"`
}

// EngineConfig is a signer browser that presents itself as itself: its
// own user agent and client hints, its automation flag as it sets it.
type EngineConfig struct {
	Engine         browser.Engine
	Binary         string
	NoSandbox      bool
	AcceptLanguage string
	CallTimeout    time.Duration
}

// EngineOpener opens signer pages in the configured browser without
// presenting it as another: no user agent or client hints of the
// identity's, the automation flag left as the browser sets it. Each page
// reaches the identity's exit through its own Forwarder (the browser's
// command line holds no credentials; the forwarder counts the page's
// proxy bytes), and signs with its own page values and navigator's, which
// the call's headers then match (EnginePage.Agent). This is the opener
// tks-spike signers compares browsers with; the crawler uses
// BrowserOpener.
func EngineOpener(c EngineConfig) Opener {
	return func(ctx context.Context, id *session.Identity, pageURL string) (Page, error) {
		var exit *url.URL
		if id.Endpoint.URL != "" {
			u, err := url.Parse(id.Endpoint.URL)
			if err != nil {
				return nil, fmt.Errorf("sign: exit: %w", err)
			}
			exit = u
		}
		fwd, err := browser.NewForwarder(exit)
		if err != nil {
			return nil, err
		}
		s, err := browser.OpenSession(ctx, c.Binary, browser.Options{URL: pageURL, Proxy: fwd.URL, AcceptLanguage: c.AcceptLanguage,
			BlockMedia: true, NoSandbox: c.NoSandbox, MaxSettle: 15 * time.Second, CallTimeout: c.CallTimeout, Engine: c.Engine})
		if err != nil {
			_ = fwd.Close()
			return nil, err
		}
		pg := &EnginePage{browserPage: browserPage{Session: s}, fwd: fwd}
		fail := func(err error) (Page, error) {
			pg.Close()
			return nil, err
		}
		if pg.values, err = pageValues(ctx, s); err != nil {
			return fail(err)
		}
		if pg.agent, err = readAgent(ctx, s); err != nil {
			return fail(err)
		}
		pg.values["browser_version"] = pg.agent.AppVersion
		pg.values["browser_platform"] = pg.agent.Platform
		pg.values["os"] = osOf(pg.agent.Platform)
		if pg.agent.Language != "" {
			pg.values["browser_language"] = pg.agent.Language
		}
		return pg, nil
	}
}

// EnginePage is a page opened by EngineOpener.
type EnginePage struct {
	browserPage
	agent Agent
	fwd   *browser.Forwarder
}

// Agent is what the page's browser says it is.
func (p *EnginePage) Agent() Agent { return p.agent }

// ProxyBytes is what the page moved through the identity's exit so far.
func (p *EnginePage) ProxyBytes() int64 { return p.fwd.Bytes() }

// PageFetch has the page send the call itself, signed by its script as
// Sign would sign it, and returns TikTok's answer.
func (p *EnginePage) PageFetch(ctx context.Context, rawURL string) (*browser.Fetched, error) {
	return p.Fetch(ctx, PageQuery(rawURL, p.values))
}

// Close ends the browser and its forwarder.
func (p *EnginePage) Close() {
	p.Session.Close()
	_ = p.fwd.Close()
}

func readAgent(ctx context.Context, s *browser.Session) (Agent, error) {
	v, err := s.Evaluate(ctx, `(() => { const d = navigator.userAgentData;
	  return {ua: navigator.userAgent, av: navigator.appVersion, p: navigator.platform, l: navigator.language, wd: !!navigator.webdriver,
	    brands: d ? d.brands.map(b => ({brand: b.brand, version: b.version})) : null, mobile: d ? d.mobile : false, platform: d ? d.platform : ""}; })()`)
	if err != nil {
		return Agent{}, fmt.Errorf("sign: navigator: %w", err)
	}
	var n struct {
		UA, AV, P, L string
		WD           bool
		Brands       []struct{ Brand, Version string }
		Mobile       bool
		Platform     string
	}
	if err := json.Unmarshal(v, &n); err != nil || n.UA == "" {
		return Agent{}, fmt.Errorf("sign: navigator: %s", v)
	}
	a := Agent{UserAgent: n.UA, AppVersion: n.AV, Platform: n.P, Language: n.L, Webdriver: n.WD}
	if a.AppVersion == "" {
		a.AppVersion = strings.TrimPrefix(n.UA, "Mozilla/")
	}
	if len(n.Brands) > 0 {
		parts := make([]string, 0, len(n.Brands))
		for _, b := range n.Brands {
			parts = append(parts, strconv.Quote(b.Brand)+";v="+strconv.Quote(b.Version))
		}
		a.SecChUa = strings.Join(parts, ", ")
		a.SecChUaMobile = "?0"
		if n.Mobile {
			a.SecChUaMobile = "?1"
		}
		a.SecChUaPlatform = strconv.Quote(n.Platform)
	}
	return a, nil
}

// osOf is the web client's os parameter for a navigator.platform.
func osOf(platform string) string {
	p := strings.ToLower(platform)
	switch {
	case strings.HasPrefix(p, "win"):
		return "windows"
	case strings.HasPrefix(p, "mac"):
		return "mac"
	case strings.Contains(p, "linux"):
		return "linux"
	}
	return p
}

// Headers returns hdr with the user agent and client hints replaced by
// a's: a call signed by a page is sent as the page's browser.
func (a Agent) Headers(hdr [][2]string) [][2]string {
	out := make([][2]string, 0, len(hdr))
	for _, h := range hdr {
		switch h[0] {
		case "user-agent":
			h[1] = a.UserAgent
		case "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform":
			v := map[string]string{"sec-ch-ua": a.SecChUa, "sec-ch-ua-mobile": a.SecChUaMobile, "sec-ch-ua-platform": a.SecChUaPlatform}[h[0]]
			if v == "" {
				continue // the browser sends none
			}
			h[1] = v
		}
		out = append(out, h)
	}
	return out
}

// Agents keeps the agent of the pages an opener opens (one browser's: its
// pages all say the same), for the calls they sign.
type Agents struct {
	mu sync.Mutex
	a  *Agent
}

// Opener wraps open: each page that says what it is (EnginePage) records
// its agent.
func (l *Agents) Opener(open Opener) Opener {
	return func(ctx context.Context, id *session.Identity, pageURL string) (Page, error) {
		pg, err := open(ctx, id, pageURL)
		if a, ok := pg.(interface{ Agent() Agent }); ok && err == nil {
			ag := a.Agent()
			l.mu.Lock()
			l.a = &ag
			l.mu.Unlock()
		}
		return pg, err
	}
}

// Agent returns the agent recorded last.
func (l *Agents) Agent() (Agent, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.a == nil {
		return Agent{}, false
	}
	return *l.a, true
}

// AsAgent is the client's signer (ttclient.SignerFor) with the calls it
// signs sent as the signing browser: its user agent and client hints in
// place of the identity's.
type AsAgent struct {
	Signer graphql.Signer
	Agents *Agents
}

// Sign implements graphql.Signer.
func (s AsAgent) Sign(ctx context.Context, id *session.Identity, q registry.Query, rawURL string, hdr [][2]string) (string, [][2]string, error) {
	u, h, err := s.Signer.Sign(ctx, id, q, rawURL, hdr)
	if err != nil || !ttclient.Signed(q) {
		return u, h, err
	}
	if a, ok := s.Agents.Agent(); ok {
		h = a.Headers(h)
	}
	return u, h, nil
}
