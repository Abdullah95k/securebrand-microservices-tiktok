package app

import (
	"fmt"
	"net/url"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// Signer is a configured tks-signer process (M9d): the browser signer's
// pages in a service of their own, so a page that runs away takes down
// only this pod, never a crawler. It reads tt.signer for its pages
// whatever tt.signer.mode says (the callers' mode is remote).
type Signer struct {
	Pool   *sign.Pool
	Server *sign.Server
}

// SignerOptions customise construction (tests).
type SignerOptions struct {
	// Opener replaces Chromium's pages (tests).
	Opener sign.Opener
	// Self is this replica's own address, which the callers report
	// TikTok's answers to (the chart sets it from the pod's IP).
	Self string
}

// NewSigner builds tks-signer: the pool of tt.signer.pages Chromium pages,
// opened on the exits of tt.proxy (the callers' proxy file: an exit is
// named, never sent), behind the service's handler.
func NewSigner(cfg *config.Config, o SignerOptions) (*Signer, error) {
	tc := cfg.TT
	base, err := url.Parse(tc.BaseURL)
	if err != nil || base.Host == "" {
		return nil, fmt.Errorf("tt.base_url %q is not a site's URL", tc.BaseURL)
	}
	proxies, err := proxy.New(tc.Proxy)
	if err != nil {
		return nil, err
	}
	open := o.Opener
	if open == nil {
		open = sign.BrowserOpener(sign.BrowserConfig{Binary: tc.Browser.Binary, NoSandbox: tc.Browser.NoSandbox,
			AcceptLanguage: tc.AcceptLanguage, CallTimeout: tc.Signer.PageTimeout, Freeze: tc.Signer.Freeze, Lite: tc.Signer.Lite})
	}
	pool := sign.New(tc.Signer, open)
	return &Signer{Pool: pool, Server: &sign.Server{Signer: pool, Proxies: proxies, Base: base, Self: o.Self}}, nil
}

// Close closes the pages.
func (s *Signer) Close() { s.Pool.Close() }
