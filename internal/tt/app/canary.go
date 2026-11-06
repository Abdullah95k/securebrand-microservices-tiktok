package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/canary"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// Canary is a configured tks-canary process.
type Canary struct {
	*canary.Canary
	// Registry is the operation registry the canary changes; Reload it
	// before a run so the client parameters are current.
	Registry *registry.DB
	// Signer is the browser signer's pool (nil with tt.signer.mode none or
	// a test signer); Close closes its pages.
	Signer *sign.Pool
}

// CanaryOptions customise construction (tests).
type CanaryOptions struct {
	DoerFactory session.DoerFactory // nil = fingerprinted tls-client
	// Signer replaces the browser signer's pages (tests; tt.signer.mode
	// still decides whether calls are signed).
	Signer ttclient.URLSigner
}

// NewCanary builds tks-canary on schema tt (db: a pool from ttstore.Open)
// with the crawler's client: its fingerprints, proxies, detection rules,
// builder, decoder and browser signer, so a variant the canary passes
// answers the crawler the same way.
func NewCanary(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, o CanaryOptions) (*Canary, error) {
	tc := cfg.TT
	if len(tc.Canary.Accounts) == 0 {
		return nil, errors.New("tt.canary.accounts is empty: set public accounts with posts, comments and replies (TKS_TT__CANARY__ACCOUNTS)")
	}
	seedBytes, err := configs.FS.ReadFile(tc.OperationSeed)
	if err != nil {
		return nil, fmt.Errorf("operation seed: %w", err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, err
	}
	reg, err := registry.NewDBFrom(ctx, db, ttstore.Registry, seed)
	if err != nil {
		return nil, err
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, err
	}
	proxies, err := proxy.New(tc.Proxy)
	if err != nil {
		return nil, err
	}
	// The crawler's first tier by default (stage.Deps.tierFor), so the
	// canary is answered as the crawler is.
	tier := tc.Canary.Tier
	if tier == "" {
		tier = "direct"
		if ts := proxies.TiersFor("default"); len(ts) > 0 {
			tier = ts[0]
		}
	}
	pool, err := session.NewPool(session.PoolConfig{
		BaseURL: tc.BaseURL, Profile: tc.FingerprintProfile, Profiles: tc.FingerprintProfiles,
		Timeout:     tc.RequestTimeout,
		IdentityRPS: tc.IdentityRPS, IdentityBurst: tc.IdentityBurst,
		MaxRequestsMin: tc.IdentityMaxRequestsMin, MaxRequestsMax: tc.IdentityMaxRequestsMax,
		MaxPerTier: 4,
	}, proxies, fps, o.DoerFactory)
	if err != nil {
		return nil, err
	}
	rules, err := ttdetect.Rules().Apply(detect.Overrides(tc.Detect))
	if err != nil {
		return nil, fmt.Errorf("tt.detect: %w", err)
	}
	loader := &session.Loader{Rules: rules, AcceptLanguage: tc.AcceptLanguage, Hooks: ttclient.Hooks(), Gate: proxies}
	if tc.Browser.Enabled {
		b, err := browser.NewBootstrapper(tc.Browser, tc.AcceptLanguage, ttclient.Hooks())
		if err != nil {
			return nil, fmt.Errorf("tt.browser is enabled: %w", err)
		}
		b.Rules = rules
		loader.Browser = b
	}
	c := &Canary{Registry: reg}
	signer := o.Signer
	if signer == nil && tc.Signer.Mode == config.SignerRemote {
		// tks-signer's pages (M9d): this process runs no browser.
		signer = sign.NewRemote(tc.Signer.RemoteURL, tc.Signer.SignTimeout, tc.Signer.PageTimeout)
	}
	if signer == nil && tc.Signer.Mode == config.SignerBrowser {
		// The canary's own pages, capped like a crawler's: it checks the
		// signer the crawler runs.
		c.Signer = sign.New(tc.Signer, sign.BrowserOpener(sign.BrowserConfig{Binary: tc.Browser.Binary, NoSandbox: tc.Browser.NoSandbox,
			AcceptLanguage: tc.AcceptLanguage, CallTimeout: tc.Signer.PageTimeout, Freeze: tc.Signer.Freeze, Lite: tc.Signer.Lite}))
		signer = c.Signer
	}
	c.Canary = &canary.Canary{
		DB: db, Cfg: tc.Canary, Tier: tier, Pool: pool, Loader: loader, Rules: rules,
		Builder: ttclient.Builder{AcceptLanguage: tc.AcceptLanguage}, Decode: ttclient.Decode,
		Params:       reg.ClientParams,
		ListPageSize: tc.ListPageSize, CommentsPageSize: tc.CommentsPageSize, RepliesPageSize: tc.RepliesPageSize,
	}
	if signer != nil {
		c.Canary.Signer = ttclient.SignerFor(tc.Signer.Mode, signer)
	}
	return c, nil
}

// Close closes the signer's pages.
func (c *Canary) Close() {
	if c.Signer != nil {
		c.Signer.Close()
	}
}
