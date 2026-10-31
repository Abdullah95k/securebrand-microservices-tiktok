package app

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	fbclient "github.com/abdullah95k/securebrand-microservices-igs/internal/fb/client"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/media"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/registrysync"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// NewRegistrySync builds the registry sync: its own small identity pool
// on the configured tier (by default the first tier routed for the
// "registry_sync" class), and the registry for the client parameters.
// Callers run reg.Reload before each sync, or reg.Run alongside.
func NewRegistrySync(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, doers session.DoerFactory) (*registrysync.Sync, *registry.DB, error) {
	seedBytes, err := configs.FS.ReadFile(cfg.FB.RegistrySeed)
	if err != nil {
		return nil, nil, fmt.Errorf("registry seed: %w", err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, nil, err
	}
	reg, err := registry.NewDB(ctx, db, seed)
	if err != nil {
		return nil, nil, err
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, nil, err
	}
	proxies, err := proxy.New(cfg.FB.Proxy)
	if err != nil {
		return nil, nil, err
	}
	rc := cfg.RegistrySync
	if rc.Tier == "" {
		rc.Tier = proxies.TiersFor("registry_sync")[0]
	}
	pool, err := session.NewPool(session.PoolConfig{
		BaseURL: cfg.FB.BaseURL, Profile: cfg.FB.FingerprintProfile, Timeout: cfg.FB.RequestTimeout,
		IdentityRPS: cfg.FB.IdentityRPS, IdentityBurst: cfg.FB.IdentityBurst,
		MaxRequestsMin: cfg.FB.IdentityMaxRequestsMin, MaxRequestsMax: cfg.FB.IdentityMaxRequestsMax,
		MaxPerTier: len(rc.CanaryPages) + 2,
	}, proxies, fps, doers)
	if err != nil {
		return nil, nil, err
	}
	rules := detect.DefaultRules()
	s := &registrysync.Sync{
		DB: db, Cfg: rc, Pool: pool, Rules: rules, Params: reg.ClientParams,
		Loader:         &session.Loader{Rules: rules, AcceptLanguage: cfg.FB.AcceptLanguage, Hooks: fbclient.Hooks(cfg.FB.AcceptLanguage)},
		AcceptLanguage: cfg.FB.AcceptLanguage, Locale: cfg.FB.Locale,
		Fetch: docids.HTTPFetcher(media.NewHTTPClient()),
	}
	if cfg.FB.Browser.Enabled {
		b, err := browser.NewBootstrapper(cfg.FB.Browser, cfg.FB.AcceptLanguage, fbclient.Hooks(cfg.FB.AcceptLanguage))
		if err != nil {
			return nil, nil, fmt.Errorf("fb.browser is enabled: %w", err)
		}
		s.Loader.Browser = b
		s.Render = b.Capture
	}
	return s, reg, nil
}
