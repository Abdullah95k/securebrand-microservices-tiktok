package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/phase0"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// Engine names for --engines: browsers that present themselves as
// themselves (S1: TikTok refused them all), and the crawler's own signer
// (sign.BrowserOpener: Chrome as the identity, its automation hidden) as
// it runs today and with each cure of its memory runaway.
const (
	engLightpanda    = "lightpanda"
	engHeadlessShell = "headless-shell"
	engChrome        = "chrome"

	engCrawler        = "crawler"         // as C4 ran it
	engCrawlerFreeze  = "crawler-freeze"  // frozen between signatures
	engCrawlerLite    = "crawler-lite"    // no autoplay, no images
	engCrawlerProfile = "crawler-profile" // pages open on the profile page
	engCrawlerFixed   = "crawler-fixed"   // all three, and the safety nets
)

// runSigners compares signer browsers (docs/runbooks/tks-signers.md): each
// presents itself as itself, works a few accounts' comments, idles, and is
// measured, under a memory guard.
func runSigners(args []string) int {
	home, _ := os.UserHomeDir()
	fs := flag.NewFlagSet("tks-spike signers", flag.ExitOnError)
	accountsFile := fs.String("accounts", "", "file with one public account per line")
	maxAccounts := fs.Int("max-accounts", 5, "accounts each browser works, spread evenly over the file")
	cfgPath := fs.String("config", os.Getenv("TKS_CONFIG"), "config file with proxy tiers under fb.proxy (phase0/proxies.yaml)")
	tier := fs.String("tier", "isp", "proxy tier")
	regPath := fs.String("registry", "phase0/out-tt-phase0/registry.yaml", "the Phase 0 variants step's registry (the variants that answer today)")
	outDir := fs.String("out-dir", "phase0/out-tt-signers", "output directory")
	engines := fs.String("engines", strings.Join([]string{engCrawler, engCrawlerFreeze, engCrawlerLite, engCrawlerProfile, engCrawlerFixed}, ","),
		"browsers to compare, in order: "+strings.Join([]string{engCrawler, engCrawlerFreeze, engCrawlerLite, engCrawlerProfile, engCrawlerFixed,
			engLightpanda, engHeadlessShell, engChrome}, ", "))
	lpBin := fs.String("lightpanda", filepath.Join(home, "tks-browsers", "lightpanda"), "Lightpanda binary")
	hsBin := fs.String("headless-shell", filepath.Join(home, "tks-browsers", "chrome-headless-shell-linux64", "chrome-headless-shell"), "chrome-headless-shell binary")
	chromeBin := fs.String("chrome", "", "Chrome or Chromium binary (default: the one tks-spike browser-check finds)")
	commentPages := fs.Int("comment-pages", 3, "comment pages read per account (one post each; 20 comments a page)")
	replyPages := fs.Int("reply-pages", 1, "reply pages read on the post's first thread with replies")
	idle := fs.Duration("idle", 3*time.Minute, "how long each browser's page stays open, unused, after its accounts")
	fetchEvery := fs.Int("fetch-every", 3, "the page also sends the call itself on its first signature and every this many after (0 = never)")
	maxBrowser := fs.Float64("max-browser-mb", 1200, "stop a browser when its processes pass this much memory")
	minHost := fs.Float64("min-host-mb", 1500, "stop a browser when the machine has less memory available than this")
	pause := fs.Duration("pause", 15*time.Second, "wait between browsers, for memory to settle")
	fingerprint := fs.String("fingerprint", "", "fingerprint profile of the plain-HTTP identity (default tt.fingerprint_profile)")
	dryRun := fs.Bool("dry-run", false, "rehearse against a built-in fake TikTok (no traffic, no proxies)")
	_ = fs.Parse(args)

	var cfg *config.Config
	var err error
	if *dryRun {
		cfg, err = config.Load("")
	} else {
		cfg, err = config.Load(*cfgPath)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "signers: config:", err)
		return 1
	}
	if *fingerprint == "" {
		*fingerprint = cfg.TT.FingerprintProfile
	}
	base, tiers, rps := cfg.TT.BaseURL, []string{*tier}, cfg.TT.IdentityRPS
	var names []string
	var reg *registry.Seed
	if *dryRun {
		w := fakett.NewWorld(fakett.DefaultSpec())
		hs := httptest.NewServer(fakett.NewServer(w, fakett.Options{Faults: fakett.Faults{RequireSignature: true,
			SignedPaths: []string{"/api/comment/list/", "/api/comment/list/reply/"}}}))
		defer hs.Close()
		base, tiers, rps = hs.URL, []string{"direct"}, 10
		for _, a := range w.Accounts() {
			names = append(names, a.Username)
		}
		names = names[:min(*maxAccounts, len(names))]
		if *outDir == "phase0/out-tt-signers" {
			*outDir = "phase0/out-tt-signers-dryrun"
		}
		_ = os.RemoveAll(*outDir)
		*idle, *pause = 5*time.Second, time.Second
		cfg.FB.Proxy = config.Proxy{}
	} else {
		if *accountsFile == "" {
			fmt.Fprintln(os.Stderr, "signers: --accounts is required (or --dry-run)")
			return 2
		}
		all, err := readAccounts(*accountsFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "signers:", err)
			return 1
		}
		names = spread(all, *maxAccounts)
		b, err := os.ReadFile(*regPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "signers: --registry: %v (the Phase 0 variants step writes it: make tks-phase0 TKS_PHASE0_ARGS='--from variants')\n", err)
			return 1
		}
		s, err := registry.ParseSeed(b)
		if err != nil {
			fmt.Fprintln(os.Stderr, "signers: --registry:", err)
			return 1
		}
		reg = &s
	}
	noSandbox := os.Getenv("TKS_BROWSER_NO_SANDBOX") != "" || os.Geteuid() == 0
	var engs []phase0.SignerEngine
	for _, name := range splitList(*engines) {
		if strings.HasPrefix(name, engCrawler) {
			bin, err := browser.FindBinary(*chromeBin)
			if err != nil {
				fmt.Fprintln(os.Stderr, "signers: chrome:", err)
				return 1
			}
			bc := sign.BrowserConfig{Binary: bin, NoSandbox: noSandbox, AcceptLanguage: cfg.TT.AcceptLanguage, CallTimeout: 30 * time.Second}
			var tune func(*config.Signer)
			switch name {
			case engCrawler:
			case engCrawlerFreeze:
				bc.Freeze = true
			case engCrawlerLite:
				bc.Lite = true
			case engCrawlerProfile:
				tune = func(c *config.Signer) { c.OpenOn = config.SignerOpenOnProfile }
			case engCrawlerFixed:
				bc.Freeze, bc.Lite = true, true
				tune = func(c *config.Signer) {
					c.OpenOn, c.IdleClose, c.MaxPageMB = config.SignerOpenOnProfile, 2*time.Minute, 800
				}
			default:
				fmt.Fprintf(os.Stderr, "signers: unknown engine %q\n", name)
				return 2
			}
			engs = append(engs, phase0.SignerEngine{Name: name, Opener: sign.BrowserOpener(bc), Tune: tune})
			continue
		}
		ec := sign.EngineConfig{NoSandbox: noSandbox, AcceptLanguage: cfg.TT.AcceptLanguage, CallTimeout: 30 * time.Second}
		switch name {
		case engLightpanda:
			ec.Binary, ec.Engine = *lpBin, browser.EngineLightpanda
		case engHeadlessShell:
			ec.Binary = *hsBin
		case engChrome:
			bin, err := browser.FindBinary(*chromeBin)
			if err != nil {
				fmt.Fprintln(os.Stderr, "signers: chrome:", err)
				return 1
			}
			ec.Binary = bin
		default:
			fmt.Fprintf(os.Stderr, "signers: unknown engine %q (%s, %s, %s)\n", name, engLightpanda, engHeadlessShell, engChrome)
			return 2
		}
		if _, err := os.Stat(ec.Binary); err != nil {
			if *dryRun {
				fmt.Fprintf(os.Stderr, "signers: %s: no binary at %s; skipped in the dry run\n", name, ec.Binary)
				continue
			}
			fmt.Fprintf(os.Stderr, "signers: %s: no binary at %s (run: make tks-signers-browsers)\n", name, ec.Binary)
			return 1
		}
		engs = append(engs, phase0.SignerEngine{Name: name, Opener: sign.EngineOpener(ec)})
	}
	if len(engs) == 0 {
		fmt.Fprintln(os.Stderr, "signers: no browser to compare")
		return 1
	}
	if avail := phase0.HostAvailMB(); avail > 0 && avail < *minHost {
		fmt.Fprintf(os.Stderr, "signers: the machine has %.0f MB available, under --min-host-mb %.0f: not starting\n", avail, *minHost)
		return 1
	}
	if _, limit := phase0.CgroupMemory(); limit == 0 && !*dryRun {
		fmt.Fprintln(os.Stderr, "signers: WARNING: not under a memory cap (run it with make tks-signers, which caps it); the guard alone protects the machine")
	}
	pool, err := identityPool(cfg.FB.Proxy, base, *fingerprint, rps, *maxAccounts+4)
	if err != nil {
		fmt.Fprintln(os.Stderr, "signers:", err)
		return 1
	}
	seed, err := ttclient.Seed()
	if err != nil {
		fmt.Fprintln(os.Stderr, "signers:", err)
		return 1
	}
	k := &phase0.Kit{
		Stack: phase0.Stack{BaseURL: base, Pool: pool, HTTP: &http.Client{Timeout: 60 * time.Second}, Rules: ttdetect.Rules(),
			Loader: &session.Loader{Rules: ttdetect.Rules(), AcceptLanguage: cfg.TT.AcceptLanguage, Hooks: ttclient.Hooks()}},
		Cfg: phase0.Config{Out: *outDir, Tiers: tiers, Accounts: names, Seed: seed, Registry: reg, IdentityRPS: rps,
			AcceptLanguage: cfg.TT.AcceptLanguage, ConfigPath: *cfgPath, Secrets: proxySecrets(cfg.FB.Proxy), Version: version},
		Log: os.Stderr,
	}
	sc := phase0.SignersConfig{Engines: engs, Accounts: len(names), Idle: *idle, FetchEvery: *fetchEvery, Pause: *pause,
		Limits: phase0.Limits{BackfillDays: 365, MaxListPages: 0, CommentPosts: 1, MaxCommentPages: *commentPages,
			ReplyThreads: 1, MaxReplyPages: *replyPages},
		Guard:  phase0.MemGuard{MaxBrowserMB: *maxBrowser, MinHostMB: *minHost, CgroupShare: 0.9},
		Signer: config.Signer{Pages: 1, SignTimeout: 30 * time.Second, PageTimeout: 60 * time.Second}}
	if *dryRun {
		sc.SampleEvery = time.Second
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "signers: %d browser(s), %d account(s) each, tier %s, %s idle, out %s; Ctrl-C stops\n",
		len(engs), len(names), tiers[0], *idle, *outDir)
	rep, err := k.Signers(ctx, sc)
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "\nsigners:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "\nsigners: %d browser(s) compared. Send %s/%s back.\n", len(rep.Engines), *outDir, phase0.FileSignersBundle)
	if err != nil {
		return 1
	}
	return 0
}

// proxySecrets are the configured proxies' user names and passwords, which
// a bundle must never hold.
func proxySecrets(pc config.Proxy) []string {
	var out []string
	for _, t := range pc.Tiers {
		out = append(out, t.Username, t.Password)
		for _, ep := range t.Endpoints {
			if u, err := url.Parse(ep); err == nil && u.User != nil {
				p, _ := u.User.Password()
				out = append(out, u.User.Username(), p)
			}
		}
	}
	return out
}
