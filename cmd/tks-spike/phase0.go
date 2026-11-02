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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/phase0"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// DryRunFaults is the dry run's fake TikTok: like the recordings and the
// probe's runs (comments and replies need a signature, videos the page's
// cookie), with the variants rotated (the creator list now needs a
// signature too, so the seed's unsigned variant is stale and the variants
// step must find the signed one), TikTok's occasional list error, and an
// IP that tolerates 60 requests a second, for the ladder.
func DryRunFaults() fakett.Faults {
	return fakett.Faults{RequireSignature: true, MediaNeedsCookie: true, MaxRPS: 60, ListErrorEvery: 40,
		SignedPaths: []string{"/api/post/item_list/", "/api/creator/item_list/", "/api/comment/list/", "/api/comment/list/reply/"}}
}

// simulatedPage signs as fakett's page script does: the dry run's signer
// when no Chromium is installed.
type simulatedPage struct{}

func (simulatedPage) Sign(_ context.Context, raw string) (string, error) { return fakett.Sign(raw) }
func (simulatedPage) Close()                                             {}

func runPhase0(args []string) int {
	fs := flag.NewFlagSet("tks-spike phase0", flag.ExitOnError)
	accountsFile := fs.String("accounts", "", "file with one public account per line (username, @username or profile link)")
	maxAccounts := fs.Int("max-accounts", 150, "accounts to measure, spread evenly over the file (0 = all)")
	cfgPath := fs.String("config", os.Getenv("TKS_CONFIG"), "config file with proxy tiers under fb.proxy (phase0/proxies.yaml)")
	tiers := fs.String("tiers", "isp", "proxy tiers to measure, comma-separated (direct = this machine's own IP)")
	outDir := fs.String("out-dir", "phase0/out-tt-phase0", "output directory")
	from := fs.String("from", "", "first step to run: "+strings.Join(phase0.Steps, ", ")+" (resume after a stop)")
	parallel := fs.Int("parallel", 10, "accounts measured at once, each on its own identity (at most the tier's IP count)")
	rps := fs.Float64("rps", 0, "requests per second per identity (default tt.identity_rps: the crawler's pace)")
	fingerprint := fs.String("fingerprint", "", "browser fingerprint profile (default tt.fingerprint_profile)")
	signer := fs.String("signer", "browser", `"browser" signs in headless Chromium as the crawler does; "none" signs nothing (comments are then refused)`)
	signerPages := fs.Int("signer-pages", 2, "signer pages open at once")
	backfill := fs.Int("backfill-days", 365, "the backfill window")
	maxList := fs.Int("max-list-pages", 30, "list pages read per account after the first (15 posts each)")
	commentPosts := fs.Int("comment-posts", 3, "posts per account whose comments are read whole")
	maxComment := fs.Int("max-comment-pages", 10, "top-level comment pages per post (20 each)")
	replyThreads := fs.Int("reply-threads", 3, "threads per post whose replies are read")
	maxReply := fs.Int("max-reply-pages", 5, "reply pages per thread (3 each)")
	videos := fs.Int("videos", 5, "accounts (the first ones) that download one video, through the exit and from this IP (T33)")
	replays := fs.Int("replays", 20, "accounts (the first ones) whose first signed comment URL is sent again later (T27)")
	replayAges := fs.String("replay-ages", "2m,10m,30m,60m", "ages at which those URLs are sent again")
	replayWait := fs.Duration("replay-wait", time.Hour, "how long the run waits at its end for replays still due")
	maxCaptchas := fs.Int("max-captchas", 10, "stop the run after this many captchas, so a bad exit list does not burn IPs")
	ladderRates := fs.String("ladder-rates", "0.33,0.66,1,2,4", "ladder steps: requests per second through one IP")
	ladderStep := fs.Duration("ladder-step", 10*time.Minute, "how long each ladder step lasts")
	ladderIPs := fs.Int("ladder-ips", 2, "exit IPs that climb the ladder (they may be refused for a while after its top)")
	ipMonth := fs.Float64("ip-month-usd", 0, "price of one ISP IP a month (0 = not priced)")
	browserHour := fs.Float64("browser-hour-usd", 0, "price of one signer page's hour (0 = not priced)")
	compute := fs.Float64("compute-usd", 0, "compute per account per day, besides the browser (0 = not priced)")
	storage := fs.Float64("storage-gb-month-usd", 0, "Postgres storage per GB-month (0 = not priced)")
	growth := fs.Float64("comment-growth", 1, "share of a post's re-read chances at which its comment count rose (1 = the upper bound)")
	mediaOn := fs.Bool("media", false, "count video downloads in the cost (D5: off today)")
	budget := fs.Float64("budget-usd", 0, "the budget per account per day (D9; 0 = not set yet)")
	target := fs.Int("target-accounts", 300_000, "fleet size the budget applies to")
	base := fs.String("base", "", "TikTok base URL (default tt.base_url)")
	dryRun := fs.Bool("dry-run", false, "rehearse against a built-in fake TikTok with rotated variants (no traffic, no proxies)")
	_ = fs.Parse(args)

	var cfg *config.Config
	var err error
	if *dryRun {
		cfg, err = config.Load("")
	} else {
		cfg, err = config.Load(*cfgPath)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0: config:", err)
		return 1
	}
	if *base == "" {
		*base = cfg.TT.BaseURL
	}
	if *fingerprint == "" {
		*fingerprint = cfg.TT.FingerprintProfile
	}
	if *rps <= 0 {
		*rps = cfg.TT.IdentityRPS
	}
	tierList := splitList(*tiers)
	rates, err := parseFloats(*ladderRates)
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0: --ladder-rates:", err)
		return 2
	}
	ages, err := parseDurations(*replayAges)
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0: --replay-ages:", err)
		return 2
	}
	var names []string
	var opener sign.Opener
	if *dryRun {
		w := fakett.NewWorld(fakett.DefaultSpec())
		hs := httptest.NewServer(fakett.NewServer(w, fakett.Options{Faults: DryRunFaults()}))
		defer hs.Close()
		*base, tierList, *rps, *parallel = hs.URL, []string{"direct"}, 10, 3
		for _, a := range w.Accounts() {
			names = append(names, a.Username)
		}
		if *outDir == "phase0/out-tt-phase0" {
			*outDir = "phase0/out-tt-phase0-dryrun"
		}
		if *from == "" {
			_ = os.RemoveAll(*outDir)
		}
		*ladderStep, *ladderIPs, rates = 2*time.Second, 1, []float64{5, 20, 150}
		ages, *replayWait = []time.Duration{time.Second, 3 * time.Second}, 5*time.Second
		*videos, *replays = 2, 3
		*ipMonth, *browserHour, *compute, *storage = 3, 0.05, 0.0001, 0.02 // made-up prices, to show the arithmetic
		cfg.FB.Proxy = config.Proxy{}
	} else if *accountsFile != "" || (*from != phase0.StepReport && *from != phase0.StepBundle) {
		if *accountsFile == "" {
			fmt.Fprintln(os.Stderr, "phase0: --accounts is required (or --dry-run, or --from report to price a report again)")
			return 2
		}
		all, err := readAccounts(*accountsFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "phase0:", err)
			return 1
		}
		names = spread(all, *maxAccounts)
	}
	if *signer == "browser" {
		bin, err := browser.FindBinary("")
		switch {
		case err == nil:
			opener = sign.BrowserOpener(bin, os.Getenv("TKS_BROWSER_NO_SANDBOX") != "" || os.Geteuid() == 0, cfg.TT.AcceptLanguage, 30*time.Second)
		case *dryRun:
			fmt.Fprintln(os.Stderr, "phase0: no Chromium: the dry run signs as fakett's page script does")
			opener = func(context.Context, *session.Identity, string) (sign.Page, error) { return simulatedPage{}, nil }
		default:
			fmt.Fprintln(os.Stderr, "phase0: --signer browser:", err, "(run: make tks-browser-check)")
			return 1
		}
	}
	for _, t := range tierList {
		if n, capped := tierIPs(cfg, t); capped && !*dryRun && *parallel > n {
			fmt.Fprintf(os.Stderr, "phase0: --parallel %d lowered to %d: tier %s has %d exit IP(s)\n", *parallel, max(n, 1), t, n)
			*parallel = max(n, 1)
		}
	}
	pool, err := identityPool(cfg.FB.Proxy, *base, *fingerprint, *rps, 2**parallel+4)
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0:", err)
		return 1
	}
	seed, err := ttclient.Seed()
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0:", err)
		return 1
	}
	sc := cfg.TT.Signer
	sc.Pages = *signerPages
	fr := cfg.TT.Freshness
	model := phase0.Model{ListInterval: fr.ListInterval, PageRefresh: fr.PageRefresh, CommentGrowth: *growth, Media: *mediaOn,
		IdentityRequests: (cfg.TT.IdentityMaxRequestsMin + cfg.TT.IdentityMaxRequestsMax) / 2, PageMaxSigns: sc.PageMaxSigns,
		IPMonthUSD: *ipMonth, BrowserHourUSD: *browserHour, ComputeUSD: *compute, StorageGBMonth: *storage, AssumedRPSPerIP: *rps}
	for _, w := range fr.MetricsWalks {
		model.MetricsWalks = append(model.MetricsWalks, phase0.Walk{Every: w.Every, Back: w.Back})
	}
	for _, w := range fr.CommentWindows {
		model.CommentWindows = append(model.CommentWindows, phase0.Window{MaxAge: w.MaxAge, Interval: w.Interval})
	}
	costs := map[string]float64{}
	var secrets []string
	for _, t := range cfg.FB.Proxy.Tiers {
		costs[t.Name] = t.CostPerGBUSD
		secrets = append(secrets, t.Username, t.Password)
		for _, ep := range t.Endpoints {
			if u, err := url.Parse(ep); err == nil && u.User != nil {
				p, _ := u.User.Password()
				secrets = append(secrets, u.User.Username(), p)
			}
		}
	}
	ladderRPS := rates[len(rates)-1]
	k := &phase0.Kit{
		Stack: phase0.Stack{BaseURL: *base, Pool: pool, HTTP: &http.Client{Timeout: 60 * time.Second}, Rules: ttdetect.Rules(),
			Loader: &session.Loader{Rules: ttdetect.Rules(), AcceptLanguage: cfg.TT.AcceptLanguage, Hooks: ttclient.Hooks()},
			Opener: opener, Signer: sc,
			LadderPools: func(tier string, n int, _ float64) ([]phase0.LadderIP, error) {
				return ladderPools(cfg.FB.Proxy, tier, n, *base, *fingerprint, ladderRPS)
			}},
		Cfg: phase0.Config{Out: *outDir, Tiers: tierList, Accounts: names, Seed: seed, From: *from, Parallel: *parallel,
			MaxCaptchas: *maxCaptchas, ReplayAges: ages, ReplayWait: *replayWait, IdentityRPS: *rps, AcceptLanguage: cfg.TT.AcceptLanguage,
			CookieDomains: cfg.TT.Media.CookieDomains, CostPerGB: costs, ConfigPath: *cfgPath, Secrets: secrets, Version: version,
			Run: phase0.Limits{BackfillDays: *backfill, MaxListPages: *maxList, CommentPosts: *commentPosts, MaxCommentPages: *maxComment,
				ReplyThreads: *replyThreads, MaxReplyPages: *maxReply, Videos: *videos, Replays: *replays},
			Smoke: phase0.Limits{BackfillDays: *backfill, MaxListPages: min(*maxList, 2), CommentPosts: 1, MaxCommentPages: min(*maxComment, 2),
				ReplyThreads: 1, MaxReplyPages: 1, Videos: min(*videos, 1)},
			Ladder:     phase0.LadderConfig{Rates: rates, Step: *ladderStep, IPs: *ladderIPs},
			Model:      model,
			Thresholds: phase0.Thresholds{BudgetUSD: *budget, TargetAccounts: *target}},
		Log: os.Stderr,
	}
	if *dryRun {
		k.Cfg.ConfigPath = ""
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "phase0: %d accounts, tiers %v, %d at a time, %.2f requests/s per identity, signer %s, out %s; Ctrl-C stops (resume with --from)\n",
		len(names), tierList, *parallel, *rps, *signer, *outDir)
	rep, err := k.Run(ctx)
	if err != nil {
		var se *phase0.StepError
		if errors.As(err, &se) && se.NeedsAction {
			fmt.Fprintf(os.Stderr, "\nphase0: %v\nphase0: send %s/%s back\n", err, *outDir, phase0.FileBundle)
			return 3
		}
		fmt.Fprintln(os.Stderr, "\nphase0:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "\nphase0: %s. Send %s/%s back.\n", rep.Verdict, *outDir, phase0.FileBundle)
	if rep.Verdict != "GO" {
		return 4
	}
	return 0
}

// identityPool builds the run's identity pool on the configured tiers, at
// the crawler's pace.
func identityPool(pc config.Proxy, base, fingerprint string, rps float64, perTier int) (*session.Pool, error) {
	fpBytes, err := configs.FS.ReadFile("fingerprints.yaml")
	if err != nil {
		return nil, err
	}
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, err
	}
	if _, ok := fps[fingerprint]; !ok {
		return nil, fmt.Errorf("unknown fingerprint %q (configs/fingerprints.yaml)", fingerprint)
	}
	// Phase 0 measures what TikTok does at the crawler's pace; the copied
	// pipeline's page-load pacing per exit is not tks's (tt.proxy).
	pc.PageLoads = config.PageLoads{}
	proxies, err := proxy.New(pc)
	if err != nil {
		return nil, err
	}
	return session.NewPool(session.PoolConfig{BaseURL: base, Profile: fingerprint, Timeout: 30 * time.Second,
		IdentityRPS: rps, IdentityBurst: 1, MaxRequestsMin: 400, MaxRequestsMax: 800, MaxPerTier: perTier}, proxies, fps, nil)
}

// ladderPools builds one identity pool per exit IP of a static tier (the
// first n), or one on this machine's IP for direct. A gateway has no fixed
// IP to climb.
func ladderPools(pc config.Proxy, tier string, n int, base, fingerprint string, rps float64) ([]phase0.LadderIP, error) {
	if tier == "direct" {
		p, err := identityPool(config.Proxy{}, base, fingerprint, rps+1, 20)
		if err != nil {
			return nil, err
		}
		return []phase0.LadderIP{{Pool: p, Tier: "direct"}}, nil
	}
	for _, t := range pc.Tiers {
		if t.Name != tier {
			continue
		}
		if t.Kind != "static_list" {
			return nil, nil
		}
		var out []phase0.LadderIP
		for i, ep := range t.Endpoints {
			if i >= n {
				break
			}
			one := t
			one.Name, one.Endpoints = tier+"-ip"+strconv.Itoa(i+1), []string{ep}
			p, err := identityPool(config.Proxy{Tiers: []config.ProxyTier{one}}, base, fingerprint, rps+1, 20)
			if err != nil {
				return nil, err
			}
			out = append(out, phase0.LadderIP{Pool: p, Tier: one.Name})
		}
		return out, nil
	}
	return nil, fmt.Errorf("no tier %q in the config", tier)
}

// spread picks n accounts spread evenly over the list (all when n is 0 or
// the list is shorter), so a sample is not just the file's top.
func spread(all []string, n int) []string {
	if n <= 0 || n >= len(all) {
		return all
	}
	out := make([]string, 0, n)
	for i := range n {
		out = append(out, all[i*len(all)/n])
	}
	return out
}

func splitList(v string) []string {
	var out []string
	for _, f := range strings.Split(v, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

func parseFloats(v string) ([]float64, error) {
	var out []float64
	for _, f := range splitList(v) {
		x, err := strconv.ParseFloat(f, 64)
		if err != nil || x <= 0 {
			return nil, fmt.Errorf("%q is not a positive number", f)
		}
		out = append(out, x)
	}
	if len(out) == 0 {
		return nil, errors.New("no rates")
	}
	return out, nil
}

func parseDurations(v string) ([]time.Duration, error) {
	var out []time.Duration
	for _, f := range splitList(v) {
		d, err := time.ParseDuration(f)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%q is not a duration", f)
		}
		out = append(out, d)
	}
	return out, nil
}
